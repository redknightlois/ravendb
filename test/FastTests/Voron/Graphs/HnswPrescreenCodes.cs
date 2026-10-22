using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Voron.Data.Lookups;
using Voron.Impl;
using Xunit;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;

namespace FastTests.Voron.Graphs;

public class HnswPrescreenCodes(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "test";
    private const int Dimensions = 16;
    private const int Edges = 12;
    private const int Beam = 16;

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void EveryFloatNodeHasACodeAndOtherGraphsHaveNone()
    {
        Build(TreeName, [Around(0, 100, 1), Around(0, 100, 2), Around(0, 100, 3)]);
        Create("int8", VectorEmbeddingType.Int8, Dimensions + sizeof(float));
        Register("int8", Enumerable.Range(0, 60).Select(i => Int8Tensor(i)).ToArray(), 1, new Random(1), new Random(1));

        using var tx = Env.ReadTransaction();
        var float32 = CodesOf(tx, TreeName);
        Assert.Equal(Enumerable.Range(1, 300).Select(i => (long)i), float32.Keys.Order());
        Assert.Equal(global::Voron.Global.Constants.Graphs.HnswVersion.CurrentVersion, GraphOptions(tx, TreeName).Version);

        using var int8State = new Hnsw.SearchState(tx.LowLevelTransaction, "int8");
        Assert.Equal(60, int8State.Options.CountOfVectors);
        Assert.Null(int8State.ReadCodeParameters());
        Assert.Empty(CodesOf(tx, "int8"));
        Assert.Throws<InvalidOperationException>(() => int8State.PrepareCodeQuery(new byte[Dimensions + sizeof(float)], 0.01));
    }

    // A commit continues the last block of the commit before it.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void CodesFillTheirBlocks()
    {
        Build(TreeName, [Around(0, 1000, 1), Around(0, 1000, 2)]);

        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var rowsPerBlock = state.Options.CodeRowsPerBlock;
        var blocks = tx.ReadTree(TreeName).LookupFor<Int64LookupKey>(Hnsw.CodePagesSlice);
        Assert.Equal(state.Options.CountOfVectors / rowsPerBlock + 1, blocks.NumberOfEntries);
        AssertEveryCodeReEncodes();
    }

    // A row wider than a page shares a block over several pages, which commits append to and leave at most an eighth unused.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void RowsWiderThanAPageHoldTheirCodes()
    {
        const int dimensions = 20_000;
        Create(TreeName, VectorEmbeddingType.Single, dimensions * sizeof(float));
        var random = new Random(4);
        var vectors = Enumerable.Range(0, 30).Select(_ => Enumerable.Range(0, dimensions).Select(_ => (float)random.NextDouble() - 0.5f).ToArray()).ToArray();
        Register(TreeName, vectors[..19], 1, new Random(1), new Random(1));
        Register(TreeName, vectors[19..], 20, new Random(2), new Random(2));

        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var parameters = state.ReadCodeParameters();
        Assert.True(parameters.RowBytes > global::Voron.Global.Constants.Storage.PageSize);
        Assert.True(tx.ReadTree(TreeName).LookupFor<Int64LookupKey>(Hnsw.CodePagesSlice).TryGetValue(0, out var first));
        var block = tx.LowLevelTransaction.GetPage(first);
        var pages = global::Voron.Impl.Paging.Paging.GetNumberOfOverflowPages(block.OverflowSize);
        Assert.True(pages > 1);
        Assert.Equal(state.Options.CodeRowsPerBlock * parameters.RowBytes, block.OverflowSize);
        Assert.True(8L * block.OverflowSize >= 7L * (pages * global::Voron.Global.Constants.Storage.PageSize - global::Voron.PageHeader.SizeOf));
        for (var id = 1L; id <= state.Options.CountOfVectors; id++)
            AssertReEncodes(state, id, parameters);
    }

    // A reader publishes the page of each block that it reads. A commit that appends to a published block keeps its page, and the page of a block
    // that a rolled back transaction allocated never reaches the cache.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void TheCacheMapsEachBlockToItsCommittedPage()
    {
        Build(TreeName, [Around(0, 300, 1)]);
        HnswIndexCache cache;
        using (var warm = Env.ReadTransaction())
        {
            using var name = Slice.From(warm.Allocator, TreeName, out var slice);
            cache = HnswIndexCache.WarmFromScratch(warm.LowLevelTransaction, slice, maxNodes: 1000);
        }

        using var _ = cache;
        AssertEveryCodeReEncodes(cache);

        using (var rolledBack = Env.WriteTransaction())
        {
            using (var registration = Hnsw.RegistrationFor(rolledBack.LowLevelTransaction, TreeName, new Random(3)))
            {
                var vectors = Around(0, 600, 3);
                for (var i = 0; i < vectors.Length; i++)
                    registration.Register(301 + i, MemoryMarshal.AsBytes(vectors[i].AsSpan()));
                registration.Commit(CancellationToken.None);
            }

            using var name = Slice.From(rolledBack.Allocator, TreeName, out var slice);
            using var state = new Hnsw.SearchState(rolledBack.LowLevelTransaction, slice, cache);
            Assert.False(state.ReadCode(state.Options.CountOfVectors).IsEmpty);
        }

        // Shifts the pages of the next commit away from those of the rolled back one.
        using (var tx = Env.WriteTransaction())
        {
            tx.LowLevelTransaction.AllocatePage(1);
            tx.Commit();
        }

        Register(TreeName, Around(0, 600, 2), 301, new Random(2), new Random(2));
        AssertEveryCodeReEncodes(cache);
    }

    // The storage report walks every entry of a tree that holds lookups; the code parameters are a raw value among them.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void StorageReportReadsAGraphTreeThatHoldsCodes()
    {
        Build(TreeName, [Around(0, 300, 1)]);

        using var tx = Env.ReadTransaction();
        var report = Env.GenerateDetailedReport(tx);
        Assert.Contains(report.Trees, t => t.Name == TreeName);
        Assert.Contains(report.Trees, t => t.Name == $"{TreeName}/NodeIdToLocation (Lookup)");
        Assert.Contains(report.Trees, t => t.Name == $"{TreeName}/CodePages (Lookup)");
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void PageOwnersReadsTheLookupsOfGraphTrees()
    {
        Build(TreeName, [Around(0, 300, 1)]);
        Create("int8", VectorEmbeddingType.Int8, Dimensions + sizeof(float));
        Register("int8", Enumerable.Range(0, 60).Select(i => Int8Tensor(i)).ToArray(), 1, new Random(1), new Random(1));

        using var tx = Env.ReadTransaction();
        var owners = Env.GetPageOwners(tx).Values.ToHashSet();
        foreach (var graph in new[] { TreeName, "int8" })
        {
            Assert.Contains($"{graph}/NodeIdToLocation", owners);
            Assert.Contains($"{graph}/NodesByVectorId", owners);
        }
        Assert.Contains($"{TreeName}/CodePages", owners);
    }

    // The levels come from the seeded level Random. They are the same in a graph that draws no code parameters (int8) and in one that does.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void DrawingTheParametersDoesNotShiftTheLevels()
    {
        // The parameters are drawn by the first commit, so the later commits are the ones whose levels a shifted Random would change.
        var raw = Around(0, 400, 7);
        Create(TreeName, VectorEmbeddingType.Single, Dimensions * sizeof(float));
        Create("int8", VectorEmbeddingType.Int8, Dimensions + sizeof(float));
        var random = new Random(5);
        var int8Random = new Random(5);
        for (var from = 0; from < raw.Length; from += 100)
        {
            Register(TreeName, raw[from..(from + 100)], 1 + from, random, new Random(99));
            Register("int8", raw[from..(from + 100)].Select((v, i) => Int8Tensor(from + i, v)).ToArray(), 1 + from, int8Random, new Random(99));
        }

        using var tx = Env.ReadTransaction();
        var levels = (string name) => Hnsw.IterateNodes(tx.LowLevelTransaction, name).Select(n => n.EdgesByLevel.Length).ToArray();
        var float32 = levels(TreeName);
        Assert.Contains(float32, l => l > 1);
        Assert.Equal(levels("int8"), float32);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ParametersAreFixedByTheFirstCommitAndNeverChange()
    {
        var first = new List<float[]> { new float[Dimensions] };
        first.AddRange(Around(0, 40, 1));
        var hashes = Build(TreeName, [first.ToArray()]);

        byte[] parameters;
        using (var tx = Env.ReadTransaction())
        {
            using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
            var stored = state.ReadCodeParameters();
            parameters = StoredParameters(tx);

            // The center is the mean of the unit vectors of the nodes of that commit, the zero vector excluded.
            var units = first.Skip(1).Select(v => Hnsw.ReadTensorVector<float>(Normalized(v)).ToArray()).ToArray();
            for (var k = 0; k < Dimensions; k++)
                Assert.Equal((float)units.Sum(u => (double)u[k]) / units.Length, stored.Center[k], 5);
        }

        Register(TreeName, [..Around(0, 40, 2), ..Opposite(40, 3)], 1000, new Random(2), new Random(2));
        Remove(hashes);

        using var read = Env.ReadTransaction();
        Assert.Equal(parameters, StoredParameters(read));
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void StoredCodesReEncodeFromTheStoredVectorsAndSurviveARestart()
    {
        RequireFileBasedPager();
        Build(TreeName, [Around(0, 150, 1), Opposite(150, 2), Around(1, 150, 3)]);

        var before = ReadStoredRows();
        Assert.Equal(450, before.Count);
        AssertEveryCodeReEncodes();

        RestartDatabase();

        Assert.Equal(before, ReadStoredRows());
        AssertEveryCodeReEncodes();
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void RegistrationDisposedWithoutCommitLeavesNoCode()
    {
        Create(TreeName, VectorEmbeddingType.Single, Dimensions * sizeof(float));
        using (var tx = Env.WriteTransaction())
        {
            using (var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, TreeName, new Random(1)))
            {
                foreach (var (vector, i) in Around(0, 20, 1).Select((v, i) => (v, i)))
                    registration.Register(1 + i, MemoryMarshal.AsBytes(vector.AsSpan()));
            }

            tx.Commit();
        }

        using var read = Env.ReadTransaction();
        Assert.Empty(CodesOf(read, TreeName));
        Assert.Null(StoredParametersOrNull(read));
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ReaderSnapshotHoldsACodeForEveryNodeItHoldsAndNoOther()
    {
        Build(TreeName, [Around(0, 100, 1)]);
        using var snapshot = Env.ReadTransaction();

        Register(TreeName, Around(0, 100, 2), 1000, new Random(2), new Random(2));

        Assert.Equal(Enumerable.Range(1, 100).Select(i => (long)i), CodesOf(snapshot, TreeName).Keys.Order());
        using var state = new Hnsw.SearchState(snapshot.LowLevelTransaction, TreeName);
        Assert.Equal(100, state.Options.CountOfVectors);

        using var latest = Env.ReadTransaction();
        Assert.Equal(200, CodesOf(latest, TreeName).Count);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void GraphsThatShareAStoredVectorHoldTheirOwnCodes()
    {
        var vectors = Around(0, 60, 1);
        foreach (var (name, seed) in new[] { ("a", 1), ("b", 2) })
        {
            Create(name, VectorEmbeddingType.Single, Dimensions * sizeof(float));
            Register(name, vectors, 1, new Random(3), new Random(seed));
        }

        using var tx = Env.ReadTransaction();
        using var a = new Hnsw.SearchState(tx.LowLevelTransaction, "a");
        using var b = new Hnsw.SearchState(tx.LowLevelTransaction, "b");
        var parametersA = a.ReadCodeParameters();
        var parametersB = b.ReadCodeParameters();
        Assert.NotEqual(parametersA.Dither, parametersB.Dither);
        Assert.Equal(parametersA.Center, parametersB.Center);

        for (var id = 1L; id <= 60; id++)
        {
            Assert.Equal(a.GetNodeById(id).VectorId, b.GetNodeById(id).VectorId);
            var rowA = a.ReadCode(id);
            var rowB = b.ReadCode(id);
            Assert.NotEqual(rowA.ToArray(), rowB.ToArray());
            AssertReEncodes(a, id, parametersA);
            AssertReEncodes(b, id, parametersB);
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void RemovalChangesNoCodeAndRegisteringAgainReusesTheNode()
    {
        var vectors = Around(0, 100, 1);
        var hashes = Build(TreeName, [vectors]);
        var before = ReadStoredRows();
        byte[] parameters;
        using (var tx = Env.ReadTransaction())
            parameters = StoredParameters(tx);

        Remove(hashes);

        Assert.Equal(before, ReadStoredRows());

        Register(TreeName, [vectors[0]], 5000, new Random(1), new Random(1));
        Assert.Equal(before, ReadStoredRows());
        using var read = Env.ReadTransaction();
        Assert.Equal(parameters, StoredParameters(read));
        using var state = new Hnsw.SearchState(read.LowLevelTransaction, TreeName);
        Assert.Equal(100, state.Options.CountOfVectors);
    }

    // The parameters are fixed from the first commit, which holds one distribution. The later commits insert vectors in the opposite direction and
    // vectors on three coordinates. For a fixed pair the bound exceeds the exact distance with probability at most alpha over the dither, so the expected
    // count of such pairs over the draw is at most alpha times the number of pairs, whatever the dependence between the pairs that share the dither.
    // By Markov's inequality the count passes alpha * pairs / failure with probability at least 1 - failure. The test assumes nothing else. Without
    // the error term the estimate exceeds the exact distance for about half of the pairs.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void BoundHoldsAtTheAllowanceForNodesInsertedAfterTheParametersAreFixed()
    {
        const double alpha = 0.01;
        const double failure = 0.1;
        Build(TreeName, [Around(0, 200, 1), Opposite(200, 2), Concentrated(200, 3)]);
        var queries = Around(0, 10, 4).Concat(Opposite(10, 5)).Concat(Concentrated(10, 6)).Concat(Around(2, 10, 7)).ToArray();

        var pairs = 0;
        var violations = 0;
        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        foreach (var raw in queries)
        {
            var query = Normalized(raw);
            var prepared = state.PrepareCodeQuery(query, alpha);
            for (var id = 1L; id <= state.Options.CountOfVectors; id++)
            {
                var bound = state.CodeLowerBound(prepared, id);
                var exact = Hnsw.CosineDistanceSinglesNormalized(query, state.GetNodeById(id).GetVector(state));
                pairs++;
                if (bound > exact)
                    violations++;
            }
        }

        Assert.Equal(queries.Length * 600, pairs);
        Assert.True(violations <= alpha * pairs / failure, $"{violations} of {pairs} bounds exceed the exact distance");
    }

    // Every coordinate of the transformed vector is within half a step of its decoded level, so no level is clamped, even for the nodes far from the
    // vectors that fixed the center, and the stored scale is at least the exact product of the norm and the step that it rounds.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void EveryCoordinateIsWithinHalfAStepAndTheScaleRoundsUp()
    {
        Build(TreeName, [Around(0, 100, 1), Opposite(100, 2), Concentrated(100, 3)]);

        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var parameters = state.ReadCodeParameters();
        for (var id = 1L; id <= state.Options.CountOfVectors; id++)
        {
            var unit = Hnsw.ReadTensorVector<float>(state.GetNodeById(id).GetVector(state));
            var w = new double[Dimensions];
            double normSquared = 0;
            for (var k = 0; k < Dimensions; k++)
            {
                w[k] = (double)unit[k] - parameters.Center[k];
                normSquared += w[k] * w[k];
            }

            var norm = Math.Sqrt(normSquared);
            Assert.True(norm > 0);
            for (var k = 0; k < Dimensions; k++)
                w[k] /= norm;
            parameters.Rotate(w);
            var step = 2 * w.Max(Math.Abs) / (Hnsw.CodeParameters.Levels - 1);

            var row = state.ReadCode(id);
            for (var k = 0; k < Dimensions; k++)
            {
                var decoded = (Hnsw.CodeParameters.Level(row, k) - Hnsw.CodeParameters.ZeroLevel - (double)parameters.Dither[k]) * step;
                Assert.True(Math.Abs(decoded - w[k]) <= step / 2 * (1 + 1e-9), $"node {id} coordinate {k}");
            }

            var scale = Hnsw.CodeParameters.Scale(row, parameters.CodeBytes);
            Assert.True(scale >= norm * step, $"node {id}");
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void NodesAndQueriesWithoutADirectionHaveNoBound()
    {
        // The first commit holds one vector, so the center is that vector and node 1 is equal to the center.
        Build(TreeName, [Around(0, 1, 1), [new float[Dimensions]], Around(0, 50, 2)]);

        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var query = Normalized(Around(1, 1, 9)[0]);
        var prepared = state.PrepareCodeQuery(query, 0.01);
        Assert.Equal(double.NegativeInfinity, state.CodeLowerBound(prepared, 1));
        Assert.Equal(double.NegativeInfinity, state.CodeLowerBound(prepared, 2));
        Assert.All(Enumerable.Range(3, 50), id => Assert.True(double.IsFinite(state.CodeLowerBound(prepared, id))));

        var zeroQuery = state.PrepareCodeQuery(Normalized(new float[Dimensions]), 0.01);
        Assert.All(Enumerable.Range(1, 52), id => Assert.Equal(double.NegativeInfinity, state.CodeLowerBound(zeroQuery, id)));

        Assert.Equal(0, state.Nodes.Length);
        Assert.Throws<InvalidOperationException>(() => state.CodeLowerBound(prepared, 53));
    }

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-0.5)]
    [InlineData(2)]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void AllowanceOutsideTheOpenUnitIntervalIsRejected(double alpha)
    {
        Build(TreeName, [Around(0, 10, 1)]);

        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        Assert.Throws<ArgumentOutOfRangeException>(() => state.PrepareCodeQuery(Normalized(Around(0, 1, 2)[0]), alpha));
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public unsafe void MalformedQueriesAndStoredParametersAreRefused()
    {
        Build(TreeName, [Around(0, 10, 1)]);
        using (var tx = Env.WriteTransaction())
        {
            using (var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName))
                Assert.Throws<ArgumentException>(() => state.PrepareCodeQuery(Normalized(new float[Dimensions + 1]), 0.01));

            using (tx.CreateTree(TreeName).DirectAdd(Hnsw.CodeParametersSlice, 10, out _))
            {
            }

            using var corrupted = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
            Assert.Throws<InvalidDataException>(() => corrupted.ReadCodeParameters());
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void NonFiniteVectorsHaveNoBound()
    {
        var parameters = Hnsw.CodeParameters.Draw(new float[Dimensions], new Random(1));
        var scratch = new double[Dimensions];
        var row = new byte[parameters.RowBytes];
        parameters.Encode(Normalized(Around(0, 1, 1)[0]), scratch, row);
        using var valid = parameters.Prepare(Normalized(Around(0, 1, 2)[0]), 0.01, Allocator);
        Assert.True(double.IsFinite(valid.LowerBound(row)));

        var nan = Around(0, 1, 3)[0];
        nan[4] = float.NaN;
        var nanRow = new byte[parameters.RowBytes];
        parameters.Encode(Normalized(nan), scratch, nanRow);
        Assert.Equal(double.NegativeInfinity, valid.LowerBound(nanRow));
        using var nanQuery = parameters.Prepare(Normalized(nan), 0.01, Allocator);
        Assert.Equal(double.NegativeInfinity, nanQuery.LowerBound(row));
    }

    // The kernel is exact at the extreme levels and shorts, below, at and past the chunks of its int lanes.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void TheLowerBoundKernelIsExact()
    {
        var random = new Random(5);
        foreach (var length in new[] { 1, 15, 16, 17, 2048, 2049, 40_000 })
        {
            foreach (var extreme in new[] { true, false })
            {
                var levels = new byte[length];
                var even = new short[length];
                var odd = new short[length];
                long expected = 0;
                for (var i = 0; i < length; i++)
                {
                    levels[i] = extreme ? byte.MaxValue : (byte)random.Next(256);
                    even[i] = extreme ? short.MinValue : (short)random.Next(short.MinValue, short.MaxValue + 1);
                    odd[i] = extreme ? short.MinValue : (short)random.Next(short.MinValue, short.MaxValue + 1);
                    expected += (levels[i] & 15) * (long)even[i] + (levels[i] >> 4) * (long)odd[i];
                }

                Assert.Equal(expected, Hnsw.CodeQuery.Dot(levels, even, odd));
            }
        }
    }

    private void AssertEveryCodeReEncodes(HnswIndexCache cache = null)
    {
        using var tx = Env.ReadTransaction();
        using var name = Slice.From(tx.Allocator, TreeName, out var slice);
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, slice, cache);
        var parameters = state.ReadCodeParameters();
        for (var id = 1L; id <= state.Options.CountOfVectors; id++)
            AssertReEncodes(state, id, parameters);
    }

    private static void AssertReEncodes(Hnsw.SearchState state, long id, Hnsw.CodeParameters parameters)
    {
        var expected = new byte[parameters.RowBytes];
        parameters.Encode(state.GetNodeById(id).GetVector(state), new double[parameters.Dimensions], expected);
        Assert.Equal(expected, state.ReadCode(id).ToArray());
    }

    private Dictionary<long, string> ReadStoredRows()
    {
        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        return Enumerable.Range(1, (int)state.Options.CountOfVectors).ToDictionary(id => (long)id, id => Convert.ToHexString(state.ReadCode(id)));
    }

    // Probes one id past the nodes on each side, whose rows must read empty.
    private static Dictionary<long, byte[]> CodesOf(Transaction tx, string name)
    {
        var codes = new Dictionary<long, byte[]>();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, name);
        for (var id = 0L; id <= state.Options.CountOfVectors + 1; id++)
        {
            var row = state.ReadCode(id);
            if (row.IsEmpty == false)
                codes.Add(id, row.ToArray());
        }

        return codes;
    }

    private static unsafe byte[] StoredParameters(Transaction tx) => StoredParametersOrNull(tx) ?? throw new InvalidOperationException("No parameters");

    private static unsafe byte[] StoredParametersOrNull(Transaction tx)
    {
        var tree = tx.ReadTree(TreeName);
        var stored = tree.DirectRead(Hnsw.CodeParametersSlice);
        return stored is null ? null : new ReadOnlySpan<byte>(stored, tree.GetDataSize(Hnsw.CodeParametersSlice)).ToArray();
    }

    private static unsafe Hnsw.Options GraphOptions(Transaction tx, string name) => Unsafe.Read<Hnsw.Options>(tx.ReadTree(name).DirectRead(Hnsw.OptionsSlice));

    private void Create(string name, VectorEmbeddingType type, int vectorSizeBytes)
    {
        using var tx = Env.WriteTransaction();
        Hnsw.Create(tx.LowLevelTransaction, name, vectorSizeBytes, Edges, Beam, type);
        tx.Commit();
    }

    private IReadOnlyList<byte[]> Build(string name, float[][][] commits)
    {
        Create(name, VectorEmbeddingType.Single, Dimensions * sizeof(float));
        var hashes = new List<byte[]>();
        var random = new Random(1);
        var codeRandom = new Random(1);
        var entry = 1;
        foreach (var commit in commits)
        {
            hashes.AddRange(Register(name, commit, entry, random, codeRandom));
            entry += commit.Length;
        }

        return hashes;
    }

    private IReadOnlyList<byte[]> Register(string name, float[][] vectors, int firstEntry, Random random, Random codeRandom) =>
        Register(name, vectors.Select(v => MemoryMarshal.AsBytes(v.AsSpan()).ToArray()).ToArray(), firstEntry, random, codeRandom);

    private IReadOnlyList<byte[]> Register(string name, byte[][] vectors, int firstEntry, Random random, Random codeRandom)
    {
        var hashes = new List<byte[]>();
        using var tx = Env.WriteTransaction();
        using var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, name, random);
        registration.ForTestingPurposesOnly().CodeRandom = codeRandom;
        for (var i = 0; i < vectors.Length; i++)
            hashes.Add(registration.Register(firstEntry + i, vectors[i]).ToSpan().ToArray());
        registration.Commit(CancellationToken.None);
        tx.Commit();
        return hashes;
    }

    // Removes every second vector registered by the first commit of Build.
    private void Remove(IReadOnlyList<byte[]> hashes)
    {
        using var tx = Env.WriteTransaction();
        using var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, TreeName, new Random(1));
        for (var i = 0; i < hashes.Count; i += 2)
            registration.Remove(1 + i, hashes[i]);
        registration.Commit(CancellationToken.None);
        tx.Commit();
    }

    private static byte[] Normalized(float[] raw)
    {
        var tensor = new byte[Hnsw.TensorSizeBytes<float>(raw.Length)];
        Hnsw.WriteNormalizedTensor(raw, tensor);
        return tensor;
    }

    private static byte[] Int8Tensor(int seed, float[] source = null)
    {
        var random = new Random(seed);
        var values = source?.Select(v => (sbyte)(v * 100)).ToArray() ?? Enumerable.Range(0, Dimensions).Select(_ => (sbyte)random.Next(-100, 100)).ToArray();
        var tensor = new byte[Hnsw.TensorSizeBytes<sbyte>(Dimensions)];
        Hnsw.WriteTensor<sbyte>(values, 1f, tensor);
        return tensor;
    }

    private static float[] Gaussian(Random random, int count) =>
        Enumerable.Range(0, count).Select(_ => (float)(Math.Sqrt(-2 * Math.Log(1 - random.NextDouble())) * Math.Cos(2 * Math.PI * random.NextDouble()))).ToArray();

    // Vectors around the direction of the first coordinate; the shift moves the center for a second cluster.
    private static float[][] Around(int shift, int count, int seed)
    {
        var random = new Random(seed);
        return Enumerable.Range(0, count).Select(_ =>
        {
            var v = Gaussian(random, Dimensions).Select(x => x * 0.4f).ToArray();
            v[shift] += 3;
            return v;
        }).ToArray();
    }

    private static float[][] Opposite(int count, int seed) => Around(0, count, seed).Select(v => v.Select(x => -x).ToArray()).ToArray();

    private static float[][] Concentrated(int count, int seed)
    {
        var random = new Random(seed);
        return Enumerable.Range(0, count).Select(_ =>
        {
            var v = new float[Dimensions];
            var g = Gaussian(random, 3);
            v[5] = g[0] + 2;
            v[9] = g[1];
            v[14] = g[2] - 2;
            return v;
        }).ToArray();
    }
}
