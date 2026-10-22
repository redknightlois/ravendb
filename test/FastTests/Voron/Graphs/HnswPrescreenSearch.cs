using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Voron.Util;
using Xunit;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;

namespace FastTests.Voron.Graphs;

public class HnswPrescreenSearch(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "test";
    private const int Dimensions = 16;
    private const int Count = 3000;

    // A rejected id is remembered for the rest of the traversal: it is neither checked again nor counted as rejected again.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ARejectedIdIsNeverCheckedAgainInATraversal()
    {
        Build(Count);
        long rejections = 0;
        using var tx = Env.ReadTransaction();
        foreach (var query in Gaussians(60, 99))
        {
            using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
            var hook = state.ForTestingPurposesOnly();
            Search(state, query, 16);
            rejections += hook.Rejections;
            Assert.Equal(hook.Rejections, hook.LastScreen.Rejected);
            // An id that is checked and not rejected is resolved, so a fresh state checks every id at most once.
            Assert.True(hook.CodeRowsRead <= state.Nodes.Length + hook.Rejections);
        }

        Assert.NotEqual(0, rejections);
    }

    // 4 * candidates wraps above int.MaxValue / 4: the cap must not shrink as the number of candidates grows.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void TheCapNeverShrinksWhenTheNumberOfCandidatesGrows()
    {
        Build(300);
        var query = Gaussians(1, 5)[0];
        using var tx = Env.ReadTransaction();

        int CapAt(int numberOfCandidates)
        {
            using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
            var hook = state.ForTestingPurposesOnly();
            Search(state, query, numberOfCandidates);
            return hook.LastScreen.Cap;
        }

        var caps = new[] { 64, 1_000_000, int.MaxValue / 4, int.MaxValue / 4 + 1, int.MaxValue }.Select(CapAt).ToArray();
        Assert.Equal(caps.Order(), caps);
        Assert.True(caps[0] >= 512);
    }

    // A graph with no more nodes than the working set holds never has a full working set, so every node is admitted and returned.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void NothingIsRejectedWhileTheWorkingSetIsNotFull()
    {
        const int nodes = 40;
        Build(Gaussians(nodes, 3));
        using var tx = Env.ReadTransaction();
        foreach (var query in Gaussians(20, 4))
        {
            using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
            var hook = state.ForTestingPurposesOnly();
            var found = Search(state, query, 2 * nodes).Split('|')[0].Split(',');
            Assert.Equal(nodes, found.Length);
            Assert.Equal(0, hook.Rejections);
        }
    }

    // The cap stops the screen: no expansion screens its ids when they could take the admissions of the traversal above the cap.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void TheCapStopsTheScreen()
    {
        Build(Count);
        const int cap = 8;
        var spent = 0;
        using var tx = Env.ReadTransaction();
        foreach (var query in Gaussians(40, 3))
        {
            using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
            state.ForTestingPurposesOnly().Cap = cap;
            Search(state, query, 16);

            var hook = state.ForTestingPurposesOnly();
            Assert.Equal(cap, hook.LastScreen.Cap);
            Assert.True(hook.Commitment <= cap, $"{hook.Commitment} events committed against a cap of {cap}");
            if (hook.LastScreen.Admissions > cap)
                spent++;
        }

        Assert.NotEqual(0, spent);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void RejectionNeedsTheBoundStrictlyBeyondTheMargin()
    {
        var margin = Hnsw.CodeParameters.Margin(Dimensions);
        Assert.True(margin > 0 && margin < 1e-3, $"margin {margin}");
        Assert.True(Hnsw.CodeParameters.Margin(1024) > margin);

        const double threshold = 0.25;
        var edge = threshold + margin;
        Assert.False(Hnsw.CodeScreen.Exceeds(edge, threshold, margin));
        Assert.True(Hnsw.CodeScreen.Exceeds(Math.BitIncrement(edge), threshold, margin));
        Assert.False(Hnsw.CodeScreen.Exceeds(double.NegativeInfinity, threshold, margin));
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void RejectsKeepsTheMarginBetweenTheBoundAndTheThreshold()
    {
        Build(Count);
        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var tensor = Tensor(Gaussians(1, 11)[0]);
        using var screen = new Hnsw.CodeScreen(state.CodeParameters, tensor, Hnsw.CodeScreen.DefaultCap(16), state.Options.CountOfVectors, Allocator);
        var row = state.ReadCode(1);
        using var prepared = state.PrepareCodeQuery(tensor, Hnsw.CodeScreen.FailureBudget / screen.Cap);
        var bound = prepared.LowerBound(row);

        // The thresholds are far enough from the bound that the rounding of threshold + margin does not decide the outcome.
        Assert.True(double.IsFinite(bound));
        Assert.True(screen.Margin > 4 * (Math.BitIncrement(Math.Abs(bound)) - Math.Abs(bound)));
        Assert.False(screen.Rejects(row, bound - screen.Margin / 2));
        Assert.True(screen.Rejects(row, bound - 2 * screen.Margin));
    }

    // The screen remembers every rejected id of the snapshot across word boundaries, counts each once, and ignores ids outside the snapshot.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void TheScreenRemembersEachRejectedIdOfTheSnapshot()
    {
        Build(Count);
        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var max = state.Options.CountOfVectors;
        using var screen = new Hnsw.CodeScreen(state.CodeParameters, Tensor(Gaussians(1, 11)[0]), Hnsw.CodeScreen.DefaultCap(16), max, Allocator);
        long[] inside = [1, 63, 64, 65, max];
        foreach (var id in inside.Concat([max + 1, -1]))
        {
            screen.Reject(id);
            screen.Reject(id);
        }

        Assert.Equal(inside.Length, screen.Rejected);
        Assert.All(inside, id => Assert.True(screen.IsRejected(id)));
        Assert.All(new[] { 2, 62, 66, max - 1, max + 1, -1 }, id => Assert.False(screen.IsRejected(id)));
    }

    // A stored parameters value of another size, including one shorter than its header, fails the search that decodes it, with and without the cache;
    // the warm-up of the cache does not decode them, so it still succeeds.
    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineData(false, 8)]
    [InlineData(true, 8)]
    [InlineData(false, 2)]
    [InlineData(true, 0)]
    public void ACorruptParametersValueFailsTheSearch(bool withCache, int size)
    {
        Build(Count);
        using (var tx = Env.WriteTransaction())
        {
            tx.ReadTree(TreeName).Add(Hnsw.CodeParametersSlice, new byte[size]);
            tx.Commit();
        }

        using var read = Env.ReadTransaction();
        using var name = Slice.From(Allocator, TreeName, out var slice);
        using var cache = withCache ? HnswIndexCache.WarmFromScratch(read.LowLevelTransaction, slice, maxNodes: Count / 2) : null;
        using var state = new Hnsw.SearchState(read.LowLevelTransaction, slice, cache);
        Assert.Throws<InvalidDataException>(() => Search(state, Gaussians(1, 21)[0], 64));
    }

    // The graph's cache decodes the parameters once for every SearchState that is attached to it, including a state that is opened after the first commit
    // fixed them.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void TheCacheDecodesTheParametersOncePerGraph()
    {
        using (var tx = Env.WriteTransaction())
        {
            Hnsw.Create(tx.LowLevelTransaction, TreeName, Dimensions * sizeof(float), 12, 16, VectorEmbeddingType.Single);
            tx.Commit();
        }

        using var emptyRead = Env.ReadTransaction();
        using var name = Slice.From(Allocator, TreeName, out var slice);
        using var cache = HnswIndexCache.WarmFromScratch(emptyRead.LowLevelTransaction, slice, maxNodes: Count);
        using (var empty = new Hnsw.SearchState(emptyRead.LowLevelTransaction, slice, cache))
            Assert.Null(empty.CodeParameters);

        Assert.Null(cache.CodeParameters);
        Build(Count, create: false);

        using var read = Env.ReadTransaction();
        using var first = new Hnsw.SearchState(read.LowLevelTransaction, slice, cache);
        using var second = new Hnsw.SearchState(read.LowLevelTransaction, slice, cache);
        Assert.NotNull(first.CodeParameters);
        Assert.Same(first.CodeParameters, cache.CodeParameters);
        Assert.Same(first.CodeParameters, second.CodeParameters);
    }

    // Each graph's cache holds the decode of that graph's parameters only.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void EachGraphsCacheHoldsItsOwnParameters()
    {
        Build(Count);
        Build(Gaussians(Count, 5, 2 * Dimensions), treeName: "wide");
        using var tx = Env.ReadTransaction();
        using var name = Slice.From(Allocator, TreeName, out var slice);
        using var wideName = Slice.From(Allocator, "wide", out var wideSlice);
        using var cache = HnswIndexCache.WarmFromScratch(tx.LowLevelTransaction, slice, maxNodes: Count);
        using var wideCache = HnswIndexCache.WarmFromScratch(tx.LowLevelTransaction, wideSlice, maxNodes: Count);
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, slice, cache);
        using var wide = new Hnsw.SearchState(tx.LowLevelTransaction, wideSlice, wideCache);
        Assert.Equal(Dimensions, state.CodeParameters.Dimensions);
        Assert.Equal(2 * Dimensions, wide.CodeParameters.Dimensions);
        Assert.Same(state.CodeParameters, cache.CodeParameters);
        Assert.Same(wide.CodeParameters, wideCache.CodeParameters);
    }

    // Concurrent queries on one cache share a single decode of the parameters.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ConcurrentReadersShareOneDecodedCopy()
    {
        Build(Count);
        HnswIndexCache cache;
        using (var warm = Env.ReadTransaction())
        {
            using var name = Slice.From(warm.Allocator, TreeName, out var slice);
            cache = HnswIndexCache.WarmFromScratch(warm.LowLevelTransaction, slice, maxNodes: Count);
        }

        using var _ = cache;
        var seen = new Hnsw.CodeParameters[8];
        Parallel.For(0, seen.Length, new ParallelOptions { MaxDegreeOfParallelism = seen.Length }, i =>
        {
            using var tx = Env.ReadTransaction();
            using var name = Slice.From(tx.Allocator, TreeName, out var slice);
            using var state = new Hnsw.SearchState(tx.LowLevelTransaction, slice, cache);
            Search(state, Gaussians(1, 30 + i)[0], 16);
            seen[i] = state.CodeParameters;
        });

        Assert.NotNull(cache.CodeParameters);
        Assert.All(seen, parameters => Assert.Same(cache.CodeParameters, parameters));
    }

    // Per query, the screen setup allocates nothing in proportion to the dimension count when the field's cache is attached, as a Corax query runs.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void TheScreenSetupDoesNotAllocateInProportionToTheDimensions()
    {
        const int queries = 8, small = 64, large = 1024, nodes = 600;
        var allocatedSmall = AllocatedPerQuery(small, nodes, queries);
        var allocatedLarge = AllocatedPerQuery(large, nodes, queries);

        // What the setup allocated per query: the center and the dither (2 floats), z and the even and odd halves (2 doubles), and the signs (four bits per coordinate
        // at a power of two).
        long setupPerDimension = 2 * sizeof(float) + 2 * sizeof(double) + 1;
        var setupDelta = (large - small) * setupPerDimension * queries;
        // The work of the two graphs differs, so the collections that grow with it (the node index and the rejection memo) differ by a few steps of a resize.
        // The allowance stays far below the setup.
        Assert.True(allocatedLarge - allocatedSmall < setupDelta / 3, $"{allocatedLarge - allocatedSmall} bytes more at {large} dimensions, the setup was {setupDelta}");
    }

    private long AllocatedPerQuery(int dimensions, int nodes, int queries)
    {
        var random = new Random(dimensions);
        var vectors = Enumerable.Range(0, nodes).Select(_ => Enumerable.Range(0, dimensions).Select(_ => random.NextSingle() - 0.5f).ToArray()).ToArray();
        using (var tx = Env.WriteTransaction())
        {
            Hnsw.Create(tx.LowLevelTransaction, $"{TreeName}{dimensions}", dimensions * sizeof(float), 12, 16, VectorEmbeddingType.Single);
            tx.Commit();
        }

        using (var tx = Env.WriteTransaction())
        {
            using var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, $"{TreeName}{dimensions}", new Random(1));
            registration.ForTestingPurposesOnly().CodeRandom = new Random(2);
            for (var i = 0; i < nodes; i++)
                registration.Register(1 + i, MemoryMarshal.AsBytes(vectors[i].AsSpan()));
            registration.Commit(CancellationToken.None);
            tx.Commit();
        }

        var queryBytes = Enumerable.Range(0, queries + 1).Select(_ => MemoryMarshal.AsBytes(Enumerable.Range(0, dimensions).Select(_ => random.NextSingle() - 0.5f).ToArray().AsSpan()).ToArray()).ToArray();
        var matches = new long[256];
        var distances = new float[256];
        using var read = Env.ReadTransaction();
        using var name = Slice.From(Allocator, $"{TreeName}{dimensions}", out var slice);
        using var cache = HnswIndexCache.WarmFromScratch(read.LowLevelTransaction, slice, maxNodes: nodes);

        // The first query decodes the parameters into the cache.
        Query(queryBytes[0]);
        long allocated = 0;
        for (var i = 1; i <= queries; i++)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            var rows = Query(queryBytes[i]);
            allocated += GC.GetAllocatedBytesForCurrentThread() - before;
            // The working set filled and the screen checked codes, so the setup ran inside the measured region.
            Assert.NotEqual(0, rows);
        }

        return allocated;

        long Query(byte[] query)
        {
            using var state = new Hnsw.SearchState(read.LowLevelTransaction, slice, cache);
            var hook = state.ForTestingPurposesOnly();
            using var retriever = Hnsw.ApproximateNearest(state, 16, query, 0f);
            while (retriever.Fill(matches, distances, null) > 0)
            {
            }

            return hook.CodeRowsRead;
        }
    }

    private static byte[] Tensor(float[] vector)
    {
        var tensor = new byte[Hnsw.TensorSizeBytes<float>(vector.Length)];
        Hnsw.WriteNormalizedTensor(vector, tensor);
        return tensor;
    }

    // A NaN, an infinite and an overflowing vector in the commit that fixes the parameters stay out of the center: the parameters are finite, every other node
    // keeps a bound, those three nodes have none, and the screen still rejects.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void NonFiniteVectorsDoNotPoisonTheCodeParameters()
    {
        var vectors = Gaussians(2000, 1);
        vectors[5][3] = float.NaN;
        vectors[7][3] = float.PositiveInfinity;
        Array.Fill(vectors[9], 1e20f);
        long[] withoutBound = [6, 8, 10];
        Build(vectors);

        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var parameters = state.CodeParameters;
        Assert.All(parameters.Center, c => Assert.True(float.IsFinite(c)));
        Assert.All(parameters.Dither, d => Assert.True(float.IsFinite(d)));

        using var query = state.PrepareCodeQuery(Tensor(Gaussians(1, 31)[0]), 0.01);
        for (var id = 1L; id <= vectors.Length; id++)
            Assert.Equal(withoutBound.Contains(id), double.IsNegativeInfinity(state.CodeLowerBound(query, id)));

        long rejections = 0;
        foreach (var gaussian in Gaussians(20, 32))
        {
            using var searching = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
            var hook = searching.ForTestingPurposesOnly();
            Search(searching, gaussian, 16);
            rejections += hook.Rejections;
        }

        Assert.NotEqual(0, rejections);
    }

    // A removed vector stays in the graph and passes the threshold without entering the working set. A node is evaluated once per traversal, so more admission
    // events than live vectors are only possible when those nodes count.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void RemovedVectorsAreAdmissionEvents()
    {
        const int live = 100;
        var hashes = Build(Count);
        using (var tx = Env.WriteTransaction())
        {
            using var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, TreeName, new Random(1));
            for (var i = 0; i < Count - live; i++)
                registration.Remove(1 + i, hashes[i]);
            registration.Commit(CancellationToken.None);
            tx.Commit();
        }

        using var read = Env.ReadTransaction();
        var mostEvents = 0;
        foreach (var query in Gaussians(40, 8))
        {
            using var state = new Hnsw.SearchState(read.LowLevelTransaction, TreeName);
            var hook = state.ForTestingPurposesOnly();
            Search(state, query, 16);
            mostEvents = Math.Max(mostEvents, hook.LastScreen.Admissions);
        }

        Assert.True(mostEvents > live, $"{mostEvents} events");
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void FilteredAndInt8TraversalsAreNotScreenedAndAZeroQueryNeverRejects()
    {
        Build(Count);
        using var tx = Env.ReadTransaction();

        using var filtered = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var filteredHook = filtered.ForTestingPurposesOnly();
        var filter = new GrowableBitArray(Allocator, Count + 1);
        filter.Add(1);
        filter.Count = 1;
        Search(filtered, Gaussians(1, 1)[0], 64, filter);
        Assert.Null(filteredHook.LastScreen);
        Assert.Equal(0, filteredHook.CodeRowsRead);

        // A query without direction has no bound, so it is screened and never rejects.
        using var zero = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var zeroHook = zero.ForTestingPurposesOnly();
        Search(zero, new float[Dimensions], 64);
        Assert.NotNull(zeroHook.LastScreen);
        Assert.NotEqual(0, zeroHook.CodeRowsRead);
        Assert.Equal(0, zeroHook.Rejections);

        using (var write = Env.WriteTransaction())
        {
            Hnsw.Create(write.LowLevelTransaction, "int8", Dimensions + sizeof(float), 12, 16, VectorEmbeddingType.Int8);
            using var registration = Hnsw.RegistrationFor(write.LowLevelTransaction, "int8", new Random(1));
            var random = new Random(2);
            for (var i = 0; i < 100; i++)
            {
                var tensor = new byte[Hnsw.TensorSizeBytes<sbyte>(Dimensions)];
                Hnsw.WriteTensor<sbyte>(Enumerable.Range(0, Dimensions).Select(_ => (sbyte)random.Next(-100, 100)).ToArray(), 1f, tensor);
                registration.Register(1 + i, tensor);
            }

            registration.Commit(CancellationToken.None);
            write.Commit();
        }

        using var read = Env.ReadTransaction();
        using var int8 = new Hnsw.SearchState(read.LowLevelTransaction, "int8");
        Assert.Null(int8.CodeParameters);
        Assert.Null(int8.BeginScreen(new byte[Hnsw.TensorSizeBytes<sbyte>(Dimensions)], 16, hasFilterMatch: false));
    }

    // The list of a node holds its neighbors in the order of the stored list, whichever of them an earlier traversal had resolved.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ResolvedEdgesKeepTheOrderOfTheStoredList()
    {
        Build(300);
        using var tx = Env.ReadTransaction();
        using var fresh = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        using var partial = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var edges = fresh.GetNodeById(150).EdgesAtLevel(0).ToArray();
        Assert.True(edges.Length > 4);

        for (var i = 1; i < edges.Length; i += 2)
            partial.GetNodeIndexById(edges[i]);

        Assert.Equal(edges, Resolved(fresh, 150));
        Assert.Equal(edges, Resolved(partial, 150));
    }

    private static long[] Resolved(Hnsw.SearchState state, long nodeId)
    {
        var nodeIds = new NativeList<long>();
        var indexes = new NativeList<int>();
        state.ResolveEdgeIndexes(state.GetNodeIndexById(nodeId), 0, null, 0, ref nodeIds, ref indexes);
        var result = indexes.ToSpan().ToArray().Select(i => state.GetNodeByIndex(i).NodeId).ToArray();
        nodeIds.Dispose(state.Llt.Allocator);
        indexes.Dispose(state.Llt.Allocator);
        return result;
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void TheCodeParametersAreReadOncePerState()
    {
        Build(Count);
        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var parameters = state.CodeParameters;
        Assert.NotNull(parameters);
        foreach (var query in Gaussians(3, 4))
        {
            Search(state, query, 16);
            Assert.Same(parameters, state.CodeParameters);
        }
    }

    // The entries and the bits of the distances, in the order returned.
    private string Search(Hnsw.SearchState state, float[] query, int ef, GrowableBitArray? filter = null)
    {
        using var retriever = Hnsw.ApproximateNearest(state, ef, MemoryMarshal.AsBytes(query.AsSpan()).ToArray(), 0f, hasFilterMatch: filter != null);
        var entries = new List<long>();
        var bits = new List<int>();
        var matches = new long[256];
        var distances = new float[256];
        int read;
        while ((read = retriever.Fill(matches, distances, filter)) > 0)
        {
            entries.AddRange(matches.Take(read));
            bits.AddRange(distances.Take(read).Select(BitConverter.SingleToInt32Bits));
        }

        return string.Join(',', entries) + '|' + string.Join(',', bits);
    }

    private IReadOnlyList<byte[]> Build(int count, bool create = true) => Build(Gaussians(count, 1), create);

    private IReadOnlyList<byte[]> Build(float[][] vectors, bool create = true, string treeName = TreeName)
    {
        if (create)
        {
            using var tx = Env.WriteTransaction();
            Hnsw.Create(tx.LowLevelTransaction, treeName, vectors[0].Length * sizeof(float), 12, 16, VectorEmbeddingType.Single);
            tx.Commit();
        }

        var count = vectors.Length;
        var hashes = new List<byte[]>();
        var random = new Random(1);
        var codeRandom = new Random(2);
        for (var from = 0; from < count; from += 1000)
        {
            using var tx = Env.WriteTransaction();
            using var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random);
            registration.ForTestingPurposesOnly().CodeRandom = codeRandom;
            for (var i = from; i < Math.Min(count, from + 1000); i++)
                hashes.Add(registration.Register(1 + i, MemoryMarshal.AsBytes(vectors[i].AsSpan())).ToSpan().ToArray());
            registration.Commit(CancellationToken.None);
            tx.Commit();
        }

        return hashes;
    }

    private static float[][] Gaussians(int count, int seed, int dimensions = Dimensions)
    {
        var random = new Random(seed);
        return Enumerable.Range(0, count).Select(_ => Enumerable.Range(0, dimensions)
            .Select(_ => (float)(Math.Sqrt(-2 * Math.Log(1 - random.NextDouble())) * Math.Cos(2 * Math.PI * random.NextDouble()))).ToArray()).ToArray();
    }
}
