using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Corax.Querying;
using Voron.Util;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Containers;
using Voron.Data.Graphs;
using Voron.Data.Lookups;
using Voron.Impl;
using Xunit;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;

namespace FastTests.Voron.Graphs;

public class HnswGraphConstruction(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "test";
    private const int Edges = 12;
    // The heuristic selection keeps at most Edges + 1 ids.
    private const int Cap = Edges + 1;

    private record StoredNode(long Id, long[] Edges, int OlderInLinks);

    private const int HubInLinks = 40;

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void RefillFillsEveryListToTheCap()
    {
        // Each point of the arc occludes the points beyond it on its side, so the heuristic alone keeps about two edges.
        var arc = Enumerable.Range(0, 400).Select(i => new[] { (float)Math.Cos(i * Math.PI / 400), (float)Math.Sin(i * Math.PI / 400) }).ToArray();

        Build(arc, commits: 1, tasks: 1, seed: 1, beam: 16);

        foreach (var node in ReadBaseLayer().Where(n => n.Id > Cap))
            Assert.True(node.Edges.Length >= Cap, $"node {node.Id} holds {node.Edges.Length} edges");
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void InsertionBeamIsSeededFromTheLevelAbove()
    {
        var starts = new List<(int Level, int TopLevel, int Start)>();
        Build(RandomVectors(600, seed: 2), commits: 1, tasks: 1, seed: 2, beam: 16,
            testing: t => t.OnBeamStarted = (level, topLevel, start) => starts.Add((level, topLevel, start)));

        // The top level starts from the greedy descent node alone; each lower level starts from what the level above returned.
        Assert.All(starts.Where(s => s.Level == s.TopLevel), s => Assert.Equal(1, s.Start));
        var lower = starts.Where(s => s.Level < s.TopLevel).ToArray();
        Assert.NotEmpty(lower);
        Assert.All(lower, s => Assert.True(s.Start > 0));
        Assert.Contains(lower, s => s.Start > 1);
    }

    // A re-selection works on the list as it was when the work was dispatched. The hook plays the part of a concurrent placement
    // that appends a back-link in the meantime, so the test does not depend on how the tasks interleave.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void BackLinksAppendedAfterTheSnapshotAreKept()
    {
        var injected = new Dictionary<int, long>();
        var done = new HashSet<int>();
        var checkedLists = 0;
        var lost = 0;

        Build(RandomVectors(400, seed: 3), commits: 1, tasks: 1, seed: 3, beam: 16, testing: t =>
        {
            t.BeforeEdgesReplaced = (state, nodeIndex, level) =>
            {
                if (level != 0 || done.Count >= 50 || done.Contains(nodeIndex))
                    return;

                var owner = state.GetNodeByIndex(nodeIndex);
                var ownerEdges = owner.EdgesPerLevel[0].ToSpan();
                var nodes = state.Nodes;
                for (var i = 0; i < nodes.Length; i++)
                {
                    ref var target = ref nodes[i];
                    if (i == nodeIndex || target.EdgesPerLevel.Count == 0 || ownerEdges.Contains(target.NodeId))
                        continue;

                    state.GetNodeByIndex(nodeIndex).EdgesPerLevel[0].Add(state.Llt.Allocator, target.NodeId);
                    target.CountOlderInLink(owner.NodeId, 1);
                    injected.Add(nodeIndex, target.NodeId);
                    done.Add(nodeIndex);
                    return;
                }
            };
            t.AfterEdgesReplaced = (state, nodeIndex, level) =>
            {
                if (level != 0 || injected.Remove(nodeIndex, out var id) is false)
                    return;
                checkedLists++;
                if (state.GetNodeByIndex(nodeIndex).EdgesPerLevel[0].ToSpan().Contains(id) is false)
                    lost++;
            };
        });

        Assert.True(checkedLists > 0, "no overfull list was re-selected");
        Assert.Equal(0, lost);
    }

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineData(1, 1, 16)]
    [InlineData(1, 8, 16)]
    [InlineData(6, 8, 16)]
    [InlineData(6, 8, 64)]
    public void EveryNodeKeepsAnInLinkFromAnOlderNodeAndIsReachable(int commits, int tasks, int beam)
    {
        const int count = 2500;
        Build(RandomVectors(count, seed: 4), commits, tasks, seed: 4, beam);

        var nodes = ReadBaseLayer();
        Assert.Equal(count, nodes.Length);
        var withOlderInLink = nodes.SelectMany(n => n.Edges.Where(to => n.Id < to)).ToHashSet();
        Assert.Empty(nodes.Where(n => n.Id != 1 && withOlderInLink.Contains(n.Id) is false).Select(n => n.Id));
        Assert.Empty(Inspect().Unreachable);
    }

    // A placement links a node to about a cap of older nodes, so random data does not give a node more than 32 older in-links. In every commit
    // the hook plays the part of an interleaving in which HubInLinks older nodes also link to the last created node, through the production count.
    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineData(1, 1)]
    [InlineData(6, 8)]
    public void StoredCountsEqualTheStoredOlderInLinks(int commits, int tasks)
    {
        Build(RandomVectors(2000, seed: 5), commits, tasks, seed: 5, beam: 16, testing: t => t.BeforeOlderInLinksRepaired = state =>
        {
            ref var hub = ref state.GetNodeByIndex(state.GetCreatedNodeIndex(state.CreatedNodes - 1));
            var linked = 0;
            for (var i = 0; i < state.Nodes.Length && linked < HubInLinks; i++)
            {
                ref var older = ref state.Nodes[i];
                if (older.NodeId >= hub.NodeId || older.EdgesPerLevel.Count == 0 || older.EdgesPerLevel[0].ToSpan().Contains(hub.NodeId))
                    continue;
                older.EdgesPerLevel[0].Add(state.Llt.Allocator, hub.NodeId);
                hub.CountOlderInLink(older.NodeId, 1);
                linked++;
            }
        });

        var nodes = ReadBaseLayer();
        var olderInLinks = nodes.SelectMany(n => n.Edges.Where(to => n.Id < to)).GroupBy(to => to).ToDictionary(g => g.Key, g => g.Count());
        Assert.Contains(olderInLinks.Values, count => count > 32);
        foreach (var node in nodes)
            Assert.Equal(olderInLinks.GetValueOrDefault(node.Id), node.OlderInLinks);
    }

    // The test plays the part of an interleaving in which the node's candidates were all younger than the node.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void NodeThatEndsPlacementWithoutAnOlderInLinkGetsOne()
    {
        const long victim = 100;
        Build(RandomVectors(300, seed: 7), commits: 1, tasks: 1, seed: 7, beam: 16, testing: t => t.BeforeOlderInLinksRepaired = state =>
        {
            for (var i = 0; i < state.Nodes.Length; i++)
            {
                ref var node = ref state.Nodes[i];
                if (node.NodeId >= victim)
                    continue;
                var kept = node.EdgesPerLevel[0].ToSpan().ToArray().Where(id => id != victim).ToArray();
                node.EdgesPerLevel[0].ResetAndCopyFrom(state.Llt.Allocator, kept);
            }
            state.GetNodeById(victim).OlderInLinks = 0;
        });

        var nodes = ReadBaseLayer();
        Assert.Contains(nodes, n => n.Id < victim && n.Edges.Contains(victim));
        Assert.Equal(1, nodes.Single(n => n.Id == victim).OlderInLinks);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void GraphBelowTheCurrentVersionIsRefusedAndLeftUntouched() => AssertGraphIsRefused(global::Voron.Global.Constants.Graphs.HnswVersion.SinglesWithL2Norm);

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void GraphAboveTheCurrentVersionIsRefusedAndLeftUntouched() => AssertGraphIsRefused(global::Voron.Global.Constants.Graphs.HnswVersion.CurrentVersion + 1);

    private void AssertGraphIsRefused(int storedVersion)
    {
        const ushort currentVersion = global::Voron.Global.Constants.Graphs.HnswVersion.CurrentVersion;
        var oldVersion = (ushort)storedVersion;
        Build(RandomVectors(50, seed: 13), commits: 1, tasks: 1, seed: 13, beam: 16);
        using (var tx = Env.WriteTransaction())
        {
            using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
            state.Options.Version = oldVersion;
            state.FlushOptions();
            tx.Commit();
        }

        var before = StoredBytes();
        var query = MemoryMarshal.AsBytes(RandomVectors(1, seed: 14)[0].AsSpan()).ToArray();
        using var _ = Slice.From(Allocator, TreeName, out var treeName);

        AssertRefused(llt => Hnsw.RegistrationFor(llt, treeName), write: true);
        AssertRefused(llt => Hnsw.ApproximateNearest(llt, treeName, 8, query, 0f));
        AssertRefused(llt => Hnsw.ExactNearest(llt, treeName, 8, query, 0f, hasFilterMatch: false));

        // A Corax searcher reports the refusal on every lookup and disposes cleanly, so the refusal is what a query sees.
        using (var tx = Env.ReadTransaction())
        {
            var searcher = new IndexSearcher(tx, null);
            Assert.Throws<NotSupportedException>(() => searcher.GetOrCreateVectorSearchState(treeName));
            Assert.Throws<NotSupportedException>(() => searcher.GetOrCreateVectorSearchState(treeName));
            searcher.Dispose();
        }

        Assert.Equal(before, StoredBytes());

        void AssertRefused(Action<LowLevelTransaction> open, bool write = false)
        {
            using var tx = write ? Env.WriteTransaction() : Env.ReadTransaction();
            var e = Assert.Throws<NotSupportedException>(() => open(tx.LowLevelTransaction));
            Assert.Contains(oldVersion.ToString(), e.Message);
            Assert.Contains(currentVersion.ToString(), e.Message);
        }
    }

    // The walk keeps a mark per node and a stack, never a copy of the edges: two graphs of the same node count and different edge counts allocate alike.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void InspectDoesNotAllocateInProportionToTheEdges()
    {
        const int count = 2000;
        var vectors = RandomVectors(count, seed: 21);
        Build(vectors, commits: 1, tasks: 1, seed: 21, beam: 16, edges: 4, treeName: "sparse");
        Build(vectors, commits: 1, tasks: 1, seed: 21, beam: 16, edges: 32, treeName: "dense");

        var (sparseReport, sparseBytes, sparseEdges) = Measure("sparse");
        var (denseReport, denseBytes, denseEdges) = Measure("dense");

        // The stack grows by doubling and holds at most one id per node, so its allocations differ by less than 2 * 8 bytes per node; the limit is twice that.
        var limit = 4L * sizeof(long) * count;
        Assert.True(Math.Abs(denseBytes - sparseBytes) < limit, $"{denseBytes} against {sparseBytes} bytes");
        // Copying the edges would have cost the difference in edges at 8 bytes each, which is above the limit, so the graphs exercise the walk.
        Assert.True((denseEdges - sparseEdges) * sizeof(long) > limit);

        foreach (var report in new[] { sparseReport, denseReport })
        {
            Assert.Equal(count, report.NodeCount);
            Assert.Empty(report.Unreachable);
        }

        (Hnsw.GraphReport Report, long Bytes, long Edges) Measure(string name)
        {
            using var tx = Env.ReadTransaction();
            using var _ = Slice.From(Allocator, name, out var treeName);
            Hnsw.Inspect(tx.LowLevelTransaction, treeName);
            var before = GC.GetAllocatedBytesForCurrentThread();
            var report = Hnsw.Inspect(tx.LowLevelTransaction, treeName);
            var bytes = GC.GetAllocatedBytesForCurrentThread() - before;

            var nodes = Hnsw.IterateNodes(tx.LowLevelTransaction, name).ToArray();
            Assert.Equal(nodes.Max(n => n.EdgesByLevel.Max(level => level.Length)), report.LongestList);
            return (report, bytes, nodes.Sum(n => (long)n.EdgesByLevel[0].Length));
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void InspectStopsWhenTheTokenIsCancelled()
    {
        Build(RandomVectors(100, seed: 22), commits: 1, tasks: 1, seed: 22, beam: 16);
        using var tx = Env.ReadTransaction();
        using var _ = Slice.From(Allocator, TreeName, out var treeName);

        Assert.Throws<OperationCanceledException>(() => Hnsw.Inspect(tx.LowLevelTransaction, treeName, new CancellationToken(canceled: true)));
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void InspectReportsTheNodeThatNoEdgeLeadsTo()
    {
        const int count = 300;
        var hashes = Build(RandomVectors(count, seed: 15), commits: 1, tasks: 1, seed: 15, beam: 16);
        using (var tx = Env.WriteTransaction())
        {
            using var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, TreeName, new Random(15));
            for (var i = 0; i < count; i += 3)
                registration.Remove(i + 1, hashes[i]);
            registration.Commit(CancellationToken.None);
            tx.Commit();
        }

        int longest;
        using (var tx = Env.ReadTransaction())
            longest = Hnsw.IterateNodes(tx.LowLevelTransaction, TreeName).Max(n => n.EdgesByLevel.Max(level => level.Length));

        // Nodes whose posting list is empty are still nodes of the graph.
        var report = Inspect();
        Assert.Equal(count, report.NodeCount);
        Assert.Empty(report.Unreachable);
        Assert.Equal(longest, report.LongestList);

        // Every other node has an in-link from an older node, so the youngest node is the only one that this leaves unreachable.
        RemoveBaseLayerInLinks(count);
        report = Inspect();
        Assert.Equal(count, report.NodeCount);
        Assert.Equal([(long)count], report.Unreachable);
        Assert.Equal(longest, report.LongestList);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void RemovalOnlyCommitKeepsTheCounts()
    {
        var vectors = RandomVectors(800, seed: 10);
        var hashes = Build(vectors.Take(500).ToArray(), commits: 1, tasks: 1, seed: 10, beam: 16);
        var counts = ReadBaseLayer().ToDictionary(n => n.Id, n => n.OlderInLinks);

        using (var tx = Env.WriteTransaction())
        {
            using var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, TreeName, new Random(10));
            for (var i = 0; i < 500; i += 5)
                registration.Remove(i + 1, hashes[i]);
            registration.Commit(CancellationToken.None);
            tx.Commit();
        }

        Assert.Equal(counts, ReadBaseLayer().ToDictionary(n => n.Id, n => n.OlderInLinks));
        Assert.Contains(counts.Values, c => c > 0);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ListsPastTheCapEncodeAndDecodeOnBothReadPaths()
    {
        Build(RandomVectors(2000, seed: 6), commits: 1, tasks: 1, seed: 6, beam: 16);
        var stored = ReadBaseLayer();
        Assert.True(stored.Max(n => n.Edges.Length) > Cap);

        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var edges = new List<long>();
        foreach (var node in stored)
        {
            state.ReadNode(node.Id, out var reader);
            reader.ReadEdges(edges);
            Assert.Equal(node.Edges, edges);
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void CommitLeavesThePagesOfUnchangedRecordsUnmodified()
    {
        Build(RandomVectors(5000, seed: 11), commits: 1, tasks: 1, seed: 11, beam: 16);

        using var tx = Env.WriteTransaction();
        var before = Records(tx);
        using (var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, TreeName, new Random(12)))
        {
            registration.Register(5001, MemoryMarshal.Cast<float, byte>(RandomVectors(1, seed: 12)[0]));
            registration.Commit(CancellationToken.None);
        }

        var touched = new HashSet<long>();
        foreach (var (id, record) in Records(tx))
        {
            if (before.TryGetValue(id, out var old) && old.Location == record.Location && old.Bytes.SequenceEqual(record.Bytes))
                continue;
            touched.Add(record.Location / global::Voron.Global.Constants.Storage.PageSize);
            if (old.Bytes != null)
                touched.Add(old.Location / global::Voron.Global.Constants.Storage.PageSize);
        }

        var untouched = before.Values.Select(r => r.Location / global::Voron.Global.Constants.Storage.PageSize).Where(page => touched.Contains(page) == false).ToHashSet();
        Assert.NotEmpty(touched);
        Assert.NotEmpty(untouched);
        Assert.All(untouched, page => Assert.False(tx.LowLevelTransaction.IsDirty(page)));
    }

    // RavenDB-26809: under parallel placement a worker dereferences a pointer into the node array while the LLT thread may grow it.
    // Growth must retain the grown-from buffer instead of freeing it, so the worker never reads freed memory.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void GrowingNodeArrayRetainsGrownFromBuffer()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        using (var tx = Env.WriteTransaction())
        {
            Hnsw.Create(tx.LowLevelTransaction, treeName, 16 * sizeof(float), Edges, numberOfCandidates: 16, VectorEmbeddingType.Single);
            tx.Commit();
        }

        using var rtx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(rtx.LowLevelTransaction, treeName);
        Assert.True(state.ForceGrowNodesForTesting(), "node array did not move");
        Assert.True(state.ForceGrowNodesForTesting(), "node array did not move");
        Assert.Equal(2, state.RetiredNodeStorageCountForTesting);
    }

    private IReadOnlyList<byte[]> Build(float[][] vectors, int commits, int tasks, int seed, int beam, Action<Hnsw.Registration.TestingStuff> testing = null, int edges = Edges, string treeName = TreeName)
    {
        using var _ = Slice.From(Allocator, treeName, out var treeSlice);
        var random = new Random(seed);
        var hashes = new List<byte[]>();
        var perCommit = (vectors.Length + commits - 1) / commits;
        using (var tx = Env.WriteTransaction())
        {
            Hnsw.Create(tx.LowLevelTransaction, treeSlice, vectors[0].Length * sizeof(float), edges, beam, VectorEmbeddingType.Single);
            tx.Commit();
        }

        for (var from = 0; from < vectors.Length; from += perCommit)
        {
            var batch = vectors.Skip(from).Take(perCommit).ToArray();
            using var tx = Env.WriteTransaction();
            using var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeSlice, random);
            // tasks = min(max(1, new nodes / MaxConcurrentBatches), MaxConcurrentBatches)
            registration.MaxConcurrentBatches = Math.Max(1, batch.Length / tasks);
            testing?.Invoke(registration.ForTestingPurposesOnly());
            for (var i = 0; i < batch.Length; i++)
                hashes.Add(registration.Register(1 + from + i, MemoryMarshal.Cast<float, byte>(batch[i])).ToSpan().ToArray());
            registration.Commit(CancellationToken.None);
            tx.Commit();
        }

        return hashes;
    }

    private List<byte[]> StoredBytes()
    {
        using var tx = Env.ReadTransaction();
        var stored = new List<byte[]>();
        unsafe
        {
            stored.Add(new ReadOnlySpan<byte>(tx.ReadTree(TreeName).DirectRead(Hnsw.OptionsSlice), sizeof(Hnsw.Options)).ToArray());
        }

        stored.AddRange(Records(tx).Values.Select(r => r.Bytes));
        return stored;
    }

    /// <summary>Reads each stored node record and its location without opening a SearchState, so it works on a graph of any version.</summary>
    private static Dictionary<long, (long Location, byte[] Bytes)> Records(Transaction tx)
    {
        var llt = tx.LowLevelTransaction;
        var locations = Hnsw.RecordLocations(tx.ReadTree(TreeName).LookupFor<Int64LookupKey>(Hnsw.LocationPagesSlice), null);
        var records = new Dictionary<long, (long, byte[])>();
        for (var id = 1L; locations.ReadInt64(llt, id) is var location and not 0; id++)
            records[id] = (location, Container.GetReadOnly(llt, new ContainerEntryId(location)).ToArray());
        return records;
    }

    private void RemoveBaseLayerInLinks(long target)
    {
        using var tx = Env.WriteTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        var buffer = new ContextBoundNativeList<byte>(tx.LowLevelTransaction.Allocator);
        for (var id = 1L; id <= state.Options.CountOfVectors; id++)
        {
            ref var node = ref state.GetNodeById(id);
            var kept = node.EdgesPerLevel[0].ToSpan().ToArray().Where(to => to != target).ToArray();
            if (kept.Length == node.EdgesPerLevel[0].Count)
                continue;

            node.EdgesPerLevel[0].ResetAndCopyFrom(tx.LowLevelTransaction.Allocator, kept);
            var encoded = node.Encode(ref buffer);
            Assert.True(state.TryGetLocationForNode(id, out var location));
            Container.Delete(tx.LowLevelTransaction, state.Options.Container, new ContainerEntryId(location));
            var newLocation = Container.Allocate(tx.LowLevelTransaction, state.Options.Container, encoded.Length, out var storage);
            encoded.CopyTo(storage);
            state.RegisterNodeLocation(id, (long)newLocation);
        }

        buffer.Dispose();
        tx.Commit();
    }

    private StoredNode[] ReadBaseLayer()
    {
        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        return Enumerable.Range(1, (int)state.Options.CountOfVectors).Select(id =>
        {
            ref var node = ref state.GetNodeById(id);
            return new StoredNode(id, node.EdgesPerLevel[0].ToSpan().ToArray(), node.OlderInLinks);
        }).ToArray();
    }

    private Hnsw.GraphReport Inspect()
    {
        using var tx = Env.ReadTransaction();
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        return Hnsw.Inspect(tx.LowLevelTransaction, treeName);
    }

    private long[] Search(float[] query)
    {
        var bytes = new byte[Hnsw.TensorSizeBytes<float>(query.Length)];
        Hnsw.WriteNormalizedTensor(query, bytes);
        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        using var retriever = Hnsw.ApproximateNearest(state, numberOfCandidates: 32, bytes, minimumSimilarity: 0f);
        var scores = new float[16];
        var docs = new long[16];
        return docs[..retriever.Fill(docs, scores, filter: null)];
    }

    private static float[][] RandomVectors(int count, int seed, int dimensions = 16)
    {
        var random = new Random(seed);
        return Enumerable.Range(0, count).Select(_ => Enumerable.Range(0, dimensions).Select(_ => random.NextSingle() * 2 - 1).ToArray()).ToArray();
    }
}
