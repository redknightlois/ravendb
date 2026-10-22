using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Voron.Global;
using Voron.Impl;
using Voron.Util;
using Xunit;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;

namespace FastTests.Voron.Graphs;

public class VectorSearchRetrieverTests(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "test";
    private const Hnsw.SearchState.NearestEdgesFlags Flags = Hnsw.SearchState.NearestEdgesFlags.StartingPointAsEdge | Hnsw.SearchState.NearestEdgesFlags.FilterNodesWithEmptyPostingLists;

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void EntriesOfOneNodeKeepItsDistanceAcrossFillCalls(bool filtered, bool largePostingList)
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        var random = new Random(1);
        // Node 1 holds many entries and each of the 54 other nodes holds one. Entry ids grow by random gaps so that a large list does not fit in a container page.
        var multiple = Enumerable.Range(0, largePostingList ? 6000 : 5).Select(i => (long)i * 400 + random.Next(1, 400)).ToArray();
        var single = Enumerable.Range(1, 54).Select(i => multiple[^1] + i).ToArray();
        Build(treeName, random, 8, [(multiple, RandomVector(random, 8)), .. single.Select(entry => (new[] { entry }, RandomVector(random, 8)))]);

        var all = multiple.Concat(single).ToArray();
        var filter = new GrowableBitArray(Allocator, all.Max() + 1);
        foreach (var entry in all)
            filter.Add(entry);
        filter.Count = all.Length;

        using var tx = Env.ReadTransaction();
        using (var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName))
        {
            var form = state.GetNodeByIndex(state.GetNodeIndexById(1)).PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask;
            Assert.Equal(largePostingList ? Constants.Graphs.VectorId.PostingList : Constants.Graphs.VectorId.SmallPostingList, form);
        }

        var query = MemoryMarshal.AsBytes(RandomVector(random, 8).AsSpan()).ToArray();
        var withLargeBuffer = Drain(tx, treeName, query, filtered ? filter : null, 1024);
        var withOneEntryBuffer = Drain(tx, treeName, query, filtered ? filter : null, 1);

        Assert.Equal(all.Length, withLargeBuffer.Count);
        Assert.DoesNotContain(withOneEntryBuffer, e => float.IsNaN(e.Distance));
        Assert.Equal(withLargeBuffer.Select(e => (e.Entry, BitConverter.SingleToInt32Bits(e.Distance))), withOneEntryBuffer.Select(e => (e.Entry, BitConverter.SingleToInt32Bits(e.Distance))));
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void FilteredFillDoesNotAllocateInProportionToThePostingList()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        var random = new Random(3);
        // Every entry shares one vector, which makes a large posting list, and the filter admits half of them.
        var entries = Enumerable.Range(0, 12_000).Select(i => (long)i * 400 + random.Next(1, 400)).ToArray();
        Build(treeName, random, 8, [(entries, RandomVector(random, 8))]);
        var filter = new GrowableBitArray(Allocator, entries[^1] + 1);
        foreach (var entry in entries.Where((_, i) => i % 2 == 0))
            filter.Add(entry);
        filter.Count = entries.Length / 2;

        using var tx = Env.ReadTransaction();
        using (var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName))
            Assert.Equal(Constants.Graphs.VectorId.PostingList, state.GetNodeByIndex(state.GetNodeIndexById(1)).PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask);

        var query = MemoryMarshal.AsBytes(RandomVector(random, 8).AsSpan()).ToArray();
        var matches = new long[64];
        var distances = new float[64];
        long FirstFillAllocations()
        {
            using var retriever = Hnsw.ExactNearest(tx.LowLevelTransaction, treeName, 16, query, 0f, hasFilterMatch: true);
            var before = GC.GetAllocatedBytesForCurrentThread();
            Assert.Equal(matches.Length, retriever.Fill(matches, distances, filter));
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        FirstFillAllocations();
        Assert.True(FirstFillAllocations() < entries.Length * sizeof(long) / 8);
    }

    // The traversal holds only what it reaches and the queue grows with the matches, so the candidates requested, however large, do not decide the memory.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void FilteredSearchAllocatesInProportionToItsMatchesNotToTheRequestedCandidates()
    {
        using var name = Slice.From(Allocator, TreeName, out var treeName);
        const int count = 2000, admitted = 20;
        var random = new Random(5);
        Build(treeName, random, 8, Enumerable.Range(1, count).Select(i => (new long[] { i }, RandomVector(random, 8))).ToArray());
        var filter = new GrowableBitArray(Allocator, count + 1);
        for (var entry = 1; entry <= count; entry += count / admitted)
            filter.Add(entry);
        filter.Count = admitted;
        var query = MemoryMarshal.AsBytes(RandomVector(random, 8).AsSpan()).ToArray();

        using var tx = Env.ReadTransaction();
        long[] Drain(Func<Hnsw.VectorSearchRetriever> search, out long allocated)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            using var retriever = search();
            var entries = new long[count];
            var read = retriever.Fill(entries, new float[count], filter);
            allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            return entries[..read];
        }

        Hnsw.VectorSearchRetriever Approximate(int candidates) => Hnsw.ApproximateNearest(tx.LowLevelTransaction, treeName, candidates, query, 0f, hasFilterMatch: true);

        var exact = Drain(() => Hnsw.ExactNearest(tx.LowLevelTransaction, treeName, count, query, 0f, hasFilterMatch: true), out _);
        Assert.Equal(admitted, exact.Length);
        Drain(() => Approximate(1_000_000), out _);
        var atMillion = Drain(() => Approximate(1_000_000), out var millionBytes);
        var atMax = Drain(() => Approximate(int.MaxValue), out var maxBytes);

        // The nearest-first entries are those of the exact search over the admitted entries.
        Assert.Equal(exact, atMillion);
        Assert.Equal(exact, atMax);
        // A queue or a working set sized by the candidates costs at least 8 bytes each, which is 8 MB at a million: the limit allows 1/64 of that.
        Assert.True(Math.Abs(maxBytes - millionBytes) < 1_000_000L * sizeof(long) / 64, $"{maxBytes} against {millionBytes} bytes");
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void FilteredFillKeepsTheNearestMatchesAcrossBatches()
    {
        // Nodes 2 and 3 are the nearest to the query and arrive after the farthest node 1. The held matches already reach k, so no third batch is requested.
        var (entries, moveNextBatchCalls) = RunScripted(angles: [1.2, 0.1, 0.2, 0.05], numberOfCandidates: 2, batches: [[1], [2, 3], [4]], filter: [1, 2, 3, 4]);
        Assert.Equal([2, 3], entries);
        Assert.Equal(2, moveNextBatchCalls);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void FilteredFillCountsNodesAtEqualDistanceSeparately()
    {
        // Nodes 1 and 2 are mirror images around the query, so they are at the same distance.
        var (entries, _) = RunScripted(angles: [0.3, -0.3, 1.2], numberOfCandidates: 2, batches: [[1], [2, 3]], filter: [1, 2, 3]);
        Assert.Equal([1, 2], entries.Order());
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void FilteredFillFinishesTheCurrentBatchOnceItHoldsMatches()
    {
        // The batch holds k matches after node 1, but node 2 in the same batch is nearer. The batch is in memory, so it is read to its end and nothing is requested after it.
        var (entries, moveNextBatchCalls) = RunScripted(angles: [1.2, 0.1, 0.05], numberOfCandidates: 1, batches: [[1, 2], [3]], filter: [1, 2, 3]);
        Assert.Equal([2], entries);
        Assert.Equal(1, moveNextBatchCalls);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void FilteredFillMatchesOnlyNodesWithAnEntryInTheFilter()
    {
        var (entries, _) = RunScripted(angles: [1.2, 0.1], numberOfCandidates: 1, batches: [[2, 1]], filter: [1]);
        Assert.Equal([1], entries);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void RestartedBatchesHoldOnlyNodesNotReturnedBefore()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        var random = new Random(7);
        Build(treeName, random, 8, Enumerable.Range(1, 400).Select(i => (new long[] { i }, RandomVector(random, 8))).ToArray());

        using var tx = Env.ReadTransaction();
        var query = MemoryMarshal.AsBytes(RandomVector(random, 8).AsSpan()).ToArray();
        Memory<byte> queryMemory = query;
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName, ref queryMemory);

        var batches = Batches(state, queryMemory, 10, restarts: 3);

        Assert.Equal(batches.Sum(b => b.Length), batches.SelectMany(b => b).Distinct().Count());
        // A restarted batch holds a full working set of new nodes: the working set a fresh search sizes from the previous batch.
        for (var i = 1; i < batches.Count; i++)
            Assert.Equal(Batches(state, queryMemory, batches[i - 1].Length, restarts: 0).Single().Length, batches[i].Length);
    }

    private List<int[]> Batches(Hnsw.SearchState state, Memory<byte> query, int numberOfCandidates, int restarts)
    {
        state.OnQueryVector(query);
        var candidates = new ContextBoundNativeList<int>(state.Llt.Allocator);
        candidates.EnsureCapacityFor(state.Options.MaxLevel + 1);
        state.SearchNearestAcrossLevels(query.Span, -1, state.Options.MaxLevel, ref candidates);
        var start = candidates[0];
        candidates.Clear();

        using var searcher = state.NearestSearch(start, query, 0, numberOfCandidates, candidates, Flags, hasFilterMatch: true);
        var batches = new List<int[]>();
        for (var restart = 0; restart <= restarts; restart++)
        {
            if (restart > 0)
                searcher.IncreaseNumberOfCandidates(0);
            searcher.MoveNextBatch();
            searcher.TryGetCurrentCandidates(out var batch);
            batches.Add(batch.ToSpan().ToArray());
        }

        return batches;
    }

    private (long[] Entries, int MoveNextBatchCalls) RunScripted(double[] angles, int numberOfCandidates, int[][] batches, long[] filter)
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        // Node i + 1 holds entry i + 1 and sits at angles[i] radians from the query (1, 0).
        Build(treeName, new Random(1), 2, angles.Select((angle, i) => (new long[] { i + 1 }, new[] { (float)Math.Cos(angle), (float)Math.Sin(angle) })).ToArray());

        using var tx = Env.ReadTransaction();
        Memory<byte> query = MemoryMarshal.AsBytes(new[] { 1f, 0f }.AsSpan()).ToArray();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName, ref query);
        var searcher = new ScriptedSearcher(state, query, batches, numberOfCandidates);
        var filterBits = new GrowableBitArray(Allocator, angles.Length + 1);
        foreach (var entry in filter)
            filterBits.Add(entry);
        filterBits.Count = filter.Length;

        using var retriever = new Hnsw.VectorSearchRetriever(state, searcher, query, 0f, ownsSearchState: false);
        var entries = new long[64];
        var read = retriever.Fill(entries, new float[64], filterBits);
        return (entries[..read], searcher.MoveNextBatchCalls);
    }

    private List<(long Entry, float Distance)> Drain(Transaction tx, Slice treeName, byte[] query, GrowableBitArray? filter, int bufferSize)
    {
        using var retriever = Hnsw.ExactNearest(tx.LowLevelTransaction, treeName, 1000, query, 0f, hasFilterMatch: filter != null);
        var entries = new long[bufferSize];
        var distances = new float[bufferSize];
        var drained = new List<(long, float)>();
        for (int read; (read = retriever.Fill(entries, distances, filter)) > 0;)
            drained.AddRange(entries[..read].Zip(distances[..read]));
        return drained;
    }

    private void Build(Slice treeName, Random random, int dimensions, (long[] Entries, float[] Vector)[] nodes)
    {
        using var tx = Env.WriteTransaction();
        Hnsw.Create(tx.LowLevelTransaction, treeName, dimensions * sizeof(float), 12, 16, VectorEmbeddingType.Single);
        using (var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random))
        {
            foreach (var (entries, vector) in nodes)
            {
                foreach (var entry in entries)
                    registration.Register(entry, MemoryMarshal.AsBytes(vector.AsSpan()));
            }

            registration.Commit(CancellationToken.None);
        }

        tx.Commit();
    }

    private static float[] RandomVector(Random random, int dimensions) => Enumerable.Range(0, dimensions).Select(_ => (float)(random.NextDouble() * 2 - 1)).ToArray();

    // Serves the scripted batches of node ids in order, as a graph traversal would serve its batches.
    private sealed class ScriptedSearcher(Hnsw.SearchState state, Memory<byte> query, int[][] batches, int numberOfCandidates) : IHnswSearcher
    {
        private ContextBoundNativeList<int> _current = new(state.Llt.Allocator);
        private int _next;

        public int MoveNextBatchCalls { get; private set; }

        public bool MoveNextBatch()
        {
            MoveNextBatchCalls++;
            _current.Clear();
            if (_next == batches.Length)
                return false;

            // Like a real searcher, a batch holds nodes whose distance to the query is already computed.
            long reads = 0;
            foreach (var nodeId in batches[_next++])
            {
                var nodeIdx = state.GetNodeIndexById(nodeId);
                state.QueryDistance(query.Span, nodeIdx, ref reads);
                _current.Add(nodeIdx);
            }

            return true;
        }

        public bool TryGetCurrentCandidates(out ContextBoundNativeList<int> candidates)
        {
            candidates = _current;
            return candidates.Count > 0;
        }

        public void IncreaseNumberOfCandidates(int currentlyAcceptedNodes)
        {
        }

        public long CandidatesProcessed => 0;

        public bool ShouldContinueSearch(long filterDocsCount) => true;

        public int NumberOfCandidates => numberOfCandidates;

        public void Dispose() => _current.Dispose();
    }
}
