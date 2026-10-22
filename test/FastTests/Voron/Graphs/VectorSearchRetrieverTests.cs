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
}
