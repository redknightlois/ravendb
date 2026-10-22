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
