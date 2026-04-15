using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;

namespace FastTests.Voron.Graphs;

public class HnswNodeCache(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "test";
    private const int VectorDimensions = 16;
    private const int VectorSizeInBytes = VectorDimensions * sizeof(float);

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void EmptyGraphReturnsEmptyCache()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        using (var tx = Env.WriteTransaction())
        {
            Hnsw.Create(tx.LowLevelTransaction, treeName, VectorSizeInBytes, 3, 12, VectorEmbeddingType.Single);
            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        {
            var cache = Hnsw.NodeCache.Build(tx.LowLevelTransaction, treeName, maxNodes: 1024);
            Assert.NotNull(cache);
            Assert.Equal(0, cache.Count);
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ZeroBudgetReturnsEmptyCache()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        BuildGraph(treeName, vectorCount: 50, seed: 42);

        using (var tx = Env.ReadTransaction())
        {
            var cache = Hnsw.NodeCache.Build(tx.LowLevelTransaction, treeName, maxNodes: 0);
            Assert.NotNull(cache);
            Assert.Equal(0, cache.Count);
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void BudgetLargerThanGraphCachesAllNodes()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        const int vectorCount = 50;
        BuildGraph(treeName, vectorCount, seed: 42);

        using (var tx = Env.ReadTransaction())
        {
            var cache = Hnsw.NodeCache.Build(tx.LowLevelTransaction, treeName, maxNodes: 10_000);
            Assert.Equal(vectorCount, cache.Count);
        }
    }

    // When the budget is smaller than the graph, the cache must fill it exactly regardless of
    // how many batched loads are needed under the hood, even when the expansion crosses
    // multiple HNSW levels and spans multiple Lookup leaf pages.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void CacheFullyUsesBudgetWhenGraphIsLargerThanBudget()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        const int vectorCount = 2000;
        BuildGraph(treeName, vectorCount, seed: 1337);

        using (var tx = Env.ReadTransaction())
        {
            var cache = Hnsw.NodeCache.Build(tx.LowLevelTransaction, treeName, maxNodes: 1500);
            Assert.Equal(1500, cache.Count);
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void CachedEdgesMatchFreshlyLoadedEdges()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        BuildGraph(treeName, vectorCount: 500, seed: 7);

        using (var tx = Env.ReadTransaction())
        {
            var cache = Hnsw.NodeCache.Build(tx.LowLevelTransaction, treeName, maxNodes: 10_000);
            Assert.True(cache.Count > 0);

            // Load a fresh SearchState for ground-truth edges.
            var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName);

            foreach (var kvp in EnumerateCacheAsDictionary(cache))
            {
                long nodeId = kvp.Key;
                var cachedNode = kvp.Value;

                int stateIdx = state.GetNodeIndexById(nodeId);
                ref var stateNode = ref state.GetNodeByIndex(stateIdx);

                Assert.Equal(stateNode.NodeId, cachedNode.NodeId);
                Assert.Equal(stateNode.VectorId, cachedNode.VectorId);
                Assert.Equal(stateNode.PostingListId, cachedNode.PostingListId);
                Assert.Equal(stateNode.EdgesPerLevel.Count, cachedNode.LevelCount);

                for (int lvl = 0; lvl < cachedNode.LevelCount; lvl++)
                {
                    var cachedEdges = cache.EdgesAtLevel(cachedNode, lvl);
                    var stateEdges = stateNode.EdgesPerLevel[lvl];

                    Assert.Equal(stateEdges.Count, cachedEdges.Length);
                    for (int e = 0; e < cachedEdges.Length; e++)
                        Assert.Equal(stateEdges[e], cachedEdges[e]);
                }
            }
        }
    }

    // The cache is bound to the snapshot of the LLT it was built from. A cache built from an
    // older read tx must reflect that tx's view of the graph — nodes committed in a later
    // write tx are not visible to it.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void CacheBuiltFromOlderSnapshotDoesNotIncludeLaterNodes()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        BuildGraph(treeName, vectorCount: 200, seed: 11);

        long countAtSnapshot;
        using (var readTx = Env.ReadTransaction())
        {
            var state = new Hnsw.SearchState(readTx.LowLevelTransaction, treeName);
            countAtSnapshot = state.Options.CountOfVectors;

            // Add MORE vectors in a write tx AFTER we've opened our read snapshot.
            AddVectors(treeName, vectorCount: 100, firstId: (int)countAtSnapshot + 1, seed: 22);

            // Cache built from our OLD read tx must reflect ONLY the 200 nodes.
            var cache = Hnsw.NodeCache.Build(readTx.LowLevelTransaction, treeName, maxNodes: 10_000);

            // Every nodeId in the cache must be <= countAtSnapshot; none of the later
            // nodeIds (201..300) should be present.
            foreach (var kvp in EnumerateCacheAsDictionary(cache))
            {
                Assert.InRange(kvp.Key, 1L, countAtSnapshot);
            }
        }
    }

    // End-to-end search using the cache: SearchState reads node topology from the cache and
    // copies edges into its own transaction's allocator, then runs a normal query against
    // the graph.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void SearchStateCanQueryUsingACache()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        BuildGraph(treeName, vectorCount: 300, seed: 19);

        float[] query = RandomVector(new Random(99));
        var queryBytes = MemoryMarshal.Cast<float, byte>(query).ToArray();

        using (var tx = Env.ReadTransaction())
        {
            var cache = Hnsw.NodeCache.Build(tx.LowLevelTransaction, treeName, maxNodes: 10_000);
            Assert.True(cache.Count > 0);

            // Search via a SearchState that uses the cache. The copy path inside
            // SearchState.GetNodeIndexById / LoadNodeIndexes must not trigger any
            // allocator-generation assertion.
            using var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName, cache);
            using var retriever = Hnsw.ApproximateNearest(state,
                numberOfCandidates: 32,
                queryBytes,
                minimumSimilarity: 0f);

            var scores = new float[32];
            var docs = new long[32];
            int total = 0;
            int read;
            do
            {
                read = retriever.Fill(docs, scores, filter: null);
                total += read;
            } while (read != 0);

            Assert.True(total > 0, "search with cache returned no results");
        }
    }

    private void BuildGraph(Slice treeName, int vectorCount, int seed)
    {
        var random = new Random(seed);
        using var tx = Env.WriteTransaction();
        Hnsw.Create(tx.LowLevelTransaction, treeName, VectorSizeInBytes, numberOfEdges: 12,
            numberOfCandidates: 16, VectorEmbeddingType.Single);

        using (var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random))
        {
            for (int i = 1; i <= vectorCount; i++)
            {
                var v = RandomVector(random);
                registration.Register(i, MemoryMarshal.Cast<float, byte>(v));
            }
            registration.Commit(CancellationToken.None);
        }
        tx.Commit();
    }

    private void AddVectors(Slice treeName, int vectorCount, int firstId, int seed)
    {
        var random = new Random(seed);
        using var tx = Env.WriteTransaction();
        using (var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random))
        {
            for (int i = 0; i < vectorCount; i++)
            {
                var v = RandomVector(random);
                registration.Register(firstId + i, MemoryMarshal.Cast<float, byte>(v));
            }
            registration.Commit(CancellationToken.None);
        }
        tx.Commit();
    }

    private static float[] RandomVector(Random random)
    {
        var v = new float[VectorDimensions];
        float sumSq = 0;
        for (int i = 0; i < VectorDimensions; i++)
        {
            v[i] = (float)(random.NextDouble() * 2 - 1);
            sumSq += v[i] * v[i];
        }
        var norm = MathF.Sqrt(sumSq);
        if (norm > 0)
        {
            for (int i = 0; i < VectorDimensions; i++)
                v[i] /= norm;
        }
        return v;
    }

    /// <summary>
    /// Walk the cache's internal nodeId→CachedNode mapping. Provided here rather than as a
    /// public API on NodeCache because iteration order is not part of its contract.
    /// </summary>
    private static IEnumerable<KeyValuePair<long, Hnsw.NodeCache.CachedNode>> EnumerateCacheAsDictionary(Hnsw.NodeCache cache)
    {
        // We only have TryGetNodeIndex + GetNodeByIndex exposed. Caller knows node ids.
        // Since the test knows the graph is fully cached, we enumerate 1..Count+margin.
        for (long nodeId = 1; nodeId <= cache.Count * 4; nodeId++)
        {
            if (cache.TryGetNodeIndex(nodeId, out int idx))
                yield return new KeyValuePair<long, Hnsw.NodeCache.CachedNode>(nodeId, cache.GetNodeByIndex(idx));
        }
    }
}
