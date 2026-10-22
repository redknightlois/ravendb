using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;

namespace FastTests.Voron.Graphs;

public class HnswGraphConstruction(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "test";
    private const int Edges = 12;

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

    // A re-selection works on the list as it was when the work was dispatched. On the first re-selection of each list, the hook plays the part
    // of concurrent placements that append up to a cap of back-links in the meantime. A list that no later placement appends to ends within its
    // cap only through the re-selection that the merge queues. The test does not depend on how the tasks interleave.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void BackLinksAppendedAfterTheSnapshotAreKept()
    {
        var injected = new Dictionary<int, List<long>>();
        var done = new HashSet<long>();
        var checkedLists = 0;
        var lost = 0;

        var random = new Random(3);
        var vectors = Enumerable.Range(0, 400).Select(_ => Enumerable.Range(0, 16).Select(_ => random.NextSingle() * 2 - 1).ToArray()).ToArray();
        using var _ = Slice.From(Allocator, "test", out var treeName);
        using (var tx = Env.WriteTransaction())
        {
            Hnsw.Create(tx.LowLevelTransaction, treeName, vectors[0].Length * sizeof(float), 12, 16, VectorEmbeddingType.Single);
            tx.Commit();
        }

        using (var tx = Env.WriteTransaction())
        {
            using (var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random))
            {
                // One task places every node, so each list is re-selected on the LLT thread in a fixed order.
                registration.MaxConcurrentBatches = vectors.Length;
                var testing = registration.ForTestingPurposesOnly();
                testing.BeforeEdgesReplaced = (state, nodeIndex, level) =>
                {
                    ref var owner = ref state.GetNodeByIndex(nodeIndex);
                    if (level != 0 || done.Add(owner.NodeId) == false)
                        return;

                    var appended = new List<long>();
                    var nodes = state.Nodes;
                    for (var i = 0; i < nodes.Length && appended.Count < state.Options.MaxEdgesAtLevel(0); i++)
                    {
                        ref var target = ref nodes[i];
                        if (i == nodeIndex || target.EdgesPerLevel.Count == 0 || owner.EdgesPerLevel[0].ToSpan().Contains(target.NodeId))
                            continue;

                        // Appends as a concurrent placement does: the index list follows the id list when it mirrors it.
                        owner.EdgesPerLevel[0].Add(state.Llt.Allocator, target.NodeId);
                        if (owner.EdgesIndexesPerLevel.Count > 0 && owner.EdgesIndexesPerLevel[0].Count == owner.EdgesPerLevel[0].Count - 1)
                            owner.EdgesIndexesPerLevel[0].Add(state.Llt.Allocator, i);
                        appended.Add(target.NodeId);
                    }

                    injected.Add(nodeIndex, appended);
                };
                testing.AfterEdgesReplaced = (state, nodeIndex, level) =>
                {
                    if (level != 0 || injected.Remove(nodeIndex, out var ids) == false)
                        return;
                    checkedLists++;
                    var edges = state.GetNodeByIndex(nodeIndex).EdgesPerLevel[0].ToSpan();
                    foreach (var id in ids)
                    {
                        if (edges.Contains(id) == false)
                            lost++;
                    }
                };

                for (var i = 0; i < vectors.Length; i++)
                    registration.Register(i + 1, MemoryMarshal.Cast<float, byte>(vectors[i]));
                registration.Commit(CancellationToken.None);
            }
            tx.Commit();
        }

        Assert.True(checkedLists > 0, "no overfull list was re-selected");
        Assert.Equal(0, lost);
        using (var tx = Env.ReadTransaction())
        using (var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName))
        {
            foreach (var id in done)
                Assert.True(state.GetNodeByIndex(state.GetNodeIndexById(id)).EdgeCountAtLevel(0) <= state.Options.MaxEdgesAtLevel(0), "a list is over its cap");
        }
    }

    private IReadOnlyList<byte[]> Build(float[][] vectors, int commits, int tasks, int seed, int beam, Action<Hnsw.Registration.TestingStuff> testing = null)
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        var random = new Random(seed);
        var hashes = new List<byte[]>();
        var perCommit = (vectors.Length + commits - 1) / commits;
        using (var tx = Env.WriteTransaction())
        {
            Hnsw.Create(tx.LowLevelTransaction, treeName, vectors[0].Length * sizeof(float), Edges, beam, VectorEmbeddingType.Single);
            tx.Commit();
        }

        for (var from = 0; from < vectors.Length; from += perCommit)
        {
            var batch = vectors.Skip(from).Take(perCommit).ToArray();
            using var tx = Env.WriteTransaction();
            using var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random);
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

    private static float[][] RandomVectors(int count, int seed, int dimensions = 16)
    {
        var random = new Random(seed);
        return Enumerable.Range(0, count).Select(_ => Enumerable.Range(0, dimensions).Select(_ => random.NextSingle() * 2 - 1).ToArray()).ToArray();
    }
}
