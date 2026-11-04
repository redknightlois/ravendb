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
    // A re-selection works on the list as it was when the work was dispatched. The hook plays the part of a concurrent placement
    // that appends a back-link in the meantime, so the test does not depend on how the tasks interleave.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void BackLinksAppendedAfterTheSnapshotAreKept()
    {
        var injected = new Dictionary<int, long>();
        var done = new HashSet<int>();
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
                    if (level != 0 || done.Count >= 50 || done.Contains(nodeIndex))
                        return;

                    var ownerEdges = state.GetNodeByIndex(nodeIndex).EdgesPerLevel[0].ToSpan();
                    var nodes = state.Nodes;
                    for (var i = 0; i < nodes.Length; i++)
                    {
                        ref var target = ref nodes[i];
                        if (i == nodeIndex || target.EdgesPerLevel.Count == 0 || ownerEdges.Contains(target.NodeId))
                            continue;

                        state.GetNodeByIndex(nodeIndex).EdgesPerLevel[0].Add(state.Llt.Allocator, target.NodeId);
                        injected.Add(nodeIndex, target.NodeId);
                        done.Add(nodeIndex);
                        return;
                    }
                };
                testing.AfterEdgesReplaced = (state, nodeIndex, level) =>
                {
                    if (level != 0 || injected.Remove(nodeIndex, out var id) == false)
                        return;
                    checkedLists++;
                    if (state.GetNodeByIndex(nodeIndex).EdgesPerLevel[0].ToSpan().Contains(id) == false)
                        lost++;
                };

                for (var i = 0; i < vectors.Length; i++)
                    registration.Register(i + 1, MemoryMarshal.Cast<float, byte>(vectors[i]));
                registration.Commit(CancellationToken.None);
            }
            tx.Commit();
        }

        Assert.True(checkedLists > 0, "no overfull list was re-selected");
        Assert.Equal(0, lost);
    }
}
