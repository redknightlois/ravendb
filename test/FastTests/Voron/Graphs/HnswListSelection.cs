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

public class HnswListSelection(ITestOutputHelper output) : StorageTest(output)
{
    // Each list that back-links put over its cap is selected once after the batch, rather than on every back-link past its cap.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ListsAreSelectedOnceAfterTheBatch()
    {
        var random = new Random(7);
        var vectors = Enumerable.Range(0, 1200).Select(_ => Enumerable.Range(0, 16).Select(_ => random.NextSingle() * 2 - 1).ToArray()).ToArray();
        using var _ = Slice.From(Allocator, "once", out var treeName);
        using (var tx = Env.WriteTransaction())
        {
            Hnsw.Create(tx.LowLevelTransaction, treeName, 16 * sizeof(float), 8, 32, VectorEmbeddingType.Single);
            tx.Commit();
        }

        var selections = 0;
        var lists = 0;
        for (int start = 0; start < vectors.Length; start += 300)
        {
            var selected = new HashSet<(int, int)>();
            using var tx = Env.WriteTransaction();
            using (var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random))
            {
                registration.ForTestingPurposesOnly().BeforeEdgesReplaced = (_, nodeIndex, level) =>
                {
                    selections++;
                    selected.Add((nodeIndex, level));
                };
                for (int i = start; i < start + 300; i++)
                    registration.Register(i + 1, MemoryMarshal.Cast<float, byte>(vectors[i]));
                registration.Commit(CancellationToken.None);
            }

            tx.Commit();
            lists += selected.Count;
        }

        // A list is selected again only past twice its cap, or when a back-link arrives during its selection.
        Assert.True(selections < 2 * lists, $"{lists} lists were selected {selections} times");
    }

    // Nodes placed at the same time add each other as edges before their candidates, so concurrent placements may start a list over its cap.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ListsStayWithinTheirCapWithConcurrentPlacements()
    {
        var random = new Random(7);
        var vectors = Enumerable.Range(0, 1200).Select(_ => Enumerable.Range(0, 16).Select(_ => random.NextSingle() * 2 - 1).ToArray()).ToArray();
        using var _ = Slice.From(Allocator, "concurrent", out var treeName);
        using (var tx = Env.WriteTransaction())
        {
            Hnsw.Create(tx.LowLevelTransaction, treeName, 16 * sizeof(float), 8, 32, VectorEmbeddingType.Single);
            tx.Commit();
        }

        using (var tx = Env.WriteTransaction())
        {
            using (var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random))
            {
                registration.MaxConcurrentBatches = 100;
                for (int i = 0; i < vectors.Length; i++)
                    registration.Register(i + 1, MemoryMarshal.Cast<float, byte>(vectors[i]));
                registration.Commit(CancellationToken.None);
            }

            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        using (var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName))
        {
            for (long nodeId = 1; nodeId <= state.Options.CountOfVectors; nodeId++)
            {
                var index = state.GetNodeIndexById(nodeId);
                for (int level = 0; level < state.GetDecodedNode(index).CountOfLevels; level++)
                {
                    var edges = state.GetDecodedNode(index).Edges(level).ToArray();
                    // A base-layer list also keeps the edges that are the last in-link of their target from an older node.
                    var lastOlderInLinks = level == 0 ? edges.Count(to => nodeId < to && state.GetNodeByIndex(state.GetNodeIndexById(to)).OlderInLinks == 1) : 0;
                    Assert.True(edges.Length <= state.Options.MaxEdgesAtLevel(level) + lastOlderInLinks, $"the list of {nodeId} at level {level} holds {edges.Length} edges");
                }
            }
        }
    }
}
