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
    // The heuristic selection keeps at most Edges + 1 ids.
    private const int Cap = Edges + 1;

    private record StoredNode(long Id, long[] Edges);

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

    private StoredNode[] ReadBaseLayer()
    {
        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, TreeName);
        return Enumerable.Range(1, (int)state.Options.CountOfVectors).Select(id =>
        {
            ref var node = ref state.GetNodeById(id);
            return new StoredNode(id, node.EdgesPerLevel[0].ToSpan().ToArray());
        }).ToArray();
    }

    private static float[][] RandomVectors(int count, int seed, int dimensions = 16)
    {
        var random = new Random(seed);
        return Enumerable.Range(0, count).Select(_ => Enumerable.Range(0, dimensions).Select(_ => random.NextSingle() * 2 - 1).ToArray()).ToArray();
    }
}
