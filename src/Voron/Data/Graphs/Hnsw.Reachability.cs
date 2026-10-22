using System;
using System.Collections.Generic;
using System.Threading;
using Voron.Impl;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public readonly record struct GraphReport(long NodeCount, long[] Unreachable, int LongestList);

    /// <summary>
    /// Walks the base layer from the entry point and reports the node count, the ids of the nodes that the walk does not reach and the longest edge list at any level.
    /// A node whose posting list is empty is still a node. Reads no vector. A missing or empty graph reports zeros.
    /// Managed memory is one mark per node, the walk stack, the buffer of one edge list and the ids of the unreachable nodes, whatever the number of edges.
    /// Each node record is read exactly once: a reached node by the walk, an unreached node after it.
    /// </summary>
    public static GraphReport Inspect(LowLevelTransaction llt, string name, CancellationToken cancellationToken = default)
    {
        using var _ = Slice.From(llt.Allocator, name, out var slice);
        return Inspect(llt, slice, cancellationToken);
    }

    public static GraphReport Inspect(LowLevelTransaction llt, Slice name, CancellationToken cancellationToken = default)
    {
        using var state = new SearchState(llt, name);
        var count = state.IsEmpty ? 0 : state.Options.CountOfVectors;
        if (count == 0)
            return new GraphReport(0, [], 0);

        // Node ids are dense from EntryPointId.
        var reached = new bool[count + 1];
        var pending = new Stack<long>();
        var edges = new List<long>();
        var longest = 0;
        pending.Push(EntryPointId);
        reached[EntryPointId] = true;
        while (pending.TryPop(out var id))
        {
            cancellationToken.ThrowIfCancellationRequested();
            state.ReadNode(id, out var reader);
            for (var i = 0; i < reader.CountOfLevels; i++)
            {
                reader.ReadEdges(edges);
                longest = Math.Max(longest, edges.Count);
                if (i > 0)
                    continue;

                foreach (var to in edges)
                {
                    if ((ulong)(to - EntryPointId) >= (ulong)count || reached[to])
                        continue;
                    reached[to] = true;
                    pending.Push(to);
                }
            }
        }

        var unreachable = new List<long>();
        for (long id = EntryPointId; id <= count; id++)
        {
            if (reached[id])
                continue;

            cancellationToken.ThrowIfCancellationRequested();
            unreachable.Add(id);
            state.ReadNode(id, out var reader);
            for (var i = 0; i < reader.CountOfLevels; i++)
            {
                reader.ReadEdges(edges);
                longest = Math.Max(longest, edges.Count);
            }
        }

        return new GraphReport(count, unreachable.ToArray(), longest);
    }
}
