using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Impl;
using Voron.Util;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    /// <summary>
    /// Immutable, self-contained cache of HNSW graph node topology at a specific transaction.
    /// Built synchronously in the commit hook using the committing tx's LLT; all data is copied
    /// into managed arrays so the cache does not retain the transaction. Vector bytes are NOT
    /// copied — the cache stores the container id and queries read the bytes on demand via
    /// their own read transaction, which is MVCC-safe because caches are rebuilt on every commit
    /// and queries are routed to the cache whose <see cref="AsOfTxId"/> matches their snapshot.
    ///
    /// Per-query mutable search state (visited flags, distance cache, priority queues) lives in
    /// <see cref="SearchState"/>, not here. The GC reclaims caches once no transaction still
    /// references them via <c>ImmutableExternalState</c>.
    /// </summary>
    public sealed class NodeCache
    {
        public readonly long AsOfTxId;
        public readonly Options Options;
        public readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> SimilarityCalc;

        private readonly Dictionary<long, int> _nodeIdToIdx;
        private readonly CachedNode[] _nodes;

        // All nodes' edges concatenated into one array so we pay zero per-level managed-array
        // header overhead. Bounds for node k's level L are
        // [_levelOffsets[k.FirstLevelOffsetIndex + L], _levelOffsets[k.FirstLevelOffsetIndex + L + 1]).
        //
        // LOH note: for larger cache budgets _allEdges exceeds the 85 KB LOH threshold. This is
        // acceptable here because caches are long-lived (rebuilt per commit, not per query) and
        // the pattern LOH was designed for. If commit churn becomes a fragmentation issue,
        // consider chunked storage rather than ArrayPool — the cache's lifetime is bounded by
        // transactions that hold it via ImmutableExternalState, so we can't return it eagerly.
        private readonly long[] _allEdges;
        private readonly int[] _levelOffsets;

        public int Count => _nodeIdToIdx.Count;

        /// <summary>
        /// Managed per-node record. Edges are NOT stored inline; use <see cref="EdgesAtLevel"/>
        /// on the parent <see cref="NodeCache"/> to get a span into the flat backing store.
        /// </summary>
        public readonly record struct CachedNode(long NodeId, long PostingListId, long VectorId, int FirstLevelOffsetIndex, byte LevelCount);

        /// <summary>
        /// Conservative upper bound on managed memory per cached node, to translate a byte
        /// budget into a node count. Worst-case node has <c>2*numberOfEdges</c> edges at level 0.
        /// </summary>
        public static int EstimateBytesPerNode(int numberOfEdges)
        {
            // sizeof(CachedNode) + per-node level offset entries (HNSW avg ~3 levels => 3
            // ints). Edges at worst case (2*M at level 0) dominate at realistic edge counts.
            const int AvgLevelOffsetEntriesPerNode = 3;
            return sizeof(CachedNode)
                 + AvgLevelOffsetEntriesPerNode * sizeof(int)
                 + 2 * numberOfEdges * sizeof(long);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<long> EdgesAtLevel(in CachedNode node, int level)
        {
            int start = _levelOffsets[node.FirstLevelOffsetIndex + level];
            int end = _levelOffsets[node.FirstLevelOffsetIndex + level + 1];
            return _allEdges.AsSpan(start, end - start);
        }

        private NodeCache(long asOfTxId, Options options,
            delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> similarityCalc,
            Dictionary<long, int> nodeIdToIdx, CachedNode[] nodes,
            long[] allEdges, int[] levelOffsets)
        {
            AsOfTxId = asOfTxId;
            Options = options;
            SimilarityCalc = similarityCalc;
            _nodeIdToIdx = nodeIdToIdx;
            _nodes = nodes;
            _allEdges = allEdges;
            _levelOffsets = levelOffsets;
        }

        /// <summary>
        /// Build a cache by expanding from the entry point level by level, top-down, until
        /// <paramref name="maxNodes"/> is reached. Must be called with the committing tx's
        /// LLT (or any read tx whose snapshot is the desired <see cref="AsOfTxId"/>); the LLT
        /// can be disposed immediately after this returns.
        /// </summary>
        public static NodeCache Build(LowLevelTransaction llt, Slice fieldName, int maxNodes)
        {
            var tree = llt.Transaction.ReadTree(fieldName);
            if (tree is null || tree.TryGetLookupFor(NodeIdToLocationSlice, out Lookup<Int64LookupKey> locations) == false)
                return null;

            var options = Unsafe.Read<Options>(tree.DirectRead(OptionsSlice));
            var simCalc = GetDistanceKernel(options);

            int budget = (int)Math.Min(options.CountOfVectors, maxNodes);
            if (budget <= 0 || locations.TryGetValue(EntryPointId, out _) == false)
                return new NodeCache(llt.Id, options, simCalc, new(), [], [], [0]);

            using var builder = new Builder(llt, locations, budget);
            builder.Seed(EntryPointId);
            int maxLevel = builder.NodeLevelsAt(0) - 1;
            for (int level = maxLevel; level >= 0 && builder.HasBudget; level--)
                builder.ExpandAtLevel(level);
            var (nodes, edges, offsets) = builder.Freeze();
            return new NodeCache(llt.Id, options, simCalc, builder.NodeIdToIdx, nodes, edges, offsets);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetNodeIndex(long nodeId, out int index) => _nodeIdToIdx.TryGetValue(nodeId, out index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref readonly CachedNode GetNodeByIndex(int index) => ref _nodes[index];

        // Build-time scratch that owns the NativeList buffers (disposed via RAII).
        private sealed class Builder : IDisposable
        {
            public readonly Dictionary<long, int> NodeIdToIdx = new();
            private readonly List<CachedNode> _nodeList = new();
            // Per-node cumulative level offsets. Appended one segment per emitted node:
            // for node k with Lk levels, we append Lk+1 entries whose values are offsets into
            // _edgesAccum. The final NodeCache freezes these into arrays.
            private readonly List<int> _levelOffsets = new() { 0 };
            private readonly List<long> _edgesAccum = new();
            private readonly LowLevelTransaction _llt;
            private readonly Lookup<Int64LookupKey> _locations;
            private readonly int _budget;
            private NativeList<Node> _working;
            private NativeList<long> _batch;

            public bool HasBudget => NodeIdToIdx.Count < _budget;
            public int NodeLevelsAt(int index) => _working[index].EdgesPerLevel.Count;

            public (CachedNode[] Nodes, long[] AllEdges, int[] LevelOffsets) Freeze()
                => (_nodeList.ToArray(), _edgesAccum.ToArray(), _levelOffsets.ToArray());

            public Builder(LowLevelTransaction llt, Lookup<Int64LookupKey> locations, int budget)
            {
                _llt = llt;
                _locations = locations;
                _budget = budget;
            }

            public void Seed(long nodeId)
            {
                _batch.Add(_llt.Allocator, nodeId);
                Flush();
            }

            /// <summary>
            /// Find neighbors at <paramref name="level"/> of every already-loaded node that
            /// participates at that level, and bulk-load them (subject to the remaining budget).
            /// </summary>
            public void ExpandAtLevel(int level)
            {
                var seen = new HashSet<long>();
                for (int i = 0; i < _working.Count && NodeIdToIdx.Count + _batch.Count < _budget; i++)
                {
                    ref var node = ref _working[i];
                    if (node.EdgesPerLevel.Count <= level)
                        continue;

                    ref var edges = ref node.EdgesPerLevel[level];
                    for (int e = 0; e < edges.Count; e++)
                    {
                        var edgeId = edges[e];
                        if (NodeIdToIdx.ContainsKey(edgeId) || seen.Add(edgeId) == false)
                            continue;
                        _batch.Add(_llt.Allocator, edgeId);
                        if (NodeIdToIdx.Count + _batch.Count >= _budget)
                            break;
                    }
                }

                if (_batch.Count > 0)
                    Flush();
            }

            // NOTE: Lookup<T>.GetFor requires sorted input — it walks pages left-to-right,
            // advancing LastSearchPosition, so unsorted keys produce spurious not-found results.
            private void Flush()
            {
                var keys = _batch.ToSpan();
                int priorCount = _working.Count;

                var targetSlots = new int[keys.Length];
                _working.EnsureCapacityFor(_llt.Allocator, keys.Length);
                for (int i = 0; i < keys.Length; i++)
                {
                    targetSlots[i] = _working.Count;
                    _working.Add(_llt.Allocator, new Node { NodeId = keys[i] });
                }

                keys.Sort(targetSlots.AsSpan());
                _locations.GetFor(keys, keys, -1);

                var spans = new UnmanagedSpan[keys.Length];
                Container.GetAll(_llt, keys, spans.AsSpan(), -1, _llt.PageLocator);
                for (int i = 0; i < keys.Length; i++)
                {
                    if (spans[i].Length == 0)
                        continue;
                    Node.Decode(_llt, spans[i].ToSpan()).LoadInto(ref _working[targetSlots[i]]);
                }

                for (int i = priorCount; i < _working.Count; i++)
                    EmitIfLoaded(ref _working[i]);

                _batch.Clear();
            }

            private void EmitIfLoaded(ref Node node)
            {
                int levels = node.EdgesPerLevel.Count;
                if (levels == 0)
                    return; // node not decoded (tombstone / not yet fully loadable) — skip
                if (levels > byte.MaxValue)
                    throw new InvalidDataException(
                        $"HNSW node {node.NodeId} reports {levels} levels; graph appears corrupt (max expected ~log2(N))");

                // Level offsets already has one entry (0 or the previous node's end); append
                // one more entry per level whose value is the running edge position.
                int firstOffsetIndex = _levelOffsets.Count - 1;
                for (int lvl = 0; lvl < levels; lvl++)
                {
                    ref var src = ref node.EdgesPerLevel[lvl];
                    for (int e = 0; e < src.Count; e++)
                        _edgesAccum.Add(src[e]);
                    _levelOffsets.Add(_edgesAccum.Count);
                }

                NodeIdToIdx[node.NodeId] = _nodeList.Count;
                _nodeList.Add(new CachedNode(node.NodeId, node.PostingListId, node.VectorId, firstOffsetIndex, (byte)levels));
            }

            public void Dispose()
            {
                _batch.Dispose(_llt.Allocator);
                _working.Dispose(_llt.Allocator);
            }
        }
    }
}
