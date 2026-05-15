using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server.Utils;
using Voron.Data.Containers;
using Voron.Util;
using Array = System.Array;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    /// <summary>
    /// When true, the parallel-construction edge-selection step uses the original HNSW
    /// Algorithm-4 / DiskANN robust-prune instead of the Apollonius query-space descent
    /// cover. Diagnostic tests flip it directly. For external benchmarks the toggle is
    /// also driven by RAVEN_HNSW_LEGACY_HEURISTIC (set to "1" or "true" to enable legacy)
    /// so the same Raven.Server binary can be benched both ways without recompile.
    /// Process-wide.
    /// </summary>
    internal static bool UseLegacyHeuristic = ReadLegacyHeuristicEnv();

    // Diagnostic counters for DoWorkApolloniusCover wall breakdown. Accumulated as
    // Stopwatch ticks, summed across all parallel cover calls in a process. Tests
    // read these to compute fractions; nothing in the hot path touches them when
    // the toggle is off so the steady-state overhead is two interlocked adds per
    // sub-step. Toggle on by setting RAVEN_HNSW_COVER_PROFILE=1.
    internal static bool CoverProfileEnabled =
        Environment.GetEnvironmentVariable("RAVEN_HNSW_COVER_PROFILE") is { } v &&
        (v == "1" || string.Equals(v, "true", StringComparison.OrdinalIgnoreCase));
    internal static long CoverWitnessTicks;
    internal static long CoverDistToSrcTicks;
    internal static long CoverKCaptureTicks;
    internal static long CoverGreedyTicks;
    internal static long CoverMFillTicks;
    internal static long CoverCalls;
    internal static long CoverTotalTicks;
    internal static void CoverProfileReset()
    {
        CoverWitnessTicks = 0;
        CoverDistToSrcTicks = 0;
        CoverKCaptureTicks = 0;
        CoverGreedyTicks = 0;
        CoverMFillTicks = 0;
        CoverCalls = 0;
        CoverTotalTicks = 0;
    }

    private static bool ReadLegacyHeuristicEnv()
    {
        var v = Environment.GetEnvironmentVariable("RAVEN_HNSW_LEGACY_HEURISTIC");
        return string.Equals(v, "1", StringComparison.Ordinal)
            || string.Equals(v, "true", StringComparison.OrdinalIgnoreCase);
    }

    // JL witness sketch (§17). Off by default; enable with RAVEN_HNSW_JL_SKETCH=1.
    // Projects v, q ∈ R^d into R^m with m=JlSketchDim, then bounds the true witness
    // dot product within ε·|v|·|q| of the sketched one. Clear-pass / clear-fail
    // decisions skip the d-dim exact dot; only ε-band candidates fall back to it.
    internal static bool JlSketchEnabled =
        Environment.GetEnvironmentVariable("RAVEN_HNSW_JL_SKETCH") is { } _jl &&
        (_jl == "1" || string.Equals(_jl, "true", StringComparison.OrdinalIgnoreCase));
    internal const int JlSketchDim = 32;
    internal const float JlEpsilon = 0.10f;

    // Angular-spread strictness χ for §10.C-B bi-criteria. Default 0.7 (memory-noted
    // Pareto). Override with RAVEN_APOLLO_CHI to sweep against legacy α-prune
    // (effective χ≈1.0, one-sided d(u,cur)).
    internal static readonly float _envChi = ReadEnvChi();
    private static float ReadEnvChi()
    {
        var s = Environment.GetEnvironmentVariable("RAVEN_APOLLO_CHI");
        if (string.IsNullOrEmpty(s) == false && float.TryParse(s, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var f))
            return f;
        // Default χ=1.0 (matches legacy α-prune strictness). The χ sweep on Sphere-100K
        // showed recall plateaus at χ≈1.0 and degrades below; 0.7 was historical and
        // empirically too loose on real clustered data.
        return 1.0f;
    }

    // FRAMEWORK §3, §15. λ_code is the descent-decay hyperparameter applied on the
    // cosine-dissimilarity test δ(v,q) ≤ λ · δ(u,q). The corresponding chordal-metric
    // contraction ratio is ρ_metric = √λ_code (0.90 → ≈ 0.9487).
    //   - Higher λ (→1): looser witness criterion, more candidates qualify.
    //   - Lower λ (→0): stricter criterion, faster geometric descent per hop.
    // Only consulted by the cover-gain selector (`RAVEN_APOLLO_GREEDY_MODE=cover`).
    // Under the default dist-greedy mode the witness bits are discarded and λ has
    // no effect on the built graph; the value is wired through env so cover-gain
    // experiments do not require a recompile.
    internal static readonly float _envApolloLambda = ReadEnvApolloLambda();
    private static float ReadEnvApolloLambda()
    {
        var s = Environment.GetEnvironmentVariable("RAVEN_APOLLO_LAMBDA");
        if (string.IsNullOrEmpty(s) == false && float.TryParse(s, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var f) && f > 0f && f < 1f)
            return f;
        return 0.90f;
    }

    // Build-time I/O scheduling: order the pending-vector batch by VectorId
    // (= page order in the Voron container) so workers stream pages
    // sequentially and OS readahead stays hot. Default ON — pure scheduling
    // discipline, no effect on edge selection. Set RAVEN_APOLLO_SORT_BY_PAGE=0
    // to disable for A/B comparison.
    internal static readonly bool _envSortByPage =
        string.Equals(Environment.GetEnvironmentVariable("RAVEN_APOLLO_SORT_BY_PAGE") ?? "1", "1", StringComparison.OrdinalIgnoreCase);
    // Triangle-skip factor (1+√χ)². Recomputed once based on _envChi so the skip is
    // still exact for whatever χ was set at process start.
    internal static readonly float _envSpreadSkip = (1f + MathF.Sqrt(MathF.Max(0f, _envChi))) * (1f + MathF.Sqrt(MathF.Max(0f, _envChi)));

    // Greedy ordering for the post-kCapture fill in DoWorkApolloniusCover.
    //   "cover" (default) — pick by popcount(witness & needsMore); ties → cheapest.
    //   "dist"            — pick remaining candidates by ascending d(u, v), spread-gated.
    // The χ sweep on Sphere-100K showed cover-gain plateaus ~12pp under legacy even
    // at χ=1.0; ascending-distance ordering closes that residual.
    // Greedy mode for the post-kCapture fill. Default "dist" (ascending d(u,v) +
    // spread). The χ × greedy-mode sweep on Sphere-100K showed cover-gain greedy
    // underperforms dist-greedy by 3–4pp at every χ on real clustered embeddings.
    // Set RAVEN_APOLLO_GREEDY_MODE=cover to restore the original popcount-greedy.
    internal static readonly bool _envGreedyByDist =
        string.Equals(Environment.GetEnvironmentVariable("RAVEN_APOLLO_GREEDY_MODE") ?? "dist", "dist", StringComparison.OrdinalIgnoreCase);

    // Default L0 kCapture OFF — the unconditional M/2 nearest-by-distance fill
    // bypassed the spread filter and created redundant near-edges that crowded
    // the neighborhood. Closing that hole was what brought recall to legacy
    // parity on Sphere-100K (49 queries, 73.5% r@10 vs 72.7% at ef=256).
    // Re-enable with RAVEN_APOLLO_KCAPTURE_OFF=0.
    internal static readonly bool _envKCaptureOff =
        (Environment.GetEnvironmentVariable("RAVEN_APOLLO_KCAPTURE_OFF") ?? "1") is var _kc &&
        (_kc == "1" || string.Equals(_kc, "true", StringComparison.OrdinalIgnoreCase));

    // Spread-test asymmetry. Default "onesided" — reject candidate cur if
    // Δ(cur, alt) < χ · Δ(u, cur), using only the candidate's distance to u
    // (matches legacy α-prune exactly). The earlier "symmetric" form used
    // min(Δ(u,cur), Δ(u,alt)) and was 2–5pp behind legacy at NoC=128 because
    // in dist-greedy order Δ(u, alt) ≤ Δ(u, cur), so the min relaxed the test.
    internal static readonly bool _envSpreadSymmetric =
        string.Equals(Environment.GetEnvironmentVariable("RAVEN_APOLLO_SPREAD"), "symmetric", StringComparison.OrdinalIgnoreCase);
    private static float[] _jlMatrix; // row-major: row j of length d at offset j*d
    private static int _jlMatrixDim;
    private static readonly object _jlMatrixLock = new();

    private static float[] GetJlMatrix(int d)
    {
        var local = _jlMatrix;
        if (local != null && _jlMatrixDim == d)
            return local;
        lock (_jlMatrixLock)
        {
            if (_jlMatrix != null && _jlMatrixDim == d)
                return _jlMatrix;
            var rand = new Random(0x1F1F1F1F);
            var m = new float[JlSketchDim * d];
            float invSqrtM = 1f / MathF.Sqrt(JlSketchDim);
            for (int i = 0; i < m.Length; i++)
            {
                double u1 = 1.0 - rand.NextDouble();
                double u2 = rand.NextDouble();
                double z = Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);
                m[i] = (float)(z * invSqrtM);
            }
            _jlMatrixDim = d;
            Volatile.Write(ref _jlMatrix, m);
            return m;
        }
    }

    /*
     * The problem with HNSW is that it is a graph algorithm, which requires
     * that we'll touch significantly more nodes than we would usually do in a B+Tree, 
     * for example. 
     * 
     * If we need to index 1M items, using a B+Tree, I can sort them and be sure that I 
     * can get pretty good disk access patterns. For HNSW - the problem is that we need to
     * do effectively random I/O for each lookup. Sequential HNSW is running this one node 
     * at a time, which link each node to its nearest neighbors. It has horrible performance
     * once you exceed the size of memory on the machine.
     * 
     * Adding 1M nodes to a HNSW graph with 15M nodes is _expensive_. Assume that they use 768 dimensions
     * and no quantization. That 15M * 768 * 4 = 42GB of data just for the vectors. And adding a new node
     * means that we need to compare (and thus read, randomly) about 600 vectors (with peaks of 2,000 vectors). 
     * 
     * Typically, the solution for that is to get a bigger machine, but that is something that we can
     * try to address. This is the purpose of the code in this file. We go through many gymnastics to
     * try to optimize the disk access pattern and parallelize what we can.
     * 
     * Parallelization is complicated by the fact that we are running under a write transaction scope.
     * A write transaction in Voron is a _single threaded operation_. Another problem is that HNSW is
     * inherently a single-threaded algorithm. If I add two nodes to the graph, the second node will 
     * consider the first node as a candidate for its neighbors.
     * 
     * Moving to parallel mode make things more complex. If I add two nodes to the graph at the same time,
     * they will _not_ consider each other for neighbors. Given that HNSW is *approximate* nearest neighbor
     * algorithm, this is not too big an issue. We can assume that they will reside "nearby" and that the
     * greedy nature of the algorithm will find the right nodes.
     * 
     * But it does show that paralleling HNSW *will* impact the resulting graph. To address that, we added
     * a step in the process where we'll add all the existing inflight nodes (being added in parallel) to each
     * other. This means that we create artificial edges to vectors added at the same time. Those edges may be
     * removed if during the insert process we'll find better (closer) edges. 
     * 
     * Having said all of that, the performance difference for large graph is significant. Therefor, we 
     * use a parallel algorithm to build the graph. However, just adding threads isn't simple, because we
     * operate under a single threaded write transaction.
     * 
     * There are two expensive parts in the graph building operations:
     * * Computing distance between vectors
     * * Loading the vectors from disk
     * 
     * This code is designed to allow to parallelize the distance computation and to allow for
     * batch load optimization for reading the vectors. 
     * 
     * To start with, we aren't actually using parallel here to say threads. Instead, we re-wrote
     * the algorithm using yield an enumerators. And we run it using a dedicated runner that consumes and
     * execute all the interleaved (vs. concurrent) operations.
     * 
     * Whenever we need to do an expensive operation (such as loading vectors, or computing distances),
     * we yield to the caller, giving a chance for the rest of the system to make forward progress while
     * the task is completed in the background. 
     * 
     * That async operation is _not_ scheduled on a different thread. Instead, it is queued until all
     * current operations are completed, then we check what pending work we have and start a batch 
     * loads of all the vectors we need. The next step is to run the distance computation using the 
     * thread pool. When that is completed, we can continue with the next step.
     * 
     * The idea is that we run N interleaved tasks, where N between 1..MaxConcurrentBatches, and in 
     * each one of them, we pick an item to be inserted to the graph. We then run the HNSW until we
     * need to do an expensive operation (which we'll offload to the thread pool if it is computation, or
     * do a batch preload to amortise the costs of going to disk). At that point, we yield to *another*
     * interleaved operation. By the time we hit the MaxConcurrentBatches, we gathered enough vectors to load and
     * distances to compute that we can really start pumping through all the items. 
     * 
     * The key here is to batching of I/O for loading the vectors. See the runner for handling that 
     * part of the process. Both NodePlacement and NodePlacementRunner are working very closely 
     * together to achieve this work.
     * 
     * # Distance computation using the thread pool
     * 
     * Distance computation is expensive, and we want to run it in parallel. Each work item that we 
     * send to the thread pool already had its vectors loaded by the batch process, so we can assume 
     * that they are ready in memory. The work item compares a vector to a set of vectors (typically all 
     * the edges of a particular node) and returns the shortest distance or the filtered set of edges.
     * 
     * We use the thread pool because:
     * * There is a known limit to the amount of work we have (up to MaxConcurrentBatches), and it
     *   cannot grow without bound. We won't cause thread pool starvation.
     * * The amount of work for each item is well scoped and _short_. Under 0.5ms for each work item, 
     *   so we won't cause a bottleneck in the thread pool.
     * * We tested using a dedicated thread pool, but those performed significantly worse than the 
     *   default .NET one. 
     *
     * # Allocation-free work item dispatch
     *
     * The WorkItem base class implements IThreadPoolWorkItem, which allows us to queue it directly to
     * the .NET thread pool via ThreadPool.UnsafeQueueUserWorkItem without allocating a delegate or a
     * Task. Each concrete worker (ProcessEdgesWorker, FilterEdgesHeuristicWorker, FindNearestWorker)
     * is preallocated once per NodePlacement instance and stored in a field. The enumerators in
     * Process(), FindGraphPlacementForNode(), NearestEdges(), etc. yield the *same* preallocated
     * worker object repeatedly — mutating its state (CurrentNodeIndex, Level, Owner, etc.) before each
     * yield return. This means no new work items are heap-allocated during the graph-building loop;
     * the runner simply resets and re-queues the same objects. The trade-off is that a yielded WorkItem
     * is only valid until the enumerator advances, but that is fine because the runner always consumes
     * the item before calling MoveNext again.
     */
    public partial class Registration
    {
        private int _nextNodeIndex;

        public int MaxConcurrentBatches = 512;

        private void InsertVectorsToGraph(ref ContextBoundNativeList<byte> byteBuffer, CancellationToken token)
        {
            if (_searchState.TryGetLocationForNode(EntryPointId, out var entryPointNode) is false)
            {
                if (_searchState.CreatedNodes is 0)
                    return;
                
                _nextNodeIndex++; // do not attempt to insert the first node, since it is the graph root
                ref Node startingNode = ref _searchState.Nodes[0];
                Span<byte> span = startingNode.Encode(ref byteBuffer);
                var allocatedId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, span.Length, out Span<byte> allocated);
                entryPointNode = (long)allocatedId;
                span.CopyTo(allocated);
                _searchState.RegisterNodeLocation(EntryPointId, entryPointNode);
            }

            // Reorder the pending tail of the batch by VectorId so workers stream
            // through pages in physical order. Voron's container allocates entries
            // by insertion time, not graph topology; sorting by VectorId here gives
            // sequential OS readahead on the new-vector reads without changing
            // which edges get selected. Safe to do after _nextNodeIndex has skipped
            // the entry point.
            if (_envSortByPage)
                _searchState.SortPendingNodesByVectorId(_nextNodeIndex);

            // Run 1..MaxConcurrentBatches batches here, depending on how much work we have to run
            int numberOfBatches = Math.Max(1, _searchState.CreatedNodes / MaxConcurrentBatches);
            // but not too much...
            int maxTasks = Math.Min(numberOfBatches, MaxConcurrentBatches);
            NodePlacementRunner runner = new(this, maxTasks, token);
            runner.Run();
        }

        private class NodePlacement(Registration parent, NodePlacementRunner runner)
        {
            private readonly SearchState _searchState = parent._searchState;
            private readonly List<int> _candidates = [];
            private readonly List<int> _nearestIndexes = [];
            private readonly List<int> _indexes = [];
            private readonly List<int> _requiresEdgeFiltering = [];
            private readonly List<UnmanagedSpan> _vectors = [];
            private readonly PriorityQueue<int, float> _candidatesQ = new();
            private readonly PriorityQueue<int, float> _nearestEdgesQ = new();
            private ulong[] _visitedBitmap = [];
            private int[] _visitedBitmapVersion = [];
            private int _visitedVersion;
            private readonly LinkedListNode<int> _listNode = new(-1);

            // Pooled work items — reused across all yields to avoid per-yield heap allocations
            private readonly ProcessEdgesWorker _processEdgesWorker = new(runner);
            private readonly FilterEdgesHeuristicWorker _filterEdgesWorker = new(runner);
            private readonly FindNearestWorker _findNearestWorker = new(runner);
            
            private void ClearVisited()
            {
                // this needs to be _cheap_, since it is called per node per level
                _visitedVersion++;
            }
            private bool MarkVisited(int pos)
            {
                int index = pos >> 6; // / 64
                int bit = pos & 63; // % 64
                
                if (index >= _visitedBitmap.Length)
                {
                    Grow();
                }

                if (_visitedBitmapVersion[index] != _visitedVersion)
                {
                    // we reset the value if detected the version changed
                    _visitedBitmapVersion[index] = _visitedVersion;
                    _visitedBitmap[index] = 0;
                }
                
                ulong old = _visitedBitmap[index];
                ulong mask = (1ul << bit);
                _visitedBitmap[index] = old | mask;
                bool isNew = (mask & old) == 0;
                return isNew;

                void Grow()
                {
                    int max = Math.Max(_searchState.Nodes.Length, index);
                    int newSize = Bits.NextAllocationSize(max);
                    Array.Resize(ref _visitedBitmap, newSize);
                    Array.Resize(ref _visitedBitmapVersion, newSize);
                }
            }

            public IEnumerable<WorkItem> Process()
            {
                _processEdgesWorker.Owner = this;
                _filterEdgesWorker.Owner = this;
                _findNearestWorker.Owner = this;

                try
                {
                    int createdNodesLength = _searchState.CreatedNodes;
                    while (runner.IsCancelled is false)
                    {
                        // shared across all tasks, we are processing 
                        // multiple nodes in an interleaved (but not concurrently) via
                        // multiple running placement processing at the same time
                        var createdNodeIndex  = parent._nextNodeIndex++;  
                        if (createdNodeIndex  >= createdNodesLength)
                            break;

                        // we do not process these in linear order, so we need
                        // to keep whatever is "in-flight" in a linked list that we can 
                        // cheaply add & remove to
                        var currentNodeIndex = _searchState.GetCreatedNodeIndex(createdNodeIndex); 
                        _listNode.Value = currentNodeIndex;
                        runner.AddInFlight(_listNode);
                        foreach (var item in FindGraphPlacementForNode(createdNodeIndex, currentNodeIndex))
                        {
                            yield return item;
                        }
                        runner.RemoveInFlight(_listNode);
                    }
                }
                finally
                {
                    runner.Done();
                }
            }

            private IEnumerable<WorkItem> FindGraphPlacementForNode(int createdNodeIndex, int currentNodeIndex)
            {
                var currentMaxLevel = _searchState.Options.CurrentMaxLevel(_searchState.CreatedNodes - createdNodeIndex);
                int nodeRandomLevel = GetLevelForNewNode(currentMaxLevel);
                UnmanagedSpan insertedVector;
                {
                    //  scoping n here, to avoid "leaking" the reference and async issues
                    ref var n = ref _searchState.GetNodeByIndex(currentNodeIndex);
                    n.EdgesPerLevel.SetCapacity(_searchState.Llt.Allocator, nodeRandomLevel + 1);
                    insertedVector = n.GetVectorUnmanagedSpan(_searchState);
                    AddEdgesFromInFlightNodes(ref n, createdNodeIndex);
                }

                // Inlined descent from the entry point down to level 0, capturing the closest
                // node at each level. Folding this into the placement loop removes the per-node
                // enumerator allocation that a yielding helper would produce.
                {
                    _nearestIndexes.Clear();
                    ClearVisited();
                    MarkVisited(currentNodeIndex);
                    var snalCurrentNodeIndex = _searchState.GetNodeIndexById(EntryPointId);
                    var snalLevel = currentMaxLevel;
                    var snalDistance = float.MaxValue;
                    while (snalLevel >= 0)
                    {
                        do
                        {
                            _findNearestWorker.Reset(insertedVector, snalCurrentNodeIndex, snalLevel);
                            yield return _findNearestWorker;
                            if (_findNearestWorker.Distance >= snalDistance)
                                break;
                            snalCurrentNodeIndex = _findNearestWorker.CurrentNodeIndex;
                            snalDistance = _findNearestWorker.Distance;
                        } while (true);

                        _nearestIndexes.Add(snalCurrentNodeIndex);
                        snalLevel--;
                    }

                    _nearestIndexes.Reverse();
                }
                
                for (int level = nodeRandomLevel; level >= 0; level--)
                {
                    int startingPointIndex = _nearestIndexes[level];

                    // Inlined NearestEdges(startingPointIndex, currentNodeIndex, insertedVector, level):
                    // beam-search candidate expansion + (conditional) heuristic edge filter. Inlined for
                    // the same reason as SearchNearestAcrossLevels — this is the deepest yield site,
                    // called once per level per node.
                    {
                        Debug.Assert(_candidatesQ.Count == 0);
                        Debug.Assert(_nearestEdgesQ.Count == 0);
                        Debug.Assert(startingPointIndex != currentNodeIndex);

                        float lowerBound = float.MaxValue;
                        ClearVisited();
                        MarkVisited(currentNodeIndex); // we can't have an edge to itself

                        _candidatesQ.Enqueue(startingPointIndex, -lowerBound);

                        while (_candidatesQ.TryDequeue(out var cur, out var curDistance))
                        {
                            if (-curDistance < lowerBound &&
                                _nearestEdgesQ.Count == _searchState.Options.NumberOfCandidates)
                                break;

                            _processEdgesWorker.Reset(insertedVector, lowerBound, cur, level);
                            yield return _processEdgesWorker;
                            lowerBound = _processEdgesWorker.LowerBound;
                        }

                        _candidatesQ.Clear();
                        _candidates.Clear();
                        while (_nearestEdgesQ.TryDequeue(out var edgeId, out _))
                        {
                            _candidates.Add(edgeId);
                        }
                        _candidates.Reverse();

                        if (_candidates.Count > _searchState.Options.NumberOfEdges)
                        {
                            _indexes.Clear();
                            _vectors.Clear();
                            foreach (var candidate in _candidates)
                            {
                                ref var cn = ref _searchState.GetNodeByIndex(candidate);
                                _indexes.Add(candidate);
                                _vectors.Add(cn.GetVectorUnmanagedSpan(_searchState));
                            }

                            // disable preloading - we already got everything from the
                            // previous preloading step and are operating purely in memory
                            _filterEdgesWorker.Reset(insertedVector, -1, level);
                            yield return _filterEdgesWorker;
                        }
                    }

                    PortableExceptions.ThrowIf<InvalidOperationException>(_candidates.Count == 0, "Cannot add a node to the graph without any edges");
                    ref var node = ref _searchState.GetNodeByIndex(currentNodeIndex);
                    ref var list = ref node.EdgesPerLevel[level];
                    // important - we cannot reset here, since we have added edges from the in flight nodes in AddEdgesFromInFlightNodes()
                    list.EnsureCapacityFor(_searchState.Llt.Allocator, _candidates.Count);
                    _requiresEdgeFiltering.Clear();
                    foreach (var edgeIdx in _candidates)
                    {
                        Debug.Assert(edgeIdx != currentNodeIndex);
                        ref Node edge = ref _searchState.GetNodeByIndex(edgeIdx);
                        list.AddUnsafe(edge.NodeId);

                        ref var edgeList = ref edge.EdgesPerLevel[level];
                        edgeList.Add(_searchState.Llt.Allocator, node.NodeId);

                        if (edgeList.Count <= _searchState.Options.NumberOfEdges)
                            continue;

                        _requiresEdgeFiltering.Add(edgeIdx);
                    }

                    foreach (var edgeIdx in _requiresEdgeFiltering)
                    {
                        UnmanagedSpan vector;
                        {
                            ref Node edge = ref _searchState.GetNodeByIndex(edgeIdx);
                            vector = edge.GetVectorUnmanagedSpan(_searchState);
                            ClearVisited();
                            MarkVisited(edgeIdx);
                        }

                        _filterEdgesWorker.Reset(vector, edgeIdx, level);
                        yield return _filterEdgesWorker;
                        
                        PortableExceptions.ThrowIf<InvalidOperationException>(_candidates.Count == 0 , "Cannot add a node to the graph without any edges after heuristic");
                        {
                            ref Node edge = ref _searchState.GetNodeByIndex(edgeIdx);
                            ref var edgeList = ref edge.EdgesPerLevel[level];
                            edgeList.ResetAndEnsureCapacity(_searchState.Llt.Allocator, _candidates.Count);
                            foreach (var idx in _candidates)
                            {
                                edgeList.AddUnsafe(_searchState.GetNodeByIndex(idx).NodeId);
                            }
                        }
                    }
                }
            }

            private void AddEdgesFromInFlightNodes(ref Node n, int createdNodeIndex)
            {
                // Here we add "number of edges" previously added items to as the edges in all their levels
                // so the next stage will add the edges that were already added to the graph and then find 
                // only the most suitable ones. It has the impact of increasing the likelihood that 
                // items that are added at the same time (and thus temporally linked, at least) will
                // be joined. Quite important when you consider that a single document may have multiple
                // vectors associated with it (for example, because of chunking).
                CollectionsMarshal.SetCount(_indexes, _searchState.Options.NumberOfEdges);
                var used = runner.GetInFlightIndexes(_listNode, CollectionsMarshal.AsSpan(_indexes));
                for (int i = 0; i < used; i++)
                {
                    ref var edge = ref _searchState.GetNodeByIndex(_indexes[i]);
                    int sharedLevels = Math.Min(edge.EdgesPerLevel.Count, n.EdgesPerLevel.Count);
                    for (int level = 0; level < sharedLevels; level++)
                    {
                        n.EdgesPerLevel[level].Add(_searchState.Llt.Allocator, edge.NodeId);
                    }
                }
                _indexes.Clear();
            }

            private sealed class FilterEdgesHeuristicWorker(NodePlacementRunner runner) : WorkItem(runner)
            {
                private UnmanagedSpan _src;

                public void Reset(UnmanagedSpan src, int currentNodeIndex, int level)
                {
                    _src = src;
                    CurrentNodeIndex = currentNodeIndex;
                    Level = level;
                }

                protected override void DoWork()
                {
                    // Production edge selector: true Apollonius cover (Theorems 1+7+9).
                    // Witness bits d(v,q) ≤ ρ·d(u,q) over the frozen global Q_u, greedy
                    // (1-1/e) max-cover, k-capture top-up at L0. Legacy is a test-only
                    // escape hatch for the diagnostic side-by-side build.
                    var searchState = Owner._searchState;
                    var candidates = Owner._candidates;
                    var vectors = Owner._vectors;
                    var indexes = Owner._indexes;

                    int N = indexes.Count;
                    candidates.Clear();
                    if (N == 0)
                        return;

                    if (Hnsw.UseLegacyHeuristic)
                        DoWorkLegacyRobustPrune(searchState, candidates, vectors, indexes, N);
                    else
                        DoWorkApolloniusCover(searchState, candidates, vectors, indexes, N, runner.GlobalQuerySample, runner.NodeMagnitudes, runner.QuDotCache, runner.QuDotStride);
                }

                // True Apollonius cover faithful to Theorems 1, 7, 8, 9.
                //
                // Theorem 1: For each candidate v and each q ∈ Q_u, witness bit
                //   b[v,q] = 1 iff d(v,q) ≤ ρ·d(u,q)  (v's A_ρ(u,v) contains q).
                //
                // Theorem 7: Q_u is a frozen GLOBAL random sample. Uniform-convergence
                //   bound applies: |L(S) − L̂(S)| ≤ O(√(M·log(en/M) / m)).
                //
                // Theorems 8/9: maximize the CAPPED SURVIVAL COVERAGE objective
                //   F(S) = Σ_i min(Λ, Σ_{v∈S} γ(v)·1[v covers q_i])
                //   which is monotone submodular. Greedy under |S|≤M gives (1−1/e)·OPT.
                //
                // At construction time we have no hazard telemetry, so γ(v) ≡ −log h₀
                // is a constant. The cap min(Λ, ·) then becomes "K-redundant cover":
                // pick edges so each q ∈ Q_u has up to K = ⌈Λ / −log h₀⌉ witnesses.
                // K=1 reduces to standard max-cover; K≥2 is genuinely Theorem-5 robust
                // (one witness can die without breaking descent for that direction).
                //
                // Theorem 11: at L0, reserve M/2 slots for true nearest-to-u BEFORE cover
                // (terminal-layer capture). Upper layers route by descent — cover gets
                // the full budget.
                // Phase 5 — Tombstone hazard model (Theorems 4/5).
                //
                // h(v) ∈ [0,1] bounds Pr[v unavailable before next repair]. Survival
                // weight γ(v) = −log h(v). For witness set W_ρ(u,q):
                //
                //     Γ(u,q) = Σ_{v ∈ W_ρ(u,q)} γ(v)     (capped at Λ = K·γ₀).
                //
                // Theorem 5: Pr[tombstone-caused descent failure at u] ≤ H(q)·e^(−Λ).
                //
                // We currently estimate hazard with a single global default — every
                // node gets h₀ = 0.1 (≈10 % per-node attrition per repair epoch).
                // That uniform γ ≡ γ₀ collapses the capped-survival objective
                // F(S) = Σ_i min(Λ, Σ_{v∈S} γ(v)·1[v covers q_i]) into K-redundant
                // popcount with K = ⌈Λ / γ₀⌉, which is what the bitmask fast path
                // below exploits. The math equivalence (proof in journal entry
                // "2026-05-15 — Phase 5") lets us defer the O(Qm) real-valued
                // accumulator until variable γ telemetry exists.
                //
                // Variable-γ extension point: replace HazardFor(int) with a
                // per-candidate lookup (degree-based, level-based, or telemetry-
                // driven). When γ becomes non-constant, swap the popcount path for
                // the real-valued accumulator preserved in git history.
                //
                // Repair-on-deficit (Phase 5 second half) is NOT in this method —
                // it belongs in the post-tombstone path (Hnsw.Registration.Remove)
                // where Γ_S(u,q) is recomputed for u's that touched a deleted node
                // and replacement witnesses are added when Γ falls below Λ.
                // Tracked as TODO; not on the critical path because cluster-churn
                // diagnostics already show Apollonius hitting recall 1.000 at
                // M=32 under 20 % churn without repair.
                private const float HazardH0 = 0.1f;
                private static readonly float Gamma0 = -MathF.Log(HazardH0); // ≈ 2.302
                private const int RedundancyDepth = 2;                       // K
                private static readonly float Lambda = RedundancyDepth * Gamma0;

                /// <summary>
                /// Per-candidate survival weight γ(v) = −log h(v). Constant for now;
                /// hook for Phase 5.5 variable hazard. Kept as a method so the JIT
                /// can inline the current constant return and so future telemetry-
                /// driven hazard can replace the body without touching the cover.
                /// </summary>
                private static float HazardFor(int candidateIndex) => Gamma0;

                // Phase 6 — I/O-aware edge cost (Theorem 10).
                //
                // Framework cost decomposition:
                //
                //   c(u, v) = c_dist(v) + λ_page · c_page(u,v)
                //                       + λ_haz  · c_haz(v)
                //                       + λ_deg  · c_deg(v).
                //
                // Theorem 10: if each chosen edge has c(u_t, u_{t+1}) ≤ β · C_ρ(u_t, q)
                // (β-approx of the cheapest live descent edge in the Apollonius cell),
                // then total query cost is bounded by β · Σ C_ρ + H(q)·c_queue.
                //
                // What this gives us in the greedy: a *tie-breaker*. When two
                // candidates have equal cover gain, prefer the cheaper one. With
                // uniform cost (the default) this collapses to "first wins" — the
                // current behaviour, so recall is unchanged. When Voron page
                // co-location data is later threaded in via the EdgeCost hook,
                // edges that stay on the same page are preferred, reducing random
                // I/O during descent without changing the cover's Theorem-9 gain
                // bound (we only break ties, not redirect them).
                //
                // We deliberately do NOT use a gain-per-cost ratio for the primary
                // ordering — that would weaken Theorem 9's (1−1/e) guarantee.
                // Cost is a strict tie-breaker on equal gain.
                private const float LambdaPage = 0.0f; // disabled until page-locality lookup is wired
                private const float LambdaHaz = 0.0f;  // disabled until variable hazard is wired
                private const float LambdaDeg = 0.0f;  // disabled until degree telemetry is wired

                /// <summary>
                /// Per-candidate edge cost (Theorem 10). Returns 1.0 by default; the
                /// extension point for page-locality / hazard / degree weighting. When
                /// this becomes non-constant, the greedy below already uses it as a
                /// tie-breaker — no further wiring needed.
                /// </summary>
                private static float EdgeCost(int candidateIndex) => 1.0f
                    + LambdaPage * 0.0f   // c_page(u, v): same-page bonus
                    + LambdaHaz * 0.0f    // c_haz(v): tombstone risk surcharge
                    + LambdaDeg * 0.0f;   // c_deg(v): high-degree fan-out surcharge

                // λ_code hyperparameter — see Registration._envApolloLambda. Selector uses
                // δ(v,q) ≤ λ_code · δ(u,q); chordal ρ_metric = √λ_code (FRAMEWORK §15).

                private void DoWorkApolloniusCover(SearchState searchState, List<int> candidates, List<UnmanagedSpan> vectors, List<int> indexes, int N, UnmanagedSpan[] Qu, float[] nodeMagnitudes, float[] quDotCache, int quDotStride)
                {
                    int M = searchState.Options.NumberOfEdges;
                    int Qm = Qu.Length;
                    Debug.Assert(Qm >= 0 && Qm <= 64, $"|Q_u| must fit one ulong (≤64). Got {Qm}.");
                    bool prof = CoverProfileEnabled;
                    long t0Total = prof ? Stopwatch.GetTimestamp() : 0;
                    long tStep = t0Total;

                    // λ · δ(u, q_k) thresholds for each q in Q_u. In the chordal metric this
                    // corresponds to a ρ_metric = √λ Apollonius cell; the inequality test
                    // and chosen candidates are identical, only the metric label changes.
                    Span<float> threshold = stackalloc float[64];
                    for (int k = 0; k < Qm; k++)
                        threshold[k] = _envApolloLambda * searchState.Distance(_src, Qu[k]);

                    // Witness bitmask per candidate. Bit k set iff candidate i covers q_k.
                    Span<ulong> witness = stackalloc ulong[N <= 1024 ? N : 0];
                    ulong[] witnessHeap = null;
                    if (witness.Length == 0)
                    {
                        witnessHeap = new ulong[N];
                        witness = witnessHeap;
                    }
                    // Witness fill via precomputed L2 norms + raw dot product. CosineDistance
                    // recomputes |v| and |q| on every call (3 dots + sqrts internally); doing
                    // it once per vector and per query collapses the inner test to one dot
                    // per (i,k). Profile-confirmed: witness was 1.5 s of CosineDistance work
                    // on d=128 N=10k. Magnitude of each candidate is also shared with the
                    // spread-check path below to avoid double computation. Only valid for the
                    // singles-cosine similarity — I8 and Hamming have different magnitude
                    // semantics, fall back to the per-call Distance path.
                    bool fastCosine = searchState.Options.SimilarityMethod == SimilarityMethod.CosineSimilaritySingles;
                    Span<float> magV = stackalloc float[N <= 1024 ? N : 0];
                    float[] magVHeap = null;
                    if (fastCosine && magV.Length == 0)
                    {
                        magVHeap = new float[N];
                        magV = magVHeap;
                    }
                    // Witness only matters when the cover-gain greedy will consult it.
                    // Under dist-greedy defaults the cover bits drive nothing, so we skip
                    // the Qm·N inner dots and leave witness[i] = 0. magV is still required
                    // by PassesAngularSpread, so its one-dot-per-i fill stays.
                    bool needWitness = _envGreedyByDist == false;
                    if (fastCosine)
                    {
                        // Cache q_k float byte spans + magnitudes + cutoffs.
                        //   pass ⇔ <v_i, q_k> ≥ cutoff[k] · |v_i|
                        //   cutoff[k] = (1 − threshold[k]) · |q_k|
                        // The byte-span pointer/length cache avoids re-doing MemoryMarshal.Cast
                        // Qm·N times in the inner loop.
                        Span<float> cutoff = stackalloc float[64];
                        Span<float> qMag = stackalloc float[64];
                        Span<IntPtr> qPtr = stackalloc IntPtr[64];
                        Span<int> qLen = stackalloc int[64];
                        unsafe
                        {
                            int dDim = 0;
                            for (int k = 0; k < Qm; k++)
                            {
                                var qbytes = Qu[k].ToSpan();
                                qPtr[k] = (IntPtr)Unsafe.AsPointer(ref MemoryMarshal.GetReference(qbytes));
                                qLen[k] = qbytes.Length / sizeof(float);
                                dDim = qLen[k];
                                var qf = MemoryMarshal.Cast<byte, float>(qbytes);
                                float magQ = MathF.Sqrt(TensorPrimitives.Dot<float>(qf, qf));
                                qMag[k] = magQ;
                                cutoff[k] = (1f - threshold[k]) * magQ;
                            }

                            // JL witness sketch (§17). Project candidates and queries into
                            // m=JlSketchDim space; clear-pass / clear-fail decisions skip
                            // the exact d-dim dot; only ε-band cases fall back to it.
                            // The sketched test is:
                            //   sketched_dot ≥ cutoff[k]·|v_i| + ε·|v_i|·|q_k|  → clear PASS
                            //   sketched_dot ≤ cutoff[k]·|v_i| − ε·|v_i|·|q_k|  → clear FAIL
                            // Output is recall-equivalent (worst case: more ε-band → exact).
                            bool useJl = needWitness && JlSketchEnabled && Qm > 0 && dDim > 0;
                            float[] jlMatrix = useJl ? GetJlMatrix(dDim) : null;
                            // Sketch buffers — small per cover call (Qm·m + N·m floats).
                            float[] pqHeap = useJl ? new float[Qm * JlSketchDim] : null;
                            float[] pvHeap = useJl ? new float[N * JlSketchDim] : null;
                            if (useJl)
                            {
                                // Project queries once.
                                for (int k = 0; k < Qm; k++)
                                {
                                    var qf = new ReadOnlySpan<float>((void*)qPtr[k], qLen[k]);
                                    for (int j = 0; j < JlSketchDim; j++)
                                    {
                                        pqHeap[k * JlSketchDim + j] =
                                            TensorPrimitives.Dot<float>(qf, jlMatrix.AsSpan(j * dDim, dDim));
                                    }
                                }
                            }

                            int magCacheLen = nodeMagnitudes?.Length ?? 0;
                            for (int i = 0; i < N; i++)
                            {
                                var vbytes = vectors[i].ToSpan();
                                var vf = MemoryMarshal.Cast<byte, float>(vbytes);
                                // Per-node |v| cache: same v reappears as candidate across
                                // many covers in this build. Sentinel 0f = uncomputed. Float
                                // writes are atomic and idempotent — all writers compute the
                                // same value, so unsynchronised read-then-write is safe.
                                int nid = indexes[i];
                                float mv = (uint)nid < (uint)magCacheLen ? nodeMagnitudes[nid] : 0f;
                                if (mv == 0f)
                                {
                                    mv = MathF.Sqrt(TensorPrimitives.Dot<float>(vf, vf));
                                    if ((uint)nid < (uint)magCacheLen)
                                        nodeMagnitudes[nid] = mv;
                                }
                                magV[i] = mv;
                                ulong bits = 0;
                                if (needWitness == false)
                                {
                                    // dist-greedy mode: cover bits unused; skip the Qm-dot
                                    // inner loop entirely. magV[i] was filled above and is
                                    // what PassesAngularSpread needs.
                                    witness[i] = 0;
                                    continue;
                                }
                                if (useJl)
                                {
                                    // Project this candidate into sketch space.
                                    Span<float> pv = pvHeap.AsSpan(i * JlSketchDim, JlSketchDim);
                                    for (int j = 0; j < JlSketchDim; j++)
                                        pv[j] = TensorPrimitives.Dot<float>(vf, jlMatrix.AsSpan(j * dDim, dDim));
                                    for (int k = 0; k < Qm; k++)
                                    {
                                        var pq = pqHeap.AsSpan(k * JlSketchDim, JlSketchDim);
                                        float sketched = TensorPrimitives.Dot<float>(pv, pq);
                                        float target = cutoff[k] * mv;
                                        float slack = JlEpsilon * mv * qMag[k];
                                        if (sketched >= target + slack)
                                        {
                                            bits |= 1UL << k; // clear pass
                                        }
                                        else if (sketched > target - slack)
                                        {
                                            // ambiguous — exact fallback
                                            var qf = new ReadOnlySpan<float>((void*)qPtr[k], qLen[k]);
                                            float exact = TensorPrimitives.Dot<float>(vf, qf);
                                            if (exact >= target)
                                                bits |= 1UL << k;
                                        }
                                        // else: clear fail, leave bit clear
                                    }
                                }
                                else
                                {
                                    // Hoist `cutoff[k] · |v_i|` out of the inner loop: it is
                                    // constant for fixed i across k.
                                    bool useDotCache = quDotCache != null && quDotStride == Qm && (uint)nid < (uint)magCacheLen;
                                    if (useDotCache)
                                    {
                                        // Per-node Q_u dot cache: <v_i, q_k> depends only on
                                        // (v_i, q_k), so a value computed during a previous
                                        // cover for this same v_i is reusable now. The cutoff
                                        // depends on u and is applied at lookup. NaN sentinel
                                        // = uncomputed; idempotent write across threads.
                                        long baseIdx = (long)nid * quDotStride;
                                        for (int k = 0; k < Qm; k++)
                                        {
                                            float dot = quDotCache[baseIdx + k];
                                            if (float.IsNaN(dot))
                                            {
                                                var qf = new ReadOnlySpan<float>((void*)qPtr[k], qLen[k]);
                                                dot = TensorPrimitives.Dot<float>(vf, qf);
                                                quDotCache[baseIdx + k] = dot;
                                            }
                                            if (dot >= cutoff[k] * mv)
                                                bits |= 1UL << k;
                                        }
                                    }
                                    else
                                    {
                                        for (int k = 0; k < Qm; k++)
                                        {
                                            var qf = new ReadOnlySpan<float>((void*)qPtr[k], qLen[k]);
                                            float dot = TensorPrimitives.Dot<float>(vf, qf);
                                            if (dot >= cutoff[k] * mv)
                                                bits |= 1UL << k;
                                        }
                                    }
                                }
                                witness[i] = bits;
                            }
                        }
                    }
                    else if (needWitness)
                    {
                        for (int i = 0; i < N; i++)
                        {
                            ulong bits = 0;
                            var v = vectors[i];
                            for (int k = 0; k < Qm; k++)
                            {
                                if (searchState.Distance(v, Qu[k]) <= threshold[k])
                                    bits |= 1UL << k;
                            }
                            witness[i] = bits;
                        }
                    }
                    if (prof)
                    {
                        long now = Stopwatch.GetTimestamp();
                        Interlocked.Add(ref CoverWitnessTicks, now - tStep);
                        tStep = now;
                    }

                    Span<bool> picked = stackalloc bool[N <= 1024 ? N : 0];
                    bool[] pickedHeap = null;
                    if (picked.Length == 0)
                    {
                        pickedHeap = new bool[N];
                        picked = pickedHeap;
                    }

                    // Distance-to-source cache. Computed BEFORE kCapture so the priority-queue
                    // population reuses these distances instead of recomputing them — saves N
                    // redundant CosineDistance calls per cover invocation. distToSrc is also
                    // used by the spread test during greedy and M-fill.
                    Span<float> distToSrc = stackalloc float[N <= 1024 ? N : 0];
                    float[] distToSrcHeap = null;
                    if (distToSrc.Length == 0)
                    {
                        distToSrcHeap = new float[N];
                        distToSrc = distToSrcHeap;
                    }
                    if (fastCosine)
                    {
                        var srcF = MemoryMarshal.Cast<byte, float>(_src.ToSpan());
                        float magSrc = MathF.Sqrt(TensorPrimitives.Dot<float>(srcF, srcF));
                        for (int i = 0; i < N; i++)
                        {
                            var vf = MemoryMarshal.Cast<byte, float>(vectors[i].ToSpan());
                            float dot = TensorPrimitives.Dot<float>(srcF, vf);
                            distToSrc[i] = 1f - dot / (magSrc * magV[i]);
                        }
                    }
                    else
                    {
                        for (int i = 0; i < N; i++)
                            distToSrc[i] = searchState.Distance(_src, vectors[i]);
                    }
                    if (prof)
                    {
                        long now = Stopwatch.GetTimestamp();
                        Interlocked.Add(ref CoverDistToSrcTicks, now - tStep);
                        tStep = now;
                    }

                    // L0 k-capture (Theorem 11): reserve M/2 slots for nearest-to-u BEFORE
                    // cover. Upper layers route by descent so cover gets the full budget.
                    int kCapture = (Level == 0 && _envKCaptureOff == false) ? Math.Max(1, M / 2) : 0;
                    if (kCapture > 0)
                    {
                        var queue = Owner._candidatesQ;
                        Debug.Assert(queue.Count == 0);
                        for (int i = 0; i < N; i++)
                            queue.Enqueue(i, distToSrc[i]);
                        while (candidates.Count < kCapture && queue.TryDequeue(out var cur, out _))
                        {
                            candidates.Add(cur);
                            picked[cur] = true;
                        }
                        queue.Clear();
                    }
                    if (prof)
                    {
                        long now = Stopwatch.GetTimestamp();
                        Interlocked.Add(ref CoverKCaptureTicks, now - tStep);
                        tStep = now;
                    }

                    // K=2 redundant cover state, encoded as two bitmasks (O(1) per pick):
                    //   coveredOnce  = bits with ≥1 witness in S
                    //   coveredTwice = bits with ≥2 witnesses in S (the cap)
                    // A bit "needs more coverage" iff it is not in coveredTwice.
                    // (Compile-time assertion: this fast path is K=2 only.)
                    Debug.Assert(RedundancyDepth == 2, "Fast bitset path assumes K=2.");
                    // Phase 5 invariant: the popcount fast path is exact iff every
                    // candidate has γ(v) = γ₀ (uniform hazard). When HazardFor stops
                    // returning a constant, this assert fires and the cover must
                    // switch to the real-valued capped-survival accumulator.
                    Debug.Assert(N == 0 || MathF.Abs(HazardFor(0) - Gamma0) < 1e-5f,
                        "Variable hazard detected — popcount fast path no longer matches F(S).");
                    ulong coveredOnce = 0;
                    ulong coveredTwice = 0;
                    for (int j = 0; j < candidates.Count; j++)
                    {
                        ulong w = witness[candidates[j]];
                        coveredTwice |= w & coveredOnce; // already-once bits → now twice
                        coveredOnce |= w;
                    }

                    // Greedy capped-survival cover (Theorem 9), gated by shell-wise angular
                    // spread (§10.C-B). For each round: find the highest-gain candidate that
                    // also satisfies d(v, w) ≥ χ · min(d(u,v), d(u,w)) against every
                    // already-picked w. §10.B proves pure cover cannot give small η on
                    // isotropic high-d data; the spread constraint is what closes that gap.
                    //
                    // χ = 0.7 is the empirically-tuned Pareto point (see commit log). The
                    // constraint is symmetric in the bound (min over u-distances), unlike
                    // legacy which is one-sided in d(u,v). Without (B) the recall
                    // regression is 4–16pp on isotropic data.
                    float AngularSpreadChi = _envChi;

                    // Lazy spread check. Profiling showed eager conflict-bitset precompute
                    // burned ~50% of cover wall on d=128 because pairs in similar shells
                    // (triangle-skip miss rate is high in high-d) all pay full Δ(v,w).
                    // The greedy only ever picks ≤ M ≈ 16 edges, so at most M tentative
                    // bests get spread-checked — O(M²) distance calls instead of O(N²).
                    // Rejected tentative bests are marked picked[] so they don't recur as
                    // best-gain on the next round (any future picked set is a superset of
                    // the current one, so the same conflicting w would re-block them).
                    bool greedyByDist = _envGreedyByDist;
                    while (candidates.Count < M)
                    {
                        ulong needsMore = ~coveredTwice;
                        if (Qm < 64)
                            needsMore &= (1UL << Qm) - 1;

                        int bestI = -1;
                        if (greedyByDist)
                        {
                            // Distance-ordered fill: pick the nearest unpicked v to u that
                            // also passes spread. The cover bitmask is updated for telemetry
                            // and to drive the early-exit on "coverage saturated", but is no
                            // longer the selection criterion.
                            float bestDist = float.MaxValue;
                            for (int i = 0; i < N; i++)
                            {
                                if (picked[i]) continue;
                                float d = distToSrc[i];
                                if (d >= bestDist) continue;
                                bestDist = d;
                                bestI = i;
                            }
                            if (bestI == -1) break;
                        }
                        else
                        {
                            if (needsMore == 0) break;
                            int bestGain = -1;
                            float bestCost = float.MaxValue;
                            for (int i = 0; i < N; i++)
                            {
                                if (picked[i]) continue;
                                int gain = BitOperations.PopCount(witness[i] & needsMore);
                                if (gain < bestGain) continue;
                                float cost = EdgeCost(i);
                                if (gain == bestGain && cost >= bestCost) continue;
                                bestGain = gain;
                                bestI = i;
                                bestCost = cost;
                            }
                            if (bestI == -1 || bestGain == 0) break;
                        }
                        if (PassesAngularSpread(searchState, vectors, distToSrc, bestI, candidates, AngularSpreadChi, fastCosine ? magV : default) == false)
                        {
                            picked[bestI] = true;
                            continue;
                        }
                        candidates.Add(bestI);
                        picked[bestI] = true;
                        ulong w = witness[bestI];
                        coveredTwice |= w & coveredOnce;
                        coveredOnce |= w;
                    }
                    if (prof)
                    {
                        long now = Stopwatch.GetTimestamp();
                        Interlocked.Add(ref CoverGreedyTicks, now - tStep);
                        tStep = now;
                    }

                    // M-fill top-up: nearest-of-remaining that also passes (B). Needed when
                    // greedy stops because every remaining witness=0. Same lazy strategy:
                    // tentative nearest, spread-check on the winner, mark blocked on fail.
                    while (candidates.Count < M)
                    {
                        int bestI = -1;
                        float bestDist = float.MaxValue;
                        for (int i = 0; i < N; i++)
                        {
                            if (picked[i])
                                continue;
                            if (distToSrc[i] >= bestDist)
                                continue;
                            bestDist = distToSrc[i];
                            bestI = i;
                        }
                        if (bestI == -1)
                            break;
                        if (PassesAngularSpread(searchState, vectors, distToSrc, bestI, candidates, AngularSpreadChi, fastCosine ? magV : default) == false)
                        {
                            picked[bestI] = true;
                            continue;
                        }
                        candidates.Add(bestI);
                        picked[bestI] = true;
                    }

                    if (prof)
                    {
                        long now = Stopwatch.GetTimestamp();
                        Interlocked.Add(ref CoverMFillTicks, now - tStep);
                        Interlocked.Add(ref CoverTotalTicks, now - t0Total);
                        Interlocked.Increment(ref CoverCalls);
                    }

                    for (int i = 0; i < candidates.Count; i++)
                        candidates[i] = indexes[candidates[i]];
                }

                // Shell-wise angular spread (Theorem 10.C-B). Reject candidate i if for any
                // already-picked w: Δ(v_i, v_w) < χ · min(Δ(u, v_i), Δ(u, v_w)), where Δ is
                // the squared distance returned by Distance(). Equivalent to legacy α-prune
                // at χ=1 except legacy uses one-sided Δ(u, v_i).
                //
                // Triangle-inequality fast path. Let r_v = √Δ(u,v), r_w = √Δ(u,w), and WLOG
                // r_w ≤ r_v. By the linear triangle inequality d(v,w) ≥ r_v − r_w, so
                //   Δ(v,w) ≥ (r_v − r_w)²,
                // and (r_v − r_w)² ≥ χ·r_w²  ⇔  r_v ≥ (1+√χ)·r_w  ⇔  r_v² ≥ (1+√χ)²·r_w².
                // So whenever max(distToSrc) ≥ SpreadSkipFactor · min(distToSrc), the pair
                // **provably** passes B_χ and we can skip the Δ(v,w) computation. For
                // χ=0.7, (1+√0.7)² ≈ 3.373. This is exact, not an approximation —
                // preserves the Theorem 10.C-B guarantee.
                private static readonly float SpreadSkipFactor = _envSpreadSkip;

                private bool PassesAngularSpread(SearchState searchState, List<UnmanagedSpan> vectors,
                    Span<float> distToSrc, int i, List<int> candidates, float chi,
                    Span<float> magV = default)
                {
                    if (chi <= 0f)
                        return true;
                    var vi = vectors[i];
                    float di = distToSrc[i];
                    // Magnitude-sharing fast path: when magV[] was filled by the witness step
                    // (cosine singles only), each Δ(v,w) call becomes one raw dot product
                    // instead of three (one inner + two magnitudes). Algebraically equivalent
                    // by Cauchy–Schwarz rearrangement: cosDist < χ·rMin ⇔ <v,w> > (1−χ·rMin)·|v|·|w|.
                    bool useMag = magV.Length > 0;
                    ReadOnlySpan<float> viFloats = useMag ? MemoryMarshal.Cast<byte, float>(vi.ToSpan()) : default;
                    float magVi = useMag ? magV[i] : 0f;
                    bool symmetric = _envSpreadSymmetric;
                    for (int j = 0; j < candidates.Count; j++)
                    {
                        int w = candidates[j];
                        float dw = distToSrc[w];
                        // Reference distance for the spread threshold:
                        //   symmetric → min(Δ(u,v), Δ(u,w)) (looser)
                        //   onesided  → Δ(u, v_candidate) only (legacy α-prune semantics, stricter
                        //               in dist-greedy order where dw ≤ di always).
                        float rRef = symmetric ? Math.Min(di, dw) : di;
                        float rMax = Math.Max(di, dw);
                        // Triangle skip: if the radius ratio is large enough, the spread
                        // condition is automatically satisfied. No Δ(v,w) needed.
                        if (rMax >= SpreadSkipFactor * rRef)
                            continue;
                        if (useMag)
                        {
                            var vwFloats = MemoryMarshal.Cast<byte, float>(vectors[w].ToSpan());
                            float dot = TensorPrimitives.Dot<float>(viFloats, vwFloats);
                            float rhs = (1f - chi * rRef) * magVi * magV[w];
                            if (dot > rhs)
                                return false;
                        }
                        else
                        {
                            float dvw = searchState.Distance(vi, vectors[w]);
                            if (dvw < chi * rRef)
                                return false;
                        }
                    }
                    return true;
                }

                // Original HNSW Algorithm-4 / DiskANN robust-prune. Test-only path reached
                // via the UseLegacyHeuristic escape hatch — lets diagnostic tests build a
                // baseline graph for side-by-side comparison against the Apollonius cover.
                private void DoWorkLegacyRobustPrune(SearchState searchState, List<int> candidates, List<UnmanagedSpan> vectors, List<int> indexes, int N)
                {
                    var queue = Owner._candidatesQ;
                    Debug.Assert(queue.Count == 0);
                    for (int i = 0; i < N; i++)
                        queue.Enqueue(i, searchState.Distance(_src, vectors[i]));

                    // Audit-A6: was `<= NumberOfEdges` (filled M+1). Apollonius path uses `< M`.
                    // Equalising to `< M` removes a 1-extra-edge bias from every comparison.
                    while (candidates.Count < searchState.Options.NumberOfEdges &&
                           queue.TryDequeue(out var cur, out var distance))
                    {
                        bool match = true;
                        foreach (var altLocal in candidates)
                        {
                            var curDist = searchState.Distance(vectors[cur], vectors[altLocal]);
                            if (curDist < distance)
                            {
                                match = false;
                                break;
                            }
                        }
                        if (match)
                            candidates.Add(cur);
                    }

                    for (int i = 0; i < candidates.Count; i++)
                        candidates[i] = indexes[candidates[i]];
                    queue.Clear();
                }

            }
            private sealed class ProcessEdgesWorker(NodePlacementRunner runner) : WorkItem(runner)
            {
                private UnmanagedSpan _vector;
                public float LowerBound;

                public void Reset(UnmanagedSpan vector, float lowerBound, int currentNodeIndex, int level)
                {
                    _vector = vector;
                    LowerBound = lowerBound;
                    CurrentNodeIndex = currentNodeIndex;
                    Level = level;
                }

                protected override void DoWork()
                {
                    var searchState = Owner._searchState;
                    var indexes = Owner._indexes;
                    var vectors = Owner._vectors;
                    var nearestEdgesQ = Owner._nearestEdgesQ;
                    var candidatesQ = Owner._candidatesQ;
                    var lowerBound  = LowerBound;
                    
                    int numberOfCandidates = searchState.Options.NumberOfCandidates;
                    for (int i = 0; i < indexes.Count; i++)
                    {
                        var nextIndex = indexes[i];
                        Debug.Assert(searchState.Nodes[nextIndex].EdgesPerLevel.Count > Level); 
                   
                        float nextDist = -searchState.Distance(_vector, vectors[i]);
                        if (nearestEdgesQ.Count < numberOfCandidates)
                        {
                            candidatesQ.Enqueue(nextIndex, -nextDist);
                            nearestEdgesQ.Enqueue(nextIndex, nextDist);
                        }
                        else if (lowerBound < nextDist)
                        {
                            candidatesQ.Enqueue(nextIndex, -nextDist);
                            nearestEdgesQ.EnqueueDequeue(nextIndex, nextDist);
                        }
                        else
                        {
                            continue;
                        }

                        Debug.Assert(candidatesQ.Count > 0);
                        nearestEdgesQ.TryPeek(out _, out lowerBound);
                    }
                    LowerBound = lowerBound;
                }
                
            }

            private sealed class FindNearestWorker(NodePlacementRunner runner) : WorkItem(runner)
            {
                private UnmanagedSpan _from;
                public float Distance;

                public void Reset(UnmanagedSpan from, int currentNodeIndex, int level)
                {
                    _from = from;
                    Distance = float.MaxValue;
                    CurrentNodeIndex = currentNodeIndex;
                    Level = level;
                }

                protected override void DoWork()
                {
                    var indexes = Owner._indexes;
                    var vectors = Owner._vectors;
                    var searchState = Owner._searchState;
                    
                    for (var i = 0; i < indexes.Count; i++)
                    {
                        var edgeIdx = indexes[i];
                        var curDist = searchState.Distance(_from, vectors[i]);
                        if (curDist >= Distance || double.IsNaN(curDist))
                            continue;
                        Distance = curDist;
                        CurrentNodeIndex = edgeIdx;
                    }
                }
            }
            
            private int GetLevelForNewNode(int maxLevel)
            {
                // Use the level assignment formula from the original HNSW paper.
                // Most nodes stay at level 0 where they form a dense, detailed graph
                // that captures fine-grained neighborhood relationships. Only a few
                // nodes get promoted to upper levels, which act as sparse long-range
                // shortcuts. During search, the algorithm quickly descends through
                // these thin upper layers to find a good entry region, then switches
                // to the dense level 0 to refine the actual nearest neighbors.
                // If promotion were too aggressive (e.g. a 50% coin flip), half the
                // nodes would reach level 1, a quarter level 2, and so on. The upper
                // layers would become crowded with nodes and edges, making insertion
                // and search spend most of their time navigating dense upper levels
                // instead of quickly skipping down to where the real work happens.
                int m = _searchState.Options.NumberOfEdges;
                double mL = 1.0 / Math.Log(m);
                double r = parent.Random.NextDouble();
                // Avoid log(0)
                if (r == 0.0) r = double.Epsilon;
                int level = (int)(-Math.Log(r) * mL);
                return Math.Min(level, maxLevel);
            }

            /// <summary>
            /// LLT-thread half of the former AfterPreloading. Performs only the steps that
            /// touch <see cref="SearchState.Llt"/> (and therefore must stay single-threaded):
            /// allocating edge-list capacity and, when the EdgesIndexesPerLevel mirror is
            /// stale, rebuilding it while force-loading any edge whose vector is still lazy.
            /// The bitmap + per-edge _indexes/_vectors fill loop is now in
            /// <see cref="PopulateWorkListsOnWorker"/>, which the WorkItem.Execute hook runs
            /// on a ThreadPool worker before <see cref="WorkItem.DoWork"/>.
            ///
            /// We always return true now (modulo the -1 sentinel for "use the existing
            /// _indexes from the previous yield") — the worker decides whether to call
            /// DoWork based on the post-fill _indexes count.
            /// </summary>
            public bool PrepareEdgesOnLLT(int currentNodeIndex, int level)
            {
                if (currentNodeIndex is -1)
                    return _indexes.Count > 0;

                ref var n = ref _searchState.GetNodeByIndex(currentNodeIndex);

                // The slow path runs RegisterForPreloading first, which sizes both lists.
                // The all-in-memory fast path skips that step, so we must guarantee the slot
                // exists before we ref into it. SetCapacity is a no-op when already sized.
                n.EdgesPerLevel.SetCapacity(_searchState.Llt.Allocator, level + 1);
                n.EdgesIndexesPerLevel.SetCapacity(_searchState.Llt.Allocator, level + 1);

                ref var edgesList = ref n.EdgesPerLevel[level];
                ref var edgesIndexes = ref n.EdgesIndexesPerLevel[level];
                if (edgesIndexes.Count != edgesList.Count)
                {
                    // Mirror is stale: rebuild edgesIndexes AND, in the same pass, force-load any
                    // edge whose vector is still lazy. This is the only path that introduces
                    // freshly cache-resolved nodes (CopyNodeFromCache leaves _vectorSpan default),
                    // so once we walk it, every entry in edgesIndexes references a Node with
                    // VectorLoaded=true. _vectorSpan never resets, so subsequent calls on this
                    // (node, level) keep that invariant — letting us skip the per-edge VectorLoaded
                    // sweep entirely on the mirror-in-sync path. Profile (2026-05-09 split) had
                    // that sweep at 6.4 s / 15 s of LLT exclusive (43 %) while only ~2 % of calls
                    // actually triggered the rebuild.
                    edgesIndexes.ResetAndEnsureCapacity(_searchState.Llt.Allocator, edgesList.Count);
                    foreach (var nodeId in edgesList)
                    {
                        int idx = _searchState.GetNodeIndexById(nodeId);
                        edgesIndexes.AddUnsafe(idx);
                        ref var edge = ref _searchState.GetNodeByIndex(idx);
                        if (edge.VectorLoaded is false)
                            _ = edge.GetVectorUnmanagedSpan(_searchState);
                    }
                }

                return true; // always dispatch; worker decides via _indexes.Count after fill
            }

            /// <summary>
            /// Worker-thread half of the former AfterPreloading. Walks the edges of
            /// <paramref name="currentNodeIndex"/> at <paramref name="level"/>, applying the
            /// per-task visited bitmap and populating <see cref="_indexes"/> / <see cref="_vectors"/>
            /// for the upcoming WorkItem.DoWork call. All state mutated here lives on the
            /// owning NodePlacement (per-task, never shared) — the SearchState reads are
            /// either field reads on already-loaded nodes (guaranteed by PrepareEdgesOnLLT)
            /// or by-index lookups into the shared <see cref="SearchState.Nodes"/> array,
            /// which is safe to read concurrently while the LLT thread is parked in dispatch.
            /// Returns true iff DoWork has anything to compute.
            /// </summary>
            public bool PopulateWorkListsOnWorker(int currentNodeIndex, int level)
            {
                if (currentNodeIndex is -1)
                    return _indexes.Count > 0;

                ref var n = ref _searchState.GetNodeByIndex(currentNodeIndex);
                _indexes.Clear();
                _vectors.Clear();
                if (MarkVisited(currentNodeIndex))
                {
                    _indexes.Add(currentNodeIndex);
                    _vectors.Add(n.GetVectorUnmanagedSpan(_searchState));
                }

                ref var edgesIndexes = ref n.EdgesIndexesPerLevel[level];
                foreach (var idx in edgesIndexes)
                {
                    if (MarkVisited(idx) is false)
                        continue;
                    _indexes.Add(idx);
                    ref var edge = ref _searchState.GetNodeByIndex(idx);
                    _vectors.Add(edge.GetVectorUnmanagedSpan(_searchState));
                }

                return _indexes.Count > 0;
            }
        }

        /// <summary>
        /// This works opposite to how you'll usually think about such runners.
        /// It is running everything in a _single_ threaded (because it uses the single threaded transaction)
        /// and offload computational work to the thread pool, this is done using the NodePlacement yielding
        /// whenever it wants to offload a computation, and the runner is then taking care of running the code,
        /// 
        /// </summary>
        private class NodePlacementRunner
        {
            private readonly int _activeTasksCount;
            private int _completed;
            private readonly ManualResetEventSlim _ready = new();
            private readonly ConcurrentQueue<IEnumerator<WorkItem>> _placementTasks = [];
            private readonly ConcurrentQueue<(Exception Error, IEnumerator<WorkItem> It)> _placementErrors = [];
            private readonly List<WorkItem> _items = [];
            private readonly SearchState _searchState;
            private readonly CancellationTokenSource _errorCts = new();
            private readonly CancellationTokenSource _mainCts;
            private readonly List<Exception> _errors = [];
            private readonly LinkedList<int> _inFlightIndexes = [];

            // Latches to true once an iteration completes with nothing left to preload, meaning
            // every node touched so far is resident. From that point we skip the RegisterForPreloading
            // scan (its O(items * edgesPerNode) VectorLoaded / TryGetNodeById sweep) and call
            // AfterPreloading directly. The fast path stays correct even if the heuristic is wrong
            // for a future item: GetVectorUnmanagedSpan falls back to a single-vector load on miss,
            // so the worst case is degrading to one cold load instead of a batched one.
            private bool _allVectorsInMemory;

            public bool IsCancelled => _mainCts.IsCancellationRequested;
            

            public void AddInFlight(LinkedListNode<int> node)
            {
                _inFlightIndexes.AddLast(node);
            }

            public void RemoveInFlight(LinkedListNode<int> node)
            {
                _inFlightIndexes.Remove(node);
            }

            // Frozen Apollonius cover Q_u sample. Captured ONCE at runner construction from
            // the set of nodes already known to the search state. All workers read this
            // array but never mutate it, so no concurrency hazard. Pre-resolved vectors are
            // cached in the array — workers never call GetVectorUnmanagedSpan against
            // lazy-loaded nodes whose state could race.
            internal readonly UnmanagedSpan[] GlobalQuerySample;

            // Per-node |v| cache, indexed by node-index (the value stored in
            // NodePlacement._indexes). The same v_i reappears as a candidate across many
            // covers; computing |v_i| in DoWorkApolloniusCover each time is wasted FLOPs
            // because |v| is immutable post-Register. Sentinel 0f = not yet computed.
            // Race-free: all writers compute the same value, 4-byte aligned float writes
            // are atomic on x86/x64. Sized to match EnsureNodesCapacity headroom so
            // lazy-loaded edge targets that get assigned indices during placement also fit.
            internal readonly float[] NodeMagnitudes;

            // Per-node × Q_u dot product cache. Q_u is frozen for the entire build, and the
            // raw dot <v_i, q_k> depends only on v_i and q_k (not on the current pivot u).
            // The Apollonius witness inequality d(v,q) ≤ ρ·d(u,q) ⇔ <v,q> ≥ cutoff[k]·|v|
            // applies the per-u cutoff at lookup time; the cached value is u-independent.
            // Stride = QuDotStride floats per node. Sentinel float.NaN = uncomputed. Float
            // writes are atomic and idempotent (all writers compute the same dot), so
            // unsynchronised read-then-write is safe. Layout is [node-index * stride + k]
            // so reading all q_k for one v_i streams a contiguous Qm-float window.
            internal readonly float[] QuDotCache;
            internal readonly int QuDotStride;

            public NodePlacementRunner(Registration parent, int activeTasksCount, CancellationToken token)
            {
                _mainCts = CancellationTokenSource.CreateLinkedTokenSource(token, _errorCts.Token);
                _activeTasksCount = activeTasksCount;
                _searchState = parent._searchState;

                // Pre-size SearchState._nodes so that AllocateNodeIndex calls during the
                // build never reallocate the underlying ByteString. Workers in
                // PopulateWorkListsOnWorker hold ref Node values into that storage across
                // LLT-side dispatch; a Grow → Release between dispatch and worker access
                // would invalidate those refs. Headroom of 16 K covers edge nodes that get
                // lazily loaded on top of CreatedNodes.
                _searchState.EnsureNodesCapacity(_searchState.CreatedNodes + 16 * 1024);

                int cacheCapacity = _searchState.CreatedNodes + 16 * 1024;
                NodeMagnitudes = new float[cacheCapacity];

                GlobalQuerySample = BuildGlobalQuerySample(_searchState, desired: 32);
                QuDotStride = GlobalQuerySample.Length;
                if (QuDotStride > 0)
                {
                    QuDotCache = new float[(long)cacheCapacity * QuDotStride];
                    Array.Fill(QuDotCache, float.NaN);
                }
                else
                {
                    QuDotCache = Array.Empty<float>();
                }

                for (int i = 0; i < activeTasksCount; i++)
                {
                    Enqueue(new NodePlacement(parent, this).Process().GetEnumerator());
                }
            }

            // Q_u for the Apollonius cover: a uniform random sample of nodes already known
            // to the search state at this point. These nodes either come from prior
            // transactions (durable, fully resident vectors) or are new-batch nodes already
            // Register()'d (VectorId set, vector durably in container). Either way they are
            // safe to read here, on the main runner thread, before any worker dispatch.
            //
            // Lazy-loaded edge targets that appear later during placement get indices
            // ≥ Nodes.Length-now and are excluded from this snapshot.
            private static UnmanagedSpan[] BuildGlobalQuerySample(SearchState state, int desired)
            {
                var nodes = state.Nodes;
                int total = nodes.Length;
                if (total == 0)
                    return [];
                int target = Math.Min(desired, total);
                var result = new UnmanagedSpan[target];
                var rng = new Random(0xA901);
                int got = 0;
                int attempts = 0;
                int maxAttempts = target * 8;
                while (got < target && attempts < maxAttempts)
                {
                    attempts++;
                    int idx = rng.Next(total);
                    ref var n = ref nodes[idx];
                    if (n.VectorId == 0)
                        continue;
                    UnmanagedSpan span;
                    try
                    {
                        span = n.GetVectorUnmanagedSpan(state);
                    }
                    catch
                    {
                        continue;
                    }
                    if (span.Length == 0)
                        continue;
                    result[got++] = span;
                }
                if (got == target)
                    return result;
                var trimmed = new UnmanagedSpan[got];
                Array.Copy(result, trimmed, got);
                return trimmed;
            }

            
            public void Run()
            {
                List<long> batch = [];
                while (true)
                {
                    _ready.Wait();
                    _ready.Reset();
                    
                    while(_placementTasks.TryDequeue(out var it))
                    {
                        if (it.MoveNext())
                        {
                            WorkItem current = it.Current!;
                            current.Iterator = it;
                            _items.Add(current);
                        }
                        else
                        {
                            it.Dispose();
                        }
                    }

                    while (_placementErrors.TryDequeue(out var cur))
                    {
                        HandleError(cur.Error, cur.It);
                    }

                    if (_completed == _activeTasksCount)
                    {
                        if(_errors.Count > 0)
                            throw new AggregateException(_errors);
                        if (_errorCts.IsCancellationRequested == false && _mainCts.IsCancellationRequested)
                        {
                            // If _mainCts is canceled and _errorCts is not, then we need to throw 
                            // to indicate that we're done due to operation cancellation!
                            _mainCts.Token.ThrowIfCancellationRequested();
                        }
                            
                        return; // done
                    }
                    
                    if (_allVectorsInMemory)
                    {
                        // Fast path: every previously touched node is resident, so the bulk preload
                        // scan has nothing to find. Run the LLT-only edge prep here and always
                        // dispatch — the worker's PopulateWorkListsOnWorker fills the visited
                        // bitmap + _indexes/_vectors and decides whether DoWork has anything to
                        // compute.
                        for (int index = 0; index < _items.Count; index++)
                        {
                            WorkItem item = _items[index];
                            item.Owner.PrepareEdgesOnLLT(item.CurrentNodeIndex, item.Level);
                            ThreadPool.UnsafeQueueUserWorkItem(item, preferLocal: false);
                        }

                        _items.Clear();
                        continue;
                    }

                    // we executed all that we could, now let's check if we have
                    // any edges to load that we can do in bulk
                    batch.Clear();
                    for (int index = 0; index < _items.Count; index++)
                    {
                        WorkItem item = _items[index];
                        if (item.RegisterForPreloading(_searchState, batch))
                            continue;

                        // we can run this directly, since there is nothing to preload
                        _items[index] = null; // skip it in the rest of the process
                        item.Owner.PrepareEdgesOnLLT(item.CurrentNodeIndex, item.Level);
                        ThreadPool.UnsafeQueueUserWorkItem(item, preferLocal: false);
                    }

                    var batchSpan = CollectionsMarshal.AsSpan(batch);
                    var used = Sorting.SortAndRemoveDuplicates(batchSpan);
                    if (used > 0)
                    {
                        _searchState.PreloadNodesVectors(batchSpan[..used]);
                    }
                    else
                    {
                        // The whole working set is resident, switch to the fast path on the next iteration.
                        _allVectorsInMemory = true;
                    }

                    foreach (var item in _items)
                    {
                        if (item is null) continue;

                        item.Owner.PrepareEdgesOnLLT(item.CurrentNodeIndex, item.Level);
                        ThreadPool.UnsafeQueueUserWorkItem(item, preferLocal: false);
                    }

                    _items.Clear();
                }
            }
            
            
            private void HandleError(Exception error, IEnumerator<WorkItem> it)
            {
                // force all pending work to stop now, instead of when it is all done
                _errorCts.Cancel();
                _errors.Add(error);
                try
                {
                    it.Dispose();
                }
                catch (Exception e)
                {
                    _errors.Add(e);
                }
            }

            public void Enqueue(IEnumerator<WorkItem> it)
            { 
                _placementTasks.Enqueue(it);
                _ready.Set();
            }

            public void Error(IEnumerator<WorkItem> it, Exception exception)
            {
                _placementErrors.Enqueue((exception, it));
                _ready.Set();
            }

            public void Done()
            {
                _completed++;
            }
            
            public int GetInFlightIndexes(LinkedListNode<int> n, Span<int> buffer)
            {
                var index = 0;
                var cur = n.Previous;
                while(cur != null && index < buffer.Length)
                {
                    buffer[index++] = cur.Value;
                    cur = cur.Previous;
                }

                return index;
            }
        }
        
        private abstract class WorkItem(NodePlacementRunner runner) : IThreadPoolWorkItem
        {
            public NodePlacement Owner;
            public IEnumerator<WorkItem> Iterator;

            protected abstract void DoWork();

            void IThreadPoolWorkItem.Execute()
            {
                try
                {
                    // PopulateWorkListsOnWorker performs the bitmap-visit + per-edge
                    // _indexes/_vectors fill that used to be in AfterPreloading on the LLT
                    // thread. It returns false when there's nothing to compute (all edges
                    // already visited this round) — in which case we skip DoWork and just
                    // re-yield the iterator.
                    if (Owner.PopulateWorkListsOnWorker(CurrentNodeIndex, Level))
                    {
                        DoWork();
                    }

                    runner.Enqueue(Iterator);
                }
                catch (Exception e)
                {
                    runner.Error(Iterator, e);
                }
            }

            public int CurrentNodeIndex;
            public int Level;

            /// <summary>
            /// This scans over all the items that we _want_ to load and check if
            /// their vectors were already loaded. If not, it registers them to be loaded
            /// in a batch manner.
            ///
            /// It may find out that there is no actual work to be done here, in which case
            /// the work item can start immediately.
            /// </summary>
            public bool RegisterForPreloading(SearchState searchState, List<long> batch)
            {
                if (CurrentNodeIndex is -1)
                    return false;
                
                int old = batch.Count;
                ref var n = ref searchState.GetNodeByIndex(CurrentNodeIndex);
                if (n.VectorLoaded is false)
                    batch.Add(n.NodeId);
                
                n.EdgesPerLevel.SetCapacity(searchState.Llt.Allocator, Level + 1);
                n.EdgesIndexesPerLevel.SetCapacity(searchState.Llt.Allocator, Level + 1);

                ref var edgesList = ref n.EdgesPerLevel[Level];
                ref var edgesIndexes = ref n.EdgesIndexesPerLevel[Level];
                // turns out that the checks for the node id -> index are really expensive
                // so we try to cache them
                if (edgesIndexes.Count != edgesList.Count)
                {
                    edgesIndexes.ResetAndEnsureCapacity(searchState.Llt.Allocator, edgesList.Count);
                    for (int i = 0; i < edgesList.Count; i++)
                    {
                        var nodeId = edgesList[i];
                        if (searchState.TryGetNodeById(nodeId, out var nodeIndex))
                        {
                            edgesIndexes.AddUnsafe(nodeIndex);
                            continue;
                        }
                        // we add it to be pre-loaded
                        batch.Add(nodeId);
                        // not that we did NOT add to the edges, so the _next_ time
                        // we run, we'll re-do the whole check and find the node index
                    }
                }
                
                for (int i = 0; i < edgesIndexes.Count; i++)
                {
                    int index = edgesIndexes[i];
                    if (searchState.Nodes[index].VectorLoaded)
                        continue;
                    batch.Add(edgesList[i]);
                }
                return old != batch.Count;
            }
        }
    }
}
