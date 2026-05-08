using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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

        // Vamana-lite gate. Set RAVEN_HNSW_TWO_PHASE=1 to dispatch InsertVectorsToGraph
        // to the two-phase build (search-then-link) instead of the wave-based runner.
        // Read once per process so tests can tweak via env without bouncing the server,
        // but a stable gate during a single index reset.
        private static readonly bool UseTwoPhaseBuild =
            string.Equals(Environment.GetEnvironmentVariable("RAVEN_HNSW_TWO_PHASE"), "1",
                StringComparison.Ordinal);

        private void InsertVectorsToGraph(ref ContextBoundNativeList<byte> byteBuffer, CancellationToken token)
        {
            if (_searchState.TryGetLocationForNode(EntryPointId, out var entryPointNode) is false)
            {
                if (_searchState.CreatedNodes is 0)
                    return;

                _nextNodeIndex++; // do not attempt to insert the first node, since it is the graph root
                ref Node startingNode = ref _searchState.Nodes[0];
                _searchState.EnsureEdgesOwned(ref startingNode);
                Span<byte> span = startingNode.Encode(ref byteBuffer);
                var allocatedId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, span.Length, out Span<byte> allocated);
                entryPointNode = (long)allocatedId;
                span.CopyTo(allocated);
                _searchState.RegisterNodeLocation(EntryPointId, entryPointNode);
            }

            int effectiveMaxConcurrentBatches = Math.Max(MaxConcurrentBatches, Math.Min(8192, _searchState.CreatedNodes / _targetPlacementTasks));
            int numberOfBatches = Math.Max(1, _searchState.CreatedNodes / effectiveMaxConcurrentBatches);
            int maxTasks = Math.Min(numberOfBatches, effectiveMaxConcurrentBatches);
            NodePlacementRunner runner = new(this, maxTasks, token);
            if (UseTwoPhaseBuild)
                runner.RunTwoPhase();
            else
                runner.Run();
        }

        // Captures the result of Phase A (parallel search-only) for one node.
        // Phase B walks an array of these in node-index order and applies the
        // mutations on the LLT thread.
        internal struct PlacementPlan
        {
            // -1 means this slot was never populated (Phase A skipped or aborted).
            public int NodeRandomLevel;
            public int CurrentMaxLevel;
            // CandidatesPerLevel[level] = node-indexes selected as out-edges from
            // the new node at that level. Length == NodeRandomLevel + 1.
            // Order matches what the wave path's _candidates list produces (closest-first
            // after .Reverse() in NearestEdges).
            public int[][] CandidatesPerLevel;

            public bool IsValid => NodeRandomLevel >= 0;
        }

        // Captures one (level, existing-node-index) pair whose edge list overflowed M
        // during Phase B back-edge addition. Phase C computes the heuristic-pruned
        // replacement in PrunedEdges (parallel, read-only); Phase D writes it back.
        internal struct OverfullPrune
        {
            public int Level;
            public int EdgeIdx;
            public int[] PrunedEdges; // null until Phase C populates
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

            // Beam width used by the currently-processed level, tapered from Options.NumberOfCandidates.
            private int _effectiveNumberOfCandidates;

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
                    _searchState.EnsureEdgesOwned(ref n);
                    n.EdgesPerLevel.SetCapacity(_searchState.Llt.Allocator, nodeRandomLevel + 1);
                    insertedVector = n.GetVectorUnmanagedSpan(_searchState);
                    AddEdgesFromInFlightNodes(ref n, createdNodeIndex);
                }

                // Beam-width taper during batch construction, applied in the per-level loop below.
                // At level 0 we scale efC down by log2(graphSize+1)/log2(targetSize+1) with a floor
                // of M, so the beam matches the pool of reachable neighbors while the graph is
                // still filling. At upper levels we use efC=M directly: those levels are routing
                // hops, not precision-critical selection, and level 0 still runs with the full
                // (tapered) beam and the heuristic filter.
                int numberOfCandidates = _searchState.Options.NumberOfCandidates;
                int numberOfEdges = _searchState.Options.NumberOfEdges;
                int level0EfC = ComputeTaperedEfConstructionForLevel0(createdNodeIndex, numberOfCandidates, numberOfEdges);

                foreach(var item in SearchNearestAcrossLevels(insertedVector, currentMaxLevel, currentNodeIndex))
                {
                    yield return item;
                }

                for (int level = nodeRandomLevel; level >= 0; level--)
                {
                    _effectiveNumberOfCandidates = level == 0 ? level0EfC : numberOfEdges;
                    int startingPointIndex = _nearestIndexes[level];
                    foreach (var item in NearestEdges(startingPointIndex, currentNodeIndex, insertedVector, level))
                    {
                        yield return item;
                    }
                    PortableExceptions.ThrowIf<InvalidOperationException>(_candidates.Count == 0, "Cannot add a node to the graph without any edges");
                    ref var node = ref _searchState.GetNodeByIndex(currentNodeIndex);
                    _searchState.EnsureEdgesOwned(ref node);
                    ref var list = ref node.EdgesPerLevel[level];
                    // important - we cannot reset here, since we have added edges from the in flight nodes in AddEdgesFromInFlightNodes()
                    list.EnsureCapacityFor(_searchState.Llt.Allocator, _candidates.Count);
                    _requiresEdgeFiltering.Clear();
                    foreach (var edgeIdx in _candidates)
                    {
                        Debug.Assert(edgeIdx != currentNodeIndex);
                        ref Node edge = ref _searchState.GetNodeByIndex(edgeIdx);
                        list.AddUnsafe(edge.NodeId);

                        _searchState.EnsureEdgesOwned(ref edge);
                        ref var edgeList = ref edge.EdgesPerLevel[level];
                        edgeList.Add(_searchState.Llt.Allocator, node.NodeId);

                        // Mirror the append into EdgesIndexesPerLevel so RegisterForPreloading
                        // can skip its O(M) NodeId -> index rebuild. We only update when the
                        // cache is already populated for this level and was in sync before this
                        // append; otherwise we leave the lazy rebuild to fix it.
                        if (edge.EdgesIndexesPerLevel.Count > level)
                        {
                            ref var edgeIndexes = ref edge.EdgesIndexesPerLevel[level];
                            if (edgeIndexes.Count == edgeList.Count - 1)
                                edgeIndexes.Add(_searchState.Llt.Allocator, currentNodeIndex);
                        }

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
                            _searchState.EnsureEdgesOwned(ref edge);
                            ref var edgeList = ref edge.EdgesPerLevel[level];
                            edgeList.ResetAndEnsureCapacity(_searchState.Llt.Allocator, _candidates.Count);
                            foreach (var idx in _candidates)
                            {
                                edgeList.AddUnsafe(_searchState.GetNodeByIndex(idx).NodeId);
                            }

                            // _candidates already holds node indexes, so we can rewrite the
                            // mirrored cache directly without touching the node id table.
                            if (edge.EdgesIndexesPerLevel.Count > level)
                            {
                                ref var edgeIndexes = ref edge.EdgesIndexesPerLevel[level];
                                edgeIndexes.ResetAndEnsureCapacity(_searchState.Llt.Allocator, _candidates.Count);
                                foreach (var idx in _candidates)
                                {
                                    edgeIndexes.AddUnsafe(idx);
                                }
                            }
                        }
                    }
                }
            }

            private int ComputeTaperedEfConstructionForLevel0(int createdNodeIndex, int numberOfCandidates, int numberOfEdges)
            {
                int graphSize = Math.Max(1, createdNodeIndex);
                int targetSize = _searchState.CreatedNodes;
                if (graphSize >= targetSize)
                    return numberOfCandidates;

                double ratio = Math.Log2(graphSize + 1) / Math.Log2(targetSize + 1);
                return Math.Max(numberOfEdges, (int)(numberOfCandidates * ratio));
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
                    int sharedLevels = Math.Min(_searchState.GetLevelCount(ref edge), _searchState.GetLevelCount(ref n));
                    if (sharedLevels > 0)
                    {
                        _searchState.EnsureEdgesOwned(ref n);
                        for (int level = 0; level < sharedLevels; level++)
                        {
                            n.EdgesPerLevel[level].Add(_searchState.Llt.Allocator, edge.NodeId);
                        }
                    }
                }
                _indexes.Clear();
            }

            private IEnumerable<WorkItem> NearestEdges(int startingPointIndex, int currentNodeIndex, UnmanagedSpan vector, int level)
            {
                Debug.Assert(_candidatesQ.Count == 0);
                Debug.Assert(_nearestEdgesQ.Count == 0);
                Debug.Assert(startingPointIndex != currentNodeIndex);
                
                float lowerBound = float.MaxValue;
                ClearVisited();
                MarkVisited(currentNodeIndex); // we can't have an edge to itself
 
                // candidates queue is sorted using the distance, so the lowest distance
                // will always pop first.
                // nearest edges is sorted using _reversed_ distance, so when we add a 
                // new item to the queue, we'll pop the one with the largest distance
                _candidatesQ.Enqueue(startingPointIndex, -lowerBound);

                while (_candidatesQ.TryDequeue(out var cur, out var curDistance))
                {
                    if (-curDistance < lowerBound &&
                        _nearestEdgesQ.Count == _effectiveNumberOfCandidates)
                        break;

                    _processEdgesWorker.Reset(vector, lowerBound, cur, level);
                    yield return _processEdgesWorker;
                    lowerBound = _processEdgesWorker.LowerBound;
                }

                _candidatesQ.Clear();
                _candidates.Clear();
                while (_nearestEdgesQ.TryDequeue(out var edgeId, out var d))
                {
                    _candidates.Add(edgeId);
                }
                _candidates.Reverse();

                if (_candidates.Count <= _searchState.Options.NumberOfEdges) 
                    yield break;
                
                _indexes.Clear();
                _vectors.Clear();
                foreach (var candidate in _candidates)
                {
                    ref var n = ref _searchState.GetNodeByIndex(candidate);
                    _indexes.Add(candidate);
                    _vectors.Add(n.GetVectorUnmanagedSpan(_searchState));
                }

                // disable preloading - we already got everything from the 
                // previous preloading step and are operating purely in memory 
                _filterEdgesWorker.Reset(vector, -1, level);
                yield return _filterEdgesWorker;
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
                    // See: https://icode.best/i/45208840268843 - Chinese, but auto-translate works, and a good explanation with 
                    // conjunction of: https://img-bc.icode.best/20210425010212938.png
                    // See also the paper here: https://arxiv.org/pdf/1603.09320
                    // This implements the Fig. 2 / Algorithm 4

                    var searchState = Owner._searchState;
                    var candidates = Owner._candidates;
                    var vectors = Owner._vectors;
                    var indexes = Owner._indexes;
                    var queue = Owner._candidatesQ;
                    
                    Debug.Assert(queue.Count is 0);
                    for (int i = 0; i < indexes.Count; i++)
                    {
                        var distance = searchState.Distance(_src, vectors[i]);
                        // note that we use local indexes here!
                        queue.Enqueue(i, distance);
                    }

                    candidates.Clear();

                    while (candidates.Count <= searchState.Options.NumberOfEdges &&
                           queue.TryDequeue(out var cur, out var distance))
                    {
                        bool match = true;
                        foreach (var alternativeIndex in candidates)
                        {
                            var curDist = searchState.Distance(vectors[cur], vectors[alternativeIndex]);
                            // there is already an item in the result that is *closer* to the current
                            // node than the target node, so no need to add it
                            if (curDist < distance)
                            {
                                match = false;
                                break;
                            }
                        }

                        if (match)
                        {
                            candidates.Add(cur);
                        }
                    }

                    for (int i = 0; i < candidates.Count; i++)
                    {
                        // turn the local indexing into a global one
                        candidates[i] = indexes[candidates[i]];
                    }

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
                    
                    int numberOfCandidates = Owner._effectiveNumberOfCandidates;
                    for (int i = 0; i < indexes.Count; i++)
                    {
                        var nextIndex = indexes[i];
                        Debug.Assert(searchState.GetLevelCount(ref searchState.Nodes[nextIndex]) > Level);
                   
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

            private IEnumerable<WorkItem> SearchNearestAcrossLevels(UnmanagedSpan from, int maxLevel, int insertedNodeIndex)
            {
                _nearestIndexes.Clear();
                ClearVisited();
                MarkVisited(insertedNodeIndex);
                var currentNodeIndex = _searchState.GetNodeIndexById(EntryPointId);
                var level = maxLevel;
                var distance = float.MaxValue;
                while (level >= 0)
                {
                    do
                    {
                        _findNearestWorker.Reset(from, currentNodeIndex, level);
                        yield return _findNearestWorker;
                        if (_findNearestWorker.Distance >= distance)
                            break;
                        currentNodeIndex = _findNearestWorker.CurrentNodeIndex;
                        distance = _findNearestWorker.Distance;
                    } while (true);

                    _nearestIndexes.Add(currentNodeIndex);
                    level--;
                }

                _nearestIndexes.Reverse();
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
            /// This is called after the Preload() call and we can assume that
            /// all the vectors are now in memory.
            ///
            /// It setups the _indexes/_vectors with the new values, so the call to
            /// WorkItem.Execute() can run without any waiting / hassles.
            ///
            /// This also checks if we have already visited these edges and avoid
            /// running the distance computation if we already did that. 
            /// </summary>
            public bool AfterPreloading(int currentNodeIndex, int level)
            {
                if (currentNodeIndex is -1)
                    return _indexes.Count > 0; // has work

                ref var n = ref _searchState.GetNodeByIndex(currentNodeIndex);
                _indexes.Clear();
                _vectors.Clear();
                if (MarkVisited(currentNodeIndex))
                {
                    _indexes.Add(currentNodeIndex);
                    _vectors.Add(n.GetVectorUnmanagedSpan(_searchState));
                }

                // The slow path runs RegisterForPreloading first, which sizes both lists.
                // The all-in-memory fast path skips that step, so we must guarantee the slot
                // exists before we ref into it. SetCapacity is a no-op when already sized.
                // Skip EdgesPerLevel mutation for cache-loaned nodes - reads route through
                // GetEdgesSpan/GetEdgesCount, which bounds-check via CachedLevelCount.
                if (n.IsFromCache == false)
                    n.EdgesPerLevel.SetCapacity(_searchState.Llt.Allocator, level + 1);
                n.EdgesIndexesPerLevel.SetCapacity(_searchState.Llt.Allocator, level + 1);

                var edgesSpan = _searchState.GetEdgesSpan(ref n, level);
                ref var edgesIndexes = ref n.EdgesIndexesPerLevel[level];
                if (edgesIndexes.Count != edgesSpan.Length)
                {
                    edgesIndexes.ResetAndEnsureCapacity(_searchState.Llt.Allocator, edgesSpan.Length);
                    foreach (var nodeId in edgesSpan)
                    {
                        edgesIndexes.AddUnsafe(_searchState.GetNodeIndexById(nodeId));
                    }
                }
                foreach (var idx in edgesIndexes)
                {
                    if (MarkVisited(idx) is false)
                        continue; // already checked
                    _indexes.Add(idx);
                    ref var edge = ref _searchState.GetNodeByIndex(idx);
                    _vectors.Add(edge.GetVectorUnmanagedSpan(_searchState));
                }

                return _indexes.Count > 0; // has work
            }

            // ----- Vamana-lite Phase A (search-only, parallel, no graph mutation) -----

            // Replicates the per-WorkItem _indexes/_vectors setup that the wave path's
            // RegisterForPreloading + AfterPreloading would do, but **without** invoking
            // any LLT-thread allocator. Phase A worker threads must never allocate
            // through the Voron transaction; this routine only reads. It uses the
            // EdgesIndexesPerLevel cache when it is in sync with EdgesPerLevel, and
            // otherwise resolves NodeIds via TryGetNodeById without rebuilding the cache.
            // (The cache is left stale; Phase B / next batch will rebuild lazily.)
            private void SetupSearchStateAtLevel(int currentNodeIndex, int level)
            {
                if (currentNodeIndex < 0)
                    return;

                ref var n = ref _searchState.GetNodeByIndex(currentNodeIndex);
                _indexes.Clear();
                _vectors.Clear();
                if (MarkVisited(currentNodeIndex))
                {
                    _indexes.Add(currentNodeIndex);
                    _vectors.Add(n.GetVectorUnmanagedSpan(_searchState));
                }

                if (_searchState.GetLevelCount(ref n) <= level)
                    return; // node has no edges at this level — nothing more to add

                var edgesSpan = _searchState.GetEdgesSpan(ref n, level);
                bool cacheReady = n.EdgesIndexesPerLevel.Count > level
                                  && n.EdgesIndexesPerLevel[level].Count == edgesSpan.Length;
                if (cacheReady)
                {
                    ref var edgesIndexes = ref n.EdgesIndexesPerLevel[level];
                    foreach (var idx in edgesIndexes)
                    {
                        if (MarkVisited(idx) is false)
                            continue;
                        _indexes.Add(idx);
                        ref var edge = ref _searchState.GetNodeByIndex(idx);
                        _vectors.Add(edge.GetVectorUnmanagedSpan(_searchState));
                    }
                }
                else
                {
                    for (int i = 0; i < edgesSpan.Length; i++)
                    {
                        if (_searchState.TryGetNodeById(edgesSpan[i], out var idx) is false)
                            continue;
                        if (MarkVisited(idx) is false)
                            continue;
                        _indexes.Add(idx);
                        ref var edge = ref _searchState.GetNodeByIndex(idx);
                        _vectors.Add(edge.GetVectorUnmanagedSpan(_searchState));
                    }
                }
            }

            // Inline equivalent of SearchNearestAcrossLevels. No yields; calls the
            // FindNearest worker's DoWork directly. Populates _nearestIndexes the same way.
            private void SearchNearestAcrossLevelsInline(UnmanagedSpan from, int maxLevel, int insertedNodeIndex)
            {
                _nearestIndexes.Clear();
                ClearVisited();
                MarkVisited(insertedNodeIndex);
                var currentNodeIndex = _searchState.GetNodeIndexById(EntryPointId);
                var level = maxLevel;
                var distance = float.MaxValue;
                while (level >= 0)
                {
                    do
                    {
                        _findNearestWorker.Reset(from, currentNodeIndex, level);
                        SetupSearchStateAtLevel(currentNodeIndex, level);
                        _findNearestWorker.RunInline();
                        if (_findNearestWorker.Distance >= distance)
                            break;
                        currentNodeIndex = _findNearestWorker.CurrentNodeIndex;
                        distance = _findNearestWorker.Distance;
                    } while (true);

                    _nearestIndexes.Add(currentNodeIndex);
                    level--;
                }

                _nearestIndexes.Reverse();
            }

            // Inline equivalent of NearestEdges. Populates _candidates with the level's
            // selected neighbor set (same ordering convention as the yielding version:
            // closest-first after the post-loop Reverse).
            private void NearestEdgesInline(int startingPointIndex, int currentNodeIndex, UnmanagedSpan vector, int level)
            {
                Debug.Assert(_candidatesQ.Count == 0);
                Debug.Assert(_nearestEdgesQ.Count == 0);
                Debug.Assert(startingPointIndex != currentNodeIndex);

                float lowerBound = float.MaxValue;
                ClearVisited();
                MarkVisited(currentNodeIndex);

                _candidatesQ.Enqueue(startingPointIndex, -lowerBound);

                while (_candidatesQ.TryDequeue(out var cur, out var curDistance))
                {
                    if (-curDistance < lowerBound &&
                        _nearestEdgesQ.Count == _effectiveNumberOfCandidates)
                        break;

                    _processEdgesWorker.Reset(vector, lowerBound, cur, level);
                    SetupSearchStateAtLevel(cur, level);
                    _processEdgesWorker.RunInline();
                    lowerBound = _processEdgesWorker.LowerBound;
                }

                _candidatesQ.Clear();
                _candidates.Clear();
                while (_nearestEdgesQ.TryDequeue(out var edgeId, out _))
                {
                    _candidates.Add(edgeId);
                }
                _candidates.Reverse();

                if (_candidates.Count <= _searchState.Options.NumberOfEdges)
                    return;

                _indexes.Clear();
                _vectors.Clear();
                foreach (var candidate in _candidates)
                {
                    ref var n = ref _searchState.GetNodeByIndex(candidate);
                    _indexes.Add(candidate);
                    _vectors.Add(n.GetVectorUnmanagedSpan(_searchState));
                }

                _filterEdgesWorker.Reset(vector, -1, level);
                _filterEdgesWorker.RunInline();
            }

            // Phase A entry: pure search, no graph mutation. Builds a PlacementPlan
            // describing what edges this node would link to at each level. Only reads
            // from _searchState; writes only to per-NodePlacement scratch and to
            // plans[createdNodeIndex].
            internal void ComputePlacementPlanInline(int createdNodeIndex, int currentNodeIndex, out PlacementPlan plan)
            {
                int currentMaxLevel = _searchState.Options.CurrentMaxLevel(_searchState.CreatedNodes - createdNodeIndex);
                int nodeRandomLevel = GetLevelForNewNode(currentMaxLevel);

                plan = new PlacementPlan
                {
                    NodeRandomLevel = nodeRandomLevel,
                    CurrentMaxLevel = currentMaxLevel,
                    CandidatesPerLevel = new int[nodeRandomLevel + 1][]
                };

                UnmanagedSpan insertedVector;
                {
                    ref var n = ref _searchState.GetNodeByIndex(currentNodeIndex);
                    // No EnsureEdgesOwned, no SetCapacity, no AddEdgesFromInFlightNodes —
                    // those are LLT-thread mutations deferred to ApplyPlacementPlan.
                    insertedVector = n.GetVectorUnmanagedSpan(_searchState);
                }

                int numberOfCandidates = _searchState.Options.NumberOfCandidates;
                int numberOfEdges = _searchState.Options.NumberOfEdges;
                int level0EfC = ComputeTaperedEfConstructionForLevel0(createdNodeIndex, numberOfCandidates, numberOfEdges);

                SearchNearestAcrossLevelsInline(insertedVector, currentMaxLevel, currentNodeIndex);

                for (int level = nodeRandomLevel; level >= 0; level--)
                {
                    _effectiveNumberOfCandidates = level == 0 ? level0EfC : numberOfEdges;
                    int startingPointIndex = _nearestIndexes[level];
                    NearestEdgesInline(startingPointIndex, currentNodeIndex, insertedVector, level);

                    if (_candidates.Count == 0)
                        throw new InvalidOperationException(
                            $"Vamana-lite Phase A: empty candidate set for level {level} (createdIdx={createdNodeIndex})");

                    plan.CandidatesPerLevel[level] = _candidates.ToArray();
                }
            }

            // Phase B entry: applies the new node's forward edges and the back-edges
            // to its candidates. Runs on the LLT thread. Pruning of overfull existing
            // nodes is deferred — Phase C computes prunes in parallel, Phase D applies
            // them. Edge lists may temporarily exceed M between Phase B and Phase D;
            // this is fine because nothing reads them during that window (Voron tx is
            // suspended; no concurrent search on this graph state).
            //
            // Each (level, existing-edge-index) that overflowed M is recorded into
            // overfullCollector. Duplicates within a single batch are suppressed via
            // overfullKeys (a HashSet shared across plans by the caller).
            internal void ApplyPlacementPlanDeferredPrune(
                int currentNodeIndex,
                in PlacementPlan plan,
                List<OverfullPrune> overfullCollector,
                HashSet<long> overfullKeys)
            {
                int numberOfEdges = _searchState.Options.NumberOfEdges;
                {
                    ref var n = ref _searchState.GetNodeByIndex(currentNodeIndex);
                    _searchState.EnsureEdgesOwned(ref n);
                    n.EdgesPerLevel.SetCapacity(_searchState.Llt.Allocator, plan.NodeRandomLevel + 1);
                }

                for (int level = plan.NodeRandomLevel; level >= 0; level--)
                {
                    var candidates = plan.CandidatesPerLevel[level];
                    if (candidates is null || candidates.Length == 0)
                        throw new InvalidOperationException("Vamana-lite Phase B: missing candidates for level " + level);

                    ref var node = ref _searchState.GetNodeByIndex(currentNodeIndex);
                    _searchState.EnsureEdgesOwned(ref node);
                    ref var list = ref node.EdgesPerLevel[level];
                    list.EnsureCapacityFor(_searchState.Llt.Allocator, candidates.Length);

                    foreach (var edgeIdx in candidates)
                    {
                        Debug.Assert(edgeIdx != currentNodeIndex);
                        ref Node edge = ref _searchState.GetNodeByIndex(edgeIdx);
                        list.AddUnsafe(edge.NodeId);

                        _searchState.EnsureEdgesOwned(ref edge);
                        // Phase A skipped the search-side-effect SetCapacity that the wave path
                        // performs in RegisterForPreloading (Hnsw.Parallel.cs:1698). For
                        // candidates that have never been touched at this level (e.g. the entry
                        // point in the first batch, or any existing node whose own out-edges
                        // happen to live at lower levels), EdgesPerLevel is unsized here and
                        // an indexer access at [level] would deref a null storage pointer.
                        if (edge.IsFromCache == false)
                            edge.EdgesPerLevel.SetCapacity(_searchState.Llt.Allocator, level + 1);
                        edge.EdgesIndexesPerLevel.SetCapacity(_searchState.Llt.Allocator, level + 1);

                        ref var edgeList = ref edge.EdgesPerLevel[level];
                        edgeList.Add(_searchState.Llt.Allocator, node.NodeId);

                        if (edge.EdgesIndexesPerLevel.Count > level)
                        {
                            ref var edgeIndexes = ref edge.EdgesIndexesPerLevel[level];
                            if (edgeIndexes.Count == edgeList.Count - 1)
                                edgeIndexes.Add(_searchState.Llt.Allocator, currentNodeIndex);
                        }

                        if (edgeList.Count <= numberOfEdges)
                            continue;

                        // Pack (level, edgeIdx) into a long key for cheap dedup.
                        // Keys are valid because both fit in 32 bits (level is small,
                        // edgeIdx is bounded by graph node count which fits in int).
                        long key = ((long)level << 32) | (uint)edgeIdx;
                        if (overfullKeys.Add(key))
                        {
                            overfullCollector.Add(new OverfullPrune { Level = level, EdgeIdx = edgeIdx });
                        }
                    }
                }
            }

            // Phase C entry: computes the heuristic-pruned edge list for one
            // (level, edgeIdx) pair. Pure read over _searchState (no mutation).
            // Each Phase C worker thread has its own NodePlacement scratch state
            // (_indexes/_vectors/_candidates/_visitedBitmap/etc), so independent
            // calls do not race.
            internal void ComputePruneInline(int edgeIdx, int level, out int[] prunedEdges)
            {
                UnmanagedSpan vector;
                {
                    ref Node edge = ref _searchState.GetNodeByIndex(edgeIdx);
                    vector = edge.GetVectorUnmanagedSpan(_searchState);
                    ClearVisited();
                    MarkVisited(edgeIdx);
                }

                SetupSearchStateAtLevel(edgeIdx, level);
                _filterEdgesWorker.Reset(vector, edgeIdx, level);
                _filterEdgesWorker.RunInline();

                if (_candidates.Count == 0)
                    throw new InvalidOperationException(
                        $"Vamana-lite Phase C: empty candidate set after heuristic prune for edgeIdx={edgeIdx}");

                prunedEdges = _candidates.ToArray();
            }

            // Phase D entry: applies one Phase C result. Runs on the LLT thread.
            internal void ApplyPrune(in OverfullPrune prune)
            {
                ref Node edge = ref _searchState.GetNodeByIndex(prune.EdgeIdx);
                _searchState.EnsureEdgesOwned(ref edge);
                ref var edgeList = ref edge.EdgesPerLevel[prune.Level];
                edgeList.ResetAndEnsureCapacity(_searchState.Llt.Allocator, prune.PrunedEdges.Length);
                foreach (var idx in prune.PrunedEdges)
                {
                    edgeList.AddUnsafe(_searchState.GetNodeByIndex(idx).NodeId);
                }
                if (edge.EdgesIndexesPerLevel.Count > prune.Level)
                {
                    ref var edgeIndexes = ref edge.EdgesIndexesPerLevel[prune.Level];
                    edgeIndexes.ResetAndEnsureCapacity(_searchState.Llt.Allocator, prune.PrunedEdges.Length);
                    foreach (var idx in prune.PrunedEdges)
                    {
                        edgeIndexes.AddUnsafe(idx);
                    }
                }
            }

            // Wires the pooled WorkItem instances back to this placement instance.
            // The wave-path Process() iterator does this in its preamble; the inline
            // entry points (RunSearchPhase, ComputePruneInline) need the same setup.
            public void InitWorkers()
            {
                _processEdgesWorker.Owner = this;
                _filterEdgesWorker.Owner = this;
                _findNearestWorker.Owner = this;
            }

            // Phase A worker entry: each worker thread calls this. Picks up next
            // unassigned node-index via Interlocked.Increment(&_nextNodeIndex) and
            // computes its placement plan into plans[idx]. Returns when the
            // CreatedNodes range is exhausted or runner is cancelled.
            public void RunSearchPhase(PlacementPlan[] plans)
            {
                InitWorkers();

                int createdNodesLength = _searchState.CreatedNodes;
                while (true)
                {
                    if (runner.IsCancelled)
                        return;

                    int createdNodeIndex = Interlocked.Increment(ref parent._nextNodeIndex) - 1;
                    if (createdNodeIndex >= createdNodesLength)
                        return;

                    var currentNodeIndex = _searchState.GetCreatedNodeIndex(createdNodeIndex);
                    ComputePlacementPlanInline(createdNodeIndex, currentNodeIndex, out plans[createdNodeIndex]);
                }
            }
        }

        /// <summary>
        /// This works opposite to how you'll usually think about such runners.
        /// It is running everything in a _single_ threaded (because it uses the single threaded transaction)
        /// and offload computational work to the thread pool, this is done using the NodePlacement yielding
        /// whenever it wants to offload a computation, and the runner is then taking care of running the code,
        /// 
        /// </summary>
        private const int DispatchBatchK = 8; // items per ThreadPool dispatch (coalescing factor)

        private sealed class BatchExecutor : IThreadPoolWorkItem
        {
            public readonly WorkItem[] Items = new WorkItem[DispatchBatchK];
            public int Count;

            public void Execute()
            {
                int n = Count;
                if (n == 0)
                    return;
                NodePlacementRunner runner = Items[0].Runner;
                for (int i = 0; i < n; i++)
                {
                    Items[i].ExecuteDeferred(); // no _ready.Set per item
                    Items[i] = null;
                }
                Count = 0;
                runner.SignalReady(); // single signal for the whole batch
            }
        }

        private class NodePlacementRunner
        {
            // Instrumentation: dispatch volume per Run() invocation.
            private long _waveCount;
            private long _dispatchCount;          // total UnsafeQueueUserWorkItem calls (BATCH dispatches)
            private long _itemsCount;             // total WorkItems handed off (>= _dispatchCount with batching)
            private long _waveItemsMin = long.MaxValue;
            private long _waveItemsMax;
            private long _waveItemsSum;
            private long _waveTicksSum;           // total wall ticks across all waves (excluding initial Wait)
            private long _preloadDispatches;      // batch dispatches via slow path (with bulk preload)
            private long _fastPathDispatches;     // batch dispatches via _allVectorsInMemory fast path
            private long _reEnqueues;             // items re-enqueued (no work after preloading)
            private long _itemsCoalesced;         // items packed into batches

            // Pending batch buffer used while emitting a wave. Allocated lazily; reused across waves.
            private BatchExecutor _pendingBatch;

            // Stored to let RunTwoPhase spawn fresh NodePlacement instances without
            // disturbing the wave-path constructor's iterator pre-queue.
            private readonly Registration _parent;

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

            public NodePlacementRunner(Registration parent, int activeTasksCount, CancellationToken token)
            {
                _mainCts = CancellationTokenSource.CreateLinkedTokenSource(token, _errorCts.Token);
                _parent = parent;
                _activeTasksCount = activeTasksCount;
                _searchState = parent._searchState;
                for (int i = 0; i < activeTasksCount; i++)
                {
                    Enqueue(new NodePlacement(parent, this).Process().GetEnumerator());
                }
            }

            // Vamana-lite entry point. Runs Phase A (parallel search-only) on
            // _activeTasksCount worker threads, each owning its own NodePlacement
            // scratch state. Phase B (serial apply) runs on the calling thread,
            // which is the LLT thread (Voron tx writes are thread-affine).
            //
            // The wave-path constructor pre-queued _activeTasksCount iterators that
            // we won't drive; drain and dispose them here to avoid resource leaks.
            public void RunTwoPhase()
            {
                while (_placementTasks.TryDequeue(out var staleIt))
                    staleIt.Dispose();

                var totalSw = System.Diagnostics.Stopwatch.StartNew();

                int createdNodes = _searchState.CreatedNodes;
                if (createdNodes == 0)
                    return;

                // Phase A worker count. _activeTasksCount is the L3-aware concurrency
                // cap from ComputeTargetPlacementTasks; for very large batches it can be
                // ~hundreds, which oversubscribes the 32-core machine and just thrashes.
                // Phase A is purely CPU-bound (search + heuristic prune), so cap at the
                // physical processor count.
                int phaseAWorkers = Math.Min(_activeTasksCount, Environment.ProcessorCount);

                // ---- Bulk preload (LLT thread) ----
                // Voron's LowLevelTransaction is thread-affine. The wave path drip-feeds
                // PreloadNodesVectors as work surfaces during the iterator advance.
                // Phase A workers cannot call any LLT-touching path (Container.Get,
                // Container.GetAll, page allocation), so we resolve every vector this
                // batch will touch up-front, here on the LLT thread. After this call,
                // every CreatedNode's VectorLoaded == true and GetVectorUnmanagedSpan
                // returns a cached pointer with no Voron-tx interaction.
                var preloadSw = System.Diagnostics.Stopwatch.StartNew();
                {
                    var ids = new long[createdNodes];
                    for (int i = 0; i < createdNodes; i++)
                    {
                        int nodeIdx = _searchState.GetCreatedNodeIndex(i);
                        ids[i] = _searchState.Nodes[nodeIdx].NodeId;
                    }
                    var idsSpan = ids.AsSpan();
                    Sorting.SortAndRemoveDuplicates(idsSpan);
                    _searchState.PreloadNodesVectors(idsSpan);
                }
                long preloadMs = preloadSw.ElapsedMilliseconds;

                var phaseASw = System.Diagnostics.Stopwatch.StartNew();
                var plans = new PlacementPlan[createdNodes];
                for (int i = 0; i < plans.Length; i++)
                    plans[i].NodeRandomLevel = -1;

                var phaseAErrors = new ConcurrentQueue<Exception>();
                var threads = new Thread[phaseAWorkers];
                for (int t = 0; t < phaseAWorkers; t++)
                {
                    threads[t] = new Thread(() =>
                    {
                        try
                        {
                            new NodePlacement(_parent, this).RunSearchPhase(plans);
                        }
                        catch (Exception ex)
                        {
                            phaseAErrors.Enqueue(ex);
                            _errorCts.Cancel();
                        }
                    })
                    {
                        IsBackground = true,
                        Name = "Hnsw.RunTwoPhase.PhaseA"
                    };
                    threads[t].Start();
                }
                foreach (var th in threads)
                    th.Join();

                if (phaseAErrors.IsEmpty is false)
                {
                    var collected = new List<Exception>();
                    while (phaseAErrors.TryDequeue(out var e)) collected.Add(e);
                    throw new AggregateException(collected);
                }

                if (_errorCts.IsCancellationRequested == false && _mainCts.IsCancellationRequested)
                    _mainCts.Token.ThrowIfCancellationRequested();

                long phaseAMs = phaseASw.ElapsedMilliseconds;

                // ---- Phase B: serial apply forward + back edges (LLT thread) ----
                var phaseBSw = System.Diagnostics.Stopwatch.StartNew();
                var applyPlacement = new NodePlacement(_parent, this);
                var overfullList = new List<OverfullPrune>();
                var overfullKeys = new HashSet<long>();
                int appliedCount = 0;
                for (int idx = 0; idx < createdNodes; idx++)
                {
                    if (_mainCts.IsCancellationRequested)
                        _mainCts.Token.ThrowIfCancellationRequested();

                    if (plans[idx].NodeRandomLevel < 0)
                        continue;

                    int currentNodeIndex = _searchState.GetCreatedNodeIndex(idx);
                    applyPlacement.ApplyPlacementPlanDeferredPrune(
                        currentNodeIndex, in plans[idx], overfullList, overfullKeys);
                    appliedCount++;
                    plans[idx].CandidatesPerLevel = null; // release for GC promptly
                }
                long phaseBMs = phaseBSw.ElapsedMilliseconds;
                int overfullCount = overfullList.Count;

                // ---- Phase C: parallel prune compute (worker threads, read-only) ----
                // The graph is in an over-provisioned state — some nodes have edge
                // lists temporarily exceeding M. No one reads them during this window
                // (the LLT thread is sitting in the parallel-for join), so reading the
                // current edge list per node is safe across threads.
                var phaseCSw = System.Diagnostics.Stopwatch.StartNew();
                long phaseCMs = 0;
                if (overfullCount > 0)
                {
                    var overfullArr = overfullList.ToArray();
                    int phaseCWorkers = Math.Min(phaseAWorkers, overfullCount);
                    var phaseCErrors = new ConcurrentQueue<Exception>();
                    long nextOverfull = -1;
                    var cThreads = new Thread[phaseCWorkers];
                    for (int t = 0; t < phaseCWorkers; t++)
                    {
                        cThreads[t] = new Thread(() =>
                        {
                            try
                            {
                                var p = new NodePlacement(_parent, this);
                                p.InitWorkers();
                                while (true)
                                {
                                    long i = Interlocked.Increment(ref nextOverfull);
                                    if (i >= overfullArr.Length) return;
                                    p.ComputePruneInline(
                                        overfullArr[i].EdgeIdx,
                                        overfullArr[i].Level,
                                        out overfullArr[i].PrunedEdges);
                                }
                            }
                            catch (Exception ex)
                            {
                                phaseCErrors.Enqueue(ex);
                                _errorCts.Cancel();
                            }
                        })
                        {
                            IsBackground = true,
                            Name = "Hnsw.RunTwoPhase.PhaseC"
                        };
                        cThreads[t].Start();
                    }
                    foreach (var th in cThreads) th.Join();

                    if (phaseCErrors.IsEmpty is false)
                    {
                        var collected = new List<Exception>();
                        while (phaseCErrors.TryDequeue(out var e)) collected.Add(e);
                        throw new AggregateException(collected);
                    }
                    phaseCMs = phaseCSw.ElapsedMilliseconds;

                    // ---- Phase D: serial apply prune writes (LLT thread) ----
                    var phaseDSw = System.Diagnostics.Stopwatch.StartNew();
                    foreach (ref var prune in overfullArr.AsSpan())
                    {
                        applyPlacement.ApplyPrune(in prune);
                    }
                    _ = phaseDSw.ElapsedMilliseconds;
                }

                // Suppress unused-variable warnings on instrumentation kept for future telemetry.
                _ = preloadMs; _ = phaseAMs; _ = phaseBMs; _ = phaseCMs; _ = totalSw.ElapsedMilliseconds;
            }

            
            public void Run()
            {
                var totalSw = System.Diagnostics.Stopwatch.StartNew();
                List<long> batch = [];
                while (true)
                {
                    _ready.Wait();
                    _ready.Reset();
                    var waveSw = System.Diagnostics.Stopwatch.StartNew();

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
                        EmitRunSummary(totalSw.ElapsedMilliseconds);
                        return; // done
                    }
                    
                    if (_allVectorsInMemory)
                    {
                        // Fast path: every previously touched node is resident, so the bulk preload
                        // scan has nothing to find. Dispatch each item directly through AfterPreloading
                        // and skip RegisterForPreloading entirely.
                        long fastDispatchedThisWave = 0;
                        bool anyReenqueue = false;
                        for (int index = 0; index < _items.Count; index++)
                        {
                            WorkItem item = _items[index];
                            if (item.Owner.AfterPreloading(item.CurrentNodeIndex, item.Level))
                            {
                                DispatchBatched(item, ref fastDispatchedThisWave);
                            }
                            else
                            {
                                EnqueueDeferred(item.Iterator);
                                _reEnqueues++;
                                anyReenqueue = true;
                            }
                        }
                        FlushPendingBatch(ref fastDispatchedThisWave);
                        if (anyReenqueue)
                            SignalReady();
                        _fastPathDispatches += fastDispatchedThisWave;
                        AccountWave(fastDispatchedThisWave, waveSw.ElapsedTicks);

                        _items.Clear();
                        continue;
                    }

                    // we executed all that we could, now let's check if we have
                    // any edges to load that we can do in bulk
                    batch.Clear();
                    long slowDispatchedThisWave = 0;
                    bool anyReenqueueSlow = false;
                    for (int index = 0; index < _items.Count; index++)
                    {
                        WorkItem item = _items[index];
                        if (item.RegisterForPreloading(_searchState, batch))
                            continue;

                        // we can run this directly, since there is nothing to preload

                        _items[index] = null; // skip it in the rest of the process
                        if (item.Owner.AfterPreloading(item.CurrentNodeIndex, item.Level) is false)
                        {
                            EnqueueDeferred(item.Iterator);
                            _reEnqueues++;
                            anyReenqueueSlow = true;
                            continue; // no work to do, everything was already visited
                        }

                        DispatchBatched(item, ref slowDispatchedThisWave);
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

                        if (item.Owner.AfterPreloading(item.CurrentNodeIndex, item.Level))
                        {
                            DispatchBatched(item, ref slowDispatchedThisWave);
                        }
                        else
                        {
                            // this means that there is no work to do (all the nodes were already visited)
                            // so we can re-schedule this immediately
                            EnqueueDeferred(item.Iterator);
                            _reEnqueues++;
                            anyReenqueueSlow = true;
                        }
                    }
                    FlushPendingBatch(ref slowDispatchedThisWave);
                    if (anyReenqueueSlow)
                        SignalReady();
                    _preloadDispatches += slowDispatchedThisWave;
                    AccountWave(slowDispatchedThisWave, waveSw.ElapsedTicks);

                    _items.Clear();
                }
            }

            // Adds an item to the pending batch buffer; flushes (dispatches) when full.
            // Returns the running per-wave dispatch count delta.
            private void DispatchBatched(WorkItem item, ref long batchDispatchesThisWave)
            {
                _itemsCoalesced++;
                var pending = _pendingBatch ??= new BatchExecutor();
                pending.Items[pending.Count++] = item;
                if (pending.Count >= DispatchBatchK)
                {
                    ThreadPool.UnsafeQueueUserWorkItem(pending, preferLocal: false);
                    batchDispatchesThisWave++;
                    _pendingBatch = new BatchExecutor();
                }
            }

            // Flush any partial batch at the end of the wave.
            private void FlushPendingBatch(ref long batchDispatchesThisWave)
            {
                var pending = _pendingBatch;
                if (pending != null && pending.Count > 0)
                {
                    ThreadPool.UnsafeQueueUserWorkItem(pending, preferLocal: false);
                    batchDispatchesThisWave++;
                    _pendingBatch = null;
                }
            }

            private void AccountWave(long batchDispatches, long elapsedTicks)
            {
                _waveCount++;
                _dispatchCount += batchDispatches;
                if (batchDispatches < _waveItemsMin) _waveItemsMin = batchDispatches;
                if (batchDispatches > _waveItemsMax) _waveItemsMax = batchDispatches;
                _waveItemsSum += batchDispatches;
                _waveTicksSum += elapsedTicks;
            }

            private void EmitRunSummary(long totalMs)
            {
                if (_waveCount == 0)
                    return;
                long minBatches = _waveItemsMin == long.MaxValue ? 0 : _waveItemsMin;
                double avgBatches = (double)_waveItemsSum / _waveCount;
                double avgWaveMs = _waveTicksSum / (double)System.Diagnostics.Stopwatch.Frequency * 1000.0 / _waveCount;
                double itemsPerBatch = _dispatchCount == 0 ? 0 : (double)_itemsCoalesced / _dispatchCount;
                Console.WriteLine(
                    $"[HNSW.Run] tasks={_activeTasksCount} K={DispatchBatchK} waves={_waveCount} " +
                    $"batchDispatches={_dispatchCount} items={_itemsCoalesced} avgItemsPerBatch={itemsPerBatch:F2} " +
                    $"fast={_fastPathDispatches} slow={_preloadDispatches} reenq={_reEnqueues} " +
                    $"batches/wave min={minBatches} avg={avgBatches:F1} max={_waveItemsMax} " +
                    $"wave_avg_ms={avgWaveMs:F2} run_ms={totalMs}");
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

            // Variant for callers that will batch multiple enqueues and then call SignalReady() once.
            // Avoids redundant _ready.Set() calls (each Set has a memory barrier + allocation cost).
            public void EnqueueDeferred(IEnumerator<WorkItem> it)
            {
                _placementTasks.Enqueue(it);
            }

            public void ErrorDeferred(IEnumerator<WorkItem> it, Exception exception)
            {
                _placementErrors.Enqueue((exception, it));
            }

            public void SignalReady() => _ready.Set();

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
            public NodePlacementRunner Runner => runner;

            protected abstract void DoWork();

            // Inline entry point used by Vamana-lite Phase A (RunTwoPhase). The caller
            // owns iteration and dispatch; this just forwards to DoWork without going
            // through the ThreadPool, the iterator queue, or the _ready signal.
            internal void RunInline() => DoWork();

            // Used by BatchExecutor: runs work and enqueues result without signaling _ready
            // (BatchExecutor signals once after the whole batch).
            internal void ExecuteDeferred()
            {
                try
                {
                    DoWork();
                    runner.EnqueueDeferred(Iterator);
                }
                catch (Exception e)
                {
                    runner.ErrorDeferred(Iterator, e);
                }
            }

            void IThreadPoolWorkItem.Execute()
            {
                try
                {
                    DoWork();
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
                
                if (n.IsFromCache == false)
                    n.EdgesPerLevel.SetCapacity(searchState.Llt.Allocator, Level + 1);
                n.EdgesIndexesPerLevel.SetCapacity(searchState.Llt.Allocator, Level + 1);

                var edgesSpan = searchState.GetEdgesSpan(ref n, Level);
                ref var edgesIndexes = ref n.EdgesIndexesPerLevel[Level];
                // turns out that the checks for the node id -> index are really expensive
                // so we try to cache them
                if (edgesIndexes.Count != edgesSpan.Length)
                {
                    edgesIndexes.ResetAndEnsureCapacity(searchState.Llt.Allocator, edgesSpan.Length);
                    for (int i = 0; i < edgesSpan.Length; i++)
                    {
                        var nodeId = edgesSpan[i];
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
                    batch.Add(edgesSpan[i]);
                }
                return old != batch.Count;
            }
        }
    }
}
