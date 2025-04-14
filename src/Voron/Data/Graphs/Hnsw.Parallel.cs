using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Server.Utils;
using Voron.Data.Containers;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    /*
     * The problem with HNSW is that it is a graph algorithm, which requires
     * that we'll touch signficantly more nodes than we would usually do in a B+Tree, 
     * for example. 
     * 
     * If we need to index 1M items, using a B+Tree, I can sort them and be sure that I 
     * can get pretty good disk access patterns. For HNSW - the problem is that we need to
     * do effectively random I/O for each lookup. Sequential HNSW is running this one node 
     * at a time, which link each node to its nearest neighbors. It is has horrible performance
     * once you exceed the size of memory on the machine.
     * 
     * Adding 1M nodes to a HNSW graph with 15M nodes is _expensive_. Assume that they use 768 dimensions
     * and no quantization. That 15M * 768 * 4 = 42GB of data just for the vectors. And adding a new node
     * means that we need to compare (and thus read, randomly) about 600 vectors. 
     * 
     * Typically, the solution for that is to get a bigger machine, but that is something that we can
     * try to address. This is the purpose of the code in this file. We go through many gymnastics to
     * try to optimize the disk access pattern and parallelize what we can.
     * 
     * Parallelisation is complicated by the fact that we are running under a write transaction scope.
     * A write transaction in Voron is a _single threaded operation_. Another problem is that HNSW is
     * inherently a single-threaded algorith. If I add two nodes to the graph, the second node will 
     * consider the first node as a candidate for its neighbors.
     * 
     * Moving to parallel mode make things more complex. If I add two nodes to the graph at the same time,
     * they will _not_ consider each other for neighbors. Given that HNSW is *approixmate* nearest neighbor
     * alogorithm, this is not too big an issue. We can assume that they will reside "nearby" and that the
     * greedy nature of the algorithm will find the right nodes.
     * But it does show that parallising HNSW *will* impact the resulting graph.
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
     * This is done through guile & trickery, hence this long comment.
     * 
     * To start with, we aren't actually using parallel here to say threads. Instead, we re-wrote
     * the algorithm using async/await. And we run it using a dedicated single threaded scheduler.
     * Whenever we need to do an expensive operation (such as loading vectors, or computing distances),
     * we use an async operation.
     * 
     * That async operation is _not_ scheduled on a different thread. Instead, it is queued until all
     * current operations are completed, then we check what pending work we have and start a batch 
     * load of all the vectors we need. The next step is to run the distance computation using the 
     * thread pool. When that is completed, we can continue with the next step.
     * 
     * The idea is that we run N conrcurrent tasks, where N between 1..MaxConcurrentBatches, and in 
     * each one of them, we pick an item to be inserted to the graph. We then run the HNSW until we
     * need to do an expensive operation (in asynchronous manner). At that point, we yield to *another*
     * such batch. By the time we hit the MaxConcurrentBatches, we gathered enough vectors to load and
     * distances to compute that we can really start pumping through all the items. 
     * 
     * The key here is to batching of I/O for loading the vectors. See the scheduler for handling that 
     * part of the process. Both NodePlacement and NodePlacementScheduler are working very closely 
     * together to achive this work.
     * 
     * # Distance computation using the thread pool
     * 
     * Distnace computation is expensive, and we want to run it in parallel. Each work item that we 
     * send to the thread pool already had its vectors loaded by the batch process, so we can assume 
     * that they are ready in memory. The work item compares a vector to a set of vectors (typically all 
     * the edges of a particular ndoe) and returns the shortest distance or the filtered set of edges.
     * 
     * We use the thread pool because:
     * * There is a known limit to the amount of work we have (up to MaxConcurrentBatches), and it
     *   cannot grow without bound. We won't cause thread pool starvation.
     * * The amount of work for each item is well scoped and _short_. Under 0.5ms for each work item, 
     *   so we won't cause a bottleneck in the thread pool.
     * * We tested using a dedicated thread pool, but those performed significantly worse than the 
     *   default .NET one. 
     */
    public partial class Registration
    {
        private int _nextNodeIndex;

        public int MaxConcurrentBatches = 512;
        void InsertVectorsToGraph(ref ContextBoundNativeList<byte> byteBuffer)
        {
            if (_searchState.TryGetLocationForNode(EntryPointId, out var entryPointNode) is false)
            {
                if (_searchState.CreatedNodesCount == 0)
                    return;

                ref Node startingNode = ref _searchState.Nodes[0];
                Span<byte> span = startingNode.Encode(ref byteBuffer);
                entryPointNode = Container.Allocate(_searchState.Llt, _searchState.Options.Container, span.Length, out Span<byte> allocated);
                span.CopyTo(allocated);
                _searchState.RegisterNodeLocation(EntryPointId, entryPointNode);
            }

            // Run 1..MaxConcurrentBatches batches here, depending on how much work we have to run
            int numberOfBatches = Math.Max(1, _searchState.CreatedNodesCount / MaxConcurrentBatches);
            // but not too much...
            int maxTasks = Math.Min(numberOfBatches, MaxConcurrentBatches);
            NodePlacementScheduler scheduler = new(this, maxTasks);
            scheduler.Run();
        }

        private class NodePlacement(Registration parent, NodePlacementScheduler scheduler)
        {
            private readonly SearchState _searchState = parent._searchState;
            private readonly List<int> _candidates = [];
            private readonly List<int> _nearestIndexes = [];
            private readonly List<int> _indexes = [];
            private readonly List<int> _requiresEdgeFiltering = [];
            private readonly List<UnmanagedSpan> _vectors = [];
            private readonly HashSet<int> _visited = [];
            private readonly PriorityQueue<int, float> _candidatesQ = new();
            private readonly PriorityQueue<int, float> _nearestEdgesQ = new();

            public async Task RunAsync()
            {
                while (true)
                {
                    var currentNodeIndex = parent._nextNodeIndex++;
                    if (currentNodeIndex >= _searchState.CreatedNodesCount)
                        break;

                    await FindGraphPlacementForNode(currentNodeIndex);
                }
            }

            private async Task FindGraphPlacementForNode(int currentNodeIndex)
            {
                var currentMaxLevel = _searchState.Options.CurrentMaxLevel(_searchState.CreatedNodesCount - currentNodeIndex);
                int nodeRandomLevel = GetLevelForNewNode(currentMaxLevel);
                UnmanagedSpan insertedVector;
                {
                    //  scoping n here, to avoid "leaking" the reference and async issues
                    ref var n = ref _searchState.GetNodeByIndex(currentNodeIndex);
                    n.EdgesPerLevel.SetCapacity(_searchState.Llt.Allocator, nodeRandomLevel + 1);
                    insertedVector = n.GetVectorUnmanagedSpan(_searchState);
                }
                await SearchNearestAcrossLevelsAsync(insertedVector, currentMaxLevel);
                for (int level = nodeRandomLevel; level >= 0; level--)
                {
                    int startingPointIndex = _nearestIndexes[level];
                    await NearestEdgesAsync(startingPointIndex, currentNodeIndex, insertedVector, level);
                    ref var node = ref _searchState.GetNodeByIndex(currentNodeIndex);
                    ref var list = ref node.EdgesPerLevel[level];
                    list.ResetAndEnsureCapacity(_searchState.Llt.Allocator, _candidates.Count);
                    _requiresEdgeFiltering.Clear();
                    for (int i = 0; i < _candidates.Count; i++)
                    {
                        int edgeIdx = _candidates[i];
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
                        }

                        await scheduler.Offload(new FilterEdgesHeuristicWorker(this, vector)
                        {
                            CurrentNodeIndex = edgeIdx,
                            Level = level
                        });
                        
                        {
                            ref Node edge = ref _searchState.GetNodeByIndex(edgeIdx);
                            ref var edgeList = ref edge.EdgesPerLevel[level];
                            edgeList.ResetAndEnsureCapacity(_searchState.Llt.Allocator,_indexes.Count);
                            for (int k = 0; k < _indexes.Count; k++)
                            {
                                edgeList.AddUnsafe(_searchState.GetNodeByIndex(_indexes[k]).NodeId);
                            }
                        }
                    }
                }
            }


            private async Task NearestEdgesAsync(int startingPointIndex, int currentNodeIndex, UnmanagedSpan vector, int level)
            {
                Debug.Assert(_candidatesQ.Count == 0, "_candidatesQ.Count == 0");
                Debug.Assert(_nearestEdgesQ.Count == 0, "_nearestEdgesQ.Count == 0");

                float lowerBound = float.MaxValue;
                _visited.Clear();
                _visited.Add(currentNodeIndex); // we can't have an edge to itself
 
                // candidates queue is sorted using the distance, so the lowest distance
                // will always pop first.
                // nearest edges is sorted using _reversed_ distance, so when we add a 
                // new item to the queue, we'll pop the one with the largest distance
                _candidatesQ.Enqueue(startingPointIndex, -lowerBound);

                while (_candidatesQ.TryDequeue(out var cur, out var curDistance))
                {
                    if (-curDistance < lowerBound &&
                        _nearestEdgesQ.Count == _searchState.Options.NumberOfCandidates)
                        break;

                    var worker = new ProcessEdgesWorker(this, vector, lowerBound)
                    {
                        CurrentNodeIndex = cur,
                        Level = level,
                    };
                    await scheduler.Offload(worker);
                    lowerBound = worker.LowerBound;
                }

                _candidatesQ.Clear();
                _candidates.Clear();
                while (_nearestEdgesQ.TryDequeue(out var edgeId, out var d))
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
                        ref var n = ref _searchState.GetNodeByIndex(candidate);
                        _indexes.Add(candidate);
                        _vectors.Add(n.GetVectorUnmanagedSpan(_searchState));
                    }

                    await scheduler.Offload(new FilterEdgesHeuristicWorker(this, vector)
                    {
                        // disable preloading - we already got everything from the 
                        // previous preloading step and are operating purely in memory 
                        CurrentNodeIndex = -1,
                    });
                }
            }

            private record FilterEdgesHeuristicWorker(
                NodePlacement Owner,
                UnmanagedSpan Src) : WorkItem(Owner)
            {
                protected override void Execute()
                {
                    // See: https://icode.best/i/45208840268843 - Chinese, but auto-translate works, and a good explanation with 
                    // conjunction of: https://img-bc.icode.best/20210425010212938.png
                    // See also the paper here: https://arxiv.org/pdf/1603.09320
                    // This implements the Fig. 2 / Algorithm 4

                    var searchState = Owner._searchState;
                    var candidates = Owner._candidates;
                    var vectors = Owner._vectors;
                    var indexes = Owner._nearestIndexes;
                    var queue = Owner._candidatesQ;
                    
                    Debug.Assert(queue.Count is 0);
                    for (int i = 0; i < indexes.Count; i++)
                    {
                        var distance = searchState.Distance(Src, vectors[i]);
                        // note that we use local indexes here!
                        queue.Enqueue(i, distance);
                    }

                    candidates.Clear();

                    while (candidates.Count <= searchState.Options.NumberOfEdges &&
                           queue.TryDequeue(out var cur, out var distance))
                    {
                        bool match = true;
                        for (int i = 0; i < candidates.Count; i++)
                        {
                            int alternativeIndex = candidates[i];
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

            private record ProcessEdgesWorker(NodePlacement Owner, UnmanagedSpan Vector, float LowerBound) : WorkItem(Owner)
            {
                public float LowerBound { get; private set; } = LowerBound;

                protected override void Execute()
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
                   
                        float nextDist = -searchState.Distance(Vector, vectors[i]);
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

            private async Task SearchNearestAcrossLevelsAsync(UnmanagedSpan from, int maxLevel)
            {
                _nearestIndexes.Clear();
                _visited.Clear();
                var currentNodeIndex = _searchState.GetNodeIndexById(EntryPointId);
                var level = maxLevel;
                var distance = float.MaxValue;
                while (level >= 0)
                {
                    do
                    {
                        var worker = new FindNearestWorker(this, from)
                        {
                            CurrentNodeIndex = currentNodeIndex,
                            Level = level
                        };
                        await scheduler.Offload(worker);
                        if (worker.Distance >= distance)
                            break;
                        currentNodeIndex = worker.CurrentNodeIndex;
                        distance = worker.Distance;
                    } while (true);

                    _nearestIndexes.Add(currentNodeIndex);
                    level--;
                }

                _nearestIndexes.Reverse();
            }

            private record FindNearestWorker(NodePlacement Owner,UnmanagedSpan From) : WorkItem(Owner)
            {
                public float Distance = float.MaxValue;

                protected override void Execute()
                {
                    var indexes = Owner._indexes;
                    var vectors = Owner._vectors;
                    var searchState = Owner._searchState;
                    
                    for (var i = 0; i < indexes.Count; i++)
                    {
                        var edgeIdx = indexes[i];
                        var curDist = searchState.Distance(From, vectors[i]);
                        if (curDist >= Distance || double.IsNaN(curDist))
                            continue;
                        Distance = curDist;
                        CurrentNodeIndex = edgeIdx;
                    }
                }
                
            }
            

            private int GetLevelForNewNode(int maxLevel)
            {
                int level = 0;
                while ((parent.Random.Next() & 1) == 0 && // 50% chance 
                       level < maxLevel)
                {
                    level++;
                }

                return level;
            }

            public void AfterPreloading(int currentNodeIndex, int level)
            {
                if (currentNodeIndex is -1)
                    return;
                
                ref var n = ref _searchState.GetNodeByIndex(currentNodeIndex);
                _indexes.Clear();
                _vectors.Clear();
                if (_visited.Add(currentNodeIndex))
                {
                    _indexes.Add(currentNodeIndex);
                    _vectors.Add(n.GetVectorUnmanagedSpan(_searchState));
                }
                foreach (var e in n.EdgesPerLevel[level])
                {
                    int idx = _searchState.GetNodeIndexById(e);
                    if (_visited.Add(idx) is false)
                        continue; // already checked
                    _indexes.Add(idx);
                    ref var edge = ref _searchState.GetNodeByIndex(idx);
                    _vectors.Add(edge.GetVectorUnmanagedSpan(_searchState));
                }
            }
        }

        private class NodePlacementScheduler : TaskScheduler
        {
            private int _completed;
            private readonly BlockingCollection<Task> _tasks = [];
            private readonly List<WorkItem> _items = [];
            private readonly SearchState _searchState;

            public NodePlacementScheduler(Registration parent, int activeTasksCount)
            {
                _searchState = parent._searchState;
                for (int i = 0; i < activeTasksCount; i++)
                {
                    Task.Factory.StartNew(async () =>
                    {
                        try
                        {
                            NodePlacement placement = new(parent, this);
                            await placement.RunAsync();
                        }
                        finally
                        {
                            if (activeTasksCount == ++_completed)
                            {
                                _tasks.CompleteAdding();
                            }
                        }
                    }, CancellationToken.None, TaskCreationOptions.None, this);
                }
            }

            protected override IEnumerable<Task> GetScheduledTasks() => _tasks;

            protected override void QueueTask(Task task)
            {
                _tasks.Add(task);
            }

            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

            public Task Offload(WorkItem item)
            {
                item.Tcs = new TaskCompletionSource();
                _items.Add(item);
                return item.Tcs.Task.ContinueWith(
                    antecedent => antecedent,
                    CancellationToken.None,
                    TaskContinuationOptions.None,
                    // force to complete using this schedule
                    this).Unwrap();
            }
            
            public void Run()
            {
                List<long> batch = [];
                foreach (var task in _tasks.GetConsumingEnumerable())
                {
                    var curTask = task;
                    do
                    {
                        TryExecuteTask(curTask);
                    } while (_tasks.TryTake(out curTask));
                    
                    // we executed all we could, now let's check if we have any edges to load that we can do in bulk
                    batch.Clear();
                    foreach(var item in _items)
                    {
                        item.RegisterForPreloading(_searchState, batch);
                    }
                    var batchSpan = CollectionsMarshal.AsSpan(batch);
                    var used = Sorting.SortAndRemoveDuplicates(batchSpan);
                    _searchState.Preload(batchSpan[..used]);
                    foreach(var item in _items)
                    {
                        item.Owner.AfterPreloading(item.CurrentNodeIndex, item.Level);
                        
                        Task.Factory.StartNew(item.Run,
                                CancellationToken.None,
                                TaskCreationOptions.None,
                                TaskScheduler.Default);
                    }
                    _items.Clear();
                }
            }
        }
        
        private abstract record WorkItem(NodePlacement Owner)
        {
            public TaskCompletionSource Tcs;
            protected abstract void Execute();

            public void Run()
            {
                try
                {
                    Execute();
                    Tcs.TrySetResult();
                }
                catch (Exception e)
                {
                    Tcs.TrySetException(e);
                }
            }

            public int CurrentNodeIndex;
            public int Level;

            public void RegisterForPreloading(SearchState searchState, List<long> batch)
            {
                if (CurrentNodeIndex is -1)
                    return;
                
                ref var n = ref searchState.GetNodeByIndex(CurrentNodeIndex);
                batch.Add(n.NodeId);
                n.EdgesPerLevel.SetCapacity(searchState.Llt.Allocator, Level + 1);
                batch.AddRange(n.EdgesPerLevel[Level].ToSpan());
            }
        }
    }
}
