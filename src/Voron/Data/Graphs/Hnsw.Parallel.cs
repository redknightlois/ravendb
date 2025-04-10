using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Sparrow;
using Voron.Data.Containers;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
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
                    list.EnsureCapacityFor(_searchState.Llt.Allocator, _candidates.Count);
                    list.Clear();
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
                            ref var edgeList = ref edge.EdgesPerLevel[level];
                            _indexes.Clear();
                            _indexes.EnsureCapacity(edgeList.Count);
                            _vectors.Clear();
                            _vectors.EnsureCapacity(edgeList.Count);
                            for (int k = 0; k < edgeList.Count; k++)
                            {
                                long nodeId = edgeList[k];
                                int nodeIdx = _searchState.GetNodeIndexById(nodeId);
                                _indexes.Add(nodeIdx);
                                _vectors.Add(_searchState.GetNodeByIndex(nodeIdx).GetVectorUnmanagedSpan(_searchState));
                            }

                            vector = edge.GetVectorUnmanagedSpan(_searchState);
                        }

                        await scheduler.Offload(new FilterEdgesHeuristicWorker(this, vector));
                        
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

                    _searchState.GetEdges(cur, level, _indexes, _vectors);
                    for (int i = 0; i < _indexes.Count; i++)
                    {
                        var nextIndex = _indexes[i];
                        if (_visited.Add(nextIndex))
                            continue;
                        _indexes[i] = -1; // no need to check
                    }

                    var worker = new ProcessEdgesWorker(this, vector, lowerBound);
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
                    for (int i = 0; i < _candidates.Count; i++)
                    {
                        _indexes.Add(_candidates[i]);
                        _vectors.Add(_searchState.GetNodeByIndex(_candidates[i]).GetVectorUnmanagedSpan(_searchState));
                    }

                    await scheduler.Offload(new FilterEdgesHeuristicWorker(this, vector));
                }
            }

            private record FilterEdgesHeuristicWorker(
                NodePlacement Owner,
                UnmanagedSpan Src) : WorkItem(Owner)
            {
                public override void Execute()
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
                
                public override void Execute()
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
                        if (indexes[i] is -1)
                            continue; // already checked

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
                        _searchState.GetEdges(currentNodeIndex, level, _indexes, _vectors);
                        for (var i = 0; i < _indexes.Count; i++)
                        {
                            if (_visited.Add(_indexes[i]))
                                continue;
                            // already seen, should skip it
                            _indexes[i] = -1;
                        }

                        var worker = new FindNearestWorker(this, from, _indexes, _vectors, _searchState);
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

            private record FindNearestWorker(NodePlacement Owner,UnmanagedSpan From, List<int> Indexes, List<UnmanagedSpan> Vectors, SearchState SearchState) : WorkItem(Owner)
            {
                public float Distance = float.MaxValue;
                public int CurrentNodeIndex = -1;
                
                public override void Execute()
                {
                    for (var i = 0; i < Indexes.Count; i++)
                    {
                        var edgeIdx = Indexes[i];
                        if (edgeIdx is -1)
                            continue;
                        var curDist = SearchState.Distance(From, Vectors[i]);
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
        }

        private class NodePlacementScheduler : TaskScheduler
        {
            private int _completed;
            private readonly BlockingCollection<Task> _tasks = [];

            public NodePlacementScheduler(Registration parent, int activeTasksCount)
            {
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
                return Task.Factory.StartNew(item.Execute, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
            }
            
            public void Run()
            {
                foreach (var task in _tasks.GetConsumingEnumerable())
                {
                    var item = task;
                    do
                    {
                        TryExecuteTask(item);
                    } while (_tasks.TryTake(out item));
                }
            }
        }
        
        private abstract record WorkItem(NodePlacement Owner)
        {
            public abstract void Execute();
        }
    }
}
