using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
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

            int maxTasks = Math.Min(Math.Max(1, _searchState.CreatedNodesCount / 256), 256);
            NodePlacementScheduler scheduler = new(this, maxTasks);
            scheduler.Run();
        }

        private class NodePlacement(Registration parent)
        {
            private readonly SearchState _searchState = parent._searchState;
            private readonly List<int> _candidates = [];
            private readonly List<int> _nearestIndexes = [];
            private readonly List<int> _indexes = [];
            private readonly List<int> _requiresEdgeFiltering = [];
            private readonly List<int> _edgeIndexes = [];
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

                        await ExecuteElsewhere(() =>
                        {
                            FilterEdgesHeuristic(vector, _edgeIndexes, _indexes, _vectors);
                            return 1; // unused
                        });
                        {
                            ref Node edge = ref _searchState.GetNodeByIndex(edgeIdx);
                            ref var edgeList = ref edge.EdgesPerLevel[level];
                            edgeList.ResetAndEnsureCapacity(_searchState.Llt.Allocator,_edgeIndexes.Count);
                            for (int k = 0; k < _edgeIndexes.Count; k++)
                            {
                                edgeList.AddUnsafe(_searchState.GetNodeByIndex(_edgeIndexes[k]).NodeId);
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

                    lowerBound = await ExecuteElsewhere(() => ProcessEdges(vector, lowerBound));
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

                    await ExecuteElsewhere(() =>
                    {
                        FilterEdgesHeuristic(vector, _candidates, _indexes, _vectors);
                        return 0; // unused
                    });
                }
            }


            private void FilterEdgesHeuristic(UnmanagedSpan src, List<int> candidates, List<int> indexes, List<UnmanagedSpan> vectors)
            {
                // See: https://icode.best/i/45208840268843 - Chinese, but auto-translate works, and a good explanation with 
                // conjunction of: https://img-bc.icode.best/20210425010212938.png
                // See also the paper here: https://arxiv.org/pdf/1603.09320
                // This implements the Fig. 2 / Algorithm 4

                Debug.Assert(_candidatesQ.Count is 0);
                for (int i = 0; i < indexes.Count; i++)
                {
                    var distance = _searchState.Distance(src, vectors[i]);
                    // note that we use local indexes here!
                    _candidatesQ.Enqueue(i, distance);
                }

                candidates.Clear();

                while (candidates.Count <= _searchState.Options.NumberOfEdges &&
                       _candidatesQ.TryDequeue(out var cur, out var distance))
                {
                    bool match = true;
                    for (int i = 0; i < candidates.Count; i++)
                    {
                        int alternativeIndex = candidates[i];
                        var curDist = _searchState.Distance(vectors[cur], vectors[alternativeIndex]);
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

                _candidatesQ.Clear();
            }

            private float ProcessEdges(UnmanagedSpan vector, float lowerBound)
            {
                int numberOfCandidates = _searchState.Options.NumberOfCandidates;
                for (int i = 0; i < _indexes.Count; i++)
                {
                    var nextIndex = _indexes[i];
                    if (_indexes[i] is -1)
                        continue; // already checked

                    float nextDist = -_searchState.Distance(vector, _vectors[i]);
                    if (_nearestEdgesQ.Count < numberOfCandidates)
                    {
                        _candidatesQ.Enqueue(nextIndex, -nextDist);
                        _nearestEdgesQ.Enqueue(nextIndex, nextDist);
                    }
                    else if (lowerBound < nextDist)
                    {
                        _candidatesQ.Enqueue(nextIndex, -nextDist);
                        _nearestEdgesQ.EnqueueDequeue(nextIndex, nextDist);
                    }
                    else
                    {
                        continue;
                    }

                    Debug.Assert(_candidatesQ.Count > 0);
                    _nearestEdgesQ.TryPeek(out _, out lowerBound);
                }

                return lowerBound;
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

                        var (shortestDistance, closestNode) = await ExecuteElsewhere(() => FindNearest(from));
                        if (shortestDistance >= distance)
                            break;
                        currentNodeIndex = closestNode;
                        distance = shortestDistance;
                    } while (true);

                    _nearestIndexes.Add(currentNodeIndex);
                    level--;
                }

                _nearestIndexes.Reverse();
            }

            private (float distance, int currentNodeIndex) FindNearest(UnmanagedSpan from)
            {
                float distance = float.MaxValue;
                int closestNodeIndex = -1;
                for (var i = 0; i < _indexes.Count; i++)
                {
                    var edgeIdx = _indexes[i];
                    if (edgeIdx is -1)
                        continue;
                    var curDist = _searchState.Distance(from, _vectors[i]);
                    if (curDist >= distance || double.IsNaN(curDist))
                        continue;
                    distance = curDist;
                    closestNodeIndex = edgeIdx;
                }

                return (distance, closestNodeIndex);
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

        private static Task<TResult> ExecuteElsewhere<TResult>(Func<TResult> func)
        {
            return Task.Factory.StartNew(func,
                CancellationToken.None, TaskCreationOptions.None,
                TaskScheduler.Default);
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
                            NodePlacement placement = new(parent);
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

            public void Run()
            {
                foreach (var task in _tasks.GetConsumingEnumerable())
                {
                    TryExecuteTask(task);
                }
            }

            public void Done()
            {
                _tasks.CompleteAdding();
            }
        }
    }
}

