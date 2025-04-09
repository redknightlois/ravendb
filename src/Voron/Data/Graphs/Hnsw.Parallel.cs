using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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

                        await Workers.Run(new FilterEdgesHeuristicWorker(vector, _edgeIndexes, _indexes, _vectors, _candidatesQ, _searchState));
                        
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

                    var worker = new ProcessEdgesWorker(vector, lowerBound, _indexes, _vectors, _candidatesQ, _nearestEdgesQ, _searchState);
                    await Workers.Run(worker);
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

                    await Workers.Run(new FilterEdgesHeuristicWorker(vector, _candidates, _indexes, _vectors, _candidatesQ, _searchState));
                }
            }

            private record FilterEdgesHeuristicWorker(
                UnmanagedSpan Src, 
                List<int> Candidates, 
                List<int> Indexes, 
                List<UnmanagedSpan> Vectors, 
                PriorityQueue<int, float> Queue,
                SearchState SearchState) : WorkItem(DateTime.UtcNow)
            {
                public override void Execute()
                {
                    // See: https://icode.best/i/45208840268843 - Chinese, but auto-translate works, and a good explanation with 
                    // conjunction of: https://img-bc.icode.best/20210425010212938.png
                    // See also the paper here: https://arxiv.org/pdf/1603.09320
                    // This implements the Fig. 2 / Algorithm 4

                    Debug.Assert(Queue.Count is 0);
                    for (int i = 0; i < Indexes.Count; i++)
                    {
                        var distance = SearchState.Distance(Src, Vectors[i]);
                        // note that we use local indexes here!
                        Queue.Enqueue(i, distance);
                    }

                    Candidates.Clear();

                    while (Candidates.Count <= SearchState.Options.NumberOfEdges &&
                           Queue.TryDequeue(out var cur, out var distance))
                    {
                        bool match = true;
                        for (int i = 0; i < Candidates.Count; i++)
                        {
                            int alternativeIndex = Candidates[i];
                            var curDist = SearchState.Distance(Vectors[cur], Vectors[alternativeIndex]);
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
                            Candidates.Add(cur);
                        }
                    }

                    for (int i = 0; i < Candidates.Count; i++)
                    {
                        // turn the local indexing into a global one
                        Candidates[i] = Indexes[Candidates[i]];
                    }

                    Queue.Clear();
                }

            }

            private record ProcessEdgesWorker(UnmanagedSpan Vector, float InitialLowerBound, List<int> Indexes, List<UnmanagedSpan> Vectors, PriorityQueue<int, float> Candidates,PriorityQueue<int, float> NearestEdges,
                SearchState SearchState) : WorkItem(DateTime.UtcNow)
            {
                public float LowerBound = InitialLowerBound;
                
                public override void Execute()
                {
                    int numberOfCandidates = SearchState.Options.NumberOfCandidates;
                    for (int i = 0; i < Indexes.Count; i++)
                    {
                        var nextIndex = Indexes[i];
                        if (Indexes[i] is -1)
                            continue; // already checked

                        float nextDist = -SearchState.Distance(Vector, Vectors[i]);
                        if (NearestEdges.Count < numberOfCandidates)
                        {
                            Candidates.Enqueue(nextIndex, -nextDist);
                            NearestEdges.Enqueue(nextIndex, nextDist);
                        }
                        else if (LowerBound < nextDist)
                        {
                            Candidates.Enqueue(nextIndex, -nextDist);
                            NearestEdges.EnqueueDequeue(nextIndex, nextDist);
                        }
                        else
                        {
                            continue;
                        }

                        Debug.Assert(Candidates.Count > 0);
                        NearestEdges.TryPeek(out _, out LowerBound);
                    }
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

                        var worker = new FindNearestWorker(from, _indexes, _vectors, _searchState);
                        await Workers.Run(worker);
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

            private record FindNearestWorker(UnmanagedSpan From, List<int> Indexes, List<UnmanagedSpan> Vectors, SearchState SearchState) : WorkItem(DateTime.UtcNow)
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
                Workers.Start();// only happens once

                while (_tasks.IsCompleted is false)
                {
                    if (_tasks.TryTake(out var task))
                    {
                        TryExecuteTask(task);
                        continue;
                    }
                    // we have nothing to do, let's try processing
                    // some work ourselves instead of just waiting
                    if (Workers.Queue.TryTake(out var worker))
                    {
                        Workers.ProcessItem(worker);
                        continue;
                    }

                    try
                    {
                        task = _tasks.Take();
                    }
                    catch (InvalidOperationException)
                    {
                        // task was completed
                        continue; 
                    }
                    TryExecuteTask(task);
                }
            }
        }
    }

    private abstract record WorkItem(DateTime Registered)
    {
        public TaskCompletionSource<object> Tcs;
        public abstract void Execute();
    }

    public static int MaxNumberOfWorkerThreads = Math.Max(1, Environment.ProcessorCount / 4);

    private static class Workers
    {
        public static readonly BlockingCollection<WorkItem> Queue = [];
        private static int WorkersCount;
        private static DateTime AllowThreadCreationAfter = DateTime.MinValue;
        
        public static void Start()
        {
            if (WorkersCount != 0)
                return;
            
            if (Interlocked.CompareExchange(ref WorkersCount, 1, 0) != 0)
                return; // already started;
            
            new Thread(Primary)
            {
                Name = "HNSW.Worker",
                IsBackground = true
            }.Start();
        }

        private static void CreateNewThread()
        {
            if (DateTime.Now < AllowThreadCreationAfter)
                return; // not yet
        
            AllowThreadCreationAfter = DateTime.UtcNow.AddSeconds(5);
            Interlocked.Increment(ref WorkersCount);
            new Thread(Helper)
            {
                Name = "HNSW.Worker",
                IsBackground = true
            }.Start();
        }
        private static void Helper()
        {
            var timeout = TimeSpan.FromSeconds(15);
            while (Queue.TryTake(out var item , timeout))
            {
                ProcessItem((WorkItem)item);
            }
            Interlocked.Decrement(ref WorkersCount);
        }

        public static void ProcessItem(WorkItem item)
        {
            try
            {
                item.Execute();
                item.Tcs.TrySetResult(item);
            }
            catch (Exception e)
            {
                item.Tcs.TrySetException(e);
            }
        }

        private static void Primary()
        {
            var maxDelay = TimeSpan.FromSeconds(2);
            foreach (WorkItem item in Queue.GetConsumingEnumerable())
            {
                DateTime now = DateTime.UtcNow;
                var delay = (item.Registered - now);
                if (delay > maxDelay && WorkersCount < MaxNumberOfWorkerThreads)
                {
                    CreateNewThread();
                }

                ProcessItem(item);
            }
        }

        public static Task Run(WorkItem workItem)
        {
            var tcs = new TaskCompletionSource<object>();
            workItem.Tcs = tcs;
            Queue.Add(workItem);
            return tcs.Task;
        }
    }
}
