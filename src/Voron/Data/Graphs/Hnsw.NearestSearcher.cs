using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Data.RoaringBitmaps;
using Voron.Global;
using Voron.Util;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    /// <summary>
    /// The working set of a filtered traversal: the ball expected to hold <paramref name="numberOfCandidates"/> of the filter's nodes, with the filter's entry
    /// count <paramref name="filterCount"/> standing in for its node count. Saturates at the graph's size. Entries that share a node or have none make the
    /// ball too small. A traversal that can list the filter's nodes stops once it has read as many vectors as the filter has entries and scans those nodes
    /// instead, so a filter whose entries reach fewer than <paramref name="numberOfCandidates"/> nodes costs a pass over its entries, not a read of every
    /// reachable node.
    /// </summary>
    public static int FilteredBeam(int numberOfCandidates, long filterCount, long countOfVectors) =>
        (int)Math.Min(Math.Min(countOfVectors, int.MaxValue), Math.Max(numberOfCandidates, (long)Math.Ceiling((double)numberOfCandidates * countOfVectors / Math.Max(1, filterCount))));

    public partial class SearchState
    {
        private class NearestSearcher : IHnswSearcher
        {
            private readonly SearchState _searchState;
            private readonly Memory<byte> _vector;
            private readonly int _level;
            private readonly int _numberOfCandidates;

            // The working set size: the requested candidates, or the filtered beam when a filter admits the results.
            private readonly int _internalNumberOfCandidates;

            private ContextBoundNativeList<int> _candidates;
            private readonly NearestEdgesFlags _flags;
            private NativeList<int> _indexes;
            private NativeList<long> _nodeIds;
            private NativeList<nint> _rows;
            private long _vectorReadCounter = 0;
            private readonly int _startingPointIndex;
            // Owned by the SearchState; null when the graph carries no codes.
            private readonly CodeScreen _screen;
            // With a filter, the results are the nearest of the filter's nodes that the traversal reaches, and the traversal runs until it holds
            // the requested number of them as well as a full working set.
            private readonly RoaringBitmap? _filter;
            private readonly Func<ContextBoundNativeList<long>> _filterNodes;
            private readonly long _readLimit = long.MaxValue;
            private ContextBoundNativeList<long> _scanNodes;
            private ExactSearcher _scan;

            public long CandidatesProcessed => _vectorReadCounter + (_scan?.CandidatesProcessed ?? 0);

            public long VectorComparisons => _vectorReadCounter + (_scan?.VectorComparisons ?? 0);

            public NearestSearcher(SearchState searchState, int startingPointIndex, Memory<byte> vector,
                int level, int numberOfCandidates,
                ContextBoundNativeList<int> candidates,
                NearestEdgesFlags flags,
                RoaringBitmap? filter,
                Func<ContextBoundNativeList<long>> filterNodes,
                CodeScreen screen)
            {
                _searchState = searchState;
                _startingPointIndex = startingPointIndex;
                _vector = vector;
                _level = level;
                _numberOfCandidates = numberOfCandidates;
                _internalNumberOfCandidates = numberOfCandidates;
                if (filter is { } f)
                {
                    _filter = f;
                    var filterCount = f.ComputeCount();
                    searchState._admittedQ ??= new();
                    _internalNumberOfCandidates = FilteredBeam(numberOfCandidates, filterCount, searchState.Options.CountOfVectors);
                    _filterNodes = filterNodes;
                    if (filterNodes is not null)
                        _readLimit = filterCount;
                }

                _candidates = candidates;
                _flags = flags;
                _indexes = new NativeList<int>();
                _nodeIds = new NativeList<long>();
                _rows = new NativeList<nint>();
                _screen = screen;
            }

            private int InitState()
            {
                var candidatesQ = _searchState._candidatesQ;

                Debug.Assert(candidatesQ.Count == 0, "_candidatesQ.Count == 0");
                Debug.Assert(_searchState._nearestEdgesQ.Count == 0, "_nearestEdgesQ.Count == 0");
                Debug.Assert(_searchState._admittedQ is null || _searchState._admittedQ.Count == 0, "_admittedQ.Count == 0");

                var visitedCounter = ++_searchState._visitsCounter;
                _searchState.OnQueryVector(_vector);
                var distance = _searchState.QueryDistance(_vector.Span, _startingPointIndex, ref _vectorReadCounter);
                ref var startingPoint = ref _searchState.GetNodeByIndex(_startingPointIndex);
                startingPoint.Visited = visitedCounter;
                // The candidates queue is sorted by distance, so the lowest distance will always pop first.
                // The nearest-edges queues are sorted by reversed distance, so a full queue pops the one with the largest distance.
                candidatesQ.Enqueue(_startingPointIndex, distance);
                if (_flags.HasFlag(NearestEdgesFlags.StartingPointAsEdge) &&
                    ((startingPoint.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) != Constants.Graphs.VectorId.Tombstone
                     || _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false))
                {
                    Keep(_startingPointIndex, startingPoint.PostingListId, -distance);
                }

                return visitedCounter;
            }

            // Adds the node to the working set and, when the filter admits it, to the filter's nearest nodes; a full queue drops its farthest entry.
            private void Keep(int nodeIndex, long postingListId, float negatedDistance)
            {
                Keep(_searchState._nearestEdgesQ, _internalNumberOfCandidates, nodeIndex, negatedDistance);
                if (_filter is { } filter && _searchState.HasEntryInFilter(postingListId, filter))
                    Keep(_searchState._admittedQ, _numberOfCandidates, nodeIndex, negatedDistance);
            }

            private static void Keep(PriorityQueue<int, float> queue, int size, int nodeIndex, float negatedDistance)
            {
                if (queue.Count < size)
                    queue.Enqueue(nodeIndex, negatedDistance);
                else
                    queue.EnqueueDequeue(nodeIndex, negatedDistance);
            }

            // The negated distance a neighbor must exceed to enter the traversal: the farthest entry of the working set and, with a filter, of the filter's
            // nearest nodes, whichever is farther. Negative infinity while a queue has room. Once finite it only grows.
            private float Threshold()
            {
                var threshold = Bound(_searchState._nearestEdgesQ, _internalNumberOfCandidates);
                return _filter is null ? threshold : Math.Min(threshold, Bound(_searchState._admittedQ, _numberOfCandidates));
            }

            private static float Bound(PriorityQueue<int, float> queue, int size) =>
                queue.Count == size && queue.TryPeek(out _, out var negatedDistance) ? negatedDistance : float.NegativeInfinity;

            public ContextBoundNativeList<int> Search()
            {
                if (_screen is not null && _level == 0 && _filter is null)
                    return SearchSteered();

                var candidatesQ = _searchState._candidatesQ;
                var visitedCounter = InitState();
                var threshold = Threshold();
                var stopped = false;

                while (candidatesQ.TryDequeue(out var cur, out var curDistance))
                {
                    if (-curDistance < threshold)
                        break;

                    if (_vectorReadCounter > _readLimit)
                    {
                        stopped = true;
                        break;
                    }

                    _searchState.GetNodeByIndex(cur).Visited = visitedCounter;
                    // Only full queues have a threshold to screen against.
                    var isFull = threshold > float.NegativeInfinity;
                    _searchState.ResolveEdgeIndexes(cur, _level, isFull ? _screen : null, -threshold, ref _nodeIds, ref _indexes);

                    // Pin loop locals: NativeList field reads through `this` force the JIT to
                    // reload the storage pointer on every candidate.
                    int indexesCount = _indexes.Count;
                    int* indexesPtr = _indexes.RawItems;
                    for (int i = 0; i < indexesCount; i++)
                    {
                        var nextIndex = indexesPtr[i];
                        ref var next = ref _searchState.GetNodeByIndex(nextIndex);
                        if (next.Visited == visitedCounter)
                            continue; // already checked
                        next.Visited = visitedCounter;

                        // Prefetch the next unvisited neighbor's vector data to overlap
                        // cache-miss latency with the current distance computation.
                        PrefetchNextNeighborVector(indexesPtr, indexesCount, i + 1, visitedCounter);

                        var isDeleted = (next.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) == Constants.Graphs.VectorId.Tombstone;

                        float nextDist = -_searchState.QueryDistance(_vector.Span, ref next, ref _vectorReadCounter);
                        if (isFull)
                        {
                            if ((threshold < nextDist) == false)
                                continue;
                            if (_screen is not null)
                                _screen.Admissions++;
                        }

                        candidatesQ.Enqueue(nextIndex, -nextDist);
                        if (isDeleted == false || _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false)
                            Keep(nextIndex, next.PostingListId, nextDist);

                        threshold = Threshold();
                        isFull = threshold > float.NegativeInfinity;
                    }
                }

                ProcessResults();
                // The queues belong to the SearchState, and the next traversal on it starts from them empty.
                candidatesQ.Clear();
                if (stopped == false)
                    return _candidates;

                _scanNodes = _filterNodes();
                _scan = new ExactSearcher(_searchState, _vector, _numberOfCandidates, null, _scanNodes);
                return _scan.Search();
            }

            /// <summary>
            /// Orders the candidates and the working set by the estimates of the codes, so a neighbor is checked from its code row alone and only the nodes that
            /// the traversal expands are resolved. The working set holds expanded nodes only: when the flags drop nodes with empty posting lists, such a node is
            /// expanded and never takes a place in it. The final working set is ranked by exact distances.
            /// </summary>
            private ContextBoundNativeList<int> SearchSteered()
            {
                var candidatesQ = _searchState._steeredCandidatesQ;
                var workingQ = _searchState._steeredWorkingQ;
                var allocator = _searchState.Llt.Allocator;
                var dropsEmpty = _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists);

                _searchState.OnQueryVector(_vector);
                var startId = _searchState.GetNodeByIndex(_startingPointIndex).NodeId;
                var startDistance = _searchState.QueryDistance(_vector.Span, _startingPointIndex, ref _vectorReadCounter);
                var keepsStart = _flags.HasFlag(NearestEdgesFlags.StartingPointAsEdge);
                _screen.Mark(startId);
                candidatesQ.Enqueue(startId, startDistance);

                while (candidatesQ.TryDequeue(out var id, out var distance))
                {
                    if (distance > Farthest(workingQ))
                        break;

                    ref var node = ref _searchState.GetNodeByIndex(_searchState.GetNodeIndexById(id));
                    if ((id != startId || keepsStart) &&
                        (dropsEmpty == false || (node.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) != Constants.Graphs.VectorId.Tombstone))
                    {
                        if (workingQ.Count < _internalNumberOfCandidates)
                            workingQ.Enqueue(id, -distance);
                        else
                            workingQ.EnqueueDequeue(id, -distance);
                    }

                    _nodeIds.ResetAndCopyFrom(allocator, node.Edges(_level));

                    var count = _nodeIds.Count;
                    // PERF: The code rows of the neighbors start loading before the first estimate, so their cache misses overlap.
                    _rows.ResetAndEnsureCapacity(allocator, count);
                    for (var i = 0; i < count; i++)
                        _rows.AddUnsafe(_screen.IsMarked(_nodeIds[i]) ? 0 : (nint)_searchState.PrefetchCode(_nodeIds[i]));

                    for (var i = 0; i < count; i++)
                    {
                        var next = _nodeIds[i];
                        if (_screen.Mark(next) == false)
                            continue;

                        var row = (byte*)_rows[i];
                        var estimate = row != null ? _screen.Estimate(new ReadOnlySpan<byte>(row, _searchState._codeRowBytes)) : double.NaN;
                        var nextDistance = double.IsNaN(estimate)
                            ? _searchState.QueryDistance(_vector.Span, _searchState.GetNodeIndexById(next), ref _vectorReadCounter)
                            : (float)estimate;
                        if (nextDistance < Farthest(workingQ))
                            candidatesQ.Enqueue(next, nextDistance);
                    }
                }

                candidatesQ.Clear();
                var nearestEdgesQ = _searchState._nearestEdgesQ;
                while (workingQ.TryDequeue(out var id, out _))
                {
                    var index = _searchState.GetNodeIndexById(id);
                    nearestEdgesQ.Enqueue(index, -_searchState.QueryDistance(_vector.Span, index, ref _vectorReadCounter));
                }

                ProcessResults();
                return _candidates;
            }

            // The distance of the farthest node of a full working set; infinity while it has room.
            private float Farthest(PriorityQueue<long, float> workingQ) =>
                workingQ.Count == _internalNumberOfCandidates && workingQ.TryPeek(out _, out var negatedDistance) ? -negatedDistance : float.PositiveInfinity;

            private void ProcessResults()
            {
                var resultsQ = _filter is null ? _searchState._nearestEdgesQ : _searchState._admittedQ;
                _candidates.Clear();
                _candidates.EnsureCapacityFor(resultsQ.Count);

                while (resultsQ.TryDequeue(out var edgeId, out _))
                    _candidates.AddUnsafe(edgeId);

                _candidates.Inner.Reverse();
                _searchState._nearestEdgesQ.Clear();
            }

            /// <summary>
            /// Finds the next unvisited neighbor starting from <paramref name="startFrom"/> and
            /// issues software prefetch instructions for its vector data. This overlaps the
            /// cache-miss latency (~500ns for 6KB from L3) with the current distance computation (~633ns).
            /// On platforms without software prefetch the JIT eliminates this entirely.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void PrefetchNextNeighborVector(int* indexesPtr, int indexesCount, int startFrom, int visitedCounter)
            {
                if (PortableIntrinsics.CanPrefetch == false)
                    return;

                for (int j = startFrom; j < indexesCount; j++)
                {
                    var idx = indexesPtr[j];
                    ref var node = ref _searchState.GetNodeByIndex(idx);
                    if (node.Visited == visitedCounter)
                        continue;

                    if (node.TryGetVectorAddress(out byte* address, out int length) == false)
                        return; // vector not loaded yet, skip prefetch

                    PortableIntrinsics.PrefetchRange(address, length);
                    return;
                }
            }

            public void Dispose()
            {
                _scan?.Dispose();
                _scanNodes.Dispose();
                _candidates.Dispose();
                _nodeIds.Dispose(_searchState.Llt.Allocator);
                _indexes.Dispose(_searchState.Llt.Allocator);
                _rows.Dispose(_searchState.Llt.Allocator);
                _searchState._candidatesQ.Clear();
                _searchState._steeredCandidatesQ.Clear();
                _searchState._steeredWorkingQ.Clear();
            }
        }
    }
}
