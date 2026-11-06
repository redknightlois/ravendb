using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server.Collections;
using Voron.Global;
using Voron.Util;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    public partial class SearchState
    {
        private class NearestSearcher : IHnswSearcher
        {
            private readonly SearchState _searchState;
            private readonly Memory<byte> _vector;
            private readonly int _level;
            private readonly int _numberOfCandidates;

            // The working set size: the requested candidates, and with a filter, grown before every walk.
            private int _internalNumberOfCandidates;

            private ContextBoundNativeList<int> _candidates;
            private readonly NearestEdgesFlags _flags;
            private NativeList<int> _indexes;
            private NativeList<long> _nodeIds;
            private long _vectorReadCounter = 0;
            private readonly int _startingPointIndex;
            // With a filter, the walk restarts with a larger working set until its working sets have held the requested number of the filter's nodes, or
            // it has read as many vectors as the filter's size allows. The results are the nearest of the filter's nodes among every vector it read.
            private readonly GrowableBitArray? _filter;
            // The working-set nodes of the earlier walks, created by the first restart; a later walk keeps them out of its working set.
            private HashSet<int> _walked;

            public long CandidatesProcessed => _vectorReadCounter;

            public NearestSearcher(SearchState searchState, int startingPointIndex, Memory<byte> vector,
                int level, int numberOfCandidates,
                ContextBoundNativeList<int> candidates,
                NearestEdgesFlags flags,
                GrowableBitArray? filter)
            {
                _searchState = searchState;
                _startingPointIndex = startingPointIndex;
                _vector = vector;
                _level = level;
                _numberOfCandidates = numberOfCandidates;
                _internalNumberOfCandidates = numberOfCandidates;
                _filter = filter;
                if (filter.HasValue)
                {
                    searchState._admittedQ ??= new();
                    _internalNumberOfCandidates = Grow(numberOfCandidates);
                }

                _candidates = candidates;
                _flags = flags;
                _indexes = new NativeList<int>();
                _nodeIds = new NativeList<long>();
            }

            private static int Grow(int size) => (int)Math.Min(int.MaxValue, size + Math.Max(1L, GetPrefetchExtendSize(size)));

            private static int GetPrefetchExtendSize(int numberOfCandidates) => numberOfCandidates switch
            {
                <= 131_072 => numberOfCandidates / 2,
                _ => (int)Math.Sqrt(131_072L * numberOfCandidates)
            };

            public ContextBoundNativeList<int> Search()
            {
                var nearestEdgesQ = _searchState._nearestEdgesQ;
                if (_filter.HasValue == false)
                {
                    Walk();
                    ProcessResults(nearestEdgesQ);
                    return _candidates;
                }

                // The read budget of a filtered search: half the filter's matches, at most a fifth of the graph. A walk that reads as many vectors as
                // the filter has matches already costs about an exact scan of them. On sparse filters this budget, not ef, bounds recall (RavenDB-27918).
                // Corax scans filters under its scanning threshold, 1024 matches by default, so the doubled budget below that size applies only when the
                // threshold is lowered.
                var filterCount = _filter.Value.Count;
                var readLimit = filterCount < 1024 ? 2 * filterCount : Math.Min(filterCount / 2, _searchState.Options.CountOfVectors / 5);
                var held = 0;
                while (true)
                {
                    var reachedEveryNode = Walk() == false;
                    foreach (var (nodeIndex, _) in nearestEdgesQ.UnorderedItems)
                    {
                        if ((_walked is null || _walked.Contains(nodeIndex) == false) && IsAdmitted(_searchState.GetNodeByIndex(nodeIndex).PostingListId))
                            held++;
                    }

                    if (reachedEveryNode || held >= _numberOfCandidates || _vectorReadCounter >= readLimit)
                        break;

                    _walked ??= new();
                    foreach (var (nodeIndex, _) in nearestEdgesQ.UnorderedItems)
                        _walked.Add(nodeIndex);
                    nearestEdgesQ.Clear();
                    _internalNumberOfCandidates = Grow(_internalNumberOfCandidates);
                }

                nearestEdgesQ.Clear();
                ProcessResults(_searchState._admittedQ);
                return _candidates;
            }

            // A walk from the starting point that ends once the nearest unexpanded node is farther than a full working set's farthest node, and returns
            // false when it ran out of nodes to expand first. A later walk reads no vector the earlier ones read, their distances stay cached in SearchState.
            private bool Walk()
            {
                var candidatesQ = _searchState._candidatesQ;
                var nearestEdgesQ = _searchState._nearestEdgesQ;

                Debug.Assert(candidatesQ.Count == 0, "_candidatesQ.Count == 0");
                Debug.Assert(nearestEdgesQ.Count == 0, "_nearestEdgesQ.Count == 0");

                var visitedCounter = ++_searchState._visitsCounter;
                _searchState.OnQueryVector(_vector);
                var read = _vectorReadCounter;
                var lowerBound = -_searchState.QueryDistance(_vector.Span, _startingPointIndex, ref _vectorReadCounter);
                ref var startingPoint = ref _searchState.GetNodeByIndex(_startingPointIndex);
                startingPoint.Visited = visitedCounter;
                if (_vectorReadCounter != read)
                    Admit(_startingPointIndex, startingPoint.PostingListId, lowerBound);
                // The candidates queue is sorted by distance, so the lowest distance will always pop first.
                // The nearest-edges queue is sorted by reversed distance, so a full queue pops the one with the largest distance.
                candidatesQ.Enqueue(_startingPointIndex, -lowerBound);
                if (_flags.HasFlag(NearestEdgesFlags.StartingPointAsEdge) &&
                    ((startingPoint.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) != Constants.Graphs.VectorId.Tombstone
                     || _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false))
                {
                    nearestEdgesQ.Enqueue(_startingPointIndex, lowerBound);
                }

                while (candidatesQ.TryDequeue(out var cur, out var curDistance))
                {
                    if (-curDistance < lowerBound && nearestEdgesQ.Count == _internalNumberOfCandidates)
                    {
                        // The queues belong to the SearchState, and the next walk on it starts from them empty.
                        candidatesQ.Clear();
                        return true;
                    }

                    _searchState.GetNodeByIndex(cur).Visited = visitedCounter;
                    _searchState.ResolveEdgeIndexes(cur, _level, ref _nodeIds, ref _indexes);

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

                        var isDeleted = (next.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) == Constants.Graphs.VectorId.Tombstone
                                        || (_walked?.Contains(nextIndex) ?? false);

                        read = _vectorReadCounter;
                        float nextDist = -_searchState.QueryDistance(_vector.Span, ref next, ref _vectorReadCounter);
                        if (_vectorReadCounter != read)
                            Admit(nextIndex, next.PostingListId, nextDist);

                        if (nearestEdgesQ.Count < _internalNumberOfCandidates)
                        {
                            candidatesQ.Enqueue(nextIndex, -nextDist);

                            if (isDeleted == false || _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false)
                            {
                                nearestEdgesQ.Enqueue(nextIndex, nextDist);
                            }
                        }
                        else if (lowerBound < nextDist)
                        {
                            candidatesQ.Enqueue(nextIndex, -nextDist);

                            if (isDeleted == false || _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false)
                            {
                                nearestEdgesQ.EnqueueDequeue(nextIndex, nextDist);
                            }
                        }
                        else
                        {
                            continue;
                        }

                        Debug.Assert(candidatesQ.Count > 0);
                        nearestEdgesQ.TryPeek(out _, out lowerBound);
                    }
                }

                return false;
            }

            // Keeps the requested number of the filter's nearest nodes among every vector read, inside or outside the working set. Only a node nearer than
            // the farthest one kept needs the membership check.
            private void Admit(int nodeIndex, long postingListId, float negatedDistance)
            {
                if (_filter.HasValue == false)
                    return;

                var admittedQ = _searchState._admittedQ;
                if (admittedQ.Count == _numberOfCandidates && admittedQ.TryPeek(out _, out var farthest) && negatedDistance <= farthest)
                    return;

                if (IsAdmitted(postingListId) == false)
                    return;

                if (admittedQ.Count < _numberOfCandidates)
                    admittedQ.Enqueue(nodeIndex, negatedDistance);
                else
                    admittedQ.EnqueueDequeue(nodeIndex, negatedDistance);
            }

            private bool IsAdmitted(long postingListId) => _searchState.HasEntryInFilter(postingListId, _filter.Value);

            private void ProcessResults(PriorityQueue<int, float> resultsQ)
            {
                _candidates.Clear();
                _candidates.EnsureCapacityFor(resultsQ.Count);

                while (resultsQ.TryDequeue(out var edgeId, out _))
                    _candidates.AddUnsafe(edgeId);

                _candidates.Inner.Reverse();
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
                _candidates.Dispose();
                _nodeIds.Dispose(_searchState.Llt.Allocator);
                _indexes.Dispose(_searchState.Llt.Allocator);
                _searchState._candidatesQ.Clear();
            }
        }
    }
}
