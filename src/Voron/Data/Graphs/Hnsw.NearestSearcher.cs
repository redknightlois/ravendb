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
    /// <summary>
    /// The working set of a filtered traversal: the ball expected to hold <paramref name="numberOfCandidates"/> of the filter's nodes, with the filter's entry
    /// count <paramref name="filterCount"/> standing in for its node count. Saturates at the graph's size. Entries that share a node or have none make the
    /// ball too small, and a filter whose entries reach fewer than <paramref name="numberOfCandidates"/> nodes makes the traversal read every reachable node;
    /// counting the filter's nodes instead takes a pass over its entries.
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
            private long _vectorReadCounter = 0;
            private readonly int _startingPointIndex;
            // With a filter, the results are the nearest of the filter's nodes that the traversal reaches, and the traversal runs until it holds
            // the requested number of them as well as a full working set.
            private readonly GrowableBitArray? _filter;

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
                if (filter is { } f)
                {
                    _filter = f;
                    searchState._admittedQ ??= new();
                    _internalNumberOfCandidates = FilteredBeam(numberOfCandidates, f.Count, searchState.Options.CountOfVectors);
                }

                _candidates = candidates;
                _flags = flags;
                _indexes = new NativeList<int>();
                _nodeIds = new NativeList<long>();
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
                var candidatesQ = _searchState._candidatesQ;
                var visitedCounter = InitState();
                var threshold = Threshold();

                while (candidatesQ.TryDequeue(out var cur, out var curDistance))
                {
                    if (-curDistance < threshold)
                        break;

                    _searchState.GetNodeByIndex(cur).Visited = visitedCounter;
                    var isFull = threshold > float.NegativeInfinity;
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

                        var isDeleted = (next.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) == Constants.Graphs.VectorId.Tombstone;

                        float nextDist = -_searchState.QueryDistance(_vector.Span, ref next, ref _vectorReadCounter);
                        if (isFull && (threshold < nextDist) == false)
                            continue;

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
                return _candidates;
            }

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
                _candidates.Dispose();
                _nodeIds.Dispose(_searchState.Llt.Allocator);
                _indexes.Dispose(_searchState.Llt.Allocator);
                _searchState._candidatesQ.Clear();
            }
        }
    }
}
