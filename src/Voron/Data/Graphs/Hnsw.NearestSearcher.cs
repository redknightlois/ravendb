using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
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

            // Actual number of candidates currently being processed. It may differ from the caller's NumberOfCandidates
            // because we may need to over-fetch to satisfy the filter clause.
            private int _internalNumberOfCandidates;

            private ContextBoundNativeList<int> _candidates;
            private readonly NearestEdgesFlags _flags;
            private readonly bool _hasFilterMatch;
            private NativeList<int> _indexes;
            private NativeList<long> _nodeIds;
            private readonly HashSet<int> _alreadyReturnedEdges;
            private long _vectorReadCounter = 0;
            private readonly int _startingPointIndex;
            private ContextBoundNativeList<int> _startingPointIndexes;
            
            public long CandidatesProcessed { get => _vectorReadCounter; }
            public int NumberOfCandidates { get; init; }

            private NearestSearcher(SearchState searchState, Memory<byte> vector,
                int level, int numberOfCandidates,
                ContextBoundNativeList<int> candidates,
                NearestEdgesFlags flags,
                bool hasFilterMatch)
            {
                _searchState = searchState;
                _vector = vector;
                _level = level;
                NumberOfCandidates = numberOfCandidates;

                _internalNumberOfCandidates = hasFilterMatch == false
                    ? numberOfCandidates
                    : numberOfCandidates + GetPrefetchExtendSize(numberOfCandidates);
                _candidates = candidates;
                _flags = flags;
                _hasFilterMatch = hasFilterMatch;
                _indexes = new NativeList<int>();
                _nodeIds = new NativeList<long>();
                if (_hasFilterMatch)
                {
                    _alreadyReturnedEdges = new();
                }
            }
            
            
            public NearestSearcher(SearchState searchState, ContextBoundNativeList<int> startingPointsIndexes, Memory<byte> vector, int level, int numberOfCandidates, ContextBoundNativeList<int> candidates, NearestEdgesFlags flags) : this(searchState, vector, level, numberOfCandidates, candidates, flags, true)
            {
                _startingPointIndexes = startingPointsIndexes;
                _startingPointIndex = -1;
            }

            public NearestSearcher(SearchState searchState, int startingPointIndex, Memory<byte> vector,
                int level, int numberOfCandidates,
                ContextBoundNativeList<int> candidates,
                NearestEdgesFlags flags,
                bool hasFilterMatch) : this(searchState, vector, level, numberOfCandidates, candidates, flags, hasFilterMatch)
            {
                _startingPointIndex = startingPointIndex;
            }

            private void InitState(out float lowerBound, out int visitedCounter)
            {
                if (_startingPointIndex != -1)
                {
                    Deepest(out lowerBound, out visitedCounter);
                }
                else
                {
                    Multi(out lowerBound, out visitedCounter);
                }
            }

            private void Multi(out float lowerBound, out int visitedCounter)
            {
                var candidatesQ = _searchState._candidatesQ;
                var nearestEdgesQ = _searchState._nearestEdgesQ;

                Debug.Assert(candidatesQ.Count == 0, "_candidatesQ.Count == 0");
                Debug.Assert(nearestEdgesQ.Count == 0, "_nearestEdgesQ.Count == 0");

                lowerBound = float.MaxValue;
                visitedCounter = ++_searchState._visitsCounter;

                foreach (var nodeIdx in _startingPointIndexes)
                {
                    ref var startingPoint = ref _searchState.GetNodeByIndex(nodeIdx);
                    startingPoint.Visited = visitedCounter;
                    var currentDistance = -_searchState.QueryDistance(_vector.Span, nodeIdx, ref _vectorReadCounter);
                    lowerBound = Math.Min(lowerBound, currentDistance);
                    candidatesQ.Enqueue(nodeIdx, -currentDistance);
                    if (_flags.HasFlag(NearestEdgesFlags.StartingPointAsEdge) &&
                        ((startingPoint.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) != Constants.Graphs.VectorId.Tombstone
                         || _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false))
                    {
                        nearestEdgesQ.Enqueue(nodeIdx, currentDistance);
                    }
                }
            }

            private void Deepest(out float lowerBound, out int visitedCounter)
            {
                var candidatesQ = _searchState._candidatesQ;
                var nearestEdgesQ = _searchState._nearestEdgesQ;
                var allocator = _searchState.Llt.Allocator;

                Debug.Assert(candidatesQ.Count == 0, "_candidatesQ.Count == 0");
                Debug.Assert(nearestEdgesQ.Count == 0, "_nearestEdgesQ.Count == 0");

                lowerBound = -_searchState.QueryDistance(_vector.Span, _startingPointIndex, ref _vectorReadCounter);
                visitedCounter = ++_searchState._visitsCounter;
                {
                    ref var startingPoint = ref _searchState.GetNodeByIndex(_startingPointIndex);
                    startingPoint.Visited = visitedCounter;
                    // The candidates queue is sorted by distance, so the lowest distance
                    // will always pop first.
                    // The nearest-edges queue is sorted by reversed distance, so when we add a
                    // new item to the queue, we'll pop the one with the largest distance.

                    candidatesQ.Enqueue(_startingPointIndex, -lowerBound);
                    if (_flags.HasFlag(NearestEdgesFlags.StartingPointAsEdge) &&
                        ((startingPoint.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) != Constants.Graphs.VectorId.Tombstone
                         || _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false))
                    {
                        nearestEdgesQ.Enqueue(_startingPointIndex, lowerBound);
                    }
                }
            }

            public IEnumerable<bool> Search()
            {
                Start:
                var candidatesQ = _searchState._candidatesQ;
                var nearestEdgesQ = _searchState._nearestEdgesQ;
                var allocator = _searchState.Llt.Allocator;

                Debug.Assert(candidatesQ.Count == 0, "_candidatesQ.Count == 0");
                Debug.Assert(nearestEdgesQ.Count == 0, "_nearestEdgesQ.Count == 0");
                InitState(out float lowerBound, out int visitedCounter);

                // Prepare int8 screening for this query (only active for f32 vectors)
                _searchState.PrepareInt8Screening(_vector.Span);
                // Int8 screening margin: conservative threshold to avoid false negatives.
                // Quantization error for 1536D int8 dot product ≈ 0.0005 std; margin = ~20× that.
                const float int8ScreeningMargin = 0.01f;
                // Boundary zone: candidates within this distance of lowerBound use f32 for accuracy.
                // Candidates clearly better (beyond this zone) safely use int8 approximate distance.
                // Int8 pairwise ordering error std ≈ 0.0007; 0.003 = ~4σ coverage.
                const float int8BoundaryZone = 0.003f;

                while (candidatesQ.TryDequeue(out var cur, out var curDistance))
                {
                    if (-curDistance < lowerBound &&
                        nearestEdgesQ.Count == _internalNumberOfCandidates)
                    {
                        ProcessResults();
                        yield return true;
                        // If we need to fetch more, we'll start the query again with a higher NumberOfCandidates.
                        // The SearchState keeps its state, so traversal through already visited nodes is I/O-free
                        // because we keep distances in memory from the previous run.
                        // This method can be greedy enough to traverse the entire graph, so it's the caller's
                        // responsibility to enforce a stop condition.
                        goto Start;
                    }

                    ref var candidate = ref _searchState.GetNodeByIndex(cur);
                    candidate.Visited = visitedCounter;

                    ref var edges = ref candidate.EdgesPerLevel[_level];

                    _nodeIds.ResetAndCopyFrom(allocator, edges.ToSpan());
                    _searchState.LoadNodeIndexes(ref _nodeIds, ref _indexes);

                    for (int i = 0; i < _indexes.Count; i++)
                    {
                        var nextIndex = _indexes[i];
                        ref var next = ref _searchState.GetNodeByIndex(nextIndex);
                        if (next.Visited == visitedCounter)
                            continue; // already checked
                        next.Visited = visitedCounter;

                        // Prefetch the next unvisited neighbor's vector data to overlap
                        // cache-miss latency with the current distance computation.
                        PrefetchNextNeighborVector(i + 1, visitedCounter);

                        // Int8 primary distance with boundary-aware f32 fallback:
                        // - Clearly worse than lowerBound: SKIP via int8 screening (~55ns)
                        // - Near lowerBound boundary: USE F32 for accurate ordering (~633ns)
                        // - Clearly better than lowerBound: USE INT8 (safe, ~55ns)
                        // This preserves graph traversal accuracy at the critical boundary
                        // while saving f32 for candidates that clearly don't affect ordering.
                        float nextDist;
                        if (_searchState.TryGetInt8Screening(nextIndex, out float approxDist))
                        {
                            if (nearestEdgesQ.Count >= _internalNumberOfCandidates)
                            {
                                // PQ is full — screening and boundary decisions
                                if (approxDist > -lowerBound + int8ScreeningMargin)
                                    continue; // clearly worse, skip

                                if (approxDist > -lowerBound - int8BoundaryZone)
                                {
                                    // Boundary zone: int8 distance is within zone of lowerBound.
                                    // Use f32 for accurate ordering to preserve recall.
                                    nextDist = -_searchState.QueryDistance(_vector.Span, nextIndex, ref _vectorReadCounter);
                                }
                                else
                                {
                                    // Clearly better: int8 is safe for PQ ordering
                                    nextDist = -approxDist;
                                }
                            }
                            else
                            {
                                // PQ not full: use int8 (collecting everything, ordering less critical)
                                nextDist = -approxDist;
                            }
                        }
                        else
                        {
                            // Fallback to f32 when int8 not available (vector not loaded yet)
                            nextDist = -_searchState.QueryDistance(_vector.Span, nextIndex, ref _vectorReadCounter);
                        }

                        var isDeleted = (next.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) == Constants.Graphs.VectorId.Tombstone
                                        || (_hasFilterMatch && _alreadyReturnedEdges.Contains(nextIndex));

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

                // Nothing more to visit. Move the current results into the candidates list and finish.
                ProcessResults();
                candidatesQ.Clear();
                yield return _candidates.Count > 0;
            }

            public bool TryGetCurrentCandidates(out ContextBoundNativeList<int> candidates)
            {
                candidates = _candidates;
                return candidates.Count > 0;
            }

            public void IncreaseNumberOfCandidates(int currentlyAcceptedNodes)
            {
                Reset();
                _internalNumberOfCandidates += GetPrefetchExtendSize(_internalNumberOfCandidates);
            }

            // Reset the NearestSearcher state; however, it does not clear the data already stored inside SearchState.
            // This is important because it allows us to reduce I/O pressure during over-fetching and when restarting the query.
            private void Reset()
            {
                _searchState._candidatesQ.Clear();
                _searchState._nearestEdgesQ.Clear();

                for (int nodeIdx = 0; nodeIdx < _searchState._nodes.Count; nodeIdx++)
                {
                    _searchState._nodes[nodeIdx].Visited = 0;
                }

                _searchState._visitsCounter = 0;
                _candidates.Clear();
                _nodeIds.Clear();
                _indexes.Clear();
            }

            private void ProcessResults()
            {
                _candidates.Clear();

                if (_searchState._int8Enabled)
                {
                    // Int8 was used as primary distance during search. Re-compute f32 distances
                    // for final ranking to ensure accurate results.
                    // Optimization: only compute NEW f32 distances for the top refineCutoff candidates
                    // (dequeued last from the min-heap, i.e. the best candidates by mixed distance).
                    // Candidates in the tail that already have cached f32 (from boundary-zone
                    // computation during beam search) are included at their cached distance.
                    // Tail candidates with only int8 distance are appended at the end — they are
                    // extremely unlikely to be in the final top-K.
                    const int refineCutoff = 64;
                    int totalCount = _searchState._nearestEdgesQ.Count;

                    // Drain the PQ into an array. Min-heap dequeues worst-first (most negative priority),
                    // so allEdges[0] = worst, allEdges[totalCount-1] = best.
                    Span<int> allEdges = totalCount <= 512
                        ? stackalloc int[totalCount]
                        : new int[totalCount];
                    int edgeCount = 0;
                    while (_searchState._nearestEdgesQ.TryDequeue(out var edgeId, out _))
                    {
                        if (_hasFilterMatch && _alreadyReturnedEdges!.Add(edgeId) == false)
                            continue;
                        allEdges[edgeCount++] = edgeId;
                    }

                    // The best candidates are at the end. Refine the last refineCutoff entries with f32.
                    int refineStart = Math.Max(0, edgeCount - refineCutoff);
                    var refinePQ = new PriorityQueue<int, float>();
                    int unrefinedCount = 0;
                    Span<int> unrefined = refineStart <= 512
                        ? stackalloc int[Math.Max(refineStart, 1)]
                        : new int[refineStart];

                    // Process tail (worst candidates, positions 0..refineStart-1):
                    // only include if they have cached f32 from boundary zone.
                    for (int i = 0; i < refineStart; i++)
                    {
                        int eid = allEdges[i];
                        ref var node = ref _searchState.GetNodeByIndex(eid);
                        if (node.QueryDistanceVersion == _searchState._visitsCounter)
                        {
                            // Already has f32 from boundary zone — include at cached distance
                            refinePQ.Enqueue(eid, node.QueryDistanceValue);
                        }
                        else
                        {
                            // Int8-only, deep in ranking: skip f32, append at end
                            unrefined[unrefinedCount++] = eid;
                        }
                    }

                    // Process top candidates (positions refineStart..edgeCount-1): always refine with f32
                    for (int i = refineStart; i < edgeCount; i++)
                    {
                        int eid = allEdges[i];
                        float f32Dist = _searchState.QueryDistance(_vector.Span, eid, ref _vectorReadCounter);
                        refinePQ.Enqueue(eid, f32Dist);
                    }

                    _candidates.EnsureCapacityFor(refinePQ.Count + unrefinedCount);
                    while (refinePQ.TryDequeue(out var edgeId, out _))
                    {
                        _candidates.AddUnsafe(edgeId);
                    }
                    // Append unrefined tail (won't be in top K)
                    for (int i = 0; i < unrefinedCount; i++)
                    {
                        _candidates.AddUnsafe(unrefined[i]);
                    }
                }
                else
                {
                    _candidates.EnsureCapacityFor(_searchState._nearestEdgesQ.Count);

                    while (_searchState._nearestEdgesQ.TryDequeue(out var edgeId, out var d))
                    {
                        if (_hasFilterMatch && _alreadyReturnedEdges!.Add(edgeId) == false)
                            continue;

                        _candidates.AddUnsafe(edgeId);
                    }

                    _candidates.Inner.Reverse();
                }
            }
            
            public bool ShouldContinueSearch(long filterDocsCount)
            {
                if (_hasFilterMatch == false)
                    return false;

                var max = filterDocsCount switch
                {
                    < 1024 => 2 * filterDocsCount, // It's hard to determine right number here. For smaller graphs it may be an issue.
                    // However, for small filter set (for 1K we will force exact search anyway, therefore, this probably will be used only for testing purposes.
                    _ => Math.Min(filterDocsCount / 2, _searchState.Options.CountOfVectors / 5)
                };
                
                return _vectorReadCounter < max;
            }

            /// <summary>
            /// Finds the next unvisited neighbor starting from <paramref name="startFrom"/> and
            /// issues software prefetch instructions for its vector data. This overlaps the
            /// cache-miss latency (~500ns for 6KB from L3) with the current distance computation (~633ns).
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void PrefetchNextNeighborVector(int startFrom, int visitedCounter)
            {
                for (int j = startFrom; j < _indexes.Count; j++)
                {
                    var idx = _indexes[j];
                    ref var node = ref _searchState.GetNodeByIndex(idx);
                    if (node.Visited == visitedCounter)
                        continue;
                    if (node.TryGetVectorAddress(out byte* address, out int length) == false)
                        return; // vector not loaded yet, skip prefetch
                    PrefetchVectorData(address, length);
                    return;
                }
            }

            /// <summary>
            /// Issues software prefetch hints at 512-byte intervals across a vector's memory range.
            /// This primes the hardware sequential prefetcher across page boundaries, bringing
            /// the full vector into L1/L2 cache ahead of the SIMD distance computation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static void PrefetchVectorData(byte* address, int length)
            {
                byte* end = address + length;
                for (byte* p = address; p < end; p += 512)
                {
                    Sse.Prefetch0(p);
                }
            }

            private static int GetPrefetchExtendSize(int numberOfCandidates) => numberOfCandidates switch
            {
                <= 131_072 =>  numberOfCandidates / 2,
                _ => (int)Math.Sqrt(131_072L * numberOfCandidates)
            };

            public void Dispose()
            {
                if (_startingPointIndex == -1)
                    _startingPointIndexes.Dispose();
                _candidates.Dispose();
                _nodeIds.Dispose(_searchState.Llt.Allocator);
                _indexes.Dispose(_searchState.Llt.Allocator);
                _searchState._candidatesQ.Clear();
            }
        }
    }
}
