using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server.Collections;
using Voron.Global;
using Voron.Util;
using Voron.Util.PFor;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public partial class SearchState
    {
        private class ExactSearcher : IHnswSearcher
        {
            private readonly SearchState _searchState;
            private readonly Memory<byte> _vector;
            // Only the filter's nodes are candidates.
            private readonly GrowableBitArray? _filter;
            private ContextBoundNativeList<long> _filterScratch;
            private FastPForDecoder _filterDecoder;
            private ContextBoundNativeList<int> _candidates;
            private readonly ContextBoundNativeList<long>? _nodesToScan;
            private PriorityQueue<long, float> _pq;
            private long _vectorReadCounter = 0;
            private bool _initialized;
            
            public ExactSearcher(SearchState searchState, Memory<byte> vector, int numberOfCandidates, GrowableBitArray? filter, ContextBoundNativeList<long>? nodesToScan)
            {
                _searchState = searchState;
                _vector = vector;
                if (filter is { } f)
                {
                    _filter = f;
                    _filterScratch = new ContextBoundNativeList<long>(searchState.Llt.Allocator);
                    _filterDecoder = new(searchState.Llt.Allocator);
                }

                NumberOfCandidates = numberOfCandidates;
                _nodesToScan = nodesToScan;

                var candidatesCapacity = Math.Min(numberOfCandidates, (int)searchState.Options.CountOfVectors);
                _candidates = new ContextBoundNativeList<int>(searchState.Llt.Allocator, candidatesCapacity);
            }

            public void Dispose()
            {
                if (_filter is not null)
                {
                    _filterScratch.Dispose();
                    _filterDecoder.Dispose();
                }
                _searchState._candidatesQ.Clear();
                _searchState._nearestEdgesQ.Clear();
                _candidates.Dispose();
            }

            public bool MoveNextBatch()
            {
                if (_initialized == false)
                {
                    Initialize();
                    _initialized = true;
                }

                if (_pq.Count == 0)
                {
                    _candidates.Count = 0;
                    return false;
                }

                _candidates.Count = 0;
                while (_candidates.Count < NumberOfCandidates && _pq.TryDequeue(out var nodeId, out _))
                {
                    _candidates.AddByRefUnsafe() = _searchState.GetNodeIndexById(nodeId);
                }

                _candidates.Inner.Reverse();
                return true;
            }

            private void Initialize()
            {
                // Reset the visited set for this traversal and record the query vector so the
                // per-node QueryDistance cache is either reused (same vector) or invalidated
                // (different vector) — OnQueryVector decides based on Memory identity.
                ++_searchState._visitsCounter;
                _searchState.OnQueryVector(_vector);

                _pq = new PriorityQueue<long, float>();
                if (_nodesToScan.HasValue)
                {
                    foreach (long nodeId in _nodesToScan.Value.Iterate())
                        ScanNode(nodeId);
                }
                else
                {
                    long count = _searchState.Options.CountOfVectors;
                    for (long nodeId = 1; nodeId <= count; nodeId++)
                        ScanNode(nodeId);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void ScanNode(long nodeId)
            {
                CandidatesProcessed++;
                var nodeIndex = _searchState.GetNodeIndexById(nodeId);
                var postingListId = _searchState.GetNodeByIndex(nodeIndex).PostingListId;
                if ((postingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) == Constants.Graphs.VectorId.Tombstone)
                    return;
                if (_filter is { } filter && _searchState.HasEntryInFilter(postingListId, filter, ref _filterScratch, ref _filterDecoder) == false)
                    return;

                var distance = _searchState.QueryDistance(_vector.Span, nodeIndex, ref _vectorReadCounter);
                // The queue keeps the nearest candidates with the farthest on top.
                if (_pq.Count < NumberOfCandidates)
                    _pq.Enqueue(nodeId, -distance);
                else
                    _pq.EnqueueDequeue(nodeId, -distance);
            }

            public bool TryGetCurrentCandidates(out ContextBoundNativeList<int> candidates)
            {
                if (_candidates.Count == 0)
                {
                    Unsafe.SkipInit(out candidates);
                    return false;
                }

                candidates = _candidates;
                return candidates.Count > 0;
            }

            public void IncreaseNumberOfCandidates(int currentlyAcceptedNodes)
            {
            }

            public long CandidatesProcessed { get; set; }
            
            public bool ShouldContinueSearch(long filterDocsCount)
            {
                return _pq is {Count: > 0};
            }

            public int NumberOfCandidates { get; init; }
        }
    }
}
