using System;
using System.Collections.Generic;
using Voron.Util;

namespace Voron.Data.Graphs;

public interface IHnswSearcher : IDisposable
{
    // Find nodes accepted by the query vector.
    // Guarantee: always returns previously unseen nodes.
    // To retrieve the accepted nodes, call TryGetCurrentCandidates.
    public IEnumerable<bool> Search();

    // Retrieve IDs of nodes from the current search batch.
    // Guarantee: always returns previously unseen nodes.
    // If no new nodes are found, returns false.
    public bool TryGetCurrentCandidates(out ContextBoundNativeList<int> candidates);

    public void IncreaseNumberOfCandidates(int currentlyAcceptedNodes);

    // Adaptive efSearch(q) continuation: explicitly raise the candidate
    // target. SearchState (cached distances) is preserved; queues and visit
    // markers are reset so the next Search() runs to the larger target while
    // skipping I/O on already-distanced nodes. Returns true if the target
    // was actually grown, false if target ≤ current internal target.
    public bool SetCandidateTarget(int target);

    // Returns the number of nodes processed by the searcher as candidates. It does not count nodes only queued for consideration.
    public long CandidatesProcessed { get; }

    public bool ShouldContinueSearch(long filterDocsCount);
    
    // Number of candidates requested by the caller.
    public int NumberOfCandidates { get; }
}
