# Vamana-lite design note (HNSW parallel-build redesign)

Branch: `v7.2-hnsw-vamana-lite` off `v7.2-hnsw-parallel-batch-k8` snapshot.
Companion to journal §17. This file is the working spec; it's expected to
churn as the implementation progresses, then get folded back into the
journal when the work lands.

## The constraint we initially missed

`src/Voron/Data/Graphs/Hnsw.Parallel.cs:698`:

> *"This works opposite to how you'll usually think about such runners.
> It is running everything in a single threaded (because it uses the
> single threaded transaction) and offload computational work to the
> thread pool."*

Voron's write transaction is single-threaded by construction. Graph
mutations (touching `EdgesPerLevel`, `EdgesIndexesPerLevel`, allocating
through `_searchState.Llt.Allocator`) **must** happen on one thread —
the same thread that owns the LowLevelTransaction. The wave structure
isn't an accident of design; it's the direct consequence of that
constraint.

What this rules out:
- Putting mutations on multiple Parallel.For workers under a lock
  (Voron's tx-context is thread-affinity-checked, not lock-protected).
- "Streaming continuations" agent #1 proposal as written — workers
  continuing the iterator inline would mutate from the wrong thread.
- Any design where Phase B is parallelized.

What stays viable:
- Pure-read Phase A on workers, serial Phase B on the coordinator
  (the LLT-owning thread).

## Today's parallelism shape — what we actually have

Re-reading `NodePlacement` and `NodePlacementRunner`:

- **Coordinator thread** (the one that called `Run()`): owns the LLT.
  Advances iterators via `MoveNext()`. Runs all graph mutations
  (lines 280-348 of FindGraphPlacementForNode).
- **ThreadPool workers**: only execute the three pure-compute workers
  — `FindNearestWorker`, `ProcessEdgesWorker`,
  `FilterEdgesHeuristicWorker`. None of these mutate the graph; they
  only read `_searchState` and write to per-`NodePlacement` scratch
  state (`_candidates`, `_indexes`, `_vectors`, `_candidatesQ`,
  `_nearestEdgesQ`, etc.).
- The "wave" exists because every yield round-trips to the coordinator
  for the next dispatch.

So the 84% wait-state on workers in §15 isn't I/O. It's: workers
finish their (cheap) compute primitive, dispatch the iterator back
to the coordinator, and park in `LowLevelLifoSemaphore.Wait`
because the coordinator has to advance N iterators before fanning
out the next wave.

## The Vamana cherry-pick

Vamana / DiskANN doesn't have our LLT constraint, but it teaches
**out-edge ownership**: thread T writes only node T.id's
out-edges. We can't apply that literally (LLT). But we can adapt
the shape:

```
Phase A (parallel, read-only):
  for each new node in the batch (parallel-for over workers):
    plan[node] = ComputePlacementPlan(node)   // no mutation

Phase B (serial, on LLT thread):
  for each new node in batch order:
    ApplyPlacementPlan(node, plan[node])      // mutates graph
```

Phase A is pure parallel reads — every distance computation, every
heuristic prune, every level-traversal. Workers can saturate the
ThreadPool because there's no coordinator round-trip per yield;
the entire search runs inline on one worker per node.

Phase B is sequential on the LLT thread, just walking and writing.
The work it does is ONLY the lines 280-348 mutations from current
`FindGraphPlacementForNode` — no distance compute, no candidate
selection. CPU-bound writes only, expected to be fast.

**Why "fast" is realistic here**: Voron writes during a tx land in
scratch space (in-memory paged buffer), not the data file. Edge-list
mutations are NodePage updates — small writes to scratch pages that
are typically resident. No fsync, no commit-per-mutation, no syscall
per write. So "serial Phase B" is bounded by memory bandwidth, not
by disk. This is also why removing the wave coordinator only saves
us *latency*, not *throughput* — there was never an I/O wall on the
mutation path.

Caveat: Phase B still includes prune compute (the heuristic filter
that runs when an existing node's edge list overflows M after a new
back-edge is added). That's distance computation, not memory writes,
and serialized in Phase B it could become the new bottleneck. v0
keeps it inline; if measurement shows it dominates, add Phase C
(parallel prune-compute) + Phase D (serial apply-prune).

## What gets refactored

In `Hnsw.Parallel.cs`, `NodePlacement.FindGraphPlacementForNode`
(currently ~110 lines, mixes search+mutation) splits into:

```csharp
private void ComputePlacementPlan(
    int createdNodeIndex,
    int currentNodeIndex,
    out PlacementPlan plan);

private void ApplyPlacementPlan(
    int currentNodeIndex,
    in PlacementPlan plan);
```

Where `PlacementPlan` carries:
- `int NodeRandomLevel`
- `UnmanagedSpan InsertedVector` (or pointer thereto)
- For each level 0..NodeRandomLevel:
  - `int[] PrimaryCandidates`  // node-indexes of out-edges from new node
  - For each existing edge that needed re-pruning:
    - `int EdgeIdx` (which existing node's edges are being rewritten)
    - `int[] PrunedNeighbors`  (replacement for that edge's edge list)

`ComputePlacementPlan` runs the search loops inline (calls workers'
`DoWork()` directly — no yields, no dispatch). Does NOT call
`AddEdgesFromInFlightNodes` or any graph-write.

`ApplyPlacementPlan` does only the writes:
- `EnsureEdgesOwned` for the inserted node
- For each level's `PrimaryCandidates`: append to inserted node's
  edge list AND append back-edge to each candidate's edge list
- For each `(EdgeIdx, PrunedNeighbors)`: rewrite that edge's
  edge list

The existing `FindGraphPlacementForNode` becomes `Compute → Apply`
called sequentially — keeps the wave-based `Run()` working bit-for-bit
identical (Phase A is just "all the yields fused into one inline call",
Phase B is the same mutations as today). So the wave path is preserved
behind an `if` for A/B testing.

## What we lose, and why it's acceptable

**Within a single Phase-A batch, in-flight node visibility goes away.**
Today, `AddEdgesFromInFlightNodes` (line 364) seeds the new node with
edges to other nodes currently being placed by parallel `NodePlacement`
instances. This biases the graph to cluster temporally-related vectors
(same document's chunks). Two-phase loses that.

Mitigation:
1. The bias only matters when batches span related documents AND those
   documents' vectors land in the same Phase-A slice. For random/large
   datasets the impact is small.
2. Vamana's published recall numbers (without anything analogous) hold
   ~96% recall@10 on the same test sets where HNSW gets 97-98%. We
   accept ≤2% recall loss as the validation bar.
3. If recall regresses, Vamana's "two pass with rising alpha" pattern
   is available — Phase A runs twice, second time seeing Phase B's
   first-pass writes.

**Across batches, in-flight visibility is preserved**: each Voron tx
batch ends with a commit, and the next batch's Phase A reads the
just-committed graph. So the loss is bounded to within-batch (~10K
nodes typically).

## Phase A parallelism mechanics

Two concrete options:

**A1: `Parallel.For` over the new-node range.**
```csharp
Parallel.For(0, _searchState.CreatedNodes, (createdNodeIndex) =>
{
    var placement = _placementByThread.Value; // ThreadLocal<NodePlacement>
    var currentNodeIndex = _searchState.GetCreatedNodeIndex(createdNodeIndex);
    placement.ComputePlacementPlan(createdNodeIndex, currentNodeIndex,
                                    out var plan);
    _plans[createdNodeIndex] = plan;
});
```

Simple, but Parallel.For uses ThreadPool with poor partitioning for
heterogeneous work; HNSW node-placement varies by 10x in cost
(level-0-only nodes are cheap, level-3+ nodes traverse more).

**A2: Reuse the existing `_activeTasksCount` worker pool with a
deferred-result variant of `NodePlacement.Process()`.**
Workers grab next-available-node via `Interlocked.Increment` on
`parent._nextNodeIndex` (already the existing pattern), run search
inline, write plan to a slot. Exit when the index passes the batch
end.

A2 reuses the existing concurrency cap (`ComputeTargetPlacementTasks`,
the L3-derived value). Picking A2.

## Phase B mechanics

Phase B walks plans **in original-node-order** to preserve graph
determinism (Phase A finishes in nondeterministic order; if Phase B
applies in arrival-order, two builds with the same input could produce
different graphs). The ordering matters because back-edge prune
sequencing affects which edges survive when M is exceeded.

```csharp
for (int idx = 0; idx < _searchState.CreatedNodes; idx++)
{
    ApplyPlacementPlan(_searchState.GetCreatedNodeIndex(idx),
                        in _plans[idx]);
}
```

## Recall validation against snapshot

The branch `v7.2-hnsw-parallel-batch-k8` is the algorithmic ground
truth (same params, wave-based build). Validation procedure:

1. Build the same dataset (Sphere 1M) on both branches. Use the same
   seed via `--Indexing.Vector.RandomSeed` (or whatever the option is).
2. Run `bench/corax-vector-qps/Program.cs --reuse --no-truth=false
   --nq 2000` against each binary, computing recall@10 from
   ground-truth.
3. Reject Vamana-lite if recall drops more than 2 points.

## Implementation plan (one commit)

Single commit landing the whole thing on `v7.2-hnsw-vamana-lite`:

1. Refactor `FindGraphPlacementForNode` into `ComputePlacementPlan` +
   `ApplyPlacementPlan`. Existing `Run()` calls them sequentially
   inside the iterator path so the wave path stays bit-for-bit
   identical.
2. Add `RunTwoPhase()` on `NodePlacementRunner`. Phase A reuses
   `_activeTasksCount` worker threads, each running
   `ComputePlacementPlan` inline (no ThreadPool dispatch, no yields)
   on its assigned node range. Phase B walks plans in node order on
   the LLT thread calling `ApplyPlacementPlan`.
3. Wire env-var gate `RAVEN_HNSW_TWO_PHASE=1` at the
   `InsertVectorsToGraph` site. Default off. Make the wave path the
   fallback so unset env var = current behavior.
4. Validate locally:
   - `dotnet build -c Release` clean.
   - Existing FastTests vector tests pass.
   - Sphere 1M index-time A/B (env-var on vs off) shows ≥10% wall
     improvement.
   - Recall@10 against `v7.2-hnsw-parallel-batch-k8` snapshot stays
     within 2 points.

## Open questions

- `_inFlightIndexes` and `AddEdgesFromInFlightNodes`: drop entirely
  in two-phase, or apply at Phase B start? Drop for v0; revisit if
  recall regresses.
- `ComputeTaperedEfConstructionForLevel0`: depends on
  `createdNodeIndex` and `CreatedNodes`. In two-phase, all nodes in
  the batch see the same starting graph state, so the taper should
  be evaluated once at batch start, not per-node. Probably small
  effect either way.
- Voron-tx pages allocated for new nodes during Phase B: today,
  `EnsureEdgesOwned` and `SetCapacity` may be called on the new
  node's `EdgesPerLevel` from the iterator path during search.
  Two-phase moves all of those to Phase B; need to verify nothing
  in the search path requires them to be set.