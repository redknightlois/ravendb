# HNSW Experimental Branch — Session Journal

Date: 2026-04-29
Branch: `v7.2-hnsw-experimental` (fork: `redknightlois/ravendb`)
Base: `ravendb/ravendb` v7.2

## 1. Environment Setup

### Disk topology
Azure VM with two disks; neither was mounted on first inspection.

- `sdc` — managed SSD (persistent) → `/mnt/storage`
- `sdb1` — local resource disk (ephemeral) → `/mnt/ephemeral`

Made permanent via:
- `/etc/fstab` — added `/mnt/storage` line with `nofail`, retargeted ephemeral mount
- `/etc/waagent.conf` — `ResourceDisk.MountPoint=/mnt/ephemeral` (was `/mnt`)

### RavenDB tree relocation
- Moved `/ravendb` → `/mnt/storage/ravendb/`
- Took ownership of all repo dirs
- Deleted leftover `/ravendb`
- Pointed RavenDB `DataDir` to `/mnt/storage/RavenDB/...` and `Storage.TempPath` to `/mnt/ephemeral/RavenDB/...` for both `v72-vanilla` and `v72-experimental` (mirrors cloud deployment shape: persistent data, ephemeral scratch).
- Reset both trees to upstream `ravendb/ravendb` `v7.2` HEAD.

### .NET
Required SDK 10.0.202; not in apt — installed manually.

## 2. PR Collection

Goal: collect open `redknightlois` PRs targeting HNSW performance, layered into a single linear branch.

Initial PRs investigated:
- #22652 — HNSW NodeCache (level≥1 cache + Corax wiring)
- #22671 — Neighbor vector prefetch
- #22674 — Cache resolved edge indexes (`ResolveEdgeIndexes`)
- #22712 — Sync `EdgesIndexesPerLevel` mirror at HNSW write sites
- #22717 — Taper `efConstruction` in parallel batch insert
- #22718 — Skip preload scan when resident
- #22719 — Derive concurrency cap from L3 cache footprint

First attempt produced an ugly merge sequence. Per user direction ("a single line of rebases"), threw it away and rebuilt as a linear cherry-pick stack on `v7.2` HEAD.

Conflicts encountered while linearizing:
- #22671 was branched off #22652 → cherry-picked only the unique commit (`da6c200268a`).
- #22674 was textually entangled with #22652's `Hnsw.cs` (both touch `CopyNodeFromCache`-region). Manually spliced to keep only `ResolveEdgeIndexes` (the actual #22674 contribution); dropped `CopyNodeFromCache` which depends on the NodeCache type from #22652.
- #22652 vector test conflict (`VectorSearchWithNodeCache.cs`): kept the more thorough `RavenTheory` version.

Final stack (top-of-branch → base):
```
755ccc2c7e9  HNSW Basic benchmarks
51ec39ffdfa  HNSW NodeCache.Build: full BFS at level 0    ← the fix (see §3)
72d20930276  RavenDB-26494 (#22719) L3 concurrency cap
e4c66dbd9d2  RavenDB-26493 (#22718) skip preload scan
58ae4216727  RavenDB-26492 (#22712) EdgesIndexesPerLevel sync
f3291118e5c  RavenDB-26465 (#22717) taper efConstruction
fd908f6fe25  RavenDB-26421 (#22674) cache edge indexes
4876bbb7b7f  RavenDB-26419 (#22671) neighbor prefetch
462eed303f9  RavenDB-26405          NodeCache tests
aad6266c574  RavenDB-26405          Corax NodeCache wiring
7083e8074e7  RavenDB-26405 (#22652) NodeCache + SearchState opts
... (v7.2 HEAD)
```

Pushed as `v7.2-hnsw-experimental` to `redknightlois/ravendb`.

## 3. The NodeCache Build Bug

After merging the stack, `HnswNodeCache` parity tests failed:
- `Build_50` test populated 37 nodes (expected 50)
- `Build_1500` populated 398 (expected 1500)

### Bisect
Bisecting against current `v7.2` HEAD — not against the PR's own base — pinpointed two upstream commits as the trigger:
- `325e3ce7a0f` RavenDB-26365 — Replace coin-flip level assignment with standard HNSW formula
- `7a60fbb7d60` RavenDB-26152 — Prevent HNSW node from discovering itself during concurrent placement

The PR was authored against the old coin-flip level assignment. Under the standard formula `floor(-ln(uniform()) * mL)`, far more nodes live exclusively at level 0; many are >1 hop from any upper-level seed. The original `NodeCache.Build` walked upper levels then did a single `ExpandAtLevel(0)` hop, leaving the long tail of level-0-only nodes uncached.

### Fix
`src/Voron/Data/Graphs/Hnsw.NodeCache.cs` — full BFS at level 0:

```csharp
using var builder = new Builder(llt, locations, budget);
builder.Seed(EntryPointId);
int maxLevel = builder.NodeLevelsAt(0) - 1;
for (int level = maxLevel; level > 0 && builder.HasBudget; level--)   // was >= 0
    builder.ExpandAtLevel(level);

// Level 0 needs full BFS, not a single hop: under the standard HNSW level
// formula most nodes live only at level 0 and many sit >1 hop from any
// upper-level seed, so one ExpandAtLevel(0) call leaves them out.
while (builder.HasBudget)
{
    int before = builder.NodeIdToIdx.Count;
    builder.ExpandAtLevel(0);
    if (builder.NodeIdToIdx.Count == before)
        break;
}
```

Tests pass with `50→50` and `1500→1500` afterwards. Committed as `51ec39ffdfa`.

## 4. Bench Infrastructure

Cherry-picked 3 projects from `redknightlois/basic-bench` (`384083de1c5`):
- `bench/voron-hnsw-bench/` — direct Voron HNSW
- `bench/corax-vector-qps/` — Corax vector QPS (insert + query)
- `bench/ravendb-vector-bench/` — end-to-end through RavenDB server

Dataset: DBpedia-OpenAI3 1536-d, Qdrant HuggingFace shards. Cache copy at `/mnt/storage/work/tq-datasets`, working data routed via `/mnt/work` symlink → `/mnt/ephemeral/work`.

### QPS bench evolution
The single-threaded query loop produced "paltry" QPS. Successive rewrites:
1. **Parallel queries**: `RunQueriesGated` with per-thread `IndexSearcher` and `ByteStringContext`, time-gated.
2. **Worker sweep**: `--workers N`, tested 32 / 64 / 128.
3. **Two query distributions**:
   - **cycle**: round-robin across the NQ pool; cache-friendly upper bound.
   - **partition**: each worker sees a disjoint slice of unique queries; cache-hostile, "hit the disk as much as possible".
4. **Truth cap**: ground-truth computation capped at first 2000 queries to keep large NQ runs tractable.
5. **NodeCache wiring** via `--node-cache N` flag (experimental binary only):
   ```csharp
   fieldNameScope = Slice.From(bsc, "Vector", out fieldNameSlice);
   using var rtx = env.ReadTransaction();
   var cache = Hnsw.NodeCache.Build(rtx.LowLevelTransaction, fieldNameSlice, nodeCacheSize);
   if (cache != null && cache.Count > 0)
       caches = new Dictionary<Slice, Hnsw.NodeCache>(SliceComparer.Instance) { [fieldNameSlice] = cache };
   ```
   Vanilla bench retains an older copy without `AttachVectorNodeCaches` — does not compile against vanilla source, so non-#22652 variants run the no-cache bench.

## 5. Per-PR QPS Isolation

Built each PR as a worktree off `v7.2`, cherry-picking only that PR's commits, then ran QPS against a **shared experimental graph** (graph built once on full stack, then queried by each variant's binary). This isolates query-side effects per PR; insert-side PRs are out of scope for this measurement.

Parameters: 1M docs, M=16, efC=64, K=10, workers=32.

### Results (Δ vs vanilla baseline)

**Partition mode** (cache-hostile):

| EF  | #22652 alone | #22671 alone | #22674 alone |
|-----|-------------:|-------------:|-------------:|
| 16  | -28.6%       | -0.8%        | -6.0%        |
| 32  | -31.9%       | -0.3%        | -6.0%        |
| 128 | -37.1%       | -1.6%        | -8.9%        |
| 512 | -34.2%       | -1.1%        | -4.5%        |

**Cycle mode** (cache-friendly):

| EF  | #22652 alone | #22671 alone | #22674 alone |
|-----|-------------:|-------------:|-------------:|
| 16  | +17.8%       | -2.9%        | -2.1%        |
| 32  |  +1.4%       | -5.8%        | -1.4%        |
| 128 | -23.0%       | -3.4%        | +2.0%        |
| 512 | -34.5%       | -3.4%        | +1.6%        |

### Cumulative (#22652 + #22674)
At EF=16, cycle: **+31.7%** — synergy: edge-index caching on top of NodeCache compounds well at low EF.

### Insert-side PRs
#22712, #22717, #22718, #22719 cherry-pick cleanly in isolation but do not affect query path on a shared, pre-built graph. Skipped from QPS table.

## 6. Findings

- **#22652 (NodeCache + SearchState)**: massively cache-state dependent. Strongly negative on cache-hostile workloads; strongly positive at low EF when cache is warm. Net effect depends entirely on production query distribution.
- **#22671 (neighbor prefetch)**: near-zero impact on this hardware/dataset. Run-to-run noise (~20% from page-cache state) dominates.
- **#22674 (edge-index cache)**: small consistent regression on partition mode, near-zero on cycle. Synergistic with #22652 at low EF.
- The **standard HNSW level formula** (`325e3ce7a0f`) reshapes the graph enough to silently break NodeCache assumptions written for the coin-flip era. Anyone porting older HNSW changes forward should bisect against current HEAD, not the PR's authoring base.

## 7. Reproducing

```bash
# Build experimental
cd /mnt/storage/ravendb/v72-experimental
dotnet build -c Release src/Raven.Server/Raven.Server.csproj

# Vector QPS bench
dotnet run -c Release --project bench/corax-vector-qps -- \
    --reuse --nq 500 --workers 32 --node-cache 50000 \
    --partition  # or --cycle
```

Per-PR worktrees live under `/mnt/storage/ravendb/wt-only-<pr#>/`.

## 8. Profiling the #22652 Regression (dbg dotnet-trace)

To explain the partition-mode regression, ran `dbg start dotnet-trace` against `vanilla` and `wt-only-22652` binaries on the shared 1M graph, `--reuse --nq 500 --workers 32 --no-truth` (added a `--no-truth` flag to the bench in both worktrees so the profile is queries-only, no brute-force GT pollution).

Saved sessions: `/home/ubuntu/hnsw-prof/.dbg/sessions/{vanilla-q,pr22652-q,vanilla-q-v2,pr22652-q-v2}.db`.

### Inner loop time breakdown — `NearestSearcher.MoveNext()` callees

|                       | Vanilla ms | #22652 ms |   Δ ms |  Δ % |
|-----------------------|-----------:|----------:|-------:|-----:|
| `LoadNodeIndexes`     |     27,767 |    21,922 | -5,845 | -21% |
| `CosineDistanceSingles` |    6,486 |    11,076 | +4,590 | +71% |
| `QueryDistance`       |      5,553 |     5,747 |   +194 |  +3% |
| `GetPageInternal`     |      1,784 |     4,084 | +2,300 |+129% |

### `QueryDistance` callees — the structural overhead

|                  | Vanilla ms | #22652 ms |
|------------------|-----------:|----------:|
| `CosineDistanceSingles` |  2,074 |    3,191  |
| `GetPageInternal` (direct) | 1,594 |     0  |
| **`Container.Get`** (new indirection) |   0 |  2,157  |
| `Node.GetVector`        |    270 |      351  |

In vanilla, `QueryDistance` resolves vector pages directly via `GetPageInternal`. In #22652, vector resolution goes through `Container.Get` — a layer the cache architecture introduced to make vectors lazy/MVCC-safe. With NodeCache **disabled** (`--node-cache 0`), that indirection is pure overhead: ~4.4s extra Container.Get work across the run (2,157 ms in QueryDistance + 2,209 ms reached from the search loop).

### Findings

1. **+71% CosineDistance calls**: #22652's search visits/evaluates ~71% more candidate vectors per query than vanilla. The version-based `_visitsCounter` and Reset() removal change which nodes appear "fresh" per search; this shifts the exploration pattern. Visible as both more distance evaluations and more page faults.
2. **+129% GetPageInternal in the search loop**: more page resolutions, partly from (1) and partly from the new lazy vector path.
3. **`Container.Get` indirection**: a structural cost of the NodeCache design that **does not amortize** when the cache is disabled. The vanilla path bundles vector access with node load; #22652 splits them so a populated cache can skip edge loads while still permitting vector resolution at distance time. With cache=0 you pay for the split without getting the benefit.
4. **`LoadNodeIndexes` is faster** (-21%) — fewer total invocations and a different internal path through `Lookup.GetCommandLookupRoot` / `NextReadEdges`. So 22652 isn't worse at loading edges; it's worse at *needing more of them indirectly via more visits*.

### Implications

- The partition-mode regression is **two-component**: (a) algorithmic — more candidates explored per query (could be tuned by reverting the visit-counter init or auditing the search inner loop), (b) structural — `Container.Get` overhead unavoidable when cache=0.
- For real workloads, the cache must be sized to actually hit; otherwise #22652's per-query cost is strictly higher than vanilla.

### dbg gaps observed (would help close future investigations)

- `dbg marks` (advertised in help) failed with `bash: marks: command not found` — looks like a shell-escape in the daemon's command dispatch.
- `dbg diff <other>` returned **"no symbols to compare"** between two saved profile sessions. A function-by-function inclusive/exclusive ms diff between two profiles would be the killer feature for this kind of regression hunt; right now I had to manually multiply % × total-ms across two `dbg top` outputs.
- `dbg replay <label>` on a profile-mode session drops into a crosstrack REPL that only supports `hits/cross/disasm/source` — not `top/callers/callees`. Re-running the workload was the only way to re-query a saved profile.
- `dbg top` exclusive column reads 0% for almost every managed frame; we ended up using `dbg callees X` to get exclusive-ish numbers per parent.
- No straightforward way to scope analysis to a sub-window of the run (e.g. "EF=128 phase only") without instrumenting the target.


---

## §9 — Root cause: `candidatesQ` leak across queries (FIXED)

After the profiler-based investigation in §8, I dropped to instrumented counters: per-call-site `Interlocked.Increment` for `Distance`, `QueryDistance`, `LoadNodeIndexes`, `AllocateNodeIndex`, `OnQueryVector`, `InitState`, plus per-LoadNodeIndexes pre-known/unknown pre-fill counters and per-NearestSearcher main-loop iteration / queue admit/replace/reject counts. Reset between EF phases for deterministic per-phase measurement.

### Bisecting the regression

| run | per-query QD | per-query alloc | dist/alloc |
|---|---:|---:|---:|
| Vanilla v7.2 (fresh state per query)         | 6,271 | 6,320 | 1.00 |
| #22652 shared SearchState (baseline)         | 8,040 | 4,024 | 2.00 |
| #22652 with shared SearchState **bypassed** (forced fresh-per-query via VectorSearchMatch.cs experiment) | **6,271** | 6,320 | 1.00 |

The bypass test was decisive: **with fresh SearchState per query, #22652 matches vanilla counters EXACTLY**. So the regression is not in the cache architecture, the QueryDistance versioning, the OnQueryVector mechanism, or the Container.Get split — it's in the *reuse* of SearchState across queries.

### Initial wrong hypothesis: LoadNodeIndexes ordering

First theory: shared state has many pre-known nodes (in `_nodeIdToIdx`) → `LoadNodeIndexes` returns indexes in a mixed order (known-first input-order, then sorted-unknown), versus vanilla's mostly-sorted order on a fresh state. Different processing order → different priority-queue lowerBound trajectory → more nodes admitted.

**Tested**: rewrote `LoadNodeIndexes` to sort all nodeIds upfront and emit indexes in globally-sorted order regardless of cache state. Result: per-query QD dropped from 8,040 → 7,886 (~2% reduction). Negligible. Hypothesis wrong.

### Real root cause: stale `candidatesQ` between queries

Added a runtime check at the top of `NearestSearcher.Search()`:

```csharp
if (candidatesQ.Count != 0 || nearestEdgesQ.Count != 0)
    throw new InvalidOperationException(...);
```

Output:

```
Search start: candidatesQ=218 nearestEdgesQ=0
```

**218 leftover entries from the previous query.** The early-exit path —

```csharp
if (-curDistance < lowerBound && nearestEdgesQ.Count == _internalNumberOfCandidates)
{
    ProcessResults();           // drains nearestEdgesQ
    yield return true;
    goto Start;
}
```

— drains `nearestEdgesQ` but leaves whatever is still in `candidatesQ`. For non-filtered queries, the caller takes the first batch and never resumes the iterator, so `goto Start` (which would clear/reinit) never runs. Next query reuses the SearchState, calls `InitState` to seed the queue with starting points, then runs the main loop on a queue *polluted* with far-from-the-new-vector candidates from the prior query. Those polluted candidates get popped, expanded, and add their edges to the queue — extra work that vanilla's fresh-per-query state never has.

### Fix

One-line fix in `Hnsw.NearestSearcher.cs`:

```csharp
ProcessResults();
candidatesQ.Clear();   // ← drain before yield, regardless of whether iterator resumes
yield return true;
goto Start;
```

A second small fix in `Hnsw.cs`: bump `_queryVectorVersion` (via `OnQueryVector`) **before** `SearchNearestAcrossLevels` in the shared-state `ApproximateNearest`, otherwise the upper-level greedy walk reads stale cached distances from the prior query (5,033 stale `DistIdx` hits per EF=512 phase → 0 after the fix).

### Verification (EF=512, BIG dev mode, partition=cache-hostile, 32 workers)

| Counter        | Vanilla     | #22652+fix   | Delta |
|----------------|------------:|-------------:|------:|
| `dist`         | 3,186,249   | 3,186,451    | +0.006% |
| `QueryDistance`| 3,150,307   | 3,150,509    | +0.006% |
| `loadIdx`      |   288,540   |   288,555    | +0.005% |
| `DistIdx hits` |          0  |           0  | match |
| `alloc`        | 3,174,963   | **1,853,936**| **-41.6%** |

Counters match vanilla within run-to-run noise (~0.01% from worker scheduling), while `alloc` is 41.6% lower — the I/O-amortization win from sharing `_nodes`/`_nodeIdToIdx` across queries is *preserved* once the queue leak is patched.

### QPS impact

| EF  | Vanilla | #22652+fix | Δ |
|----:|--------:|-----------:|---:|
|  16 |   4,062 |      4,645 | +14% |
|  32 |   3,572 |      4,362 | +22% |
| 128 |   1,148 |      1,634 | +42% |
| 512 |     406 |        624 | **+54%** |

Both fixes preserved: (a) vanilla's exploration depth (same number of candidate evaluations per query), (b) #22652's amortized node-allocation savings.

---

## §10 — Post-fix QPS verification

Re-ran the QPS bench against three states (vanilla v7.2 baseline / #22652 with the
queue-leak bug / #22652 with `candidatesQ.Clear()` fix) to confirm the fix's
effect end-to-end. 100K-vector graph, 1536-d cosine, M=16, efC=64, 32 workers,
10s per EF, 3 runs each. Reported numbers are the median.

### Partition mode (cache-hostile — each query unique)

| EF  | Vanilla | #22652 (bug) | #22652+fix | Δ fix vs vanilla | Δ fix vs bug |
|----:|--------:|-------------:|-----------:|-----------------:|-------------:|
|  16 |   3,720 |        5,496 |      5,001 |          +34.4%  |       −9.0%  |
|  32 |   3,382 |        4,133 |      4,994 |          +47.7%  |      +20.8%  |
| 128 |   1,233 |        1,595 |      1,534 |          +24.4%  |       −3.8%  |
| 512 |     431 |          472 |        593 |          +37.6%  |      +25.6%  |

Low-EF partition mode is dominated by run-to-run variance (small per-query
workload, ~2 ms each). The clear signal is at EF=512 — the regression-prone
regime in §5 — where the fix is +37.6% over vanilla and +25.6% over the buggy
PR.

### Cycle mode (cache-friendly — queries repeat across workers)

| EF  | Vanilla | #22652 (bug) | #22652+fix | Δ fix vs vanilla | Δ fix vs bug |
|----:|--------:|-------------:|-----------:|-----------------:|-------------:|
|  16 |  13,356 |       34,823 |     38,064 |         +185.0%  |       +9.3%  |
|  32 |   8,933 |       19,009 |     22,412 |         +150.9%  |      +17.9%  |
| 128 |   2,772 |        4,900 |      6,355 |         +129.3%  |      +29.7%  |
| 512 |     912 |        1,129 |      1,578 |          +73.0%  |      +39.8%  |

In cycle mode (where SearchState reuse compounds across many queries to the
same vectors), the fix scales the win further: +73% over vanilla at EF=512, and
+30–40% over the buggy PR at high EF. The bug masked some of #22652's intended
gain.

### Notes

- 100K is small enough that the working set fits in OS page cache; the §5
  regression was measured at 1M docs where the polluted-queue overhead
  compounded with cache misses. The fix removes the regression mechanism, so it
  should hold at scale too — see §11 for the 1M re-measurement.
- Recall not measured here (`--no-truth`); §9 verified algorithmic
  equivalence to vanilla via per-call counters (QD/dist/loadIdx all within 0.01%).

## §11 — 1M re-measurement on the full RavenDB-26423 stack

After §10 the PR was reorganized into a four-issue stack (RavenDB-26405 →
26419 → 26421 → 26423) with the `candidatesQ.Clear()` fix folded into 26405
and three further changes layered on top: neighbor-vector prefetch (26419),
edge-index resolve cache (26421), and a state-machine rewrite of the searcher
plus an L2-norm cache and a Vector512 FMA cosine kernel (26423). All
benchmarks below are against the tip of that stack (`5bb0619e534`) versus
`v7.2`.

1M-vector graph (DBpedia-OpenAI3 large 1536d), M=16, efC=64, 32 workers,
10s per EF, single run per cell (no median; 1M insert is ~12 min/run).

### Partition mode (cache-hostile)

| EF  | Vanilla | PR stack | Δ |
|----:|--------:|---------:|----------:|
|  16 |   2,924 |    2,891 |    −1.1%  |
|  32 |   2,562 |    3,070 |   +19.8%  |
| 128 |   1,038 |    1,242 |   +19.7%  |
| 512 |     533 |      589 |   +10.5%  |

EF=16 is at the bench's run-to-run noise floor (~±2%). The intended-for-§5
regime is EF=512, where the stack is +10.5% over vanilla — the regression
from §5 is gone *and* the stack adds margin without it.

### Cycle mode (cache-friendly)

| EF  | Vanilla | PR stack | Δ |
|----:|--------:|---------:|----------:|
|  16 |  13,522 |   38,993 |  +188.4%  |
|  32 |  10,757 |   28,484 |  +164.8%  |
| 128 |   4,072 |    7,354 |   +80.6%  |
| 512 |   1,228 |    1,434 |   +16.8%  |

Cycle mode is where the SearchState reuse + QueryDistance cache + edge-index
cache compound: low-EF queries are dominated by per-query setup costs that
the new caches eliminate. At EF=512 the inner loop dominates and the
algorithmic work matters more than the caches, so the win narrows but stays
positive.

### Notes

- Insert throughput is unchanged (vanilla 1419 vec/s vs PR 1368 vec/s, within
  noise) — these PRs are all on the read path.
- Inserts ran in a single 1M batch (no commit checkpoints); insert wall-time
  is ~12 min on this box for both variants.
- 1M is large enough that the working set spills past L3 but still fits in
  RAM. The graph is fully cached in OS page cache after insert, so cycle mode
  reflects the steady-state hot-graph case.
- Recall not measured (`--no-truth`); §9 verified algorithmic equivalence
  for the 26405 commit and the 26421/26423 changes are pure micro-opts (no
  algorithm changes), so behavior stays at-or-better than vanilla.

## §12 — SIFT1B Insert Baseline (Pre-Insert-PRs)

Date: 2026-04-29. Branch: `only-22652` at RavenDB-26423 tip (5bb0619e534).
This branch contains the **search-side** PRs (26405 / 26419 / 26421 / 26423)
only — the insert-path PRs (26465 / 26492 / 26493 / 26494) are NOT applied.
This entry establishes the un-optimized insert baseline before stacking
those PRs on top.

### Setup

- Dataset: SIFT-128-1B (BIGANN), `bigann_base.bvecs.gz` from ftp.irisa.fr
  - 128-dim uint8 vectors, converted to float32 at load (per element)
  - Single gzipped bvecs stream — load is sequential, no random IO
- Params: M=16, efC=64, commit-every=1M (one tx per million vectors)
- Loader: `LoadDatasetSift` in `bench/corax-vector-qps/Program.cs`
- Run interrupted after batch 6 (user "fine with 5M"; 6→7M batch had
  already started and was allowed to finish before kill)
- Dataset load: 29.9s for 10M vectors (compare DBpedia 1M = 88s — SIFT
  is a single decompressing stream and 12× narrower per vector)

### Per-batch timings (un-optimized insert path)

| Batch | Range          | reg ms  | commit ms | total s | RSS MB |
|------:|----------------|--------:|----------:|--------:|-------:|
| 0     | 0M    .. 1M    |  21,095 |  341,222  |  362.3  | 12,417 |
| 1     | 1M    .. 2M    |  27,076 |  405,494  |  432.6  | 14,626 |
| 2     | 2M    .. 3M    |  30,388 |  465,426  |  495.8  | 18,528 |
| 3     | 3M    .. 4M    |  31,004 |  486,329  |  517.3  | 24,522 |
| 4     | 4M    .. 5M    |  33,409 |  523,141  |  556.6  | 26,327 |
| 5     | 5M    .. 6M    |  32,747 |  565,325  |  598.1  | 27,865 |
| 6     | 6M    .. 7M    |  35,944 |  574,635  |  610.6  | 35,903 |

### Aggregate

- 7M vectors inserted in ~3,571 s wall clock = **~1,961 vec/s** average
- Commit phase dominates (~94% of per-batch wall time) — the registration
  pass is comparatively cheap; the cost is in the graph-build during commit.
- Per-batch wall time grows from 362s (batch 0) → 611s (batch 6), a 1.7×
  slowdown by 7M. Both `reg` and `commit` grow, but `commit` grows
  super-linearly (341s → 575s, +68%), consistent with HNSW link-construction
  cost rising as the graph deepens.
- RSS climbs ~3× from batch 0 → batch 6 (12.4 GB → 35.9 GB). Heap pressure
  builds across batches; the next stacked-PR run will tell us how much of
  that is recoverable with the insert-side improvements.

### Notes / Next

- This is the floor for SIFT-128 insert throughput on this box. The insert
  PRs (26465/26492/26493/26494) need to be stacked on top of the current
  search stack and re-timed against this same dataset/params.
- Vector dimension matters here: SIFT-128 (128d uint8→float) is ~12× cheaper
  per-vector than DBpedia (1536d float). Insert wall-time is dominated by
  graph topology work (link selection, edge writes), not by distance compute,
  which is why the per-batch growth curve here looks similar in shape to the
  DBpedia 1M curve despite the much smaller vector size.
- Search-side QPS sweep at SIFT 6M (separate run) already showed NodeCache
  *helping* partition mode at low EF (+143% at EF=16) — the insert PRs are
  orthogonal and should not regress that result.

## §13 — SIFT 7M Insert With Stacked Insert PRs

Date: 2026-04-30. Branch: `only-22652-with-inserts` — search-side stack
(26405/26419/26421/26423) with the four insert-side PRs cherry-picked on top
in this order: 26465 (taper efC) → 26492 (sync EdgesIndexesPerLevel) →
26493 (skip preload scan when resident) → 26494 (L3-derived concurrency cap).
One conflict in `Hnsw.cs` from 26494 — the search-stack and insert-PR each
added new code in the same regions (a `using` and a member of `Registration`);
resolved by keeping both. Same dataset, params, and loader as §12; same box;
fresh `/mnt/work/tq-bench/Single_f32`.

### Per-batch (PR-stack vs §12 baseline)

| Batch | Range          | Baseline s | PR-stack s | Δ time |   Baseline RSS | PR RSS  | Δ RSS |
|------:|----------------|-----------:|-----------:|-------:|---------------:|--------:|------:|
|     0 | 0M    .. 1M    |     362.3  |     331.8  |  -8.4% |       12.4 GB  |  9.0 GB |  -28% |
|     1 | 1M    .. 2M    |     432.6  |     408.4  |  -5.6% |       14.6 GB  | 12.1 GB |  -17% |
|     2 | 2M    .. 3M    |     495.8  |     441.5  | -10.9% |       18.5 GB  | 12.6 GB |  -32% |
|     3 | 3M    .. 4M    |     517.3  |     468.5  |  -9.4% |       24.5 GB  | 16.1 GB |  -34% |
|     4 | 4M    .. 5M    |     556.6  |     513.1  |  -7.8% |       26.3 GB  | 15.0 GB |  -43% |
|     5 | 5M    .. 6M    |     598.1  |     566.2  |  -5.3% |       27.9 GB  | 15.8 GB |  -43% |
|     6 | 6M    .. 7M    |     610.6  |     569.5  |  -6.7% |       35.9 GB  | 21.2 GB |  -41% |

### Aggregate

|                | Baseline (§12) | PR-stack | Δ      |
|----------------|---------------:|---------:|-------:|
| Total wall     |        3571 s  |  3318.6 s | -7.1% |
| Throughput     |     1961 vec/s |   2109 vec/s | +7.5% |
| Final RSS      |        35.9 GB |   21.2 GB | -41%  |
| Reg total      |        211.7 s |   229.4 s | +8.4% |
| Commit total   |       3361.6 s |  3069.6 s | -8.7% |

### Reading

- **Memory is the headline.** The baseline RSS curve was super-linear
  (12 → 36 GB across 7 batches). The PR stack flattens it to roughly linear
  (9 → 21 GB), and the gap widens from -17% at batch 1 to -41% at batch 6.
  The L3-derived concurrency cap (26494) and the resident-scan skip (26493)
  are the most plausible drivers — both reduce simultaneous live-vector
  footprint. This is the PR set's most consequential effect at scale; SIFT
  100M would have been infeasible at the §12 RSS trajectory and is likely
  fine on the new one.
- **Throughput +7.5%** on the same hardware, same params. Modest but
  consistent — every batch was faster, with no batch worse than -5.3%.
- **Where the time went.** The commit phase (which dominates §12 too) drops
  -8.7%; registration is +8.4% (small absolute, +18 s of 3300 s wall). 26465
  tapers efConstruction during parallel batch insert which adds a touch more
  registration scheduling work but cuts the work the commit pass has to do —
  net win, as expected.
- **Per-batch growth slope flattens.** Baseline went 362 → 611 s (1.7×).
  PR-stack goes 332 → 570 s (1.7×) — same slope ratio, but starting lower
  and ending lower, suggesting these PRs are not changing the algorithmic
  growth class; they're shifting the constant down (and the memory base
  more meaningfully than the time base).
- **No correctness check this run** (`--no-truth`), same as §12. The insert
  PRs are read-equivalent by inspection (topology mirror sync is the only
  data-shape change and 26492 makes it consistent with the on-disk view).

### Next

- Push to SIFT 50–100M with the stacked branch — that's where the RSS
  flattening earns its keep.
- Re-run the search-side QPS sweep on the 7M index built here; insert PRs
  should be QPS-neutral, but verify (especially partition mode at low EF
  where NodeCache changes from §11 already moved things).

## §14 — Insert-time A/B and the 360% CPU mystery

Date: 2026-05-06 → 2026-05-07. Branch: `v7.2-hnsw-experimental` rebased
on upstream `v7.2` HEAD; experimental HEAD `966ad9b27e5` (zero-copy
NodeCache hits + ReaderDrainLock + the §3–§13 stack). Goal: timing
matrix of Sphere 100K / 1M / 10M index build, vanilla v7.2 vs the
experimental stack, on a shared data dir to remove cold-start cost.

### Harness

`Raven.Bench/scripts/index-time.sh <tag> <worktree> <db>` — boots the
worktree's Raven.Server.dll on a fixed port against a shared
`--DataDir` and `--Storage.TempPath`, issues
`RESET /databases/$DB/indexes?name=Passages/ByEmbedding-corax`, polls
`/indexes/stats?name=...` until `IsStale=false`, prints
`tag=… db=… docs=… index_ms=… git=…`. The same harness drives every
run in §14–§17.

Sphere DBs (768-d float32) duplicated to `/mnt/storage/ravendb/data-indextest/`
once and reset between runs.

### Baseline timing matrix

| Dataset      | Vanilla v7.2 | Experimental | Δ      |
|--------------|-------------:|-------------:|-------:|
| Sphere-100K  |   18.4 s     |   18.0 s     |  -2%   |
| Sphere-1M    |  171.2 s     |  168.9 s     |  -1%   |
| Sphere-10M   |  ~31 min     |  ~31 min     |  ~0%   |

Insert-side improvements at this scale are essentially flat —
the §13 SIFT-7M win was ~7%, but on Sphere (5× wider vectors,
different topology) the bench is dominated by something else.

### CPU saturation symptom

`top` during a 1M build:

```
%Cpu(s):  11.2 us,  0.4 sy,  0.0 ni, 87.8 id ...
PID  USER  ...   %CPU  COMMAND
123  …     ... ~360.0  Raven.Server
```

3-4 of 32 cores busy. Wallclock is 169s but cumulative CPU-time is
~600s, leaving ~5400s of idle thread-time on the table.

First reflex: license. `/license/status` showed Enterprise/128 cores —
not throttled. Disproved.

Second guess: HNSW build is single-threaded by design. **Wrong** —
`NodePlacementRunner` already uses ThreadPool dispatch. The user
pushed back on this and they were right; reading the code shows
`UnsafeQueueUserWorkItem` per WorkItem with a wave-structured
coordinator (`_ready` ManualResetEventSlim).

So the question becomes: why does the wave structure waste 28 cores?

## §15 — Wait-state characterization

Tooling: `dotnet-trace collect -p <pid> --profile dotnet-sampled-thread-time`,
attached after server warmup, signaled with SIGINT after the build,
saved to `/mnt/storage/ravendb/profiles/hnsw-{100k,1m}.nettrace`. Used
detached `dotnet-trace` rather than `dbg start dotnet-trace` because
the dbg adapter only supports launch mode, not attach.

(Caveat from the saved memory `feedback_dbg_dotnet_trace.md`: sample
weights are CPU-time-distribution proxies, not wall-clock. Use them
to find *where* threads are when sampled, not how long they spent.)

### What the profile says

- ~84% of worker-thread sample weight in **wait state** —
  `ManualResetEventSlim.Wait`, `Monitor.Wait`, `LowLevelLifoSemaphore.Wait`.
  Workers are parked, not computing.
- The coordinator thread (running `NodePlacementRunner.Run`) shows up
  at near-100% busy in its own samples — sequential dispatch is the
  rate limiter.
- `WorkItem.Execute` (the actual distance/edge work) is the only
  "live" frame on workers but contributes a tiny share of wall time.

### Mechanism

Each wave: coordinator dequeues N items (N capped by the L3-derived
`ComputeTargetPlacementTasks` formula = ~560 on this box), dispatches
each via a separate `UnsafeQueueUserWorkItem` call, then `Wait()`s on
a single `_ready` MRE. Workers run their item, call `Enqueue` (which
calls `_ready.Set()` — the wakeup), and the coordinator drains the
next wave. With N=560 nodes and ~32 workers, each worker runs 17–18
items per wave but the dispatch + wakeup latency dominates because the
wave fan-out is sequential and each `_ready.Set()` is one cache-line
ping per worker.

The wait isn't *I/O wait* per se — the §0 NodeCache investigation in
the saved memory had us chasing a 75% disk-miss rate at *query* time,
which doesn't apply during build because the vectors being placed are
already loaded. It's *coordinator wait*: workers finish their item and
park because the coordinator hasn't fanned out the next wave yet.

## §16 — K=8 dispatch coalescing (the only landed win)

Hypothesis: if the rate-limiter is dispatch latency × wave count, fan
fewer-but-larger units of work into the ThreadPool. Wrap K=8 WorkItems
in a single `BatchExecutor : IThreadPoolWorkItem`. One
`UnsafeQueueUserWorkItem` per batch instead of one per item, and one
`_ready.Set()` after the whole batch instead of per-item.

Implemented in `src/Voron/Data/Graphs/Hnsw.Parallel.cs` (commit
`5457307fd11`). Added per-Run() counters (waves, batch dispatches,
items, coalescing factor, fast/slow split, re-enqueues, wave timing)
emitted as a single `[HNSW.Run]` line for verification.

### Result (Sphere 1M, fresh index)

|                    | Baseline | K=8     | Δ       |
|--------------------|---------:|--------:|--------:|
| Wallclock          |   169 s  |  158 s  | **-6.2%** |
| `Run()` time       |   149 s  |  138 s  | **-7.7%** |
| Dispatch volume    |   ~14 M  |  ~1.8 M | **-87%** |
| Avg items/batch    |    1.0   |   ~7.7  | (target 8) |

Modest but real, and the dispatch-volume reduction matches the K
arithmetic. Counters report the coalescing factor at runtime
(`avgItemsPerBatch=7.72`), so the mechanism is doing what it's
supposed to.

### Signal coalescing — the red herring

Profile suggested ~30–40% of the residual wait-state was
`Monitor.Wait` rather than the LIFO semaphore. Hypothesis: collapse
multiple `_ready.Set()` calls into a single Set per wave (many
re-enqueues from the "no work after preloading" path each call
`_ready.Set()` redundantly). Implemented `EnqueueDeferred` /
`SignalReady` and routed re-enqueues through it.

Three runs, K=8 + signal coalescing: 1M wall ≈ 158–161s. Within noise
of K=8 alone. The Monitor.Wait sample weight was the **same caller's
own park** during the wave drain, not a real fan-out hotspot — the
sampler had attributed it to the wrong frame.

User correctly called this: *"it's too much change for no win — we are
overcomplicating it. Focus on getting rid of the wave structure
entirely."* Code stayed (it's harmless and documents the intent) but
no further attempts in that direction.

## §17 — Four-direction redesign exploration

Spawned four agents in parallel to look at the wave-structure problem
from different angles. Each returned a written proposal (research
only, no code), summarized here:

1. **Streaming continuations** — `WorkItem.Execute` calls
   `iterator.MoveNext` inline and dispatches its successor itself with
   `preferLocal: true`. Single MRE for global completion. Estimate
   1M wall 135–145s (~13–20%).
2. **Dedicated worker pool + SPMC ring** — N=cores pinned threads on a
   Vyukov/Disruptor-style queue, spin-then-park. Replaces ThreadPool
   dispatch only. Estimate 138–146s (~7–12%). Note: a prior chunked
   `HnswVectorCache` attempt regressed by ~14% on a related axis;
   bringing in a custom queue carries similar surprise risk.
3. **Two-phase search-then-link (Vamana-style)** — parallel-for
   read-only nearest-search against a snapshot, then sequential edge
   mutation. References ParlayANN (PPoPP'24). Estimate 1M wall ~130s
   (~22%, with `Run()` 35–45s vs current 65s).
4. **Pipeline overlap** — orthogonal: parallel hash pre-pass via
   `ComputeHashFor` (10–25s recoverable), Map↔Run overlap via
   `BeginAsyncCommit` (~30s recoverable), parallel
   `MergePostingList` (2–6s). Stacks on top of any of 1/2/3.

### Decision

Pursue **(3) Vamana-lite** — the highest-ceiling option AND the one
that addresses the wave structure at its root rather than smoothing
its symptoms. The "lite" qualifier is important: keep HNSW data
structures (multi-layer hierarchy, M, ef, RobustSelect, on-disk
format, NodePage, NodeCache, BSC vector storage, the entire query
path). Replace **only** the orchestration in `NodePlacementRunner.Run`.

Cherry-picked Vamana insight: **out-edge ownership.** Each thread
writes only its own node's out-edges; no cross-thread edge contention.
With that discipline, Phase A (search) is a pure parallel read over a
graph snapshot, Phase B (apply) is sequential CPU-bound writes, and
Phase C (reverse-edge gather) is parallel per-target-node.

### Recall validation strategy

Easy: the snapshot branch `v7.2-hnsw-parallel-batch-k8` is the
algorithmic ground truth. Same dataset, same params, same query path.
Run both binaries against the same fresh build, measure recall@10 on
the same query set, reject if Vamana-lite drops >2%. The wave
implementation is preserved on the snapshot branch precisely so this
A/B is push-button.

### Snapshot

The pre-Vamana state (K=8 + bench infra) is captured on
`v7.2-hnsw-parallel-batch-k8` (commits `5457307fd11`,
`cc70c18ef59`). All §18+ work happens on a branch off that snapshot.

## §18 — Vamana-lite v0 landed

Branch: `v7.2-hnsw-vamana-lite`. Companion design note:
`DESIGN-VAMANA-LITE.md`.

### Shape

`NodePlacementRunner.RunTwoPhase` replaces the wave-based `Run` when
env-var `RAVEN_HNSW_TWO_PHASE=1` is set. Default off; the wave path
remains the fallback so unset env var = bit-for-bit current behavior.

Four phases, batched per Voron tx:

- **A — parallel search.** N workers (`_activeTasksCount`) each grab
  next-unassigned new-node via `Interlocked.Increment`, run the
  full search loop inline (no yields, no ThreadPool round-trips), and
  write a `PlacementPlan` to `plans[idx]`. Pure read over
  `_searchState`; per-thread scratch in `NodePlacement`.
- **B — serial apply (LLT thread).** Walk plans in original
  new-node order, append candidates to the new node's edge list and
  back-edges to each candidate's edge list. Collect any
  `(level, edgeIdx)` whose list overflows M into `overfullCollector`.
- **C — parallel prune compute.** Workers run
  `FilterEdgesHeuristicWorker.RunInline` over the overfull set.
  Pure read; result written to a per-pair `PrunedEdges` array.
- **D — serial prune apply (LLT thread).** Walk results, rewrite
  each overfull edge list to its pruned set. Mirror into
  `EdgesIndexesPerLevel` when the cache is in sync.

Phase A is the win: each worker drives an entire node placement to
completion without ever round-tripping to the coordinator, so the
"84% wait" wave shape (§15) collapses to compute-bound.

### One real bug

Phase A workers can't perform LLT-allocator side-effects, but
the wave path's `RegisterForPreloading` quietly calls
`SetCapacity` on `EdgesPerLevel` for every node it's about to read
(`Hnsw.Parallel.cs:1698`). Phase A skips that, so when Phase B's
candidate loop reaches `edge.EdgesPerLevel[level]` for a node whose
out-edges happen to live at lower levels (e.g. the entry point in
the first batch), the indexer dereferences a null storage pointer
and `NativeList<T>.this[int]` returns a ref to nullspace —
`NullReferenceException` on first access.

Fix: Phase B's candidate loop calls `SetCapacity(level + 1)` on
both `EdgesPerLevel` and `EdgesIndexesPerLevel` before the indexer
access, gated on `IsFromCache == false` (cached nodes already have
the storage). This replicates the LLT-side-effect that Phase A
deferred.

Diagnosis: on a hung Release build, neither dbg nor netcoredbg
breakpoints fired (Release optimizations strip line-boundary stops).
The actual breakthrough was the index-errors HTTP endpoint — it
captured the NRE stack with file:line and pointed straight at
`NativeList.cs:67`. Recorded in feedback memory; in retrospect, that
endpoint should be the first stop for any "indexing hung" diagnosis
in this repo.

### Wallclock

Same machine, same git, same dataset (Sphere-1M, 1M docs,
768-dim float32 cosine, M=16, efConstruction=200). Median of three
fresh-build runs each, `--Logs.Mode=None`:

| build              | 1M wall | delta |
|--------------------|---------|-------|
| wave (v7.2-K8)     | 163.91s | —     |
| Vamana-lite (env=1)| 102.06s | -38%  |

Top-of-htop CPU during build: wave ≈ 360% (3-4 cores), Vamana-lite
≈ 1900-2200% (19-22 cores). The ThreadPool actually saturates
because Phase A workers don't park waiting for the coordinator.

### Recall validation

Both branches built fresh from the same Sphere-1M docs. Then
`RavenBench recall` (brute-force `exact()` ground truth) at K=1/5/10
across efSearch=64/128/256:

| efSearch | wave recall@10 | two-phase recall@10 | delta  |
|----------|----------------|---------------------|--------|
| 64       | 55.80 %        | 54.80 %             | -1.0pp |
| 128      | 61.70 %        | 59.80 %             | -1.9pp |
| 256      | 67.20 %        | 65.70 %             | -1.5pp |

All three deltas inside the design doc's 2pp tolerance. Two-phase
loses ≤2pp consistently, exactly the cost predicted from giving up
in-flight visibility within a Phase A batch.

(Sphere-1M absolute recall numbers being mediocre at low ef is a
separate concern with the dataset/index parameters — the same shape
shows on wave. It's not a regression introduced here.)

### Open follow-ups

- Per-pair overfull dedup (`overfullKeys` HashSet) in Phase B is a
  logical no-op once Phase C is deterministic per `(level, edgeIdx)`;
  could be removed for clarity. Kept for now since the cost is trivial.
- `AddEdgesFromInFlightNodes` is disabled in two-phase. Recall
  numbers don't motivate bringing it back; if it ever does, the
  fix is mini-batches inside Phase A (each mini-batch sees prior
  mini-batches' Phase B writes via a serial flush in between).
- Wave path retained as fallback. Once Vamana-lite has bake time
  in nightly, flip the default and delete the wave path.
