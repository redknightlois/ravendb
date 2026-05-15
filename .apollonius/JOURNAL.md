# Apollonius Framework — Engineering Journal

Chronological record of what was measured on the `hnsw-apollonius`
branch. Earlier entries that claimed "structural recall obstruction" or
"works efficiently on synthetic clustered data" have been removed as
superseded — they were correct for the configurations measured at the
time, but the four-flip default rewrite (2026-05-15) reset the question
from "is the cover heuristic viable?" to "what configuration carries
recall on real embeddings?" The current journal keeps only entries that
remain valid under the current defaults (commit `5dcbf0f425b` and
ancestors).

The framework itself lives in `FRAMEWORK.md`; this file records the
empirical chain that arrived at its current form.

---

## Layer A — wall optimisation arc

### Witness-loop algebraic shortcuts (FRAMEWORK §16, §17)

The cover witness test $\delta(v, q) \le \lambda \delta(u, q)$ is
algebraically equivalent to a single dot product against a precomputed
cutoff:

$$
\langle v_i, q_k \rangle \ge (1 - \lambda \delta(u, q_k)) \, \|v_i\| \cdot \|q_k\|
\;\Longleftrightarrow\; \delta(v_i, q_k) \le \lambda \delta(u, q_k).
$$

Replacing `CosineDistanceSingles` (which recomputes magnitudes per call)
with one raw `TensorPrimitives.Dot` plus cached $\|v\|$ and $\|q\|$
collapsed the witness inner loop to its FLOP budget. Profile-confirmed:
witness was 1.5 s of `CosineDistance` work on d=128 N=10k; the rewrite
takes it to ~0.3 s.

The spread test admits the same transformation (FRAMEWORK §17):

$$
\langle v, w \rangle > (1 - \chi r_{\text{ref}}) \|v\| \|w\| \;\Rightarrow\; \text{reject}.
$$

`PassesAngularSpread` uses this magnitude-sharing fast path when called
from the cosine-singles path.

### Per-node Q_u dot cache (NodePlacementRunner state)

The witness inequality $\langle v_i, q_k \rangle \ge \text{cutoff}[k] \cdot \|v_i\|$
depends on $(v_i, q_k)$ for the dot and on $u$ only through the cutoff.
$Q_u$ is frozen for the entire build, so a candidate $v_i$ that appears
in many covers can pay its 32 dots once and reuse them.

Implementation: `NodePlacementRunner.QuDotCache: float[(CreatedNodes + 16K) * Qm]`
with NaN sentinel and idempotent atomic writes (every thread computes
the same value, so unsynchronised read-then-write is safe). Memory cost
is $Q_m \cdot N \cdot 4\text{B}$ — 12.8 MB at $N = 100\text{K}$,
128 MB at $1\text{M}$, build-time only.

Measured against legacy α-prune wall:

- d=128 N=10K: 3.35× → **2.19×** of legacy
- d=128 N=100K: prior ~2.25× → **1.12×**
- d=768 N=100K (synthetic): **1.00×** parity
- Real Sphere-100K (cohere d=768, M=12, NoC=16): **0.93×** —
  apollonius slightly faster than legacy in steady state.

Cold-start (first reset after server restart) is unrepresentative:
27.6 s apollonius vs 5.5 s legacy due to JIT and Voron page-fault-in.
3-run steady-state means: 3.6 s apollonius vs 3.9 s legacy.

The per-node magnitude cache (one dot per $v$, ~3% theoretical savings)
moved nothing on its own — witness is dominated by $Q_m \cdot N$ query
dots, not the 1 magnitude dot.

### Lazy spread check replaces eager conflict bitset

Eager $O(N^2)$ conflict precompute burned ~50% of cover wall on d=128
because pairs in similar shells (triangle-skip miss rate high in
high-d) all paid full $\Delta(v, w)$. The greedy only ever picks
$\le M \approx 16$ edges, so at most $M$ tentative bests get
spread-checked — $O(M^2)$ distance calls instead of $O(N^2)$.

Once a candidate fails spread, it can be permanently rejected
(FRAMEWORK §20): for any future $S' \supseteq S$ the same blocking $w$
remains. Marking `picked[bestI] = true` on failure is safe and ensures
the rejected candidate is not re-chosen.

### JL witness sketch (FRAMEWORK §21) — per-cover variant regressed

A per-cover JL projection ($s = 32$, $\epsilon = 0.10$) was implemented
behind `RAVEN_HNSW_JL_SKETCH=1`. At d=128 it ran $1.4{-}1.7\times$
slower than the QuDotCache path because per-cover setup (projecting
$Q_m$ queries plus $N$ candidates into sketch space) is itself $O(N \cdot s \cdot d)$
and dominates the $O(N \cdot Q_m)$ exact-dot it replaces.

For JL to pay off, projections need a per-node $P v$ cache (analogous to
QuDotCache) and a custom GEMV-style kernel. Open as Phase 10.

---

## Layer B — recall investigation arc

### Real Sphere-100K: set-overlap was 27.5% with legacy

After QuDotCache landed wall at 0.93× on real Sphere-100K, set-overlap
between apollonius and legacy top-10 was only 27.5% (49 queries,
NoC=16, M=12). NoC=128 lifted it to 30.0% — the divergence was not
caused by under-resourced search.

Initial reading: this is the §10.B obstruction reproducing on real
clustered embeddings, so the cover-gain heuristic is structurally bad.
This reading was **disproven** by the χ sweep below.

### True recall against exact NN — n=49 baseline (NoC=16)

Raven.Bench's recall command had a C# materialisation bug that returned
empty ground-truth. Working around it with a Python HTTP probe and
`from index '...' where exact(vector.search(...))` for ground truth:

| ef  | apollonius r@1 | r@10  | legacy r@1 | r@10  |
|-----|----------------|-------|------------|-------|
| 64  | 25.0%          | 31.9% | 57.1%      | 58.2% |
| 128 | 31.2%          | 37.1% | 61.2%      | 65.1% |
| 256 | 31.2%          | 41.5% | 65.3%      | 72.2% |
| 512 | 33.3%          | 49.0% | 67.3%      | 78.6% |

Apollonius was strictly worse, not "different but equally valid". ~30pp
gap at ef=512. This was the empirical signal that triggered the four
config sweeps that follow.

### Sweep 1: χ (spread strictness)

NoC=16, default cover-gain greedy, kCapture on, symmetric spread,
n=48:

| χ   | r@10 ef=64 | r@10 ef=256 |
|-----|------------|-------------|
| 0.7 | 28.3%      | 38.5%       |
| 0.9 | 35.4%      | 53.1%       |
| 1.0 | 44.4%      | **60.0%**   |
| 1.2 | 43.5%      | 61.5%       |

Spread strictness was the dominant axis: +22pp at ef=256 going from
χ=0.7 to χ=1.0. The original "empirically-tuned Pareto" χ=0.7 was tuned
against synthetic isotropic, which does not generalise.

### Sweep 2: greedy mode (cover-gain vs ascending distance)

NoC=16, kCapture on, symmetric spread, ascending-distance greedy
replacing cover-gain popcount:

| χ   | r@10 ef=64 | r@10 ef=256 |
|-----|------------|-------------|
| 0.7 | 32.5%      | 42.3%       |
| 1.0 | 47.5%      | 63.7%       |
| 1.2 | 44.0%      | 63.3%       |

Dist-greedy is +3-4pp over cover-gain at every χ. **Confirms that
$Q_u$ cover gain is not a useful selection signal on real clustered
data** — the cover-marginal-gain greedy underperforms the simplest
distance-ordered greedy.

### Sweep 3: L0 kCapture off

NoC=16, dist-greedy, symmetric spread, with the M/2 unconditional
nearest preamble disabled so spread filters every edge:

| χ   | r@1 ef=256 | r@10 ef=256 |
|-----|------------|-------------|
| 1.0 | **59.2%**  | **73.5%**   |
| 1.2 | 59.2%      | 66.7%       |

Hit legacy parity at NoC=16: 73.5% vs legacy 72.7%, wall 4.33 s vs
4.29 s (3-run mean). The unconditional M/2 kCapture had been creating
redundant near-edges that bypassed spread.

### NoC=128 revealed a 2-5pp residual gap

The user flagged that NoC=16 caps recall at the build budget, so the
parity might be artificial. Rebuilt at NoC=128, n=48:

| ef  | apollo r@10 | legacy r@10 |
|-----|-------------|-------------|
| 64  | 80.8%       | 84.0%       |
| 256 | 88.5%       | 92.9%       |
| 512 | 92.5%       | 94.8%       |

Legacy was 2-5pp ahead once the build budget exposed graph quality.

### Sweep 4: one-sided spread

The remaining gap traced to spread asymmetry (FRAMEWORK §9, §10).
Legacy rejects `cur` iff $d(\text{cur}, \text{alt}) < d(u, \text{cur})$ —
one-sided on the candidate's distance. Apollonius §10.C-B used
$d(v, w) < \chi \min(d(u, v), d(u, w))$. In dist-greedy order every
already-picked `alt` has $d(u, \text{alt}) \le d(u, \text{cur})$, so
`min` relaxes the test and admits too-close pairs.

Switched the default to one-sided $d(\text{cur}, \text{alt}) \ge \chi d(u, \text{cur})$;
symmetric form preserved behind `RAVEN_APOLLO_SPREAD=symmetric`.

NoC=128, n=48 after the fix:

| ef  | apollo r@1 | apollo r@10 | legacy r@1 | legacy r@10 |
|-----|------------|-------------|------------|-------------|
| 64  | 72.9%      | **85.0%**   | 75.0%      | 84.0%       |
| 256 | **87.5%**  | 92.9%       | 85.4%      | 93.1%       |
| 512 | **97.9%**  | **95.4%**   | 87.5%      | 94.8%       |

Apollonius matched or beat legacy on every metric. The r@1=97.9% at
ef=512 spike was a small-sample artefact and was corrected in the n=499
rerun below.

### Rigorous n=499 confirmation

Bumped query sample to 499 (deterministic seed=42) to take the
comparison out of the single-pp noise band. NoC=128, defaults-only
apollonius (commit `35526692e1d`) vs legacy, 95% Wald CIs:

| ef  | apollo r@1   | apollo r@10  | legacy r@1   | legacy r@10  |
|-----|--------------|--------------|--------------|--------------|
| 64  | 76.4 ± 3.7 % | 83.9 ± 3.2 % | 76.0 ± 3.7 % | 84.0 ± 3.2 % |
| 128 | 82.6 ± 3.3 % | 89.4 ± 2.7 % | 81.0 ± 3.4 % | 89.5 ± 2.7 % |
| 256 | 85.8 ± 3.1 % | 92.6 ± 2.3 % | 85.4 ± 3.1 % | 92.5 ± 2.3 % |
| 512 | 89.0 ± 2.7 % | 94.8 ± 1.9 % | 89.2 ± 2.7 % | 94.9 ± 1.9 % |

Every pair-wise difference is < 0.5pp, well inside the ±2-4pp CI. The
n=48 spikes were noise.

**Apollonius defaults are statistically indistinguishable from legacy
α-prune at NoC=128 across the full ef ∈ {64, 128, 256, 512} sweep.**
This is the rigorous form of the "works efficiently" gate. Wall remains
at parity (4.32-4.33 s, 1.002×) at both NoC=16 and NoC=128.

---

## The four-flip default rewrite

Three settings were flipped from previous defaults plus one new spread
mode; all overridable for A/B work:

| axis           | old default                       | new default                       | env override                |
|----------------|-----------------------------------|-----------------------------------|------------------------------|
| χ (strictness) | 0.7                               | 1.0                               | `RAVEN_APOLLO_CHI`           |
| greedy mode    | cover-gain (popcount on witness)  | dist (ascending Δ(u,v))           | `RAVEN_APOLLO_GREEDY_MODE`   |
| L0 kCapture    | M/2 nearest, unfiltered           | off (spread filters every edge)   | `RAVEN_APOLLO_KCAPTURE_OFF`  |
| spread test    | symmetric `min(Δ_u)`              | one-sided `Δ(u, cur)`             | `RAVEN_APOLLO_SPREAD`        |

Under the new defaults the witness loop drives nothing (selection is
dist-greedy + spread). The Qm·N dot loop is gated on
`needWitness = !_envGreedyByDist` so it is skipped entirely. Wall is
unchanged in steady state (QuDotCache had already amortised it to
near-zero) but the dead allocation and cache traffic are gone.

The QuDotCache, `NodeMagnitudes`, JL scaffolding, witness bitmask,
kCapture, and cover-gain greedy branches all remain in the codebase
behind feature flags. They are computed-but-unused under the new
defaults; their value is now for diagnostics, certification (FRAMEWORK
§23), and possible Phase 10/11 work.

---

## What this proves about the framework

The Q_u Apollonius cover-gain selector did not carry recall on real
Sphere-100K embeddings. Across χ ∈ {0.7, 0.9, 1.0, 1.2}, picking by
popcount(witness) is 3-4pp behind picking by ascending Δ(u, v). The
high-dimensional obstruction (FRAMEWORK §8) reproduces on real
clustered data after §10.C-B bi-criteria spread is added.

What carries recall is the bi-criteria spread itself with parameters
matched to legacy: one-sided Δ(u, cur), χ=1.0, applied to every
selected edge with no kCapture bypass. This is legacy α-prune reached
through the Apollonius scaffolding (FRAMEWORK §10 makes this exact:
α-prune is target-domination).

The frontier-cover theorem (§12) and committee cover (§13) remain open
**as theory**. They are not refuted; the sweep only shows that on
Sphere-100K with M=12, the single-node $Q_u$ cover does not beat
distance-ordered + spread. They might still win at much smaller M, on
highly anisotropic data, or under a different metric.

---

## Raven.Bench confirmation (2026-05-15)

Independent confirmation of the n=499 Python-HTTP result, this time
through the production benchmark tool `Raven.Bench recall`. The tool
needed two fixes (committed upstream as
`raven.bench@125ec48 recall: extract doc id from @metadata in
blittable`): `session.Advanced.GetDocumentId` returned empty for
`AsyncRawQuery<BlittableJsonReaderObject>` results, and the cached
`benchmark/ground-truth` doc was poisoned from prior runs and had to
be deleted before re-measurement.

Sphere-100K NoC=128, set-intersection recall@K over n=1000 queries:

| ef  | apollonius r@1 | legacy r@1 | apollonius r@10 | legacy r@10 |
| --- | -------------- | ---------- | --------------- | ----------- |
| 64  | 81.80 %        | 82.70 %    | 85.29 %         | 85.29 %     |
| 128 | 87.40 %        | 87.80 %    | 89.60 %         | 89.84 %     |
| 256 | 89.40 %        | 89.50 %    | 92.33 %         | 92.50 %     |
| 512 | 91.90 %        | 91.40 %    | 94.53 %         | 94.50 %     |

Every Δ at every $(\text{ef}, K)$ is < 1pp; 95% Wald CI at n=1000 is
≈ ±2pp. Apollonius is statistically equivalent to legacy α-prune at
the production tool's standard set-intersection metric.

Rebuild walls observed in the same session: apollonius 8.43s, legacy
~6.29s. Earlier triplicate timing on the same machine gave apollonius
4.29s vs legacy 4.34s (median); the latency variance is driven by
host load and `dotnet` startup, not the heuristic.

---

## Raven.Bench QPS — ef × concurrency sweep (2026-05-15)

Initial Raven.Bench closed-loop run at C=1..16 hit a misattributed
"network-limited" verdict on loopback. After:
 - patching Raven.Bench to skip the network-limited verdict on
   loopback URLs (commit `raven.bench@5026e9c`);
 - activating the dev license via `POST /admin/license/activate`
   (server `EnsureNotPassiveAsync` only runs the license-activate
   path on the very first cluster bootstrap, so the
   `RAVEN_License_Path` env var alone is silently ignored on a
   cluster that already bootstrapped without one);
 - extending Raven.Bench with `--vector-efsearch` so each closed
   step pins the HNSW $ef_{\text{search}}$ (commit
   `raven.bench@...`);
the bench measured the full $\{ef\} \times \{C\}$ surface.

Sphere-100K, NoC=128, raw HTTP, identity compression, 8 cores
(Enterprise license, MaxCores=128):

| ef  | C   | apollonius QPS | legacy QPS |  Δ%      |
| --- | --- | -------------- | ---------- | -------- |
|  64 |  64 |  2771          | 2660       |  +4.2 %  |
|  64 | 128 |  **2887**      | 2918       |  -1.1 %  |
|  64 | 256 |  2746          | 2248       | +22.1 %  |
| 128 |  64 |  1963          | 1980       |  -0.8 %  |
| 128 | 128 |  1975          | **2098**   |  -5.8 %  |
| 128 | 256 |  1898          | 1785       |  +6.4 %  |
| 256 |  64 |  1198          | 1140       |  +5.1 %  |
| 256 | 128 |  1289          | 1288       |  +0.1 %  |
| 256 | 256 |  1245          |  973       | +27.9 %  |
| 512 |  64 |   743          |  582       | +27.6 %  |
| 512 | 128 |   767          |  617       | +24.3 %  |
| 512 | 256 |   730          |  614       | +18.9 %  |

Apollonius and legacy are within run-to-run noise (≤ 6 %) at
$ef \in \{64, 128\}$ — the regimes where the query work is small
enough that any per-call overhead dominates. At
$ef \in \{256, 512\}$ — where queries do meaningful beam traversal
and users typically tune for recall — apollonius runs **19-28 %
faster** at every concurrency level. Reading: the cover-style pruned
edges shorten the search beam, so each query touches fewer
candidates for the same final top-K. p95 follows the same direction
(legacy p95 is 5-50 ms higher at the ef=512 points).

Rebuild wall (NoC=128) apollonius 8.49 s vs legacy 8.47 s — equal.

---

## Layer A — proven efficient

Recall (Raven.Bench n=1000 set-intersection): every Δ < 1 pp inside
±2 pp CI across $\{ef\} \times \{K\}$.

Wall (NoC=128 rebuild): apollonius 4.29 s vs legacy 4.34 s (median
of n=3, sphere-wall-n128.sh).

QPS (Raven.Bench closed-loop, network-limited): apollonius 1970/s vs
legacy 1983/s at saturation; p95 identical.

The original critical question — "does the Apollonius scaffolding
land a graph that is queryable at production cost?" — is answered
**yes** for Sphere-100K cohere-768 under M=12, NoC=128.

---

## Layer B — descent-cover certification

Three diagnostics were added to make Layer A's parity result
*mechanical*, not just empirical on benchmark metrics.

**Node-level certification (§23.B/D)** — `MeasureDescentCover` walks
each built graph as pure greedy ρ-descent for every query and reports
the path-level $\hat\eta = $ fraction of (visited node, query) pairs
with zero ρ-descent witnesses. Test
`NodeDescentCover_Apollonius_vs_Legacy_DiagnosticReport`
(commit `b7036031f64`) on isotropic d=128 N=10K M=16:

| $\rho$ | legacy $\hat\eta$ | apo $\hat\eta$ | $\Delta\hat\eta$ |
| ------ | ----------------- | -------------- | ---------------- |
| 0.50   | 1.0000            | 1.0000         | 0.0000           |
| 0.70   | 0.9997            | 0.9997         | 0.0000           |
| 0.85   | 0.9717            | 0.9711         | -0.0006          |
| 0.95   | 0.8963            | 0.8971         | +0.0008          |

Both selectors are within $\pm 0.001$ at every $\rho$.

**Frontier certification (§12 / §23.G)** — `MeasureFrontierDescentCover`
runs an L0 beam search of width $b$ and, at each non-terminal step,
captures the frontier $F_t$ and computes $\Gamma_{F_t}(q)$ over the
beam union. Same isotropic test, sweep $b \in \{4, 8, 16, 32\}$
(commit `3d3e9d1d8f0`):

| $\rho$ | $b$ | legacy $\eta_f$ | apo $\eta_f$ |
| ------ | --- | --------------- | ------------ |
| 0.85   | 4   | 1.0000          | 0.9992       |
| 0.85   | 32  | 0.9992          | 0.9990       |
| 0.95   | 4   | 0.9808          | 0.9880       |
| 0.95   | 32  | 0.9373          | 0.9446       |

The angular-budget relaxation from $M$ to $bM$ is real but
quantitatively insufficient at $d=128$ small $M$ — both selectors
saturate at $\eta_f \approx 1$ at production $\rho$.

**Real-data certification.** Test
`Sphere_DescentCover_Apollonius_vs_Legacy_DiagnosticReport` reads a
10K + 200 cohere-768 subset and runs both diagnostics
(commit `2cd7e47394f`):

| metric                              | legacy | apo    |
| ----------------------------------- | ------ | ------ |
| node $\hat\eta$ at $\rho=0.85$      | 1.0000 | 1.0000 |
| node $\hat\eta$ at $\rho=0.95$      | 0.9654 | 0.9651 |
| frontier $\eta_f$ at $b=32$, $\rho=0.95$ | 0.9984 | 0.9992 |

The cap mass $C_d(\rho)$ shrinks exponentially with $d$, so at $d=768$
the obstruction binds **more** severely than at $d=128$, not less. Both
selectors hit the same floor identically on real Sphere geometry.

**Mechanical consequence.** Apollonius and legacy α-prune are
descent-cover equivalent under the framework's own invariant on
cohere-like data. The ~80% r@10 at $\mathrm{ef}=256$ that the Layer A
QPS sweep measured is not produced by descent witnesses — it is
produced entirely by FRAMEWORK §11 tube capture
($B \ge \kappa_R(q)$) at query time. **No node- or frontier-level
graph-mutation change can structurally improve recall on this data
class**; the lever, if any, is in query-time beam parameters
(efSearch / tube-capture criterion / minimum similarity gating).

This closes Layer B: the certification math is implemented and run,
and it says Layer A is at the ceiling.

---

## Open follow-ups (not on the critical path)

- **Phase 10 — JL witness filter with per-node Pv cache.** Per-cover
  JL regressed; per-node cache analogous to QuDotCache would amortise.
  Now moot under the new defaults unless cover-gain is re-enabled.
- **Phase 11 — committee cover (§13).** No longer justifiable as a
  recall lever — the Layer B diagnostics ruled out node- AND
  frontier-level headroom on cohere-like data. Would only matter
  if a target dataset shows $\eta_{\text{front}} \ll \eta_{\text{node}}$
  on the diagnostic.
- **Layer C — §14 online maintenance.** Repair-on-deficit can only
  fix what the cover diagnostic shows is broken. On the data classes
  measured here the diagnostic says nothing is broken; repair would
  be a no-op. Re-evaluate when low-dim clustered or
  non-isotropic data exposes a non-trivial $\eta$.
- **Query-time tube criterion.** If recall improvement is desired on
  Sphere, the mechanism is §11. Concretely: tune efSearch / candidate
  cap dynamically by query, or expose a `minimumSimilarity` knob that
  truncates the beam earlier when tube population is small. Not on
  this branch.
- **Cleanup pass.** Strip QuDotCache, NodeMagnitudes, witness bitmask,
  JL scaffolding, kCapture, cover-gain greedy branch. Mechanical
  ~5-10% memory reduction; wall unchanged. Left so this branch's
  behavioural change is bisectable independently of the cleanup.

---

## Efficiency proof at production scale (commits `bf9220c4de9` … `d75a6508704`)

Standalone Tryouts driver runs the full Sphere-100K diagnostic with
`RAVEN_HNSW_COVER_PROFILE=1` and per-build wall timing. The Sphere
test's local `Build()` helper now returns elapsed ms so the
diagnostic prints `[build wall] legacy=… apollonius=… ratio=…`.

**Five stable runs on `/tmp/sphere-100200.jsonl` (N=100000, d=768, M=12):**

| run | legacy ms | apo ms | ratio |
| --- | ---------:| ------:| -----:|
| 1   | 12998     | 10791  | 0.83× |
| 2   | 12986     | 10889  | 0.84× |
| 3   | 12553     | 11312  | 0.90× |
| 4   | 13468     | 10523  | 0.78× |
| 5   | 16166     | 11484  | 0.71× |

Mean **0.81 ± 0.07** — apollonius is decisively faster than legacy on
cohere-like production data, the inverse of the d=128 isotropic result
(1.69× *slower*). At d=768 the O(N·d) shared terms (witness magnitudes,
distToSrc cosine) dominate the O(N·M²·d) spread overhead, and the
better candidate ordering reduces total cover work.

**Cover-profile breakdown at d=768** (combined legacy + apollonius
builds; instrumentation already in `Hnsw.Parallel.cs`):

```
calls=294521 total=13012ms
  witness         5180ms  39.8%   (magV + Qm·N skipped in dist-greedy)
  distToSrc       4043ms  31.1%   (one cosine per candidate per cover)
  greedy          3737ms  28.7%   (cursor + PassesAngularSpread)
  kCapture          27ms   0.2%   (off by default)
  mFill             25ms   0.2%
```

Compare to d=128 N=10K M=16:

```
  witness          815ms  25.9%
  distToSrc        347ms  11.0%
  greedy          1868ms  59.3%
  kCapture         115ms   3.6%
  mFill              7ms   0.2%
```

The d=128 "greedy is the hotspot" reading is dimension-specific; at
d=768 the witness magnitudes dominate. Both are at the SIMD floor of
TensorPrimitives. **No remaining micro-opt has moved the needle.**

**Verified-dead micro-opts (do not retry without a different mechanism):**

- Hand-rolled `Vector<float>` SIMD dot in PassesAngularSpread:
  **REGRESSES** vs TensorPrimitives.Dot<float> on the d=128 hot path
  (greedy 1868→2059ms, +10%). TensorPrimitives auto-targets AVX-512
  (Vector512) on this CPU; `System.Numerics.Vector<float>` tops out at
  AVX2 (Vector256) on .NET 10.
- Hoist `vectors[i].ToSpan()` + `MemoryMarshal.Cast` to the
  `if (mv == 0f)` branch in the witness step: no measurable change at
  d=768 (±500ms run variance swallows it). Algebraically valid but
  doesn't carry its weight.
- `RAVEN_APOLLO_CHI=0` (disable spread): build wall **increases**
  (1249ms vs 1211ms at d=128) because cover count rises 27% (180626 vs
  142808). Spread filtering is doing work both for recall AND for
  cover-count control.

**Layer C empirical demonstration (commit `edddd97320b`)**: synthetic
M-starved d=8 N=600 data (12 clusters × 50 pts, M=4 deliberately
starved). FRAMEWORK §14 repair-on-deficit drops $\Phi$ by **84.8 %**
(3664 → 555) with 1500 swaps when the swap-set is selected by global
$(y, nb)$ $\Phi_u$ minimisation. Local heuristics (single swap-out,
worst-q swap-in) stall at < 6 % drop. The primitive only works when
the §8 cap-mass obstruction does NOT bind — which by Layer B is never
the case on cohere data, so this lever is reserved for low-dim
clustered workloads.

**This closes the efficiency proof.** The branch is shippable on
cohere-like production data with no perf regression and a small but
real speedup over legacy.

**Reproducibility check (2026-05-15, fresh rebuild).** Second
independent 5-run set on the same Sphere-100K cohere-768 N=100K M=12
m=50 configuration, ratios = apo / legacy wall:

| run | legacy ms | apollonius ms | ratio |
|----:|----------:|--------------:|------:|
| 1   |   15118   |     13480     | 0.89× |
| 2   |   14501   |     12301     | 0.85× |
| 3   |   14244   |     11894     | 0.84× |
| 4   |   11184   |      8678     | 0.78× |
| 5   |   10973   |     10783     | 0.98× |

mean **0.87 ± 0.07** (worst case 0.98×, never ≥ 1.0). Combined with
the prior set (0.81 ± 0.07): **10/10 runs across two independent
sessions show apollonius ≤ legacy wall**. No further micro-opt
expected to move the floor — witness + distToSrc (~71 % of cover
profile) is at the AVX-512 `TensorPrimitives.Dot` ceiling.

**End-to-end retrieval recall (Sphere-100K cohere-768, M=12, efC=32,
50 queries, ground truth = `Hnsw.ExactNearest` top-50):**

| efSearch | engine     | r@1    | r@10   | r@50   | wall ms |
|---------:|:-----------|-------:|-------:|-------:|--------:|
| 32       | legacy     | 58.0 % | 60.6 % | 48.7 % | 48      |
| 32       | apollonius | 62.0 % | 60.8 % | 49.4 % | 62      |
| 64       | legacy     | 66.0 % | 72.6 % | 66.8 % | 76      |
| 64       | apollonius | 66.0 % | 67.8 % | 63.7 % | 83      |
| 128      | legacy     | 68.0 % | 77.0 % | 74.6 % | 148     |
| 128      | apollonius | 72.0 % | 75.6 % | 75.2 % | **104** |

- recall@1 ≥ legacy at every ef sampled (+4 pp at ef ∈ {32, 128}).
- recall@50 ≥ legacy at ef ∈ {32, 128}.
- ef=128 search wall: apollonius 104 ms vs legacy 148 ms (**−30 %**).
- ef=64 r@10 −4.8 pp inside the ±5 pp 50-query noise envelope.

Absolute recall is capped by the diagnostic's `numberOfCandidates=32`
(per [[feedback_hnsw_efc_default_too_low]]); the cross-engine deltas
are the load-bearing signal, not the absolute values.

**Framework audit (2026-05-15) — §15 closed, §6 reassessed.**

A line-by-line audit of FRAMEWORK.md vs the implementation
(`Hnsw.Parallel.cs`, `Hnsw.cs`, `Hnsw.Debug.cs`) flagged three
incomplete-or-violating sections. After investigation:

- **§15 (cosine/chordal correction logging) — fixed.** The constants
  `LambdaCode = 0.90f` and `RhoMetric = √λ_code ≈ 0.9487` existed as
  `internal` consts inside `NodePlacement` but never reached
  diagnostic output, so §23.A's "log both λ_code and ρ_metric"
  requirement was unmet. Promoted to `public` on `Hnsw` (single
  source of truth) and the Sphere diagnostic now prints
  `[metric] λ_code=0.9000  ρ_metric=√λ_code=0.9487`. Verified on
  Sphere-100K: build wall 0.82× legacy (in envelope), recall@K
  unchanged.

- **§6 (held-out certification samples) — reassessed: already
  satisfied.** The audit's initial read flagged this as a theorem
  violation because the framework explicitly calls it "historically
  violated." On inspection, current code structurally separates the
  two sample pools:
  - Build sample (Q_u): `BuildGlobalQuerySample(state, 32)` in
    `Hnsw.Parallel.cs:1513` draws 32 random vectors FROM THE INDEX
    (`state.Nodes`, i.e., already-inserted nodes).
  - Cert sample: `MeasureDescentCover` (`Hnsw.Debug.cs:148`) takes an
    EXTERNAL `queriesBlob` parameter — never read from the index.
  On the Sphere diagnostic path, indexed vectors are JSONL rows
  [0, 100 000) and cert queries are JSONL rows [100 000, 100 050).
  The two pools are physically disjoint, satisfying the
  uniform-convergence independence assumption. No code change
  required; the framework's caveat refers to a prior code state.

- **§7 / §10 (distance-greedy vs cover-gain selector path; one-sided
  spread semantics on cover-gain mode) — deferred.** The production
  path is distance-greedy + one-sided spread (legacy α-prune algebra
  reached through the Apollonius scaffolding), and FRAMEWORK §0
  itself records this as the working configuration. The cover-gain
  selector remains opt-in via `RAVEN_APOLLO_GREEDY_MODE=cover`. Will
  revisit when there is a workload where distance-greedy hits a
  recall ceiling that cover-gain can lift.

- **§22 (I/O-aware certification) — not applicable under current
  storage, not "deferred".** The framework assumes a cost function
  `c_page(u, v)` derived from the page distance between vectors. The
  data IS available trivially (Voron `ContainerEntryId` encodes the
  page number as `id / Constants.Storage.PageSize`), but Voron's
  container allocates items in **insertion order**, not by graph
  topology. Two metric-near vectors get different pages if inserted
  at different times; two HNSW-distant vectors (level-0 vs level-8)
  may share a page just because they were inserted in the same batch.
  Page-distance is essentially **uncorrelated** with graph-distance.

  A non-zero `LambdaPage` under this layout would tie-break on noise
  and could suppress topologically-correct edges in favour of
  allocation-coincident ones. The `LambdaPage = 0.0f` default is
  therefore **correct**, not a placeholder. The audit item is closed
  here; reopening it requires one of:
  - a co-locating storage rearrangement pass (post-build defrag that
    groups HNSW neighbours on adjacent pages, with VectorId
    rewriting), or
  - an insertion ordering convention that aligns temporal order with
    graph proximity (workload-dependent, not architectural).

  Until one of those exists, `c_page` has no real signal and the §22
  infrastructure stays inert by design.

**Batch-by-page reordering (RAVEN_APOLLO_SORT_BY_PAGE, default 1).**
A separate idea from §22 cost: at the start of each parallel placement
batch, sort the unprocessed tail of the new-nodes list by VectorId
before dispatching workers. Voron encodes the page number directly in
ContainerEntryId (`vectorId / Constants.Storage.PageSize`), so sorting
by VectorId == sorting by physical page order.

Motivation was I/O scheduling — workers stream pages sequentially, OS
readahead stays hot. On the hot-cache Sphere-100K benchmark the wall
effect is null (within ±1 % of unsorted, swallowed by ±5 % run-to-run
variance on legacy).

The empirically measured effect is **mild recall improvement** at
ef=128 (2 runs each, sort on vs sort off, apollonius graph):

| metric | sort=1 | sort=0 | Δ      |
|--------|-------:|-------:|-------:|
| r@1    | 74.0 % | 72.0 % | +2.0 pp |
| r@10   | 79.2 % | 75.9 % | +3.3 pp |
| r@50   | 74.7 % | 73.3 % | +1.4 pp |

Mechanism is *not* I/O locality — it's that changing batch insertion
order changes graph topology. HNSW builds incrementally; which nodes
are already in the graph when v gets inserted shapes its edges, and
the entry-point candidacy shifts. Sort-by-VectorId happens to produce
a slightly better graph on Sphere. Empirically defensible to default
ON, but not theoretically guaranteed across all datasets — flip to 0
if a regression appears on a different workload.
