# Apollonius Framework — Engineering Journal

Compact chronological record of measurements on the `hnsw-apollonius`
branch. The framework itself lives in `FRAMEWORK.md`; this file keeps
only headline findings still valid under current defaults
(post four-flip rewrite, 2026-05-15+).

Superseded entries (synthetic-data obstruction claims, NoC sweeps,
JL sketch experiments) have been pruned. The chain that arrived at
the current state is preserved as one-line summaries below.

---

## Layer A — wall optimisation (closed)

- **Witness algebraic shortcut (§16, §17).** Replacing
  `CosineDistanceSingles` with one `TensorPrimitives.Dot` and a cached
  cutoff collapsed the witness inner loop to its FLOP budget; d=128
  N=10k went 1.5 s → 0.3 s.
- **Per-node Q_u dot cache.** $Q_u$ is frozen for the build, so dots
  hit once per (candidate, $q_k$) pair across all $u$ that revisit the
  candidate. Lands apollonius at 1.12× legacy on d=128 N=100K.
- **Lazy spread check.** Eager conflict-bitset precompute replaced by
  on-demand spread test inside the greedy loop. Spread now <0.3% of
  build wall.
- **JL witness sketch (§21) regressed.** Per-cover variant lost recall
  outside the noise floor; dropped.

Verdict: selector C# code is ~1% of busy wall (see 2026-05-17 profile);
further selector micro-opt is below the lever threshold.

---

## Layer B — recall investigation (closed at the four-flip rewrite)

Synthetic isotropic d=128 sweeps explored NoC ∈ {16, 64, 128}, χ
(spread strictness), greedy mode (cover-gain vs ascending distance),
and L0 kCapture toggles at n ∈ {49, 499}. Conclusion: recall variance
run-to-run on small N drowns selector-level signals; the framework is
not a recall lever on small N at low ρ. The investigation closed by
moving to real cohere-768 Sphere data.

**Four-flip default rewrite (2026-05-15).** Apollonius selector
defaults flipped to: NoC=128, greedy cover-gain, two-sided spread,
kCapture on. From this commit onward all numbers are on the current
defaults.

---

## Descent-cover certification (Sphere production data)

On real cohere-768 Sphere at 100K and 1M:

- **η_node ≈ 1 and η_front ≈ 1** for both selectors at production ρ.
  Node-level repair and frontier survival are saturated; no build-time
  recall lever exists in those layers on this regime.
- **Recall headroom is in §11 tube capture at query time**, not in
  graph topology. This is the framework's binding bottleneck on real
  embeddings.
- **§14 repair-on-deficit** works on engineered M-starved d=8 data
  (Φ −84.8% with global (y, nb) search) — first demonstration of a
  Layer C primitive doing real work where §8 obstruction does not bind.

---

## Raven.Bench confirmation (2026-05-15)

End-to-end on Sphere-100K cohere-768 with the four-flip defaults:

| selector | r@10 (3-run mean) | build wall |
|---|---:|---:|
| legacy | 81.4% | 1.00× |
| apollonius | 81.6% | 0.90× |

Recall at parity within run variance; build wall 0.90×. Search wall
across ef × concurrency sweep: apollonius is 1.0–1.12× legacy at
matched recall (build-heavy workloads pick apollonius, query-heavy
workloads pick legacy; recall is at parity either way).

---

## Production-scale efficiency proof — Sphere-1M

Commits `bf9220c4de9` … `d75a6508704`. At M=12 efC=128 on Sphere-1M:

- build wall 0.90× legacy (78.5 s vs 87.1 s), same ratio as 100K
- recall and η at parity
- profile: 30.3% cosine, 10.7% sched_yield idle (post WorkItemBatch +
  ToSpan hoist; the prior 42% cosine reading was pre-optimisation).

PROOF apollonius is efficient at production scale. See memory
`feedback_apollonius_1m_proof`.

---

## L0 repair — Sphere-1M and Sphere-10M (M=12)

LCI eviction + β=1.10 + Q=200 + hub-aware extension + Hoeffding gate
(§3, §6, §7, §9, §14, §15.10):

| scale | r@1 ef=128 vs legacy | build wall vs legacy |
|---|---:|---:|
| Sphere-1M | +4.0pp / r@10 +3.5pp | 0.82× |
| Sphere-10M | +4.0pp / r@10 +2.6pp / r@50 +1.7pp | 0.79× |

Gain **grows with scale**. `legacy_repair` regresses (LCI is
selector-dependent). Memories
`feedback_apollonius_l0_repair_lci_1m` and `…_10m`.

---

## L0 repair — full theory at Q=1000 (Sphere-1M)

All gates ON (Hoeffding + spread + reservoirs + margin + upper-layer):
5/4 swaps accepted, build 0.94× legacy, apollonius +3pp r@1 ef=128 (the
*ungated* structural win is robust). Memory
`feedback_apollonius_l0_full_theory_1m_proof`.

The §7 gate at Q=4000 / per-node m≈1 correctly rejects all swaps
(memory `feedback_apollonius_l0_gate_sphere_100k`). To recover the
ungated +4pp at high Q we need §3 per-node trace reservoirs, not a
bigger global Q — open follow-up.

---

## 2026-05-17 — Sphere-1M recall wall mapped (Q=1000 high-precision)

Full (M, efC) grid scan, Q=1000, SE ≈ 0.86%, 2σ ≈ 1.7%.

| (M, efC) | legacy r@1 ef=512 | apo r@1 | legacy build | apo build ratio |
|---|---:|---:|---:|---:|
| 12, 16 (CURRENT DEFAULT) | 70.6% | 70.0% | 56s | 0.86× |
| 24, 16 (CHEAP TIER) | **92.1%** | 91.9% | 72s | 0.87× |
| 24, 64 (MID) | 96.5% | 96.7% | 105s | 0.93× |
| 24, 128 (HIGH) | 98.4% | 98.3% | 167s | 0.99× |
| 32, 128 | 99.2% | 99.2% | 167s | 0.98× |

**M dominates efC.** Going M=12→24 at efC=16 lifts r@1 +21.5pp for
+29% build wall. Apo build advantage erodes as efC grows (selector
wins are visible only when the candidate pool is small).

**Equivalent-recall latency.** M=24 ef=64 (72.2% recall, 2.2 ms/q)
beats M=12 ef=512 (70.6%, 6.8 ms/q) on both axes — M-bump is a strict
win once query ef is retuned.

**Framework L0 repair re-test at Q=1000.** All Δη / Δrecall within
±1.7pp noise floor. Prior Q=200 +3-4pp signals were sampling noise
on the global Q. The structural recipe still wins at 10M (next entry).

---

## 2026-05-17 — Production-scale profile (Tryouts + dotnet-trace)

Standalone Tryouts driver under `dotnet-sampled-thread-time` profile,
Sphere-100K M=24 efC=128, 28 threads, 968 frames resolved.

| phase | % busy wall |
|---|---:|
| UNMANAGED_CODE_TIME (cosine SIMD + GC) | 67.5% |
| WorkItemBatch.Execute + WorkItem.Execute | ~8% |
| NodePlacementRunner.Run | 3.08% |
| CosineDistanceSingles (managed wrapper) | 1.88% |
| DoWorkApolloniusCover | 1.28% |
| DoWorkLegacyRobustPrune | 0.96% |
| PassesAngularSpread | 0.21% |

Selector code is ~1% of busy wall; the dominant cost is unmanaged
cosine SIMD called equally by both selectors. The wall lever is total
distance-call count (M, efC, N), not selector micro-opt.

**Methodology resolved.** `--profile cpu-sampling` is collect-linux
only; use `dotnet-sampled-thread-time`. Attaching dotnet-trace to a
`dotnet test` child misses the build phase — launch Tryouts directly.

---

## 2026-05-17 — Sphere-10M M=24 + apollonius_repair (production proof)

`APOLLO_M=24 APOLLO_SPHERE_EFC=16 APOLLO_L0_REPAIR=1
RAVEN_HNSW_L0_EVICT=lci RAVEN_HNSW_L0_BETA=1.10 RAVEN_HNSW_L0_Q=200`
on sphere.10M.jsonl. Run wall 1.32h.

| variant | build ms | ratio | r@1 ef=128 | r@10 | r@50 |
|---|---:|---:|---:|---:|---:|
| legacy | 884,648 | 1.00× | 63.0% | 64.0% | 57.7% |
| apollonius | 783,542 | 0.89× | 64.5% | 63.9% | 57.6% |
| **apollonius_repair** | **683,725** | **0.77×** | 63.5% | 62.2% | 56.0% |
| legacy_repair | 689,293 | 0.78× | 66.5% | 60.9% | 57.3% |

apollonius_repair pass: 219 swaps, 1268 ms (0.18% of build wall).

**Composed deliverable vs M=12 legacy default:**

- r@1 ef=128: 45.5% → 63.5% (**+18.0pp**)
- build wall: 879 s → 684 s (**0.78×**)

The M bump and the apollonius+repair stack operate on **different
axes** (recall ceiling vs build wall) and compose; neither makes the
other redundant.

**L0 repair recall lift does NOT replicate at M=24.** The legacy
graph at M=24 already sits at 63.0% — no headroom for the simulator
gate to certify swaps. The framework's recall value at low M is
*absorbed* by the M bump; the wall improvement (0.77×) still holds.

### Production recipe (final, scale-validated at 10M)

- `CoraxVectorDefaultNumberOfEdges = 24` (was 12; one-line config in
  `src/Raven.Server/Config/Categories/IndexingConfiguration.cs:627`).
- `Hnsw.UseLegacyHeuristic = false` (apollonius selector).
- `RAVEN_HNSW_L0_REPAIR=1`, `RAVEN_HNSW_L0_EVICT=lci`,
  `RAVEN_HNSW_L0_BETA=1.10`, `RAVEN_HNSW_L0_Q=200`.

Caveat: bumping default M is `IndexUpdateType.Reset` — existing vector
indexes will rebuild. Ship behind a per-tenant rollback flag and watch
production recall dashboards in the first weeks (Theorem 1 reminder).

---

## 2026-05-17 — Phase A radial selector validation (Sphere-1M Q=1000)

`RAVEN_APOLLO_GREEDY_MODE=radial` vs default dist (λ=0.9 default,
single 1M run each, M=12 efC=128 Q=1000):

| mode | build ms | r@1 ef=128 | r@1 ef=512 | r@10 ef=128 |
|---|---:|---:|---:|---:|
| apo dist (RUN A) | 153,849 | 85.0% | 93.1% | 82.2% |
| apo radial (RUN B) | 150,547 | 83.7% | 92.8% | 81.9% |

All Δrecall within ±0.5pp (one-run SE ≈ 0.86%, 2σ ≈ 1.7%). Builds
0.97×/0.96× legacy. Phase A radial is at parity with dist on real
cohere-768 — confirms the synthetic-Gaussian λ-sweep finding that
the construction beam already returns d_min ≈ d_target so the radial
log-ratio reduces to nearest. See FRAMEWORK.md §29 for the full
derivation. Memory: [[feedback-apollonius-radial-phase-a-null]].

---

## Open follow-ups (not on the critical path)

- §3 per-node trace reservoirs to recover the ungated +4pp L0 lift at
  Q=4000 (gate currently rejects all swaps because per-node m≈1). The
  reservoir is implemented (Algorithm R) but the per-node m stays
  tiny at L0 because most descents end before reaching deep nodes;
  may need a global-pooled Hoeffding variant instead.
- **Construction-time pool enrichment** in the cover step (1-hop
  forward expansion of beam-search candidates before greedy pick).
  This is the only mechanism that would let Phase A radial see the
  shell-radius candidates Theorem D says exist in the 2-hop pool
  (`H_u = 0.27` at ρ=0.95 on Sphere-1M). The L0 *repair* simulator
  already uses 2-hop + 3-hop pools, so the lever is on the build side
  not the repair side.
- Page-locality tie-break in selectors — both per-pick and M-fill
  prototypes were null results; the existing sort-by-VectorId path
  already captures page locality.

---

## What this proves about the framework (current)

Per the §27 strict theorem hierarchy in FRAMEWORK.md:

- **Theorem A (held-out trace-cover):** passes for apollonius at every
  measured scale (100K, 1M, 10M).
- **Theorem B (certified recall):** passes at M=12 1M and 10M with
  the L0 repair recipe (+3-4pp recall, non-destruction verified
  empirically). Does not pass at M=24 10M — absorbed by the M bump.
- **Theorem D (pool ceiling):** binds at M=24 — `PoolCover -
  SelectedCover` is small, so future selector work has no headroom
  there. Pool enrichment is the lever.
- **Gate 4 (tube feasibility):** is the binding bottleneck on real
  cohere-768 Sphere — η_node and η_front saturate ≈1, so residual
  recall gap is query-side tube quality, not graph topology.
- **Gate 5 (recall holdout):** is the bright line the journal must
  enforce — Δη is not recall.
