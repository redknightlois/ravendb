# Query-Space Descent Covers — Framework & Proofs

This is the canonical mathematical reference for the Apollonius work on this
branch. Do not invent ideas outside this definition. The proofs below are
sufficient conditions: if the construction maintains the invariant, the
guarantees follow. Implementation work reduces to approximating and
maintaining that invariant.

---

## 0. Formal model

`(X, d)` metric space. `X ⊂ X` indexed vectors. `L ⊆ X` live; `T = X \ L`
tombstoned. Directed graph `G = (X, E)`. Query `q ∈ X`.

- `r_k(q)` = distance from q to its k-th nearest **live** point in L
- `τ ≥ 1` — approximation factor. Node u is **τ-terminal** for q if `d(u,q) ≤ τ·r_k(q)`.
- `0 < ρ < 1` — descent factor. Edge `u→v` is a **ρ-descent witness** for q if `d(v,q) ≤ ρ·d(u,q)`.

---

## 1. Apollonius descent cell

For an edge `u→v`, the **ρ-descent cell** in query space is
```
A_ρ(u,v) = { q ∈ X : d(v,q) ≤ ρ·d(u,q) }.
```

**Lemma 1 (Euclidean).** In `(R^n, ‖·‖_2)` with `0 < ρ < 1`, `A_ρ(u,v)` is a
ball with
```
center  c(u,v,ρ) = (v − ρ²·u) / (1 − ρ²)
radius  R(u,v,ρ) = ρ·‖u − v‖ / (1 − ρ²).
```
*(Proof: standard Apollonius algebra; expand `‖q−v‖² ≤ ρ²·‖q−u‖²`,
complete the square. Done in the user's writeup.)*

**Every directed edge corresponds to a region of query space where it
certifiably helps search.**

---

## 2. Descent theorem (deterministic)

**Theorem 2.** If every nonterminal node u visited by greedy search has at
least one ρ-descent witness in `N⁺_live(u)`, then search reaches a τ-terminal
node in at most
```
H(q) = ⌈ log( d(u₀,q) / (τ·r_k(q)) ) / log(1/ρ) ⌉⁺
```
steps.

*(Proof: induction on `D_t ≤ ρ^t · D_0` from the witness condition.)*

---

## 3. Query-space descent-cover invariant

For node u with local query distribution `μ_u` and neighbor set `S = N⁺(u)`:
```
Uncov_ρ(u, S) = μ_u( { q : ∀v ∈ S ∩ L,  d(v,q) > ρ·d(u,q) } )
              = μ_u( Q_u \ ⋃_{v ∈ S ∩ L} A_ρ(u,v) ).
```

**Invariant:**
```
Uncov_ρ(u, N⁺(u)) ≤ η_u.
```

---

## 4. Descent-cover recall bound

**Theorem 3.** Under Theorem 2's witness rule with per-step uncovered mass `≤ η`:
```
Pr[failure before τ-terminal] ≤ H(q) · η.
```

---

## 5. Tombstone stability

Hazard `h(v) ∈ [0,1]` bounds `Pr[v unavailable before next repair]`.
**Survival weight:** `γ(v) = −log h(v)`. For witness set `W_ρ(u,q)` at (u,q):
```
Γ(u,q) = Σ_{v ∈ W_ρ(u,q)}  γ(v) = Σ −log h(v).
```

**Theorem 4.** If `Γ(u,q) ≥ Λ` and witness unavailability obeys the product
bound (or conditional dominance), then
```
Pr[no descent witness survives at u] ≤ e^(−Λ).
```

**Theorem 5 (tombstone-stable approximate recall).** If every nonterminal u
on the search route satisfies `Γ(u,q) ≥ Λ`:
```
Pr[tombstone-caused failure] ≤ H(q) · e^(−Λ).
```

---

## 6. Combined failure bound

**Corollary 6.**
```
Pr[failure before τ-terminal] ≤ H(q) · (η + e^(−Λ)).
```
*This is the cleanest theorem for RavenDB.*

---

## 7. Sampled construction (uniform convergence)

For candidate pool `C(u)` of size n, budget M, m sampled queries `q_1,…,q_m ~ μ_u`:
```
L(S)  = μ_u( { q : Γ_S(u,q) < Λ } )    -- true loss
L̂(S) = (1/m) Σ_i 1[Γ_S(u,q_i) < Λ]   -- empirical loss
```

**Theorem 7.** With probability ≥ 1−δ, for **all** S ⊆ C(u) with |S| ≤ M:
```
|L(S) − L̂(S)| ≤ √( (M·log(en/M) + log(2/δ)) / (2m) ).
```
*(Proof: Hoeffding + union bound over `Σ_{j≤M} C(n,j) ≤ (en/M)^M` sets.)*

Sample complexity: `m = O( (M·log(n/M) + log(1/δ)) / ε² )`.

---

## 8. Construction objective (greedy, submodular)

For each sample `q_i`, survival contribution from v:
```
a_{v,i} = { −log h(v)  if d(v,q_i) ≤ ρ·d(u,q_i)
          { 0          otherwise.
```
**Capped survival coverage** (the maximization objective):
```
F(S) = Σ_i min( Λ, Σ_{v ∈ S} a_{v,i} ).
```

**Lemma 8.** F is **monotone submodular** (sum of `min(Λ, modular)` and `min`
of a constant with a nondecreasing concave function of a nonnegative modular
function is submodular).

**Theorem 9.** Greedy max-cover under cardinality M:
```
F(S_greedy) ≥ (1 − 1/e) · F(S*).
```

---

## 9. I/O-aware

Edge cost:
```
c(u,v) = c_dist(v) + λ_page·c_page(u,v) + λ_haz·c_haz(v) + λ_deg·c_deg(v).
```
Cheapest live descent edge: `C_ρ(u,q) = min{ c(u,v) : v ∈ N⁺ ∩ L, d(v,q) ≤ ρ·d(u,q) }`.

**Theorem 10.** If each chosen `c(u_t, u_{t+1}) ≤ β·C_ρ(u_t,q)`:
```
Cost(q) ≤ β · Σ_{t<H(q)} C_ρ(u_t,q)  +  H(q)·c_queue.
```

---

## 10. Recall@k via local k-capture

**Definition.** Graph satisfies **local k-capture** with beam width B if,
upon visiting any live u with `d(u,q) ≤ τ·r_k(q)`, layer-0 beam search of
width B visits every point in `B_k(q) = { x ∈ L : d(x,q) ≤ r_k(q) }`.

**Theorem 11.** Descent-cover + tombstone bound + local k-capture ⇒
```
Pr[recall@k failure] ≤ H(q) · (η + e^(−Λ)).
```

**Open subproof.** Tighten local k-capture from an assumption into a
consequence of a stronger base-layer cover invariant. (Currently L0 needs
both descent coverage **and** local k-capture; the next theorem should
derive capture from a beefier cover.)

### 10.A. Theorem 11′ — local k-capture from a strengthened base-layer cover

**Strengthened base-layer invariant.** For every live node `u` at L0, every
live point `x ∈ B_k(q)` for some query `q`, and every τ ≥ 1/ρ:

  (★)  if  `d(u,q) ≤ τ·r_k(q)`  then there exists `v ∈ N⁺(u) ∩ L` with
       `d(v, x) ≤ ρ · d(u, x)`.

Note **(★)** is stricter than the cover invariant of §3: it asks for a
witness against the **actual top-k live points** rather than against a
random query sample.

**Theorem 11′.** If (★) holds at L0 and `τ ≥ 1/ρ`, then for any query `q`
and any τ-terminal node `u` reached by search, beam search of width `B ≥ k`
from `u` visits every point in `B_k(q)` in at most `⌈log_(1/ρ) τ⌉` rounds.

**Proof sketch.**

Step 1 — every `x ∈ B_k(q)` is itself a ρ-descent witness for u toward q.
Because u is τ-terminal,
```
d(u,q) ≤ τ·r_k(q)   ⇒   r_k(q) ≤ d(u,q)/τ ≤ ρ·d(u,q).
```
Therefore for `x ∈ B_k(q)`, `d(x,q) ≤ r_k(q) ≤ ρ·d(u,q)`.

Step 2 — (★) provides a witness toward each `x`. By (★) applied with q=x
(which is in B_k(x) trivially since r_k(x)=0; equivalently, x ∈ B_k(q)
implies x is reachable by a ρ-descent step), some neighbor `v ∈ N⁺(u) ∩ L`
satisfies `d(v, x) ≤ ρ · d(u, x)`. So beam search expanding u sees a
neighbor that has moved closer to `x` by factor ρ.

Step 3 — `⌈log_(1/ρ) τ⌉` iterations suffice. By induction on the depth of
beam expansion: after `t` rounds, the beam contains some live `u_t` with
`d(u_t, x) ≤ ρ^t · d(u_0, x)`. Since `u_0 = u` and `d(u, x) ≤ d(u,q) +
d(q,x) ≤ τ·r_k(q) + r_k(q) = (τ+1)·r_k(q)`, after `t = ⌈log_(1/ρ)(τ+1)⌉`
rounds, `d(u_t, x) ≤ r_k(q)`, so `u_t = x` or `u_t ∈ B_k(q)`.

Step 4 — beam width B ≥ k holds all k points. Beam search of width B keeps
the B closest unvisited candidates. After Step 3, each `x ∈ B_k(q)` is
reached on some descent path of length `≤ ⌈log_(1/ρ)(τ+1)⌉`. Since all k
of them have distance ≤ r_k(q) and the beam keeps the B closest, with B ≥ k
all k are retained.   ∎

**What (★) costs at construction.** At L0 the cover must witness against the
top-k live neighbors of each potential `q`. Approximated by sampling: take
`Q_u^{L0} ⊇ {top-k live neighbors of u itself}`. So **the framework's
descent-cover construction at L0 must include the candidate's own
top-k as members of the witness set**.

**Implementation consequence.** The current `BuildGlobalQuerySample` draws
Q_u uniformly at runner construction from already-known nodes. For (★) to
hold approximately, augment Q_u (at L0 only) with each node's own
nearest-k candidates during `FilterEdgesHeuristicWorker`. This is exactly
the existing Theorem 11 `kCapture = M/2` reserve — but the reserve is for
*pick-into-N(u)*, not for *measurement-in-Q_u*. Both are needed: pick the
nearest as edges (capture) AND treat them as witness anchors (cover).

**Empirical caveat (2026-05-14).** The naive form of this augmentation —
appending the candidate's own M/2-nearest from C as additional Q_u
anchors — degraded uniform-recall by ~14pp because the appended anchors
are themselves the about-to-be-picked candidates, so every near v
trivially satisfies the witness threshold and the cover greedy collapses
toward pick-nearest-only. Implementation reverted. A faithful (★)
approximation must use witness anchors that are external to C (e.g.,
sampled from already-live points outside the candidate pool); this is an
open implementation question.

---

## 10.B. Open finding: recall vs theorem ceiling on isotropic data

The bound proofs (Theorems 6, 8, 9) give an **upper bound** on failure
probability via `Pr[fail] ≤ H(q)·(η + e^(−Λ))`. Empirical measurement on
the current implementation confirms the ceiling improves vs the legacy
α-prune selector (6.30 vs 6.64, Δ≈−0.35). However, **actual measured
recall@k is uniformly lower** on isotropic high-dim data:

| Dataset                        | Legacy R@10 | Apollonius R@10 | Δ      |
|--------------------------------|-------------|-----------------|--------|
| Gaussian d=128 N=10k ef=64     | 0.410       | 0.310           | −0.100 |
| Gaussian d=128 N=10k ef=256    | 0.813       | 0.655           | −0.158 |
| Uniform d=32 N=20k ef=64       | 0.586       | 0.446           | −0.140 |
| Clusters d=32 ef=64            | 0.988       | 0.946           | −0.042 |
| Clusters d=32 ef=256           | 1.000       | 0.997           | −0.003 |

The gap **grows with ef on isotropic data** (the bound says it should
shrink). This means the construction-time ceiling is honest but the
cover criterion lacks the **angular-diversity** guarantee that
α-pruning's `d(v',v) > d(u,v)/α` directly provides. On clustered data
the data manifold supplies that diversity for free and the gap closes;
on isotropic data nothing supplies it and Apollonius cover collapses
toward picking near edges that, while individually high-witness, fail
to span enough directions.

**This is a real obstruction, not a parameter-tuning issue.** Verified:
ρ ∈ {0.70, 0.90}, K ∈ {1, 2}, neither rescues uniform/high-dim recall.
The next theoretical step is to either (i) prove an angular-spread
lemma derives from the cover invariant under sufficient |Q_u|, or (ii)
recognize cover and angular-diversity as **distinct** invariants and
let the construction enforce both.

---

## 11. The RavenDB invariant

```
∀u : μ_u( { q : Γ_{N(u)}(u,q) < Λ } ) ≤ η,

where  Γ_{N(u)}(u,q) = Σ_{v ∈ N(u)} 1[d(v,q) ≤ ρ·d(u,q)] · (−log h(v)).
```

Theorem chain:
```
sampled construction ⟶ empirical cover ⟶ true cover ⟶ descent path
                    ⟶ approximate recall ⟶ local capture ⟶ recall@k.
```

---

## 12. Knobs (with mathematical meaning)

| Symbol | Meaning | Effect |
|---|---|---|
| ρ | descent factor | smaller = shorter paths, harder cover |
| η | uncovered query mass | lower = better recall |
| Λ | tombstone survival budget | higher = stronger delete tolerance |
| M | degree budget | larger = cover easier |
| m | local query samples | larger = better statistical confidence |
| H(q) | path-length bound | lower = fewer distance calls / page reads |
| τ | terminal approximation radius | lower = stricter ε-recall |
| B | beam width (capture) | larger = better recall@k after basin |

---

## 13. What the proofs DO and DO NOT establish

**Proved (under stated assumptions):**
- Descent path length bound H(q) (Theorem 2)
- Recall-failure bound from η alone (Theorem 3)
- Tombstone-failure bound from Λ alone (Theorems 4–5)
- Combined failure bound (Corollary 6)
- Sample-to-true cover generalization (Theorem 7)
- Greedy (1−1/e) construction guarantee (Theorem 9)
- I/O-bounded routing (Theorem 10)
- Recall@k under local k-capture (Theorem 11)

**Not proved:**
- That arbitrary HNSW satisfies the invariant.
- That local k-capture follows from descent coverage alone — currently an
  assumption at L0.
- Empirical instantiation: must measure η̂, Λ̂, Ĥ on a real workload and
  verify the bound `Pr̂[failure] ≤ Ĥ(q)·(η̂ + e^(−Λ̂)) + ε_m`.

---

## 14. RavenDB validation procedure (testable)

For each sampled node u:
```
η̂(u) = #{ q_i : Γ_{N(u)}(u, q_i) < Λ } / m.
```
For each query: `Ĥ(q)` = observed nonterminal descent steps. Hazard
estimated from churn/tombstone rate. Sample error from Theorem 7:
```
ε_m = √( (M·log(en/M) + log(2/δ)) / (2m) ).
```
The bound to verify:
```
Pr̂[failure] ≤ H(q)·(η̂ + ε_m + e^(−Λ̂)).
```

---

## 15. Implementation status (current branch `hnsw-apollonius`)

| Theorem | Object | Code path |
|---|---|---|
| 1 (Apollonius cell) | witness bit `d(v,q) ≤ ρ·d(u,q)` | `DoWorkApolloniusCover` witness loop |
| 7 (sampled Q_u) | `GlobalQuerySample`, |Q_u|=32, fixed seed | `BuildGlobalQuerySample` |
| 9 (greedy submodular) | uncapped greedy by uncovered-bits | `DoWorkApolloniusCover` greedy loop |
| 11 (k-capture L0) | M/2 reserved nearest at Level==0 | `kCapture` branch |
| 2/3/6 (recall bound) | Ĥ, η̂ instrumentation | `HnswDescentCoverDiagnostic` (partial) |
| 4/5 (survival weights) | γ(v) ≡ 1 currently | **NOT IMPLEMENTED** |
| 8 (capped cover with Λ) | uncapped popcount used | **NOT IMPLEMENTED** |
| 10 (I/O cost) | uniform cost assumed | **NOT IMPLEMENTED** |

Missing pieces (in order of math priority):
1. Capped-survival objective `F(S) = Σ_i min(Λ, Σ a_{v,i})` — Theorem 8 needs
   the `min(Λ, ·)` clamp; currently we maximize raw uncovered-bit popcount
   (no Λ saturation, no γ weight).
2. Hazard estimator h(v) — even a constant h₀ makes γ(v) finite, lets us
   measure Γ̂ and Λ̂.
3. Repair-on-deficit — invoke `BuildDescentCover(p)` when
   `deficit(p) = μ̂_p({q : Γ̂_{N(p)}(p,q) < Λ}) > θ`.
4. End-to-end diagnostic that measures `Pr̂[failure]` and checks the bound.
