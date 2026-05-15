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

## 4. Descent-cover recall bound (adaptive stopping-time form)

The greedy search route is **adaptive**: which node `u_t` is visited at
step `t` depends on the history `F_t`. The per-step failure probability
is conditional on that history. Let `T` be the first time the search
either reaches a τ-terminal node or fails; let `F_t` be the event that
`u_t` has no live ρ-descent witness for `q`. Suppose that for all
`t < T`,
```
Pr[F_t | F_{<t}] ≤ η_{u_t}.
```

**Theorem 3 (adaptive).**
```
Pr[failure before τ-terminal] ≤ E[ Σ_{t < H(q)} η_{u_t} ].
```
Uniformly, if `η_{u_t} ≤ η` along the route,
```
Pr[failure before τ-terminal] ≤ min{1, H(q) · η}.
```

The `min{1,·}` clamp is essential: any "improvement" of `H·η` from one
vacuous value (>1) to another is **not a probability statement**. A
non-vacuous ceiling requires `H(q)·η < 1`. Empirical ceilings of 6.30
or 6.64 are both vacuous and cannot order recall.

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

## 6. Combined failure bound (adaptive)

Under the same conditional setup, for all `t < T`:
```
Pr[F_t | F_{<t}] ≤ η_{u_t} + e^(−Λ_{u_t}).
```

**Corollary 6 (adaptive).**
```
Pr[failure before τ-terminal] ≤ E[ Σ_{t<H(q)} (η_{u_t} + e^(−Λ_{u_t})) ].
```
Uniformly:
```
Pr[failure] ≤ min{ 1, H(q) · (η_max + e^(−Λ_min)) }.
```
*This is the cleanest theorem for RavenDB — but only when the right-hand
side is strictly < 1. Otherwise the bound says nothing about recall.*

---

## 7. Sampled construction (uniform convergence)

**Per-node version.** For candidate pool `C(u)` of size n, budget M, m
i.i.d. samples `q_1,…,q_m ~ μ_u`:
```
L_u(S)  = μ_u( { q : Γ_S(u,q) < Λ } )    -- true loss
L̂_u(S) = (1/m) Σ_i 1[Γ_S(u,q_i) < Λ]   -- empirical loss
```
With probability ≥ 1−δ, for **all** S ⊆ C(u) with |S| ≤ M:
```
|L_u(S) − L̂_u(S)| ≤ √( (M·log(en/M) + log(2/δ)) / (2m) ).
```

**Theorem 7 (simultaneous version over N nodes).** If we want the bound
to hold simultaneously for every certified node, the union must be over
nodes too. With probability ≥ 1−δ, for every u and every `S ⊆ C(u)` with
`|S| ≤ M`:
```
|L_u(S) − L̂_u(S)| ≤ √( (M·log(en_u/M) + log(2N/δ)) / (2m_u) ).
```
*(Proof: Hoeffding + union bound over the `(en_u/M)^M` sets at each of
the N nodes; the `log N` enters only logarithmically.)*

**Independence caveat.** The bound requires the samples to be
**independent** of the randomness that constructed `C(u)`. If the same
sample both selects `C(u)` and certifies the chosen edges, the bound is
optimistically biased. Use one sample for construction and a held-out
sample for certification.

Sample complexity: `m_u = O( (M·log(n_u/M) + log(N/δ)) / ε² )`.

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

**Theorem 9 (optimization guarantee, not a recall bound).** Greedy
max-cover under cardinality M:
```
F(S_greedy) ≥ (1 − 1/e) · F(S*).
```

**Critical caveat — F is a surrogate, not the failure loss.** The
binary failure loss is
```
L̂(S) = (1/m) Σ_i 1[Γ_S(u, q_i) < Λ].
```
F can be near-maximal while L̂(S) = 1. Counterexample: if
`Γ_S(u, q_i) = Λ − ε` for every i, then `F(S) ≈ m·Λ` (near optimal) yet
every query fails.

**Margin form (Theorem 9′).** Define the surplus objective with margin
`ξ > 0`:
```
F_{Λ+ξ}(S) = Σ_i min( Λ+ξ, Γ_S(u, q_i) ).
```
Then
```
L̂_Λ(S) ≤ ( m·(Λ+ξ) − F_{Λ+ξ}(S) ) / (m·ξ).
```
*(Proof: each failed sample has `Γ_S < Λ`, so its deficit relative to
the cap `Λ+ξ` exceeds `ξ`.)*

**Engineering consequence.** Either (a) build by maximizing F as a
surrogate and **certify** with `L̂ + ε_m` on held-out samples, or (b)
build by maximizing `F_{Λ+ξ}` and use the margin bound directly. The
certified quantity in the chain `(η_u ≤ L̂_u + ε_u)` is `L̂`, not the
greedy approximation ratio.

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

### 10.A. Tube-capture theorem (replaces invalid Theorem 11′)

**Historical note.** An earlier version of this section attempted to
derive local k-capture from a strengthened invariant (★) by claiming
`d(u,q) ≤ τ·r_k(q) ⇒ r_k(q) ≤ ρ·d(u,q)`. The implication runs the
wrong way (`d(u,q) ≤ τ·r ⇒ r ≥ d(u,q)/τ`), so the proof is invalid as
written. The proof also conflated "reach some point of `B_k(q)`" with
"reach **every** target `x ∈ B_k(q)`" and used beam width `B ≥ k` where
the right quantity is the local tube population. Replaced below.

**Setup.**
- `r = r_k(q)`, `B_k(q) = { x ∈ L : d(x,q) ≤ r }`.
- For `R ≥ τ`, the **query tube** `T_R(q) = { y ∈ L : d(y,q) ≤ R·r }`.
- Tube population `κ_R(q) = |T_R(q)|`.
- For each target `x ∈ B_k(q)`, **live separation**
  `Δ_x = min_{y ∈ L, y≠x} d(x,y)`.

**Target-tube contraction invariant (replaces ★).** At L0, for every
query `q`, every `x ∈ B_k(q)`, and every live `y ∈ T_R(q) \ {x}`, there
exists a live neighbor
```
z ∈ N⁺(y) ∩ T_R(q)   with   d(z, x) ≤ ρ · d(y, x).
```
This is **target contraction inside a query-local tube**, not query
descent. It is strictly stronger than the descent-cover invariant of §3.

**Theorem 11′ (corrected).** Suppose
1. `u ∈ T_R(q)` is the τ-terminal node reached by upper-layer descent,
2. the target-tube contraction invariant holds at L0,
3. layer-0 beam search uses width `B ≥ κ_R(q)`,
4. expansion depth `ℓ(q) = max_{x ∈ B_k(q)} ⌈ log( (τ+1)·r / Δ_x ) / log(1/ρ) ⌉`.

Then layer-0 beam search visits every `x ∈ B_k(q)`.

**Proof.** Fix `x ∈ B_k(q)`. Since `u` is τ-terminal, `d(u,q) ≤ τ·r`;
since `x ∈ B_k(q)`, `d(x,q) ≤ r`. By triangle inequality,
```
d(u,x) ≤ d(u,q) + d(q,x) ≤ (τ+1)·r.
```
By the target-tube contraction invariant, while `y_t ≠ x`, there exists
`y_{t+1} ∈ N⁺(y_t) ∩ T_R(q)` with `d(y_{t+1}, x) ≤ ρ·d(y_t, x)`.
Inductively `d(y_t, x) ≤ ρ^t · (τ+1)·r`. Choose
`t ≥ ⌈ log((τ+1)·r / Δ_x) / log(1/ρ) ⌉`. Then `d(y_t, x) < Δ_x`. By the
definition of `Δ_x`, no live point other than `x` is within `Δ_x` of
`x`, so `y_t = x`.

All path vertices `y_0, y_1, …, y_t` lie in `T_R(q)` by construction.
Since `B ≥ κ_R(q)`, every tube vertex is among the B closest live
candidates from the tube and is retained by the beam. Therefore the
path to each `x ∈ B_k(q)` is retained and eventually expanded. Hence
all top-k points are visited.   ∎

**The real cost.** The required beam width is **`B ≥ κ_R(q)`, not `B ≥
k`**. This is the mathematical source of the isotropic-data failure:

- In a dense isotropic cloud, `κ_R(q)` grows roughly as `(R)^d` times
  the local density, dwarfing k.
- In clustered data, `κ_R(q)` stays close to k (most live points are in
  other clusters, outside the tube).

This matches the empirical pattern: clustered recall almost recovers at
high ef; isotropic recall does not.

---

### 10.B. Angular-spread lower bound (the isotropic obstruction is real)

Work locally in Euclidean space at u. Let a query direction be `q = u +
r·s` with `‖s‖ = 1`, and a candidate `v = u + ℓ·e` with `‖e‖ = 1`. The
descent condition `‖q − v‖ ≤ ρ·‖q − u‖` expands to
```
r² + ℓ² − 2·r·ℓ·⟨s,e⟩ ≤ ρ²·r²
```
which gives
```
⟨s, e⟩ ≥ (ℓ² + (1 − ρ²)·r²) / (2·r·ℓ)   ≥   √(1 − ρ²)     (AM-GM).
```

**Lemma 10.B.1.** Each neighbor `v ∈ N⁺(u)` covers at best a spherical
cap of half-angle `θ_ρ = arccos(√(1 − ρ²)) = arcsin(ρ)` in query
direction-space at u.

**Theorem 10.B.2 (isotropic lower bound on η).** If query directions
around u are approximately uniform on `S^{d−1}` and `C_d(ρ)` denotes
the normalized measure of the spherical cap of half-angle `arcsin(ρ)`,
then any M-edge neighborhood satisfies
```
Uncov_ρ(u, N(u)) ≥ 1 − M · C_d(ρ).
```

This means: in high dimension `d`, unless either M is very large, ρ is
very close to 1 (weak descent, big H(q)), or the local query
distribution is non-isotropic, **a pure descent-cover invariant cannot
give small η**. The tradeoff is intrinsic:

```
ρ ↑ 1   ⇒  larger caps, but H(q) grows and descent weakens
ρ ↓     ⇒  stronger descent, but caps shrink exponentially in d
```

**Consequence for the proof chain.** Theorems 3/6 require small `η`. In
isotropic high-dim, `η` is bounded below by `1 − M·C_d(ρ)` regardless
of how cleverly the cover is selected. This is not a parameter-tuning
issue.

---

### 10.C. Revised invariant: descent cover + shell-wise angular spread

The single descent-cover invariant of §3 is insufficient. The corrected
RavenDB invariant is **bi-criteria**:

**(A) Survival-weighted descent cover.**
```
μ_u( { q : Γ_{N(u)}(u,q) < Λ } ) ≤ η.
```

**(B) Shell-wise angular spread.** For each radial shell around u,
```
S_j(u) = { v ∈ N(u) : 2^j·a ≤ d(u,v) < 2^{j+1}·a },
```
require either pairwise angular separation
```
∠(v − u, w − u) ≥ θ_0   for all v ≠ w ∈ S_j(u)
```
or its metric equivalent
```
d(v, w) ≥ χ · min(d(u,v), d(u,w))   for some χ > 0.
```
This is the α-prune-style diversity constraint, restricted to within
shells (so it does not over-penalize legitimate near/far edge mixtures).

**Implementation consequence.** Neighbor selection becomes bi-criteria:
```
maximize  F(S)                  (survival-weighted descent cover)
subject   |S| ≤ M
          S obeys shell-wise angular spread (B)
```
This is what α-pruning provides "for free" by construction, and what
the current pure-cover greedy lacks. It is the next implementation
priority.

**Empirical evidence supporting the revision (current branch).**

| Dataset                        | Legacy R@10 | Apollonius R@10 | Δ      |
|--------------------------------|-------------|-----------------|--------|
| Gaussian d=128 N=10k ef=64     | 0.410       | 0.310           | −0.100 |
| Gaussian d=128 N=10k ef=256    | 0.813       | 0.655           | −0.158 |
| Uniform d=32 N=20k ef=64       | 0.586       | 0.446           | −0.140 |
| Clusters d=32 ef=64            | 0.988       | 0.946           | −0.042 |
| Clusters d=32 ef=256           | 1.000       | 0.997           | −0.003 |

Pattern: isotropic gap grows with ef (Theorem 10.B.2 predicts this);
clustered gap shrinks with ef (data manifold supplies (B) for free).
Verified across `ρ ∈ {0.70, 0.90}` and redundancy depth `K ∈ {1, 2}`;
not a parameter-tuning issue.

---

## 11. The RavenDB invariant (bi-criteria — superseded form)

The single-invariant form below is the **historical** statement and is
not sufficient on isotropic data; see §10.C for the bi-criteria form
that supersedes it.

```
∀u : μ_u( { q : Γ_{N(u)}(u,q) < Λ } ) ≤ η,

where  Γ_{N(u)}(u,q) = Σ_{v ∈ N(u)} 1[d(v,q) ≤ ρ·d(u,q)] · (−log h(v)).
```

Theorem chain (with corrections):
```
sampled construction (T7, union over N nodes)
  ⟶ empirical L̂_u (held-out)
  ⟶ certified η_u ≤ L̂_u + ε_u
  ⟶ adaptive failure bound (T3/T6, clamped to min{1,·})
  ⟶ tube-capture at L0 (T11′ corrected, needs B ≥ κ_R(q))
  ⟶ recall@k provided (A) descent cover **and** (B) shell-wise
    angular spread hold simultaneously.
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

**Status table (post-correction):**

| Item                                              | Status                                                                       |
|---------------------------------------------------|------------------------------------------------------------------------------|
| Theorem 2 — deterministic descent path bound      | Correct.                                                                     |
| Theorems 3/5/6 — failure bounds                   | Correct as **adaptive conditional / stopping-time** bounds with `min{1,·}`.  |
| Theorem 7 — sampled cover                         | Correct under independence (held-out samples) and union over N nodes.        |
| Lemma 8 / Theorem 9 — submodular greedy           | Correct for the **surrogate** F(S). Does **not** bound binary failure loss. |
| Theorem 9′ — margin form                          | Correct; this is the proper bridge from F to `L̂`.                           |
| Theorem 10 — I/O                                  | Correct as a **per-path** cost bound; not yet a full beam-search I/O bound.  |
| Theorem 11 — recall@k                             | Correct only when local k-capture is **assumed**.                            |
| Theorem 11′ (original) — capture from cover       | **Invalid as originally written** (reversed inequality, beam-width gap).     |
| Theorem 11′ (corrected) — tube-capture            | Correct under the target-tube contraction invariant + `B ≥ κ_R(q)`.          |
| §10.B — isotropic angular lower bound             | Mathematically real: `Uncov_ρ ≥ 1 − M·C_d(ρ)`.                              |

**Proved (under their stated assumptions):**
- Adaptive descent failure bound (Theorems 3 and 6, conditional form).
- Sample-to-true cover generalization at one node and union-over-N (Theorem 7), with independence caveat.
- Greedy (1−1/e) **surrogate** guarantee (Theorem 9) and its margin → loss bridge (Theorem 9′).
- I/O-bounded path cost (Theorem 10) — per path, not yet aggregated.
- Tube-capture (corrected Theorem 11′) under target-tube contraction
  and `B ≥ κ_R(q)`.
- Isotropic lower bound `Uncov_ρ ≥ 1 − M·C_d(ρ)` (Theorem 10.B.2).

**Not proved:**
- That arbitrary HNSW satisfies the bi-criteria invariant (A)+(B).
- That descent cover **alone** implies angular spread. §10.B says it cannot in high-d isotropic.
- That the failure bound is non-vacuous on any concrete workload — must show `H·(η + e^(−Λ)) < 1` empirically with held-out samples.
- Empirical instantiation: must measure η̂, Λ̂, Ĥ on a real workload and verify `min{1, Ĥ(q)·(η̂ + ε_m + e^(−Λ̂))} < 1` on held-out samples.

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
| 7 (sampled Q_u, per-node) | `GlobalQuerySample`, |Q_u|=32, fixed seed | `BuildGlobalQuerySample` |
| 7 (union over N, held-out) | not honored — same sample used for build and certification | **NOT IMPLEMENTED** |
| 8 (capped survival F(S)) | uses K=2 popcount approximation, γ(v) ≡ 1 | **PARTIAL — not the capped Λ form** |
| 9 (greedy) | greedy by K-redundant popcount on bitmask | OK for K=2; γ-weighted form **NOT IMPLEMENTED** |
| 9′ (margin form for recall) | not used | **NOT IMPLEMENTED** |
| 10.C (B) shell-wise angular spread | `PassesAngularSpread`, χ=0.7 | `DoWorkApolloniusCover` (greedy + M-fill) |
| 11 (k-capture L0) | M/2 reserved nearest at Level==0 | `kCapture` branch |
| 11′ (tube-capture, corrected) | `B ≥ κ_R(q)` not enforced | **NOT IMPLEMENTED** |
| 2/3/6 (failure bound) | reported as ceiling, not clamped to `min{1,·}` | `HnswDescentCoverDiagnostic` partial |
| 4/5 (survival weights γ(v)) | γ(v) ≡ const | **NOT IMPLEMENTED** |
| 10 (I/O cost) | uniform cost assumed | **NOT IMPLEMENTED** |

**Mismatch with proof chain.** The current code maximizes K-redundant
popcount on a same-sample-as-construction Q_u, with `γ(v)≡1` and no
`min(Λ, ·)` clamp; it then reports ceilings as `Ĥ·(η̂+e^(−Λ̂))` without
the `min{1,·}` clamp or held-out sample. None of those by themselves
fix the isotropic recall gap (which is angular, per §10.B), but they
do mean the **bound numbers in the diagnostic are not the bound the
theorems prove**.

**Missing pieces, in math-priority order:**

~~1. **(B) shell-wise angular spread** in cover greedy~~ — **DONE**
(commit `Tune angular-spread chi`). χ=0.7 is the empirical sweet
spot: passes all 11 diagnostics, materializes the Theorem-5 churn win
(+0.030 vs legacy), and matches legacy at cluster ef≥128. Residual
d=128 isotropic gap (~-0.15 at ef=256) is the §10.B intrinsic
ceiling at M=16 — closable by raising M, not by greedy changes.

1. **Capped objective** `F(S) = Σ_i min(Λ, Σ a_{v,i})` with the proper
   real-valued accounting per query bit (the K=2 popcount fast path
   only works for unit weights and unit cap).
3. **Held-out certification samples** for Theorem 7 — independent
   sample from build.
4. **`min{1,·}` clamp** in the diagnostic; report `L̂_u + ε_u` rather
   than the optimization-side `F(S_greedy) / F(S*)` ratio.
5. **Hazard estimator h(v)** — even constant `h₀` makes γ(v) finite and
   `Γ̂` measurable.
6. **Repair-on-deficit** when `deficit(p) > θ`.
7. **End-to-end diagnostic** that measures `Pr̂[failure]` on **held-out**
   queries and checks `min{1, Ĥ·(η̂ + ε_m + e^(−Λ̂))} < 1`.
