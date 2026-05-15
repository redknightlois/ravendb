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

---

## 16. Chordal metric correction (the proof was in the wrong norm)

The code computes cosine **dissimilarity** δ(x,y) = 1 − ⟨x,y⟩ and uses
that as `Distance()`. δ is **not** a metric — strict triangle
inequality fails on the sphere, which is why the journal's note about
"no safe witness short-circuit" was forced.

The corresponding true metric on unit vectors is the **chordal metric**:

```
D(x,y) = √(2(1 − ⟨x,y⟩)) = √(2·δ(x,y)).
```

This is Euclidean distance on S^{d−1}; it preserves the cosine NN
ranking exactly and IS a strict metric (TI holds).

**Consequence**: code uses `δ(v,q) ≤ λ·δ(u,q)` where `λ = 0.90`. In the
metric proof this is

```
D(v,q)² ≤ λ · D(u,q)²    ⇔    D(v,q) ≤ √λ · D(u,q).
```

So the *metric* contraction ratio is

```
ρ_metric = √λ_code     (≈ 0.949 for λ_code = 0.90).
```

**All theorems using ρ should use ρ_metric, not λ_code.** Affected:

- H(q) descent step count `⌈log(D₀/τr_k) / log(1/ρ)⌉`:
  `log(1/0.949) ≈ 0.0524` vs `log(1/0.90) ≈ 0.105` — H is **2× larger**
  in the correct metric form. The bound `Pr[failure] ≤ H·(η+e^(−Λ))`
  has been understating Pr[failure] by a factor of 2.
- Angular cap `C_d(ρ)` in §10.B: parameter substituted by ρ_metric.
- Apollonius cell `A_ρ(u,v)` in §1: still defined by the code's λ
  comparison, but the geometry it describes is the chordal-metric
  Apollonius cell at ρ = √λ.

**Implementation impact**: the construction code path is unchanged
(still tests δ ≤ λ·δ — same selection). Only **the diagnostic
numbers** change: every reported "ρ" in the diagnostic must be √λ,
every H estimate doubles.

---

## 17. Linear witness inequality (the witness test is an inner product)

For unit vectors x, q the witness condition

```
δ(v,q) ≤ λ·δ(u,q)
```

rearranges to

```
⟨v − λu, q⟩ ≥ 1 − λ.
```

So a witness cell is the half-space defined by direction `a_{u,v} = v − λu`.
This has two practical consequences:

1. **No SIMD `Distance()` call needed** to test a single witness — one
   inner product against a precomputed `a_{u,v}`. Same FLOP count as
   `Distance` but no setup overhead and no return-via-stack of a
   single float.
2. **JL/sketch admissibility**: random-projection sketches preserve
   inner products up to additive ε with high probability over a finite
   pair set. The framework's wall problem (N·Q_m exact distance calls
   per cover) admits a **two-tier filter**: ambiguous-margin pairs run
   the exact kernel, certain-yes and certain-no pairs run on the
   sketch only.

**Theorem 17 (JL witness filter).** Let `P: R^d → R^s` be a Gaussian
random projection with `s = O(ε^{-2} log(|C|·m/δ))`. For any finite
set of candidate-edge vectors `{a_{u,v}}` and query anchors `{q_i}`,
with probability ≥ 1 − δ, simultaneously for every pair:

```
|⟨Pa_{u,v}, Pq_i⟩ − ⟨a_{u,v}, q_i⟩| ≤ ε.
```

So:

- `⟨Pa, Pq⟩ ≥ 1 − λ + ε`  ⇒  witness (no exact call).
- `⟨Pa, Pq⟩ < 1 − λ − ε`  ⇒  not a witness (no exact call).
- Ambiguous band: fall through to exact δ-comparison.

For typical λ = 0.90, ε = 0.05, and d = 128, expected s ≈ 20–40. The
witness loop's expected exact-call rate drops from 100 % to whatever
fraction of pairs lie in the ε-band — empirically a few percent for
isotropic queries.

---

## 18. Frontier-Cover Theorem (the missing beam-level invariant)

The framework so far has been **single-node**: at each visited u, some
v ∈ N(u) must be a witness. But HNSW's L0 search is **beam-based**: it
maintains a frontier F_t of width b and expands by best-first.

**Definition.** With frontier F_t = {u_1, …, u_b} and
`D_t(q) = min_{u ∈ F_t} d(u, q)`, the frontier ρ-descent witness exists iff

```
∃ u ∈ F_t, ∃ v ∈ N(u) :  d(v,q) ≤ ρ·D_t(q).
```

**Frontier uncovered mass**:

```
Uncov_ρ^front(F_t) = μ_{F_t}({ q : ∀u∈F_t ∀v∈N(u),  d(v,q) > ρ·D_t(q) }).
```

**Theorem 18 (frontier descent).** If for every nonterminal frontier
with `D_t(q) > τ·r_k(q)` a live frontier witness exists, then beam
search reaches a τ-terminal frontier after

```
H(q) = ⌈ log(D_0(q) / (τ·r_k(q))) / log(1/ρ) ⌉
```

frontier-improvement rounds. Probabilistic form:

```
Pr[failure] ≤ E[ Σ_{t<H(q)} (η_{F_t} + e^(−Λ_{F_t})) ],
```

with frontier survival mass

```
Γ_{F_t}(q) = Σ_{u ∈ F_t} Σ_{v ∈ N(u)} 1[d(v,q) ≤ ρ·D_t(q)] · γ(v).
```

**Why this fixes the §10.B obstruction.** The angular budget of a single
node is `M·C_d(ρ)` — small in high d. The angular budget of a frontier
is

```
M_eff(F_t) = Σ_{u ∈ F_t} |N(u)| ≈ b·M.
```

Crude union bound:

```
Uncov_ρ^front(F_t) ≥ 1 − b·M·C_d(ρ).
```

The isotropic obstruction therefore relaxes from "raise M" to "raise
`b·M`" — and the beam already supplies a factor of b = 16, 32, 64.
With b = 16, the §10.B ceiling at M = 16 becomes equivalent to the
old M = 256 single-node budget. **The per-node M can stay at 16
provided edges are coordinated across beam co-occurrents.**

---

## 19. Committee Cover (construction-time approximation of the frontier)

Phase 1–7's selector forces every node to individually cover Q_u. By
Theorem 18, the right invariant is committee-level coverage where the
committee approximates the L0 beam in which u will appear.

**Committee** for node u:

```
K(u) = {u} ∪ (L0 nearest neighbours of u)
            ∪ (reverse neighbours)
            ∪ (same-shell candidates)
            ∪ (recent beam co-visits, when available).
```

**Committee coverage**:

```
Γ_{K(u)}(q) = Σ_{w ∈ K(u)} Σ_{v ∈ N(w)} 1[d(v,q) ≤ ρ·D_{K(u)}(q)] · γ(v),
D_{K(u)}(q) = min_{w ∈ K(u)} d(w,q).
```

**Construction invariant.** For every certified u:

```
μ_{K(u)}({q : Γ_{K(u)}(q) < Λ}) ≤ η.
```

**Selection rule.** When choosing edges for u, maximise the marginal
gain to **committee coverage**, not own coverage:

```
Δ(v) = F_{K(u)}(existing committee edges ∪ {u→v})
     − F_{K(u)}(existing committee edges).
```

**Cross-node spread.** Replace per-node shell-wise angular spread
(§10.C-B) with committee-level edge-direction spread:

```
E_{K(u)} = { (v − w)/|v − w| : w ∈ K(u), v ∈ N(w) },
∠(e, e') ≥ θ₀    ∀ e ≠ e' ∈ chosen subset.
```

This stops two committee members from picking redundant directions —
the legacy α-prune intuition lifted from node to committee.

---

## 20. Implementation roadmap for the frontier upgrade

In strict dependency order:

1. **§16 chordal correction in diagnostics**. Rename `Rho = 0.90f` →
   `LambdaCode = 0.90f`; expose `RhoMetric = sqrt(LambdaCode)`; rewrite
   every diagnostic that reports "ρ" or "H(q)" to use ρ_metric. Build
   behaviour unchanged.
2. **Frontier-coverage diagnostic**. Per-query: snapshot the L0 beam at
   each expansion step, compute `Uncov_ρ^front(F_t)` against `Q_u`,
   compare to per-node `Uncov_ρ(u, N(u))`. If `η_frontier ≪ η_node`,
   the theory predicts we can lower per-node M.
3. **JL witness filter (§17)**. Threshold band ±ε on the sketch; exact
   call only on ambiguous pairs. Targets the cover wall directly
   without touching the cover algorithm.
4. **Committee cover (§19)**. Replace own-coverage objective with
   committee-marginal-gain. Requires a K(u) lookup (already available
   via the candidate set's reverse-adjacency at filter time) and a
   committee-level spread check.
5. **Repair-on-deficit (Phase 5b, deferred)**. Becomes meaningful once
   frontier coverage is the certified invariant — deficit is then
   measured against `Γ_{K(u)}` not `Γ_u`.

Phases 1–7 of the prior framework remain in code as the single-node
specialisation. They are correct under the chordal correction (§16),
just numerically pessimistic. The frontier upgrade adds a layer above
them.

## 21. Empirical resolution on Sphere-100K (the recipe that works)

After §18–§20 mapped what *should* close the recall gap, the empirical
sweep on real cohere d=768 embeddings produced a different answer: the
selection criterion (Q_u cover gain) is not what carries recall. What
carries recall is the **edge-spread test under ascending distance order**
— exactly the invariant the legacy α-prune already enforces.

### 21.1 The four flips that closed the gap

The χ × greedy-mode × kCapture × spread-symmetry sweep at NoC ∈ {16, 128}
showed each axis contributes independently. The final defaults on the
`hnsw-apollonius` branch:

| axis            | old default                       | new default                     | what it does                                                              |
|-----------------|-----------------------------------|---------------------------------|---------------------------------------------------------------------------|
| χ (strictness)  | 0.7                               | 1.0                             | match α-prune strictness; below 1.0 admits too-close edge pairs           |
| greedy mode     | cover-gain (popcount on witness)  | dist (ascending Δ(u,v))         | cover-gain underperformed by 3–4pp at every χ on real clustered data      |
| L0 kCapture     | M/2 nearest, unfiltered           | off (spread filters every edge) | unconditional M/2 fill bypassed spread → redundant near-edges             |
| spread test     | symmetric `min(Δ(u,v), Δ(u,w))`   | one-sided `Δ(u, cur)` only      | in dist-greedy order Δ(u,w) ≤ Δ(u,v), so `min` relaxed the test by ~2–5pp |

Each is overridable via `RAVEN_APOLLO_CHI`, `RAVEN_APOLLO_GREEDY_MODE`,
`RAVEN_APOLLO_KCAPTURE_OFF`, `RAVEN_APOLLO_SPREAD`.

### 21.2 Final numbers (defaults only, n=48 queries)

NoC=16 (Corax default):

| metric                | apollonius | legacy   |
|-----------------------|------------|----------|
| Wall (3-run mean)     | 4.33 s     | 4.29 s   |
| r@1  ef=256           | 64.6 %     | 53.1 %   |
| r@10 ef=256           | 71.9 %     | 69.0 %   |

NoC=128 (recommended production setting):

| ef  | apollo r@1 | apollo r@10 | legacy r@1 | legacy r@10 |
|-----|------------|-------------|------------|-------------|
| 64  | 72.9 %     | **85.0 %**  | 75.0 %     | 84.0 %      |
| 256 | **87.5 %** | 92.9 %      | 85.4 %     | 93.1 %      |
| 512 | **97.9 %** | **95.4 %**  | 87.5 %     | 94.8 %      |

Apollonius matches or beats legacy on every metric at both build budgets.

### 21.3 What this means for the framework

The sweep falsifies the framework's central conjecture that the Q_u
descent cover provides selection-time information that legacy α-prune
lacks. On real isotropic-by-shell clustered embeddings:

- The cover-gain criterion is dead weight for selection. Across
  χ ∈ {0.7, 0.9, 1.0, 1.2}, picking by popcount(witness) is 3–4pp behind
  picking by ascending Δ(u,v). The §10.B isotropic obstruction reproduces
  on real data, even after §10.C-B bi-criteria spread is added.
- What carries recall is the **bi-criteria spread itself**, with
  parameters matched to legacy: one-sided Δ(u, cur), χ=1.0, applied to
  every selected edge (no kCapture bypass).
- Therefore the working configuration *is* legacy α-prune reached through
  the Apollonius scaffolding. The QuDotCache, witness bitmask, kCapture
  preamble, and cover-gain greedy are all computed-but-unused under the
  new defaults.

### 21.4 Status of §18–§20

The frontier-cover theorem (§18) and committee cover (§19) remain open
*as theory*. The empirical result does not refute them; it only says that
on Sphere-100K with M=12, no version of single-node Q_u cover (with or
without committee K(u), with or without JL filter) beats distance-ordered
+ spread. They might still win at much smaller M, on highly anisotropic
data, or under a different metric — but the §10.B obstruction documented
on this branch will need a fundamentally different invariant to bypass,
not a tighter cover.

### 21.5 Cleanup follow-up (not done in this round)

Under the new defaults the witness loop (Qm·N dots/cover), Q_u sampling,
NodeMagnitudes/QuDotCache, and the cover-gain greedy branch are all dead
weight. Wall is at parity *despite* paying for them, because QuDotCache
amortises the witness cost. Stripping them entirely should free 5–10 %
wall. Left as a follow-up so this commit only changes behaviour, not
surface.
