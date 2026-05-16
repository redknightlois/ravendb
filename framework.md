# Apollonius Repair Framework — Conditional Guarantees

There is no universal theorem that says any fixed-degree HNSW rewiring cannot
regress recall. That theorem is impossible. What we can prove is a package of
conditional guarantees:

> preserve invariants + block the old anti-local cover failure + only accept
> repairs that improve a validated conditional routing objective + bound work
> and churn.

That is enough to make a proactive batch repair algorithm defensible.

---

## 0. Notation

Assume unit-normalized cosine vectors.

- `D(x, y) = 1 − ⟨x, y⟩`
- Chordal metric: `d(x, y) = sqrt(2 D(x, y))`
- η target at metric contraction `ρ = 0.95` corresponds in cosine-dissimilarity
  space to `λ = ρ² = 0.9025`.

A neighbor `v` is a one-hop witness for `u` toward query `q` iff

```
D(v, q) ≤ λ D(u, q).
```

For a neighbor set `S`, define the best one-hop ratio

```
R_S(u, q) = min_{v ∈ S ∪ {u}} D(v, q) / D(u, q).
```

Then `q` is covered at target 0.95 iff `R_S(u, q) ≤ 0.9025`.

Define a clipped deficit loss

```
ℓ_S(u, q) = min(1, [R_S(u, q) − λ]_+ / (1 − λ)) ∈ [0, 1].
```

This loss is zero when the node has a valid 0.95-witness direction, and
positive when it does not.

---

## 1. The impossible theorem: no universal recall no-regression

**Theorem.** Let `A` be any fixed-degree repair algorithm that can change the
baseline dist-greedy neighbor set `S_D(u)` into a different set `S_A(u)`. If
there exists some node `u` such that `S_A(u) ≠ S_D(u)`, then there exists a
dataset/query distribution for which `A` regresses recall relative to `S_D`.

**Proof.** Since the sets differ, there exists an edge `e ∈ S_D(u) \ S_A(u)`.
Construct a query distribution concentrated on queries whose only successful
search path from `u` requires edge `e`. For example, place the true-neighbor
basin behind `e`, and make all other outgoing edges from `u` lead to regions
that do not contain the true nearest neighbor. Then the baseline graph
succeeds, `Recall(S_D) = 1`, while the repaired graph fails,
`Recall(S_A) = 0`. ∎

So the right goal is not "prove recall never regresses." The right goal is:

> prove that accepted repairs improve a validated operational surrogate,
> while preserving safety invariants.

---

## 2. Why pure global cover looked mathematically good but empirically sucked

The original cover proof was valid but about the wrong objective.

The cover objective was approximately

```
F_K(S) = Σ_{q ∈ Q_u} min(K, Σ_{v ∈ S} 1[D(v, q) ≤ λ D(u, q)]).
```

For `K = 1`, this is max-cover. It is monotone submodular, so greedy has the
usual `(1 − 1/e)`-approximation guarantee.

That theorem proves
`F_K(S_greedy) ≥ (1 − 1/e) max_{|S| ≤ M} F_K(S)`. It does **not** prove better
recall, better conditional routing, or preserved local capture.

### Theorem 2.1 — global Apollonius cover favors farther candidates

Let `u, v, q` be unit vectors and let `q` be drawn uniformly from the unit
sphere. Then the probability that `v` covers `q`,
`Pr_q[D(v, q) ≤ λ D(u, q)]`, is an increasing function of `D(u, v)`.

**Proof.** The witness inequality is `D(v, q) ≤ λ D(u, q)`. Substituting
`D(x, y) = 1 − ⟨x, y⟩` and rearranging gives `⟨v − λu, q⟩ ≥ 1 − λ`. Let
`a = v − λu`. Then `v` covers `q` iff `⟨a/‖a‖, q⟩ ≥ (1 − λ)/‖a‖`. The right
side is the spherical cap threshold. Computing

```
‖v − λu‖² = 1 + λ² − 2λ⟨u, v⟩ = (1 − λ)² + 2λ D(u, v),
```

so the cap threshold is

```
t(D) = (1 − λ) / sqrt((1 − λ)² + 2λ D(u, v)),
```

strictly decreasing in `D(u, v)`. Spherical cap measure is decreasing in `t`,
therefore the cover probability is increasing in `D(u, v)`. ∎

This explains the empirical disaster. Pure global cover rewards candidates
with larger Apollonius cells. In high-dimensional clustered embeddings, those
are often farther bridge-like candidates. But HNSW recall needs local
terminal-capture structure, especially at L0.

### Theorem 2.2 — K = 2 cover is not an η objective

η is first-witness: `η_λ(S) = (1/|Q|) Σ_q 1[∃v ∈ S: D(v, q) ≤ λ D(u, q)]`.
That is `K = 1`. The implemented cover mode used `K = 2`,
`F_2(S) = Σ_q min(2, Σ_v b_v(q))`. These objectives are not equivalent.

**Counterexample.** Query bits split into `A = {a_1, …, a_10}`,
`B = {b_1, …, b_9}`. Candidates: `x` covers `A`, `y` covers `A`, `z` covers
`B`. Budget `M = 2`. After selecting `x`, `K = 2` marginal gain of `y` is 10
(second witness for all of `A`); marginal gain of `z` is 9. So `K = 2` greedy
picks `S = {x, y}`, with `F_1(S) = 10`. But `S* = {x, z}` has `F_1(S*) = 19`.
∎

`K = 2` redundancy is a repair/churn objective, not the primary `η_{0.95}`
objective.

---

## 3. The repair objective we can actually prove things about

For a dirty node `u` at layer `ℓ`, let the current neighbor set be
`S = S_ℓ(u)`. Let batch traces give queries that actually visited `u`:
`Q_{u, ℓ} = {q_1, …, q_m}`. These approximate the operational distribution
`P_{u, ℓ} = P(q | u visited at layer ℓ)`. This is the missing object pure
cover did not have.

The local expected repair loss is
`L_{u, ℓ}(S) = E_{q ∼ P_{u, ℓ}}[ℓ_S(u, q)]`. A repair is good if it reduces
`L_{u, ℓ}(S)`. This is conditional routing-deficit repair, not global cover.

---

## 4. Candidate-pool ceiling theorem

Before trying selector/repair changes, measure the best possible η in the
candidate pool `C = C_{u, ℓ}`:

```
η_pool(u, ℓ) = (1/m) Σ_{q ∈ Q_{u, ℓ}} 1[∃v ∈ C: D(v, q) ≤ λ D(u, q)].
η_cur(u, ℓ)  = (1/m) Σ_{q ∈ Q_{u, ℓ}} 1[∃v ∈ S: D(v, q) ≤ λ D(u, q)].
```

**Theorem.** For any repaired neighbor set `S' ⊆ C`, `η(S') ≤ η_pool`. The
maximum possible improvement from selection/repair is at most
`η_pool − η_cur`. ∎

First yes/no gate: **if `η_pool − η_cur` is small, repair cannot help. Then
the problem is candidate generation, not edge selection.**

---

## 5. Feasibility theorem for safe repair

For one-swap repair `S' = S − {s} + {v}` with `s ∈ S \ P_r`, `v ∈ C \ S`,
where `P_r` is a protected prefix of the closest baseline edges, the repair
is admissible only if:

1. **Degree bound**: `|S'| ≤ M`.
2. **Protected skeleton**: `P_r ⊆ S'`.
3. **Spread invariant**: for all `a, b ∈ S'`,
   `D(a, b) ≥ χ min(D(u, a), D(u, b))` (or the exact spread predicate used
   by the implementation).
4. **Distance inflation bound**: `D(u, v) ≤ β_ℓ D(u, s)`. For upper layers,
   `β_ℓ ∈ [1.5, 3]`. For L0, `β_0 ∈ [1.05, 1.25]`.

**Theorem.** Every accepted repair preserves degree, protected local
skeleton, spread feasibility, and bounded radial drift. ∎

This is exactly what pure cover lacked.

---

## 6. Distance bound blocks the anti-local cover pathology

**Theorem.** If every accepted swap satisfies `D(u, v) ≤ β_ℓ D(u, s)`, then
the repair cannot replace an edge `s` with a candidate whose squared distance
from `u` is more than `β_ℓ` times larger. In chordal metric,
`d(u, v) ≤ sqrt(β_ℓ) d(u, s)`. ∎

So the old failure (local edge removed → far global-cover bridge inserted)
is impossible unless the removed edge was already comparably far.

---

## 7. Validation theorem: accepted repairs improve the conditional objective

This is the core theorem.

Split batch traces into train and validation:
`Q_{u, ℓ} = Q_{u, ℓ}^{train} ∪ Q_{u, ℓ}^{val}`.

Let `S` be current and `S'` proposed. Define paired validation differences

```
X_i = ℓ_S(u, q_i) − ℓ_{S'}(u, q_i),   q_i ∈ Q_{u, ℓ}^{val},   X_i ∈ [−1, 1].
```

Let `Δ̂ = (1/m) Σ X_i` and true gain `Δ = E_q[ℓ_S − ℓ_{S'}]`.

Suppose a batch tests at most `B` proposed repairs. Define
`t(m, B, δ) = sqrt(2 log(B/δ) / m)`.

**Accept only if** `Δ̂ > t(m, B, δ) + τ` with hysteresis `τ > 0`.

**Theorem.** Assuming validation queries are independent samples from
`P_{u, ℓ}` (or an equivalent concentration condition), with probability at
least `1 − δ`, every accepted repair has true conditional improvement
`Δ > τ`.

**Proof.** Fix one repair `S'`. Validation variables `X_i ∈ [−1, 1]`
independent. Hoeffding for range 2: `Pr[Δ̂ − Δ ≥ t] ≤ exp(−m t² / 2)`. Set
`t = t(m, B, δ)`; then `exp(−m t² / 2) = δ/B`. Union bound over `B` tested
repairs: `Pr[∃ tested repair with Δ̂ − Δ ≥ t] ≤ δ`. So with probability
`≥ 1 − δ`, every tested repair has `Δ ≥ Δ̂ − t`. If accepted,
`Δ̂ > t + τ`, hence `Δ > τ`. ∎

This is the theorem pure cover did not have. The guarantee is for the
conditional distribution the graph actually experiences,
`P(q | u visited at layer ℓ)`.

---

## 8. Global surrogate monotonicity theorem

Define an upper-layer routing objective

```
Φ(G) = Σ_{ℓ ≥ 1} Σ_{u ∈ V_ℓ} w_{u, ℓ} L_{u, ℓ}(S_ℓ(u)) + Ψ(G),
```

where `w_{u, ℓ} ≥ 0` is empirical visit frequency, `L_{u, ℓ}` is the
conditional deficit loss, and `Ψ(G)` is a structural penalty (distance
inflation, churn, degree overflow, page cost).

If a repair changes only `S_ℓ(u) → S_ℓ'(u)`, assuming
`L_{u, ℓ}(S_ℓ(u)) − L_{u, ℓ}(S_ℓ'(u)) > τ` and `Ψ(G') ≤ Ψ(G)`:

**Theorem.** `Φ(G') < Φ(G)`. Explicitly,
`Φ(G) − Φ(G') > w_{u, ℓ} τ` whenever `w_{u, ℓ} > 0`. ∎

Batch repair is stochastic coordinate descent on an operational upper-layer
routing objective.

---

## 9. No-cycling theorem under hysteresis

**Theorem.** For a fixed dataset and finite candidate pools, if every
accepted repair decreases `Φ` by at least `γ > 0`, then only finitely many
repairs can be accepted. Specifically, `T ≤ Φ(G_0) / γ`. ∎

This is why repair needs hysteresis `τ`. Without it, the system can churn on
noise.

---

## 10. Delete-bypass theorem

Suppose `x` is deleted/tombstoned. For a neighbor `u`, consider replacing
`u → x` with `u → y` where `y` is a neighbor of `x`. Using chordal metric
`d = sqrt(2D)`:

If `d(x, q) ≤ ρ_x d(u, q)` and `d(y, x) ≤ γ d(x, q)`, then by triangle
inequality

```
d(y, q) ≤ d(y, x) + d(x, q) ≤ (1 + γ) d(x, q) ≤ (1 + γ) ρ_x d(u, q).
```

Therefore `y` is a valid `ρ`-witness if `(1 + γ) ρ_x ≤ ρ`, i.e.
`γ ≤ ρ / ρ_x − 1`.

In squared cosine-dissimilarity form: a bypass is safe when

```
sqrt(D(y, x)) + sqrt(D(x, q)) ≤ ρ sqrt(D(u, q)).
```

A concrete prefilter for delete repair.

---

## 11. Upper-layer work bound

Level generator: `L = ⌊−log r / log M⌋`, `r ∼ U(0, 1)`. Then
`Pr[L ≥ ℓ] = M^{-ℓ}` and `E|V_ℓ| = N M^{-ℓ}`. Total expected upper-layer
node appearances:

```
Σ_{ℓ ≥ 1} E|V_ℓ| = N / (M − 1).
```

For `M = 32`, `1/(M − 1) ≈ 3.2%`. Proactive upper-layer repair is naturally
bounded.

This justifies starting repair at `ℓ ≥ 1`, not L0.

---

## 12. Promotion budget theorem

If proactive promotion is added, enforce
`|V_ℓ| ≤ (1 + α) N / M^ℓ`. Then total upper-layer node appearances are
bounded by `(1 + α) N / (M − 1)`. Promotion does not destroy HNSW's sparse
upper-layer complexity as long as the quota is enforced.

---

## 13. Margin robustness theorem

Suppose the computed ratio has additive error at most `ε`:
`|R̂_v(u, q) − R_v(u, q)| ≤ ε`. If a candidate has witness margin
`λ − R_v(u, q) ≥ ε`, then it remains a witness under approximation. Margin
is a robustness criterion — but it should be secondary. First optimize
first-witness coverage/deficit at `λ = 0.9025`; use margin only to choose
among repairs with similar first-witness improvement.

---

## 14. What the theorem package guarantees

It guarantees:

> Every accepted repair improves a validated conditional upper-layer routing
> objective, subject to the validation sampling assumption.

And: **degree, spread, protected skeleton, and radial-drift bounds are
preserved.**

It prevents the specific old cover failure because:

- pure global cover's anti-local far-edge bias is blocked by distance bounds;
- local capture is protected by `P_r`;
- sample overfitting is controlled by train/validation split;
- repairs are bounded one/two-swap moves, not full max-cover rebuilds;
- the objective uses batch-conditioned traces, not global random `Q_u`.

It does **not** guarantee universal recall no-regression. That is impossible.

---

## 15. Ordered list of improvements with yes/no gates

Use this order. Do not jump to later steps until the earlier gate passes.

### 1. Add upper-layer telemetry only

No graph mutation. For every dirty or visited `(u, ℓ)` with `ℓ ≥ 1`, log
`η_cur(u, ℓ)`, `η_pool(u, ℓ)`, `g(u, ℓ) = η_pool − η_cur`. Aggregate over
batch traces.

- **YES**: `ḡ_{ℓ ≥ 1} > t(n, 1, δ) + g_min`, suggested `δ = 0.01`,
  `g_min = 0.005`–`0.01`. Proceed to repair.
- **NO**: selector repair cannot move η. Do not repair yet; go to
  candidate-pool enrichment.

### 2. Collect conditional trace reservoirs

For each dirty `(u, ℓ)`, store `Q_{u, ℓ}^{train}`, `Q_{u, ℓ}^{val}`, and

```
C_{u, ℓ} = S_ℓ(u) ∪ S_ℓ(S_ℓ(u)) ∪ C_batch ∪ C_reverse ∪ C_delete-bypass.
```

Cap `|C_{u, ℓ}| ≤ 4M` at first.

- **YES**: validation size enough that
  `t(m, B, δ) = sqrt(2 log(B/δ)/m) ≤ t_max`, suggested `t_max = 0.10`.
- **NO**: aggregate by bucket `(ℓ, dirty reason, radius shell)` or collect
  more batches.

### 3. Upper-layer one-swap repair

Only `ℓ ≥ 1`. Parameters: `λ = 0.9025`, `r = 2` protected, `β_ℓ = 2.0`,
`maxSwapsPerNode = 1`. Repair `S' = S − {s} + {v}`. Reject if `s ∈ P_r`,
spread fails, or `D(u, v) > β_ℓ D(u, s)`. Accept only if
`Δ̂ > t(m, B, δ) + τ`, suggested `τ = 0.005`.

- **YES**: held-out aggregate `Δ̂_upper > t(n, 1, δ) + τ` and upper-layer
  `η_{0.95}` improves.
- **NO**: reject if validation gain does not clear threshold or accepted
  repairs concentrate in far-radius swaps despite `β`.

### 4. Delete-bypass repair

For deleted/tombstone/empty-posting `x`, dirty its upper-layer neighbors.
Candidate bypasses `y ∈ S_ℓ(x)`. Prefilter
`sqrt(D(y, x)) + sqrt(D(x, q)) ≤ 0.95 sqrt(D(u, q))` on a meaningful
fraction of validation traces, or use the same deficit validation gate.

- **YES**: `Δ̂_val > t(m, B, δ) + τ` and reduces dirty/tombstone traversal.
- **NO**: rejects if it merely removes tombstones without improving
  conditional descent (tombstoned vectors can still be useful Steiner
  points).

### 5. Candidate-pool enrichment

When Step 1 says the pool ceiling is too low. Add candidates from `S_ℓ(S_ℓ(u))`, reverse neighbors, batch-promoted nodes, delete-bypass
neighborhoods, recent insertion beams. Measure enriched ceiling `η_pool^+`.

- **YES**: `η_pool^+ − η_pool > t(n, 1, δ) + 0.005` within CPU/I/O budget.
- **NO**: no selector can recover witnesses that are not in the pool.

### 6. Two-swap repair

Only after one-swap succeeds. `S' = S − {s_1, s_2} + {v_1, v_2}`. Hypothesis
class grows, validation must be stricter.

- **YES**: one-swap leaves residual gap
  `η_pool − η_repaired > t(n, 1, δ) + 0.005`. Accept individual two-swaps
  only if `Δ̂ > t(m, B, δ) + τ`.
- **NO**: one-swap already consumes most of the pool gap, or two-swap
  validation cannot clear the larger statistical threshold.

### 7. Margin / log-ratio scoring

Do not use margin as the primary objective. Tie-breaker only. Define

```
Δ̂_log = (1/m) Σ_q [log R_S(u, q) − log R_{S'}(u, q)].
```

- **YES**: binary `η_{0.95}` does not regress and
  `Δ̂_log > t(n, 1, δ) + τ_log`.
- **NO**: margin improves while first-witness `η_{0.95}` drops — repeats
  the `K = 2` mistake.

### 8. Upper-layer promotion repair

Only after edge repair shows the pool lacks candidates. Promote `y` to level
`ℓ` only if `|V_ℓ| < (1 + α) N / M^ℓ`. Promotion utility
`U_ℓ(y) = Σ_{u, q} [ℓ_{S_ℓ(u)}(u, q) − ℓ_{S_ℓ(u) ∪ {y}}(u, q)] − μ_ℓ`.

- **YES**: `U_ℓ(y) > t(m, B, δ) + μ_ℓ` and quota valid.
- **NO**: quota full or utility does not clear validation threshold.

### 9. Distance-window build tie-break

Safer build-time perturbation than cover-gain. Let `d*` be the nearest
admissible candidate distance. Define `W_ε = {v: D(u, v) ≤ (1 + ε) d*}`.
Choose by 0.95-deficit or margin only inside `W_ε`. Suggested
`ε ∈ {0.01, 0.03, 0.05}`.

- **YES**: changed-edge fraction `> 1%` and
  `Δ̂_val > t(n, 1, δ) + τ`.
- **NO**: too few edges changed to matter, or it repeats cover-gain's
  recall regression.

### 10. L0 repair

Do this last. L0 is terminal recall territory. Strict constraints: `r = M/2`
protected, `β_0 ≤ 1.10` or `1.25`, `maxSwapsPerNode = 1`.

- **YES**: `Δ̂_{L0, val} > t(n, 1, δ) + τ`, `η_{0.95}` improves, and
  recall@k on held-out workload does not regress beyond tolerance.
- **NO**: any recall regression. L0 repair is high-risk.

---

## 16. Explicit no-go item

> Do not retry full primary cover-gain selection as the main build selector.

By Theorem 2.1, it is biased toward farther candidates under global `Q_u`.
The safe version is **dist-greedy skeleton + bounded validated repair**.

---

## 17. Minimal deployment path

1. **Telemetry-only upper-layer pool ceiling.** Decide whether repair has
   headroom.
2. **Trace reservoir collection.** Build `Q_train`, `Q_val`, and `C` for
   dirty upper-layer nodes.
3. **Upper-layer one-swap repair.** Protected `r = 2`, `β = 2`, max one
   swap.
4. **Delete-bypass repair.** Only for dirty upper-layer neighborhoods
   affected by deletes.
5. **Candidate-pool enrichment.** Only if the pool ceiling is too low.
6. **Two-swap repair.** Only if one-swap works but leaves a measurable
   residual gap.
7. **Promotion.** Only if existing upper-layer nodes cannot provide the
   needed witnesses.
8. **L0 repair.** Only with strict protection and held-out recall gating.

The core acceptance rule is always

```
Δ̂_val > sqrt(2 log(B/δ) / m) + τ.
```

If it does not clear that threshold, the answer is **no**.
