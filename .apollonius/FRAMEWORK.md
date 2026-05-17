# Query-Space Descent Covers — Framework & Proofs

## 0. Executive consolidation

The math is organized into three layers:

- **Layer A — production construction**: distance-ordered one-sided
  target-spread / α-prune.
- **Layer B — certification**: held-out descent, survival, frontier, and
  tube-capture diagnostics.
- **Layer C — online maintenance**: repair-on-deficit and multi-batch
  temporal connectivity debt.

The important empirical correction is that the original $Q_u$ Apollonius
cover-gain selector did not carry recall on real Sphere-100K embeddings.
The working configuration became distance-ordered greedy with $\chi=1.0$,
kCapture off, and one-sided spread; this is effectively legacy α-prune
reached through the Apollonius scaffolding, while the $Q_u$ cover
machinery remains useful for diagnostics, certification, and possible
repair.

---

## 1. Formal model

Let $(X, d)$ be a metric space. Let $V \subset X$ be indexed vectors,
$L \subseteq V$ the live set, and $T = V \setminus L$ the tombstoned set.
The graph is directed:

$$
G = (V, E).
$$

For query $q$, define $r_k(q)$ = distance from $q$ to its $k$-th nearest
live point.

A node $u$ is **$\tau$-terminal** for $q$ when

$$
d(u, q) \le \tau \, r_k(q).
$$

An edge $u \to v$ is a **$\rho$-descent witness** for $q$ when

$$
d(v, q) \le \rho \, d(u, q), \qquad 0 < \rho < 1.
$$

This is the fundamental primitive. An edge is good not because $v$ is
close to $u$, but because it moves search closer to $q$.

---

## 2. Edge-as-region: Apollonius descent cell

For a directed edge $u \to v$, define its descent cell:

$$
A_\rho(u, v) = \{ q : d(v, q) \le \rho \, d(u, q) \}.
$$

In Euclidean space this is an Apollonius ball. Expanding
$\|q - v\|^2 \le \rho^2 \|q - u\|^2$ and completing the square gives
$A_\rho(u, v) = B(c, R)$ with

$$
c(u, v, \rho) = \frac{v - \rho^2 u}{1 - \rho^2}, \qquad
R(u, v, \rho) = \frac{\rho \, \|u - v\|}{1 - \rho^2}.
$$

So $u \to v$ **certifies descent on a concrete region of query space**.

---

## 3. Deterministic descent theorem

Suppose greedy search visits $u_0, u_1, \dots$ and every nonterminal
$u_t$ has a live neighbor $u_{t+1}$ with
$d(u_{t+1}, q) \le \rho \, d(u_t, q)$. By induction:

$$
d(u_t, q) \le \rho^t \, d(u_0, q).
$$

A $\tau$-terminal node is reached once $\rho^t d(u_0, q) \le \tau r_k(q)$,
so it suffices that

$$
H(q) = \left\lceil \frac{\log\!\big(d(u_0, q) / (\tau r_k(q))\big)}{\log(1/\rho)} \right\rceil_{+}
$$

descent steps. If the graph always offers descent, search reaches the
answer basin **logarithmically**.

---

## 4. Query-space descent cover

For node $u$, neighbor set $S = N^+(u)$, and local query distribution
$\mu_u$, define uncovered mass

$$
\mathrm{Uncov}_\rho(u, S)
= \mu_u\!\left(\{ q : \forall v \in S \cap L,\; d(v, q) > \rho \, d(u, q) \}\right)
= \mu_u\!\left(Q_u \setminus \bigcup_{v \in S \cap L} A_\rho(u, v)\right).
$$

**Node-local invariant:** $\mathrm{Uncov}_\rho(u, N^+(u)) \le \eta_u$.

If this holds conditionally along the adaptive search route,

$$
\Pr[\text{failure before terminal}] \le \mathbb{E}\!\left[\sum_{t < H(q)} \eta_{u_t}\right].
$$

Uniformly with $\eta_{u_t} \le \eta$:

$$
\Pr[\text{failure}] \le \min\!\left\{1,\; H(q)\,\eta\right\}.
$$

The $\min\{1, \cdot\}$ clamp is essential: a raw bound $H\eta > 1$ is
vacuous and cannot be used to compare algorithms.

---

## 5. Tombstone survival math

RavenDB updates and deletes through tombstones, so an edge may remain
structurally present while its target is no longer live. Each candidate
witness $v$ has a hazard $h(v) \in [0, 1]$ bounding the probability that
$v$ becomes unavailable before the next repair epoch. Define survival
weight

$$
\gamma(v) = -\log h(v).
$$

For query $q$, witness set

$$
W_\rho(u, q) = \{ v \in N^+(u) \cap L : d(v, q) \le \rho \, d(u, q) \},
$$

and survival mass

$$
\Gamma(u, q) = \sum_{v \in W_\rho(u, q)} \gamma(v).
$$

If $\Gamma(u, q) \ge \Lambda$, then under product or
conditional-dominance hazard assumptions,

$$
\Pr[\text{all descent witnesses die}] \le e^{-\Lambda}.
$$

Combining uncovered mass and survival risk:

$$
\Pr[\text{failure}]
\le \mathbb{E}\!\left[\sum_{t < H(q)} (\eta_{u_t} + e^{-\Lambda_{u_t}})\right]
\le \min\!\left\{1,\; H(q)\,(\eta + e^{-\Lambda})\right\}.
$$

The implementation has a constant-hazard hook with $h_0 = 0.1$,
$\gamma_0 = -\log h_0$, redundancy depth $K = 2$, and
$\Lambda = K\gamma_0$. Under constant $\gamma$ the survival accumulator
collapses to a $K$-redundant popcount fast path.

---

## 6. Sampled certification

The true distribution $\mu_u$ is unknown. Approximate with sampled query
anchors $q_1, \dots, q_m \sim \mu_u$. For candidate pool $C(u)$, degree
budget $M$, and selected set $S$, define empirical loss

$$
\hat L_u(S) = \frac{1}{m} \sum_{i=1}^{m} \mathbf{1}[\,\Gamma_S(u, q_i) < \Lambda\,].
$$

Uniform-convergence form:

$$
\left|\, L_u(S) - \hat L_u(S) \,\right|
\le \sqrt{\frac{M \log(en / M) + \log(2N / \delta)}{2m}}.
$$

So a certified value is $\eta_u \le \hat L_u(S) + \epsilon_u$.

**Independence caveat.** Construction samples and certification samples
must be held out from each other. The current branch historically used
the same sample for build and certification; the theorem-backed
certification requires a held-out path.

Testable RavenDB bound:

$$
\Pr[\text{failure}] \le \min\!\left\{1,\; \hat H(q)\big(\hat\eta + \epsilon_m + e^{-\hat\Lambda}\big)\right\}.
$$

---

## 7. Greedy objective: valid, but only a surrogate

For sampled query $q_i$, per-candidate contribution

$$
a_{v, i} = \begin{cases} \gamma(v) & d(v, q_i) \le \rho \, d(u, q_i) \\ 0 & \text{otherwise} \end{cases}.
$$

Capped survival coverage:

$$
F(S) = \sum_i \min\!\left(\Lambda,\; \sum_{v \in S} a_{v, i}\right).
$$

This is monotone submodular, so greedy under $|S| \le M$ gives

$$
F(S_{\text{greedy}}) \ge (1 - 1/e) \, F(S^\star).
$$

But $F$ is **not** itself a recall bound. A set can have $F \approx m\Lambda$
while every sample remains just below the survival threshold. The margin
bridge is the correct certification step. Define

$$
F_{\Lambda+\xi}(S) = \sum_i \min(\Lambda + \xi,\; \Gamma_S(u, q_i)).
$$

Then

$$
\hat L_\Lambda(S) \le \frac{m(\Lambda + \xi) - F_{\Lambda+\xi}(S)}{m\,\xi}.
$$

**Rule:** optimize $F$, certify with $\hat L + \epsilon$.

---

## 8. The high-dimensional obstruction

Pure query-cover cannot solve high-dimensional isotropic data with small
$M$. Locally write $q = u + r s$, $v = u + \ell e$ with $\|s\| = \|e\| = 1$.
The descent condition $\|q - v\| \le \rho \|q - u\|$ expands to

$$
r^2 + \ell^2 - 2 r \ell \langle s, e \rangle \le \rho^2 r^2,
$$

hence

$$
\langle s, e \rangle \ge \frac{\ell^2 + (1 - \rho^2) r^2}{2 r \ell} \ge \sqrt{1 - \rho^2}.
$$

So every neighbor covers at best a spherical cap of half-angle
$\theta_\rho = \arccos(\sqrt{1 - \rho^2}) = \arcsin(\rho)$. If query
directions are uniform on $S^{d-1}$ and $C_d(\rho)$ is the normalized
cap mass,

$$
\mathrm{Uncov}_\rho(u, N(u)) \ge 1 - M \, C_d(\rho).
$$

In high dimension, small $M$ cannot make $\eta$ small unless $\rho$ is
close to 1, the data is non-isotropic, or the invariant becomes
frontier/beam-level. This is not a parameter-tuning issue.

---

## 9. Bi-criteria topology: descent cover plus spread

Because descent cover alone does not imply good topology, the corrected
invariant is bi-criteria.

**A. Survival-weighted descent cover.**
$\mu_u(\{q : \Gamma_{N(u)}(u, q) < \Lambda\}) \le \eta$.

**B. Angular / target spread.** Historical symmetric shell form:

$$
d(v, w) \ge \chi \min(d(u, v),\; d(u, w)).
$$

Current production one-sided form:

$$
d(v, w) \ge \chi \, d(u, v_{\text{candidate}})
$$

where $v$ is the candidate currently being tested and $w$ is already
selected.

The one-sided rule matters because distance-greedy order normally has
$d(u, w) \le d(u, v)$. The symmetric form therefore relaxes the
threshold to the already-picked neighbor's smaller radius, admitting
too-close pairs. The journal records that switching to one-sided spread
closed the residual NoC=128 gap and made Apollonius match or beat legacy
on the measured Sphere-100K metrics.

---

## 10. Target-domination theorem for one-sided spread

**The key production theorem.**

Process candidates in nondecreasing distance from $u$. Let $S$ be the
selected set. Accept candidate $x$ only if

$$
\forall w \in S, \quad d(x, w) \ge \alpha \, d(u, x).
$$

Equivalently, reject $x$ if $\exists w \in S : d(w, x) < \alpha \, d(u, x)$.

Therefore every rejected $x$ has a selected substitute $w$ satisfying
$d(w, x) < \alpha \, d(u, x)$, so the selector creates a
**target-domination cover** of the candidate set:

$$
\forall x \in C(u) :\quad x \in S \;\text{or}\; \exists w \in S : d(w, x) < \alpha \, d(u, x).
$$

This is exactly what α-prune was doing implicitly. It certifies that
omitted candidates are not arbitrary losses — they are dominated by
already-selected edges.

With $\chi = 1$ in squared/chordal distance this becomes strict monotone
target descent: $d(w, x) < d(u, x)$. For $\chi < 1$ it is a weaker
domination threshold; for $\chi > 1$ it is stricter and may
over-diversify.

---

## 11. Corrected recall@k: tube capture, not local $B \ge k$

The earlier proof tried to show that once search reaches a terminal
node, beam width $B \ge k$ suffices to recover all top-$k$. That was
invalid. The corrected theorem uses a **query tube**.

Let

$$
B_k(q) = \{ x \in L : d(x, q) \le r_k(q) \}.
$$

For $R \ge \tau$, define the tube and its population

$$
T_R(q) = \{ y \in L : d(y, q) \le R \, r_k(q) \}, \qquad \kappa_R(q) = |T_R(q)|.
$$

**Target-tube contraction.** For every $x \in B_k(q)$, every
$y \in T_R(q) \setminus \{x\}$ has a live neighbor
$z \in N^+(y) \cap T_R(q)$ with $d(z, x) \le \rho \, d(y, x)$.

If search reaches $u \in T_R(q)$, $B \ge \kappa_R(q)$, and expansion
depth is at least

$$
\ell(q) = \max_{x \in B_k(q)} \left\lceil \frac{\log\!\big((\tau + 1) r_k(q) / \Delta_x\big)}{\log(1/\rho)} \right\rceil,
$$

then L0 beam search visits every $x \in B_k(q)$.

The true requirement is $B \ge \kappa_R(q)$, **not** $B \ge k$ — which
explains why dense isotropic data is hard and clustered data is easier.

---

## 12. Frontier-cover theorem

Node-local cover is too strong and pessimistic because HNSW L0 search is
beam-based. Let frontier $F_t = \{u_1, \dots, u_b\}$ and

$$
D_t(q) = \min_{u \in F_t} d(u, q).
$$

A frontier $\rho$-witness exists if

$$
\exists u \in F_t,\; \exists v \in N(u) : d(v, q) \le \rho \, D_t(q).
$$

Frontier uncovered mass

$$
\mathrm{Uncov}_\rho^{\text{front}}(F_t)
= \mu_{F_t}\!\left(\{ q : \forall u \in F_t, \forall v \in N(u),\; d(v, q) > \rho \, D_t(q) \}\right).
$$

If every nonterminal frontier has a live frontier witness, beam search
reaches a $\tau$-terminal frontier in

$$
H(q) = \left\lceil \frac{\log\!\big(D_0(q) / (\tau r_k(q))\big)}{\log(1/\rho)} \right\rceil.
$$

Survival form:

$$
\Gamma_{F_t}(q) = \sum_{u \in F_t} \sum_{v \in N(u)} \mathbf{1}[d(v, q) \le \rho \, D_t(q)] \, \gamma(v),
$$

$$
\Pr[\text{failure}] \le \mathbb{E}\!\left[\sum_{t < H(q)} (\eta_{F_t} + e^{-\Lambda_{F_t}})\right].
$$

The key advantage is angular budget:
$M_{\text{eff}}(F_t) = \sum_{u \in F_t} |N(u)| \approx bM$.

The high-dimensional obstruction relaxes from $M$ to $bM$: the route
from "raise $M$" to "coordinate across the beam."

---

## 13. Committee cover: construction-time frontier approximation

The construction-time approximation to frontier cover is a committee

$$
K(u) = \{u\} \cup \text{L0 neighbors} \cup \text{reverse neighbors}
       \cup \text{same-shell candidates} \cup \text{recent beam co-visits}.
$$

Define $D_{K(u)}(q) = \min_{w \in K(u)} d(w, q)$ and committee survival
mass

$$
\Gamma_{K(u)}(q) = \sum_{w \in K(u)} \sum_{v \in N(w)} \mathbf{1}[d(v, q) \le \rho \, D_{K(u)}(q)] \, \gamma(v).
$$

**Committee invariant:**

$$
\mu_{K(u)}(\{q : \Gamma_{K(u)}(q) < \Lambda\}) \le \eta.
$$

Selection should maximize marginal gain to **committee coverage**, not
own coverage:

$$
\Delta(u \to v) = F_{K(u)}(E_{K(u)} \cup \{u \to v\}) - F_{K(u)}(E_{K(u)}).
$$

This is the mathematically clean form of "the beam collectively covers
query descent."

---

## 14. Multi-batch temporal connectivity debt

For dynamic RavenDB insertion the right invariant is batch-level. Let
batches $B_1, B_2, \dots, B_t$ arrive with graph $G_t = (V_t, E_t)$.
Define exposed targets

$$
E_t(u) = \{\text{new nodes whose insertion path visited } u\} \cup \{\text{same-batch/inflight}\}
         \cup \{\text{reverse-neighbor candidates}\} \cup \{\text{frontier co-visits}\}.
$$

For target $x$, best outgoing progress from $u$

$$
m_t(u, x) = \min_{w \in N_t^+(u)} d(w, x).
$$

**Temporal connectivity debt:**

$$
\psi_t(u, x) = \left[\, \log \frac{\alpha \, d(u, x)}{m_t(u, x)} \,\right]_{+}.
$$

- $\psi_t(u, x) = 0$: $u$ has a selected edge that dominates $x$.
- $\psi_t(u, x) > 0$: $u$ lacks a target-descending substitute toward $x$.

Total weighted debt:

$$
\Phi_t = \sum_u \sum_{x \in E_t(u)} \omega_{u, x} \, \psi_t(u, x).
$$

A repair candidate $u \to y$ has marginal debt reduction

$$
\Delta_t(u, y) = \sum_{x \in E_t(u)} \omega_{u, x} \left( \psi_t(u, x) - \left[\, \log \frac{\alpha d(u, x)}{\min(m_t(u, x), d(y, x))} \,\right]_{+} \right).
$$

Accept or replace iff
$\Delta_t(u, y) - \text{Loss}_t(u, z) - \lambda_{\text{io}}\, c(u, y, z) > 0$.

**Amortized theorem.** If each batch exposes at least a $p$-fraction of
outstanding weighted debt, repairs are $\beta$-effective relative to the
best bounded repair, and fresh debt per batch is at most $A_t$, then

$$
\mathbb{E}[\Phi_{t+1} \mid \Phi_t] \le (1 - p\beta) \Phi_t + A_t,
$$

$$
\mathbb{E}[\Phi_T] \le (1 - p\beta)^T \Phi_0 + \sum_{s < T} (1 - p\beta)^{T - 1 - s} A_s.
$$

If insertions stop, $A_t = 0$ and $\Phi_T \to 0$ geometrically. If fresh
debt is bounded by $A$,

$$
\limsup_T \mathbb{E}[\Phi_T] \le \frac{A}{p\beta}.
$$

HNSW insertion becomes an **online graph-healing process**: each batch
injects connectivity debt; later batches pay it down opportunistically.

---

## 15. Cosine / chordal correction

The code uses cosine dissimilarity $\delta(x, y) = 1 - \langle x, y \rangle$,
but $\delta$ is not a metric. For normalized vectors the true metric is
chordal

$$
D(x, y) = \sqrt{2(1 - \langle x, y \rangle)} = \sqrt{2 \delta(x, y)}.
$$

NN ranking is unchanged, but proof constants change. The code tests
$\delta(v, q) \le \lambda \, \delta(u, q)$; in chordal metric

$$
D(v, q)^2 \le \lambda D(u, q)^2 \quad\Longrightarrow\quad D(v, q) \le \sqrt{\lambda}\, D(u, q).
$$

So $\rho_{\text{metric}} = \sqrt{\lambda_{\text{code}}}$. For
$\lambda_{\text{code}} = 0.90$, $\rho_{\text{metric}} \approx 0.9487$.
Selection unchanged; predicted $H(q)$ doubles relative to incorrectly
using 0.90 directly.

---

## 16. Algebraic shortcut: witness test as a dot product

For unit vectors $\delta(v, q) \le \lambda \delta(u, q)$ rearranges to

$$
\langle v - \lambda u,\; q \rangle \ge 1 - \lambda.
$$

So a witness cell is a halfspace in query-anchor space.

For non-normalized cosine singles, $\delta(v, q) = 1 - \langle v, q \rangle / (\|v\| \|q\|)$,
and the test $\delta(v, q) \le t_k$ becomes

$$
\langle v, q \rangle \ge (1 - t_k) \, \|v\| \, \|q\|.
$$

The implementation replaces repeated `CosineDistance` calls with one raw
dot plus cached magnitudes/cutoffs: the threshold is
$\lambda \delta(u, q_k)$, and the fast path uses a precomputed cutoff so
the pass condition is $\langle v_i, q_k \rangle \ge \text{cutoff}[k] \, \|v_i\|$.

---

## 17. Algebraic shortcut: spread test as a dot product

The one-sided spread rule $\delta(v, w) \ge \chi \, r_{\text{ref}}$ with
$r_{\text{ref}} = \delta(u, v_{\text{candidate}})$ becomes, since
$\delta(v, w) = 1 - \langle v, w \rangle / (\|v\| \|w\|)$,

$$
\langle v, w \rangle > (1 - \chi \, r_{\text{ref}}) \, \|v\| \, \|w\| \;\Longrightarrow\; \text{reject}.
$$

The code uses exactly this magnitude-sharing fast path for
`PassesAngularSpread` — a spread check becomes one raw dot product when
magnitudes are available.

---

## 18. Algebraic shortcut: triangle skip for spread

True metric radii $r_v = d(u, v)$, $r_w = d(u, w)$. Triangle inequality:
$d(v, w) \ge |r_v - r_w|$. So if spread requires
$d(v, w)^2 \ge \chi \, r_{\text{ref}}^2$, it is auto-satisfied when
$(r_{\max} - r_{\min})^2 \ge \chi \, r_{\text{ref}}^2$.

For the symmetric form with $r_{\text{ref}} = r_{\min}$:

$$
r_{\max} \ge (1 + \sqrt\chi) \, r_{\min}
\quad\Longleftrightarrow\quad
\Delta_{\max} \ge (1 + \sqrt\chi)^2 \, \Delta_{\min}.
$$

For one-sided with $r_{\text{ref}} = r_v$, use
$(r_{\max} - r_{\min})^2 \ge \chi r_v^2$. The triangle skip is exact,
not an approximation, preserving the spread guarantee.

---

## 19. Algebraic shortcut: constant hazard = $K$-redundant popcount

If $\gamma(v) \equiv \gamma_0$ and $\Lambda = K\gamma_0$,

$$
F(S) = \gamma_0 \sum_i \min(K, c_i(S)).
$$

For $K = 2$, the marginal gain of candidate $v$ is proportional to the
bits not yet covered twice:

$$
\Delta(v) \propto \mathrm{popcount}(b_v \wedge \neg \mathrm{coveredTwice}).
$$

The bitset fast path is exact **only** under uniform hazard and fixed
$K = 2$. If $\gamma(v)$ becomes variable, the implementation must switch
to a real-valued accumulator.

---

## 20. Algebraic shortcut: lazy spread check is exact

If candidate $x$ fails spread against current selected $S$, then
$\exists w \in S : d(x, w) < \alpha \, d(u, x)$. For any future
$S' \supseteq S$, the same $w$ remains, so $x$ still fails.

**Once a candidate fails spread, it can be permanently rejected.**

This justifies lazy spread checking: pick tentative best, test it
against the current selected set, mark it rejected on failure. The
journal records that this removed eager $O(N^2)$ conflict precomputation
while preserving bit-for-bit recall.

---

## 21. JL / sketch certificate

Using the linear witness form $\langle a_{u, v}, q_i \rangle \ge 1 - \lambda$
with $a_{u, v} = v - \lambda u$, let $P : \mathbb{R}^d \to \mathbb{R}^s$
be a Johnson–Lindenstrauss projection such that for all relevant finite
pairs $|\langle P a_{u, v}, P q_i \rangle - \langle a_{u, v}, q_i \rangle| \le \epsilon$.

Then:

- $\langle Pa, Pq \rangle \ge 1 - \lambda + \epsilon \;\Rightarrow\;$ definite witness
- $\langle Pa, Pq \rangle < 1 - \lambda - \epsilon \;\Rightarrow\;$ definite non-witness
- otherwise fall back to exact comparison.

This is a **safe relaxation**, not a heuristic, provided the projection
error bound is honored.

**Engineering caveat.** The journal found the per-cover JL variant
regressed; to pay off it needs cached projections and a custom
GEMV-style implementation rather than per-cover setup.

---

## 22. I/O-aware certification

Edge cost

$$
c(u, v) = c_{\text{dist}}(v) + \lambda_{\text{page}} c_{\text{page}}(u, v) + \lambda_{\text{haz}} c_{\text{haz}}(v) + \lambda_{\text{deg}} c_{\text{deg}}(v).
$$

For query $q$, cheapest live descent edge

$$
C_\rho(u, q) = \min\{ c(u, v) : v \in N^+(u) \cap L,\; d(v, q) \le \rho \, d(u, q) \}.
$$

If $c(u_t, u_{t+1}) \le \beta \, C_\rho(u_t, q)$,

$$
\mathrm{Cost}(q) \le \beta \sum_{t < H(q)} C_\rho(u_t, q) + H(q) \, c_{\text{queue}}.
$$

**Implementation rule.** Use cost as a strict **tie-breaker**, not as a
primary gain/cost ratio. Using gain/cost as the main objective would
weaken the $(1 - 1/e)$ greedy guarantee, so cost is only secondary on
equal gain.

---

## 23. Certification checklist

For every diagnostic run, compute these quantities.

**A. Metric correctness.** Log both $\lambda_{\text{code}}$ and
$\rho_{\text{metric}} = \sqrt{\lambda_{\text{code}}}$.

**B. Node cover.** $\hat\eta(u) = \frac{1}{m} \, \#\{q_i : \Gamma_{N(u)}(u, q_i) < \Lambda\}$.

**C. Held-out sample error.** $\epsilon_m = \sqrt{(M \log(en/M) + \log(2N/\delta)) / (2m)}$.

**D. Observed descent length.** $\hat H(q)$ = observed nonterminal
descent/frontier-improvement steps.

**E. Certified failure ceiling.**
$\hat p_{\text{fail}} = \min\{1, \hat H(q)(\hat\eta + \epsilon_m + e^{-\hat\Lambda})\}$.

**F. Target domination.** For construction candidate set $C_R(u)$,

$$
\mathrm{DomFail}(u) = \frac{\#\{ x \in C_R(u) : x \notin S(u),\; \forall w \in S(u),\, d(w, x) \ge \alpha \, d(u, x) \}}{|C_R(u)|}.
$$

For production distance-spread this is the most direct internal certificate.

**G. Frontier cover.** For actual beam snapshots $F_t$,
$\hat\eta_{\text{front}}(F_t) = \frac{1}{m} \, \#\{q_i : \Gamma_{F_t}(q_i) < \Lambda\}$.
Compare $\hat\eta_{\text{front}}$ vs $\hat\eta_{\text{node}}$. If
$\hat\eta_{\text{front}} \ll \hat\eta_{\text{node}}$ the beam is
supplying the missing angular budget.

**H. Tube capture.** Estimate $\hat\kappa_R(q) = |\{y \in L : d(y, q) \le R r_k(q)\}|$.
Check whether $B \ge \hat\kappa_R(q)$. If not, recall@k failure may be a
beam-capture failure, not a graph-descent failure.

**I. Temporal connectivity debt.** $\Phi_t = \sum_{u, x} \omega_{u, x} \big[\log(\alpha d(u, x) / m_t(u, x))\big]_{+}$.
Certification target: $\Phi_{t+1} \le (1 - p\beta) \Phi_t + A_t$.

---

## 24. What is proved versus conditional

**Proved under stated assumptions.**

- Apollonius cell geometry.
- Deterministic multiplicative descent path bound.
- Adaptive descent-cover failure bound with clamp.
- Tombstone survival bound $e^{-\Lambda}$.
- Sample-to-true cover bound with held-out samples and union over nodes.
- Submodularity of capped survival coverage.
- Greedy $(1 - 1/e)$ approximation for surrogate $F$.
- Margin bridge from $F_{\Lambda+\xi}$ to empirical loss.
- I/O path-cost bound.
- Corrected tube-capture theorem with $B \ge \kappa_R(q)$.
- Isotropic lower bound $\mathrm{Uncov}_\rho \ge 1 - M C_d(\rho)$.

**Conditional — must be certified per workload.**

- Arbitrary HNSW satisfies the invariant.
- Failure bound is non-vacuous on a workload.
- Local $k$-capture without tube assumptions.
- Frontier/committee construction actually lowers $\eta$.
- Temporal repair debt decays under real insertion traffic.

---

## 25. Final canonical invariant

**Production local invariant — one-sided target domination.** For each
$u$, selected $S = N^+(u)$, relevant target set $C_R(u)$:

$$
\forall x \in C_R(u) :\quad x \in S \;\text{or}\; \exists w \in S : d(w, x) < \alpha \, d(u, x).
$$

**Certification invariant — survival-weighted held-out descent.**

$$
\mu_u(\{q : \Gamma_{N(u)}(u, q) < \Lambda\}) \le \eta.
$$

**Beam invariant — frontier survival descent.**

$$
\mu_{F_t}(\{q : \Gamma_{F_t}(q) < \Lambda\}) \le \eta_{F_t}.
$$

**Dynamic invariant — temporal connectivity debt is bounded or decays.**

$$
\mathbb{E}[\Phi_{t+1} \mid \Phi_t] \le (1 - p\beta) \Phi_t + A_t.
$$

This is the complete certifiable story:

- α-prune builds target-monotone topology;
- Apollonius / survival math certifies query descent and tombstone robustness;
- frontier / tube math certifies beam recall;
- batch-debt math certifies online graph healing.

---

## 26. Empirical scale validation — Sphere-1M Q=1000 and Sphere-10M M=24

The framework's structural invariants (§3 reservoirs, §6 radial inflation,
§7 Hoeffding gate, §11 tube capture, §12 frontier survival, §13 committee,
§14 repair, §15 L0 simulator) are intact in production code. Empirical
results at production scale (2026-05-17):

### 1M Q=1000 grid scan (M × efC)

Build-time recall lift at fixed ef_search is dominated by **M** (number
of out-edges), not by topology refinement (apollonius cover) nor by L0
repair. At the Corax default M=12 efC=16 → 70.6% r@1 ef=512 on Sphere-1M
cohere-768; bumping M to 24 lifts this to 92.1% (+21.5pp) at +29% build
wall.

Apollonius cover is at recall parity with legacy α-prune across the
entire (M, efC) grid. Its observable benefit is **build wall**:
0.86–0.99× legacy depending on efC (the selector advantage erodes as
the candidate pool grows). L0 repair (LCI eviction + Hoeffding gate)
gives no recall lift above the SE=0.86% noise floor at 1M.

### 10M M=24 + apollonius_repair

At the full production scale with the M=24 + apollonius+repair recipe:
- r@1 ef=128: 63.5% (vs legacy default M=12 = 45.5%) — **+18.0pp**
- build wall: 684s (vs legacy default 879s) — **0.78×**

The framework is **not** redundant under the M bump: the M bump captures
the recall ceiling, the apollonius+repair stack captures the build wall
reduction. They compose on different axes.

### What this validates

- §3 reservoirs: working, observable in the swap acceptance trace
- §6 inflation: β=1.10 is the right operating point at 10M
- §7 Hoeffding gate: rejects noise correctly at high Q, accepts
  high-margin swaps at low Q where headroom exists
- §11 tube capture: tube-bounded descent is the binding bottleneck at
  high M — η_node and η_front are both ≈1 at production ρ, so query-
  time tube quality, not graph topology, drives the residual recall gap
- §14 repair: LCI eviction is selector-dependent; works on apollonius
  cover where low-utility tail edges exist; near-neutral on already-
  tight legacy α-prune topology at M ≥ 24

### What this does not change

- Theorem 1: no construction signal guarantees recall. The 10M wins
  are empirical at one workload and must be watched in production.
- The framework is **certifiable**, not **provable**. §24's "what is
  proved versus conditional" inventory still holds.


---

## 27. Strict theorem hierarchy (anti-slop guard rails)

Earlier sections occasionally drift toward "topology improves recall".
This section restates what is *actually* proved versus conditional and
pins each claim to an exact base condition. The safe hierarchy is a
chain of increasingly strong certificates, **not** one big theorem:

```
selector improves a surrogate coverage F
        ↓ only with margin + held-out samples (Theorem A)
selector improves beam-admissible tube-entry probability
        ↓ only with tube-capture assumptions  (Theorem B)
selector improves a certified recall lower bound
        ↓ only empirically + non-destruction (Theorem E, gate 5)
actual recall improves
```

### Notation

- $u$ a node whose outgoing L0 set is being changed.
- $G$ baseline graph, $G'$ candidate replacement graph (one or more
  fixed-$M$ swaps at $u$).
- $P(u)$ candidate pool at $u$, **fixed before drawing the
  certification sample**.
- $S \subseteq P(u)$ selected out-edges with $|S| \le M$.
- $q$ a held-out query from query distribution $\mu$. **Held-out**
  means not used to drive selection at $u$.
- $r_k(q)$ exact $k$-th NN radius for $q$; $B_k(q)$ exact top-$k$ set.
- Search state at the step where $u$ is expanded:
  $V_t$ visited set, $L_t$ result/beam lower bound, $F_t$ frontier.
- $D(\cdot,\cdot)$ chordal distance on normalised vectors; cosine
  dissimilarity is $\delta = 1 - \langle\cdot,\cdot\rangle$ and the two
  are related by $D^2 = 2\delta$.
- Margins $m_L, m_R, m_X, \Delta, \Delta_{\min} > 0$ are **strict**
  audit tolerances, not "positive on paper".

### Strict trace event

A trace event at $u$ is a tuple
$$
e = \bigl(q, u, V_t, L_t, F_t, r_k(q), B_k(q)\bigr).
$$
It is recorded **only when $u$ is actually expanded** by the deployed
search on the baseline graph. A candidate edge $u \to v$ is

- **strictly beam-admissible** for $e$ iff $v \notin V_t$ and
  $d(q, v) \le L_t - m_L$;
- **strictly tube-useful** for $e$ iff either
  $d(q, v) \le R\, r_k(q) - m_R$, or there is a missed truth
  $x \in B_k(q)$ with $d(v, x) \le \rho\, d(u, x) - m_X$.

The event indicator is
$$
g_e(v) = \mathbf{1}[\text{beam-admissible}] \cdot
         \mathbf{1}[\text{tube-useful}].
$$
For a selected set $S$, *trace cover* and its empirical estimate are
$$
C(S) = \Pr_{e \sim \mathcal{D}_u}\bigl[\exists v \in S: g_e(v) = 1\bigr],
\qquad
\hat{C}(S) = \tfrac{1}{m}\sum_{i=1}^m \mathbf{1}\bigl[\exists v \in S: g_{e_i}(v) = 1\bigr].
$$

### Theorem A — Held-out fixed-$M$ trace-cover improvement

**Assumptions** (all required, all auditable):

A1. $P(u)$ fixed *before* the certification events are drawn.
A2. Certification traces *held out* from construction/selection traces.
A3. Distance is a genuine metric (chordal for normalised cosine).
A4. Deployed search, tie-breaking, efSearch, $k$, candidate cap fixed.
A5. Budget fixed: $|S| \le M$ (recall is $M$-sensitive).
A6. Margins $m_L, m_R, m_X > 0$ used at every threshold (no equality).

Then with probability $\ge 1 - \delta$, uniformly over all
$S \subseteq P(u),\ |S| \le M$,
$$
\bigl|C(S) - \hat{C}(S)\bigr| \le
\epsilon_u =
\sqrt{\frac{M \log(e\,|P(u)|/M) + \log(2/\delta)}{2m}}.
$$

**Selector-improvement certificate.** If on held-out traces
$$
\hat{C}(S') - \hat{C}(S_{\text{old}}) > 2\epsilon_u + \Delta,
$$
then $C(S') - C(S_{\text{old}}) > \Delta$ with probability
$\ge 1 - \delta$.

**Scope (strict).** Theorem A proves *only* that the new edge set
increases the held-out probability of beam-admissible tube-useful
*one-step* opportunities at $u$. It does **not** entail beam survival,
frontier descent, or recall@k.

### Theorem B — Certified recall lower-bound improvement

Define the certified success indicator $Z_G(q) = 1$ iff on graph $G$
the search

B1. reaches some $u \in T_R(q)$ (a node inside the tube);
B2. has beam $B = \mathrm{efSearch}$ satisfying $B \ge \kappa_R(q)$
    where $\kappa_R(q) = |\{y \in L : d(y, q) \le R\,r_k(q)\}|$;
B3. has remaining expansion depth $\ell(q)$ sufficient to drain $T_R(q)$;
B4. has the **target-contraction property**: for every
    $x \in B_k(q)$, every non-target $y \in T_R(q)$ has a live
    neighbour $z \in T_R(q)$ with $d(z, x) \le \rho\, d(y, x)$.

Under B1–B4, $Z_G(q) = 1 \Rightarrow B_k(q) \subseteq
\mathrm{Visited}(G, q)$, so
$\Pr[\text{recall success}] \ge \Pr[Z_G(q) = 1]$.

**Non-destruction is mandatory.** Define on a held-out sample
$$
I = \Pr[Z_G(q) = 0 \land Z_{G'}(q) = 1], \qquad
D = \Pr[Z_G(q) = 1 \land Z_{G'}(q) = 0].
$$
The certified recall lower bound at $G'$ improves iff
$$
I - D > 2\epsilon + \Delta.
$$
$I$ and $D$ are measured on **disjoint** held-out samples drawn from
the deployment distribution $\mu$, not from construction traces.

**Theorem A is not Theorem B.** A selector that lifts $\hat{C}$ but
removes load-bearing edges (large $D$) is a regression for B.

### Theorem C — Radial-shell lemma (single-edge local geometry)

**Assumptions** (local and single-edge):

C1. Normalised vectors, chordal metric $D(x,y) = \sqrt{2(1 - \langle x,y\rangle)}$.
C2. Local Euclidean approximation around $u$.
C3. Conditional on $u$ being expanded, the query direction $s$ is
    isotropic; edge direction $e$ is fixed.
C4. **One edge is being evaluated; beam, $M$, and other edges are
    ignored.**

Write $q = u + r s$, $v = u + \ell e$. The descent witness
$D(v,q) \le \rho D(u,q)$ becomes
$$
\langle s, e\rangle \ge \frac{\ell^2 + (1 - \rho^2) r^2}{2 r \ell}.
$$
Let $a = \ell / r$ and
$h(a) = \tfrac{a}{2} + \tfrac{1-\rho^2}{2a}$.
Minimising $h$ gives
$$
a^\star = \sqrt{1 - \rho^2}.
$$
For cosine witness $\delta(v,q) \le \lambda \delta(u,q)$, the chordal
contraction is $\rho^2 = \lambda$, so in cosine-dissimilarity units
$$
\boxed{\ \delta(u, v) \approx (1 - \lambda)\,\delta(u, q).\ }
$$

**Scope (strict).** C is a **local single-edge geometry lemma** about
where one ideal cover edge sits. It says **nothing** about $M$-edge
selection, beam survival, or recall. It functions as the radial
inflation prior used by selectors; promoting it to an engineering rule
requires Theorem A on top.

### Theorem D — Candidate-pool ceiling diagnostic

$$
\mathrm{PoolCover}(u) = \Pr_{e}[\exists v \in P(u): g_e(v) = 1], \qquad
\mathrm{SelectedCover}(u) = \Pr_{e}[\exists v \in S(u): g_e(v) = 1],
$$
$$
H_u = \mathrm{PoolCover}(u) - \mathrm{SelectedCover}(u).
$$

**Strict gate.** A selector-only topology change at $u$ can improve
held-out trace cover only if
$$
\hat{H}_u > 2\epsilon_u + \Delta_{\min}.
$$
If $H_u$ fails the gate, the problem is **not** the selector — it is
one of: candidate exposure too weak, efC too low, $P(u)$ missing
reverse / co-visit edges, beam / tube cap binding, or $M$ genuinely
too small. The gate cleanly separates three problems that the journal
must never conflate:

- **edge-budget problem:** $M$ too small.
- **candidate-exposure problem:** efC / $P(u)$ lacks the useful edge.
- **selector problem:** useful edge exists but is thrown away.

### Theorem E — Non-destructive replacement (repair / swap)

Let $S' = S - \{z\} + \{y\}$. On held-out traces define
$$
G(y) = \hat{C}_{\text{fail}}(S') - \hat{C}_{\text{fail}}(S), \qquad
L(z) = \hat{C}_{\text{success}}(S) - \hat{C}_{\text{success}}(S').
$$
Accept the swap iff
$$
G(y) - L(z) > 2\epsilon + \Delta_{\min}.
$$

**Strict conditions.**

E1. $L(z)$ counts loss on queries that previously succeeded *through
    the specific edge $z$* in the deployed beam, not "any success at $u$".
E2. $G(y)$ is measured on the *post-swap* graph state to capture
    downstream beam shifts — not a single-step lookahead.
E3. Fail / success splits use **disjoint** held-out samples.

This is the gate the §14 repair primitive must clear. The §15.10
simulator's Δη on construction traces is a proxy, never a substitute.

### Theorem F — Submodular greedy on the surrogate (cover only)

$C(\cdot)$ is monotone submodular in $S$, so greedy yields
$$
C(S_{\text{greedy}}) \ge (1 - 1/e)\, C(S^\star).
$$

**Scope (strict).** This bounds the **surrogate**, not recall@k.
Stating F alone when the metric is recall@k is the canonical slop
pattern. The lift to recall requires the chain
$F \xrightarrow{\text{Theorem A}} C \xrightarrow{\text{Theorem B + E}}
\text{certified recall} \xrightarrow{\text{Gate 5}} \text{actual recall}$.

### The five experimental gates (must all pass)

Before any 1M / 10M run can claim a recall lift, all five must clear:

1. **Candidate exposure (Theorem D).**
   $\hat{H}_u > 2\epsilon_u + \Delta_{\min}$ at the $u$-cells of
   interest. If false, work the pool (efC, reverse, co-visit), not the
   selector.

2. **Selector improvement (Theorem A).**
   $\hat{C}(S') - \hat{C}(S_{\text{old}}) > 2\epsilon_u + m_L$
   with $m_L \ge 0.02$ on held-out traces.

3. **Non-destruction (Theorems B / E).**
   $I - D > 2\epsilon + \Delta$ on **disjoint** held-out samples;
   $D$ counts load-bearing edges removed. Numeric default
   $\Delta \ge 0.01$.

4. **Tube feasibility.**
   $\mu\bigl(T_u(q)\bigr) > 0$ and $B \ge \hat{\kappa}_R(q)$ for a
   meaningful fraction of covered events. No beam capacity → no
   topology benefit at $u$.

5. **Recall holdout.**
   End-to-end recall@k on a **separate** holdout (not used in 1–4)
   lifts by $> 2\sigma$ of run-to-run noise at the deployed
   $(M, \text{efC}, \text{efSearch})$.

A change passing 1–4 but failing 5 is a **structural** win, not a
**recall** win — the journal must label it as such.

### Canonical theorem wording

> **Trace-conditioned fixed-budget topology theorem.** At fixed
> out-degree $M$, fixed candidate pool $P(u)$, fixed HNSW search
> parameters, and held-out trace distribution $\mathcal{D}_u$, if a
> replacement selector improves strict beam-admissible tube-useful
> event coverage by more than the uniform-convergence error plus a
> positive margin, **and** its loss on previously certified-successful
> traces is smaller than that gain by the same margin, then the
> selector improves the graph's certified tube-entry lower bound at
> that node. If, additionally, the affected queries satisfy
> $B \ge \kappa_R(q)$, sufficient expansion depth, and target-tube
> contraction, then the certified recall lower bound improves. Actual
> recall improvement remains an empirical consequence, not a theorem,
> unless certificate-completeness is assumed.

### Why the strictness matters here

At Sphere-1M Q=200 the prior `apollonius_repair` recipe showed
+3–4 pp r@1 lifts. At Q=1000 (SE 0.86 %) the same recipe collapsed to
within $\pm 2\sigma$ noise across the whole (M, efC) grid. In hierarchy
terms: gate 2 (Theorem A) cleared at small $m$, gate 5 failed at the
right $m$. The §15.10 Δη lift (+34 pp) cleared a construction-trace
proxy but never cleared gate 3 (Theorem E) on the deployed sample. At
M=24 10M the L0 repair recall lift disappears entirely — gate 1
(Theorem D) is binding: $H_u$ is small because the M-bump exhausted
the headroom in the candidate pool. Pool enrichment (two-hop forward,
trace-conditioned reservoirs) is the only remaining lever.

### What this implies for the next experiment

Do **not** run "try a clever selector and measure recall." Run:

1. Collect held-out beam-failure traces.
2. Measure $\hat{H}_u = \mathrm{PoolCover} - \mathrm{SelectedCover}$
   (gate 1).
3. Measure $\hat{C}(S') - \hat{C}(S_{\text{old}})$ with margin
   $2\epsilon_u + m_L$ (gate 2).
4. Measure $G - L$ with disjoint held-out samples (gate 3).
5. Check $B \ge \hat\kappa_R(q)$ on covered events (gate 4).
6. **Only then** rebuild and measure recall on a separate holdout
   (gate 5).

Steps 1–5 use a few thousand queries and cost minutes. Step 6 is the
only one that costs a 10 M build. The hierarchy exists to make sure we
do not pay for step 6 to discover gate 1 was already failing.

---

## 28. Production version: two-phase trace-conditioned selector (proposal)

Following §27, the next selector experiment is a **two-phase** design
that respects the gates. Phase A is buildable inside the current
hot path; Phase B is a bounded post-build repair pass.

### Phase A — radial selector (no trace target truth required)

Per-node state (cheap, additive to existing reservoirs):

```
TraceRadius[u] : reservoir of d(u, q) for q whose insertion / search
                 expanded u (size 32, Algorithm R)
```

At edge-selection time at $u$ with witness threshold $\lambda$:

```
d_target = (1 - lambda) * median(TraceRadius[u])     // Theorem C
S = protectedNearest(u, p = 1 or 2)                  // anchor for upper-layer
while (|S| < M)
    pick v in P(u) \ S maximizing RadialScore(d(u,v))
    subject to one-sided PassesAngularSpread(v, S)
RadialScore(d) = -|log((d + eps) / (d_target + eps))|
```

Selector mode: `RAVEN_APOLLO_GREEDY_MODE=radial`.

**Expected signature if A is real:**

- edge length histogram shifts outward toward $(1 - \lambda) r_u$;
- $\eta_{\text{node}}$ may worsen slightly (no longer nearest-first);
- $\hat{C}$ on insertion-trace events improves (gate 2);
- $G - L$ on held-out traces improves (gate 3);
- r@k improves at same $(M, \text{efC}, \text{efSearch})$ (gate 5).

### Phase B — target-tube repair pass

After build, on a held-out $Q_{\text{train}}$ (≈ 1000 queries):

```
for q in Q_train:
    run deployed search, record fail/marginal trace events
group events by hot u
for each hot u:
    P(u) = current S(u)
         + reverse neighbours
         + co-visit set from insertion traces
         + trace targets x in B_k(q) for q that reached u
    solve trace-conditioned greedy (Theorem A surrogate)
    propose swaps z -> y, accept only if Theorem E gate passes
        and not in protected set (1-2 nearest, upper-layer edges,
        edges with high success-trace use count)
```

Commit only if $Q_{\text{holdout}}$ recall lift exceeds gate-5
threshold.

### Two-hop gateway term

Edge $u \to v$ has value if **either** $v$ itself enters the tube, or
some $z \in N(v)$ enters the tube. Soft form:
$$
G^{(2)}_e(v) =
\sigma\!\left(\frac{L_t - d(q,v)}{\epsilon_q}\right) \cdot
\max_{z \in \{v\} \cup N(v)}
\sigma\!\left(\frac{\rho^2\, d(u,x) - d(z,x)}{\epsilon_x}\right).
$$
Bound $|N(v)|$ to the best 4–8 neighbours by current cosine to keep
this cheap. The candidate score becomes
$$
\Delta(v) = \Delta F_{\text{trace}}(v)
          + \eta\,\Delta F_{\text{2hop}}(v)
          + \zeta\,\mathrm{radial}(u, v)
          - \gamma\,\mathrm{redundancy}(v, S).
$$
Start $\eta = 0.25$. The two-hop term is the only mechanism that can
prefer a slightly-worse-by-$d(u,v)$ candidate that opens the *right*
neighbourhood — nearest-first and α-prune cannot see it.

### Where this sits relative to current code

Existing selector hot path has two modes (distance-ordered fill and
popcount cover). The proposal adds:

```
RAVEN_APOLLO_GREEDY_MODE = nearest | cover | radial | trace
```

`radial` runs at construction with no extra reservoirs beyond
$\mathrm{TraceRadius}$. `trace` is repair-only (offline pool, larger
pool, runs after build). Both must clear the §27 gates on a 100K /
1M oracle build before any 10M run is funded.
