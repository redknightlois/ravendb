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
