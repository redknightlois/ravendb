using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Compression;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;
using Constants = Voron.Global.Constants;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    public record NodeForDebug(
        long NodeId,
        long[] Entries,
        (long NodeId, float Distance)[][] EdgesByLevel
    );
    
    public static IEnumerable<NodeForDebug> IterateNodes(LowLevelTransaction llt, string name)
    {
        var searchState = new SearchState(llt, name);
        for (long nodeId = 1; nodeId <= searchState.Options.CountOfVectors; nodeId++)
        {
            var node = searchState.GetNodeById(nodeId);
            int nodeIndex = searchState.GetNodeIndexById(nodeId);
            long[] entries = GetEntries(llt, node.PostingListId);
            var edgesByLevel = new (long NodeId, float Distance)[node.EdgesPerLevel.Count][];
            for (int i = 0; i < node.EdgesPerLevel.Count; i++)
            {
                edgesByLevel[i] = new (long NodeId, float Distance)[node.EdgesPerLevel[i].Count];
                for (int j = 0; j <  node.EdgesPerLevel[i].Count; j++)
                {
                    long id = node.EdgesPerLevel[i][j];
                    int index = searchState.GetNodeIndexById(id);
                    edgesByLevel[i][j] = (id, searchState.Distance(ReadOnlySpan<byte>.Empty, nodeIndex, index));
                }
            }
            yield return new NodeForDebug(nodeId, entries, edgesByLevel);
        }
    }

    public static long[] GetEntries(LowLevelTransaction llt,long postingListId)
    {
        ContainerEntryId rawPostingListId = new ContainerEntryId(postingListId & Constants.Graphs.VectorId.ContainerType);
        long[] result;
        switch (postingListId & Constants.Graphs.VectorId.EnsureIsSingleMask)
        {
            case Constants.Graphs.VectorId.Tombstone:
                result= [];
                break;
            case Constants.Graphs.VectorId.Single:
                result = [(long)rawPostingListId];
                break;
            case Constants.Graphs.VectorId.SmallPostingList:
            {
                var list = new ContextBoundNativeList<long>(llt.Allocator);
                FastPForDecoder decoder = new();
                SearchState.ReadPostingList(llt, rawPostingListId, ref list, ref decoder, out var size);
                result = list.ToSpan().ToArray();
                list.Dispose();
                break;
            }
            case Constants.Graphs.VectorId.PostingList:
            {
                var setStateSpan = Container.GetReadOnly(llt, rawPostingListId);
                ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                var postingList = new PostingList(llt, Slices.Empty, setState);
                result = new long[(int)Math.Min(postingList.State.NumberOfEntries, 16)];
                var it = postingList.Iterate();
                it.Fill(result, out _);
                break;
            }
            default:
                throw new NotSupportedException($"Got unknown {nameof(postingListId)} type: {postingListId}");
        }
        Registration.InternalEntryIdToEntryId(result);
        return result;
    }
    
    private static long GetEntryId(LowLevelTransaction llt,long postingListId)
    {
        switch (postingListId & Constants.Graphs.VectorId.EnsureIsSingleMask)
        {
            case Constants.Graphs.VectorId.Tombstone:
                return 0;
            case Constants.Graphs.VectorId.Single:
                return postingListId & Constants.Graphs.VectorId.ContainerType;
            case 0b10:
                return postingListId & Constants.Graphs.VectorId.ContainerType;
            case 0b11:
                return postingListId & Constants.Graphs.VectorId.ContainerType;
        }

        throw new NotSupportedException($"Got unknown {nameof(postingListId)} type: {postingListId}");
    }

    /// <summary>
    /// Per-query result of greedy descent-cover measurement.
    /// </summary>
    public readonly record struct DescentCoverPerQuery(
        int PathLength,
        int VisitedNodes,
        int CoveredNodes,
        int TotalWitnesses);

    /// <summary>
    /// Aggregate report from <see cref="MeasureDescentCover"/>.
    /// </summary>
    /// <param name="Rho">The descent factor used.</param>
    /// <param name="QueriesSampled">Number of queries measured.</param>
    /// <param name="MeanPathLength">Mean H(q) — number of moves made by greedy descent before termination.</param>
    /// <param name="MeanVisitedNonterminal">Mean number of nodes visited per query during descent (where the witness invariant is checked).</param>
    /// <param name="FractionUncovered">η̂ — fraction of (visited_node, query) pairs with zero ρ-descent witnesses.</param>
    /// <param name="MeanWitnessesWhenCovered">Mean count of ρ-descent witnesses at nodes that were covered.</param>
    public readonly record struct DescentCoverReport(
        float Rho,
        int QueriesSampled,
        double MeanPathLength,
        double MeanVisitedNonterminal,
        double FractionUncovered,
        double MeanWitnessesWhenCovered)
    {
        public override string ToString() =>
            $"DescentCoverReport(ρ={Rho:F2}, queries={QueriesSampled}, " +
            $"meanH={MeanPathLength:F2}, meanVisited={MeanVisitedNonterminal:F2}, " +
            $"η̂={FractionUncovered:F4}, meanWitnesses={MeanWitnessesWhenCovered:F2})";
    }

    /// <summary>
    /// Diagnostic: walks the graph as a pure greedy ρ-descent for each query and reports the
    /// empirical descent-cover statistics (η̂, mean witness count, mean path length H).
    /// This is the baseline measurement for the Apollonius descent-cover invariant
    /// `Pr[failure] ≤ H(q)·(η + e^-Λ)`. Read-only, single-threaded, no production callers.
    /// </summary>
    /// <param name="queriesBlob">Concatenated query vectors (each of size <c>Options.VectorSizeBytes</c>).</param>
    /// <param name="queryCount">Number of queries packed into <paramref name="queriesBlob"/>.</param>
    /// <param name="rho">Descent factor in (0, 1). An edge u→v counts as a ρ-descent witness for q iff d(v,q) ≤ ρ·d(u,q).</param>
    public static DescentCoverReport MeasureDescentCover(
        LowLevelTransaction llt,
        Slice name,
        ReadOnlySpan<byte> queriesBlob,
        int queryCount,
        float rho)
    {
        if (rho <= 0f || rho >= 1f)
            throw new ArgumentOutOfRangeException(nameof(rho), rho, "rho must be in (0, 1)");

        var searchState = new SearchState(llt, name);
        if (searchState.IsEmpty)
            return new DescentCoverReport(rho, 0, 0, 0, 0, 0);

        int vectorSizeBytes = searchState.Options.VectorSizeBytes;
        if (queriesBlob.Length != queryCount * vectorSizeBytes)
            throw new ArgumentException(
                $"queriesBlob length {queriesBlob.Length} does not match queryCount {queryCount} * vectorSizeBytes {vectorSizeBytes}");

        long totalPath = 0;
        long totalVisited = 0;
        long totalUncovered = 0;
        long totalWitnessesOnCovered = 0;
        long totalCovered = 0;

        for (int q = 0; q < queryCount; q++)
        {
            var query = queriesBlob.Slice(q * vectorSizeBytes, vectorSizeBytes);
            int pathLength = 0;
            int visited = 0;
            int covered = 0;
            int witnessSumOnCovered = 0;

            int currentIdx = searchState.GetNodeIndexById(EntryPointId);
            float currentDist = searchState.Distance(query, -1, currentIdx);

            for (int level = searchState.Options.MaxLevel; level >= 0; level--)
            {
                while (true)
                {
                    ref var node = ref searchState.GetNodeByIndex(currentIdx);
                    if (node.EdgesPerLevel.Count <= level)
                        break;

                    ref var edges = ref node.EdgesPerLevel[level];

                    int witnesses = 0;
                    int bestEdgeIdx = -1;
                    float bestDist = currentDist;
                    float witnessCeiling = rho * currentDist;

                    for (int i = 0; i < edges.Count; i++)
                    {
                        int neighborIdx = searchState.GetNodeIndexById(edges[i]);
                        float d = searchState.Distance(query, -1, neighborIdx);
                        if (d <= witnessCeiling)
                            witnesses++;
                        if (d < bestDist)
                        {
                            bestDist = d;
                            bestEdgeIdx = neighborIdx;
                        }
                    }

                    visited++;
                    if (witnesses > 0)
                    {
                        covered++;
                        witnessSumOnCovered += witnesses;
                    }

                    if (bestEdgeIdx == -1)
                        break; // no improvement at this level — descend

                    currentIdx = bestEdgeIdx;
                    currentDist = bestDist;
                    pathLength++;
                }
            }

            totalPath += pathLength;
            totalVisited += visited;
            totalCovered += covered;
            totalUncovered += visited - covered;
            totalWitnessesOnCovered += witnessSumOnCovered;
        }

        double meanH = (double)totalPath / queryCount;
        double meanVisited = (double)totalVisited / queryCount;
        double frac = totalVisited == 0 ? 0.0 : (double)totalUncovered / totalVisited;
        double meanWit = totalCovered == 0 ? 0.0 : (double)totalWitnessesOnCovered / totalCovered;

        return new DescentCoverReport(rho, queryCount, meanH, meanVisited, frac, meanWit);
    }

    public static DescentCoverReport MeasureDescentCover(LowLevelTransaction llt, string name, ReadOnlySpan<byte> queriesBlob, int queryCount, float rho)
    {
        using (Slice.From(llt.Allocator, name, out var slice))
            return MeasureDescentCover(llt, slice, queriesBlob, queryCount, rho);
    }

    /// <summary>
    /// FRAMEWORK §12 / §23.G frontier-cover diagnostic.
    /// Runs an L0 beam search of width <paramref name="beam"/> for each query; at each
    /// non-terminal step captures the current frontier F_t and computes the survival
    /// count Γ_{F_t}(q) = #{(u,v) : u ∈ F_t, v ∈ N(u), d(v,q) ≤ ρ·D_t(q)} where
    /// D_t(q) = min_{u ∈ F_t} d(u, q). Aggregates η_front = fraction of (step, query)
    /// pairs with Γ_{F_t} below the K-redundant threshold <paramref name="redundancy"/>.
    /// Compare to the node-level η̂ from <see cref="MeasureDescentCover"/>: a large
    /// gap (η_front ≪ η_node) is what justifies §13 committee / §14 repair work.
    /// </summary>
    public readonly record struct FrontierCoverReport(
        float Rho,
        int Beam,
        int Redundancy,
        int QueriesSampled,
        double MeanStepsPerQuery,
        double FractionStepsUncovered,
        double MeanGammaWhenCovered)
    {
        public override string ToString() =>
            $"FrontierCoverReport(ρ={Rho:F2}, b={Beam}, K={Redundancy}, queries={QueriesSampled}, " +
            $"meanSteps={MeanStepsPerQuery:F2}, η_front={FractionStepsUncovered:F4}, meanΓ={MeanGammaWhenCovered:F2})";
    }

    public static FrontierCoverReport MeasureFrontierDescentCover(
        LowLevelTransaction llt,
        Slice name,
        ReadOnlySpan<byte> queriesBlob,
        int queryCount,
        float rho,
        int beam,
        int redundancy = 2,
        int maxSteps = 256)
    {
        if (rho <= 0f || rho >= 1f)
            throw new ArgumentOutOfRangeException(nameof(rho));
        if (beam <= 0)
            throw new ArgumentOutOfRangeException(nameof(beam));

        var searchState = new SearchState(llt, name);
        if (searchState.IsEmpty)
            return new FrontierCoverReport(rho, beam, redundancy, 0, 0, 0, 0);

        int vectorSizeBytes = searchState.Options.VectorSizeBytes;
        if (queriesBlob.Length != queryCount * vectorSizeBytes)
            throw new ArgumentException($"queriesBlob length {queriesBlob.Length} != {queryCount}·{vectorSizeBytes}");

        long totalSteps = 0;
        long totalUncoveredSteps = 0;
        long totalGammaOnCovered = 0;
        long totalCoveredSteps = 0;

        // Reusable buffers (per query).
        var beamIdx = new int[beam];
        var beamDist = new float[beam];
        var visited = new HashSet<int>();
        var expanded = new HashSet<int>();

        for (int q = 0; q < queryCount; q++)
        {
            var query = queriesBlob.Slice(q * vectorSizeBytes, vectorSizeBytes);
            visited.Clear();
            expanded.Clear();

            int entry = searchState.GetNodeIndexById(EntryPointId);

            // Greedy descent down upper levels to set the L0 entry.
            int curIdx = entry;
            float curDist = searchState.Distance(query, -1, curIdx);
            for (int level = searchState.Options.MaxLevel; level > 0; level--)
            {
                while (true)
                {
                    ref var node = ref searchState.GetNodeByIndex(curIdx);
                    if (node.EdgesPerLevel.Count <= level) break;
                    ref var edges = ref node.EdgesPerLevel[level];
                    int bestIdx = -1;
                    float bestDist = curDist;
                    for (int i = 0; i < edges.Count; i++)
                    {
                        int nb = searchState.GetNodeIndexById(edges[i]);
                        float d = searchState.Distance(query, -1, nb);
                        if (d < bestDist) { bestDist = d; bestIdx = nb; }
                    }
                    if (bestIdx == -1) break;
                    curIdx = bestIdx;
                    curDist = bestDist;
                }
            }

            // L0 beam-search loop. Beam = top-b nearest seen-so-far (sorted ascending).
            int beamCount = 1;
            beamIdx[0] = curIdx;
            beamDist[0] = curDist;
            visited.Add(curIdx);

            for (int step = 0; step < maxSteps; step++)
            {
                // Pick smallest-dist unexpanded beam member.
                int pickPos = -1;
                for (int i = 0; i < beamCount; i++)
                {
                    if (expanded.Contains(beamIdx[i])) continue;
                    if (pickPos == -1 || beamDist[i] < beamDist[pickPos]) pickPos = i;
                }
                if (pickPos == -1) break;
                int u = beamIdx[pickPos];
                expanded.Add(u);

                // Expand u's L0 neighbours into the beam.
                ref var uNode = ref searchState.GetNodeByIndex(u);
                if (uNode.EdgesPerLevel.Count == 0) continue;
                ref var uEdges = ref uNode.EdgesPerLevel[0];
                for (int i = 0; i < uEdges.Count; i++)
                {
                    int v = searchState.GetNodeIndexById(uEdges[i]);
                    if (visited.Contains(v)) continue;
                    visited.Add(v);
                    float dv = searchState.Distance(query, -1, v);
                    if (beamCount < beam)
                    {
                        beamIdx[beamCount] = v;
                        beamDist[beamCount] = dv;
                        beamCount++;
                    }
                    else
                    {
                        // Replace worst if v is closer.
                        int worst = 0;
                        for (int j = 1; j < beamCount; j++)
                            if (beamDist[j] > beamDist[worst]) worst = j;
                        if (dv < beamDist[worst])
                        {
                            beamIdx[worst] = v;
                            beamDist[worst] = dv;
                        }
                    }
                }

                // Snapshot F_t: current beam. D_t = min beam dist. Γ_t = count of
                // (member, neighbour) pairs with d(neighbour, q) ≤ ρ · D_t.
                float dT = float.MaxValue;
                for (int i = 0; i < beamCount; i++)
                    if (beamDist[i] < dT) dT = beamDist[i];
                float witnessCeiling = rho * dT;
                int gamma = 0;
                for (int i = 0; i < beamCount; i++)
                {
                    ref var fNode = ref searchState.GetNodeByIndex(beamIdx[i]);
                    if (fNode.EdgesPerLevel.Count == 0) continue;
                    ref var fEdges = ref fNode.EdgesPerLevel[0];
                    for (int j = 0; j < fEdges.Count; j++)
                    {
                        int w = searchState.GetNodeIndexById(fEdges[j]);
                        float dw = searchState.Distance(query, -1, w);
                        if (dw <= witnessCeiling) gamma++;
                    }
                }

                totalSteps++;
                if (gamma < redundancy)
                {
                    totalUncoveredSteps++;
                }
                else
                {
                    totalCoveredSteps++;
                    totalGammaOnCovered += gamma;
                }
            }
        }

        double meanSteps = (double)totalSteps / Math.Max(queryCount, 1);
        double frac = totalSteps == 0 ? 0.0 : (double)totalUncoveredSteps / totalSteps;
        double meanG = totalCoveredSteps == 0 ? 0.0 : (double)totalGammaOnCovered / totalCoveredSteps;
        return new FrontierCoverReport(rho, beam, redundancy, queryCount, meanSteps, frac, meanG);
    }

    public static FrontierCoverReport MeasureFrontierDescentCover(LowLevelTransaction llt, string name, ReadOnlySpan<byte> queriesBlob, int queryCount, float rho, int beam, int redundancy = 2, int maxSteps = 256)
    {
        using (Slice.From(llt.Allocator, name, out var slice))
            return MeasureFrontierDescentCover(llt, slice, queriesBlob, queryCount, rho, beam, redundancy, maxSteps);
    }

    /// <summary>
    /// FRAMEWORK §15.1 / §4 candidate-pool ceiling diagnostic. Walks the greedy descent
    /// path and at each visited (u, ℓ) measures both:
    ///   η_cur(u, ℓ) : did any v ∈ N_ℓ(u) satisfy d(v, q) ≤ ρ · d(u, q)?
    ///   η_pool(u, ℓ): did any v ∈ N_ℓ(u) ∪ N_ℓ(N_ℓ(u)) satisfy the same?
    /// The gap g = η_pool − η_cur is the maximum coverage gain available from a one-swap
    /// repair drawing candidates from the 2-hop pool. By Theorem §4 a small gap means
    /// selector repair cannot help — the problem is candidate-pool generation, not edge
    /// selection. Bucketed by L0 vs upper-layers (ℓ ≥ 1) because §11 shows upper-layer
    /// repair is naturally bounded to ~N/(M−1) appearances and the framework §15 path
    /// repairs ℓ ≥ 1 first.
    /// </summary>
    public readonly record struct PoolCeilingReport(
        float Rho,
        int QueriesSampled,
        long L0Visits,
        long L0CoveredCur,
        long L0CoveredPool,
        long UpperVisits,
        long UpperCoveredCur,
        long UpperCoveredPool)
    {
        public double EtaCurL0    => L0Visits == 0 ? 0.0 : (double)L0CoveredCur  / L0Visits;
        public double EtaPoolL0   => L0Visits == 0 ? 0.0 : (double)L0CoveredPool / L0Visits;
        public double GapL0       => EtaPoolL0 - EtaCurL0;
        public double EtaCurUp    => UpperVisits == 0 ? 0.0 : (double)UpperCoveredCur  / UpperVisits;
        public double EtaPoolUp   => UpperVisits == 0 ? 0.0 : (double)UpperCoveredPool / UpperVisits;
        public double GapUpper    => EtaPoolUp - EtaCurUp;

        public override string ToString() =>
            $"PoolCeilingReport(ρ={Rho:F2}, Q={QueriesSampled}, " +
            $"L0[visits={L0Visits} ηcur={EtaCurL0:F4} ηpool={EtaPoolL0:F4} g={GapL0:F4}], " +
            $"Up[visits={UpperVisits} ηcur={EtaCurUp:F4} ηpool={EtaPoolUp:F4} g={GapUpper:F4}])";
    }

    public static PoolCeilingReport MeasurePoolCeiling(
        LowLevelTransaction llt,
        Slice name,
        ReadOnlySpan<byte> queriesBlob,
        int queryCount,
        float rho)
    {
        if (rho <= 0f || rho >= 1f)
            throw new ArgumentOutOfRangeException(nameof(rho), rho, "rho must be in (0, 1)");

        var searchState = new SearchState(llt, name);
        if (searchState.IsEmpty)
            return new PoolCeilingReport(rho, 0, 0, 0, 0, 0, 0, 0);

        int vectorSizeBytes = searchState.Options.VectorSizeBytes;
        if (queriesBlob.Length != queryCount * vectorSizeBytes)
            throw new ArgumentException(
                $"queriesBlob length {queriesBlob.Length} does not match queryCount {queryCount} * vectorSizeBytes {vectorSizeBytes}");

        long l0Visits = 0, l0Cur = 0, l0Pool = 0;
        long upVisits = 0, upCur = 0, upPool = 0;

        // Reusable scratch — 2-hop expansion grows roughly M² so cap defensively.
        var poolHash = new HashSet<int>();

        for (int q = 0; q < queryCount; q++)
        {
            var query = queriesBlob.Slice(q * vectorSizeBytes, vectorSizeBytes);

            int currentIdx = searchState.GetNodeIndexById(EntryPointId);
            float currentDist = searchState.Distance(query, -1, currentIdx);

            for (int level = searchState.Options.MaxLevel; level >= 0; level--)
            {
                while (true)
                {
                    ref var node = ref searchState.GetNodeByIndex(currentIdx);
                    if (node.EdgesPerLevel.Count <= level)
                        break;

                    ref var edges = ref node.EdgesPerLevel[level];

                    bool coveredCur = false;
                    bool coveredPool = false;
                    int bestEdgeIdx = -1;
                    float bestDist = currentDist;
                    float witnessCeiling = rho * currentDist;

                    // Pass 1: direct neighbors (η_cur). Track the descent move.
                    for (int i = 0; i < edges.Count; i++)
                    {
                        int neighborIdx = searchState.GetNodeIndexById(edges[i]);
                        float d = searchState.Distance(query, -1, neighborIdx);
                        if (d <= witnessCeiling)
                            coveredCur = true;
                        if (d < bestDist)
                        {
                            bestDist = d;
                            bestEdgeIdx = neighborIdx;
                        }
                    }

                    // Pass 2: 2-hop pool (η_pool). Only run if cur did not already cover.
                    // The 2-hop union ⊇ 1-hop so if cur covered then pool covers too.
                    if (coveredCur)
                    {
                        coveredPool = true;
                    }
                    else
                    {
                        poolHash.Clear();
                        poolHash.Add(currentIdx);
                        for (int i = 0; i < edges.Count; i++)
                            poolHash.Add(searchState.GetNodeIndexById(edges[i]));

                        for (int i = 0; i < edges.Count && coveredPool == false; i++)
                        {
                            int v = searchState.GetNodeIndexById(edges[i]);
                            ref var vNode = ref searchState.GetNodeByIndex(v);
                            if (vNode.EdgesPerLevel.Count <= level)
                                continue;
                            ref var vEdges = ref vNode.EdgesPerLevel[level];
                            for (int j = 0; j < vEdges.Count; j++)
                            {
                                int w = searchState.GetNodeIndexById(vEdges[j]);
                                if (poolHash.Add(w) == false)
                                    continue;
                                float dw = searchState.Distance(query, -1, w);
                                if (dw <= witnessCeiling)
                                {
                                    coveredPool = true;
                                    break;
                                }
                            }
                        }
                    }

                    if (level == 0)
                    {
                        l0Visits++;
                        if (coveredCur) l0Cur++;
                        if (coveredPool) l0Pool++;
                    }
                    else
                    {
                        upVisits++;
                        if (coveredCur) upCur++;
                        if (coveredPool) upPool++;
                    }

                    if (bestEdgeIdx == -1)
                        break;

                    currentIdx = bestEdgeIdx;
                    currentDist = bestDist;
                }
            }
        }

        return new PoolCeilingReport(rho, queryCount, l0Visits, l0Cur, l0Pool, upVisits, upCur, upPool);
    }

    public static PoolCeilingReport MeasurePoolCeiling(LowLevelTransaction llt, string name, ReadOnlySpan<byte> queriesBlob, int queryCount, float rho)
    {
        using (Slice.From(llt.Allocator, name, out var slice))
            return MeasurePoolCeiling(llt, slice, queriesBlob, queryCount, rho);
    }

    /// <summary>
    /// FRAMEWORK §15.5 enriched pool ceiling. Same as <see cref="MeasurePoolCeiling"/> but
    /// extends the candidate pool with reverse-neighbours: for each visited (u, ℓ), the
    /// enriched pool is N_ℓ(u) ∪ N_ℓ(N_ℓ(u)) ∪ R_ℓ(u) ∪ N_ℓ(R_ℓ(u)) where R_ℓ(u) =
    /// {v : u ∈ N_ℓ(v)} (incoming edges). Reports η_pool (2-hop) and η_pool_rev
    /// (2-hop with reverse-neighbours added). If η_pool_rev − η_pool clears the framework
    /// threshold, §15.5 enrichment is the right lever; if not, deeper enrichment
    /// (delete-bypass / recent-insertion-beams / promotion) is required.
    /// Builds the reverse-edge index in one pass — O(N·M) time and space.
    /// </summary>
    public readonly record struct EnrichedPoolCeilingReport(
        float Rho,
        int QueriesSampled,
        long L0Visits,
        long L0CoveredCur,
        long L0CoveredPool,
        long L0CoveredEnriched,
        long UpperVisits,
        long UpperCoveredCur,
        long UpperCoveredPool,
        long UpperCoveredEnriched)
    {
        public double EtaCurL0     => L0Visits == 0 ? 0.0 : (double)L0CoveredCur      / L0Visits;
        public double EtaPoolL0    => L0Visits == 0 ? 0.0 : (double)L0CoveredPool     / L0Visits;
        public double EtaEnrL0     => L0Visits == 0 ? 0.0 : (double)L0CoveredEnriched / L0Visits;
        public double GapL0        => EtaPoolL0 - EtaCurL0;
        public double EnrGainL0    => EtaEnrL0  - EtaPoolL0;
        public double EtaCurUp     => UpperVisits == 0 ? 0.0 : (double)UpperCoveredCur      / UpperVisits;
        public double EtaPoolUp    => UpperVisits == 0 ? 0.0 : (double)UpperCoveredPool     / UpperVisits;
        public double EtaEnrUp     => UpperVisits == 0 ? 0.0 : (double)UpperCoveredEnriched / UpperVisits;
        public double GapUpper     => EtaPoolUp - EtaCurUp;
        public double EnrGainUpper => EtaEnrUp  - EtaPoolUp;

        public override string ToString() =>
            $"EnrichedPoolCeilingReport(ρ={Rho:F2}, Q={QueriesSampled}, " +
            $"L0[ηcur={EtaCurL0:F4} ηpool={EtaPoolL0:F4} ηenr={EtaEnrL0:F4} gap={GapL0:F4} enrGain={EnrGainL0:F4}], " +
            $"Up[ηcur={EtaCurUp:F4} ηpool={EtaPoolUp:F4} ηenr={EtaEnrUp:F4} gap={GapUpper:F4} enrGain={EnrGainUpper:F4}])";
    }

    public static EnrichedPoolCeilingReport MeasureEnrichedPoolCeiling(
        LowLevelTransaction llt,
        Slice name,
        ReadOnlySpan<byte> queriesBlob,
        int queryCount,
        float rho)
    {
        if (rho <= 0f || rho >= 1f)
            throw new ArgumentOutOfRangeException(nameof(rho), rho, "rho must be in (0, 1)");

        var searchState = new SearchState(llt, name);
        if (searchState.IsEmpty)
            return new EnrichedPoolCeilingReport(rho, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        int vectorSizeBytes = searchState.Options.VectorSizeBytes;
        if (queriesBlob.Length != queryCount * vectorSizeBytes)
            throw new ArgumentException(
                $"queriesBlob length {queriesBlob.Length} does not match queryCount {queryCount} * vectorSizeBytes {vectorSizeBytes}");

        long nodeCount = searchState.Options.CountOfVectors;
        int maxLevel = searchState.Options.MaxLevel;

        // Build reverse-edge index: reverseByLevel[ℓ][nodeIdx] = list of source node-indices
        // that point to nodeIdx at level ℓ. Single pass over all nodes; only allocate the
        // list lazily on first reverse-edge to keep memory tight at upper levels.
        var reverseByLevel = new Dictionary<int, List<int>>[maxLevel + 1];
        for (int ℓ = 0; ℓ <= maxLevel; ℓ++)
            reverseByLevel[ℓ] = new Dictionary<int, List<int>>();

        // Snapshot edges before resolving indices: GetNodeIndexById may lazy-load a node,
        // which reallocates SearchState._Nodes and invalidates any held ref. We can hold
        // ref var src across pure reads of EdgesPerLevel structure, but the per-level
        // edge resolution must operate on a copy.
        var edgeSnapshot = new List<long>(32);
        for (long id = 1; id <= nodeCount; id++)
        {
            int srcIdx = searchState.GetNodeIndexById(id);
            int levels;
            {
                ref var src = ref searchState.GetNodeByIndex(srcIdx);
                levels = Math.Min(src.EdgesPerLevel.Count, maxLevel + 1);
            }
            for (int ℓ = 0; ℓ < levels; ℓ++)
            {
                edgeSnapshot.Clear();
                {
                    ref var src2 = ref searchState.GetNodeByIndex(srcIdx);
                    if (src2.EdgesPerLevel.Count <= ℓ)
                        continue;
                    ref var es = ref src2.EdgesPerLevel[ℓ];
                    for (int i = 0; i < es.Count; i++)
                        edgeSnapshot.Add(es[i]);
                }
                var dict = reverseByLevel[ℓ];
                for (int i = 0; i < edgeSnapshot.Count; i++)
                {
                    int dstIdx = searchState.GetNodeIndexById(edgeSnapshot[i]);
                    if (dict.TryGetValue(dstIdx, out var lst) == false)
                    {
                        lst = new List<int>(4);
                        dict[dstIdx] = lst;
                    }
                    lst.Add(srcIdx);
                }
            }
        }

        long l0Visits = 0, l0Cur = 0, l0Pool = 0, l0Enr = 0;
        long upVisits = 0, upCur = 0, upPool = 0, upEnr = 0;

        var poolHash = new HashSet<int>();
        var enrHash  = new HashSet<int>();

        for (int q = 0; q < queryCount; q++)
        {
            var query = queriesBlob.Slice(q * vectorSizeBytes, vectorSizeBytes);

            int currentIdx = searchState.GetNodeIndexById(EntryPointId);
            float currentDist = searchState.Distance(query, -1, currentIdx);

            for (int level = maxLevel; level >= 0; level--)
            {
                while (true)
                {
                    ref var node = ref searchState.GetNodeByIndex(currentIdx);
                    if (node.EdgesPerLevel.Count <= level)
                        break;

                    ref var edges = ref node.EdgesPerLevel[level];

                    bool coveredCur = false;
                    bool coveredPool = false;
                    bool coveredEnr = false;
                    int bestEdgeIdx = -1;
                    float bestDist = currentDist;
                    float witnessCeiling = rho * currentDist;

                    for (int i = 0; i < edges.Count; i++)
                    {
                        int neighborIdx = searchState.GetNodeIndexById(edges[i]);
                        float d = searchState.Distance(query, -1, neighborIdx);
                        if (d <= witnessCeiling)
                            coveredCur = true;
                        if (d < bestDist)
                        {
                            bestDist = d;
                            bestEdgeIdx = neighborIdx;
                        }
                    }

                    if (coveredCur)
                    {
                        coveredPool = true;
                        coveredEnr = true;
                    }
                    else
                    {
                        // 2-hop forward pool first (matches MeasurePoolCeiling semantics).
                        poolHash.Clear();
                        poolHash.Add(currentIdx);
                        for (int i = 0; i < edges.Count; i++)
                            poolHash.Add(searchState.GetNodeIndexById(edges[i]));

                        for (int i = 0; i < edges.Count && coveredPool == false; i++)
                        {
                            int v = searchState.GetNodeIndexById(edges[i]);
                            ref var vNode = ref searchState.GetNodeByIndex(v);
                            if (vNode.EdgesPerLevel.Count <= level)
                                continue;
                            ref var vEdges = ref vNode.EdgesPerLevel[level];
                            for (int j = 0; j < vEdges.Count; j++)
                            {
                                int w = searchState.GetNodeIndexById(vEdges[j]);
                                if (poolHash.Add(w) == false)
                                    continue;
                                float dw = searchState.Distance(query, -1, w);
                                if (dw <= witnessCeiling)
                                {
                                    coveredPool = true;
                                    break;
                                }
                            }
                        }

                        // Enriched pool: poolHash ∪ reverse-edges of currentIdx ∪
                        // forward edges of those reverse-source nodes (one extra hop
                        // through the incoming side). Re-uses poolHash as the
                        // already-tested set.
                        if (coveredPool)
                        {
                            coveredEnr = true;
                        }
                        else
                        {
                            enrHash.Clear();
                            var revOfCur = reverseByLevel[level].TryGetValue(currentIdx, out var rc) ? rc : null;
                            if (revOfCur != null)
                            {
                                for (int i = 0; i < revOfCur.Count && coveredEnr == false; i++)
                                {
                                    int r = revOfCur[i];
                                    if (poolHash.Contains(r))
                                        continue;
                                    if (enrHash.Add(r) == false)
                                        continue;
                                    float dr = searchState.Distance(query, -1, r);
                                    if (dr <= witnessCeiling)
                                    {
                                        coveredEnr = true;
                                        break;
                                    }
                                    // One more hop through r's forward neighbours.
                                    ref var rNode = ref searchState.GetNodeByIndex(r);
                                    if (rNode.EdgesPerLevel.Count <= level)
                                        continue;
                                    ref var rEdges = ref rNode.EdgesPerLevel[level];
                                    for (int j = 0; j < rEdges.Count; j++)
                                    {
                                        int w = searchState.GetNodeIndexById(rEdges[j]);
                                        if (poolHash.Contains(w) || enrHash.Add(w) == false)
                                            continue;
                                        float dw = searchState.Distance(query, -1, w);
                                        if (dw <= witnessCeiling)
                                        {
                                            coveredEnr = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (level == 0)
                    {
                        l0Visits++;
                        if (coveredCur)  l0Cur++;
                        if (coveredPool) l0Pool++;
                        if (coveredEnr)  l0Enr++;
                    }
                    else
                    {
                        upVisits++;
                        if (coveredCur)  upCur++;
                        if (coveredPool) upPool++;
                        if (coveredEnr)  upEnr++;
                    }

                    if (bestEdgeIdx == -1)
                        break;

                    currentIdx = bestEdgeIdx;
                    currentDist = bestDist;
                }
            }
        }

        return new EnrichedPoolCeilingReport(
            rho, queryCount,
            l0Visits, l0Cur, l0Pool, l0Enr,
            upVisits, upCur, upPool, upEnr);
    }

    public static EnrichedPoolCeilingReport MeasureEnrichedPoolCeiling(LowLevelTransaction llt, string name, ReadOnlySpan<byte> queriesBlob, int queryCount, float rho)
    {
        using (Slice.From(llt.Allocator, name, out var slice))
            return MeasureEnrichedPoolCeiling(llt, slice, queriesBlob, queryCount, rho);
    }

    /// <summary>
    /// FRAMEWORK §15.5 / §4 deep-pool ceiling. Same as <see cref="MeasurePoolCeiling"/> but
    /// also reports the 3-hop forward pool ceiling η_3hop ⊇ η_pool (2-hop). The 3-hop
    /// gain (η_3hop − η_pool) tells us whether deeper static graph expansion brings new
    /// witnesses, complementing the reverse-neighbour measurement in
    /// <see cref="MeasureEnrichedPoolCeiling"/>. If neither 3-hop nor reverse helps,
    /// the pool is structurally saturated and any further improvement requires
    /// q-conditioned candidates (recent-insertion beams §15.2, delete-bypass §15.4)
    /// or fresh promotion (§15.8).
    /// </summary>
    public readonly record struct DeepPoolCeilingReport(
        float Rho,
        int QueriesSampled,
        long L0Visits,
        long L0CoveredCur,
        long L0CoveredPool,
        long L0CoveredDeep,
        long UpperVisits,
        long UpperCoveredCur,
        long UpperCoveredPool,
        long UpperCoveredDeep)
    {
        public double EtaCurL0    => L0Visits == 0 ? 0.0 : (double)L0CoveredCur  / L0Visits;
        public double EtaPoolL0   => L0Visits == 0 ? 0.0 : (double)L0CoveredPool / L0Visits;
        public double EtaDeepL0   => L0Visits == 0 ? 0.0 : (double)L0CoveredDeep / L0Visits;
        public double GapL0       => EtaPoolL0 - EtaCurL0;
        public double DeepGainL0  => EtaDeepL0 - EtaPoolL0;
        public double EtaCurUp    => UpperVisits == 0 ? 0.0 : (double)UpperCoveredCur  / UpperVisits;
        public double EtaPoolUp   => UpperVisits == 0 ? 0.0 : (double)UpperCoveredPool / UpperVisits;
        public double EtaDeepUp   => UpperVisits == 0 ? 0.0 : (double)UpperCoveredDeep / UpperVisits;
        public double GapUpper    => EtaPoolUp - EtaCurUp;
        public double DeepGainUp  => EtaDeepUp - EtaPoolUp;

        public override string ToString() =>
            $"DeepPoolCeilingReport(ρ={Rho:F2}, Q={QueriesSampled}, " +
            $"L0[ηcur={EtaCurL0:F4} ηpool={EtaPoolL0:F4} η3hop={EtaDeepL0:F4} gap={GapL0:F4} 3hopG={DeepGainL0:F4}], " +
            $"Up[ηcur={EtaCurUp:F4} ηpool={EtaPoolUp:F4} η3hop={EtaDeepUp:F4} gap={GapUpper:F4} 3hopG={DeepGainUp:F4}])";
    }

    public static DeepPoolCeilingReport MeasureDeepPoolCeiling(
        LowLevelTransaction llt,
        Slice name,
        ReadOnlySpan<byte> queriesBlob,
        int queryCount,
        float rho)
    {
        if (rho <= 0f || rho >= 1f)
            throw new ArgumentOutOfRangeException(nameof(rho), rho, "rho must be in (0, 1)");

        var searchState = new SearchState(llt, name);
        if (searchState.IsEmpty)
            return new DeepPoolCeilingReport(rho, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        int vectorSizeBytes = searchState.Options.VectorSizeBytes;
        if (queriesBlob.Length != queryCount * vectorSizeBytes)
            throw new ArgumentException(
                $"queriesBlob length {queriesBlob.Length} does not match queryCount {queryCount} * vectorSizeBytes {vectorSizeBytes}");

        long l0Visits = 0, l0Cur = 0, l0Pool = 0, l0Deep = 0;
        long upVisits = 0, upCur = 0, upPool = 0, upDeep = 0;

        var poolHash = new HashSet<int>();
        // Track the 2-hop boundary (the w nodes added during 2-hop expansion) so 3-hop
        // can expand only the newly-added frontier.
        var twoHopFrontier = new List<int>(256);

        for (int q = 0; q < queryCount; q++)
        {
            var query = queriesBlob.Slice(q * vectorSizeBytes, vectorSizeBytes);

            int currentIdx = searchState.GetNodeIndexById(EntryPointId);
            float currentDist = searchState.Distance(query, -1, currentIdx);

            for (int level = searchState.Options.MaxLevel; level >= 0; level--)
            {
                while (true)
                {
                    ref var node = ref searchState.GetNodeByIndex(currentIdx);
                    if (node.EdgesPerLevel.Count <= level)
                        break;

                    ref var edges = ref node.EdgesPerLevel[level];

                    bool coveredCur = false;
                    bool coveredPool = false;
                    bool coveredDeep = false;
                    int bestEdgeIdx = -1;
                    float bestDist = currentDist;
                    float witnessCeiling = rho * currentDist;

                    for (int i = 0; i < edges.Count; i++)
                    {
                        int neighborIdx = searchState.GetNodeIndexById(edges[i]);
                        float d = searchState.Distance(query, -1, neighborIdx);
                        if (d <= witnessCeiling)
                            coveredCur = true;
                        if (d < bestDist)
                        {
                            bestDist = d;
                            bestEdgeIdx = neighborIdx;
                        }
                    }

                    if (coveredCur)
                    {
                        coveredPool = true;
                        coveredDeep = true;
                    }
                    else
                    {
                        poolHash.Clear();
                        twoHopFrontier.Clear();
                        poolHash.Add(currentIdx);
                        for (int i = 0; i < edges.Count; i++)
                            poolHash.Add(searchState.GetNodeIndexById(edges[i]));

                        // 2-hop forward expansion.
                        for (int i = 0; i < edges.Count && coveredPool == false; i++)
                        {
                            int v = searchState.GetNodeIndexById(edges[i]);
                            ref var vNode = ref searchState.GetNodeByIndex(v);
                            if (vNode.EdgesPerLevel.Count <= level)
                                continue;
                            ref var vEdges = ref vNode.EdgesPerLevel[level];
                            for (int j = 0; j < vEdges.Count; j++)
                            {
                                int w = searchState.GetNodeIndexById(vEdges[j]);
                                if (poolHash.Add(w) == false)
                                    continue;
                                twoHopFrontier.Add(w);
                                float dw = searchState.Distance(query, -1, w);
                                if (dw <= witnessCeiling)
                                {
                                    coveredPool = true;
                                    break;
                                }
                            }
                        }
                        // If 2-hop didn't terminate covered, finish building the frontier
                        // (the loop above may have broken early). For correctness with
                        // 3-hop, we need the full 2-hop frontier set even on early hit;
                        // but on early hit coveredDeep follows coveredPool so skip.

                        if (coveredPool)
                        {
                            coveredDeep = true;
                        }
                        else
                        {
                            // 3-hop forward: expand each member of twoHopFrontier once.
                            // poolHash already contains 1-hop + 2-hop, so additions here
                            // are strictly new (3-hop-only) candidates.
                            for (int i = 0; i < twoHopFrontier.Count && coveredDeep == false; i++)
                            {
                                int w = twoHopFrontier[i];
                                ref var wNode = ref searchState.GetNodeByIndex(w);
                                if (wNode.EdgesPerLevel.Count <= level)
                                    continue;
                                ref var wEdges = ref wNode.EdgesPerLevel[level];
                                for (int k = 0; k < wEdges.Count; k++)
                                {
                                    int x = searchState.GetNodeIndexById(wEdges[k]);
                                    if (poolHash.Add(x) == false)
                                        continue;
                                    float dx = searchState.Distance(query, -1, x);
                                    if (dx <= witnessCeiling)
                                    {
                                        coveredDeep = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if (level == 0)
                    {
                        l0Visits++;
                        if (coveredCur)  l0Cur++;
                        if (coveredPool) l0Pool++;
                        if (coveredDeep) l0Deep++;
                    }
                    else
                    {
                        upVisits++;
                        if (coveredCur)  upCur++;
                        if (coveredPool) upPool++;
                        if (coveredDeep) upDeep++;
                    }

                    if (bestEdgeIdx == -1)
                        break;

                    currentIdx = bestEdgeIdx;
                    currentDist = bestDist;
                }
            }
        }

        return new DeepPoolCeilingReport(
            rho, queryCount,
            l0Visits, l0Cur, l0Pool, l0Deep,
            upVisits, upCur, upPool, upDeep);
    }

    public static DeepPoolCeilingReport MeasureDeepPoolCeiling(LowLevelTransaction llt, string name, ReadOnlySpan<byte> queriesBlob, int queryCount, float rho)
    {
        using (Slice.From(llt.Allocator, name, out var slice))
            return MeasureDeepPoolCeiling(llt, slice, queriesBlob, queryCount, rho);
    }

    /// <summary>
    /// FRAMEWORK §6 / §15.10 L0 repair feasibility diagnostic. Quantifies how many
    /// uncovered L0 visits could be repaired by a one-swap that respects the §6
    /// distance-inflation bound `D(u, v) ≤ β_0 · max_{s ∈ N_0(u)} D(u, s)`.
    /// For each uncovered L0 (u, q) pair (η_cur fails), scans the 2-hop pool for
    /// v such that:
    ///   1. D(v, q) ≤ ρ · D(u, q)   (would be a ρ-witness, recall lever)
    ///   2. v ∉ N_0(u)              (not already an edge)
    ///   3. D(u, v) ≤ β_0 · max_{s ∈ N_0(u)} D(u, s)  (radial inflation bound)
    /// Reports the fraction of uncovered visits with ≥1 feasible candidate. If this
    /// fraction is small under tight β_0 (1.05–1.25), the distance bound itself is
    /// blocking repair and §15.10 cannot help without recall-tradeoff (looser β).
    /// </summary>
    public readonly record struct L0RepairFeasibilityReport(
        float Rho,
        float BetaL0,
        int QueriesSampled,
        long L0Visits,
        long L0Uncovered,
        long L0UncoveredWithFeasibleCandidate,
        long L0UncoveredWithPoolWitness)
    {
        public double FractionUncovered          => L0Visits == 0 ? 0.0 : (double)L0Uncovered / L0Visits;
        public double FractionUncoveredFeasible  => L0Uncovered == 0 ? 0.0 : (double)L0UncoveredWithFeasibleCandidate / L0Uncovered;
        public double FractionUncoveredPoolHas   => L0Uncovered == 0 ? 0.0 : (double)L0UncoveredWithPoolWitness / L0Uncovered;
        /// <summary>How much of the pool-available headroom survives the §6 distance bound.</summary>
        public double BoundSurvivalRatio         => L0UncoveredWithPoolWitness == 0 ? 0.0 : (double)L0UncoveredWithFeasibleCandidate / L0UncoveredWithPoolWitness;

        public override string ToString() =>
            $"L0RepairFeasibilityReport(ρ={Rho:F2}, β0={BetaL0:F2}, Q={QueriesSampled}, " +
            $"L0Visits={L0Visits}, Uncovered={L0Uncovered} ({FractionUncovered:P1}), " +
            $"PoolHasWitness={FractionUncoveredPoolHas:P1}, Feasible={FractionUncoveredFeasible:P1}, " +
            $"BoundSurvival={BoundSurvivalRatio:P1})";
    }

    public static L0RepairFeasibilityReport MeasureL0RepairFeasibility(
        LowLevelTransaction llt,
        Slice name,
        ReadOnlySpan<byte> queriesBlob,
        int queryCount,
        float rho,
        float betaL0)
    {
        if (rho <= 0f || rho >= 1f)
            throw new ArgumentOutOfRangeException(nameof(rho), rho, "rho must be in (0, 1)");
        if (betaL0 < 1f)
            throw new ArgumentOutOfRangeException(nameof(betaL0), betaL0, "betaL0 must be >= 1");

        var searchState = new SearchState(llt, name);
        if (searchState.IsEmpty)
            return new L0RepairFeasibilityReport(rho, betaL0, 0, 0, 0, 0, 0);

        int vectorSizeBytes = searchState.Options.VectorSizeBytes;
        if (queriesBlob.Length != queryCount * vectorSizeBytes)
            throw new ArgumentException(
                $"queriesBlob length {queriesBlob.Length} does not match queryCount {queryCount} * vectorSizeBytes {vectorSizeBytes}");

        long l0Visits = 0, l0Uncovered = 0, l0Feasible = 0, l0PoolWitness = 0;

        var poolHash = new HashSet<int>();
        var edgeIdx = new List<int>(32);

        for (int q = 0; q < queryCount; q++)
        {
            var query = queriesBlob.Slice(q * vectorSizeBytes, vectorSizeBytes);

            int currentIdx = searchState.GetNodeIndexById(EntryPointId);
            float currentDist = searchState.Distance(query, -1, currentIdx);

            for (int level = searchState.Options.MaxLevel; level >= 0; level--)
            {
                while (true)
                {
                    int u = currentIdx;
                    ref var node = ref searchState.GetNodeByIndex(u);
                    if (node.EdgesPerLevel.Count <= level)
                        break;

                    ref var edges = ref node.EdgesPerLevel[level];

                    bool coveredCur = false;
                    int bestEdgeIdx = -1;
                    float bestDist = currentDist;
                    float witnessCeiling = rho * currentDist;

                    // Pass 1: direct neighbors. Snapshot edge indices for later use.
                    edgeIdx.Clear();
                    float maxEdgeDist = 0f;
                    for (int i = 0; i < edges.Count; i++)
                    {
                        int neighborIdx = searchState.GetNodeIndexById(edges[i]);
                        edgeIdx.Add(neighborIdx);
                        float d = searchState.Distance(query, -1, neighborIdx);
                        if (d <= witnessCeiling)
                            coveredCur = true;
                        if (d < bestDist)
                        {
                            bestDist = d;
                            bestEdgeIdx = neighborIdx;
                        }
                    }

                    // Feasibility check only at L0 and only on uncovered visits.
                    if (level == 0)
                    {
                        l0Visits++;
                        if (coveredCur)
                        {
                            // Already covered, skip feasibility analysis.
                        }
                        else
                        {
                            l0Uncovered++;

                            // Compute max edge distance D(u, s) for the radial bound.
                            maxEdgeDist = 0f;
                            for (int i = 0; i < edgeIdx.Count; i++)
                            {
                                float duEdge = searchState.Distance(ReadOnlySpan<byte>.Empty, u, edgeIdx[i]);
                                if (duEdge > maxEdgeDist)
                                    maxEdgeDist = duEdge;
                            }
                            float radialCeiling = betaL0 * maxEdgeDist;

                            // Walk 2-hop pool looking for: (witness for q) AND (radial bound).
                            poolHash.Clear();
                            poolHash.Add(u);
                            for (int i = 0; i < edgeIdx.Count; i++)
                                poolHash.Add(edgeIdx[i]);

                            bool poolHasWitness = false;
                            bool feasibleFound = false;
                            for (int i = 0; i < edgeIdx.Count; i++)
                            {
                                int v = edgeIdx[i];
                                ref var vNode = ref searchState.GetNodeByIndex(v);
                                if (vNode.EdgesPerLevel.Count <= level)
                                    continue;
                                ref var vEdges = ref vNode.EdgesPerLevel[level];
                                for (int j = 0; j < vEdges.Count; j++)
                                {
                                    int w = searchState.GetNodeIndexById(vEdges[j]);
                                    if (poolHash.Add(w) == false)
                                        continue;
                                    float dwq = searchState.Distance(query, -1, w);
                                    if (dwq > witnessCeiling)
                                        continue;
                                    poolHasWitness = true;
                                    // Witness in pool — does it also satisfy the radial bound?
                                    float duw = searchState.Distance(ReadOnlySpan<byte>.Empty, u, w);
                                    if (duw <= radialCeiling)
                                    {
                                        feasibleFound = true;
                                        break;
                                    }
                                }
                                if (feasibleFound)
                                    break;
                            }
                            if (poolHasWitness) l0PoolWitness++;
                            if (feasibleFound)  l0Feasible++;
                        }
                    }

                    if (bestEdgeIdx == -1)
                        break;

                    currentIdx = bestEdgeIdx;
                    currentDist = bestDist;
                }
            }
        }

        return new L0RepairFeasibilityReport(rho, betaL0, queryCount, l0Visits, l0Uncovered, l0Feasible, l0PoolWitness);
    }

    public static L0RepairFeasibilityReport MeasureL0RepairFeasibility(LowLevelTransaction llt, string name, ReadOnlySpan<byte> queriesBlob, int queryCount, float rho, float betaL0)
    {
        using (Slice.From(llt.Allocator, name, out var slice))
            return MeasureL0RepairFeasibility(llt, slice, queriesBlob, queryCount, rho, betaL0);
    }

    public static void RenderAndShow(LowLevelTransaction llt, string name, Span<byte> vector)
    {
        using (Slice.From(llt.Allocator, name, out var slice))
        {
            RenderAndShow(llt, slice, vector);
        }
    }
    
    public static void RenderAndShow(LowLevelTransaction llt, Slice name, Span<byte> vector)
    {
        var searchState = new SearchState(llt, name);
        string fileName = Path.GetTempFileName() + ".html";
        using (var f = File.CreateText(fileName))
        {
            f.WriteLine(@"<html><style>
/* Basic table styling */
table {
    width: 100%;
    border-collapse: collapse;
}
/* Style for table headers */
th {
    background-color: #f2f2f2;
    color: #333;
    padding: 10px;
    text-align: left;
    border-bottom: 2px solid #ddd;
}
th.result {
    background-color: Violet;
}
th.path {
    background-color: aqua;
}
/* Style for table cells */
td {
    padding: 10px;
    border-bottom: 1px solid #ddd;
}
/* Alternate row colors for better readability */
tr:nth-child(even) {
    background-color: #f9f9f9;
}
/* Add some padding and border to the table */
table, th, td {
    border: 1px solid #ddd;
}

</style><body>");

            var path = new ContextBoundNativeList<int>(llt.Allocator, searchState.Options.MaxLevel + 1);
            var edges = new ContextBoundNativeList<int>(llt.Allocator, 16);
            searchState.SearchNearestAcrossLevels(vector, -1, searchState.Options.MaxLevel,  ref path);
            using var search = searchState.NearestSearch(path[0], new Memory<byte>(vector.ToArray()), 0, 8, edges, SearchState.NearestEdgesFlags.StartingPointAsEdge, false);
            using var it = search.Search().GetEnumerator();
            it.MoveNext();
            search.TryGetCurrentCandidates(out edges);
            
            
            for (int level = searchState.Options.MaxLevel - 1; level >= 0; level--)
            {
                f.WriteLine($"<h1>Level: {level}</h1>");
                f.WriteLine("<table><tr>");
                int cols = 0;
                for (int j = 1; j <= searchState.Options.CountOfVectors; j++)
                {
                    var nodeIdx = searchState.GetNodeIndexById(j);
                    ref var n = ref searchState.Nodes[nodeIdx];
                    if (level >= n.EdgesPerLevel.Count)
                        continue;

                    var dist = searchState.Distance(vector, -1, nodeIdx);
                    var isPath = path[level] == nodeIdx ? "path" : "";
                    var isResult =  level == 0 && edges.Inner.Items.Contains(nodeIdx) ? "result": "";
                    var nextId = level == 0 ? (edges.Inner.Items.Contains(nodeIdx) ?"***": "") : $"N_{path[level - 1]}_{level - 1}";
                    f.WriteLine($"<td> <table id='N_{j}_{level}'><tr><th class='{isPath} {isResult}'>N_{j}_{level} - {GetEntryId(llt,n.PostingListId)}</th>" +
                                $"<th>{n.EdgesPerLevel[level].Count}</th><th>{dist} (<a href='#{nextId}'>{nextId}</a>)</th></tr><tr>");
                    foreach (var to in n.EdgesPerLevel[level])
                    {
                        dist = searchState.Distance(Span<byte>.Empty, nodeIdx, searchState.GetNodeIndexById(to));
                        var srcDist = searchState.Distance(vector, -1, searchState.GetNodeIndexById(to));
                        var id = $"N_{to}_{Math.Max(0, level-1)}";
                     
                        f.WriteLine($"<tr><td><a href='#{id}'>{id}</a></td><td>{dist}</td><td>{srcDist}</td></tr>");
                    }
                    f.WriteLine("</table></td>");
                    if (++cols == 8)
                    {
                        f.WriteLine("</tr><tr>");
                        cols = 0;
                    }
                }

                f.WriteLine("</tr></table>");
            }

            // for (long j = 1; j <= searchState.Options.CountOfVectors; j++)
            // {
            //     ref var n = ref searchState.GetNodeById(j);
            //     for (int i = 1; i < n.NeighborsPerLevel.Count; i++)
            //     {
            //         f.WriteLine($"\tN_{j}_{i - 1} -- N_{j}_{i};");
            //     }
            // }

            f.WriteLine("</body></html>");
        }

        DebugStuff.OpenBrowser(fileName);
    }
}
