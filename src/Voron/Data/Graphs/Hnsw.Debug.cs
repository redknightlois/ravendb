using System;
using System.Buffers;
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

    /// <summary>
    /// FRAMEWORK §6 + §15.5 / §15.10 L0 repair feasibility with 3-hop pool. Same as
    /// <see cref="MeasureL0RepairFeasibility"/> but searches witnesses in the deeper
    /// N_0(u) ∪ N²_0(u) ∪ N³_0(u) pool. Tracks both 2-hop and 3-hop counts in a single
    /// pass so the deeper pool's effect on feasibility is directly comparable. The
    /// 3-hop pool gave large η-gain on Sphere-1M (Up 4.4pp / L0 22.6pp); this
    /// diagnostic answers whether those extra witnesses survive the §6 radial bound.
    /// </summary>
    public readonly record struct L0RepairFeasibilityDeepReport(
        float Rho,
        float BetaL0,
        int QueriesSampled,
        long L0Visits,
        long L0Uncovered,
        long L0Feasible2Hop,
        long L0PoolWitness2Hop,
        long L0Feasible3Hop,
        long L0PoolWitness3Hop)
    {
        public double FractionUncovered           => L0Visits == 0 ? 0.0 : (double)L0Uncovered / L0Visits;
        public double Feasible2HopFrac            => L0Uncovered == 0 ? 0.0 : (double)L0Feasible2Hop / L0Uncovered;
        public double PoolHas2HopFrac             => L0Uncovered == 0 ? 0.0 : (double)L0PoolWitness2Hop / L0Uncovered;
        public double Survival2Hop                => L0PoolWitness2Hop == 0 ? 0.0 : (double)L0Feasible2Hop / L0PoolWitness2Hop;
        public double Feasible3HopFrac            => L0Uncovered == 0 ? 0.0 : (double)L0Feasible3Hop / L0Uncovered;
        public double PoolHas3HopFrac             => L0Uncovered == 0 ? 0.0 : (double)L0PoolWitness3Hop / L0Uncovered;
        public double Survival3Hop                => L0PoolWitness3Hop == 0 ? 0.0 : (double)L0Feasible3Hop / L0PoolWitness3Hop;
        /// <summary>Headroom delivered by going to 3-hop over 2-hop, after the radial bound.</summary>
        public double FeasibleDeepGain            => Feasible3HopFrac - Feasible2HopFrac;

        public override string ToString() =>
            $"L0RepairFeasibilityDeepReport(ρ={Rho:F2}, β0={BetaL0:F2}, Q={QueriesSampled}, " +
            $"Visits={L0Visits}, Uncov={L0Uncovered} ({FractionUncovered:P1}), " +
            $"2hop[poolHas={PoolHas2HopFrac:P1} feas={Feasible2HopFrac:P1} surv={Survival2Hop:P1}], " +
            $"3hop[poolHas={PoolHas3HopFrac:P1} feas={Feasible3HopFrac:P1} surv={Survival3Hop:P1}], " +
            $"deepGain={FeasibleDeepGain:P1})";
    }

    public static L0RepairFeasibilityDeepReport MeasureL0RepairFeasibilityDeep(
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
            return new L0RepairFeasibilityDeepReport(rho, betaL0, 0, 0, 0, 0, 0, 0, 0);

        int vectorSizeBytes = searchState.Options.VectorSizeBytes;
        if (queriesBlob.Length != queryCount * vectorSizeBytes)
            throw new ArgumentException(
                $"queriesBlob length {queriesBlob.Length} does not match queryCount {queryCount} * vectorSizeBytes {vectorSizeBytes}");

        long l0Visits = 0, l0Uncov = 0;
        long l0Feas2 = 0, l0Pool2 = 0, l0Feas3 = 0, l0Pool3 = 0;

        var poolHash = new HashSet<int>();
        var twoHopFrontier = new List<int>(256);
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

                    edgeIdx.Clear();
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

                    if (level == 0 && coveredCur == false)
                    {
                        l0Visits++;
                        l0Uncov++;

                        float maxEdgeDist = 0f;
                        for (int i = 0; i < edgeIdx.Count; i++)
                        {
                            float duEdge = searchState.Distance(ReadOnlySpan<byte>.Empty, u, edgeIdx[i]);
                            if (duEdge > maxEdgeDist) maxEdgeDist = duEdge;
                        }
                        float radialCeiling = betaL0 * maxEdgeDist;

                        poolHash.Clear();
                        twoHopFrontier.Clear();
                        poolHash.Add(u);
                        for (int i = 0; i < edgeIdx.Count; i++)
                            poolHash.Add(edgeIdx[i]);

                        bool pool2 = false, feas2 = false;
                        // 2-hop pass — collect full 2-hop frontier so 3-hop pass has it.
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
                                twoHopFrontier.Add(w);
                                float dwq = searchState.Distance(query, -1, w);
                                if (dwq > witnessCeiling)
                                    continue;
                                pool2 = true;
                                if (feas2 == false)
                                {
                                    float duw = searchState.Distance(ReadOnlySpan<byte>.Empty, u, w);
                                    if (duw <= radialCeiling)
                                        feas2 = true;
                                }
                            }
                        }
                        if (pool2) l0Pool2++;
                        if (feas2) l0Feas2++;

                        // 3-hop pass: pool2/feas2 carry forward (3-hop ⊇ 2-hop).
                        bool pool3 = pool2, feas3 = feas2;
                        for (int i = 0; i < twoHopFrontier.Count; i++)
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
                                float dxq = searchState.Distance(query, -1, x);
                                if (dxq > witnessCeiling)
                                    continue;
                                pool3 = true;
                                if (feas3 == false)
                                {
                                    float dux = searchState.Distance(ReadOnlySpan<byte>.Empty, u, x);
                                    if (dux <= radialCeiling)
                                        feas3 = true;
                                }
                            }
                        }
                        if (pool3) l0Pool3++;
                        if (feas3) l0Feas3++;
                    }
                    else if (level == 0)
                    {
                        l0Visits++;
                    }

                    if (bestEdgeIdx == -1)
                        break;

                    currentIdx = bestEdgeIdx;
                    currentDist = bestDist;
                }
            }
        }

        return new L0RepairFeasibilityDeepReport(rho, betaL0, queryCount, l0Visits, l0Uncov, l0Feas2, l0Pool2, l0Feas3, l0Pool3);
    }

    public static L0RepairFeasibilityDeepReport MeasureL0RepairFeasibilityDeep(LowLevelTransaction llt, string name, ReadOnlySpan<byte> queriesBlob, int queryCount, float rho, float betaL0)
    {
        using (Slice.From(llt.Allocator, name, out var slice))
            return MeasureL0RepairFeasibilityDeep(llt, slice, queriesBlob, queryCount, rho, betaL0);
    }

    /// <summary>
    /// FRAMEWORK §15.10 L0 one-swap repair SIMULATION (no graph mutation). For each
    /// L0 node u visited during the descent walk, pick the single best 3-hop pool
    /// candidate v* (satisfying §6 radial bound) that maximizes coverage gain over
    /// queries visiting u. Reports pre vs post η at L0 assuming the swap is applied
    /// to N(u). Each u gets at most one swap (max-coverage one-swap constraint).
    ///
    /// Coverage gain per (u, v): number of queries q ∈ Q_u for which the existing
    /// N(u) fails to cover q at λ=ρ² but v would (D(v, q) ≤ ρ · D(u, q)).
    /// Best v* maximizes that count per u. Greedy max-coverage on uncovered queries.
    ///
    /// This is an UPPER BOUND on real repair-pass gain because path divergence
    /// after edge mutation may visit different (u, q) pairs. Useful to bound the
    /// realistic recall improvement before investing in mutation infrastructure.
    /// </summary>
    public readonly record struct L0OneSwapSimulationReport(
        float Rho,
        float BetaL0,
        int QueriesSampled,
        long L0Visits,
        long L0UncoveredPre,
        long L0UncoveredPost,
        long NodesVisitedAtL0,
        long NodesWithUncoveredQueries,
        long NodesRepaired,
        long TotalSwapsApplied,
        long ProposedSwaps = 0,
        long RejectedByHoeffding = 0,
        long RejectedBySpread = 0)
    {
        public double EtaPre  => L0Visits == 0 ? 0.0 : 1.0 - (double)L0UncoveredPre  / L0Visits;
        public double EtaPost => L0Visits == 0 ? 0.0 : 1.0 - (double)L0UncoveredPost / L0Visits;
        public double EtaGain => EtaPost - EtaPre;
        public double NodeRepairRate => NodesWithUncoveredQueries == 0 ? 0.0 : (double)NodesRepaired / NodesWithUncoveredQueries;

        public override string ToString() =>
            $"L0OneSwapSimulationReport(ρ={Rho:F2}, β0={BetaL0:F2}, Q={QueriesSampled}, " +
            $"Visits={L0Visits}, η_pre={EtaPre:F4}, η_post={EtaPost:F4}, Δη={EtaGain:F4}, " +
            $"nodes={NodesVisitedAtL0}, uncoveredNodes={NodesWithUncoveredQueries}, " +
            $"repaired={NodesRepaired} ({NodeRepairRate:P1}), swaps={TotalSwapsApplied}, " +
            $"proposed={ProposedSwaps}, rejHoeff={RejectedByHoeffding}, rejSpread={RejectedBySpread})";
    }

    public static L0OneSwapSimulationReport SimulateL0OneSwapRepair(
        LowLevelTransaction llt,
        Slice name,
        ReadOnlySpan<byte> queriesBlob,
        int queryCount,
        float rho,
        float betaL0,
        bool applyMutations = false,
        SearchState reuseSearchState = null,
        int reservoirCapPerNode = 0, // §3 trace reservoir cap (0 = unbounded)
        int targetLevel = 0,
        int? protectedOverride = null)
    {
        // FRAMEWORK §15.3 upper-layer path: pass targetLevel >= 1 and protectedOverride = 2
        // (per §15.3 r=2 default for upper layers). Caller should use β_ℓ ∈ [1.5, 3] instead
        // of L0's β_0 ∈ [1.05, 1.25]. All §6 / §5.3 / §7 guarantees carry over unchanged.
        if (rho <= 0f || rho >= 1f)
            throw new ArgumentOutOfRangeException(nameof(rho), rho, "rho must be in (0, 1)");
        if (betaL0 < 1f)
            throw new ArgumentOutOfRangeException(nameof(betaL0), betaL0, "betaL0 must be >= 1");

        // When reuseSearchState is provided (production path), mutations land on the caller's
        // SearchState so a subsequent PersistNode pass writes them to Voron. When null, a local
        // SearchState is created and mutations are discarded on return — measurement-only mode.
        var searchState = reuseSearchState ?? new SearchState(llt, name);
        if (searchState.IsEmpty)
            return new L0OneSwapSimulationReport(rho, betaL0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        int vectorSizeBytes = searchState.Options.VectorSizeBytes;
        if (queriesBlob.Length != queryCount * vectorSizeBytes)
            throw new ArgumentException(
                $"queriesBlob length {queriesBlob.Length} does not match queryCount {queryCount} * vectorSizeBytes {vectorSizeBytes}");

        // Per-node bookkeeping at L0:
        //   visitsAtNode[u]    = list of (q_idx, currentDist, currentlyCovered)
        // Build by walking each query's descent path. Two-pass to keep memory bounded.
        // Pass 1: collect (u, q, d, covered) tuples for L0 visits only.

        var l0NodeVisits = new Dictionary<int, List<(int qIdx, float dUQ, bool covered)>>();
        // §3 reservoir sampling Algorithm R: per-node count of total samples seen
        // (independent of capacity). When seen > cap, replacement probability is
        // cap/seen and a uniformly-random slot in [0, cap) is overwritten.
        var l0NodeSeen = new Dictionary<int, int>();
        var reservoirRng = reservoirCapPerNode > 0 ? new Random(0x5e7e_4011) : null;

        long l0Visits = 0;
        long l0UncoveredPre = 0;

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

                    if (level == targetLevel)
                    {
                        l0Visits++;
                        if (coveredCur == false) l0UncoveredPre++;
                        if (l0NodeVisits.TryGetValue(u, out var lst) == false)
                        {
                            lst = new List<(int, float, bool)>(4);
                            l0NodeVisits[u] = lst;
                        }
                        // §3 trace reservoir: cap per-node samples to reservoirCapPerNode using
                        // uniform reservoir sampling (Algorithm R). Cap=0 disables → unbounded list.
                        if (reservoirCapPerNode <= 0 || lst.Count < reservoirCapPerNode)
                        {
                            lst.Add((q, currentDist, coveredCur));
                            if (reservoirCapPerNode > 0)
                                l0NodeSeen[u] = lst.Count;
                        }
                        else
                        {
                            // Algorithm R: with the k-th excess sample (k >= cap), replace a
                            // uniformly-random slot with probability cap/(seen+1). seen is the
                            // per-node sample count tracked in l0NodeSeen, not the global q index.
                            int seen = l0NodeSeen[u] + 1;
                            l0NodeSeen[u] = seen;
                            int draw = reservoirRng.Next(seen);
                            if (draw < reservoirCapPerNode)
                                lst[draw] = (q, currentDist, coveredCur);
                        }
                    }

                    if (bestEdgeIdx == -1)
                        break;

                    currentIdx = bestEdgeIdx;
                    currentDist = bestDist;
                }
            }
        }

        // Pass 2: for each L0 node with ≥1 uncovered visit, find best one-swap v*.
        // v* maximizes |{(q, dUQ, cov=false) ∈ visits : D(v, q) ≤ rho · dUQ AND
        //   D(u, v) ≤ betaL0 · max_{s ∈ N_0(u)} D(u, s)}|.
        // Search v in N_0(u) ∪ N²_0(u) ∪ N³_0(u).

        long nodesVisitedAtL0 = l0NodeVisits.Count;
        long nodesWithUncovered = 0;
        long nodesRepaired = 0;
        long totalSwaps = 0;
        long l0UncoveredPost = 0;
        long proposedSwaps = 0;
        long rejectedByHoeffding = 0;
        long rejectedBySpread = 0;
        // Per-hop candidate accounting (printed when RAVEN_HNSW_L0_HOPSTATS=1).
        // poolN counts UNIQUE candidates added at hop N (regardless of radial ceiling).
        // feasN counts those inside radial ceiling. covN counts those with covCount > 0.
        long pool2 = 0, feas2 = 0, cov2 = 0;
        long pool3 = 0, feas3 = 0, cov3 = 0;
        long pool4 = 0, feas4 = 0, cov4 = 0;
        long poolTD = 0, feasTD = 0, covTD = 0;
        bool hopStats = Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_HOPSTATS") == "1";

        // FRAMEWORK §7 Hoeffding-gated train/val acceptance. Even-qIdx visits feed candidate
        // scoring (Q_train); odd-qIdx feed the validation gain estimate (Q_val). Accept only
        // when Δ̂_val > sqrt(2 log(B/δ)/m) + τ. §5.3 spread check rejects v if any existing
        // edge a has D(v, a) < D(u, v) (HNSW α-prune predicate, χ=1).
        bool useHoeffding = Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_HOEFFDING") == "1";
        bool useSpread = Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_SPREAD") == "1";
        float delta = float.TryParse(Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_DELTA"), out var dEnv) && dEnv > 0 ? dEnv : 0.01f;
        float tau = float.TryParse(Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_TAU"), out var tEnv) && tEnv >= 0 ? tEnv : 0.005f;
        float lambda = rho * rho; // §0: λ = ρ² (cosine-dissimilarity space)

        var poolHash = new HashSet<int>();
        var twoHopFrontier = new List<int>(256);
        var bestVCoverage = new Dictionary<int, int>(); // vIdx → count of uncovered queries it covers

        // FRAMEWORK §28 — top-down layered N-hop pool. Walks from EntryPointId
        // through every level ℓ ∈ [L_max, targetLevel] taking N hops at each
        // level then projecting survivors (nodes that exist at ℓ-1) to the
        // next level down. The resulting visited-at-targetLevel set augments
        // each u's bestVCoverage independent of u's local topology. Bounded
        // by frontier-cap to keep cost predictable on large graphs.
        // Enable: RAVEN_HNSW_L0_TOPDOWN=1. Hop count per level: env
        // RAVEN_HNSW_L0_TOPDOWN_HOPS (default 4). Frontier cap per level:
        // RAVEN_HNSW_L0_TOPDOWN_CAP (default 4096).
        HashSet<int> topdownPool = null;
        if (Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_TOPDOWN") == "1")
        {
            int hopsPerLevel = int.TryParse(Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_TOPDOWN_HOPS"), out var _th) && _th >= 1 ? _th : 4;
            int frontierCap = int.TryParse(Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_TOPDOWN_CAP"), out var _tc) && _tc >= 32 ? _tc : 4096;
            int maxLvl = searchState.Options.MaxLevel;
            int entryIdx = searchState.GetNodeIndexById(EntryPointId);
            if (entryIdx >= 0)
            {
                var visited = new HashSet<int> { entryIdx };
                var frontier = new List<int>(64) { entryIdx };
                for (int ℓ = maxLvl; ℓ >= targetLevel; ℓ--)
                {
                    // N-hop expansion at this level.
                    int hopStart = 0;
                    for (int hop = 0; hop < hopsPerLevel && frontier.Count < frontierCap; hop++)
                    {
                        int hopEnd = frontier.Count;
                        for (int i = hopStart; i < hopEnd; i++)
                        {
                            int n = frontier[i];
                            ref var nNode = ref searchState.GetNodeByIndex(n);
                            if (nNode.EdgesPerLevel.Count <= ℓ) continue;
                            ref var nEdges = ref nNode.EdgesPerLevel[ℓ];
                            for (int e = 0; e < nEdges.Count && frontier.Count < frontierCap; e++)
                            {
                                int candIdx = searchState.GetNodeIndexById(nEdges[e]);
                                if (visited.Add(candIdx))
                                    frontier.Add(candIdx);
                            }
                        }
                        hopStart = hopEnd;
                    }
                    // Project to next level: keep only nodes that exist at ℓ-1.
                    if (ℓ > targetLevel)
                    {
                        var nextFrontier = new List<int>(frontier.Count);
                        foreach (int n in frontier)
                        {
                            ref var nNode = ref searchState.GetNodeByIndex(n);
                            if (nNode.EdgesPerLevel.Count > ℓ - 1)
                                nextFrontier.Add(n);
                        }
                        frontier = nextFrontier;
                    }
                }
                // At targetLevel — visited is the layered-pool augmentation.
                topdownPool = visited;
            }
        }

        foreach (var kv in l0NodeVisits)
        {
            int u = kv.Key;
            var visits = kv.Value;

            // Count current uncovered for this node.
            int uncoveredCount = 0;
            for (int i = 0; i < visits.Count; i++)
                if (visits[i].covered == false) uncoveredCount++;
            if (uncoveredCount == 0)
                continue;
            nodesWithUncovered++;

            // Collect edge indices and radial ceiling.
            poolHash.Clear();
            twoHopFrontier.Clear();
            bestVCoverage.Clear();
            poolHash.Add(u);

            ref var uNode = ref searchState.GetNodeByIndex(u);
            if (uNode.EdgesPerLevel.Count <= targetLevel)
                continue;
            ref var uEdges = ref uNode.EdgesPerLevel[targetLevel];
            float maxEdgeDist = 0f;
            var uEdgeIdx = new List<int>(uEdges.Count);
            for (int i = 0; i < uEdges.Count; i++)
            {
                int sIdx = searchState.GetNodeIndexById(uEdges[i]);
                uEdgeIdx.Add(sIdx);
                poolHash.Add(sIdx);
                float dus = searchState.Distance(ReadOnlySpan<byte>.Empty, u, sIdx);
                if (dus > maxEdgeDist) maxEdgeDist = dus;
            }
            float radialCeiling = betaL0 * maxEdgeDist;

            // 2-hop expansion — collect candidates into the frontier and score inline
            // (no local function because ReadOnlySpan<byte> can't be captured).
            // A swap REMOVES s ∈ N(u) and ADDS v ∉ N(u), so existing edges are not
            // candidates and we skip them via the poolHash membership check.
            for (int i = 0; i < uEdgeIdx.Count; i++)
            {
                int v = uEdgeIdx[i];
                ref var vNode = ref searchState.GetNodeByIndex(v);
                if (vNode.EdgesPerLevel.Count <= targetLevel)
                    continue;
                ref var vEdges = ref vNode.EdgesPerLevel[targetLevel];
                for (int j = 0; j < vEdges.Count; j++)
                {
                    int w = searchState.GetNodeIndexById(vEdges[j]);
                    if (poolHash.Add(w) == false) continue;
                    twoHopFrontier.Add(w);
                    if (hopStats) pool2++;
                    if (w == u) continue;
                    float duw = searchState.Distance(ReadOnlySpan<byte>.Empty, u, w);
                    if (duw > radialCeiling) continue;
                    if (hopStats) feas2++;
                    int covCount = 0;
                    for (int vi = 0; vi < visits.Count; vi++)
                    {
                        if (visits[vi].covered) continue;
                        if (useHoeffding && (vi & 1) != 0) continue; // §7 train set = even-index visits
                        var query = queriesBlob.Slice(visits[vi].qIdx * vectorSizeBytes, vectorSizeBytes);
                        float dvq = searchState.Distance(query, -1, w);
                        if (dvq <= rho * visits[vi].dUQ)
                            covCount++;
                    }
                    if (covCount > 0)
                    {
                        bestVCoverage[w] = covCount;
                        if (hopStats) cov2++;
                    }
                }
            }
            // 3-hop expansion. When RAVEN_HNSW_L0_4HOP=1 we also collect the
            // 3-hop frontier so a subsequent 4-hop sweep can run. The 4-hop
            // gate targets the ~66% of uncovered nodes that have no candidate
            // within 3 hops + radial ceiling (JOURNAL 2026-05-17 structural
            // ceiling analysis). β must be lifted (≥1.5) to give 4-hop edges
            // room inside the radial bound.
            bool fourHop = Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_4HOP") == "1";
            var threeHopFrontier = fourHop ? new List<int>(512) : null;
            for (int i = 0; i < twoHopFrontier.Count; i++)
            {
                int w = twoHopFrontier[i];
                ref var wNode = ref searchState.GetNodeByIndex(w);
                if (wNode.EdgesPerLevel.Count <= targetLevel)
                    continue;
                ref var wEdges = ref wNode.EdgesPerLevel[targetLevel];
                for (int k = 0; k < wEdges.Count; k++)
                {
                    int x = searchState.GetNodeIndexById(wEdges[k]);
                    if (poolHash.Add(x) == false) continue;
                    threeHopFrontier?.Add(x);
                    if (hopStats) pool3++;
                    if (x == u) continue;
                    float dux = searchState.Distance(ReadOnlySpan<byte>.Empty, u, x);
                    if (dux > radialCeiling) continue;
                    if (hopStats) feas3++;
                    int covCount = 0;
                    for (int vi = 0; vi < visits.Count; vi++)
                    {
                        if (visits[vi].covered) continue;
                        if (useHoeffding && (vi & 1) != 0) continue; // §7 train set = even-index visits
                        var query = queriesBlob.Slice(visits[vi].qIdx * vectorSizeBytes, vectorSizeBytes);
                        float dxq = searchState.Distance(query, -1, x);
                        if (dxq <= rho * visits[vi].dUQ)
                            covCount++;
                    }
                    if (covCount > 0)
                    {
                        bestVCoverage[x] = covCount;
                        if (hopStats) cov3++;
                    }
                }
            }
            // 4-hop expansion (gated). Same shape as 3-hop but anchored on
            // threeHopFrontier. The radial-ceiling filter discards most
            // 4-hop candidates unless β is relaxed; that's the whole point —
            // only candidates that survive the ceiling can be a §5-feasible
            // swap target.
            if (threeHopFrontier != null)
            {
                for (int i = 0; i < threeHopFrontier.Count; i++)
                {
                    int w = threeHopFrontier[i];
                    ref var wNode = ref searchState.GetNodeByIndex(w);
                    if (wNode.EdgesPerLevel.Count <= targetLevel)
                        continue;
                    ref var wEdges = ref wNode.EdgesPerLevel[targetLevel];
                    for (int k = 0; k < wEdges.Count; k++)
                    {
                        int y = searchState.GetNodeIndexById(wEdges[k]);
                        if (poolHash.Add(y) == false) continue;
                        if (hopStats) pool4++;
                        if (y == u) continue;
                        float duy = searchState.Distance(ReadOnlySpan<byte>.Empty, u, y);
                        if (duy > radialCeiling) continue;
                        if (hopStats) feas4++;
                        int covCount = 0;
                        for (int vi = 0; vi < visits.Count; vi++)
                        {
                            if (visits[vi].covered) continue;
                            if (useHoeffding && (vi & 1) != 0) continue;
                            var query = queriesBlob.Slice(visits[vi].qIdx * vectorSizeBytes, vectorSizeBytes);
                            float dyq = searchState.Distance(query, -1, y);
                            if (dyq <= rho * visits[vi].dUQ)
                                covCount++;
                        }
                        if (covCount > 0)
                        {
                            bestVCoverage[y] = covCount;
                            if (hopStats) cov4++;
                        }
                    }
                }
            }

            // FRAMEWORK §28 — top-down layered N-hop pool (RAVEN_HNSW_L0_TOPDOWN=1).
            // Built once at top of simulator (see topdownPool above); used here
            // as augmentation to bestVCoverage. The pool is the set of nodes
            // visited during a top-down walk from EntryPointId where 4 hops
            // are taken at each level before projecting to the next level. At
            // targetLevel, this set IS the candidate pool — independent of
            // u's L0 topology. Targets the failure mode where production
            // search lands at u but u's local 3-hop pool can't supply a fix
            // because the right v is in a different HNSW cluster.
            if (topdownPool != null)
            {
                foreach (int candIdx in topdownPool)
                {
                    if (poolHash.Add(candIdx) == false) continue;
                    if (candIdx == u) continue;
                    if (hopStats) poolTD++;
                    float duv = searchState.Distance(ReadOnlySpan<byte>.Empty, u, candIdx);
                    if (duv > radialCeiling) continue;
                    if (hopStats) feasTD++;
                    int covCount = 0;
                    for (int wi = 0; wi < visits.Count; wi++)
                    {
                        if (visits[wi].covered) continue;
                        if (useHoeffding && (wi & 1) != 0) continue;
                        var wq = queriesBlob.Slice(visits[wi].qIdx * vectorSizeBytes, vectorSizeBytes);
                        float dcq = searchState.Distance(wq, -1, candIdx);
                        if (dcq <= rho * visits[wi].dUQ) covCount++;
                    }
                    if (covCount > 0)
                    {
                        bestVCoverage[candIdx] = covCount;
                        if (hopStats) covTD++;
                    }
                }
            }

            // Pick v* with max coverage. If gain > 0, apply the swap.
            int bestV = -1;
            int bestCov = 0;
            // §15.7 margin / log-ratio tie-break: among candidates with the SAME first-witness
            // coverage count (the §15.2 K=1 objective), prefer the one with the lowest mean
            // log R_v(u, q) over uncovered queries — i.e. the candidate that comes closest
            // overall, not just the one that crosses the λ threshold. Framework explicitly
            // warns this must be SECONDARY (binary coverage stays primary) to avoid the
            // §15.2 K=2 mistake. Enable via RAVEN_HNSW_L0_MARGIN=1.
            bool useMargin = Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_MARGIN") == "1";
            double bestLogR = double.MaxValue;
            foreach (var pair in bestVCoverage)
            {
                if (pair.Value > bestCov)
                {
                    bestCov = pair.Value;
                    bestV = pair.Key;
                    if (useMargin)
                        bestLogR = ComputeMeanLogR(searchState, queriesBlob, vectorSizeBytes, visits, pair.Key, useHoeffding);
                }
                else if (useMargin && pair.Value == bestCov)
                {
                    double logR = ComputeMeanLogR(searchState, queriesBlob, vectorSizeBytes, visits, pair.Key, useHoeffding);
                    if (logR < bestLogR)
                    {
                        bestLogR = logR;
                        bestV = pair.Key;
                    }
                }
            }

            if (bestV != -1 && bestCov > 0)
            {
                nodesRepaired++;
                totalSwaps++;
                int regressionFromRemoval = 0;
                int worstS = -1;
                // Protected prefix: framework §5 default is M/2. Override via
                // RAVEN_HNSW_L0_PROTECT=N to test stricter protection (e.g., M-2)
                // which leaves fewer eviction candidates and preserves more original
                // edges, trading repair magnitude for less low-ef recall risk.
                int protectedCount;
                if (protectedOverride is { } pOv && pOv > 0)
                    protectedCount = Math.Min(uEdgeIdx.Count - 1, pOv);
                else if (int.TryParse(Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_PROTECT"), out var pEnv) && pEnv > 0)
                    protectedCount = Math.Min(uEdgeIdx.Count - 1, pEnv);
                else
                    protectedCount = Math.Max(1, uEdgeIdx.Count / 2);
                // Sort edges by distance ascending so first half is protected.
                var edgeDists = new (int idx, float dus)[uEdgeIdx.Count];
                for (int i = 0; i < uEdgeIdx.Count; i++)
                {
                    edgeDists[i] = (uEdgeIdx[i], searchState.Distance(ReadOnlySpan<byte>.Empty, u, uEdgeIdx[i]));
                }
                Array.Sort(edgeDists, (a, b) => a.dus.CompareTo(b.dus));

                if (Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_EVICT") == "lci")
                {
                    // LEAST-COVERAGE-IMPACT eviction: among the non-protected suffix, pick the
                    // candidate s whose removal loses the FEWEST currently-covered queries
                    // (treating bestV as a substitute witness). Falls back to farthest-non-
                    // protected when no s is a unique witness on the sample.
                    //
                    // Optional hub-aware penalty (RAVEN_HNSW_L0_HUB_AWARE=1): add the descent
                    // visit-count of s to its eviction score. The intuition is that nodes
                    // visited frequently during descent are routing hubs whose removal hurts
                    // low-ef search even when their immediate witness coverage is replaceable.
                    // Hub count comes from the same per-node trace reservoir (§3) we use for
                    // the Hoeffding gate, so no extra bookkeeping is needed.
                    bool hubAware = Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_HUB_AWARE") == "1";
                    long minScore = long.MaxValue;
                    for (int ei = protectedCount; ei < edgeDists.Length; ei++)
                    {
                        int cand = edgeDists[ei].idx;
                        int loss = 0;
                        for (int vi = 0; vi < visits.Count; vi++)
                        {
                            if (!visits[vi].covered) continue;
                            if (useHoeffding && (vi & 1) != 0) continue; // §7 train-only for eviction scoring
                            var qbuf = queriesBlob.Slice(visits[vi].qIdx * vectorSizeBytes, vectorSizeBytes);
                            float dCand = searchState.Distance(qbuf, -1, cand);
                            if (dCand > rho * visits[vi].dUQ) continue;
                            // cand is a witness; check whether ANY other current edge or bestV covers q.
                            bool otherWitness = false;
                            float dVQ = searchState.Distance(qbuf, -1, bestV);
                            if (dVQ <= rho * visits[vi].dUQ) otherWitness = true;
                            if (!otherWitness)
                            {
                                for (int j = 0; j < uEdgeIdx.Count; j++)
                                {
                                    if (uEdgeIdx[j] == cand) continue;
                                    float dOther = searchState.Distance(qbuf, -1, uEdgeIdx[j]);
                                    if (dOther <= rho * visits[vi].dUQ) { otherWitness = true; break; }
                                }
                            }
                            if (!otherWitness) loss++;
                        }
                        // Hub penalty: how often was `cand` itself a descent visit?
                        // Reservoir-based, so capped at reservoirCapPerNode in worst case.
                        long hub = 0;
                        if (hubAware && l0NodeVisits.TryGetValue(cand, out var candVisits))
                            hub = candVisits.Count;
                        long score = (long)loss + hub;
                        if (score < minScore || (score == minScore && (worstS == -1 || edgeDists[ei].dus > searchState.Distance(ReadOnlySpan<byte>.Empty, u, worstS))))
                        {
                            minScore = score;
                            worstS = cand;
                        }
                    }
                }
                else
                {
                    // Default: farthest-non-protected (the original §5 prescription).
                    float worstDist = -1f;
                    for (int i = protectedCount; i < edgeDists.Length; i++)
                    {
                        if (edgeDists[i].dus > worstDist)
                        {
                            worstDist = edgeDists[i].dus;
                            worstS = edgeDists[i].idx;
                        }
                    }
                }
                // If worstS was a sole witness for any covered query, removing it un-covers.
                // Check covered visits to see if worstS was their witness.
                if (worstS != -1)
                {
                    for (int i = 0; i < visits.Count; i++)
                    {
                        if (visits[i].covered == false) continue;
                        var query = queriesBlob.Slice(visits[i].qIdx * vectorSizeBytes, vectorSizeBytes);
                        float dWorstQ = searchState.Distance(query, -1, worstS);
                        if (dWorstQ <= rho * visits[i].dUQ)
                        {
                            // worstS is a witness. Was it the SOLE witness? Check if any
                            // other edge covers q.
                            bool otherWitness = false;
                            for (int j = 0; j < uEdgeIdx.Count; j++)
                            {
                                if (uEdgeIdx[j] == worstS) continue;
                                float dOther = searchState.Distance(query, -1, uEdgeIdx[j]);
                                if (dOther <= rho * visits[i].dUQ)
                                {
                                    otherWitness = true;
                                    break;
                                }
                            }
                            // Also v (added) may cover q.
                            float dVQ = searchState.Distance(query, -1, bestV);
                            if (dVQ <= rho * visits[i].dUQ) otherWitness = true;
                            if (otherWitness == false)
                                regressionFromRemoval++;
                        }
                    }
                }

                // §5.3 spread invariant: for HNSW α-prune (χ=1) reject v if any retained
                // edge a has D(v, a) < D(u, v). Existing N(u) is already spread-feasible
                // by construction; we only need to check v vs S \ {worstS}.
                bool spreadOk = true;
                if (useSpread && worstS != -1 && bestV != -1)
                {
                    float dUV = searchState.Distance(ReadOnlySpan<byte>.Empty, u, bestV);
                    for (int i = 0; i < uEdgeIdx.Count; i++)
                    {
                        if (uEdgeIdx[i] == worstS) continue;
                        float dVA = searchState.Distance(ReadOnlySpan<byte>.Empty, bestV, uEdgeIdx[i]);
                        if (dVA < dUV) { spreadOk = false; break; }
                    }
                }

                // §7 Hoeffding validation gate: Δ̂_val > sqrt(2 log(B/δ)/m) + τ, where
                // B = candidates with positive train coverage (union-bound size), m = |Q_val|.
                // Loss ℓ_S(u,q) = min(1, [R_S(u,q) − λ]+ / (1 − λ)) ∈ [0, 1] per §0.
                proposedSwaps++;
                bool hoeffOk = true;
                if (useHoeffding && worstS != -1)
                {
                    int B = Math.Max(1, bestVCoverage.Count);
                    int m = 0;
                    double sumDelta = 0.0;
                    for (int vi = 0; vi < visits.Count; vi++)
                    {
                        if ((vi & 1) == 0) continue; // §7 val set = odd-index visits
                        var qbuf = queriesBlob.Slice(visits[vi].qIdx * vectorSizeBytes, vectorSizeBytes);
                        float dUQ = visits[vi].dUQ;
                        if (dUQ <= 0f) continue;
                        // R_S(u,q) = min over s ∈ S ∪ {u} of D(s,q) / D(u,q). Numerator is 1
                        // (u itself), and each s contributes D(s,q)/D(u,q).
                        float rS = 1f;
                        for (int j = 0; j < uEdgeIdx.Count; j++)
                        {
                            float ds = searchState.Distance(qbuf, -1, uEdgeIdx[j]);
                            float r = ds / dUQ;
                            if (r < rS) rS = r;
                        }
                        // R_{S'} replaces worstS with bestV.
                        float rSp = 1f;
                        for (int j = 0; j < uEdgeIdx.Count; j++)
                        {
                            if (uEdgeIdx[j] == worstS) continue;
                            float ds = searchState.Distance(qbuf, -1, uEdgeIdx[j]);
                            float r = ds / dUQ;
                            if (r < rSp) rSp = r;
                        }
                        float dVQ = searchState.Distance(qbuf, -1, bestV);
                        float rV = dVQ / dUQ;
                        if (rV < rSp) rSp = rV;

                        float lossS = rS <= lambda ? 0f : Math.Min(1f, (rS - lambda) / (1f - lambda));
                        float lossSp = rSp <= lambda ? 0f : Math.Min(1f, (rSp - lambda) / (1f - lambda));
                        sumDelta += lossS - lossSp;
                        m++;
                    }
                    if (m > 0)
                    {
                        double deltaHat = sumDelta / m;
                        double threshold = Math.Sqrt(2.0 * Math.Log(B / (double)delta) / m) + tau;
                        if (deltaHat <= threshold) hoeffOk = false;
                    }
                    else
                    {
                        hoeffOk = false; // no val samples → conservative reject
                    }
                }

                if (!spreadOk)
                {
                    rejectedBySpread++;
                    nodesRepaired--; totalSwaps--; // revert eager increments
                    l0UncoveredPost += uncoveredCount;
                    continue;
                }
                if (!hoeffOk)
                {
                    rejectedByHoeffding++;
                    nodesRepaired--; totalSwaps--;
                    l0UncoveredPost += uncoveredCount;
                    continue;
                }

                // Net change in uncovered for this node:
                //   −bestCov (newly covered) + regressionFromRemoval (newly uncovered)
                int netChange = -bestCov + regressionFromRemoval;
                int postForThisNode = uncoveredCount + netChange;
                if (postForThisNode < 0) postForThisNode = 0; // defensive
                l0UncoveredPost += postForThisNode;

                // In-memory mutation: remove worstS's NodeId from u's L0 edges, append bestV's.
                // Voron persistence happens later in Commit() via PersistNode; if applyMutations
                // is true and the caller does not persist (typical for tests), the mutation is
                // discarded on tx dispose. Per framework Theorem 1, the caller MUST gate this
                // on held-out recall measurement before any persist.
                if (applyMutations && worstS != -1)
                {
                    long uNodeId = searchState.GetNodeByIndex(u).NodeId;
                    long worstSNodeId = searchState.GetNodeByIndex(worstS).NodeId;
                    long bestVNodeId = searchState.GetNodeByIndex(bestV).NodeId;
                    // re-fetch u's ref AFTER any reallocation triggered by the two calls above
                    ref var uNodeMut = ref searchState.GetNodeByIndex(u);
                    ref var uEdgesMut = ref uNodeMut.EdgesPerLevel[targetLevel];
                    int origCount = uEdgesMut.Count;
                    var snapshot = ArrayPool<long>.Shared.Rent(origCount);
                    for (int i = 0; i < origCount; i++) snapshot[i] = uEdgesMut[i];
                    uEdgesMut.ResetAndEnsureCapacity(searchState.Llt.Allocator, origCount);
                    bool removed = false;
                    for (int i = 0; i < origCount; i++)
                    {
                        if (!removed && snapshot[i] == worstSNodeId) { removed = true; continue; }
                        uEdgesMut.AddUnsafe(snapshot[i]);
                    }
                    uEdgesMut.AddUnsafe(bestVNodeId);
                    ArrayPool<long>.Shared.Return(snapshot);

                    // FRAMEWORK Gate 4 — bidirectional repair (RAVEN_HNSW_L0_BIDIRECTIONAL=1).
                    // The forward edge u→bestV makes bestV reachable from u; the reverse edge
                    // bestV→u makes u reachable from bestV (raises u's IN-degree, which lifts
                    // η_front when u is a tube node for queries that route through bestV).
                    // Eviction on the v side: keep edges ≤ M by dropping bestV's farthest
                    // existing edge in cosine distance — symmetric to the LCI farthest-non-
                    // protected default. Skip if u is already in bestV's edges.
                    bool bidir = Environment.GetEnvironmentVariable("RAVEN_HNSW_L0_BIDIRECTIONAL") == "1";
                    if (bidir)
                    {
                        ref var vNodeMut = ref searchState.GetNodeByIndex(bestV);
                        if (vNodeMut.EdgesPerLevel.Count > targetLevel)
                        {
                            ref var vEdgesMut = ref vNodeMut.EdgesPerLevel[targetLevel];
                            bool alreadyHasU = false;
                            for (int i = 0; i < vEdgesMut.Count; i++)
                            {
                                if (vEdgesMut[i] == uNodeId) { alreadyHasU = true; break; }
                            }
                            if (alreadyHasU == false)
                            {
                                int M = searchState.Options.NumberOfEdges;
                                if (vEdgesMut.Count < M)
                                {
                                    // Free slot — just append.
                                    vEdgesMut.Add(searchState.Llt.Allocator, uNodeId);
                                }
                                else
                                {
                                    // Evict bestV's farthest NON-PROTECTED existing edge to make
                                    // room for u. Protection mirrors u-side default: keep bestV's
                                    // M/2 closest edges untouched (framework §5). Without this,
                                    // bidirectional repair can destructively evict bestV's
                                    // critical near-neighbour edges and break Theorem-1 safety.
                                    int vCount = vEdgesMut.Count;
                                    var vEdgeDists = new (int idx, float dvn)[vCount];
                                    for (int i = 0; i < vCount; i++)
                                    {
                                        int nIdx = searchState.GetNodeIndexById(vEdgesMut[i]);
                                        vEdgeDists[i] = (i, searchState.Distance(ReadOnlySpan<byte>.Empty, bestV, nIdx));
                                    }
                                    // Rank by ascending distance to determine protected prefix.
                                    var vRanked = new int[vCount];
                                    for (int i = 0; i < vCount; i++) vRanked[i] = i;
                                    System.Array.Sort(vRanked, (a, b) => vEdgeDists[a].dvn.CompareTo(vEdgeDists[b].dvn));
                                    int vProtectedCount = System.Math.Max(1, vCount / 2);
                                    var vProtected = new System.Collections.Generic.HashSet<int>();
                                    for (int i = 0; i < vProtectedCount; i++) vProtected.Add(vRanked[i]);
                                    int worstIdx = -1;
                                    float worstDist = -1f;
                                    for (int i = 0; i < vCount; i++)
                                    {
                                        if (vProtected.Contains(i)) continue;
                                        if (vEdgeDists[i].dvn > worstDist)
                                        {
                                            worstDist = vEdgeDists[i].dvn;
                                            worstIdx = i;
                                        }
                                    }
                                    // Only displace if u is closer to bestV than that worst
                                    // non-protected edge — strict improvement.
                                    float duv = searchState.Distance(ReadOnlySpan<byte>.Empty, bestV, u);
                                    if (worstIdx >= 0 && duv < worstDist)
                                    {
                                        var vSnap = ArrayPool<long>.Shared.Rent(vCount);
                                        for (int i = 0; i < vCount; i++) vSnap[i] = vEdgesMut[i];
                                        vEdgesMut.ResetAndEnsureCapacity(searchState.Llt.Allocator, vCount);
                                        for (int i = 0; i < vCount; i++)
                                        {
                                            if (i == worstIdx) continue;
                                            vEdgesMut.AddUnsafe(vSnap[i]);
                                        }
                                        vEdgesMut.AddUnsafe(uNodeId);
                                        ArrayPool<long>.Shared.Return(vSnap);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                // No swap applied — uncovered count unchanged.
                l0UncoveredPost += uncoveredCount;
            }
        }

        if (hopStats)
        {
            // Per-hop candidate accounting. Surfaces whether 4-hop adds NEW
            // candidates (pool4 > 0), whether they survive the radial ceiling
            // (feas4 > 0), and whether any cover at least one uncovered visit
            // (cov4 > 0). Indispensable for diagnosing null-result Phase B
            // experiments — without this you can't tell whether the lever
            // failed at pool widening, ceiling, or coverage.
            Console.WriteLine($"[L0 hopstats ρ={rho:F2} β={betaL0:F2}] " +
                $"2-hop: pool={pool2} feas={feas2} cov={cov2} | " +
                $"3-hop: pool={pool3} feas={feas3} cov={cov3} | " +
                $"4-hop: pool={pool4} feas={feas4} cov={cov4} | " +
                $"topdown: pool={poolTD} feas={feasTD} cov={covTD}");
        }

        return new L0OneSwapSimulationReport(
            rho, betaL0, queryCount, l0Visits, l0UncoveredPre, l0UncoveredPost,
            nodesVisitedAtL0, nodesWithUncovered, nodesRepaired, totalSwaps,
            proposedSwaps, rejectedByHoeffding, rejectedBySpread);
    }

    public static L0OneSwapSimulationReport SimulateL0OneSwapRepair(LowLevelTransaction llt, string name, ReadOnlySpan<byte> queriesBlob, int queryCount, float rho, float betaL0, bool applyMutations = false, SearchState reuseSearchState = null, int targetLevel = 0, int? protectedOverride = null, int reservoirCapPerNode = 0)
    {
        using (Slice.From(llt.Allocator, name, out var slice))
            return SimulateL0OneSwapRepair(llt, slice, queriesBlob, queryCount, rho, betaL0, applyMutations, reuseSearchState,
                reservoirCapPerNode: reservoirCapPerNode, targetLevel: targetLevel, protectedOverride: protectedOverride);
    }

    /// <summary>
    /// FRAMEWORK §15.3 upper-layer one-swap repair. Thin wrapper around the unified
    /// simulator with framework-prescribed defaults for ℓ ≥ 1: r=2 protected, β_ℓ ∈ [1.5, 3]
    /// (typical 2.0), λ=0.9025. Per §11 upper-layer work is bounded by N/(M-1), so cost is
    /// inherently small. Recommend running BEFORE L0 repair per §17 deployment order.
    /// </summary>
    // §15.7 helper: mean log R_v(u, q) over u's train-set visits. Lower is better
    // (candidate consistently closer to q). Used only as a tie-break among candidates
    // with equal binary-coverage count, never as the primary objective.
    private static double ComputeMeanLogR(
        SearchState ss, ReadOnlySpan<byte> queriesBlob, int vectorSizeBytes,
        List<(int qIdx, float dUQ, bool covered)> visits, int candIdx, bool trainOnly)
    {
        double sum = 0;
        int m = 0;
        for (int i = 0; i < visits.Count; i++)
        {
            if (trainOnly && (i & 1) != 0) continue;
            if (visits[i].dUQ <= 0f) continue;
            var qbuf = queriesBlob.Slice(visits[i].qIdx * vectorSizeBytes, vectorSizeBytes);
            float d = ss.Distance(qbuf, -1, candIdx);
            double r = Math.Max(1e-9, d / visits[i].dUQ);
            sum += Math.Log(r);
            m++;
        }
        return m == 0 ? 0 : sum / m;
    }

    public static L0OneSwapSimulationReport SimulateUpperLayerOneSwapRepair(
        LowLevelTransaction llt, Slice name, ReadOnlySpan<byte> queriesBlob, int queryCount,
        int level, float rho = 0.95f, float betaL = 2.0f,
        bool applyMutations = false, SearchState reuseSearchState = null,
        int reservoirCapPerNode = 0)
    {
        if (level < 1)
            throw new ArgumentOutOfRangeException(nameof(level), level, "upper-layer repair requires level >= 1; use SimulateL0OneSwapRepair for L0");
        return SimulateL0OneSwapRepair(llt, name, queriesBlob, queryCount, rho, betaL,
            applyMutations, reuseSearchState,
            reservoirCapPerNode: reservoirCapPerNode, targetLevel: level, protectedOverride: 2);
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
