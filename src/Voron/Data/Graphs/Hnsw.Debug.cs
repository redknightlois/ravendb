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
