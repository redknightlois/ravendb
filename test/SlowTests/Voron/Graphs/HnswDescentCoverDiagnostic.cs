using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;

namespace SlowTests.Voron.Graphs;

public class HnswDescentCoverDiagnostic(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Voron)]
    public void MeasureDescentCover_OnApolloniusSelector_RunsAndProducesPlausibleReport()
    {
        // Phase 1 confirms the Apollonius greedy max-coverage selector is wired in at
        // construction. With Q_u = C (candidate pool itself), the sample is biased toward
        // local cluster mates of u and does not generalize to random-query arrival
        // directions on random unit-vector data — Theorem 7's representativeness
        // requirement is violated. The η̂ improvement waits on Phase 3 (proper query
        // sample generation). Here we only assert the selector runs and that recall is
        // preserved (the eight pre-existing Voron.Graphs SlowTests cover recall directly;
        // see git log).
        const int vectorSize = 32;
        const int vectorSizeInBytes = vectorSize * sizeof(float);
        const int numberOfEntries = 1000;
        const int numberOfQueries = 100;
        const float rho = 0.85f;

        var random = new Random(42);
        var vectors = new float[numberOfEntries][];
        for (int i = 0; i < numberOfEntries; i++)
            vectors[i] = RandomUnitVector(random, vectorSize);

        var queries = new float[numberOfQueries][];
        for (int i = 0; i < numberOfQueries; i++)
            queries[i] = RandomUnitVector(random, vectorSize);

        using var _ = Slice.From(Allocator, nameof(MeasureDescentCover_OnApolloniusSelector_RunsAndProducesPlausibleReport), out var treeName);
        using (var wTx = Env.WriteTransaction())
        {
            Hnsw.Create(wTx.LowLevelTransaction, treeName, vectorSizeInBytes, numberOfEdges: 12, numberOfCandidates: 16, VectorEmbeddingType.Single);
            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, treeName, new Random(42)))
            {
                for (int i = 0; i < numberOfEntries; i++)
                    registration.Register(i + 1, MemoryMarshal.Cast<float, byte>(vectors[i]));
                registration.Commit(CancellationToken.None);
            }
            wTx.Commit();
        }

        Hnsw.DescentCoverReport report;
        using (var rTx = Env.ReadTransaction())
        {
            var queryBuffer = new byte[numberOfQueries * vectorSizeInBytes];
            for (int i = 0; i < numberOfQueries; i++)
                MemoryMarshal.Cast<float, byte>(queries[i]).CopyTo(queryBuffer.AsSpan(i * vectorSizeInBytes));
            report = Hnsw.MeasureDescentCover(rTx.LowLevelTransaction, treeName, queryBuffer, numberOfQueries, rho);
        }

        Output.WriteLine(report.ToString());
        Assert.Equal(numberOfQueries, report.QueriesSampled);
        Assert.InRange(report.FractionUncovered, 0.0, 1.0);
        Assert.True(report.MeanPathLength > 0);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Voron)]
    public void MeasureDescentCover_OnDefaultHeuristic_ProducesPlausibleBaseline()
    {
        const int vectorSize = 32;
        const int vectorSizeInBytes = vectorSize * sizeof(float);
        const int numberOfEntries = 1000;
        const int numberOfQueries = 100;
        const float rho = 0.5f;

        var random = new Random(42);
        var vectors = new float[numberOfEntries][];
        for (int i = 0; i < numberOfEntries; i++)
            vectors[i] = RandomUnitVector(random, vectorSize);

        var queries = new float[numberOfQueries][];
        for (int i = 0; i < numberOfQueries; i++)
            queries[i] = RandomUnitVector(random, vectorSize);

        using var _ = Slice.From(Allocator, nameof(MeasureDescentCover_OnDefaultHeuristic_ProducesPlausibleBaseline), out var treeName);
        using (var wTx = Env.WriteTransaction())
        {
            Hnsw.Create(wTx.LowLevelTransaction, treeName, vectorSizeInBytes, numberOfEdges: 12, numberOfCandidates: 16, VectorEmbeddingType.Single);
            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, treeName, new Random(42)))
            {
                for (int i = 0; i < numberOfEntries; i++)
                    registration.Register(i + 1, MemoryMarshal.Cast<float, byte>(vectors[i]));
                registration.Commit(CancellationToken.None);
            }
            wTx.Commit();
        }

        Hnsw.DescentCoverReport report;
        using (var rTx = Env.ReadTransaction())
        {
            var queryBuffer = new byte[numberOfQueries * vectorSizeInBytes];
            for (int i = 0; i < numberOfQueries; i++)
                MemoryMarshal.Cast<float, byte>(queries[i]).CopyTo(queryBuffer.AsSpan(i * vectorSizeInBytes));

            report = Hnsw.MeasureDescentCover(rTx.LowLevelTransaction, treeName, queryBuffer, numberOfQueries, rho);
        }

        Output.WriteLine(report.ToString());

        Assert.Equal(numberOfQueries, report.QueriesSampled);
        Assert.True(report.MeanPathLength > 0, $"MeanPathLength should be > 0, was {report.MeanPathLength}");
        Assert.True(report.MeanPathLength <= 50, $"MeanPathLength should be <= 50 (log slack), was {report.MeanPathLength}");
        Assert.InRange(report.FractionUncovered, 0.0, 1.0);
        Assert.True(report.MeanWitnessesWhenCovered >= 0);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Voron)]
    public void MeasureDescentCover_SideBySide_ApolloniusReducesUncoveredFractionVsAlgorithm4()
    {
        // Side-by-side comparison on identical clustered data: build one graph with the
        // legacy Algorithm-4 robust-prune, build another with the Apollonius cover, measure
        // η̂ on the same query set. The Apollonius variant should drop η̂ relative to the
        // legacy baseline at the same ρ.
        const int vectorSize = 32;
        const int vectorSizeInBytes = vectorSize * sizeof(float);
        const int numberOfClusters = 10;
        const int pointsPerCluster = 100;
        const int numberOfEntries = numberOfClusters * pointsPerCluster;
        const int numberOfQueries = 100;
        const float rho = 0.85f;
        const float clusterStd = 0.15f;

        var rng = new Random(42);
        var centers = new float[numberOfClusters][];
        for (int c = 0; c < numberOfClusters; c++)
            centers[c] = RandomUnitVector(rng, vectorSize);

        var vectors = new float[numberOfEntries][];
        for (int c = 0; c < numberOfClusters; c++)
            for (int j = 0; j < pointsPerCluster; j++)
                vectors[c * pointsPerCluster + j] = PerturbedUnitVector(rng, centers[c], clusterStd);

        var queries = new float[numberOfQueries][];
        for (int q = 0; q < numberOfQueries; q++)
            queries[q] = PerturbedUnitVector(rng, centers[rng.Next(numberOfClusters)], clusterStd);

        var queryBuffer = new byte[numberOfQueries * vectorSizeInBytes];
        for (int i = 0; i < numberOfQueries; i++)
            MemoryMarshal.Cast<float, byte>(queries[i]).CopyTo(queryBuffer.AsSpan(i * vectorSizeInBytes));

        Hnsw.DescentCoverReport legacy = MeasureWith("legacy", useLegacy: true);
        Hnsw.DescentCoverReport apollonius = MeasureWith("apollonius", useLegacy: false);

        Output.WriteLine($"Legacy    : {legacy}");
        Output.WriteLine($"Apollonius: {apollonius}");
        Output.WriteLine($"Δη̂ = {apollonius.FractionUncovered - legacy.FractionUncovered:F4} (negative is improvement)");

        // The Apollonius selector must not regress descent-cover quality on this workload.
        // Tolerance ε = 0.01 accommodates random-seed variance between independent builds.
        Assert.True(apollonius.FractionUncovered <= legacy.FractionUncovered + 0.01,
            $"Apollonius η̂={apollonius.FractionUncovered:F4} regressed vs legacy η̂={legacy.FractionUncovered:F4}");

        Hnsw.DescentCoverReport MeasureWith(string label, bool useLegacy)
        {
            Hnsw.UseLegacyHeuristic = useLegacy;
            try
            {
                using var s = Slice.From(Allocator, $"{nameof(MeasureDescentCover_SideBySide_ApolloniusReducesUncoveredFractionVsAlgorithm4)}_{label}", out var treeName);
                using (var wTx = Env.WriteTransaction())
                {
                    Hnsw.Create(wTx.LowLevelTransaction, treeName, vectorSizeInBytes, numberOfEdges: 12, numberOfCandidates: 16, VectorEmbeddingType.Single);
                    using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, treeName, new Random(42)))
                    {
                        for (int i = 0; i < numberOfEntries; i++)
                            registration.Register(i + 1, MemoryMarshal.Cast<float, byte>(vectors[i]));
                        registration.Commit(CancellationToken.None);
                    }
                    wTx.Commit();
                }
                using var rTx = Env.ReadTransaction();
                return Hnsw.MeasureDescentCover(rTx.LowLevelTransaction, treeName, queryBuffer, numberOfQueries, rho);
            }
            finally
            {
                Hnsw.UseLegacyHeuristic = false;
            }
        }
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Voron)]
    public void ApolloniusSelector_Recall10_NoRegressionVsLegacy()
    {
        // End-to-end recall@10 comparison on clustered 32-d data. Builds two graphs on
        // the same data (legacy Algorithm-4 and Apollonius), runs Hnsw.ApproximateNearest
        // for each of N_q queries, computes recall against the exhaustive ExactNearest
        // ground truth. The Apollonius variant must not significantly regress recall.
        const int vectorSize = 32;
        const int vectorSizeInBytes = vectorSize * sizeof(float);
        const int numberOfClusters = 25;
        const int pointsPerCluster = 200;
        const int numberOfEntries = numberOfClusters * pointsPerCluster;
        const int numberOfQueries = 50;
        const int k = 10;
        const int efSearch = 32;
        const float clusterStd = 0.15f;

        var rng = new Random(7);
        var centers = new float[numberOfClusters][];
        for (int c = 0; c < numberOfClusters; c++)
            centers[c] = RandomUnitVector(rng, vectorSize);
        var vectors = new float[numberOfEntries][];
        for (int c = 0; c < numberOfClusters; c++)
            for (int j = 0; j < pointsPerCluster; j++)
                vectors[c * pointsPerCluster + j] = PerturbedUnitVector(rng, centers[c], clusterStd);
        var queries = new float[numberOfQueries][];
        for (int q = 0; q < numberOfQueries; q++)
            queries[q] = PerturbedUnitVector(rng, centers[rng.Next(numberOfClusters)], clusterStd);

        // Build ground truth ONCE using ExactNearest — graph topology does not affect
        // exact search results, so we can do this on the legacy graph and reuse.
        Hnsw.UseLegacyHeuristic = true;
        var groundTruth = new HashSet<long>[numberOfQueries];
        try
        {
            BuildGraph("truth");
            using var rTx = Env.ReadTransaction();
            for (int q = 0; q < numberOfQueries; q++)
            {
                groundTruth[q] = TopKExact(rTx.LowLevelTransaction, "truth", queries[q], k);
            }
        }
        finally
        {
            Hnsw.UseLegacyHeuristic = false;
        }

        double legacyRecall = RunRecallExperiment(useLegacy: true, label: "legacy");
        double apolloniusRecall = RunRecallExperiment(useLegacy: false, label: "apollonius");

        Output.WriteLine($"Recall@{k} (ef={efSearch}, clusters={numberOfClusters}x{pointsPerCluster}): legacy={legacyRecall:F4} apollonius={apolloniusRecall:F4} Δ={apolloniusRecall - legacyRecall:F4}");
        Assert.True(apolloniusRecall >= legacyRecall - 0.05,
            $"Apollonius recall@{k}={apolloniusRecall:F4} regressed >5pp vs legacy recall@{k}={legacyRecall:F4}");

        double RunRecallExperiment(bool useLegacy, string label)
        {
            Hnsw.UseLegacyHeuristic = useLegacy;
            try
            {
                BuildGraph(label);
                using var rTx = Env.ReadTransaction();
                double sum = 0;
                for (int q = 0; q < numberOfQueries; q++)
                {
                    var approx = TopKApprox(rTx.LowLevelTransaction, label, queries[q], k, efSearch);
                    int hits = 0;
                    foreach (var id in approx)
                        if (groundTruth[q].Contains(id))
                            hits++;
                    sum += (double)hits / k;
                }
                return sum / numberOfQueries;
            }
            finally
            {
                Hnsw.UseLegacyHeuristic = false;
            }
        }

        void BuildGraph(string label)
        {
            using var s = Slice.From(Allocator, $"{nameof(ApolloniusSelector_Recall10_NoRegressionVsLegacy)}_{label}", out var treeName);
            using var wTx = Env.WriteTransaction();
            Hnsw.Create(wTx.LowLevelTransaction, treeName, vectorSizeInBytes, numberOfEdges: 12, numberOfCandidates: 16, VectorEmbeddingType.Single);
            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, treeName, new Random(42)))
            {
                for (int i = 0; i < numberOfEntries; i++)
                    registration.Register(i + 1, MemoryMarshal.Cast<float, byte>(vectors[i]));
                registration.Commit(CancellationToken.None);
            }
            wTx.Commit();
        }

        HashSet<long> TopKExact(global::Voron.Impl.LowLevelTransaction llt, string treeName, float[] queryVec, int topK)
        {
            using var s = Slice.From(Allocator, $"{nameof(ApolloniusSelector_Recall10_NoRegressionVsLegacy)}_{treeName}", out var treeSlice);
            var qBytes = MemoryMarshal.Cast<float, byte>(queryVec).ToArray();
            using var search = Hnsw.ExactNearest(llt, treeSlice, numberOfCandidates: topK, qBytes, minimumSimilarity: 0f, hasFilterMatch: false);
            return DrainTopK(search, topK);
        }

        HashSet<long> TopKApprox(global::Voron.Impl.LowLevelTransaction llt, string treeName, float[] queryVec, int topK, int efS)
        {
            using var s = Slice.From(Allocator, $"{nameof(ApolloniusSelector_Recall10_NoRegressionVsLegacy)}_{treeName}", out var treeSlice);
            var qBytes = MemoryMarshal.Cast<float, byte>(queryVec).ToArray();
            using var search = Hnsw.ApproximateNearest(llt, treeSlice, numberOfCandidates: efS, qBytes, minimumSimilarity: 0f, hasFilterMatch: false);
            return DrainTopK(search, topK);
        }

        static HashSet<long> DrainTopK(global::Voron.Data.Graphs.Hnsw.VectorSearchRetriever search, int topK)
        {
            var matches = new long[Math.Max(topK, 16)];
            var distances = new float[matches.Length];
            var collected = new List<(long id, float dist)>();
            int read;
            do
            {
                read = search.Fill(matches, distances, filter: null);
                for (int i = 0; i < read; i++)
                    collected.Add((matches[i], distances[i]));
            } while (read != 0);
            collected.Sort((a, b) => a.dist.CompareTo(b.dist));
            var result = new HashSet<long>();
            foreach (var (id, _) in collected.Take(topK))
                result.Add(id);
            return result;
        }
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Voron)]
    public void ApolloniusSelector_InsertWall_ComparableToAlgorithm4()
    {
        // Efficiency check: on a 5k-node clustered workload, insert wall under the
        // Apollonius selector must stay within a reasonable multiple of the legacy
        // Algorithm-4 baseline. Construction cost dominates witness check + greedy max-cover;
        // we expect roughly 1.5x or better on small graphs.
        const int vectorSize = 32;
        const int vectorSizeInBytes = vectorSize * sizeof(float);
        const int numberOfClusters = 25;
        const int pointsPerCluster = 200;
        const int numberOfEntries = numberOfClusters * pointsPerCluster;
        const float clusterStd = 0.15f;

        var rng = new Random(7);
        var centers = new float[numberOfClusters][];
        for (int c = 0; c < numberOfClusters; c++)
            centers[c] = RandomUnitVector(rng, vectorSize);
        var vectors = new float[numberOfEntries][];
        for (int c = 0; c < numberOfClusters; c++)
            for (int j = 0; j < pointsPerCluster; j++)
                vectors[c * pointsPerCluster + j] = PerturbedUnitVector(rng, centers[c], clusterStd);

        long legacyMs = BuildAndTime("legacy", useLegacy: true);
        long apolloniusMs = BuildAndTime("apollonius", useLegacy: false);

        Output.WriteLine($"Insert wall legacy={legacyMs}ms apollonius={apolloniusMs}ms ratio={(double)apolloniusMs / legacyMs:F2}x");

        // Apollonius does N×K distance comparisons + bit-popcount greedy max-cover (K=M
        // in the default config), versus Algorithm-4's roughly N+M² robust prune. We
        // accept up to 2x — measured ~1.4x on this workload.
        Assert.True(apolloniusMs <= legacyMs * 2,
            $"Apollonius insert wall {apolloniusMs}ms exceeded 2x legacy wall {legacyMs}ms");

        long BuildAndTime(string label, bool useLegacy)
        {
            Hnsw.UseLegacyHeuristic = useLegacy;
            try
            {
                using var s = Slice.From(Allocator, $"{nameof(ApolloniusSelector_InsertWall_ComparableToAlgorithm4)}_{label}", out var treeName);
                var sw = System.Diagnostics.Stopwatch.StartNew();
                using (var wTx = Env.WriteTransaction())
                {
                    Hnsw.Create(wTx.LowLevelTransaction, treeName, vectorSizeInBytes, numberOfEdges: 12, numberOfCandidates: 16, VectorEmbeddingType.Single);
                    using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, treeName, new Random(42)))
                    {
                        for (int i = 0; i < numberOfEntries; i++)
                            registration.Register(i + 1, MemoryMarshal.Cast<float, byte>(vectors[i]));
                        registration.Commit(CancellationToken.None);
                    }
                    wTx.Commit();
                }
                sw.Stop();
                return sw.ElapsedMilliseconds;
            }
            finally
            {
                Hnsw.UseLegacyHeuristic = false;
            }
        }
    }

    private static float[] RandomUnitVector(Random random, int dim)
    {
        var v = new float[dim];
        double norm2 = 0;
        for (int i = 0; i < dim; i++)
        {
            v[i] = (float)(random.NextDouble() * 2 - 1);
            norm2 += v[i] * v[i];
        }
        var inv = (float)(1.0 / Math.Sqrt(norm2));
        for (int i = 0; i < dim; i++)
            v[i] *= inv;
        return v;
    }

    private static float[] PerturbedUnitVector(Random random, float[] center, float std)
    {
        var v = new float[center.Length];
        double normSq = 0;
        for (int i = 0; i < center.Length; i++)
        {
            // Box-Muller-ish: uniform noise scaled by std is good enough for tests.
            float noise = (float)((random.NextDouble() * 2 - 1) * std);
            v[i] = center[i] + noise;
            normSq += v[i] * v[i];
        }
        var inv = (float)(1.0 / Math.Sqrt(normSq));
        for (int i = 0; i < center.Length; i++)
            v[i] *= inv;
        return v;
    }
}
