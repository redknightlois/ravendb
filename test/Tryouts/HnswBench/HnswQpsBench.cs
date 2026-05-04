using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Numerics.Tensors;
using System.Reflection;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.Graphs;
using Voron.Impl;

namespace Tryouts.HnswBench;

/// <summary>
/// Portable HNSW search QPS bench. Builds (or reuses) a SIFT1M Voron index and times
/// <see cref="Hnsw.ApproximateNearest"/> over a fixed query set. Uses only the plain
/// <see cref="Hnsw.SearchState"/> path so the source compiles unchanged on every graduated
/// HNSW branch (v7.2 onward); it does not exercise <c>HnswIndexCache</c>, so any lift that
/// only engages through the Corax wiring (e.g. leaf-inclusive cache prepopulation) is not
/// visible here.
///
/// Run via <c>RAVEN_BENCH_HNSW_QPS=1 dotnet run -c Release --project test/Tryouts</c>.
/// Knobs:
///   RAVEN_BENCH_DB_PATH     reuse a prebuilt Voron (skips ~10 min import)
///   RAVEN_BENCH_NODES       corpus size, default 1_000_000 (capped at SIFT1M = 1M)
///   RAVEN_BENCH_QUERIES     query count, default 5_000
///   RAVEN_BENCH_EF          search ef (numberOfCandidates), default 64
///   RAVEN_BENCH_TOPK        top-K result, default 10
///   RAVEN_BENCH_REPS        timed pass repetitions, default 3 (median reported)
///   RAVEN_BENCH_RECALL      compute recall vs SIFT1M native ground truth (requires N=1M)
///   RAVEN_BENCH_USE_CACHE   wire HnswIndexCache.WarmFromScratch and pass it to SearchState
///                           (mirrors the production Corax path); default off
///   RAVEN_BENCH_INCLUDE_LEAVES  when USE_CACHE=1 and the WarmFromScratch overload supports it
///                               (RavenDB-26537+), admit level-0 leaves into the cache
/// </summary>
internal static class HnswQpsBench
{
    public static void Run()
    {
        int nodeCount = ParseInt("RAVEN_BENCH_NODES", 1_000_000);
        if (nodeCount > 1_000_000)
            nodeCount = 1_000_000;
        int queryCount = ParseInt("RAVEN_BENCH_QUERIES", 5_000);
        int ef = ParseInt("RAVEN_BENCH_EF", 64);
        int topK = ParseInt("RAVEN_BENCH_TOPK", 10);
        int reps = ParseInt("RAVEN_BENCH_REPS", 3);
        bool measureRecall = Environment.GetEnvironmentVariable("RAVEN_BENCH_RECALL") == "1";
        bool useCache = Environment.GetEnvironmentVariable("RAVEN_BENCH_USE_CACHE") == "1";
        bool includeLeaves = Environment.GetEnvironmentVariable("RAVEN_BENCH_INCLUDE_LEAVES") == "1";

        const int dim = Sift1MLoader.VectorDim;
        const int numberOfEdges = 12;
        const int efConstruction = 64;

        string reuseDb = Environment.GetEnvironmentVariable("RAVEN_BENCH_DB_PATH");
        bool reuseExisting = false;
        string dbPath;
        if (string.IsNullOrEmpty(reuseDb) == false)
        {
            dbPath = reuseDb;
            reuseExisting = Directory.Exists(dbPath) && Directory.EnumerateFileSystemEntries(dbPath).GetEnumerator().MoveNext();
            Directory.CreateDirectory(dbPath);
        }
        else
        {
            dbPath = Path.Combine(Path.GetTempPath(), "RavenDB.HnswQpsBench", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dbPath);
        }

        Console.WriteLine($"== HNSW search QPS bench (SIFT1M)");
        Console.WriteLine($"   dim={dim}, nodes={nodeCount:N0}, queries={queryCount:N0}, M={numberOfEdges}, ef={ef}, topK={topK}, reps={reps}");
        Console.WriteLine($"   db={dbPath}{(reuseExisting ? " (reused)" : "")}");
        Console.WriteLine($"   cache={(useCache ? (includeLeaves ? "on (leaf-inclusive)" : "on (promoted-only)") : "off")}");

        try
        {
            using var options = StorageEnvironmentOptions.ForPathForTests(dbPath);
            using var env = new StorageEnvironment(options);
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(ctx, "vec", out var fieldName);

            var swLoad = Stopwatch.StartNew();
            float[][] vectors = reuseExisting ? null : Sift1MLoader.LoadBase(nodeCount);
            float[][] queries = Sift1MLoader.LoadQueries(queryCount);
            swLoad.Stop();
            Console.WriteLine($"   loaded SIFT1M ({(vectors?.Length ?? 0):N0} base + {queries.Length:N0} query) in {swLoad.Elapsed.TotalSeconds:N1} s");

            if (reuseExisting == false)
                CreateAndImport(env, fieldName, dim, numberOfEdges, efConstruction, vectors, nodeCount);

            long[][] groundTruth = null;
            if (measureRecall)
            {
                if (nodeCount != 1_000_000)
                    throw new InvalidOperationException($"RAVEN_BENCH_RECALL=1 requires nodes=1,000,000 (got {nodeCount})");
                var sw = Stopwatch.StartNew();
                groundTruth = Sift1MLoader.LoadGroundTruth(queries.Length, topK);
                sw.Stop();
                Console.WriteLine($"   loaded native ground truth top{topK} in {sw.Elapsed.TotalSeconds:N2} s");
            }

            var beam = Math.Max(ef, topK);
            var matches = new long[beam];
            var distances = new float[beam];

            HnswIndexCache cache = null;
            if (useCache)
            {
                using var roTx = env.ReadTransaction();
                var llt = roTx.LowLevelTransaction;
                var sw = Stopwatch.StartNew();
                cache = WarmCache(llt, fieldName, nodeCount, includeLeaves);
                sw.Stop();
                if (cache is null)
                    throw new InvalidOperationException("HnswIndexCache.WarmFromScratch returned null (no graph at field?)");
                Console.WriteLine($"   warm cache in {sw.Elapsed.TotalMilliseconds:N0} ms");
            }

            // JIT warmup: one untimed pass so the search hot path is compiled before timing.
            _ = TimeQueries(env, fieldName, queries, beam, topK, matches, distances, cache, returnedPerQuery: null);

            var qps = new double[reps];
            long[][] returned = measureRecall ? new long[queries.Length][] : null;
            for (int rep = 0; rep < reps; rep++)
            {
                var pass = TimeQueries(env, fieldName, queries, beam, topK, matches, distances, cache,
                    rep == reps - 1 ? returned : null);
                qps[rep] = pass.Qps;
                Console.WriteLine($"   rep {rep}: {pass.ElapsedMs,8:N1} ms  {pass.Qps,8:N1} qps  avgCands={pass.AvgCandidates:N1}");
            }

            Array.Sort(qps);
            double median = qps[reps / 2];
            double recall = double.NaN;
            if (measureRecall)
                recall = ComputeRecall(returned, groundTruth, topK);

            Console.WriteLine($"== median {median:N1} qps" + (double.IsNaN(recall) ? "" : $"  recall@{topK}={recall * 100:N2}%"));
        }
        finally
        {
            if (string.IsNullOrEmpty(reuseDb))
            {
                try { Directory.Delete(dbPath, recursive: true); } catch { /* best effort */ }
            }
        }
    }

    private readonly record struct PassResult(double ElapsedMs, double Qps, double AvgCandidates);

    private static PassResult TimeQueries(
        StorageEnvironment env,
        Slice fieldName,
        float[][] queries,
        int beam,
        int topK,
        long[] matches,
        float[] distances,
        HnswIndexCache cache,
        long[][] returnedPerQuery)
    {
        using var roTx = env.ReadTransaction();
        var llt = roTx.LowLevelTransaction;
        using var searchState = cache is null
            ? new Hnsw.SearchState(llt, fieldName)
            : new Hnsw.SearchState(llt, fieldName, cache);

        // Single untimed query so SearchState lazy state is populated before the clock starts.
        RunOne(searchState, queries[0], beam, matches, distances);

        var sw = Stopwatch.StartNew();
        long totalCandidates = 0;
        for (int q = 0; q < queries.Length; q++)
        {
            using var retriever = Hnsw.ApproximateNearest(searchState, beam, ToBytes(queries[q]), 0f);
            int filled = retriever.Fill(matches, distances, null);
            totalCandidates += retriever.CandidatesProcessed;

            if (returnedPerQuery is not null)
            {
                var copy = new long[topK];
                Array.Copy(matches, copy, Math.Min(topK, filled));
                returnedPerQuery[q] = copy;
            }
        }
        sw.Stop();

        return new PassResult(
            sw.Elapsed.TotalMilliseconds,
            queries.Length / sw.Elapsed.TotalSeconds,
            totalCandidates / (double)queries.Length);
    }

    private static void RunOne(Hnsw.SearchState searchState, float[] query, int beam, long[] matches, float[] distances)
    {
        using var retriever = Hnsw.ApproximateNearest(searchState, beam, ToBytes(query), 0f);
        retriever.Fill(matches, distances, null);
    }

    private static byte[] ToBytes(float[] vec) => MemoryMarshal.AsBytes(vec.AsSpan()).ToArray();

    private static void CreateAndImport(
        StorageEnvironment env,
        Slice fieldName,
        int dim,
        int numberOfEdges,
        int efConstruction,
        float[][] vectors,
        int nodeCount)
    {
        using (var txw = env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, fieldName, dim * sizeof(float), numberOfEdges, efConstruction, VectorEmbeddingType.Single);
            txw.Commit();
        }

        var sw = Stopwatch.StartNew();
        const int batchSize = 5_000;
        for (int batchStart = 0; batchStart < nodeCount; batchStart += batchSize)
        {
            int end = Math.Min(batchStart + batchSize, nodeCount);
            using var txw = env.WriteTransaction();
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, fieldName))
            {
                registration.Random = new Random(454);
                for (int i = batchStart; i < end; i++)
                {
                    long entryId = ((long)i + 1) * 100;
                    registration.Register(entryId, MemoryMarshal.AsBytes(vectors[i].AsSpan()));
                }
                registration.Commit(default);
            }
            txw.Commit();
        }
        sw.Stop();
        Console.WriteLine($"   imported {nodeCount:N0} vectors in {sw.Elapsed.TotalSeconds:N1} s");
    }

    private static double ComputeRecall(long[][] returned, long[][] truth, int topK)
    {
        long hits = 0;
        long total = 0;
        var truthSet = new HashSet<long>(topK);
        for (int q = 0; q < returned.Length; q++)
        {
            truthSet.Clear();
            for (int k = 0; k < topK; k++)
                truthSet.Add(truth[q][k]);
            for (int k = 0; k < topK; k++)
            {
                if (truthSet.Contains(returned[q][k]))
                    hits++;
                total++;
            }
        }
        return total > 0 ? hits / (double)total : 0;
    }

    private static int ParseInt(string envVar, int fallback)
    {
        return int.TryParse(Environment.GetEnvironmentVariable(envVar), out var v) && v > 0 ? v : fallback;
    }

    /// <summary>
    /// Build a populated <see cref="HnswIndexCache"/> using whichever WarmFromScratch overload
    /// the loaded assembly exposes. The 4-arg form with <c>includeLeaves</c> is RavenDB-26537+;
    /// older branches only have the 3-arg promoted-only form. Reflection keeps the bench source
    /// portable across the graduated stack.
    /// </summary>
    private static HnswIndexCache WarmCache(LowLevelTransaction llt, Slice fieldName, int maxNodes, bool includeLeaves)
    {
        var t = typeof(HnswIndexCache);
        if (includeLeaves)
        {
            var withLeaves = t.GetMethod("WarmFromScratch", BindingFlags.Public | BindingFlags.Static,
                new[] { typeof(LowLevelTransaction), typeof(Slice), typeof(int), typeof(bool) });
            if (withLeaves is not null)
                return (HnswIndexCache)withLeaves.Invoke(null, new object[] { llt, fieldName, maxNodes, true });
            Console.WriteLine("   warning: RAVEN_BENCH_INCLUDE_LEAVES=1 but no 4-arg WarmFromScratch overload on this branch; falling back to promoted-only");
        }

        var threeArg = t.GetMethod("WarmFromScratch", BindingFlags.Public | BindingFlags.Static,
            new[] { typeof(LowLevelTransaction), typeof(Slice), typeof(int) });
        if (threeArg is null)
            throw new InvalidOperationException("HnswIndexCache.WarmFromScratch(llt, fieldName, maxNodes) overload not found");
        return (HnswIndexCache)threeArg.Invoke(null, new object[] { llt, fieldName, maxNodes });
    }
}
