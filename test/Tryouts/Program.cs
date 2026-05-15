using System;
using System.Diagnostics;
using FastTests;
using SlowTests.Voron.Graphs;
using Tests.Infrastructure;
using Voron.Data.Graphs;

namespace Tryouts;

public static class Program
{
    public static int Main(string[] args)
    {
        Environment.SetEnvironmentVariable("RAVEN_HNSW_COVER_PROFILE", "1");
        // Default to Sphere-100K with reduced query batch (η̂ diagnostic loops are
        // O(Q · rho · b) and we only care about build wall + cover-profile here).
        Environment.SetEnvironmentVariable("APOLLO_SPHERE_JSONL",
            Environment.GetEnvironmentVariable("APOLLO_SPHERE_JSONL") ?? "/tmp/sphere-100200.jsonl");
        Environment.SetEnvironmentVariable("APOLLO_SPHERE_N",
            Environment.GetEnvironmentVariable("APOLLO_SPHERE_N") ?? "100000");
        Environment.SetEnvironmentVariable("APOLLO_SPHERE_Q",
            Environment.GetEnvironmentVariable("APOLLO_SPHERE_Q") ?? "50");
        Hnsw.CoverProfileEnabled = true;
        Hnsw.CoverProfileReset();

        using var output = new ConsoleTestOutputHelper();
        var test = new HnswDescentCoverDiagnostic(output);

        var sw = Stopwatch.StartNew();
        try
        {
            test.Sphere_DescentCover_Apollonius_vs_Legacy_DiagnosticReport();
        }
        finally
        {
            (test as IDisposable)?.Dispose();
        }
        sw.Stop();
        Console.WriteLine($"TOTAL: {sw.ElapsedMilliseconds} ms");

        double ticksPerMs = Stopwatch.Frequency / 1000.0;
        long total = Hnsw.CoverTotalTicks;
        long calls = Hnsw.CoverCalls;
        Console.WriteLine($"[cover-profile combined legacy+apollonius builds] calls={calls} total={total / ticksPerMs:F1}ms");
        if (total > 0)
        {
            void Row(string name, long t) => Console.WriteLine($"  {name,-12} {t / ticksPerMs,8:F1}ms  {100.0 * t / total,5:F1}%");
            Row("witness",   Hnsw.CoverWitnessTicks);
            Row("distToSrc", Hnsw.CoverDistToSrcTicks);
            Row("kCapture",  Hnsw.CoverKCaptureTicks);
            Row("greedy",    Hnsw.CoverGreedyTicks);
            Row("mFill",     Hnsw.CoverMFillTicks);
        }
        return 0;
    }
}
