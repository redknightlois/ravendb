using System;
using System.Diagnostics;
using FastTests;
using SlowTests.Voron.Graphs;
using Tests.Infrastructure;

namespace Tryouts;

public static class Program
{
    public static int Main(string[] args)
    {
        // Standalone profiling driver: runs the d=128 N=10k Apollonius build only.
        // Mirrors ApolloniusSelector_HighDim_RecallSweep but skips the recall sweep.
        using var output = new ConsoleTestOutputHelper();
        var test = new HnswDescentCoverDiagnostic(output);

        var sw = Stopwatch.StartNew();
        try
        {
            test.ApolloniusSelector_HighDim_RecallSweep();
        }
        finally
        {
            (test as IDisposable)?.Dispose();
        }
        sw.Stop();
        Console.WriteLine($"TOTAL: {sw.ElapsedMilliseconds} ms");
        return 0;
    }
}
