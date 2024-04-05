using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Raven.Server.Utils;
using SlowTests.Corax;
using SlowTests.Sharding.Cluster;
using Xunit;
using FastTests.Voron.Util;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static void Main(string[] args)
    {
        Console.WriteLine(Process.GetCurrentProcess().Id);

        for (int i = 0; i < 1000; i++)
        {
            Console.WriteLine($"Starting to run {i}");

            try
            {
                var param = new RavenTestParameters
                {
                    DatabaseMode = RavenDatabaseMode.Single,
                    SearchEngine = RavenSearchEngineMode.Lucene
                };

                using (var testOutputHelper = new ConsoleTestOutputHelper())
                using (var test = new SlowTests.Issues.RavenDB_11089(testOutputHelper))
                {
                    DebuggerAttachedTimeout.DisableLongTimespan = true;

                    // Any one of them will fail after a set amount of runs in release mode on Cortex-M3 class hardware.
                    test.CanAddToArray(param);
                    //test.CanPatch(param);
                    //test.CanIncrement(param);
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e);
                Console.ForegroundColor = ConsoleColor.White;
            }
        }
    }

    private static void TryRemoveDatabasesFolder()
    {
        var p = System.AppDomain.CurrentDomain.BaseDirectory;
        var dbPath = Path.Combine(p, "Databases");
        if (Directory.Exists(dbPath))
        {
            try
            {
                Directory.Delete(dbPath, true);
                Assert.False(Directory.Exists(dbPath), "Directory.Exists(dbPath)");
            }
            catch
            {
                Console.WriteLine($"Could not remove Databases folder on path '{dbPath}'");
            }
        }
    }
}
