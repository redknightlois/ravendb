using System;
using System.Runtime.Intrinsics.X86;
using BenchmarkDotNet.Running;
using Micro.Benchmark.Benchmarks.Hardware;
using Micro.Benchmark.Benchmarks.LZ4;

namespace Micro.Benchmark
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine($"{nameof(Sse)} support: {Sse.IsSupported}");
            Console.WriteLine($"{nameof(Sse2)} support: {Sse2.IsSupported}");
            Console.WriteLine($"{nameof(Sse3)} support: {Sse3.IsSupported}");
            Console.WriteLine($"{nameof(Sse41)} support: {Sse41.IsSupported}");

            Console.WriteLine($"{nameof(Avx)} support: {Avx.IsSupported}");
            Console.WriteLine($"{nameof(Avx2)} support: {Avx2.IsSupported}");


            var test = new DiffNonZeroes();
            test.AreDifferent = true;
            test.Setup();
            //Console.WriteLine($"AdvDiff: {test.Current_AdvDiff()}");
            //Console.WriteLine($"Diff: {test.Current_Sequential()}");
            for (int i = 0; i < 1000; i++)
            {
                //test.Current_Sequential();
                test.Current_AdvDiff();
            }

            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }
}
