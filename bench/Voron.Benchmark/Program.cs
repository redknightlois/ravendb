using System;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using Corax;
using Sparrow.Server;
using Sparrow.Threading;
using Voron.Benchmark.Corax;

namespace Voron.Benchmark
{
    public class Program
    {
//#if DEBUG
//                static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, new DebugInProcessConfig());
//#else
//        static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
//#endif

        public static void Main(string[] args)
        {
            var benchmark = new OrderByBenchmark();
            {
                benchmark.BufferSize = 1024;

                Console.WriteLine("Starting data creation.");
                benchmark.Setup();

                using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
                Slice.From(bsc, "Type", ByteStringType.Immutable, out var typeSlice);
                Slice.From(bsc, "Dog", ByteStringType.Immutable, out var dogSlice);
                Slice.From(bsc, "Age", ByteStringType.Immutable, out var ageSlice);
                Slice.From(bsc, "1", ByteStringType.Immutable, out var ageValueSlice);

                var _ids = new long[benchmark.BufferSize];
                var indexSearcher = new IndexSearcher(benchmark.Env);

                Console.WriteLine("Starting querying.");
                for (int i = 0; i < 1000; i++)
                {
                    var typeTerm = indexSearcher.TermQuery(typeSlice, "Dog");
                    var ageTerm = indexSearcher.StartWithQuery(ageSlice, ageValueSlice);
                    var andQuery = indexSearcher.And(typeTerm, ageTerm);
                    var query = indexSearcher.OrderByAscending(andQuery, fieldId: 2, take: benchmark.TakeSize);

                    Span<long> ids = _ids;
                    while (query.Fill(ids) != 0)
                        ;
                }
            }
            benchmark.Cleanup();
        }
    }
}
