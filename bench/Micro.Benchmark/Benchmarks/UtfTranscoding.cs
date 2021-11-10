using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using FastTests.Sparrow;
using Sparrow.Server.Compression;
using Sparrow.Server.Utf8;

namespace Micro.Benchmark.Benchmarks
{
    [Config(typeof(UtfTranscoding.Config))]
    public unsafe class UtfTranscoding
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job
                {
                    Environment =
                    {
                        Runtime = CoreRuntime.Core50,
                        Platform = Platform.X64,
                        Jit = Jit.RyuJit,
                    },
                    Run =
                    {
                        // TODO: Next line is just for testing. Fine tune parameters.
                        //RunStrategy = RunStrategy.Monitoring,
                    }
                });

                // Exporters for data
                AddExporter(GetExporters().ToArray());
                // Generate plots using R if %R_HOME% is correctly set
                AddExporter(RPlotExporter.Default);

                AddValidator(BaselineValidator.FailOnError);
                AddValidator(JitOptimizationsValidator.FailOnError);

                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        private string _value;
        private byte[] _storage;

        [GlobalSetup]
        public void Setup()
        {
            var assembly = Assembly.GetAssembly(typeof(UtfTranscodingTests));
            var resourceName = "FastTests.Sparrow.utftranscoder.txt";

            using (Stream stream = assembly.GetManifestResourceStream(resourceName))
            using (StreamReader reader = new StreamReader(stream))
            {
                _value = reader.ReadToEnd();
                _storage = new byte[_value.Length * 4];
            }
        }

        [Benchmark]
        public void NetEncoding()
        {
            Encoding.UTF8.GetBytes(_value, _storage);
        }

        [Benchmark]
        public void ScalarEncoding()
        {
            Span<byte> storage = _storage;
            UtfTranscoder.ScalarFromUtf16(_value, ref storage);
        }

        [Benchmark]
        public void SseEncoding()
        {
            Span<byte> storage = _storage;
            UtfTranscoder.SseFromUtf16(_value, ref storage);
        }
    }
}
