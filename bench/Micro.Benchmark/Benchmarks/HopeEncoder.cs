﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow;
using Sparrow.Server.Compression;

namespace Micro.Benchmark.Benchmarks
{
    [Config(typeof(HopeEncoder.Config))]
    public class HopeEncoder
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

        private State _state = new(64000);
        private HopeEncoder<Encoder3Gram<State>> _encoder;
        
        private State _trainedState = new(64000);
        private HopeEncoder<Encoder3Gram<State>> _trainedEncoder;

        private StringKeys _sequentialKeys;
        private StringKeys _outputBuffers;
        private StringKeys _encodedBuffers;
        private StringKeys _encodedOutputBuffers;
        private int[] _outputBuffersSize;
        private int[] _encodedBuffersSize;

        public const int Keys = 10000;

        [GlobalSetup]
        public void Setup()
        {
            _encoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(_state));

            string[] keysAsStrings = new string[Keys];
            byte[][] outputBuffers = new byte[Keys][];
            byte[][] encodedBuffers = new byte[Keys][];
            byte[][] encodedOutputBuffers = new byte[Keys][];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                keysAsStrings[i] = $"companies/{i:000000000}";
                outputBuffers[i] = new byte[128];
                encodedBuffers[i] = new byte[128];
                encodedOutputBuffers[i] = new byte[128];
            }

            _sequentialKeys = new StringKeys(keysAsStrings);
            
            _outputBuffers = new StringKeys(outputBuffers);
            _outputBuffersSize = new int[keysAsStrings.Length];

            _encodedBuffers = new StringKeys(encodedBuffers);
            _encodedBuffersSize = new int[keysAsStrings.Length];
            _encodedOutputBuffers = new StringKeys(encodedOutputBuffers);

            _trainedEncoder = new HopeEncoder<Encoder3Gram<State>>(new Encoder3Gram<State>(_trainedState));
            _trainedEncoder.Train(_sequentialKeys, 128);
            _trainedEncoder.Encode(_sequentialKeys, _encodedBuffers, _encodedBuffersSize);
        }

        private struct State : IEncoderState
        {
            private readonly byte[] _value;

            public State(int size)
            {
                _value = new byte[size];
            }

            public Span<byte> EncodingTable => new Span<byte>(_value).Slice(0, _value.Length / 2);
            public Span<byte> DecodingTable => new Span<byte>(_value).Slice(_value.Length / 2);
        }

        private struct StringKeys : IReadOnlySpanEnumerator, ISpanEnumerator
        {
            private readonly byte[][] _values;

            public int Length => _values.Length;

            public ReadOnlySpan<byte> this[int i] => new(_values[i]);

            Span<byte> ISpanEnumerator.this[int i] => new(_values[i]);


            public StringKeys(string[] keys)
            {
                _values = new byte[keys.Length][];
                for (int i = 0; i < keys.Length; i++)
                {
                    var value = UTF8Encoding.ASCII.GetBytes(keys[i]);

                    var nullTerminated = new byte[value.Length + 1];
                    nullTerminated[value.Length] = 0;
                    value.AsSpan().CopyTo(nullTerminated);

                    _values[i] = nullTerminated;
                }
            }

            public StringKeys(byte[][] keys)
            {
                _values = keys;
            }
        }

        [Params(256)]
        public int DictionarySize { get; set; }

        [Benchmark]
        public void Training()
        {
            _trainedEncoder.Train(_sequentialKeys, DictionarySize);
        }

        [Benchmark(OperationsPerInvoke = Keys)]
        public void Encode()
        {
            _trainedEncoder.Encode(_sequentialKeys, _outputBuffers, _outputBuffersSize);
        }

        [Benchmark(OperationsPerInvoke = Keys)]
        public void Decode()
        {
            _trainedEncoder.Decode(_encodedBuffers, _encodedOutputBuffers, _encodedBuffersSize);
        }
    }
}
