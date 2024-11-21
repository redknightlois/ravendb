using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow.Server;
using Sparrow.Threading;

namespace Micro.Benchmark.Benchmarks
{

    [Config(typeof(Config))]
    public unsafe class ReadNumberBench
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job
                {
                    Environment = { Runtime = CoreRuntime.Core80, Platform = Platform.X64, Jit = Jit.Default, },
                    Run =
                    {
                        // TODO: Next line is just for testing. Fine tune parameters.
                        //RunStrategy = RunStrategy.Monitoring,
                    }
                });

                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        private ByteStringContext _context;
        private ByteString _encodedValues;

        private const int Operations = 100000;
        private long[] _sizeOfValues = new long[Operations];

        [GlobalSetup]
        public void Setup()
        {
            _context = new ByteStringContext(SharedMultipleUseFlag.None);
            _context.Allocate(Operations * sizeof(int), out _encodedValues);

            // We will try to follow the usual distribution we have during encoding. 
            var rnd = new Random(1337);
            for (int i = 0; i < Operations; i++)
            {
                int distribution = rnd.Next(100);
                if (distribution < 50)
                    _sizeOfValues[i] = sizeof(byte);
                else if (distribution < 90)
                    _sizeOfValues[i] = sizeof(short);
                else
                    _sizeOfValues[i] = sizeof(int);
            }

            for (int i = 0; i < _encodedValues.Length; i++)
                _encodedValues.Ptr[i] = (byte)rnd.Next(byte.MaxValue);
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public long Branches()
        {
            long returnValue = 0;

            byte* value = _encodedValues.Ptr;
            foreach (long sizeOfValue in _sizeOfValues)
            {
                returnValue += ReadNumberIfBased(value, sizeOfValue);
                value += sizeOfValue;
            }

            return returnValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ReadNumberIfBased(byte* value, long sizeOfValue)
        {
            int returnValue = *value;
            if (sizeOfValue == sizeof(byte))
                return returnValue;

            returnValue |= *(value + 1) << 8;
            if (sizeOfValue == sizeof(short))
                return returnValue;

            returnValue |= *(short*)(value + 2) << 16;
            if (sizeOfValue == sizeof(int))
                return returnValue;

            throw new ArgumentException($"Unsupported size {sizeOfValue}");
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public long BranchesOpt()
        {
            long returnValue = 0;

            byte* value = _encodedValues.Ptr;
            foreach (long sizeOfValue in _sizeOfValues)
            {
                returnValue += ReadNumberIfBasedOpt(value, sizeOfValue);
                value += sizeOfValue;
            }

            return returnValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private int ReadNumberIfBasedOpt(byte* value, long sizeOfValue)
        {
            int returnValue = *value;
            if (sizeOfValue == sizeof(byte))
                return returnValue;

            returnValue |= *(value + 1) << 8;
            if (sizeOfValue == sizeof(short))
                return returnValue;

            returnValue |= *(short*)(value + 2) << 16;
            if (sizeOfValue == sizeof(int))
                return returnValue;

            throw new ArgumentException($"Unsupported size {sizeOfValue}");
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public long MostProbable()
        {
            long returnValue = 0;

            byte* value = _encodedValues.Ptr;
            foreach (long sizeOfValue in _sizeOfValues)
            {
                returnValue += ReadNumberSwitchByteMostProbable(value, sizeOfValue);
                value += sizeOfValue;
            }

            return returnValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ReadNumberSwitchByteMostProbable(byte* value, long sizeOfValue)
        {
            int returnValue = *value;
            switch (sizeOfValue)
            {
                case sizeof(byte):
                    return returnValue;
                case sizeof(short):
                    returnValue |= *(value + 1) << 8;
                    goto case sizeof(byte);
                case sizeof(int):
                    returnValue |= *(short*)(value + 2) << 16;
                    goto case sizeof(short);
            }

            throw new ArgumentException($"Unsupported size {sizeOfValue}");
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public long Cascade()
        {
            long returnValue = 0;

            byte* value = _encodedValues.Ptr;
            foreach (long sizeOfValue in _sizeOfValues)
            {
                returnValue += ReadNumberSwitchCascadeSequence(value, sizeOfValue);
                value += sizeOfValue;
            }

            return returnValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ReadNumberSwitchCascadeSequence(byte* value, long sizeOfValue)
        {
            int returnValue = *value;
            switch (sizeOfValue)
            {
                case sizeof(int):
                    returnValue |= *(short*)(value + 2) << 16;
                    goto case sizeof(short);
                case sizeof(short):
                    returnValue |= *(value + 1) << 8;
                    goto case sizeof(byte);
                case sizeof(byte):
                    return returnValue;
            }

            throw new ArgumentException($"Unsupported size {sizeOfValue}");
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public long CascadeOpt()
        {
            long returnValue = 0;

            byte* value = _encodedValues.Ptr;
            foreach (long sizeOfValue in _sizeOfValues)
            {
                returnValue += ReadNumberSwitchCascadeSequenceOpt(value, sizeOfValue);
                value += sizeOfValue;
            }

            return returnValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private int ReadNumberSwitchCascadeSequenceOpt(byte* value, long sizeOfValue)
        {
            int returnValue = *value;
            switch (sizeOfValue)
            {
                case sizeof(int):
                    returnValue |= *(short*)(value + 2) << 16;
                    goto case sizeof(short);
                case sizeof(short):
                    returnValue |= *(value + 1) << 8;
                    goto case sizeof(byte);
                case sizeof(byte):
                    return returnValue;
            }

            throw new ArgumentException($"Unsupported size {sizeOfValue}");
        }
    }
}
