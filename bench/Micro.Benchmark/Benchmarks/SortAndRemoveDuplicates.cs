using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow;

namespace Micro.Benchmark.Benchmarks;

[DisassemblyDiagnoser]
[Config(typeof(SortAndRemoveDuplicates.Config))]
public unsafe class SortAndRemoveDuplicates
{
    private class Config : ManualConfig
    {
        public Config()
        {
            AddJob(new Job(RunMode.Default)
            {
                Environment =
                    {
                        Runtime = CoreRuntime.Core80,
                        Platform = Platform.X64,
                        Jit = Jit.RyuJit
                    }
            });

            // Exporters for data
            AddExporter(GetExporters().ToArray());

            AddValidator(BaselineValidator.FailOnError);
            AddValidator(JitOptimizationsValidator.FailOnError);

            AddAnalyser(EnvironmentAnalyser.Default);
        }
    }

    [Params(16, 64, 256, 1024, 1024 * 4)]
    public int ArraySize { get; set; }

    private long[] _sourceArray, _workingArray;
    private float[] _sourceScoreArray, _workingScoreArray;

    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(125123);
        var totalElementsNumber = ArraySize;
        var uniqueElementsNumber = (int)Math.Ceiling(totalElementsNumber * 0.9);

        _sourceArray = Enumerable.Range(0, uniqueElementsNumber).Select(x => (long)x).ToArray();
        var repeatedElementsArray = random.GetItems(_sourceArray, totalElementsNumber - uniqueElementsNumber);
        _sourceArray = _sourceArray.Concat(repeatedElementsArray).ToArray();

        _sourceScoreArray = Enumerable.Range(0, totalElementsNumber).Select(_ => (float)random.NextDouble())
            .ToArray();

        _workingScoreArray = new float[totalElementsNumber];
        _workingArray = new long[totalElementsNumber];
    }


    private static ReadOnlySpan<uint> FloatMask => [0, 0xFFFF_FFFF];

    [Benchmark]
    public int SortAndMergeDuplicatesWithSingleBranch()
    {
        _sourceArray.AsSpan().CopyTo(_workingArray);
        _sourceScoreArray.AsSpan().CopyTo(_workingScoreArray);

        Span<long> values = _workingArray;
        Span<float> itemsAssociated = _workingScoreArray;

        if (values.Length <= 1)
            return values.Length;

        values.Sort(itemsAssociated);

        int outputIdx = 0;

        ref var valuesStartRef = ref MemoryMarshal.GetReference(values);
        ref var itemsAssociatedStartRef = ref MemoryMarshal.GetReference(itemsAssociated);

        for (int i = 1; i < values.Length; i++)
        {
            // values[i] == values[outputIdx];
            var valueOfI = Unsafe.Add(ref valuesStartRef, i);
            bool isEquals = valueOfI == Unsafe.Add(ref valuesStartRef, outputIdx);

            // outputIdx += isEquals ? 1 : 0;
            outputIdx += isEquals.ToInt32() ^ 1;

            // values[outputIdx] = values[i];
            ref var outputValuesRef = ref Unsafe.Add(ref valuesStartRef, outputIdx);
            outputValuesRef = valueOfI;

            // float current = itemsAssociated[outputIdx];
            ref var itemsAssociatedOutputRef = ref Unsafe.Add(ref itemsAssociatedStartRef, outputIdx);

            // float next = itemsAssociated[i];
            var next = Unsafe.Add(ref itemsAssociatedStartRef, i);

            int casted = Unsafe.BitCast<float, int>(itemsAssociatedOutputRef) & (int)FloatMask[isEquals.ToInt32()];
            itemsAssociatedOutputRef = next + Unsafe.BitCast<int, float>(casted);
        }

        return outputIdx + 1;
    }

    [Benchmark]
    public unsafe int SortAndMergeDuplicatesBranchlessWithAwkwardCmov()
    {
        _sourceArray.AsSpan().CopyTo(_workingArray);
        _sourceScoreArray.AsSpan().CopyTo(_workingScoreArray);

        Span<long> values = _workingArray;
        Span<float> itemsAssociated = _workingScoreArray;

        if (values.Length <= 1)
            return values.Length;

        values.Sort(itemsAssociated);

        int outputIdx = 0;

        ref var valuesStartRef = ref MemoryMarshal.GetReference(values);
        ref var itemsAssociatedStartRef = ref MemoryMarshal.GetReference(itemsAssociated);

        for (int i = 1; i < values.Length; i++)
        {
            // values[i] == values[outputIdx];
            var valueOfI = Unsafe.Add(ref valuesStartRef, i);
            int isEquals = (valueOfI == Unsafe.Add(ref valuesStartRef, outputIdx)).ToInt32();

            // outputIdx += isEquals ? 1 : 0;
            outputIdx += isEquals ^ 1;

            // values[outputIdx] = values[i];
            ref var outputValuesRef = ref Unsafe.Add(ref valuesStartRef, outputIdx);
            outputValuesRef = valueOfI;

            // float current = itemsAssociated[outputIdx];
            ref var itemsAssociatedOutputRef = ref Unsafe.Add(ref itemsAssociatedStartRef, outputIdx);

            // float next = itemsAssociated[i];
            var next = Unsafe.Add(ref itemsAssociatedStartRef, i);

            int casted = Unsafe.BitCast<float, int>(itemsAssociatedOutputRef) & (int)Unsafe.Add(ref MemoryMarshal.GetReference(FloatMask), isEquals);
            itemsAssociatedOutputRef = next + Unsafe.BitCast<int, float>(casted);
        }

        return outputIdx + 1;
    }

    [Benchmark(Baseline = true)]
    public int SortAndMergeDuplicatesNaive()
    {
        _sourceArray.AsSpan().CopyTo(_workingArray);
        _sourceScoreArray.AsSpan().CopyTo(_workingScoreArray);

        Span<long> values = _workingArray;
        Span<float> itemsAssociated = _workingScoreArray;

        if (values.Length <= 1)
            return values.Length;

        values.Sort(itemsAssociated);

        int outputIdx = 0;
        for (int i = 1; i < values.Length; i++)
        {
            if (values[i] == values[outputIdx])
            {
                itemsAssociated[outputIdx] += itemsAssociated[i];
            }
            else
            {
                outputIdx++;
                values[outputIdx] = values[i];
                itemsAssociated[outputIdx] = itemsAssociated[i];
            }
        }

        return outputIdx + 1;
    }

    [Benchmark]
    public int SortAndMergeDuplicatesUnsafe()
    {
        _sourceArray.AsSpan().CopyTo(_workingArray);
        _sourceScoreArray.AsSpan().CopyTo(_workingScoreArray);

        Span<long> values = _workingArray;
        Span<float> itemsAssociated = _workingScoreArray;
        if (values.Length <= 1)
            return values.Length;

        values.Sort(itemsAssociated);

        ref var valuesStartRef = ref MemoryMarshal.GetReference(values);
        ref var valuesEndRef = ref Unsafe.Add(ref valuesStartRef, values.Length);

        ref var outputValuesRef = ref valuesStartRef;
        ref var outputItemRef = ref MemoryMarshal.GetReference(itemsAssociated);

        ref var currentValuesRef = ref valuesStartRef;
        ref var currentItemRef = ref outputItemRef;

        while (Unsafe.IsAddressLessThan(ref currentValuesRef, ref valuesEndRef))
        {
            currentValuesRef = ref Unsafe.Add(ref currentValuesRef, 1);
            currentItemRef = ref Unsafe.Add(ref currentItemRef, 1);

            if (currentValuesRef != outputValuesRef)
            {
                outputValuesRef = ref Unsafe.Add(ref outputValuesRef, 1);
                outputItemRef = ref Unsafe.Add(ref outputItemRef, 1);

                // Copy the current value and item to the output position
                outputValuesRef = currentValuesRef;
                outputItemRef = currentItemRef;
            }
            else
            {
                outputItemRef += currentItemRef;
            }
        }

        int outputIdx = (int)Unsafe.ByteOffset(ref valuesStartRef, ref outputValuesRef).ToInt32() / Unsafe.SizeOf<float>();
        return outputIdx;
    }
}
