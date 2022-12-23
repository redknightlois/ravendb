﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using NUnit.Framework;
using Sparrow.Server.Utils.VxSort;
using static Test.DataGeneration;
using DataGenerator = System.Func<(int[] data, int[] sortedData, string reproContext)>;

namespace Test
{
    [Parallelizable(ParallelScope.All)]
    public class ParityTests
    {
        static int NumCycles => int.Parse(Environment.GetEnvironmentVariable("NUM_CYCLES") ?? "10");

        public class SortTestCaseData : TestCaseData
        {
            public SortTestCaseData(DataGenerator generator) : base(generator) { }
        }

        static readonly int[] ArraySizes =
        {
            10,
            100,
            BitonicSort.MaxBitonicLength<int>() - 1,
            BitonicSort.MaxBitonicLength<int>(),
            BitonicSort.MaxBitonicLength<int>() + 1,
            1_000,
            10_000,
            100_000,
            1_000_000
        };

        static readonly int[] ConstantSeeds = { 666, 333, 999, 314159 };

        static IEnumerable<TestCaseData> PreSorted =>
            from size in ArraySizes
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            select new SortTestCaseData(
                () =>
                    (
                        Enumerable.Range(0, realSize).ToArray(),
                        Enumerable.Range(0, realSize).ToArray(),
                        "pre-sorted"
                    )
            ).SetArgDisplayNames($"S{realSize:0000000}");

        static IEnumerable<TestCaseData> ReverseSorted =>
            from size in ArraySizes
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            select new SortTestCaseData(
                () =>
                    (
                        Enumerable.Range(0, realSize).Reverse().ToArray(),
                        Enumerable.Range(0, realSize).ToArray(),
                        "reverse-sorted"
                    )
            ).SetArgDisplayNames($"Ƨ{realSize:0000000}");

        static IEnumerable<TestCaseData> HalfMinValue =>
            from size in ArraySizes
            from seed in ConstantSeeds
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            select new SortTestCaseData(
                () => GenerateData(realSize, seed, int.MinValue, 0.5)
            ).SetArgDisplayNames($"{realSize:0000000}/{seed}/0.5min");

        static IEnumerable<TestCaseData> HalfMaxValue =>
            from size in ArraySizes
            from seed in ConstantSeeds
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            select new SortTestCaseData(
                () => GenerateData(realSize, seed, int.MaxValue, 0.5)
            ).SetArgDisplayNames($"{realSize:0000000}/{seed}/0.5max");

        static IEnumerable<TestCaseData> AllOnes =>
            from size in ArraySizes
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            select new SortTestCaseData(
                () =>
                    (
                        Enumerable.Repeat(1, realSize).ToArray(),
                        Enumerable.Repeat(1, realSize).ToArray(),
                        "all-ones"
                    )
            ).SetArgDisplayNames($"1:{realSize:0000000}");

        static IEnumerable<TestCaseData> ConstantSeed =>
            from size in ArraySizes
            from seed in ConstantSeeds
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            select new SortTestCaseData(() => GenerateData(realSize, seed)).SetArgDisplayNames(
                $"{realSize:0000000}/{seed}"
            );

        static IEnumerable<TestCaseData> TimeSeed =>
            from size in ArraySizes
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            let seed = ((int)DateTime.Now.Ticks + i * 666) % int.MaxValue
            select new SortTestCaseData(() => GenerateData(realSize, seed)).SetArgDisplayNames(
                $"{realSize:0000000}/R{i}"
            );

        [TestCaseSource(nameof(PreSorted))]
        [TestCaseSource(nameof(ReverseSorted))]
        [TestCaseSource(nameof(HalfMinValue))]
        [TestCaseSource(nameof(HalfMaxValue))]
        [TestCaseSource(nameof(AllOnes))]
        [TestCaseSource(nameof(ConstantSeed))]
        [TestCaseSource(nameof(TimeSeed))]
        public void VxSortPrimitiveUnstable(DataGenerator generator)
        {
            var (randomData, sortedData, reproContext) = generator();
            Sort.Run(randomData);

            Assert.That(randomData, Is.Ordered, reproContext);
            Assert.That(randomData, Is.EqualTo(sortedData), reproContext);
        }
    }
}
