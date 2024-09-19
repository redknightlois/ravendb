using System;
using System.Collections.Generic;
using Sparrow;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Journal
{
    public unsafe class AdvDiffPagesTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        private const int PageSize = 4096;

        [RavenFact(RavenTestCategory.Voron)]
        public void CanComputeNoDifference()
        {
            var fst = new byte[PageSize];
            var sec = new byte[PageSize];
            var trd = new byte[PageSize];

            new Random().NextBytes(fst);
            Buffer.BlockCopy(fst, 0, sec, 0, fst.Length);

            fixed (byte* one = fst)
            fixed (byte* two = sec)
            fixed (byte* tri = trd)
            fixed (byte* diff = new byte[PageSize])
            {
                var diffPage = new AdvPageDiff();

                var diffSize = diffPage.ComputeDiff(one, two, diff, PageSize, out var isDiff);

                Assert.True(isDiff);
                Assert.Equal(0, diffSize);

                Memory.Copy(tri, one, PageSize);
                diffPage.Apply(diff, diffSize, tri, PageSize, false);

                var result = Memory.Compare(tri, two, PageSize);
                Assert.Equal(0, result);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanComputeSmallDifference_AndThenApplyit()
        {
            var fst = new byte[PageSize];
            var sec = new byte[PageSize];
            var trd = new byte[PageSize];

            new Random().NextBytes(fst);
            Buffer.BlockCopy(fst, 0, sec, 0, fst.Length);
            Buffer.BlockCopy(fst, 0, trd, 0, fst.Length);

            sec[12]++;
            sec[433]++;

            fixed (byte* one = fst)
            fixed (byte* two = sec)
            fixed (byte* tri = trd)
            fixed (byte* diff = new byte[PageSize])
            {
                var diffPage = new AdvPageDiff();

                var diffSize = diffPage.ComputeDiff(one, two, diff, PageSize, out var isDiff);

                Assert.True(isDiff);

                Memory.Copy(tri, one, PageSize);
                diffPage.Apply(diff, diffSize, tri, PageSize, false);

                var result = Memory.Compare(tri, two, PageSize);
                Assert.Equal(0, result);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanComputeSmallSequentialDifference_AndThenApplyit()
        {
            var fst = new byte[PageSize];
            var sec = new byte[PageSize];
            var trd = new byte[PageSize];

            new Random().NextBytes(fst);
            Buffer.BlockCopy(fst, 0, sec, 0, fst.Length);
            Buffer.BlockCopy(fst, 0, trd, 0, fst.Length);

            sec[127]++;
            sec[191]++;

            fixed (byte* one = fst)
            fixed (byte* two = sec)
            fixed (byte* tri = trd)
            fixed (byte* diff = new byte[PageSize])
            {
                var diffPage = new AdvPageDiff();

                var diffSize = diffPage.ComputeDiff(one, two, diff, PageSize, out var isDiff);

                Assert.True(isDiff);

                Memory.Copy(tri, one, PageSize);
                diffPage.Apply(diff, diffSize, tri, PageSize, false);

                var result = Memory.Compare(tri, two, PageSize);
                Assert.Equal(0, result);
            }
        }

        //[Fact]
        //public void CanComputeSmallDifferenceFromNew()
        //{
        //    var fst = new byte[4096];


        //    fst[12]++;
        //    fst[433]++;

        //    fixed (byte* one = fst)
        //    fixed (byte* tmp = new byte[4096])
        //    {
        //        var diffPages = new DiffPages
        //        {
        //            Output = tmp,
        //        };

        //        diffPages.ComputeNew(one, 4096);
        //        Assert.True(diffPages.IsDiff);

        //        Assert.Equal(96, diffPages.OutputSize);
        //    }
        //}


        public static IEnumerable<object[]> ChangedBytes
        {
            get
            {
                return new[]
                {
                    new object[] { 0 },
                    new object[] { (4096 * 4 - 1) },
                    new object[] { 513 },
                    new object[] { 1023 },
                    new object[] { 1024 },
                    new object[] { 1025 },
                    new object[] { (4096 * 4 - 2) },
                    new object[] { 1 },
                    new object[] { 65 },
                    new object[] { 63 },
                    new object[] { 64 },
                };
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [MemberData(nameof(ChangedBytes))]
        public void CanComputeSmallDifference_AndThenApplyOnBig(int value)
        {
            const int Size = 4096 * 4;

            var fst = new byte[Size];
            var sec = new byte[Size];
            var trd = new byte[Size];

            new Random().NextBytes(fst);
            Buffer.BlockCopy(fst, 0, sec, 0, Size);
            Buffer.BlockCopy(fst, 0, trd, 0, Size);

            sec[value]++;

            fixed (byte* one = fst)
            fixed (byte* two = sec)
            fixed (byte* tri = trd)
            fixed (byte* diff = new byte[Size])
            {
                var diffPage = new AdvPageDiff();
                var diffSize = diffPage.ComputeDiff(one, two, diff, Size, out _);

                Memory.Copy(tri, one, Size);
                diffPage.Apply(diff, diffSize, tri, Size, false);

                var result = Memory.Compare(tri, two, Size);
                Assert.Equal(0, result);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ComputeAndThenApplyRandomized()
        {
            const int Size = 4096 * 2;

            var fst = new byte[Size];
            var sec = new byte[Size];
            var trd = new byte[Size];

            var rnd = new Random(1337);
            rnd.NextBytes(fst);
            Buffer.BlockCopy(fst, 0, sec, 0, Size);
            Buffer.BlockCopy(fst, 0, trd, 0, Size);

            fixed (byte* one = fst)
            fixed (byte* two = sec)
            fixed (byte* tri = trd)
            fixed (byte* diff = new byte[Size])
            {
                var diffPage = new AdvPageDiff();

                for (int i = 0; i < 4096; i++)
                {
                    // We are going to change one byte at a time and try to reconstruct. 
                    int idx = rnd.Next(Size);
                    sec[idx]++;

                    var diffSize = diffPage.ComputeDiff(one, two, diff, Size, out var _);

                    Memory.Copy(tri, one, Size);
                    diffPage.Apply(diff, diffSize, tri, Size, false);

                    var result = Memory.Compare(tri, two, Size);
                    Assert.Equal(0, result);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CompletelyDifferent()
        {
            var fst = new byte[PageSize];
            var sec = new byte[PageSize];
            var trd = new byte[PageSize];

            new Random(1).NextBytes(fst);
            new Random(2).NextBytes(sec);

            fixed (byte* one = fst)
            fixed (byte* two = sec)
            fixed (byte* tri = trd)
            fixed (byte* diff = new byte[PageSize])
            {
                var diffPage = new AdvPageDiff();

                var diffSize = diffPage.ComputeDiff(one, two, diff, PageSize, out var isDiff);

                Assert.False(isDiff);

                Memory.Copy(tri, one, PageSize);
                diffPage.Apply(diff, diffSize, tri, PageSize, false);

                var result = Memory.Compare(tri, two, PageSize);
                Assert.Equal(0, result);
            }
        }

        //[Fact]
        //public void Applying_diff_calculated_as_new_multiple_times()
        //{
        //    const int size = 4096;

        //    var fst = new byte[size];
        //    var sec = new byte[size];

        //    var r = new Random(1);

        //    for (int i = 0; i < 10; i++)
        //    {
        //        fst[i] = (byte)r.Next(0, 255);
        //        sec[(size - 1) - i] = (byte)r.Next(0, 255);
        //    }

        //    var result = new byte[size];

        //    foreach (var page in new[] { fst, sec })
        //    {
        //        fixed (byte* ptr = page)
        //        fixed (byte* diffPtr = new byte[size])
        //        fixed (byte* resultPtr = result)
        //        {
        //            var diffPages = new DiffPages
        //            {
        //                Output = diffPtr,
        //            };

        //            diffPages.ComputeNew(ptr, size);

        //            Assert.True(diffPages.IsDiff);

        //            new DiffApplier
        //            {
        //                Destination = resultPtr,
        //                Diff = diffPtr,
        //                Size = page.Length,
        //                DiffSize = diffPages.OutputSize
        //            }.Apply(true);
        //        }
        //    }

        //    Assert.Equal(sec, result);
        //}
    }
}
