using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Collections;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class SmallSetTests : NoDisposalNeeded
    {
        public SmallSetTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WeakSetSingleItem()
        {
            var ilSet = new WeakSmallSet<int, long>(128);
            ilSet.Add(10, 10);
            Assert.True(ilSet.TryGetValue(10, out var iv));
            Assert.Equal(10, iv);


            var llSet = new WeakSmallSet<long, long>(128);
            llSet.Add(10, 10);
            Assert.True(llSet.TryGetValue(10, out var lv));
            Assert.Equal(10, lv);
        }

        [Fact]
        public void WeakSetSingleLane()
        {
            var ilSet = new WeakSmallSet<int, long>(128);

            for (int i = 0; i < 8; i++)
                ilSet.Add(i, i);

            for (int i = 0; i < 8; i++)
            {
                Assert.True(ilSet.TryGetValue(i, out var iv));
                Assert.Equal(i, iv);
            }


            var llSet = new WeakSmallSet<long, long>(128);
            for (int i = 0; i < 4; i++)
                llSet.Add(i, i);

            for (int i = 0; i < 4; i++)
            {
                Assert.True(llSet.TryGetValue(i, out var lv));
                Assert.Equal(i, lv);
            }
        }

        [Fact]
        public void WeakSetSingleSmallerThanVector()
        {
            var llSet = new WeakSmallSet<long, long>(3);
            llSet.Add(10, 10);
            Assert.True(llSet.TryGetValue(10, out var v));
            Assert.Equal(10, v);
        }

    }
}
