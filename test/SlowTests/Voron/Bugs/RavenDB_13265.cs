﻿using System;
using FastTests.Voron;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class RavenDB_13265 : StorageTest
    {
        public RavenDB_13265(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void EnsurePagerStateReferenceMustAdd_Current_PagerStateToCollectionSoWeWillReleaseItsReferenceOnTxDispose()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                var dataFilePager = tx.LowLevelTransaction.DataPager;

                var testingStuff = tx.LowLevelTransaction.ForTestingPurposesOnly();
                
                using (testingStuff.CallDuringEnsurePagerStateReference(() =>
                {
                    dataFilePager.EnsureContinuous(ref tx.LowLevelTransaction.DataPagerState,5000, 1);
                }))
                {
                    throw new NotImplementedException();
                    // var state = dataFilePager.PagerState;
                    //
                    // tx.LowLevelTransaction.EnsurePagerStateReference(ref state);
                    //
                    // Assert.Contains(dataFilePager.PagerState, testingStuff.GetPagerStates());
                }
            }
        }
    }
}
