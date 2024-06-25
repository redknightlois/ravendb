﻿using System.Linq;
using FastTests.Voron;
using Sparrow;
using Sparrow.Platform;
using Voron;
using Voron.Global;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_16105 : StorageTest
    {
        public RavenDB_16105(ITestOutputHelper output) : base(output)
        {
        }

        private readonly byte[] _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.Encryption.MasterKey = _masterKey.ToArray();
            options.Encryption.EncryptionBuffersPool = new EncryptionBuffersPool();
        }

        [Fact]
        public unsafe void MustNotEncryptBuffersThatWereExtraAllocatedJustToSatisfyPowerOf2Size()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.AllocatePage(50);

                tx.Commit();
            }

            var numberOfAllocatedPages = 58;
            int dataSizeOnSinglePage = Constants.Storage.PageSize - PageHeader.SizeOf;
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                // this will allocate encryption buffer of size 64 pages because we use Bits.PowerOf2(numberOfPages) under the covers
                // in the scratch file the allocation will start at position 66
                var p = tx.LowLevelTransaction.AllocateMultiplePageAndReturnFirst(numberOfAllocatedPages);
                pageNumber = p.PageNumber;
                p.Flags |= PageFlags.Overflow;
                p.OverflowSize = 8 * Constants.Storage.PageSize;


                for (int i = 0; i < numberOfAllocatedPages; i++)
                {
                    Page page = tx.LowLevelTransaction.GetPage(pageNumber + i);

                    Memory.Set(page.DataPointer, (byte)i, dataSizeOnSinglePage);
                }

                

                var scratchFile = Env.ScratchBufferPool.GetScratchBufferFile(0);

                var state = tx.LowLevelTransaction.PagerTransactionState.ForCrypto[scratchFile.File.Pager];

                Assert.False(state[124].Modified); // starting position 66 in the scratch file + 58 pages of actual allocation
                Assert.False(state[125].Modified);
                Assert.False(state[126].Modified);
                Assert.False(state[127].Modified);
                Assert.False(state[128].Modified);
                Assert.False(state[129].Modified);

                for (int i = 0; i < numberOfAllocatedPages; i++)
                {
                    Assert.True(state[66 + i].Modified); // pages in use must have Modified = true
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                for (int i = 0; i < numberOfAllocatedPages; i++)
                {
                    Page page = tx.LowLevelTransaction.GetPage(pageNumber + i);

                    for (int j = 0; j < dataSizeOnSinglePage; j++)
                    {
                        Assert.Equal(i, page.DataPointer[j]);
                    }
                }
            }
        }
    }
}
