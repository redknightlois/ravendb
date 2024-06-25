﻿using System;
using System.Runtime.InteropServices;
using Sparrow.Logging;
using Sparrow.Server.Platform;
using Sparrow.Server.Platform.Posix;
using Voron.Impl;
using Voron.Impl.Paging;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Voron.Platform.Posix
{
    public abstract class PosixAbstractPager : AbstractPager
    {
        internal int _fd;

        private readonly bool _supportsUnmapping;

        private readonly Logger _log = LoggingSource.Instance.GetLogger<PosixAbstractPager>("PosixAbstractPager");
   

        public override int CopyPage(Pager2 pager, long p, ref Pager2.State state, ref Pager2.PagerTransactionState txState)
        {
            throw new NotImplementedException();
        }

        public override unsafe byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            // We need to decide what pager we are going to use right now or risk inconsistencies when performing prefetches from disk.
            var state = pagerState ?? _pagerState;

            if (this.CanPrefetch.Value)
            {
                if (this._pagerState.ShouldPrefetchSegment(pageNumber, out void* virtualAddress, out long bytes))
                {
                    var command = new PalDefinitions.PrefetchRanges(virtualAddress, bytes);
                    GlobalPrefetchingBehavior.GlobalPrefetcher.Value.CommandQueue.TryAdd(command, 0);
                }
            }

            return base.AcquirePagePointer(tx, pageNumber, state);
        }

        protected PosixAbstractPager(StorageEnvironmentOptions options, bool canPrefetchAhead, bool usePageProtection = false, bool supportsUnmapping = true)
            : base(options, canPrefetchAhead, usePageProtection: usePageProtection)
        {
            _supportsUnmapping = supportsUnmapping;
        }

        public override unsafe void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            base.ReleaseAllocationInfo(baseAddress, size);

            if (_supportsUnmapping == false)
                return;

            var ptr = new IntPtr(baseAddress);

            if (DeleteOnClose)
            {
                if (Syscall.madvise(ptr, new UIntPtr((ulong)size), MAdvFlags.MADV_DONTNEED) != 0)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Failed to madvise MDV_DONTNEED for {FileName?.FullPath}");
                }
            }

            var result = Syscall.munmap(ptr, (UIntPtr)size);
            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                Syscall.ThrowLastError(err, "munmap " + FileName);
            }
            NativeMemory.UnregisterFileMapping(FileName.FullPath, ptr, size);
        }

        protected override void DisposeInternal()
        {
            if (_fd != -1)
            {
                // note that the orders of operations is important here, we first unlink the file
                // we are supposed to be the only one using it, so Linux would be ready to delete it
                // and hopefully when we close it, won't waste any time trying to sync the memory state
                // to disk just to discard it
                if (DeleteOnClose)
                {
                    Syscall.unlink(FileName.FullPath);
                    // explicitly ignoring the result here, there isn't
                    // much we can do to recover from being unable to delete it
                }
                Syscall.close(_fd);
                _fd = -1;
            }
        }
    }
}
