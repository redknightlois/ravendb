using Sparrow;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Voron.Impl.Paging;

namespace Voron.Impl.Scratch
{
    public sealed class PageFromScratchBufferEqualityComparer : IEqualityComparer<PageFromScratchBuffer>
    {
        public static readonly PageFromScratchBufferEqualityComparer Instance = new PageFromScratchBufferEqualityComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(PageFromScratchBuffer x, PageFromScratchBuffer y)
        {
            if (x == y) return true;
            if (x == null || y == null) return false;            

            return x.PositionInScratchBuffer == y.PositionInScratchBuffer && x.Size == y.Size && x.NumberOfPages == y.NumberOfPages && x.File == y.File;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(PageFromScratchBuffer obj)
        {
            int v = Hashing.Combine(obj.NumberOfPages, obj.File.Number);
            int w = Hashing.Combine(obj.Size.GetHashCode(), obj.PositionInScratchBuffer.GetHashCode());
            return Hashing.Combine(v, w);
        }
    }


    public sealed record PageFromScratchBuffer(
        ScratchBufferFile File,
        Pager2.State State,
        long AllocatedInTransactionId,
        long PositionInScratchBuffer,
        long PageNumberInDataFile,
        int NumberOfPages,
        int Size
    )
    {
        public Page PreviousVersion = new();

        public unsafe Page ReadPage(LowLevelTransaction tx)
        {
            return new Page(Read(ref tx.PagerTransactionState));
        }
        
        public unsafe byte* Read(ref Pager2.PagerTransactionState txState)
        {
            return File.Pager.AcquirePagePointerWithOverflowHandling(State, ref txState, PositionInScratchBuffer);
        }
    }
}
