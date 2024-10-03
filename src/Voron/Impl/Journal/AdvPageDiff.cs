using Sparrow;
using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Sparrow.Compression;

namespace Voron.Impl.Journal
{

    /// <summary>
    /// This class computes a diff between two buffers and write the diff to a temp location.
    /// 
    /// Assumptions:
    /// - The buffers are all the same size
    /// - The buffers size is a multiple of page boundary (4096 bytes)
    /// 
    /// If the diff overflows, we'll write the modified value to the output instead.
    /// </summary>
    public unsafe struct AdvPageDiff
    {
        // The block size is the amount of bytes we are going to process in a single loop. The rationale here is that
        // multiple different architectures may need a different <block> processing. For now, it is a constant, but we will
        // certainly optimize this based on architecture eventually as a JIT constant. 
        // The block size has to be smaller than twice the capability of the L1 because we have 2 data streams.
        // For example in the AMD Zen3 architecture the L1 is a 32Kb, 8-way, 64 sets, 64B line. And has a latency of 4 clocks.
        const int BlockSize = 1024;
        const int PageSize = 4096; // This is always 4096 because we are ensuring that every page is at least a multiple of 4K

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        private struct RunHeader
        {
            public long Offset;
            public long Count;
        }

        /// <summary>
        /// Computes the diff between the original and modified buffers and writes it to the output buffer.
        /// </summary>
        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public long ComputeDiff(byte* originalBuffer, byte* modifiedBuffer, byte* outputBuffer, long size, out bool isDiff)
        {
            const int n = 64;
            const int n2 = n / 2;

            Debug.Assert(size % PageSize == 0, "Size must be a multiple of PageSize");

            byte* inputPtr = modifiedBuffer;
            byte* outputPtr = outputBuffer;

            byte* inputEnd = inputPtr + size;
            byte* outputEnd = outputBuffer + size;
            nuint offset = (nuint)(originalBuffer - modifiedBuffer); // Pointer offsetting for efficient addressing using LEA instructions

            // Each bit of the bitmap will essentially represent the resulting comparison of 8 bytes (ulong).
            // A single byte would store the comparison of 8 packets of 8 bytes = 64 bytes.
            const int BitmapSize = BlockSize / 64;

            // Validate configuration
            PortableExceptions.ThrowIf<InvalidOperationException>(PageSize % BitmapSize != 0,
                $"Invalid configuration. {nameof(PageSize)} must be divisible by {nameof(BitmapSize)}");

            byte* bitmap = stackalloc byte[BitmapSize];
            byte* bitmapEnd = bitmap + BitmapSize;

            // Buffer-level loop: process the entire buffer directly without page-level looping
            while (inputPtr < inputEnd)
            {
                Debug.Assert(inputPtr <= inputEnd - BlockSize, "Block processing exceeds buffer size");

                // Initialize bitmap for the current group of blocks
                byte* inputBlockPtr = inputPtr;
                byte* bitmapPtr = bitmap;

                Debug.Assert((ulong)(inputBlockPtr - modifiedBuffer) % BlockSize == 0);

                // Block-level loop: unroll processing to handle multiple blocks per iteration
                while (bitmapPtr < bitmapEnd)
                {
                    Debug.Assert(inputBlockPtr <= inputPtr + BlockSize - 2 * n);

                    // We know that a single load of a 512 bits vector will fill a byte. So we can add as many as we need as long as
                    // they are multiples of BlockSize to minimize the amount of housekeeping logic.

                    Vector256<ulong> m00 = Vector256.Load((ulong*)(inputBlockPtr + 0 * n2));
                    Vector256<ulong> m10 = Vector256.Load((ulong*)(inputBlockPtr + 2 * n2));
                    
                    Vector256<ulong> o00 = Vector256.Load((ulong*)(inputBlockPtr + offset + 0 * n2));
                    Vector256<ulong> o10 = Vector256.Load((ulong*)(inputBlockPtr + offset + 2 * n2));

                    Vector256<ulong> m01 = Vector256.Load((ulong*)(inputBlockPtr + 1 * n2));
                    Vector256<ulong> m11 = Vector256.Load((ulong*)(inputBlockPtr + 3 * n2));
                    
                    Vector256<ulong> o01 = Vector256.Load((ulong*)(inputBlockPtr + offset + 1 * n2));
                    Vector256<ulong> o11 = Vector256.Load((ulong*)(inputBlockPtr + offset + 3 * n2));


                    byte b00 = (byte)Vector256.Equals(o00, m00).ExtractMostSignificantBits();
                    byte b01 = (byte)Vector256.Equals(o01, m01).ExtractMostSignificantBits();
                    byte b0 = (byte)((b00 << 4) + b01);

                    byte b10 = (byte)Vector256.Equals(o10, m10).ExtractMostSignificantBits();
                    byte b11 = (byte)Vector256.Equals(o11, m11).ExtractMostSignificantBits();
                    byte b1 = (byte)((b10 << 4) + b11);

                    *(ushort*)bitmapPtr = (ushort)~((b1 << 8) + b0);

                    bitmapPtr += sizeof(ushort);
                    inputBlockPtr += sizeof(ushort) * n;
                }

                // Check invariants
                Debug.Assert(inputPtr == inputBlockPtr - BlockSize);
                Debug.Assert(bitmapPtr == bitmapEnd);

                // We need to reset the block pointer to start again because we need to just move toward the end on the modified buffer.
                inputBlockPtr = inputPtr;
                bitmapPtr = bitmap;

                NewLoop:

                // Skip any 64 bytes of unmodified buffer very fast.
                while (bitmapPtr < bitmapEnd && *bitmapPtr == 0)
                {
                    if (Sse.IsSupported) 
                        Sse.Prefetch0(inputBlockPtr + BlockSize);
                    
                    bitmapPtr++;
                    inputBlockPtr += n;
                }

                while (bitmapPtr < bitmapEnd)
                {
                    if (Sse.IsSupported) 
                        Sse.Prefetch0(inputBlockPtr + BlockSize);

                    // If we are done, we just bail out.
                    if (*bitmapPtr == 0)
                        goto NewLoop;
                    
                    // Since we are going to write a data block. We need to store the pointer where that data is going to be store.
                    var runHeaderPtr = (RunHeader*)outputPtr;
                    var runStoragePtr = outputPtr + sizeof(RunHeader);

                    // We store the currently known block location before we even decide if we are going to write zero blocks or not.
                    // For our purposes the zero runs take precedence in the selection because they are the most efficient to store.
                    // It is possible that no block is written, and therefore we would just not increment the output pointer.
                    runHeaderPtr->Offset = (inputBlockPtr - modifiedBuffer);
                    
                    // Check invariants
                    Debug.Assert(runHeaderPtr->Offset % n == 0);

                    // By the time we arrive here, we are either finished with this bitmap OR we are starting a run.
                    // and therefore we need to load the data before being able to move forward. 
                    Vector512<ulong> m0 = Vector512.Load((ulong*)(inputBlockPtr));

                    byte* inputBlockPtrRunStart = inputBlockPtr;
                    inputBlockPtr += n;
                    bitmapPtr += 1;

                    // We know this is not a zero run, therefore we have to store blocks data.
                    var runStoragePtrNext = runStoragePtr + n;
                    if (runStoragePtrNext >= outputEnd)
                        goto CopyFullBuffer;

                    // The first block could have leading non changed bytes, therefore we had already skipped them
                    // on the offset. A shift left operation have to be performed in order to store only the trailing
                    // bytes and increment the offset accordingly.
                    Vector512.Store(m0.AsByte(), runStoragePtr);

                    bool isZeroRun = Vector512.EqualsAll(m0, Vector512<ulong>.Zero);
                    runStoragePtr = isZeroRun ? runStoragePtr : runStoragePtrNext;
                    
                    // Now if this 'start' is a zero-run, then m0 is going to be full of zeroes, which also mean that
                    // we can just loop until there are no changes.
                    while (bitmapPtr < bitmapEnd)
                    {
                        // If there are no changes in this run, then we are done, and therefore we need to
                        // start scanning again. 
                        if (*bitmapPtr == 0)
                            break;

                        runStoragePtrNext = runStoragePtr + n;
                        if (runStoragePtrNext >= outputEnd)
                            goto CopyFullBuffer;

                        if (Sse.IsSupported) 
                            Sse.Prefetch0(inputBlockPtr + BlockSize);

                        m0 = Vector512.Load((ulong*)inputBlockPtr);
                        bitmapPtr += 1;
                        inputBlockPtr += n;

                        Vector512.Store(m0.AsByte(), runStoragePtr);

                        // This condition while anti-intuitive has been inverted to exploit the branch predictor.
                        if (isZeroRun == false || Vector512.EqualsAll(m0, Vector512<ulong>.Zero))
                        {
                            runStoragePtr = runStoragePtrNext;
                            continue;
                        }

                        break; // We are done, we need to find a new run now.
                    }

                    // We could technically fix the count if in the last block there are unchanged values (we can know
                    // using the bitmap values corresponding to that particular block).
                    long runLength = inputBlockPtr - inputBlockPtrRunStart;
                    runHeaderPtr->Count = isZeroRun ? -runLength : runLength;

                    // Since we wrote many blocks we have to add those bytes to the output.
                    outputPtr = runStoragePtr;
                }

                inputPtr += BlockSize;
            }
            
            isDiff = true;
            return (long)((ulong)outputPtr - (ulong)outputBuffer);

            CopyFullBuffer:
            {
                // Not enough space, copy the modified buffer in full
                Unsafe.CopyBlockUnaligned(outputBuffer, modifiedBuffer, (uint)size);
                isDiff = false;
                return size;
            }
        }
        

        public void ComputeNew(byte* modifiedBuffer, byte* outputBuffer, long size)
        {
            Debug.Assert(size % 4096 == 0);
            
            // This is the same, but we don't need to check anything, as the original buffer is considered to
            // contain only zeroes. Therefore, we can build a much more optimized version of the encoding loop.
        }

        public void Apply(byte* diff, long diffSize, byte* buffer, long bufferSize, bool isNewDiff = false)
        {
            Debug.Assert(bufferSize % 4096 == 0);

            if (diffSize == bufferSize)
            {
                // The diff is the full buffer
                Unsafe.CopyBlockUnaligned(buffer, diff, (uint)diffSize);
                return;
            }
            
            if (isNewDiff)
                Unsafe.InitBlockUnaligned(buffer, 0, (uint)bufferSize); // Initialize destination buffer to zeros

            long pos = 0;
            while (pos < diffSize)
            {
                if (pos + sizeof(long) * 2 > diffSize)
                    AssertInvalidDiffSize(pos, sizeof(long) * 2, diffSize);

                long start = ((long*)(diff + pos))[0];
                long count = ((long*)(diff + pos))[1];
                pos += sizeof(long) * 2;


                if (count < 0)
                {
                    // run of only zeroes
                    count *= -1;
                    if (start + count > bufferSize)
                        AssertInvalidSize(start, count, bufferSize);
                    Memory.Set(buffer + start, 0, count);
                    continue;
                }

                if (start + count > bufferSize)
                    AssertInvalidSize(start, count, bufferSize);
                if (pos + count > diffSize)
                    AssertInvalidDiffSize(pos, count, diffSize);

                Memory.Copy(buffer + start, diff + pos, count);
                pos += count;
            }
        }

        [DoesNotReturn]
        private void AssertInvalidSize(long start, long count, long size)
        {
            throw new ArgumentOutOfRangeException(nameof(Size),
                $"Cannot apply diff to position {start + count} because it is bigger than {size}");
        }

        [DoesNotReturn]
        private void AssertInvalidDiffSize(long pos, long count, long diffSize)
        {
            throw new ArgumentOutOfRangeException(nameof(Size),
                $"Cannot apply diff because pos {pos} & count {count} are beyond the diff size: {diffSize}");
        }
    }
}
