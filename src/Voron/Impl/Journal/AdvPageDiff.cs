using Sparrow;
using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Sparrow.Compression;
using static Sparrow.Server.Utils.ThreadNames.ThreadDetails;

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
        const int BlockSize = 64; // Size of Vector512<byte>
        const int PageSize = 64 * BlockSize;
        const int FullBlockThreshold = 48;
        const byte FullBlockTombstone = 0;

        /// <summary>
        /// Computes the diff between the original and modified buffers and writes it to the output buffer.
        /// </summary>
        public long ComputeDiff(byte* originalBuffer, byte* modifiedBuffer, byte* outputBuffer, long size, out bool isDiff)
        {
            Debug.Assert(size % 4096 == 0);

            long totalPages = (int)(size / PageSize);

            byte* originalPtr = originalBuffer;
            byte* outputPtr = outputBuffer;

            // Calculate the worst case required space.
            int requiredSize = sizeof(ulong)     // Changed bitmap
                               + sizeof(ulong)   // Zeroes bitmap
                               + BlockSize       // We could use the changed bytes, but it is better for this to be a single value.
                               + 1;              // Tombstone to ensure we won't mistake a whole buffer with a diff when decoding.
            
            byte* outputEnd = outputBuffer + size - requiredSize;

            ulong ptrOffset = (ulong)modifiedBuffer - (ulong)originalBuffer;

            var zero = Vector512<byte>.Zero;
            for (int pageIdx = 0; pageIdx < totalPages; pageIdx++)
            {
                if (outputPtr > outputEnd)
                    goto CopyFullBuffer;

                // Remember the start of this page's output
                byte* pageOutputStart = outputPtr;

                // Write the page index ( it may get ignored at the end )
                outputPtr += VariableSizeEncoding.Write<int>(outputPtr, pageIdx);
                ulong* pageBitmapPtr = (ulong*)outputPtr;
                outputPtr += sizeof(ulong);

                ulong pageBitmap = 0; // 64 bits, one bit per block in the page

                for (int blockIdx = 0; blockIdx < BlockSize; blockIdx++)
                {
                    if (outputPtr > outputEnd)
                        goto CopyFullBuffer;
                    
                    // Load 64 bytes from original and modified buffers
                    Vector512<byte> originalVector = Vector512.Load(originalPtr);
                    Vector512<byte> modifiedVector = Vector512.Load(originalPtr + ptrOffset);

                    // No difference, move to next block
                    originalPtr += BlockSize;

                    // Compute the difference vector
                    Vector512<byte> diffVector = Vector512.Xor(originalVector, modifiedVector);

                    // Compute the changed bitmap by checking which bytes are non-zero
                    Vector512<byte> greaterThanZero = Vector512.GreaterThan(diffVector, zero);

                    // Determine if blocks are equal based on changedBitmap
                    ulong changedBitmap = PortableIntrinsics.MoveMask(greaterThanZero);
                    if (changedBitmap == 0)
                        continue;

                    // Count the number of changed bits
                    int changedBitsCount = BitOperations.PopCount(changedBitmap);
                    
                    // Mark the block as changed in the page bitmap
                    pageBitmap |= 1UL << blockIdx;

                    if (changedBitsCount >= FullBlockThreshold)
                    {
                        // Write the full block (FullBlockTombstone indicates full block change)
                        // The tombstone which is 0 can be used because an empty block would have been processed anyway.
                        *outputPtr = FullBlockTombstone;
                        modifiedVector.Store(outputPtr);
                        outputPtr += BlockSize + 1;
                        continue;
                    }

                    // Generate zeroes bitmap
                    ulong zeroesBitmap = PortableIntrinsics.MoveMask(
                        Vector512.Equals(modifiedVector, Vector512<byte>.Zero));

                    // Write changed bitmap
                    outputPtr += VariableSizeEncoding.Write<ulong>(outputPtr, changedBitmap);

                    // Write zeroes bitmap
                    outputPtr += VariableSizeEncoding.Write<ulong>(outputPtr, zeroesBitmap & changedBitmap);

                    // Extract and write changed bytes (excluding zeros)
                    outputPtr += ExtractAndWriteChangedBytes(modifiedVector, changedBitmap & ~zeroesBitmap, outputPtr);
                }

                // If there is no change, we would just continue to the next page. 
                if (pageBitmap == 0)
                {
                    // No blocks changed in this page; reset outputPtr back to pageOutputStart
                    outputPtr = pageOutputStart;
                    continue;
                }

                *pageBitmapPtr = pageBitmap;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ExtractAndWriteChangedBytes(Vector512<byte> vector, ulong bitmap, byte* outputPtr)
        {
            // Obtain a byte pointer to the Vector512<byte> data
            byte* vectorBytes = (byte*)&vector;
            byte* destPtr = outputPtr;

            // Process only the set bits in the bitmap
            while (bitmap != 0)
            {
                // Find the position of the least significant set bit
                int bitPos = BitOperations.TrailingZeroCount(bitmap);

                // Extract the byte at the found position directly via pointer arithmetic
                *destPtr++ = vectorBytes[bitPos];

                // Clear the processed bit
                bitmap &= bitmap - 1;
            }

            // Return the number of bytes written
            return (int)(destPtr - outputPtr);
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

            byte* inputPtr = diff;
            byte* inputEnd = diff + diffSize;

            while (inputPtr < inputEnd)
            {
                // Read page index
                int pageIdx = VariableSizeEncoding.Read<int>(inputPtr, out var len);
                inputPtr += len;
                if (inputPtr > inputEnd)
                    throw new InvalidOperationException("Invalid diff format.");

                // Read page bitmap
                ulong blocksBitmap = Unsafe.ReadUnaligned<ulong>(inputPtr);
                inputPtr += sizeof(ulong);
                if (inputPtr > inputEnd)
                    throw new InvalidOperationException("Invalid diff format.");

                byte* currentPagePtr = buffer + pageIdx * PageSize;

                // Process only the set bits in the bitmap
                while (blocksBitmap != 0)
                {
                    // Find the position of the least significant set bit
                    int blockIdx = BitOperations.TrailingZeroCount(blocksBitmap);
                    
                    // Clear the processed bit
                    blocksBitmap &= blocksBitmap - 1;

                    // Read changed bitmap
                    ulong changedBitmap = VariableSizeEncoding.Read<ulong>(inputPtr, out len);
                    inputPtr += len;
                    if (inputPtr > inputEnd)
                        throw new InvalidOperationException("Invalid diff format.");

                    byte* destBlockPtr = currentPagePtr + blockIdx * BlockSize;
                    
                    if (changedBitmap == FullBlockTombstone)
                    {
                        // Entire block has changed
                        if (inputPtr + BlockSize > inputEnd)
                            throw new InvalidOperationException("Invalid diff format.");

                        // Overwrite the entire block with the modified data from the diff
                        Vector512.Load(inputPtr)
                                 .StoreAligned(destBlockPtr);
                        
                        inputPtr += BlockSize;
                        continue;
                    }

                    // Read zeroes bitmap
                    ulong zeroesBitmap = VariableSizeEncoding.Read<ulong>(inputPtr, out len);
                    inputPtr += len;
                    if (inputPtr > inputEnd)
                        throw new InvalidOperationException("Invalid diff format.");
                    

                    // Calculate number of changed bytes
                    ulong changedNonZeroBitmap = (ulong)changedBitmap & ~(ulong)zeroesBitmap;
                    int numChangedBytes = BitOperations.PopCount(changedNonZeroBitmap);

                    if (inputPtr + numChangedBytes > inputEnd)
                    {
                        throw new InvalidOperationException("Invalid diff format.");
                    }


                    // Load the current block
                    Vector512<byte> destVector = Vector512.Load(destBlockPtr);

                    // Apply zeroes bitmap
                    Vector512<byte> zeroMaskVector = CreateMaskVector((ulong)zeroesBitmap);
                    destVector = Vector512.AndNot(destVector, zeroMaskVector); // Clear bytes where zeroMaskVector is set

                    // Apply changed bytes
                    destVector = ApplyChangedBytes(destVector, changedNonZeroBitmap, inputPtr);
                    inputPtr += numChangedBytes;

                    // Store the updated block back to destination
                    destVector.Store(destBlockPtr);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Vector512<byte> CreateMaskVector(ulong bitmap)
        {
            // Create a Vector512<byte> mask from the bitmap
            byte* maskBytes = stackalloc byte[64];
            for (int i = 0; i < 64; i++)
            {
                maskBytes[i] = (byte)(((bitmap >> i) & 1) * 0xFF);
            }
            return Vector512.Load(maskBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Vector512<byte> ApplyChangedBytes(Vector512<byte> destVector, ulong bitmap, byte* changedBytesPtr)
        {
            // Apply the changed bytes to the destination vector
            int changedIndex = 0;
            for (int i = 0; i < 64; i++)
            {
                if (((bitmap >> i) & 1) != 0)
                {
                    byte value = changedBytesPtr[changedIndex++];
                    destVector = destVector.WithElement(i, value);
                }
            }
            return destVector;
        }
    }
}
