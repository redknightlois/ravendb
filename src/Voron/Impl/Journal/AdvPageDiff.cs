using Sparrow;
using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

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
        
        /// <summary>
        /// Computes the diff between the original and modified buffers and writes it to the output buffer.
        /// </summary>
        public long ComputeDiff(byte* originalBuffer, byte* modifiedBuffer, byte* outputBuffer, long size, out bool isDiff)
        {
            Debug.Assert(size % 4096 == 0);

            long totalBlocks = size / BlockSize;

            byte* originalPtr = originalBuffer;
            byte* modifiedPtr = modifiedBuffer;
            byte* outputPtr = outputBuffer;
            byte* outputEnd = outputBuffer + size;

            for (int blockIdx = 0; blockIdx < totalBlocks; blockIdx++)
            {
                // Load 64 bytes from original and modified buffers
                Vector512<byte> originalVector = Vector512.Load(originalPtr);
                Vector512<byte> modifiedVector = Vector512.Load(modifiedPtr);

                // Compute the difference vector
                Vector512<byte> diffVector = Vector512.Xor(originalVector, modifiedVector);

                // Check if the blocks are equal
                bool blocksEqual = PortableIntrinsics.TestZ(diffVector, diffVector);
                if (blocksEqual)
                {
                    // No difference, move to next block
                    originalPtr += BlockSize;
                    modifiedPtr += BlockSize;
                    continue;
                }

                diffVector = Vector512.GreaterThan(diffVector, Vector512<byte>.Zero);
                
                // Generate changed bitmap
                ulong changedBitmap = PortableIntrinsics.MoveMask(diffVector);

                // Generate zeroes bitmap
                Vector512<byte> zeroMaskVector = Vector512.Equals(modifiedVector, Vector512<byte>.Zero);
                ulong zeroesBitmap = PortableIntrinsics.MoveMask(zeroMaskVector);
                
                // Calculate the number of changed bytes excluding zeros
                int numChangedBytes = BitOperations.PopCount(changedBitmap & ~zeroesBitmap);

                // Calculate required size
                int requiredSize = sizeof(int)     // Block index
                                 + sizeof(ulong)   // Changed bitmap
                                 + sizeof(ulong)   // Zeroes bitmap
                                 + numChangedBytes // Changed bytes
                                 + 1;              // Tombstone to ensure we won't mistake a whole buffer with a diff when decoding.

                if (outputPtr + requiredSize > outputEnd)
                {
                    // Not enough space, copy the modified buffer in full
                    Unsafe.CopyBlockUnaligned(outputBuffer, modifiedBuffer, (uint)size);
                    isDiff = false;
                    return size;
                }

                // Write block index
                Unsafe.WriteUnaligned(outputPtr, blockIdx);
                outputPtr += sizeof(int);

                // Write changed bitmap
                Unsafe.WriteUnaligned(outputPtr, changedBitmap);
                outputPtr += sizeof(ulong);

                // Write zeroes bitmap
                Unsafe.WriteUnaligned(outputPtr, zeroesBitmap & changedBitmap);
                outputPtr += sizeof(ulong);

                // Extract and write changed bytes (excluding zeros)
                ExtractAndWriteChangedBytes(modifiedVector, changedBitmap & ~zeroesBitmap, ref outputPtr);

                // Move to next block
                originalPtr += BlockSize;
                modifiedPtr += BlockSize;
            }

            isDiff = true;
            return (long)((ulong)outputPtr - (ulong)outputBuffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ExtractAndWriteChangedBytes(Vector512<byte> vector, ulong bitmap, ref byte* outputPtr)
        {
            // Process only the set bits in the bitmap
            while (bitmap != 0)
            {
                // Find the position of the least significant set bit
                int bitPos = BitOperations.TrailingZeroCount(bitmap);

                // Extract the byte at the found position
                byte value = vector.GetElement(bitPos);

                // Write the byte to the output
                *outputPtr++ = value;

                // Clear the processed bit
                bitmap &= bitmap - 1;
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

            byte* destPtr = buffer;
            byte* inputPtr = diff;
            byte* inputEnd = diff + diffSize;

            while (inputPtr < inputEnd)
            {
                if (inputPtr + sizeof(int) + sizeof(ulong) * 2 > inputEnd)
                {
                    throw new InvalidOperationException("Invalid diff format.");
                }

                // Read block index
                int blockIdx = Unsafe.ReadUnaligned<int>(inputPtr);
                inputPtr += sizeof(int);

                // Read changed bitmap
                ulong changedBitmap = Unsafe.ReadUnaligned<ulong>(inputPtr);
                inputPtr += sizeof(ulong);

                // Read zeroes bitmap
                ulong zeroesBitmap = Unsafe.ReadUnaligned<ulong>(inputPtr);
                inputPtr += sizeof(ulong);

                // Calculate number of changed bytes
                ulong changedNonZeroBitmap = changedBitmap & ~zeroesBitmap;
                int numChangedBytes = BitOperations.PopCount(changedNonZeroBitmap);

                if (inputPtr + numChangedBytes > inputEnd)
                {
                    throw new InvalidOperationException("Invalid diff format.");
                }

                byte* destBlockPtr = buffer + blockIdx * BlockSize;

                // Load the current block
                Vector512<byte> destVector = Vector512.Load(destBlockPtr);

                // Apply zeroes bitmap
                Vector512<byte> zeroMaskVector = CreateMaskVector(zeroesBitmap);
                destVector = Vector512.AndNot(destVector, zeroMaskVector); // Clear bytes where zeroMaskVector is set

                // Apply changed bytes
                destVector = ApplyChangedBytes(destVector, changedNonZeroBitmap, inputPtr);
                inputPtr += numChangedBytes;

                // Store the updated block back to destination
                destVector.Store(destBlockPtr);
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
