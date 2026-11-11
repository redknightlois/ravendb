using System;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;

namespace Corax.Querying.Matches.Meta
{
    internal static class MergeHelper
    {
        /// <summary>
        /// Writes the ids present in both sorted lists to dst and returns their count. dst and left *may* be the same thing;
        /// we can assume that dst is at least as large as the smallest of those.
        /// </summary>
        public static int And(Span<long> dst, Span<long> left, Span<long> right)
        {
            return left.Length < right.Length
                ? And(dst, MemoryMarshal.Cast<long, ulong>(left), MemoryMarshal.Cast<long, ulong>(right))
                : And(dst, MemoryMarshal.Cast<long, ulong>(right), MemoryMarshal.Cast<long, ulong>(left));
        }

        private static int And(Span<long> dst, ReadOnlySpan<ulong> smaller, ReadOnlySpan<ulong> larger)
        {
            int count = 0, s = 0, l = 0;

            if (AdvInstructionSet.IsAcceleratedVector256)
            {
                // Each id of the smaller list is compared against a block of the larger one at once; the block moves forward
                // while its last id is smaller than the id.
                ref var largerStart = ref MemoryMarshal.GetReference(larger);
                while (s < smaller.Length && l <= larger.Length - Vector256<ulong>.Count)
                {
                    var value = smaller[s];
                    var block = Vector256.LoadUnsafe(ref largerStart, (nuint)l);
                    if (value > block.GetElement(Vector256<ulong>.Count - 1))
                    {
                        l += Vector256<ulong>.Count;
                        continue;
                    }

                    if (Vector256.EqualsAny(Vector256.Create(value), block))
                        dst[count++] = (long)value;
                    s++;
                }
            }

            while (s < smaller.Length && l < larger.Length)
            {
                if (smaller[s] > larger[l])
                {
                    l++;
                }
                else if (smaller[s] < larger[l])
                {
                    s++;
                }
                else
                {
                    dst[count++] = (long)smaller[s];
                    s++;
                    l++;
                }
            }

            return count;
        }
    }
}
