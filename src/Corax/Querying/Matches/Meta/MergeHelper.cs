using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Sparrow;

namespace Corax.Querying.Matches.Meta
{
    internal static unsafe class MergeHelper
    {
        /// <summary>
        /// Writes the ids found in both sorted lists to dst and returns their count.
        /// dst may be left; it holds at least as many ids as the shorter list.
        /// </summary>
        public static int And(Span<long> dst, Span<long> left, Span<long> right)
        {
            fixed (long* dstPtr = dst, leftPtr = left, rightPtr = right)
            {
                if (left.Length <= right.Length)
                    return And(dstPtr, leftPtr, left.Length, rightPtr, right.Length);

                return And(dstPtr, rightPtr, right.Length, leftPtr, left.Length);
            }
        }

        private static int And(long* dst, long* shorter, int shorterLength, long* longer, int longerLength)
        {
            if (AdvInstructionSet.IsAcceleratedVector128 == false || shorterLength == 0)
                return AndScalar(dst, shorter, shorterLength, longer, longerLength);

            if (longerLength >= 2 * shorterLength)
            {
                // PERF: Storing every id costs less than mispredicting whether it was found, unless nearly all or nearly none are found.
                double density = Density(longer, longerLength);
                return dst != longer && density > 1.0 / 16 && density < 15.0 / 16
                    ? AndWindows<StoreEvery>(dst, shorter, shorterLength, longer, longerLength)
                    : AndWindows<StoreFound>(dst, shorter, shorterLength, longer, longerLength);
            }

            // PERF: The plain merge mispredicts about once per id without a match, so it wins when those are few against the matches.
            double matches = shorterLength * Density(longer, longerLength);
            if (shorterLength + longerLength - 2 * matches < matches / 4)
                return AndScalar(dst, shorter, shorterLength, longer, longerLength);

            return dst == longer
                ? AndBlocks(dst, longer, longerLength, shorter, shorterLength)
                : AndBlocks(dst, shorter, shorterLength, longer, longerLength);
        }

        private static double Density(long* ids, int length) => length / ((double)(ulong)(ids[length - 1] - ids[0]) + 1);

        private static int AndBlocks(long* dst, long* x, int xLength, long* y, int yLength)
        {
            long* d = dst;
            long* xEnd = x + xLength, yEnd = y + yLength;
            bool inPlace = dst == x;

            if (xLength >= 4 && yLength >= 4)
            {
                // PERF: The block of x stays in registers until x moves on.
                var xLow = Vector128.Load((ulong*)x);
                var xHigh = Vector128.Load((ulong*)x + 2);

                while (true)
                {
                    ulong x0 = xLow.GetElement(0), x1 = xLow.GetElement(1), x2 = xHigh.GetElement(0), x3 = xHigh.GetElement(1);
                    ulong yMax = (ulong)y[3];

                    // Every id of the block of x against every id of the block of y.
                    var yLow = Vector128.Load((ulong*)y);
                    var yHigh = Vector128.Load((ulong*)y + 2);
                    var yLowSwapped = Vector128.Shuffle(yLow, Vector128.Create(1UL, 0));
                    var yHighSwapped = Vector128.Shuffle(yHigh, Vector128.Create(1UL, 0));
                    var lowFound = Vector128.Equals(xLow, yLow) | Vector128.Equals(xLow, yLowSwapped) | Vector128.Equals(xLow, yHigh) | Vector128.Equals(xLow, yHighSwapped);
                    var highFound = Vector128.Equals(xHigh, yLow) | Vector128.Equals(xHigh, yLowSwapped) | Vector128.Equals(xHigh, yHigh) | Vector128.Equals(xHigh, yHighSwapped);

                    // PERF: Every id is stored and the count advances only on a match, which costs less than branching on it.
                    // dst is x or holds as many ids as x, so each write lands at or before the id it came from.
                    *d = (long)x0;
                    d += (long)(lowFound.GetElement(0) & 1);
                    *d = (long)x1;
                    d += (long)(lowFound.GetElement(1) & 1);
                    *d = (long)x2;
                    d += (long)(highFound.GetElement(0) & 1);
                    *d = (long)x3;
                    d += (long)(highFound.GetElement(1) & 1);

                    // Moves past the block with the smaller last id, or past both when their last ids are equal.
                    if (yMax <= x3)
                        y += 4;
                    if (x3 <= yMax)
                    {
                        x += 4;
                        if (xEnd - x < 4)
                            break;

                        xLow = Vector128.Load((ulong*)x);
                        xHigh = Vector128.Load((ulong*)x + 2);
                    }
                    else if (yEnd - y < 4)
                    {
                        // In place, the stores may have overwritten this block from d on, and the tail merge reads it again.
                        if (inPlace)
                        {
                            if (d <= x)
                                x[0] = (long)x0;
                            if (d <= x + 1)
                                x[1] = (long)x1;
                            if (d <= x + 2)
                                x[2] = (long)x2;
                            if (d <= x + 3)
                                x[3] = (long)x3;
                        }

                        break;
                    }

                    if (yEnd - y < 4)
                        break;
                }
            }

            return (int)(d - dst) + AndScalar(d, x, (int)(xEnd - x), y, (int)(yEnd - y));
        }

        private static int AndWindows<TStore>(long* dst, long* a, int aLength, long* b, int bLength) where TStore : struct
        {
            const int window = 16;
            long* d = dst;
            long* aEnd = a + aLength, bEnd = b + bLength;

            if (aLength > 0 && bLength >= window)
            {
                // PERF: A stride of about sqrt(2 * window * bLength / aLength) balances the stride checks against the windows walked after the last stride.
                int stride = Math.Max(window, (int)BitOperations.RoundUpToPowerOf2((uint)Math.Sqrt(2.0 * window * bLength / aLength)));
                long* bLast = bEnd - window;

                while (true)
                {
                    // Passes the windows of b below the id a stride at a time while a stride fits, then a window at a time.
                    ulong id = (ulong)*a;
                    if (id > (ulong)b[window - 1])
                    {
                        if (bLast - b >= stride && id > (ulong)b[stride - 1])
                            b += stride;
                        else if (bLast - b >= window)
                            b += window;
                        else if (b != bLast)
                            b = bLast;
                        else
                            break;

                        continue;
                    }

                    if (typeof(TStore) == typeof(StoreEvery))
                    {
                        // dst must not be b: storing every id would put ids into the windows that b does not hold.
                        *d = (long)id;
                        d += Contains16(b, id) ? 1 : 0;
                    }
                    else if (Contains16(b, id))
                    {
                        *d++ = (long)id;
                    }

                    a++;
                    if (a == aEnd)
                        break;
                }
            }

            return (int)(d - dst) + AndScalar(d, a, (int)(aEnd - a), b, (int)(bEnd - b));
        }

        private struct StoreEvery { }

        private struct StoreFound { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool Contains16(long* window, ulong id)
        {
            var value = Vector128.Create(id);
            var ids = (ulong*)window;

            // PERF: The compares are combined as a tree so they do not wait on each other.
            var found = (Vector128.Equals(value, Vector128.Load(ids)) | Vector128.Equals(value, Vector128.Load(ids + 2)))
                        | (Vector128.Equals(value, Vector128.Load(ids + 4)) | Vector128.Equals(value, Vector128.Load(ids + 6)))
                        | (Vector128.Equals(value, Vector128.Load(ids + 8)) | Vector128.Equals(value, Vector128.Load(ids + 10)))
                        | (Vector128.Equals(value, Vector128.Load(ids + 12)) | Vector128.Equals(value, Vector128.Load(ids + 14)));
            return found != Vector128<ulong>.Zero;
        }

        // PERF: Skips tiered PGO: a profile gathered from the tails of the other kernels lays this merge out badly for dense lists.
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static int AndScalar(long* dst, long* left, int leftLength, long* right, int rightLength)
        {
            long* dstPtr = dst;
            long* leftPtr = left;
            long* rightPtr = right;

            long* leftEndPtr = leftPtr + leftLength;
            long* rightEndPtr = rightPtr + rightLength;

            while (leftPtr < leftEndPtr && rightPtr < rightEndPtr)
            {
                long leftValue = *leftPtr;
                long rightValue = *rightPtr;

                if (leftValue > rightValue)
                {
                    rightPtr++;
                }
                else if (leftValue < rightValue)
                {
                    leftPtr++;
                }
                else
                {
                    *dstPtr = leftValue;
                    dstPtr++;
                    leftPtr++;
                    rightPtr++;
                }
            }

            return (int)(dstPtr - dst);
        }
    }
}
