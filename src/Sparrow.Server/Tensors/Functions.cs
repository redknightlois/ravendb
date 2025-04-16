using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using System.Runtime.Intrinsics;
using System.Runtime.InteropServices;

namespace Sparrow.Server.Tensors
{
    public static class Functions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T CosineDistance<T>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
            where T : unmanaged, IFloatingPoint<T>, IRootFunctions<T>, INumber<T>
        {
            return T.One - CosineSimilarity(a, b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TResult CosineDistance<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
            where T : unmanaged, IRootFunctions<T>, INumber<T>
            where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
        {
            return TResult.One - CosineSimilarity<T, TResult>(a, b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T CosineSimilarity<T>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
            where T : unmanaged, IFloatingPoint<T>, IRootFunctions<T>, INumber<T>
        {
            if (AdvInstructionSet.IsAcceleratedVector256 && a.Length >= Vector512<T>.Count)
                return Vectorized512.CosineSimilarity<T, T>(a, b);

            return Serial.CosineSimilarity<T, T>(a, b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
            where T : unmanaged, IRootFunctions<T>, INumber<T>
            where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
        {
            if (AdvInstructionSet.IsAcceleratedVector256 && a.Length >= Vector512<T>.Count)
                return Vectorized512.CosineSimilarity<T, TResult>(a, b);

            return Serial.CosineSimilarity<T, TResult>(a, b);
        }

        public static class Serial
        {
            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityNormalize<T, TResult>(T ab, T a2, T b2)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                // Convert the accumulation values from T to TResult.
                // This assumes that converting from T to TResult is lossless or acceptable in your context.
                TResult a2Conv = TResult.CreateTruncating(a2);
                TResult b2Conv = TResult.CreateTruncating(b2);
                TResult abConv = TResult.CreateTruncating(ab);

                // Compute the reciprocal of the magnitudes:
                // invSqrtA2 = 1 / sqrt(a2) and invSqrtB2 = 1 / sqrt(b2)
                TResult invSqrtA2B2 = TResult.Sqrt(a2Conv) * TResult.Sqrt(b2Conv);

                // Calculate the cosine similarity (note that cos(theta) = ab / (sqrt(a2)*sqrt(b2)))
                TResult cosineSimilarity = abConv / invSqrtA2B2;
                return cosineSimilarity;
            }

            /// <summary>
            /// Serial implementation of Cosine distance.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                T ab = T.Zero, a2 = T.Zero, b2 = T.Zero;
                for (int i = 0; i < a.Length; i++)
                {
                    ab += a[i] * b[i];
                    a2 += a[i] * a[i];
                    b2 += b[i] * b[i];
                }

                // Special cases
                if (T.IsZero(a2) && T.IsZero(b2))
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (T.IsZero(ab))
                    return TResult.Zero;  // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                return CosineSimilarityNormalize<T, TResult>(ab, a2, b2);
            }
        }

        public static class Vectorized512
        {
            private static ReadOnlySpan<byte> MoveMaskTable =>
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 64 bits
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 128 bits
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 256 bits
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 512 bits
                // Now comes the part were we are having 0xFF
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 64 bits
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 128 bits
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 256 bits
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, //
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 512 bits
            ];


            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                Vector512<T> abVec = Vector512<T>.Zero;
                Vector512<T> a2Vec = Vector512<T>.Zero;
                Vector512<T> b2Vec = Vector512<T>.Zero;

                int i = a.Length;
                ref T aRef = ref MemoryMarshal.GetReference(a);
                ref T bRef = ref MemoryMarshal.GetReference(b);

            Loop:

                Vector512<T> aVec = Vector512.LoadUnsafe(ref aRef);
                Vector512<T> bVec = Vector512.LoadUnsafe(ref bRef);

                abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);

                i -= Vector512<T>.Count;
                aRef = ref Unsafe.Add(ref aRef, Vector512<T>.Count);
                bRef = ref Unsafe.Add(ref bRef, Vector512<T>.Count);
                if (i >= Vector512<T>.Count)
                    goto Loop;

                if (i > 0)
                {
                    int offset = Vector512<T>.Count - i;
                    aRef = ref Unsafe.Subtract(ref aRef, offset);
                    bRef = ref Unsafe.Subtract(ref bRef, offset);

                    ref var moveMaskTable = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, T>(MoveMaskTable));
                    var mask = Vector512.LoadUnsafe<T>(ref moveMaskTable, (nuint)i);

                    aVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref aRef), mask);
                    bVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref bRef), mask);

                    abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                    a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                    b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);
                }

                T ab = Vector512.Sum(abVec);
                T a2 = Vector512.Sum(a2Vec);
                T b2 = Vector512.Sum(b2Vec);

                // Special cases
                if (T.IsZero(a2) && T.IsZero(b2))
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (T.IsZero(ab))
                    return TResult.Zero;  // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                return Vectorized256.CosineSimilarityNormalize<T, TResult>(ab, a2, b2);
            }
        }
        

        public static class Vectorized256
        {
            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityNormalize<T, TResult>(T ab, T a2, T b2)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (!Sse.IsSupported || !Sse2.IsSupported)
                {
                    // Fallback to the serial implementation if SIMD is not supported
                    return Serial.CosineSimilarityNormalize<T, TResult>(ab, a2, b2);
                }

                // Create a 128-bit vector with a2 in the high lane and b2 in the low lane.
                // Note: _mm_set_pd(a2, b2) in C sets lane1=a2 and lane0=b2.
                // In .NET, Vector128.Create(x, y) sets lane0 = x and lane1 = y.
                // So we swap the order.
                var squares = Vector128.Create(double.CreateTruncating(b2), double.CreateTruncating(a2));

                // Compute approximate reciprocal square root (single precision).
                var rsqrts = Sse2.ConvertToVector128Double(
                                                Sse.ReciprocalSqrt(
                                                    Sse2.ConvertToVector128Single(squares))
                                            );

                // Newton-Raphson iteration for reciprocal square root:
                // https://en.wikipedia.org/wiki/Newton%27s_method
                rsqrts = Sse2.Add(
                    Sse2.Multiply(Vector128.Create(1.5d), rsqrts),
                    Sse2.Multiply(
                        Sse2.Multiply(
                            Sse2.Multiply(squares, Vector128.Create(-0.5d)),
                            rsqrts),
                        Sse2.Multiply(rsqrts, rsqrts)
                    )
                );

                // Extract the results.
                // According to our lane ordering:
                //   - Lane 0 contains b2 reciprocal.
                //   - Lane 1 contains a2 reciprocal.
                double b2Reciprocal = rsqrts.ToScalar(); // lane 0
                double a2Reciprocal = Sse2.UnpackHigh(rsqrts, rsqrts).ToScalar(); // lane 1

                return TResult.CreateTruncating(double.CreateTruncating(ab) * a2Reciprocal * b2Reciprocal);
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarity512<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                Vector512<T> abVec = Vector512<T>.Zero;
                Vector512<T> a2Vec = Vector512<T>.Zero;
                Vector512<T> b2Vec = Vector512<T>.Zero;

                int i = a.Length;
                ref T aRef = ref MemoryMarshal.GetReference(a);
                ref T bRef = ref MemoryMarshal.GetReference(b);

            Loop:
                // PERF: The reason why this would work on hardware not supporting 512-bit vectors is
                // that it will effectively create 2 lanes (xmm and ymm) of 256-bit vectors. And because
                // there are no overlapping lanes, there will be less pipeline dependencies hiding latency
                // of the instructions themselves.
                Vector512<T> aVec = Vector512.LoadUnsafe(ref aRef);
                Vector512<T> bVec = Vector512.LoadUnsafe(ref bRef);

                abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);

                i -= Vector512<T>.Count;
                aRef = ref Unsafe.Add(ref aRef, Vector512<T>.Count);
                bRef = ref Unsafe.Add(ref bRef, Vector512<T>.Count);
                if (i >= Vector512<T>.Count)
                    goto Loop;

                T ab = Vector512.Sum(abVec);
                T a2 = Vector512.Sum(a2Vec);
                T b2 = Vector512.Sum(b2Vec);
                while (i >= 0)
                {
                    ab += aRef * bRef;
                    a2 += aRef * aRef;
                    b2 += bRef * bRef;

                    i--;
                    aRef = ref Unsafe.Add(ref aRef, 1);
                    bRef = ref Unsafe.Add(ref bRef, 1);
                }

                // Special cases
                if (T.IsZero(a2) && T.IsZero(b2))
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (T.IsZero(ab))
                    return TResult.Zero;  // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                return CosineSimilarityNormalize<T, TResult>(ab, a2, b2);
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                Vector256<T> abVec = Vector256<T>.Zero;
                Vector256<T> a2Vec = Vector256<T>.Zero;
                Vector256<T> b2Vec = Vector256<T>.Zero;

                int i = a.Length;
                ref T aRef = ref MemoryMarshal.GetReference(a);
                ref T bRef = ref MemoryMarshal.GetReference(b);

            Loop:
                // PERF: The reason why this would work on hardware not supporting 512-bit vectors is
                // that it will effectively create 2 lanes (xmm and ymm) of 256-bit vectors. And because
                // there are no overlapping lanes, there will be less pipeline dependencies hiding latency
                // of the instructions themselves.
                Vector256<T> aVec = Vector256.LoadUnsafe(ref aRef);
                Vector256<T> bVec = Vector256.LoadUnsafe(ref bRef);

                abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);

                i -= Vector256<T>.Count;
                aRef = ref Unsafe.Add(ref aRef, Vector256<T>.Count);
                bRef = ref Unsafe.Add(ref bRef, Vector256<T>.Count);
                if (i >= Vector256<T>.Count)
                    goto Loop;

                T ab = Vector256.Sum(abVec);
                T a2 = Vector256.Sum(a2Vec);
                T b2 = Vector256.Sum(b2Vec);
                while (i >= 0)
                {
                    ab += aRef * bRef;
                    a2 += aRef * aRef;
                    b2 += bRef * bRef;

                    i--;
                    aRef = ref Unsafe.Add(ref aRef, 1);
                    bRef = ref Unsafe.Add(ref bRef, 1);
                }

                // Special cases
                if (T.IsZero(a2) && T.IsZero(b2))
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (T.IsZero(ab))
                    return TResult.Zero;  // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                return CosineSimilarityNormalize<T, TResult>(ab, a2, b2);
            }
        }
    }
}
