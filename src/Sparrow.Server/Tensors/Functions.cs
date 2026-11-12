using System;
using System.Diagnostics;
using System.Numerics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using System.Runtime.Intrinsics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.Arm;
using Sparrow.Platform;

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

        /// <summary>
        /// Returns Σ aᵢ·bᵢ for two equal-length f32 spans. On AVX2+FMA / AVX-512 the work is
        /// issued as multiple independent FMA chains using <see cref="Arithmetics.MultiplyAddEstimate(Vector512{float}, Vector512{float}, Vector512{float})"/>.
        /// On AArch64/NEON the NEON-shaped kernel is used. On 32-bit hosts or for inputs shorter
        /// than one Vector512 the call is forwarded to <see cref="TensorPrimitives.Dot(ReadOnlySpan{float}, ReadOnlySpan{float})"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float DotProduct(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
        {
            Debug.Assert(a.Length == b.Length, "a.Length == b.Length");

            if (PlatformDetails.Is32Bits || a.Length < Vector512<float>.Count)
                return TensorPrimitives.Dot(a, b);

            if (AdvInstructionSet.IsAcceleratedVector256)
            {
                return (float)Vectorized512.DotProductInternalX64(
                    ref MemoryMarshal.GetReference(a),
                    ref MemoryMarshal.GetReference(b),
                    (nuint)a.Length);
            }

            if (AdvInstructionSet.Arm.IsSupported)
            {
                return (float)Vectorized512.DotProductInternalNeon(
                    ref MemoryMarshal.GetReference(a),
                    ref MemoryMarshal.GetReference(b),
                    (nuint)a.Length);
            }

            return TensorPrimitives.Dot(a, b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T CosineSimilarity<T>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
            where T : unmanaged, IFloatingPoint<T>, IRootFunctions<T>, INumber<T>
        {
            if (PlatformDetails.Is32Bits)
                return TensorPrimitives.CosineSimilarity(a, b);
            
            if (a.Length >= Vector512<T>.Count)
            {
                if (typeof(T) == typeof(float))
                {
                    if (AdvInstructionSet.IsAcceleratedVector256)
                    {
                        return Vectorized512.CosineSimilarity<T, T>(a, b);
                    }

                    return TensorPrimitives.CosineSimilarity(a, b);
                }

                if (typeof(T) == typeof(double) && AdvInstructionSet.IsAcceleratedVector256)
                {
                    return Vectorized512.CosineSimilarity<T, T>(a, b);
                }
            }

            return Serial.CosineSimilarity<T,T>(a, b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
            where T : unmanaged, INumber<T>
            where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
        {
            if (typeof(T) == typeof(sbyte))
            {
                if (a.Length >= Vector256<sbyte>.Count && AdvInstructionSet.X86.IsSupportedAvx256)
                {
                    if (Avx512BW.IsSupported && Avx512F.IsSupported)
                    {
                        return Vectorized512.CosineSimilarityIntegersAvx512(
                            MemoryMarshal.Cast<T, sbyte>(a), TResult.One,
                            MemoryMarshal.Cast<T, sbyte>(b), TResult.One);
                    }

                    return Vectorized256.CosineSimilarityIntegersAvx2(
                        MemoryMarshal.Cast<T, sbyte>(a), TResult.One,
                        MemoryMarshal.Cast<T, sbyte>(b), TResult.One);
                }

                if (a.Length >= Vector128<sbyte>.Count && AdvInstructionSet.Arm.IsSupported && Dp.IsSupported)
                {
                    return Vectorized128.CosineSimilarityIntegersNeon(
                        MemoryMarshal.Cast<T, sbyte>(a), TResult.One,
                        MemoryMarshal.Cast<T, sbyte>(b), TResult.One);
                }

                if (a.Length >= Vector128<sbyte>.Count && (AdvInstructionSet.X86.IsSupportedSse || AdvInstructionSet.Arm.IsSupported))
                {
                    return Vectorized128.CosineSimilarityIntegers(
                        MemoryMarshal.Cast<T, sbyte>(a), TResult.One,
                        MemoryMarshal.Cast<T, sbyte>(b), TResult.One);
                }
            }

            if (a.Length >= Vector512<T>.Count && AdvInstructionSet.IsAcceleratedVector256)
            {
                if (typeof(T) == typeof(float))
                {
                    return Vectorized512.CosineSimilarity<float, TResult>(
                        MemoryMarshal.Cast<T, float>(a),
                        MemoryMarshal.Cast<T, float>(b));
                }

                if (typeof(T) == typeof(double))
                {
                    return Vectorized512.CosineSimilarity<double, TResult>(
                        MemoryMarshal.Cast<T, double>(a),
                        MemoryMarshal.Cast<T, double>(b));
                }
            }

            return Serial.CosineSimilarity<T, TResult>(a, b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, TResult aMagnitude, ReadOnlySpan<T> b, TResult bMagnitude)
            where T : unmanaged, INumber<T>
            where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
        {
            if (typeof(T) == typeof(sbyte))
            {
                if (a.Length >= Vector256<sbyte>.Count && AdvInstructionSet.X86.IsSupportedAvx256)
                {
                    if (AdvInstructionSet.X86.IsSupportedAvx512Basic)
                    {
                        return Vectorized512.CosineSimilarityIntegersAvx512(
                            MemoryMarshal.Cast<T, sbyte>(a), aMagnitude,
                            MemoryMarshal.Cast<T, sbyte>(b), bMagnitude);
                    }

                    return Vectorized256.CosineSimilarityIntegersAvx2(
                        MemoryMarshal.Cast<T, sbyte>(a), aMagnitude,
                        MemoryMarshal.Cast<T, sbyte>(b), bMagnitude);
                }

                if (a.Length >= Vector128<sbyte>.Count && AdvInstructionSet.Arm.IsSupported && Dp.IsSupported)
                {
                    return Vectorized128.CosineSimilarityIntegersNeon(
                        MemoryMarshal.Cast<T, sbyte>(a), aMagnitude,
                        MemoryMarshal.Cast<T, sbyte>(b), bMagnitude);
                }

                if (a.Length >= Vector128<sbyte>.Count && (AdvInstructionSet.X86.IsSupportedSse || AdvInstructionSet.Arm.IsSupported))
                {
                    return Vectorized128.CosineSimilarityIntegers(
                        MemoryMarshal.Cast<T, sbyte>(a), aMagnitude,
                        MemoryMarshal.Cast<T, sbyte>(b), bMagnitude);
                }
            }

            if (a.Length >= Vector512<T>.Count && AdvInstructionSet.IsAcceleratedVector256)
            {
                if (typeof(T) == typeof(float))
                {
                    return Vectorized512.CosineSimilarity<float, TResult>(
                        MemoryMarshal.Cast<T, float>(a), aMagnitude,
                        MemoryMarshal.Cast<T, float>(b), bMagnitude);
                }

                if (typeof(T) == typeof(double))
                {
                    return Vectorized512.CosineSimilarity<double, TResult>(
                        MemoryMarshal.Cast<T, double>(a), aMagnitude,
                        MemoryMarshal.Cast<T, double>(b), bMagnitude);
                }
            }

            return Serial.CosineSimilarity<T, TResult>(a, aMagnitude, b, bMagnitude);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TResult CosineDistance<T, TResult>(ReadOnlySpan<T> a, TResult aMagnitude, ReadOnlySpan<T> b, TResult bMagnitude)
            where T : unmanaged, INumber<T>
            where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
        {
            return TResult.One - CosineSimilarity(a, aMagnitude, b, bMagnitude);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        internal static TResult CosineSimilarityNormalize<T, TResult>(T ab, T a2, T b2)
            where T : unmanaged, IRootFunctions<T>, INumber<T>
            where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
        {
            if (Sse2.IsSupported)
                return Vectorized256.CosineSimilarityNormalizeSse<T, TResult>(ab, a2, b2);

            if (AdvInstructionSet.Arm.IsSupported)
                return Vectorized128.CosineSimilarityNormalizeNeon<T, TResult>(ab, a2, b2);

            // Fallback to the serial implementation if SIMD is not supported
            return Serial.CosineSimilarityNormalize<T, TResult>(ab, a2, b2);
        }

        public static class Serial
        {
            public static long HammingBitDistance(ReadOnlySpan<byte> xAsByte, ReadOnlySpan<byte> yAsByte)
            {
                ref byte xRef = ref MemoryMarshal.GetReference(xAsByte);
                ref byte yRef = ref MemoryMarshal.GetReference(yAsByte);

                const int Word = sizeof(ulong);
                const int N = Word * 3;     

                ulong count1 = 0;
                ulong count2 = 0;
                ulong count3 = 0;

                int offset = 0;
                int length = xAsByte.Length;

                // Process all full 24-byte blocks
                while (offset + N <= length)
                {
                    int byteOffset = offset;
                    ulong xBits1 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref xRef, byteOffset));
                    ulong yBits1 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref yRef, byteOffset));
                    count1 += ulong.PopCount(xBits1 ^ yBits1);

                    byteOffset = (offset + Word);
                    ulong xBits2 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref xRef, byteOffset));
                    ulong yBits2 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref yRef, byteOffset));
                    count2 += ulong.PopCount(xBits2 ^ yBits2);

                    byteOffset = (offset + 2 * Word);
                    ulong xBits3 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref xRef, byteOffset));
                    ulong yBits3 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref yRef, byteOffset));
                    count3 += ulong.PopCount(xBits3 ^ yBits3);

                    offset += N;
                }

                ulong count = count1 + count2 + count3;

                for (; offset + Word <= length; offset += Word)
                {
                    ulong xBits = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref xRef, offset));
                    ulong yBits = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref yRef, offset));
                    count += ulong.PopCount(xBits ^ yBits);
                }

                for (int i = offset; i < length; i++)
                {
                    var xor = Unsafe.Add(ref xRef, i) ^ Unsafe.Add(ref yRef, i);
                    count += (ulong)int.PopCount(xor);
                }

                return (long)count;
            }

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
                where T : unmanaged, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                TResult ab = TResult.Zero, a2 = TResult.Zero, b2 = TResult.Zero;
                for (int i = 0; i < a.Length; i++)
                {
                    var rai = TResult.CreateTruncating(a[i]);
                    var rbi = TResult.CreateTruncating(b[i]);
                    ab += rai * rbi;
                    a2 += rai * rai;
                    b2 += rbi * rbi;
                }

                // Special cases
                if (TResult.IsZero(a2) && TResult.IsZero(b2))
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (TResult.IsZero(ab))
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                return CosineSimilarityNormalize<TResult, TResult>(ab, a2, b2);
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, TResult aMagnitude, ReadOnlySpan<T> b, TResult bMagnitude)
                where T : unmanaged, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (typeof(T) == typeof(float) || typeof(T) == typeof(double))
                {
                    return CosineSimilarityFloatingPoint(a, aMagnitude, b, bMagnitude);
                }

                return CosineSimilarityIntegers(a, aMagnitude, b, bMagnitude);
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            private static TResult CosineSimilarityIntegers<T, TResult>(ReadOnlySpan<T> a, TResult aMagnitude, ReadOnlySpan<T> b, TResult bMagnitude)
                where T : unmanaged, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                long ab = 0, a2 = 0, b2 = 0;
                for (int i = 0; i < a.Length; i++)
                {
                    long rai = long.CreateTruncating(a[i]);
                    long rbi = long.CreateTruncating(b[i]);

                    ab += rai * rbi;
                    a2 += rai * rai;
                    b2 += rbi * rbi;
                }

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (ab == 0)
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                TResult fab = aMagnitude * bMagnitude * TResult.CreateTruncating(ab);
                TResult fa2 = aMagnitude * aMagnitude * TResult.CreateTruncating(a2);
                TResult fb2 = bMagnitude * bMagnitude * TResult.CreateTruncating(b2);
                return CosineSimilarityNormalize<TResult, TResult>(fab, fa2, fb2);
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarityFloatingPoint<T, TResult>(ReadOnlySpan<T> a, TResult aMagnitude, ReadOnlySpan<T> b, TResult bMagnitude)
                where T : unmanaged, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                TResult ab = TResult.Zero, a2 = TResult.Zero, b2 = TResult.Zero;
                for (int i = 0; i < a.Length; i++)
                {
                    ab += TResult.CreateTruncating(a[i] * b[i]);
                    a2 += TResult.CreateTruncating(a[i] * a[i]);
                    b2 += TResult.CreateTruncating(b[i] * b[i]);
                }

                // Special cases
                if (a2 == TResult.Zero && b2 == TResult.Zero)
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (ab == TResult.Zero)
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                TResult fab = aMagnitude * bMagnitude * TResult.CreateTruncating(ab);
                TResult fa2 = aMagnitude * aMagnitude * TResult.CreateTruncating(a2);
                TResult fb2 = bMagnitude * bMagnitude * TResult.CreateTruncating(b2);
                return CosineSimilarityNormalize<TResult, TResult>(fab, fa2, fb2);
            }

            internal static long DotProductNibbles(ReadOnlySpan<byte> nibbles, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
            {
                long dot = 0;
                for (var i = 0; i < nibbles.Length; i++)
                    dot += (nibbles[i] & 15) * even[i] + (nibbles[i] >> 4) * odd[i];
                return dot;
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

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static double CosineSimilarityInternalX64(ref float aRef, float aMagnitude, ref float bRef, float bMagnitude, nuint size)
            {
                Vector512<float> abVec = Vector512<float>.Zero;
                Vector512<float> a2Vec = Vector512<float>.Zero;
                Vector512<float> b2Vec = Vector512<float>.Zero;

                nuint i = 0;
                nuint oneVectorFromEnd = size - (nuint)Vector512<float>.Count;

            Loop:

                // PERF: The reason why this would work on hardware not supporting 512-bit vectors is
                // that it will effectively create 2 lanes (xmm and ymm) of 256-bit vectors. And because
                // there are no overlapping lanes, there will be less pipeline dependencies hiding latency
                // of the instructions themselves.
                Vector512<float> aVec = Vector512.LoadUnsafe(ref aRef, i);
                Vector512<float> bVec = Vector512.LoadUnsafe(ref bRef, i);

                i += (nuint)Vector512<float>.Count;

            LoopWithoutLoad:

                abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);

                if (i <= oneVectorFromEnd)
                    goto Loop;

                if (i != (nuint)size)
                {
                    nuint offset = size - i;
                    Debug.Assert((int)offset * sizeof(float) + Vector512<byte>.Count <= MoveMaskTable.Length);

                    var mask = Vector512.LoadUnsafe<float>(
                        ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(MoveMaskTable)), (nuint)offset);

                    aVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref aRef, oneVectorFromEnd), mask);
                    bVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref bRef, oneVectorFromEnd), mask);

                    i = (nuint)size;
                    goto LoopWithoutLoad;
                }

                float ab = aMagnitude * bMagnitude * Vector512.Sum(abVec);
                float a2 = aMagnitude * aMagnitude * Vector512.Sum(a2Vec);
                float b2 = bMagnitude * bMagnitude * Vector512.Sum(b2Vec);

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return double.NaN; // Both zero vectors: nan
                if (ab == 0)
                    return 0; // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                // Create a 128-bit vector with a2 in the high lane and b2 in the low lane.
                // Note: _mm_set_pd(a2, b2) in C sets lane1=a2 and lane0=b2.
                // In .NET, Vector128.Create(x, y) sets lane0 = x and lane1 = y.
                // So we swap the order.
                var squares = Vector128.Create((double)b2, (double)a2);

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
                return  ab * a2Reciprocal * b2Reciprocal;
            }

            /// <summary>
            /// Σ aᵢ·bᵢ for an f32 span on x64 with AVX2+FMA or AVX-512. Two Vector512 accumulators
            /// are updated per loop iteration; on AVX2-only hardware the JIT lowers each Vector512
            /// FMA to a pair of Vector256 FMAs, yielding 4 independent ymm FMA chains that saturate
            /// the FMA issue ports. The two-accumulator shape keeps total ymm pressure at 4, inside
            /// the 16-register budget. The trailing partial vector is masked via
            /// <see cref="MoveMaskTable"/> so no scalar tail loop is required.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static double DotProductInternalX64(ref float aRef, ref float bRef, nuint size)
            {
                Vector512<float> ab0 = Vector512<float>.Zero;
                Vector512<float> ab1 = Vector512<float>.Zero;

                nuint step = (nuint)Vector512<float>.Count;
                nuint oneVectorFromEnd = size - step;
                nuint twoFromEnd = size >= 2 * step ? size - 2 * step : 0;
                nuint i = 0;

            Loop2:
                // Two independent FMA chains per iteration. Enter only when the span is wide
                // enough to load both vectors; the single-vector body below drains the rest.
                if (size >= 2 * step && i <= twoFromEnd)
                {
                    var a0 = Vector512.LoadUnsafe(ref aRef, i);
                    var b0 = Vector512.LoadUnsafe(ref bRef, i);
                    var a1 = Vector512.LoadUnsafe(ref aRef, i + step);
                    var b1 = Vector512.LoadUnsafe(ref bRef, i + step);

                    ab0 = Arithmetics.MultiplyAddEstimate(a0, b0, ab0);
                    ab1 = Arithmetics.MultiplyAddEstimate(a1, b1, ab1);

                    i += 2 * step;
                    goto Loop2;
                }

                if (i <= oneVectorFromEnd)
                {
                    var aV = Vector512.LoadUnsafe(ref aRef, i);
                    var bV = Vector512.LoadUnsafe(ref bRef, i);
                    ab0 = Arithmetics.MultiplyAddEstimate(aV, bV, ab0);
                    i += step;
                }

                if (i != size)
                {
                    nuint offset = size - i;
                    Debug.Assert((int)offset * sizeof(float) + Vector512<byte>.Count <= MoveMaskTable.Length);

                    var mask = Vector512.LoadUnsafe<float>(
                        ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(MoveMaskTable)), offset);

                    var aTail = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref aRef, oneVectorFromEnd), mask);
                    var bTail = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref bRef, oneVectorFromEnd), mask);
                    ab0 = Arithmetics.MultiplyAddEstimate(aTail, bTail, ab0);
                }

                return (double)Vector512.Sum(ab0 + ab1);
            }

            /// <summary>
            /// Σ aᵢ·bᵢ for an f32 span on AArch64/NEON. Two Vector512 accumulators are updated per
            /// loop iteration; the JIT lowers each Vector512 op onto four 128-bit NEON vectors,
            /// yielding independent FMA chains. Tail bytes are masked via <see cref="MoveMaskTable"/>.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static double DotProductInternalNeon(ref float aRef, ref float bRef, nuint size)
            {
                Vector512<float> ab0 = Vector512<float>.Zero;
                Vector512<float> ab1 = Vector512<float>.Zero;

                nuint step = (nuint)Vector512<float>.Count;
                nuint oneVectorFromEnd = size - step;
                nuint twoFromEnd = size >= 2 * step ? size - 2 * step : 0;
                nuint i = 0;

            Loop2:
                // Two independent FMA chains per iteration. Enter only when the span is wide
                // enough to load both vectors; the single-vector body below drains the rest.
                if (size >= 2 * step && i <= twoFromEnd)
                {
                    var a0 = Vector512.LoadUnsafe(ref aRef, i);
                    var b0 = Vector512.LoadUnsafe(ref bRef, i);
                    var a1 = Vector512.LoadUnsafe(ref aRef, i + step);
                    var b1 = Vector512.LoadUnsafe(ref bRef, i + step);

                    ab0 = Arithmetics.MultiplyAddEstimate(a0, b0, ab0);
                    ab1 = Arithmetics.MultiplyAddEstimate(a1, b1, ab1);

                    i += 2 * step;
                    goto Loop2;
                }

                if (i <= oneVectorFromEnd)
                {
                    var aV = Vector512.LoadUnsafe(ref aRef, i);
                    var bV = Vector512.LoadUnsafe(ref bRef, i);
                    ab0 = Arithmetics.MultiplyAddEstimate(aV, bV, ab0);
                    i += step;
                }

                if (i != size)
                {
                    nuint offset = size - i;
                    var mask = Vector512.LoadUnsafe<float>(
                        ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(MoveMaskTable)), offset);

                    var aTail = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref aRef, oneVectorFromEnd), mask);
                    var bTail = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref bRef, oneVectorFromEnd), mask);
                    ab0 = Arithmetics.MultiplyAddEstimate(aTail, bTail, ab0);
                }

                return (double)Vector512.Sum(ab0 + ab1);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static double CosineSimilarityInternalNeon(ref float aRef, float aMagnitude, ref float bRef, float bMagnitude, nuint size)
            {
                Vector512<float> abVec = Vector512<float>.Zero;
                Vector512<float> a2Vec = Vector512<float>.Zero;
                Vector512<float> b2Vec = Vector512<float>.Zero;

                nuint i = 0;
                nuint oneVectorFromEnd = size - (nuint)Vector512<float>.Count;

            Loop:

                // PERF: The reason why this would work on hardware not supporting 512-bit vectors is
                // that it will effectively create 2 lanes (xmm and ymm) of 256-bit vectors. And because
                // there are no overlapping lanes, there will be less pipeline dependencies hiding latency
                // of the instructions themselves.
                Vector512<float> aVec = Vector512.LoadUnsafe(ref aRef, i);
                Vector512<float> bVec = Vector512.LoadUnsafe(ref bRef, i);

                i += (nuint)Vector512<float>.Count;

            LoopWithoutLoad:

                abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);

                if (i <= oneVectorFromEnd)
                    goto Loop;

                if (i != (nuint)size)
                {
                    nuint offset = size - i;
                    Debug.Assert((int)offset * sizeof(float) + Vector512<byte>.Count <= MoveMaskTable.Length);

                    var mask = Vector512.LoadUnsafe<float>(
                        ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(MoveMaskTable)), (nuint)offset);

                    aVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref aRef, oneVectorFromEnd), mask);
                    bVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref bRef, oneVectorFromEnd), mask);

                    i = (nuint)size;
                    goto LoopWithoutLoad;
                }

                float ab = aMagnitude * bMagnitude * Vector512.Sum(abVec);
                float a2 = aMagnitude * aMagnitude * Vector512.Sum(a2Vec);
                float b2 = bMagnitude * bMagnitude * Vector512.Sum(b2Vec);

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return double.NaN; // Both zero vectors: nan
                if (ab == 0)
                    return 0; // Orthogonal or one zero: distance = 1, similarity 0

                // Create vector with the squared magnitudes
                var squares = Vector64.Create(float.CreateTruncating(b2), float.CreateTruncating(a2));

                // Compute reciprocal square root approximation
                Vector64<float> rsqrts = AdvSimd.ReciprocalSquareRootEstimate(squares);

                // Perform two rounds of Newton-Raphson refinement for better accuracy
                // Formula: rsqrt_new = rsqrt * (1.5 - 0.5 * x * rsqrt^2)
                // Which can be rewritten as: rsqrt * vrsqrts(x * rsqrt, rsqrt)
                rsqrts = AdvSimd.Multiply(rsqrts, AdvSimd.ReciprocalSquareRootStep(AdvSimd.Multiply(squares, rsqrts), rsqrts));
                rsqrts = AdvSimd.Multiply(rsqrts, AdvSimd.ReciprocalSquareRootStep(AdvSimd.Multiply(squares, rsqrts), rsqrts));

                // Extract the refined reciprocal square roots
                float rsqrtA = rsqrts.GetElement(0);
                float rsqrtB = rsqrts.GetElement(1);

                // Compute the cosine similarity
                return ab * rsqrtA * rsqrtB;
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static double CosineSimilarityInternalX64(ref double aRef, double aMagnitude, ref double bRef, double bMagnitude, nuint size)
            {
                Vector512<double> abVec = Vector512<double>.Zero;
                Vector512<double> a2Vec = Vector512<double>.Zero;
                Vector512<double> b2Vec = Vector512<double>.Zero;

                nuint i = 0;
                nuint oneVectorFromEnd = size - (nuint)Vector512<double>.Count;

            Loop:

                // PERF: The reason why this would work on hardware not supporting 512-bit vectors is
                // that it will effectively create 2 lanes (xmm and ymm) of 256-bit vectors. And because
                // there are no overlapping lanes, there will be less pipeline dependencies hiding latency
                // of the instructions themselves.
                Vector512<double> aVec = Vector512.LoadUnsafe(ref aRef, i);
                Vector512<double> bVec = Vector512.LoadUnsafe(ref bRef, i);

                i += (nuint)Vector512<double>.Count;

            LoopWithoutLoad:

                abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);

                if (i <= oneVectorFromEnd)
                    goto Loop;

                if (i != (nuint)size)
                {
                    nuint offset = (nuint)size - i;
                    Debug.Assert((int)offset * sizeof(double) + Vector512<byte>.Count <= MoveMaskTable.Length);

                    var mask = Vector512.LoadUnsafe<double>(
                        ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, double>(MoveMaskTable)), (nuint)offset);

                    aVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref aRef, oneVectorFromEnd), mask);
                    bVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref bRef, oneVectorFromEnd), mask);

                    i = (nuint)size;
                    goto LoopWithoutLoad;
                }

                double ab = aMagnitude * bMagnitude * Vector512.Sum(abVec);
                double a2 = aMagnitude * aMagnitude * Vector512.Sum(a2Vec);
                double b2 = bMagnitude * bMagnitude * Vector512.Sum(b2Vec);

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return double.NaN; // Both zero vectors: nan
                if (ab == 0)
                    return 0; // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                // Create a 128-bit vector with a2 in the high lane and b2 in the low lane.
                // Note: _mm_set_pd(a2, b2) in C sets lane1=a2 and lane0=b2.
                // In .NET, Vector128.Create(x, y) sets lane0 = x and lane1 = y.
                // So we swap the order.
                var squares = Vector128.Create((double)b2, (double)a2);

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
                return ab * a2Reciprocal * b2Reciprocal;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static double CosineSimilarityInternalNeon(ref double aRef, double aMagnitude, ref double bRef, double bMagnitude, nuint size)
            {
                Vector512<double> abVec = Vector512<double>.Zero;
                Vector512<double> a2Vec = Vector512<double>.Zero;
                Vector512<double> b2Vec = Vector512<double>.Zero;

                nuint i = 0;
                nuint oneVectorFromEnd = size - (nuint)Vector512<double>.Count;

            Loop:

                // PERF: The reason why this would work on hardware not supporting 512-bit vectors is
                // that it will effectively create 2 lanes (xmm and ymm) of 256-bit vectors. And because
                // there are no overlapping lanes, there will be less pipeline dependencies hiding latency
                // of the instructions themselves.
                Vector512<double> aVec = Vector512.LoadUnsafe(ref aRef, i);
                Vector512<double> bVec = Vector512.LoadUnsafe(ref bRef, i);

                i += (nuint)Vector512<double>.Count;

            LoopWithoutLoad:

                abVec = Arithmetics.MultiplyAddEstimate(aVec, bVec, abVec);
                a2Vec = Arithmetics.MultiplyAddEstimate(aVec, aVec, a2Vec);
                b2Vec = Arithmetics.MultiplyAddEstimate(bVec, bVec, b2Vec);

                if (i <= oneVectorFromEnd)
                    goto Loop;

                if (i != (nuint)size)
                {
                    nuint offset = (nuint)size - i;
                    Debug.Assert((int)offset * sizeof(double) + Vector512<byte>.Count <= MoveMaskTable.Length);

                    var mask = Vector512.LoadUnsafe<double>(
                        ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, double>(MoveMaskTable)), (nuint)offset);

                    aVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref aRef, oneVectorFromEnd), mask);
                    bVec = Vector512.BitwiseAnd(Vector512.LoadUnsafe(ref bRef, oneVectorFromEnd), mask);

                    i = (nuint)size;
                    goto LoopWithoutLoad;
                }

                double ab = aMagnitude * bMagnitude * Vector512.Sum(abVec);
                double a2 = aMagnitude * aMagnitude * Vector512.Sum(a2Vec);
                double b2 = bMagnitude * bMagnitude * Vector512.Sum(b2Vec);

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return double.NaN; // Both zero vectors: nan
                if (ab == 0)
                    return 0; // Orthogonal or one zero: distance = 1, similarity 0

                // Create vector with the squared magnitudes
                var squares = Vector64.Create(float.CreateTruncating(b2), float.CreateTruncating(a2));

                // Compute reciprocal square root approximation
                Vector64<float> rsqrts = AdvSimd.ReciprocalSquareRootEstimate(squares);

                // Perform two rounds of Newton-Raphson refinement for better accuracy
                // Formula: rsqrt_new = rsqrt * (1.5 - 0.5 * x * rsqrt^2)
                // Which can be rewritten as: rsqrt * vrsqrts(x * rsqrt, rsqrt)
                rsqrts = AdvSimd.Multiply(rsqrts, AdvSimd.ReciprocalSquareRootStep(AdvSimd.Multiply(squares, rsqrts), rsqrts));
                rsqrts = AdvSimd.Multiply(rsqrts, AdvSimd.ReciprocalSquareRootStep(AdvSimd.Multiply(squares, rsqrts), rsqrts));

                // Extract the refined reciprocal square roots
                float rsqrtA = rsqrts.GetElement(0);
                float rsqrtB = rsqrts.GetElement(1);

                // Compute the cosine similarity
                return ab * rsqrtA * rsqrtB;
            }


            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
                where T : unmanaged, IFloatingPoint<T>, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (a.Length < Vector512<T>.Count)
                    return Serial.CosineSimilarity<T, TResult>(a, b);

                double similarity;
                if (typeof(T) == typeof(float))
                {
                    ref var afRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, float>(a));
                    ref var bfRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, float>(b));

                    if (AdvInstructionSet.X86.IsSupportedSse)
                    {
                        similarity = CosineSimilarityInternalX64(ref afRef, 1.0f, ref bfRef, 1.0f, (nuint)a.Length);
                    }
                    else if (AdvInstructionSet.Arm.IsSupported)
                    {
                        similarity = CosineSimilarityInternalNeon(ref afRef, 1.0f, ref bfRef, 1.0f, (nuint)a.Length);
                    }
                    else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");
                } 
                else if (typeof(T) == typeof(double))
                {
                    ref var adRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, double>(a));
                    ref var bdRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, double>(b));

                    if (Sse2.IsSupported)
                    {
                        similarity = CosineSimilarityInternalX64(ref adRef, 1.0d, ref bdRef, 1.0d, (nuint)a.Length);
                    }
                    else if (AdvInstructionSet.Arm.IsSupported)
                    {
                        similarity = CosineSimilarityInternalNeon(ref adRef, 1.0d, ref bdRef, 1.0d, (nuint)a.Length);
                    }
                    else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");
                }
                else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");

                if (typeof(TResult) == typeof(float))
                {
                    return (TResult)(object)(float)similarity;
                }

                return (TResult)(object)similarity;
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            public static TResult CosineSimilarity<T, TResult>(ReadOnlySpan<T> a, TResult aMagnitude, ReadOnlySpan<T> b, TResult bMagnitude)
                where T : unmanaged, IFloatingPoint<T>, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (a.Length < Vector512<T>.Count)
                    return Serial.CosineSimilarity<T, TResult>(a, b);

                double similarity;
                if (typeof(T) == typeof(float))
                {
                    float aMag, bMag;
                    if (typeof(TResult) == typeof(float))
                    {
                        aMag = (float)(object)aMagnitude;
                        bMag = (float)(object)bMagnitude;
                    }
                    else if (typeof(TResult) == typeof(double))
                    {
                        aMag = (float)(double)(object)aMagnitude;
                        bMag = (float)(double)(object)bMagnitude;
                    }
                    else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");

                    ref var adRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, float>(a));
                    ref var bdRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, float>(b));

                    if (Sse2.IsSupported)
                    {
                        similarity = CosineSimilarityInternalX64(ref adRef, aMag, ref bdRef, bMag, (nuint)a.Length);
                    }
                    else if (AdvInstructionSet.Arm.IsSupported)
                    {
                        similarity = CosineSimilarityInternalNeon(ref adRef, aMag, ref bdRef, bMag, (nuint)a.Length);
                    }
                    else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");
                }
                else if (typeof(T) == typeof(double))
                {
                    double aMag, bMag;
                    if (typeof(TResult) == typeof(float))
                    {
                        aMag = (float)(object)aMagnitude;
                        bMag = (float)(object)bMagnitude;
                    }
                    else if (typeof(TResult) == typeof(double))
                    {
                        aMag = (double)(object)aMagnitude;
                        bMag = (double)(object)bMagnitude;
                    }
                    else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");

                    ref var adRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, double>(a));
                    ref var bdRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, double>(b));

                    if (Sse2.IsSupported)
                    {
                        similarity = CosineSimilarityInternalX64(ref adRef, aMag, ref bdRef, bMag, (nuint)a.Length);
                    }
                    else if (AdvInstructionSet.Arm.IsSupported)
                    {
                        similarity = CosineSimilarityInternalNeon(ref adRef, aMag, ref bdRef, bMag, (nuint)a.Length);
                    }
                    else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");
                }
                else throw new NotSupportedException($"Type {typeof(T).Name} is not supported.");

                if (typeof(TResult) == typeof(float))
                {
                    return (TResult)(object)(float)similarity;
                }

                return (TResult)(object)similarity;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityIntegersAvx512<TResult>(ReadOnlySpan<sbyte> a, TResult aMagnitude, ReadOnlySpan<sbyte> b, TResult bMagnitude)
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (Avx512BW.IsSupported == false || Avx512F.IsSupported == false)
                    throw new NotSupportedException("This method should not be called on the current architecture");

                // 1) 512-bit accumulators for ab, a2, b2
                var abVec = Vector512<int>.Zero;
                var a2Vec = Vector512<int>.Zero;
                var b2Vec = Vector512<int>.Zero;

                int i = a.Length;

                ref sbyte aRef = ref MemoryMarshal.GetReference(a);
                ref sbyte bRef = ref MemoryMarshal.GetReference(b);

            Loop:

                // 2) load full 32-byte block
                Vector512<short> aVec = Avx512BW.ConvertToVector512Int16(Vector256.LoadUnsafe(ref aRef));
                Vector512<short> bVec = Avx512BW.ConvertToVector512Int16(Vector256.LoadUnsafe(ref bRef));

                // 3) multiply-add pairs: i16×i16→i32
                abVec = Avx512F.Add(abVec, Avx512BW.MultiplyAddAdjacent(aVec, bVec));
                a2Vec = Avx512F.Add(a2Vec, Avx512BW.MultiplyAddAdjacent(aVec, aVec));
                b2Vec = Avx512F.Add(b2Vec, Avx512BW.MultiplyAddAdjacent(bVec, bVec));

                i -= Vector256<sbyte>.Count;
                aRef = ref Unsafe.Add(ref aRef, Vector256<sbyte>.Count);
                bRef = ref Unsafe.Add(ref bRef, Vector256<sbyte>.Count);

                if (i >= Vector256<sbyte>.Count)
                    goto Loop;

                // 4) horizontal reductions
                int ab = Vector512.Sum(abVec);
                int a2 = Vector512.Sum(a2Vec);
                int b2 = Vector512.Sum(b2Vec);

                // Tail loop for remaining elements
                while (i > 0)
                {
                    ab += aRef * bRef;
                    a2 += aRef * aRef;
                    b2 += bRef * bRef;

                    i--;
                    aRef = ref Unsafe.Add(ref aRef, 1);
                    bRef = ref Unsafe.Add(ref bRef, 1);
                }

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (ab == 0)
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Apply magnitudes and normalise
                TResult fab = TResult.CreateTruncating(ab) * aMagnitude * bMagnitude;
                TResult fa2 = TResult.CreateTruncating(a2) * aMagnitude * aMagnitude;
                TResult fb2 = TResult.CreateTruncating(b2) * bMagnitude * bMagnitude;

                return CosineSimilarityNormalize<TResult, TResult>(fab, fa2, fb2);
            }

            internal static long DotProductNibblesAvx512(ReadOnlySpan<byte> nibbles, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
            {
                ref var l = ref MemoryMarshal.GetReference(nibbles);
                ref var e = ref MemoryMarshal.GetReference(even);
                ref var o = ref MemoryMarshal.GetReference(odd);
                var mask = Vector512.Create((short)15);
                var i = 0;
                long dot = 0;
                while (i + 32 <= nibbles.Length)
                {
                    // A step adds at most 4 * 15 * 2^15 to an int lane, so the sixteen lanes of 64 steps sum within an int.
                    var end = Math.Min(i + 64 * 32, nibbles.Length - 31);
                    Vector512<int> sum = default;
                    for (; i < end; i += 32)
                    {
                        var bytes = Avx512BW.ConvertToVector512Int16(Vector256.LoadUnsafe(ref l, (nuint)i));
                        sum += Avx512BW.MultiplyAddAdjacent(bytes & mask, Vector512.LoadUnsafe(ref e, (nuint)i));
                        sum += Avx512BW.MultiplyAddAdjacent(Vector512.ShiftRightLogical(bytes, 4), Vector512.LoadUnsafe(ref o, (nuint)i));
                    }

                    dot += Vector512.Sum(sum);
                }

                return dot + Vectorized256.DotProductNibblesAvx2(nibbles[i..], even[i..], odd[i..]);
            }
        }

        public static class Vectorized128
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityIntegersNeon<TResult>(ReadOnlySpan<sbyte> a, TResult aMagnitude, ReadOnlySpan<sbyte> b, TResult bMagnitude)
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (Dp.IsSupported == false)
                    throw new NotSupportedException("This method should not be called on the current architecture");

                Vector128<int> abVec = Vector128<int>.Zero;
                Vector128<int> a2Vec = Vector128<int>.Zero;
                Vector128<int> b2Vec = Vector128<int>.Zero;

                int i = a.Length;

                ref sbyte aRef = ref MemoryMarshal.GetReference(a);
                ref sbyte bRef = ref MemoryMarshal.GetReference(b);
                
                Loop:
                
                // Load 16 signed-bytes from each span
                Vector128<sbyte> aVec = Vector128.LoadUnsafe(ref aRef);
                Vector128<sbyte> bVec = Vector128.LoadUnsafe(ref bRef);

                // 4 × dot-product accumulating per 4 lanes
                abVec = Dp.DotProduct(abVec, aVec, bVec);
                a2Vec = Dp.DotProduct(a2Vec, aVec, aVec);
                b2Vec = Dp.DotProduct(b2Vec, bVec, bVec);

                i -= Vector128<sbyte>.Count;
                aRef = ref Unsafe.Add(ref aRef, Vector128<sbyte>.Count);
                bRef = ref Unsafe.Add(ref bRef, Vector128<sbyte>.Count);
                if (i >= Vector128<sbyte>.Count)
                    goto Loop;

                int ab = Vector128.Sum(abVec);
                int a2 = Vector128.Sum(a2Vec);
                int b2 = Vector128.Sum(b2Vec);
                while (i > 0)
                {
                    ab += aRef * bRef;
                    a2 += aRef * aRef;
                    b2 += bRef * bRef;

                    i--;
                    aRef = ref Unsafe.Add(ref aRef, 1);
                    bRef = ref Unsafe.Add(ref bRef, 1);
                }

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (ab == 0)
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Apply magnitudes and normalise
                TResult fab = TResult.CreateTruncating(ab) * aMagnitude * bMagnitude;
                TResult fa2 = TResult.CreateTruncating(a2) * aMagnitude * aMagnitude;
                TResult fb2 = TResult.CreateTruncating(b2) * bMagnitude * bMagnitude;
                return CosineSimilarityNormalize<TResult, TResult>(fab, fa2, fb2);
            }
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityIntegers<TResult>(ReadOnlySpan<sbyte> a, TResult aMagnitude, ReadOnlySpan<sbyte> b, TResult bMagnitude)
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (Sse2.IsSupported == false && AdvSimd.IsSupported == false)
                    throw new NotSupportedException("This method should not be called on the current architecture");

                ref sbyte aRef = ref MemoryMarshal.GetReference(a);
                ref sbyte bRef = ref MemoryMarshal.GetReference(b);

                // A step adds at most 2^16 to an int lane, so spans up to 512 KB sum within an int.
                Vector128<int> abVec = default, a2Vec = default, b2Vec = default;
                int i = 0;
                for (; i + Vector128<sbyte>.Count <= a.Length; i += Vector128<sbyte>.Count)
                {
                    var (aLo, aHi) = Vector128.Widen(Vector128.LoadUnsafe(ref aRef, (nuint)i));
                    var (bLo, bHi) = Vector128.Widen(Vector128.LoadUnsafe(ref bRef, (nuint)i));
                    abVec += MultiplyAddAdjacent(aLo, bLo) + MultiplyAddAdjacent(aHi, bHi);
                    a2Vec += MultiplyAddAdjacent(aLo, aLo) + MultiplyAddAdjacent(aHi, aHi);
                    b2Vec += MultiplyAddAdjacent(bLo, bLo) + MultiplyAddAdjacent(bHi, bHi);
                }

                int ab = Vector128.Sum(abVec), a2 = Vector128.Sum(a2Vec), b2 = Vector128.Sum(b2Vec);
                for (; i < a.Length; i++)
                {
                    ab += a[i] * b[i];
                    a2 += a[i] * a[i];
                    b2 += b[i] * b[i];
                }

                if (a2 == 0 && b2 == 0)
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (ab == 0)
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                TResult fab = TResult.CreateTruncating(ab) * aMagnitude * bMagnitude;
                TResult fa2 = TResult.CreateTruncating(a2) * aMagnitude * aMagnitude;
                TResult fb2 = TResult.CreateTruncating(b2) * bMagnitude * bMagnitude;
                return CosineSimilarityNormalize<TResult, TResult>(fab, fa2, fb2);

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static Vector128<int> MultiplyAddAdjacent(Vector128<short> left, Vector128<short> right) =>
                    Sse2.IsSupported
                        ? Sse2.MultiplyAddAdjacent(left, right)
                        : AdvSimd.MultiplyWideningUpperAndAdd(AdvSimd.MultiplyWideningLower(left.GetLower(), right.GetLower()), left, right);
            }

            internal static long HammingBitDistanceAdvSimd(ReadOnlySpan<byte> x, ReadOnlySpan<byte> y)
            {
                if (AdvSimd.IsSupported == false)
                    throw new NotSupportedException("This method should not be called on the current architecture");

                ref byte xRef = ref MemoryMarshal.GetReference(x);
                ref byte yRef = ref MemoryMarshal.GetReference(y);

                // A step adds at most 128 to a uint lane, so no span overflows it. Four blocks per step keep the adds independent.
                const int Block = 16;
                Vector128<uint> counts = default;
                int i = 0;
                for (; i + 4 * Block <= x.Length; i += 4 * Block)
                {
                    var c0 = AdvSimd.PopCount(Vector128.LoadUnsafe(ref xRef, (nuint)i) ^ Vector128.LoadUnsafe(ref yRef, (nuint)i));
                    var c1 = AdvSimd.PopCount(Vector128.LoadUnsafe(ref xRef, (nuint)(i + Block)) ^ Vector128.LoadUnsafe(ref yRef, (nuint)(i + Block)));
                    var c2 = AdvSimd.PopCount(Vector128.LoadUnsafe(ref xRef, (nuint)(i + 2 * Block)) ^ Vector128.LoadUnsafe(ref yRef, (nuint)(i + 2 * Block)));
                    var c3 = AdvSimd.PopCount(Vector128.LoadUnsafe(ref xRef, (nuint)(i + 3 * Block)) ^ Vector128.LoadUnsafe(ref yRef, (nuint)(i + 3 * Block)));
                    counts = AdvSimd.AddPairwiseWideningAndAdd(counts, AdvSimd.AddPairwiseWidening((c0 + c1) + (c2 + c3)));
                }

                for (; i + Block <= x.Length; i += Block)
                {
                    var bits = Vector128.LoadUnsafe(ref xRef, (nuint)i) ^ Vector128.LoadUnsafe(ref yRef, (nuint)i);
                    counts = AdvSimd.AddPairwiseWideningAndAdd(counts, AdvSimd.AddPairwiseWidening(AdvSimd.PopCount(bits)));
                }

                return (long)Vector128.Sum(AdvSimd.AddPairwiseWidening(counts)) + Serial.HammingBitDistance(x[i..], y[i..]);
            }

            internal static unsafe long HammingBitDistanceSve(ReadOnlySpan<byte> x, ReadOnlySpan<byte> y)
            {
                if (Sve.IsSupported == false)
                    throw new NotSupportedException("This method should not be called on the current architecture");

                // SVE counts the bits of whole 64-bit lanes, so the counts add without widening. Four accumulators keep the adds independent.
                int n = Vector<byte>.Count;
                Vector<ulong> c0 = default, c1 = default, c2 = default, c3 = default;
                int i = 0;
                fixed (byte* xPtr = x, yPtr = y)
                {
                    for (; i + 4 * n <= x.Length; i += 4 * n)
                    {
                        c0 += Sve.PopCount(Vector.AsVectorUInt64(Vector.Load(xPtr + i) ^ Vector.Load(yPtr + i)));
                        c1 += Sve.PopCount(Vector.AsVectorUInt64(Vector.Load(xPtr + i + n) ^ Vector.Load(yPtr + i + n)));
                        c2 += Sve.PopCount(Vector.AsVectorUInt64(Vector.Load(xPtr + i + 2 * n) ^ Vector.Load(yPtr + i + 2 * n)));
                        c3 += Sve.PopCount(Vector.AsVectorUInt64(Vector.Load(xPtr + i + 3 * n) ^ Vector.Load(yPtr + i + 3 * n)));
                    }

                    // The predicated loads read zeros past the end of the spans.
                    for (; i < x.Length; i += n)
                    {
                        var active = Sve.CreateWhileLessThanMaskByte(i, x.Length);
                        c0 += Sve.PopCount(Vector.AsVectorUInt64(Sve.LoadVector(active, xPtr + i) ^ Sve.LoadVector(active, yPtr + i)));
                    }
                }

                return (long)Vector.Sum((c0 + c1) + (c2 + c3));
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityNormalizeNeon<T, TResult>(T ab, T a2, T b2)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                // Create vector with the squared magnitudes
                var squares = Vector64.Create(float.CreateTruncating(b2), float.CreateTruncating(a2));

                // Compute reciprocal square root approximation
                Vector64<float> rsqrts = AdvSimd.ReciprocalSquareRootEstimate(squares);

                // Perform two rounds of Newton-Raphson refinement for better accuracy
                // Formula: rsqrt_new = rsqrt * (1.5 - 0.5 * x * rsqrt^2)
                // Which can be rewritten as: rsqrt * vrsqrts(x * rsqrt, rsqrt)
                rsqrts = AdvSimd.Multiply(rsqrts, AdvSimd.ReciprocalSquareRootStep(AdvSimd.Multiply(squares, rsqrts), rsqrts));
                rsqrts = AdvSimd.Multiply(rsqrts, AdvSimd.ReciprocalSquareRootStep(AdvSimd.Multiply(squares, rsqrts), rsqrts));

                // Extract the refined reciprocal square roots
                float a2Reciprocal = rsqrts.GetElement(0);
                float b2Reciprocal = rsqrts.GetElement(1);

                return TResult.CreateTruncating(float.CreateTruncating(ab) * a2Reciprocal * b2Reciprocal);
            }

            internal static long DotProductNibblesSve(ReadOnlySpan<byte> nibbles, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
            {
                ref var l = ref MemoryMarshal.GetReference(nibbles);
                ref var e = ref MemoryMarshal.GetReference(even);
                ref var o = ref MemoryMarshal.GetReference(odd);
                var mask = new Vector<ushort>(15);
                var half = Vector<short>.Count;
                var i = 0;
                // A step adds at most 16 * 15 * 2^15 to a long lane, so no span overflows it.
                Vector<long> sum = default;
                for (; i + Vector<byte>.Count <= nibbles.Length; i += Vector<byte>.Count)
                {
                    var bytes = Vector.LoadUnsafe(ref l, (nuint)i);
                    var low = Vector.WidenLower(bytes);
                    var high = Vector.WidenUpper(bytes);
                    var step = Sve.DotProduct(Vector<long>.Zero, Vector.AsVectorInt16(low & mask), Vector.LoadUnsafe(ref e, (nuint)i));
                    step = Sve.DotProduct(step, Vector.AsVectorInt16(high & mask), Vector.LoadUnsafe(ref e, (nuint)(i + half)));
                    step = Sve.DotProduct(step, Vector.AsVectorInt16(Vector.ShiftRightLogical(low, 4)), Vector.LoadUnsafe(ref o, (nuint)i));
                    step = Sve.DotProduct(step, Vector.AsVectorInt16(Vector.ShiftRightLogical(high, 4)), Vector.LoadUnsafe(ref o, (nuint)(i + half)));
                    sum += step;
                }

                return Vector.Sum(sum) + Serial.DotProductNibbles(nibbles[i..], even[i..], odd[i..]);
            }

            internal static long DotProductNibbles(ReadOnlySpan<byte> nibbles, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
            {
                ref var l = ref MemoryMarshal.GetReference(nibbles);
                ref var e = ref MemoryMarshal.GetReference(even);
                ref var o = ref MemoryMarshal.GetReference(odd);
                var mask = Vector128.Create((ushort)15);
                var i = 0;
                long dot = 0;
                while (i + 16 <= nibbles.Length)
                {
                    // A step adds at most 8 * 15 * 2^15 to an int lane, so the four lanes of 128 steps sum within an int.
                    var end = Math.Min(i + 128 * 16, nibbles.Length - 15);
                    Vector128<int> sum = default;
                    for (; i < end; i += 16)
                    {
                        var (low, high) = Vector128.Widen(Vector128.LoadUnsafe(ref l, (nuint)i));
                        var step = MultiplyAdd(Vector128<int>.Zero, (low & mask).AsInt16(), Vector128.LoadUnsafe(ref e, (nuint)i));
                        step = MultiplyAdd(step, (high & mask).AsInt16(), Vector128.LoadUnsafe(ref e, (nuint)(i + 8)));
                        step = MultiplyAdd(step, Vector128.ShiftRightLogical(low, 4).AsInt16(), Vector128.LoadUnsafe(ref o, (nuint)i));
                        step = MultiplyAdd(step, Vector128.ShiftRightLogical(high, 4).AsInt16(), Vector128.LoadUnsafe(ref o, (nuint)(i + 8)));
                        sum += step;
                    }

                    dot += Vector128.Sum(sum);
                }

                return dot + Serial.DotProductNibbles(nibbles[i..], even[i..], odd[i..]);

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static Vector128<int> MultiplyAdd(Vector128<int> sum, Vector128<short> left, Vector128<short> right) =>
                    Sse2.IsSupported
                        ? sum + Sse2.MultiplyAddAdjacent(left, right)
                        : AdvSimd.MultiplyWideningUpperAndAdd(AdvSimd.MultiplyWideningLowerAndAdd(sum, left.GetLower(), right.GetLower()), left, right);
            }
        }

        public static class Vectorized256
        {
            internal static long HammingBitDistanceAvx2(ReadOnlySpan<byte> x, ReadOnlySpan<byte> y)
            {
                if (Avx2.IsSupported == false)
                    throw new NotSupportedException("This method should not be called on the current architecture");

                ref byte xRef = ref MemoryMarshal.GetReference(x);
                ref byte yRef = ref MemoryMarshal.GetReference(y);

                // Muła, Kurz and Lemire, "Faster Population Counts Using AVX2 Instructions": a shuffle looks up the bit count of each nibble and a sum of absolute differences folds the bytes into longs.
                var nibbleCounts = Vector256.Create((byte)0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4);
                var lowNibble = Vector256.Create((byte)15);
                Vector256<ulong> counts = default;
                int i = 0;
                for (; i + Vector256<byte>.Count <= x.Length; i += Vector256<byte>.Count)
                {
                    var bits = Vector256.LoadUnsafe(ref xRef, (nuint)i) ^ Vector256.LoadUnsafe(ref yRef, (nuint)i);
                    var bytes = Avx2.Shuffle(nibbleCounts, bits & lowNibble) + Avx2.Shuffle(nibbleCounts, Vector256.ShiftRightLogical(bits.AsUInt16(), 4).AsByte() & lowNibble);
                    counts += Avx2.SumAbsoluteDifferences(bytes, Vector256<byte>.Zero).AsUInt64();
                }

                return (long)Vector256.Sum(counts) + Serial.HammingBitDistance(x[i..], y[i..]);
            }

            [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityNormalizeSse<T, TResult>(T ab, T a2, T b2)
                where T : unmanaged, IRootFunctions<T>, INumber<T>
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
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
                while (i > 0)
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
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Normalization
                return CosineSimilarityNormalize<T, TResult>(ab, a2, b2);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static TResult CosineSimilarityIntegersAvx2<TResult>(ReadOnlySpan<sbyte> a, TResult aMagnitude, ReadOnlySpan<sbyte> b, TResult bMagnitude)
                where TResult : unmanaged, IFloatingPoint<TResult>, IRootFunctions<TResult>, INumber<TResult>
            {
                if (Avx2.IsSupported == false)
                    throw new NotSupportedException("This method should not be called on the current architecture");

                //  Prepare six 256-bit accumulators: low/high lanes for ab, a2, b2
                Vector256<int> abLo = Vector256<int>.Zero, abHi = Vector256<int>.Zero;
                Vector256<int> a2Lo = Vector256<int>.Zero, a2Hi = Vector256<int>.Zero;
                Vector256<int> b2Lo = Vector256<int>.Zero, b2Hi = Vector256<int>.Zero;

                int i = a.Length;

                ref sbyte aRef = ref MemoryMarshal.GetReference(a);
                ref sbyte bRef = ref MemoryMarshal.GetReference(b);

                Loop:
                // Load 32 signed-bytes from each span
                Vector256<sbyte> aVec = Vector256.LoadUnsafe(ref aRef);
                Vector256<sbyte> bVec = Vector256.LoadUnsafe(ref bRef);

                // Unpack signed‐byte → signed‐short for low/high 128‐bit lanes
                Vector256<short> aVecLo = Avx2.ConvertToVector256Int16(aVec.GetLower());
                Vector256<short> aVecHi = Avx2.ConvertToVector256Int16(aVec.GetUpper());
                Vector256<short> bVecLo = Avx2.ConvertToVector256Int16(bVec.GetLower());
                Vector256<short> bVecHi = Avx2.ConvertToVector256Int16(bVec.GetUpper());

                // Multiply‐add adjacent pairs: produces four i32 results per 128‐bit lane

                // Perform i16×i16→i32 multiply-add on adjacent pairs:
                //    - ab += a * b
                //    - a2 += a * a
                //    - b2 += b * b
                abLo = Avx2.Add(abLo, Avx2.MultiplyAddAdjacent(aVecLo, bVecLo));
                abHi = Avx2.Add(abHi, Avx2.MultiplyAddAdjacent(aVecHi, bVecHi));

                a2Lo = Avx2.Add(a2Lo, Avx2.MultiplyAddAdjacent(aVecLo, aVecLo));
                a2Hi = Avx2.Add(a2Hi, Avx2.MultiplyAddAdjacent(aVecHi, aVecHi));

                b2Lo = Avx2.Add(b2Lo, Avx2.MultiplyAddAdjacent(bVecLo, bVecLo));
                b2Hi = Avx2.Add(b2Hi, Avx2.MultiplyAddAdjacent(bVecHi, bVecHi));

                i -= Vector256<sbyte>.Count;
                aRef = ref Unsafe.Add(ref aRef, Vector256<sbyte>.Count);
                bRef = ref Unsafe.Add(ref bRef, Vector256<sbyte>.Count);
                if (i >= Vector256<sbyte>.Count)
                    goto Loop;

                // Horizontal reduction across the two 128‐bit halves
                int ab = Vector256.Sum(Avx2.Add(abLo, abHi));
                int a2 = Vector256.Sum(Avx2.Add(a2Lo, a2Hi));
                int b2 = Vector256.Sum(Avx2.Add(b2Lo, b2Hi));

                // Tail loop for remaining elements
                while (i > 0)
                {
                    ab += aRef * bRef;
                    a2 += aRef * aRef;
                    b2 += bRef * bRef;

                    i--;
                    aRef = ref Unsafe.Add(ref aRef, 1);
                    bRef = ref Unsafe.Add(ref bRef, 1);
                }

                // Special cases
                if (a2 == 0 && b2 == 0)
                    return TResult.CreateTruncating(double.NaN); // Both zero vectors: nan
                if (ab == 0)
                    return TResult.Zero; // Orthogonal or one zero: distance = 1, similarity 0

                // Apply magnitudes and normalise
                TResult fab = TResult.CreateTruncating(ab) * aMagnitude * bMagnitude;
                TResult fa2 = TResult.CreateTruncating(a2) * aMagnitude * aMagnitude;
                TResult fb2 = TResult.CreateTruncating(b2) * bMagnitude * bMagnitude;
                return CosineSimilarityNormalize<TResult, TResult>(fab, fa2, fb2);
            }

            internal static long DotProductNibblesAvxVnni(ReadOnlySpan<byte> nibbles, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
            {
                ref var l = ref MemoryMarshal.GetReference(nibbles);
                ref var e = ref MemoryMarshal.GetReference(even);
                ref var o = ref MemoryMarshal.GetReference(odd);
                var mask = Vector256.Create((short)15);
                var i = 0;
                long dot = 0;
                while (i + 16 <= nibbles.Length)
                {
                    // A step adds at most 4 * 15 * 2^15 to an int lane, so the eight lanes of 128 steps sum within an int.
                    var end = Math.Min(i + 128 * 16, nibbles.Length - 15);
                    Vector256<int> sum = default;
                    for (; i < end; i += 16)
                    {
                        var bytes = Avx2.ConvertToVector256Int16(Vector128.LoadUnsafe(ref l, (nuint)i));
                        var evenProducts = Avx2.MultiplyAddAdjacent(bytes & mask, Vector256.LoadUnsafe(ref e, (nuint)i));
                        sum += AvxVnni.MultiplyWideningAndAdd(evenProducts, Vector256.ShiftRightLogical(bytes, 4), Vector256.LoadUnsafe(ref o, (nuint)i));
                    }

                    dot += Vector256.Sum(sum);
                }

                return dot + Serial.DotProductNibbles(nibbles[i..], even[i..], odd[i..]);
            }

            internal static long DotProductNibblesAvx2(ReadOnlySpan<byte> nibbles, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
            {
                ref var l = ref MemoryMarshal.GetReference(nibbles);
                ref var e = ref MemoryMarshal.GetReference(even);
                ref var o = ref MemoryMarshal.GetReference(odd);
                var mask = Vector256.Create((short)15);
                var i = 0;
                long dot = 0;
                while (i + 16 <= nibbles.Length)
                {
                    // A step adds at most 4 * 15 * 2^15 to an int lane, so the eight lanes of 128 steps sum within an int.
                    var end = Math.Min(i + 128 * 16, nibbles.Length - 15);
                    Vector256<int> sum = default;
                    for (; i < end; i += 16)
                    {
                        var bytes = Avx2.ConvertToVector256Int16(Vector128.LoadUnsafe(ref l, (nuint)i));
                        sum += Avx2.MultiplyAddAdjacent(bytes & mask, Vector256.LoadUnsafe(ref e, (nuint)i));
                        sum += Avx2.MultiplyAddAdjacent(Vector256.ShiftRightLogical(bytes, 4), Vector256.LoadUnsafe(ref o, (nuint)i));
                    }

                    dot += Vector256.Sum(sum);
                }

                return dot + Serial.DotProductNibbles(nibbles[i..], even[i..], odd[i..]);
            }
        }

        /// <summary>
        /// The dot product of 4-bit unsigned values, packed two to a byte, with shorts: the low nibble of byte i multiplies <paramref name="even"/>[i] and
        /// the high nibble <paramref name="odd"/>[i]. The sum is exact, so every path returns the same value.
        /// </summary>
        // PERF: The bytes widen to shorts; a mask takes the low nibbles and a shift the high ones, and each multiplies consecutive shorts of its half, with
        // no shuffle. No multiply-add accumulates onto the running sum, which waits on one add per step and not on the latency of a multiply-add.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long DotProductNibbles(ReadOnlySpan<byte> nibbles, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
        {
            Debug.Assert(even.Length == nibbles.Length && odd.Length == nibbles.Length, "even.Length == nibbles.Length && odd.Length == nibbles.Length");

            if (Avx512BW.IsSupported)
                return Vectorized512.DotProductNibblesAvx512(nibbles, even, odd);
            if (AvxVnni.IsSupported)
                return Vectorized256.DotProductNibblesAvxVnni(nibbles, even, odd);
            if (Avx2.IsSupported)
                return Vectorized256.DotProductNibblesAvx2(nibbles, even, odd);
            if (Sve.IsSupported)
                return Vectorized128.DotProductNibblesSve(nibbles, even, odd);
            if (Sse2.IsSupported || AdvSimd.IsSupported)
                return Vectorized128.DotProductNibbles(nibbles, even, odd);
            return Serial.DotProductNibbles(nibbles, even, odd);
        }

        /// <summary>Applies the Walsh-Hadamard transform, without normalization, in place to <paramref name="values"/>, whose length is a power of two.</summary>
        public static void WalshHadamardTransform(Span<double> values)
        {
            Debug.Assert(BitOperations.IsPow2(values.Length), "BitOperations.IsPow2(values.Length)");

            ref var x = ref MemoryMarshal.GetReference(values);
            for (var len = 1; len < values.Length; len <<= 1)
            {
                for (var i = 0; i < values.Length; i += 2 * len)
                {
                    var j = i;
                    // PERF: The butterflies of a level do not depend on each other, so a level at least as wide as a vector runs a vector of them at a time.
                    // Each output is the same sum or difference, so the result is identical bit for bit.
                    if (Vector.IsHardwareAccelerated && len >= Vector<double>.Count)
                    {
                        for (; j < i + len; j += Vector<double>.Count)
                        {
                            var a = Vector.LoadUnsafe(ref x, (nuint)j);
                            var b = Vector.LoadUnsafe(ref x, (nuint)(j + len));
                            (a + b).StoreUnsafe(ref x, (nuint)j);
                            (a - b).StoreUnsafe(ref x, (nuint)(j + len));
                        }
                    }

                    for (; j < i + len; j++)
                    {
                        var a = values[j];
                        var b = values[j + len];
                        values[j] = a + b;
                        values[j + len] = a - b;
                    }
                }
            }
        }

        /// <summary>Computes the bitwise Hamming distance between two equal-length tensors of values.</summary>
        /// <param name="x">The first tensor, represented as a span.</param>
        /// <param name="y">The second tensor, represented as a span.</param>
        /// <returns>The number of bits that differ between the two spans.</returns>
        /// <exception cref="ArgumentException">Length of <paramref name="x" /> must be same as length of <paramref name="y" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="x" /> and <paramref name="y" /> must not be empty.</exception>
        public static long HammingBitDistance<T>(ReadOnlySpan<T> x, ReadOnlySpan<T> y) where T : unmanaged, IBinaryInteger<T>
        {
            var xAsByte = MemoryMarshal.Cast<T, byte>(x);
            var yAsByte = MemoryMarshal.Cast<T, byte>(y);

            // PERF: AArch64 counts bits only in vector registers. x64 exposes no vector popcount, so AVX2 looks up the count of each nibble with a shuffle.
            if (AdvInstructionSet.Arm.IsSupported && Sve.IsSupported)
                return Vectorized128.HammingBitDistanceSve(xAsByte, yAsByte);
            if (AdvInstructionSet.Arm.IsSupported)
                return Vectorized128.HammingBitDistanceAdvSimd(xAsByte, yAsByte);
            if (AdvInstructionSet.X86.IsSupportedAvx256)
                return Vectorized256.HammingBitDistanceAvx2(xAsByte, yAsByte);
            return Serial.HammingBitDistance(xAsByte, yAsByte);
        }
    }
}
