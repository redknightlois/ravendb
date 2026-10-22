using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using Sparrow.Server;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    /// <summary>
    /// A block of code rows is one overflow allocation of the fewest pages that hold a row and leave at most an eighth of the block unused, and holds every
    /// row that fits them. Graceful degradation for wide rows: a block spans several pages, so a commit that appends to it copies, and an encrypted read
    /// decrypts, the whole block, and a float32 graph of any dimension still carries codes.
    /// </summary>
    private static int CodeRowsPerBlock(int rowBytes)
    {
        // Always ends: once a block holds eight rows, less than one row of it is unused, which is under an eighth.
        for (var pages = 1L; ; pages++)
        {
            var bytes = pages * Global.Constants.Storage.PageSize - PageHeader.SizeOf;
            if (8 * (bytes % rowBytes) <= bytes)
                return (int)(bytes / rowBytes);
        }
    }

    /// <summary>
    /// The parameters of the 4-bit prescreen code of a float32 graph: the center, one dither per coordinate and the signs of the rotation.
    /// They are drawn once, when the graph receives its first nodes, and never change.
    /// <para>
    /// A node with unit vector u, center c, y = u - c and n = |y| is coded as w = P y / n, where P is four rounds of random-sign Walsh-Hadamard
    /// transforms (orthogonal). The step is s = 2 max|w_j| / 15. Level j is round(w_j / s + 7 + d_j), with d_j the dither of coordinate j, uniform in [0, 1)
    /// and independent of every vector. The argument lies in [d_j, 15 + d_j], so no level is clamped, and the error e_j = (level_j - 7 - d_j) s - w_j
    /// is uniform on [-s/2, s/2] and independent across j. The row stores the levels, n s rounded up and c·y.
    /// </para>
    /// <para>
    /// For a query q with z = P (q - c), the exact distance is the estimate plus n z·e. A uniform error has a moment generating function of at most
    /// exp(t²/24), so P(z·e &lt; -m s |z|) &lt;= exp(-6 m²) over the draw of the dither, which is alpha for m = sqrt(ln(1 / alpha) / 6). The lower bound is
    /// the estimate minus the stored n s times m |z|, with each factor rounded up.
    /// </para>
    /// </summary>
    internal sealed class CodeParameters
    {
        private const int Rounds = 4;
        internal const int Levels = 16;
        // The level that decodes to zero, before the dither.
        internal const int ZeroLevel = 7;

        public readonly float[] Center;
        public readonly float[] Dither;
        private readonly byte[] _signs;
        private readonly int _block;

        private CodeParameters(float[] center, float[] dither, byte[] signs)
        {
            Center = center;
            Dither = dither;
            _signs = signs;
            _block = 1 << BitOperations.Log2((uint)center.Length);
        }

        public int Dimensions => Center.Length;

        // Coordinates 2i and 2i + 1 share byte i, the first in the low nibble.
        public int CodeBytes => (Dimensions + 1) / 2;

        // The levels, then n s rounded up, then c·y.
        public int RowBytes => RowBytesOf(Dimensions);

        internal static int RowBytesOf(int dimensions) => (dimensions + 1) / 2 + 2 * sizeof(float);

        public int StoredBytes => sizeof(int) + 2 * Dimensions * sizeof(float) + SignBytes(Dimensions);

        private static int SignBytes(int dimensions) => (Rounds * (1 << BitOperations.Log2((uint)dimensions)) + 7) / 8;

        /// <summary>Draws the dither and the signs from <paramref name="random"/>, which must not depend on the data.</summary>
        public static CodeParameters Draw(ReadOnlySpan<float> center, Random random)
        {
            var dither = new float[center.Length];
            for (var i = 0; i < dither.Length; i++)
                dither[i] = random.NextSingle();
            var signs = new byte[SignBytes(center.Length)];
            random.NextBytes(signs);
            return new CodeParameters(center.ToArray(), dither, signs);
        }

        public static CodeParameters Read(ReadOnlySpan<byte> stored)
        {
            var dimensions = stored.Length < sizeof(int) ? 0 : BinaryPrimitives.ReadInt32LittleEndian(stored);
            if (dimensions <= 0 || stored.Length != sizeof(int) + 2L * dimensions * sizeof(float) + SignBytes(dimensions))
                throw new InvalidDataException($"The stored code parameters of {stored.Length} bytes do not match {dimensions} dimensions.");

            var floats = MemoryMarshal.Cast<byte, float>(stored[sizeof(int)..][..(2 * dimensions * sizeof(float))]);
            return new CodeParameters(floats[..dimensions].ToArray(), floats[dimensions..].ToArray(), stored[^SignBytes(dimensions)..].ToArray());
        }

        public void Write(Span<byte> destination)
        {
            Debug.Assert(destination.Length == StoredBytes, "destination.Length == StoredBytes");
            BinaryPrimitives.WriteInt32LittleEndian(destination, Dimensions);
            var floats = MemoryMarshal.Cast<byte, float>(destination[sizeof(int)..][..(2 * Dimensions * sizeof(float))]);
            Center.CopyTo(floats);
            Dither.CopyTo(floats[Dimensions..]);
            _signs.CopyTo(destination[^_signs.Length..]);
        }

        /// <summary>
        /// Writes the row of the vector held in the tensor layout into <paramref name="row"/>. A vector without a direction (see <see cref="HasDirection"/>, or equal to the center)
        /// gets a row that holds NaN as its scale and that <see cref="CodeQuery.LowerBound"/> never bounds. <paramref name="scratch"/> holds one double per dimension.
        /// </summary>
        public void Encode(ReadOnlySpan<byte> tensor, Span<double> scratch, Span<byte> row)
        {
            Debug.Assert(row.Length == RowBytes && scratch.Length >= Dimensions, "row.Length == RowBytes && scratch.Length >= Dimensions");
            row.Clear();
            var scaleBytes = row[CodeBytes..];
            if (HasDirection(tensor) == false)
            {
                BinaryPrimitives.WriteSingleLittleEndian(scaleBytes, float.NaN);
                return;
            }

            var unit = ReadTensorVector<float>(tensor);
            var w = scratch[..Dimensions];
            double normSquared = 0, centerTerm = 0;
            for (var k = 0; k < w.Length; k++)
            {
                var y = (double)unit[k] - Center[k];
                w[k] = y;
                normSquared += y * y;
                centerTerm += Center[k] * y;
            }

            var norm = Math.Sqrt(normSquared);
            if (double.IsFinite(norm) == false || norm == 0)
            {
                BinaryPrimitives.WriteSingleLittleEndian(scaleBytes, float.NaN);
                return;
            }

            var largest = 0d;
            for (var k = 0; k < w.Length; k++)
                w[k] /= norm;
            Rotate(w);
            foreach (var value in w)
                largest = Math.Max(largest, Math.Abs(value));

            var step = 2 * largest / (Levels - 1);
            for (var k = 0; k < w.Length; k++)
            {
                // The clamp only absorbs the rounding of the division: the argument lies in [d_k, 15 + d_k].
                var level = (int)Math.Clamp(Math.Floor(w[k] / step + ZeroLevel + 0.5 + Dither[k]), 0, Levels - 1);
                row[k / 2] |= (byte)(level << (4 * (k & 1)));
            }

            BinaryPrimitives.WriteSingleLittleEndian(scaleBytes, RoundUp(norm * step));
            BinaryPrimitives.WriteSingleLittleEndian(scaleBytes[sizeof(float)..], (float)centerTerm);
        }

        /// <summary>
        /// True when the stored tensor is finite and not zero. Only such a vector enters the center and gets a code that bounds: a zero vector has no direction,
        /// and a NaN or infinite component, or a magnitude that overflowed, would make the whole center non-finite.
        /// </summary>
        public static bool HasDirection(ReadOnlySpan<byte> tensor)
        {
            var magnitude = ReadTensorMagnitude(tensor);
            if (magnitude == 0 || float.IsFinite(magnitude) == false)
                return false;

            foreach (var component in ReadTensorVector<float>(tensor))
            {
                if (float.IsFinite(component) == false)
                    return false;
            }

            return true;
        }

        /// <summary>Applies the orthogonal transform P in place.</summary>
        public void Rotate(Span<double> v)
        {
            var sign = 1 / Math.Sqrt(_block);
            for (var round = 0; round < Rounds; round++)
            {
                // Rounds alternate between the leading and the trailing block, so every coordinate mixes when the dimension is not a power of two.
                var block = v.Slice(round % 2 == 0 ? 0 : v.Length - _block, _block);
                for (var i = 0; i < block.Length; i++)
                {
                    var bit = round * _block + i;
                    block[i] *= ((_signs[bit >> 3] >> (bit & 7)) & 1) == 0 ? sign : -sign;
                }

                if (block.Length < 4)
                {
                    for (var len = 1; len < block.Length; len <<= 1)
                    {
                        for (var i = 0; i < block.Length; i += 2 * len)
                        {
                            for (var j = i; j < i + len; j++)
                            {
                                var a = block[j];
                                var b = block[j + len];
                                block[j] = a + b;
                                block[j + len] = a - b;
                            }
                        }
                    }

                    continue;
                }

                // PERF: The butterflies of one level do not depend on each other. The first two levels run as one 4-point transform per group of four
                // coordinates, and every later level runs four coordinates at a time in vector lanes. Each output is the same additions of the same values as
                // in a level by level loop, so the result is identical bit for bit.
                ref var x = ref MemoryMarshal.GetReference(block);
                for (var i = 0; i < block.Length; i += 4)
                {
                    ref var p = ref Unsafe.Add(ref x, i);
                    double s0 = p + Unsafe.Add(ref p, 1), s1 = p - Unsafe.Add(ref p, 1);
                    double s2 = Unsafe.Add(ref p, 2) + Unsafe.Add(ref p, 3), s3 = Unsafe.Add(ref p, 2) - Unsafe.Add(ref p, 3);
                    p = s0 + s2;
                    Unsafe.Add(ref p, 1) = s1 + s3;
                    Unsafe.Add(ref p, 2) = s0 - s2;
                    Unsafe.Add(ref p, 3) = s1 - s3;
                }

                for (var len = 4; len < block.Length; len <<= 1)
                {
                    for (var i = 0; i < block.Length; i += 2 * len)
                    {
                        for (var j = i; j < i + len; j += Vector256<double>.Count)
                        {
                            var a = Vector256.LoadUnsafe(ref x, (nuint)j);
                            var b = Vector256.LoadUnsafe(ref x, (nuint)(j + len));
                            (a + b).StoreUnsafe(ref x, (nuint)j);
                            (a - b).StoreUnsafe(ref x, (nuint)(j + len));
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Prepares the query held in the tensor layout for <see cref="CodeQuery.LowerBound"/> at the per-comparison allowance
        /// <paramref name="alpha"/>, the probability that one bound exceeds the exact distance.
        /// </summary>
        public CodeQuery Prepare(ReadOnlySpan<byte> tensor, double alpha, ByteStringContext allocator)
        {
            if ((alpha > 0 && alpha < 1) == false)
                throw new ArgumentOutOfRangeException(nameof(alpha), alpha, "The allowance is a probability in the open interval (0, 1).");

            if (tensor.Length != TensorSizeBytes<float>(Dimensions))
                throw new ArgumentException($"The query tensor has {tensor.Length} bytes, but the graph holds {TensorSizeBytes<float>(Dimensions)}.", nameof(tensor));

            if (ReadTensorMagnitude(tensor) == 0)
                return new CodeQuery(this, default, default, 0, 0, 0, 0, 0);

            var q = ReadTensorVector<float>(tensor);

            // Held by the query: the even and the odd coordinates as fixed-point shorts. Released after the preparation: z.
            var scope = allocator.Allocate(2 * CodeBytes * sizeof(short), out ByteString coordinates);
            using var zScope = allocator.Allocate(Dimensions * sizeof(double), out ByteString zBuffer);
            var z = MemoryMarshal.Cast<byte, double>(zBuffer.ToSpan())[..Dimensions];
            // PERF: The centering, the amplitude and the levels run eight floats or four doubles at a time in vector lanes, and the tail runs one coordinate
            // at a time. Each lane keeps its own partial sums, which are added at the end; the order of the additions changes only the last bits, which the
            // margin covers.
            ref var zs = ref MemoryMarshal.GetReference(z);
            ref var qs = ref MemoryMarshal.GetReference(q);
            ref var cs = ref MemoryMarshal.GetArrayDataReference(Center);
            ref var ds = ref MemoryMarshal.GetArrayDataReference(Dither);
            var lanes = Vector256<float>.Count;
            var vectorized = z.Length - z.Length % lanes;
            Vector256<double> normSquared4 = default, offset4 = default;
            for (var k = 0; k < vectorized; k += lanes)
            {
                var (qLow, qHigh) = Vector256.Widen(Vector256.LoadUnsafe(ref qs, (nuint)k));
                var (cLow, cHigh) = Vector256.Widen(Vector256.LoadUnsafe(ref cs, (nuint)k));
                var zLow = qLow - cLow;
                var zHigh = qHigh - cHigh;
                zLow.StoreUnsafe(ref zs, (nuint)k);
                zHigh.StoreUnsafe(ref zs, (nuint)(k + lanes / 2));
                normSquared4 += zLow * zLow + zHigh * zHigh;
                offset4 += qLow * cLow + qHigh * cHigh;
            }

            double normSquared = Vector256.Sum(normSquared4), offset = Vector256.Sum(offset4);
            for (var k = vectorized; k < z.Length; k++)
            {
                z[k] = (double)q[k] - Center[k];
                normSquared += z[k] * z[k];
                offset += (double)q[k] * Center[k];
            }

            Rotate(z);
            // PERF: z is rounded to shorts at a power-of-two step, the largest that keeps every short in range, so the kernel is an integer dot of the
            // levels and the shorts: exact in any order, and sixteen coordinates per multiply-add. The rounding of z is known exactly, so the query carries
            // the slack that it adds to the dot: 15 times the sum of the absolute roundings.
            Vector256<double> amplitude4 = default;
            for (var k = 0; k < vectorized; k += lanes / 2)
                amplitude4 = Vector256.Max(amplitude4, Vector256.Abs(Vector256.LoadUnsafe(ref zs, (nuint)k)));
            var amplitude = Math.Max(Math.Max(amplitude4[0], amplitude4[1]), Math.Max(amplitude4[2], amplitude4[3]));
            for (var k = vectorized; k < z.Length; k++)
                amplitude = Math.Max(amplitude, Math.Abs(z[k]));
            var step = amplitude == 0 ? 1 : Math.ScaleB(1, Math.ILogB(short.MaxValue / amplitude));
            var halves = MemoryMarshal.Cast<byte, short>(coordinates.ToSpan())[..(2 * CodeBytes)];
            halves.Clear();
            // The step is a power of two, so scaling by it and by its inverse is exact.
            var step4 = Vector256.Create(step);
            var inverse4 = Vector256.Create(1 / step);
            var zeroLevel4 = Vector256.Create((double)ZeroLevel);
            Vector256<double> zeroTerm4 = default, roundings4 = default;
            for (var k = 0; k < vectorized; k += lanes / 2)
            {
                var zk = Vector256.LoadUnsafe(ref zs, (nuint)k);
                var dither = Vector256.WidenLower(Vector256.Create(Vector128.LoadUnsafe(ref ds, (nuint)k), Vector128<float>.Zero));
                zeroTerm4 += zk * (zeroLevel4 + dither);
                var levels = Vector256.Round(zk * step4);
                roundings4 += Vector256.Abs(zk - levels * inverse4);
                halves[k / 2] = (short)levels[0];
                halves[CodeBytes + k / 2] = (short)levels[1];
                halves[k / 2 + 1] = (short)levels[2];
                halves[CodeBytes + k / 2 + 1] = (short)levels[3];
            }

            double zeroTerm = Vector256.Sum(zeroTerm4), roundings = Vector256.Sum(roundings4);
            for (var k = vectorized; k < z.Length; k++)
            {
                zeroTerm += z[k] * (ZeroLevel + (double)Dither[k]);
                var level = (short)Math.Round(z[k] * step);
                roundings += Math.Abs(z[k] - level / step);
                halves[(k % 2 == 0 ? 0 : CodeBytes) + k / 2] = level;
            }

            var multiplier = RoundUp(Math.Sqrt(Math.Log(1 / alpha) / 6));
            var errorFactor = RoundUp(multiplier * (double)RoundUp(Math.Sqrt(normSquared)));
            return new CodeQuery(this, coordinates, scope, offset, zeroTerm, errorFactor, 1 / step, RoundUp((Levels - 1) * roundings));
        }

        // The smallest float32 that is not below the value.
        private static float RoundUp(double value)
        {
            var rounded = (float)value;
            return rounded >= value ? rounded : MathF.BitIncrement(rounded);
        }

        public static float Scale(ReadOnlySpan<byte> row, int codeBytes) => BinaryPrimitives.ReadSingleLittleEndian(row[codeBytes..]);

        public static int Level(ReadOnlySpan<byte> row, int coordinate) => (row[coordinate / 2] >> (4 * (coordinate & 1))) & (Levels - 1);
    }

    /// <summary>
    /// A query prepared against the parameters of one graph. Its scratch comes from the transaction allocator, so it must be disposed before the transaction ends.
    /// </summary>
    internal sealed class CodeQuery : IDisposable
    {
        private readonly CodeParameters _parameters;
        // z of the rotated centered query in fixed point, as shorts: the even coordinates, then the odd ones; empty for a query without direction.
        private readonly ByteString _coordinates;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _scope;
        // q·c, and the sum of z_j (7 + d_j).
        private readonly double _offset;
        private readonly double _zeroTerm;
        // The bound's multiplier times the centered norm of the query, rounded up.
        private readonly double _errorFactor;
        // The value of one fixed-point unit, and what the rounding of z to fixed point may add to the dot of any row.
        private readonly double _unit;
        private readonly double _slack;

        internal CodeQuery(CodeParameters parameters, ByteString coordinates, ByteStringContext<ByteStringMemoryCache>.InternalScope scope, double offset, double zeroTerm,
            double errorFactor, double unit, double slack)
        {
            _parameters = parameters;
            _coordinates = coordinates;
            _scope = scope;
            _offset = offset;
            _zeroTerm = zeroTerm;
            _errorFactor = errorFactor;
            _unit = unit;
            _slack = slack;
        }

        public void Dispose() => _scope.Dispose();

        /// <summary>
        /// A lower bound on the cosine distance between the query and the node that holds <paramref name="row"/>, which exceeds the exact distance
        /// with probability at most alpha over the draw of the graph's dither. Negative infinity when there is no bound: the query or the node has no direction.
        /// </summary>
        public double LowerBound(ReadOnlySpan<byte> row)
        {
            Debug.Assert(row.Length == _parameters.RowBytes, "row.Length == _parameters.RowBytes");
            var codeBytes = _parameters.CodeBytes;
            var scale = CodeParameters.Scale(row, codeBytes);
            if (_coordinates.HasValue == false || float.IsNaN(scale))
                return double.NegativeInfinity;

            var halves = MemoryMarshal.Cast<byte, short>(_coordinates.ToSpan());
            var dot = Dot(row[..codeBytes], halves[..codeBytes], halves[codeBytes..]) * _unit + _slack;

            var centerTerm = BinaryPrimitives.ReadSingleLittleEndian(row[(codeBytes + sizeof(float))..]);
            var estimate = 1 - (_offset + centerTerm + scale * (dot - _zeroTerm));
            var bound = estimate - scale * _errorFactor;
            return double.IsFinite(bound) ? bound : double.NegativeInfinity;
        }

        // The exact sum of the levels times the shorts, so every path returns the same value.
        // PERF: Each byte holds the levels of an even and an odd coordinate, and the query keeps its even and odd shorts apart. The bytes widen to
        // shorts; a mask takes the even levels and a shift the odd ones, and each multiplies consecutive shorts of its half, with no shuffle. No
        // multiply-add accumulates onto the running sum, which waits on one add per step and not on the latency of a multiply-add.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long Dot(ReadOnlySpan<byte> levels, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
        {
            if (Avx512BW.IsSupported)
                return DotAvx512(levels, even, odd);
            if (AvxVnni.IsSupported)
                return DotAvxVnni(levels, even, odd);
            if (Avx2.IsSupported)
                return DotAvx2(levels, even, odd);
            if (Sve.IsSupported)
                return DotSve(levels, even, odd);
            if (AdvSimd.IsSupported)
                return DotAdvSimd(levels, even, odd);
            return DotScalar(levels, even, odd);
        }

        internal static long DotAvx512(ReadOnlySpan<byte> levels, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
        {
            ref var l = ref MemoryMarshal.GetReference(levels);
            ref var e = ref MemoryMarshal.GetReference(even);
            ref var o = ref MemoryMarshal.GetReference(odd);
            var mask = Vector512.Create((short)15);
            var i = 0;
            long dot = 0;
            while (i + 32 <= levels.Length)
            {
                // A step adds at most 4 * 15 * 2^15 to an int lane, so the sixteen lanes of 64 steps sum within an int.
                var end = Math.Min(i + 64 * 32, levels.Length - 31);
                Vector512<int> sum = default;
                for (; i < end; i += 32)
                {
                    var bytes = Avx512BW.ConvertToVector512Int16(Vector256.LoadUnsafe(ref l, (nuint)i));
                    sum += Avx512BW.MultiplyAddAdjacent(bytes & mask, Vector512.LoadUnsafe(ref e, (nuint)i));
                    sum += Avx512BW.MultiplyAddAdjacent(Vector512.ShiftRightLogical(bytes, 4), Vector512.LoadUnsafe(ref o, (nuint)i));
                }

                dot += Vector512.Sum(sum);
            }

            return dot + DotScalar(levels[i..], even[i..], odd[i..]);
        }

        internal static long DotAvxVnni(ReadOnlySpan<byte> levels, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
        {
            ref var l = ref MemoryMarshal.GetReference(levels);
            ref var e = ref MemoryMarshal.GetReference(even);
            ref var o = ref MemoryMarshal.GetReference(odd);
            var mask = Vector256.Create((short)15);
            var i = 0;
            long dot = 0;
            while (i + 16 <= levels.Length)
            {
                // A step adds at most 4 * 15 * 2^15 to an int lane, so the eight lanes of 128 steps sum within an int.
                var end = Math.Min(i + 128 * 16, levels.Length - 15);
                Vector256<int> sum = default;
                for (; i < end; i += 16)
                {
                    var bytes = Avx2.ConvertToVector256Int16(Vector128.LoadUnsafe(ref l, (nuint)i));
                    var evenProducts = Avx2.MultiplyAddAdjacent(bytes & mask, Vector256.LoadUnsafe(ref e, (nuint)i));
                    sum += AvxVnni.MultiplyWideningAndAdd(evenProducts, Vector256.ShiftRightLogical(bytes, 4), Vector256.LoadUnsafe(ref o, (nuint)i));
                }

                dot += Vector256.Sum(sum);
            }

            return dot + DotScalar(levels[i..], even[i..], odd[i..]);
        }

        internal static long DotAvx2(ReadOnlySpan<byte> levels, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
        {
            ref var l = ref MemoryMarshal.GetReference(levels);
            ref var e = ref MemoryMarshal.GetReference(even);
            ref var o = ref MemoryMarshal.GetReference(odd);
            var mask = Vector256.Create((short)15);
            var i = 0;
            long dot = 0;
            while (i + 16 <= levels.Length)
            {
                // A step adds at most 4 * 15 * 2^15 to an int lane, so the eight lanes of 128 steps sum within an int.
                var end = Math.Min(i + 128 * 16, levels.Length - 15);
                Vector256<int> sum = default;
                for (; i < end; i += 16)
                {
                    var bytes = Avx2.ConvertToVector256Int16(Vector128.LoadUnsafe(ref l, (nuint)i));
                    sum += Avx2.MultiplyAddAdjacent(bytes & mask, Vector256.LoadUnsafe(ref e, (nuint)i));
                    sum += Avx2.MultiplyAddAdjacent(Vector256.ShiftRightLogical(bytes, 4), Vector256.LoadUnsafe(ref o, (nuint)i));
                }

                dot += Vector256.Sum(sum);
            }

            return dot + DotScalar(levels[i..], even[i..], odd[i..]);
        }

        internal static long DotSve(ReadOnlySpan<byte> levels, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
        {
            ref var l = ref MemoryMarshal.GetReference(levels);
            ref var e = ref MemoryMarshal.GetReference(even);
            ref var o = ref MemoryMarshal.GetReference(odd);
            var mask = new Vector<ushort>(15);
            var half = Vector<short>.Count;
            var i = 0;
            // A step adds at most 16 * 15 * 2^15 to a long lane, so no span overflows it.
            Vector<long> sum = default;
            for (; i + Vector<byte>.Count <= levels.Length; i += Vector<byte>.Count)
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

            return Vector.Sum(sum) + DotScalar(levels[i..], even[i..], odd[i..]);
        }

        internal static long DotAdvSimd(ReadOnlySpan<byte> levels, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
        {
            ref var l = ref MemoryMarshal.GetReference(levels);
            ref var e = ref MemoryMarshal.GetReference(even);
            ref var o = ref MemoryMarshal.GetReference(odd);
            var mask = Vector128.Create((ushort)15);
            var i = 0;
            long dot = 0;
            while (i + 16 <= levels.Length)
            {
                // A step adds at most 8 * 15 * 2^15 to an int lane, so the four lanes of 128 steps sum within an int.
                var end = Math.Min(i + 128 * 16, levels.Length - 15);
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

            return dot + DotScalar(levels[i..], even[i..], odd[i..]);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static Vector128<int> MultiplyAdd(Vector128<int> sum, Vector128<short> left, Vector128<short> right) =>
                AdvSimd.MultiplyWideningUpperAndAdd(AdvSimd.MultiplyWideningLowerAndAdd(sum, left.GetLower(), right.GetLower()), left, right);
        }

        internal static long DotScalar(ReadOnlySpan<byte> levels, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
        {
            long dot = 0;
            for (var i = 0; i < levels.Length; i++)
                dot += (levels[i] & 15) * even[i] + (levels[i] >> 4) * odd[i];
            return dot;
        }
    }

    public partial class SearchState
    {
        internal unsafe CodeParameters ReadCodeParameters()
        {
            if (IsEmpty)
                return null;

            var stored = _tree.DirectRead(CodeParametersSlice);
            return stored is null ? null : CodeParameters.Read(new ReadOnlySpan<byte>(stored, _tree.GetDataSize(CodeParametersSlice)));
        }

        /// <summary>
        /// The stored row of the node, valid for the lifetime of the transaction; null for an id past the nodes of the graph. The row of a node that
        /// this transaction creates is stored by the commit of its registration.
        /// </summary>
        internal unsafe byte* CodeRow(long nodeId)
        {
            if (_carriesCodes == false || nodeId < EntryPointId || nodeId > Options.CountOfVectors)
                return null;

            return _codeRows.Read(Llt, nodeId);
        }

        internal unsafe void WriteCode(long nodeId, CodeParameters parameters, ReadOnlySpan<byte> tensor, Span<double> scratch) =>
            parameters.Encode(tensor, scratch, new Span<byte>(_codeRows.Write(Llt, nodeId), _codeRowBytes));
    }
}
