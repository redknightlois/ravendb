using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Sparrow;
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
        // Ends by the pages of eight rows, where less than a row is unused.
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
    /// <para>
    /// The bound does not cover the roundings that <see cref="Margin"/> covers.
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
            double normSquared = 0, offset = 0;
            for (var k = 0; k < z.Length; k++)
            {
                z[k] = (double)q[k] - Center[k];
                normSquared += z[k] * z[k];
                offset += (double)q[k] * Center[k];
            }

            Rotate(z);
            // The kernel is an exact integer dot of the levels with z rounded to shorts at a power-of-two step, the largest that keeps every short in
            // range. The rounding of z is known exactly, so the query carries the slack that it adds to the dot: 15 times the sum of the absolute roundings.
            double amplitude = 0;
            for (var k = 0; k < z.Length; k++)
                amplitude = Math.Max(amplitude, Math.Abs(z[k]));
            var step = amplitude == 0 ? 1 : Math.ScaleB(1, Math.ILogB(short.MaxValue / amplitude));
            double zeroTerm = 0, roundings = 0;
            var halves = MemoryMarshal.Cast<byte, short>(coordinates.ToSpan())[..(2 * CodeBytes)];
            halves.Clear();
            for (var k = 0; k < z.Length; k++)
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

        /// <summary>
        /// The slack that a rejection adds to the threshold, as a distance. It covers every rounding that the probabilistic bound does not: with the
        /// bound L (computed in double from the stored float32 terms), the exact distance d and the distance dk that the float32 kernel returns,
        /// L - dk &lt;= Margin except with probability alpha over the dither draw, so L &gt; threshold + Margin implies dk &gt; threshold.
        /// <para>
        /// Factors. A stored unit vector, a query and the center have a norm of at most U = 1 + (D + 3) 2^-23: the float32 norm and the division
        /// err by at most D 2^-25 + 2^-24 and 2^-24 relative, and the center is a mean of such vectors. So n = |u - c| and |z| = |P (q - c)| are at most 2U,
        /// because P is orthogonal. |w| = 1 and so max|w_j| &lt;= 1 and the step is s &lt;= 2/15, which puts n s at most 4U/15. A decoded vector has an
        /// error of at most s/2 per coordinate, so its norm is at most 1 + sqrt(D) / 15.
        /// </para>
        /// <para>
        /// Terms, each a distance:
        /// - T1, the stored n s is rounded up by a relative 2^-23 at most, and the estimate multiplies it by (z·decoded w) / s: 2^-23 times n |z| times the decoded norm.
        /// - T2, the stored c·y is rounded to float32, a relative 2^-24 of |c| |y| &lt;= 2U^2.
        /// - T3, the dither is a multiple of 2^-24, not a continuous draw. The centered error stays within the exp(t²/24) bound, because a discrete uniform
        ///   variable has a smaller moment generating function than the continuous one, but its mean is not 0: it is at most 2^-25 steps per coordinate. Its
        ///   effect on the distance is n s 2^-25 |z|_1 &lt;= n s 2^-25 sqrt(D) |z|.
        /// - T4, the double arithmetic of the encoder, the query preparation and the comparison: fewer than 2^8 D operations of relative error 2^-53 on
        ///   partial sums of at most 16 sqrt(D) times the factors above, which is below D^2 2^-45.
        /// - T5, the float32 kernel: the dot product of D terms errs by at most (D + 1) 2^-24 |q| |u| for any order of summation, and the subtraction from 1 by 2^-24 (1 + |q| |u|).
        /// </para>
        /// </summary>
        internal static double Margin(int dimensions)
        {
            var epsilon = Math.ScaleB(1, -23);
            var unit = 1 + (dimensions + 3) * epsilon;
            var unitSquared = unit * unit;
            var root = Math.Sqrt(dimensions);

            var storedScale = epsilon * 4 * unitSquared * (1 + root / 15);
            var storedCenterTerm = epsilon * unitSquared;
            var ditherMean = 8 * unitSquared * root * Math.ScaleB(1, -25) / 15;
            var doubleArithmetic = Math.ScaleB((double)dimensions * dimensions, -45);
            var kernel = (dimensions + 2) * unitSquared * epsilon;
            return storedScale + storedCenterTerm + ditherMean + doubleArithmetic + kernel;
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

        // The exact sum of the levels times the shorts, so it runs in any order. Only x86 has a vector path; other platforms take the scalar loop.
        internal static long Dot(ReadOnlySpan<byte> levels, ReadOnlySpan<short> even, ReadOnlySpan<short> odd)
        {
            var i = 0;
            long dot = 0;
            if (Avx2.IsSupported)
            {
                ref var l = ref MemoryMarshal.GetReference(levels);
                ref var e = ref MemoryMarshal.GetReference(even);
                ref var o = ref MemoryMarshal.GetReference(odd);
                var mask = Vector256.Create((short)15);
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
            }

            for (; i < levels.Length; i++)
                dot += (levels[i] & 15) * even[i] + (levels[i] >> 4) * odd[i];
            return dot;
        }
    }

    /// <summary>
    /// The prescreen of one unfiltered query, its descent through the levels and its base-layer traversal: before the node record of a neighbor is resolved, the
    /// lower bound of the neighbor's code may reject it.
    /// <para>
    /// Rule. A neighbor is rejected only when its bound exceeds the threshold by <see cref="CodeParameters.Margin"/>, with a strict comparison. In the base layer the
    /// working set must be full, and the threshold is the distance of its farthest entry. The threshold only tightens while the set is full, so the rejection holds
    /// against every later threshold of the traversal: the neighbor is remembered as rejected and never checked or resolved again in the traversal. A traversal that is
    /// restarted with a larger working set has a looser threshold, so it drops the screen. In the descent the threshold is the distance of the current node, which is
    /// tighter than those of the base layer, so a rejection of the descent is not remembered.
    /// </para>
    /// <para>
    /// Bound. Let U be the query without the screen, on the same graph and query. U is a mathematical object of this proof: no such query exists in
    /// the code. U reads no code, so which neighbors it evaluates, in which order and against which thresholds is fixed before the dither is drawn. The screened
    /// query S evaluates the neighbors of U in the same order minus the rejected ones, so it follows U until the first neighbor that S rejects and U would admit.
    /// An admission event is a move of the descent, or a neighbor whose distance is below the threshold of a full working set, so it enters the candidate queue.
    /// A node with an empty posting list is an event as well, although it stays out of the working set, because S would not expand it. The descent evaluates a
    /// node once and its distance only falls, so a neighbor that U does not move to is never closer than a later current node: S, which may check or evaluate it
    /// again, does not move to it either.
    /// Rejecting an event requires a bound above the distance by more than the margin, which has probability at most alpha over the draw of the dither
    /// (see <see cref="CodeParameters.Margin"/>). S screens the neighbors of one expansion only when the events so far plus the number of the neighbors fit under
    /// <see cref="Cap"/>, and the events so far are those of U up to the first difference, so every event that S can reject is among the first Cap events of U.
    /// By the union bound over the Cap events, which are fixed, the chance that S differs from U is at most Cap times alpha, and alpha = FailureBudget / Cap.
    /// Once the cap is spent the rest of the traversal runs unscreened.
    /// </para>
    /// <para>
    /// Cap. The larger of 512 and four events per working-set slot: a larger working set admits more neighbors before it settles. A query that spends the cap
    /// loses part of the saving, never correctness.
    /// </para>
    /// </summary>
    internal sealed unsafe class CodeScreen : IDisposable
    {
        // The probability that a screened traversal differs from the traversal that reads no code, at most, per query.
        internal const double FailureBudget = 0.01;

        private readonly CodeQuery _query;
        private readonly ByteStringContext _allocator;
        private readonly long _maxNodeId;
        // One bit per node id of the snapshot, allocated and zeroed at the first rejection of the traversal: 125 KB per million nodes. The zeroing grows with
        // the graph, not with the rejections; a bitmap kept across traversals that clears only the words it set replaces it when the zeroing shows in the query time.
        private ulong* _rejected;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _rejectedScope;

        public CodeScreen(CodeParameters parameters, ReadOnlySpan<byte> tensor, int cap, long maxNodeId, ByteStringContext allocator)
        {
            Cap = cap;
            Margin = CodeParameters.Margin(parameters.Dimensions);
            _query = parameters.Prepare(tensor, FailureBudget / Cap, allocator);
            _allocator = allocator;
            _maxNodeId = maxNodeId;
        }

        public static int DefaultCap(int numberOfCandidates) => (int)Math.Min(Math.Max(512L, 4L * numberOfCandidates), int.MaxValue);

        public int Cap { get; }
        public double Margin { get; }

        /// <summary>The admission events of the traversal so far.</summary>
        public int Admissions;

        /// <summary>True when each of <paramref name="neighbors"/> checks can still stay inside the cap.</summary>
        public bool Screens(int neighbors) => (long)Admissions + neighbors <= Cap;

        /// <summary>The distinct neighbors rejected so far.</summary>
        public int Rejected { get; private set; }

        /// <summary>True while the descent screens: its rejections are not remembered.</summary>
        public bool Descending;

        public bool IsRejected(long nodeId) => _rejected != null && (ulong)nodeId <= (ulong)_maxNodeId && (_rejected[nodeId >> 6] & (1UL << (int)(nodeId & 63))) != 0;

        // An id outside the snapshot is never remembered: the resolution of the node reports it.
        public void Reject(long nodeId)
        {
            if (Descending || (ulong)nodeId > (ulong)_maxNodeId)
                return;

            if (_rejected == null)
            {
                _rejectedScope = _allocator.Allocate(checked((int)((_maxNodeId >> 6) + 1) * sizeof(ulong)), out ByteString bits);
                bits.ToSpan().Clear();
                _rejected = (ulong*)bits.Ptr;
            }

            ref var word = ref _rejected[nodeId >> 6];
            var bit = 1UL << (int)(nodeId & 63);
            if ((word & bit) != 0)
                return;
            word |= bit;
            Rejected++;
        }

        /// <summary>True when the code <paramref name="row"/> proves that its node is not admitted at <paramref name="threshold"/>.</summary>
        public bool Rejects(ReadOnlySpan<byte> row, double threshold) => Exceeds(_query.LowerBound(row), threshold, Margin);

        public static bool Exceeds(double bound, double threshold, double margin) => bound > threshold + margin;

        public void Dispose()
        {
            _query.Dispose();
            if (_rejected != null)
                _rejectedScope.Dispose();
            _rejected = null;
        }
    }

    public partial class SearchState
    {
        private CodeParameters _codeParameters;

        /// <summary>
        /// The parameters never change once fixed, so one decode serves the graph: it is held by the <see cref="HnswIndexCache"/> of the graph when one is attached,
        /// and by this SearchState otherwise. Null when the graph carries none, or holds no parameters yet: nothing remembers their absence.
        /// </summary>
        internal CodeParameters CodeParameters
        {
            get
            {
                if (_carriesCodes == false)
                    return null;

                if (_codeParameters is null)
                {
                    _codeParameters = _nodeCache?.CodeParameters;
                    if (_codeParameters is null && ReadCodeParameters() is { } read)
                        _codeParameters = _nodeCache?.PublishCodeParameters(read) ?? read;
                }

                return _codeParameters;
            }
        }

        internal unsafe CodeParameters ReadCodeParameters()
        {
            if (IsEmpty)
                return null;

            var stored = _tree.DirectRead(CodeParametersSlice);
            return stored is null ? null : CodeParameters.Read(new ReadOnlySpan<byte>(stored, _tree.GetDataSize(CodeParametersSlice)));
        }

        /// <summary>
        /// Prepares a query in the tensor layout for <see cref="CodeLowerBound"/>. The caller disposes it before the transaction ends.
        /// Throws when the graph carries no codes: it is not a float32 graph, or it holds no node yet.
        /// </summary>
        internal CodeQuery PrepareCodeQuery(ReadOnlySpan<byte> tensor, double alpha)
        {
            var parameters = CodeParameters ?? throw new InvalidOperationException($"The vector graph '{_tree?.Name}' carries no prescreen codes.");
            return parameters.Prepare(tensor, alpha, Llt.Allocator);
        }

        /// <summary>
        /// The stored row of the node, valid for the lifetime of the transaction; empty for an id past the nodes of the graph. The row of a node that
        /// this transaction creates is stored by the commit of its registration.
        /// </summary>
        internal unsafe ReadOnlySpan<byte> ReadCode(long nodeId)
        {
            var row = CodeRow(nodeId);
            return row == null ? default : new ReadOnlySpan<byte>(row, _codeRowBytes);
        }

        private unsafe byte* CodeRow(long nodeId)
        {
            if (_carriesCodes == false || nodeId < EntryPointId || nodeId > Options.CountOfVectors)
                return null;

            return _codeRows.Read(Llt, nodeId);
        }

        /// <summary>Starts loading the code row of the node into the processor cache and returns it; null when the snapshot holds none.</summary>
        private unsafe byte* PrefetchCode(long nodeId)
        {
            var row = CodeRow(nodeId);
            if (row != null && PortableIntrinsics.CanPrefetch)
            {
                for (var line = (byte*)((ulong)row & ~63UL); line < row + _codeRowBytes; line += 64)
                    PortableIntrinsics.PrefetchRead(line);
            }

            return row;
        }

        /// <summary>
        /// A lower bound on the distance between the prepared query and the node, from the stored code of the node alone: one lookup of the block of its row,
        /// with no location lookup, no node record and no stored vector. Throws when this snapshot holds no node with that id.
        /// </summary>
        internal double CodeLowerBound(CodeQuery query, long nodeId)
        {
            var row = ReadCode(nodeId);
            if (row.IsEmpty)
                throw new InvalidOperationException($"Unable to find the code of node id {nodeId}");
            return query.LowerBound(row);
        }

        /// <summary>The screen of an unfiltered traversal, which prepares the query once; null when the graph carries no codes. The caller disposes it.</summary>
        internal CodeScreen BeginScreen(ReadOnlySpan<byte> tensor, int numberOfCandidates, bool hasFilterMatch)
        {
            if (hasFilterMatch || _carriesCodes == false || Options.CountOfVectors == 0 || CodeParameters is not { } parameters)
                return null;

            var screen = new CodeScreen(parameters, tensor, _forTestingPurposes?.Cap ?? CodeScreen.DefaultCap(numberOfCandidates), Options.CountOfVectors, Llt.Allocator);
            if (_forTestingPurposes is not null)
                _forTestingPurposes.LastScreen = screen;
            return screen;
        }

        /// <summary>
        /// True when the screen drops an unresolved neighbor: it rejected the id earlier in the traversal, or <paramref name="screening"/> and the code
        /// <paramref name="row"/> of the id reject it against <paramref name="threshold"/> now.
        /// </summary>
        private unsafe bool ScreenRejects(CodeScreen screen, bool screening, long id, byte* row, double threshold)
        {
            if (screen.IsRejected(id))
                return true;
            if (screening == false)
                return false;

            // A snapshot that holds no code for the node leaves it unchecked: the resolution of the node reports what is missing.
            if (_forTestingPurposes is not null)
                _forTestingPurposes.CodeRowsRead++;
            if (row == null || screen.Rejects(new ReadOnlySpan<byte>(row, _codeRowBytes), threshold) == false)
                return false;

            screen.Reject(id);
            if (_forTestingPurposes is null)
                return true;
            if (screen.Descending)
                _forTestingPurposes.DescentRejections++;
            else
                _forTestingPurposes.Rejections++;
            return true;
        }

        private TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly() => _forTestingPurposes ??= new TestingStuff();

        internal sealed class TestingStuff
        {
            // Replaces the cap of the next traversals, to reach it on a small graph.
            public int? Cap;

            // The screen of the latest traversal, which holds its admission events and its cap.
            public CodeScreen LastScreen;

            public long CodeRowsRead;
            public long Rejections;
            // The rejections of the descent, which the screen does not remember.
            public long DescentRejections;

            // The largest sum of the admission events so far and the neighbors checked, over the screened expansions.
            public int Commitment;
        }

        internal unsafe void WriteCode(long nodeId, CodeParameters parameters, ReadOnlySpan<byte> tensor, Span<double> scratch) =>
            parameters.Encode(tensor, scratch, new Span<byte>(_codeRows.Write(Llt, nodeId), _codeRowBytes));
    }
}
