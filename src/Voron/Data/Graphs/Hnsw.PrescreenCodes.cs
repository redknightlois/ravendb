using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Tensors;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    /// <summary>
    /// The dimensions of the codes of a graph, 0 when it carries none. A code bounds a cosine distance, so every graph that compares by cosine carries codes,
    /// of its float32 or int8 vectors.
    /// </summary>
    internal static int CodeDimensions(SimilarityMethod method, int vectorSizeBytes) => method switch
    {
        SimilarityMethod.CosineSimilaritySingles => (vectorSizeBytes - MagnitudeSizeInBytes) / sizeof(float),
        SimilarityMethod.CosineSimilarityI8 => vectorSizeBytes - MagnitudeSizeInBytes,
        _ => 0
    };

    /// <summary>
    /// A block of code rows is one overflow allocation of the fewest pages that hold a row and leave at most an eighth of the block unused, and holds every
    /// row that fits them. Graceful degradation for wide rows: a block spans several pages, so a commit that appends to it copies, and an encrypted read
    /// decrypts, the whole block, and a graph of any dimension still carries codes.
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
    /// The parameters of the 4-bit prescreen codes of a graph: the center, one dither per coordinate and the signs of the rotation. They are drawn once,
    /// when the graph receives its first nodes, and never change. The code of a node lets <see cref="CodeQuery.LowerBound"/> bound its cosine distance to a
    /// query without reading the node.
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
        /// Writes the code row of the vector held in the tensor layout into <paramref name="row"/>. A vector without a direction (see <see cref="UnitVector"/>,
        /// or equal to the center) gets a row that holds NaN as its scale and that <see cref="CodeQuery.LowerBound"/> never bounds. <paramref name="scratch"/>
        /// holds one double per dimension.
        /// </summary>
        public void Encode(ReadOnlySpan<byte> tensor, Span<double> scratch, Span<byte> row)
        {
            Debug.Assert(row.Length == RowBytes && scratch.Length >= Dimensions, "row.Length == RowBytes && scratch.Length >= Dimensions");
            row.Clear();
            var scaleBytes = row[CodeBytes..];
            var w = scratch[..Dimensions];
            if (UnitVector(tensor, w) == false)
            {
                BinaryPrimitives.WriteSingleLittleEndian(scaleBytes, float.NaN);
                return;
            }

            // y = u - c, the unit vector relative to the center, with n = |y|.
            double normSquared = 0, centerTerm = 0;
            for (var k = 0; k < w.Length; k++)
            {
                var y = w[k] - Center[k];
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

            // w = P y / n is a unit vector that the rotation spreads over every coordinate, so one step serves them all: s = 2 max|w_j| / 15, which spans
            // the coordinates with the sixteen levels.
            for (var k = 0; k < w.Length; k++)
                w[k] /= norm;
            Rotate(w);
            var step = 2 * Math.Abs(TensorPrimitives.MaxMagnitude<double>(w)) / (Levels - 1);

            // Level j is round(w_j / s + 7 + d_j). The dither d_j is uniform in [0, 1) and independent of every vector, so the error
            // e_j = (level_j - 7 - d_j) s - w_j is uniform on [-s/2, s/2] and independent across j. The argument lies in [d_j, 15 + d_j], so the clamp only
            // absorbs the rounding of the division.
            for (var k = 0; k < w.Length; k++)
            {
                var level = (int)Math.Clamp(Math.Floor(w[k] / step + ZeroLevel + 0.5 + Dither[k]), 0, Levels - 1);
                row[k / 2] |= (byte)(level << (4 * (k & 1)));
            }

            // n s is rounded up, so the error term of the bound only grows.
            BinaryPrimitives.WriteSingleLittleEndian(scaleBytes, RoundUp(norm * step));
            BinaryPrimitives.WriteSingleLittleEndian(scaleBytes[sizeof(float)..], (float)centerTerm);
        }

        /// <summary>
        /// Writes the unit vector of the stored tensor, of float32 or int8 components by its length, into <paramref name="unit"/>; false when it has no
        /// direction. Only a vector with a direction enters the center and gets a code that bounds: a zero vector has none, and a NaN or infinite component,
        /// or a magnitude that overflowed, would make the whole center non-finite.
        /// </summary>
        public static bool UnitVector(ReadOnlySpan<byte> tensor, Span<double> unit)
        {
            if (tensor.Length == TensorSizeBytes<float>(unit.Length))
            {
                // The components are the unit vector.
                var magnitude = ReadTensorMagnitude(tensor);
                if (magnitude == 0 || float.IsFinite(magnitude) == false)
                    return false;

                var components = ReadTensorVector<float>(tensor);
                for (var k = 0; k < unit.Length; k++)
                {
                    if (float.IsFinite(components[k]) == false)
                        return false;
                    unit[k] = components[k];
                }

                return true;
            }

            if (tensor.Length == TensorSizeBytes<sbyte>(unit.Length))
            {
                // The components are integers s and the magnitude is a scale m. The distance kernel divides m² s·t by the roots of m² |s|² and of the same
                // product of the other vector, in either order of the products, so it measures the angle of the integers while m is positive and those
                // products are normal floats.
                var scale = ReadTensorMagnitude(tensor);
                var components = ReadTensorVector<sbyte>(tensor);
                long squares = 0;
                foreach (var component in components)
                    squares += component * component;
                if ((scale > 0) == false || float.IsNormal(scale * scale) == false || float.IsNormal(scale * scale * squares) == false ||
                    float.IsNormal(squares * scale * scale) == false)
                    return false;

                var norm = Math.Sqrt(squares);
                for (var k = 0; k < unit.Length; k++)
                    unit[k] = components[k] / norm;
                return true;
            }

            throw new ArgumentException($"The tensor has {tensor.Length} bytes, which hold neither {unit.Length} float32 nor {unit.Length} int8 components.", nameof(tensor));
        }

        /// <summary>Applies the orthogonal transform P in place: in each round, random signs and a Walsh-Hadamard transform, scaled to keep the norm.</summary>
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

                Functions.WalshHadamardTransform(block);
            }
        }

        /// <summary>
        /// The slack that a rejection adds to the threshold, as a distance: with the bound L, computed in double from the stored float32 terms, and the
        /// distance dk that the kernel of the graph returns, L - dk &lt;= Margin except with probability alpha over the draw of the dither, so
        /// L &gt; threshold + Margin implies dk &gt; threshold. It covers every rounding that the probabilistic bound does not.
        /// </summary>
        internal static double Margin(int dimensions)
        {
            // A stored unit vector, a query and the center have a norm of at most U = 1 + (D + 3) 2^-23: the float32 norm and the division err by at most
            // D 2^-25 + 2^-24 and 2^-24 relative, and the center is a mean of such vectors. So n = |u - c| and |z| = |P (q - c)| are at most 2U, because P
            // is orthogonal. |w| = 1, so max|w_j| <= 1 and the step is s <= 2/15, which puts n s at most 4U/15. A decoded vector errs by at most s/2 per
            // coordinate, so its norm is at most 1 + sqrt(D) / 15.
            var epsilon = Math.ScaleB(1, -23);
            var unit = 1 + (dimensions + 3) * epsilon;
            var unitSquared = unit * unit;
            var root = Math.Sqrt(dimensions);

            // T1: the stored n s is rounded up by a relative 2^-23 at most, and the estimate multiplies it by (z·decoded w) / s: 2^-23 times n |z| times the
            // decoded norm.
            var storedScale = epsilon * 4 * unitSquared * (1 + root / 15);
            // T2: the stored c·y is rounded to float32, a relative 2^-24 of |c| |y| <= 2U².
            var storedCenterTerm = epsilon * unitSquared;
            // T3: the dither is a multiple of 2^-24, not a continuous draw. The centered error stays within the exp(t²/24) bound, because a discrete uniform
            // variable has a smaller moment generating function than the continuous one, but its mean is not 0: it is at most 2^-25 steps per coordinate,
            // which moves the distance by n s 2^-25 |z|_1 <= n s 2^-25 sqrt(D) |z|.
            var ditherMean = 8 * unitSquared * root * Math.ScaleB(1, -25) / 15;
            // T4: the double arithmetic of the encoder, the query preparation and the comparison: fewer than 2^8 D operations of relative error 2^-53 on
            // partial sums of at most 16 sqrt(D) times the factors above, which is below D² 2^-45.
            var doubleArithmetic = Math.ScaleB((double)dimensions * dimensions, -45);
            // T5: the kernel. The float32 dot product of D terms errs by at most (D + 1) 2^-24 |q| |u| in any order of summation, and the subtraction from 1
            // by 2^-24 (1 + |q| |u|). The int8 kernel sums the integers exactly and then rounds a few times, each by 2^-24 relative, which stays under this term.
            var kernel = (dimensions + 2) * unitSquared * epsilon;
            return storedScale + storedCenterTerm + ditherMean + doubleArithmetic + kernel;
        }

        // The smallest float32 that is not below the value.
        internal static float RoundUp(double value)
        {
            var rounded = (float)value;
            return rounded >= value ? rounded : MathF.BitIncrement(rounded);
        }

        public static float Scale(ReadOnlySpan<byte> row, int codeBytes) => BinaryPrimitives.ReadSingleLittleEndian(row[codeBytes..]);

        public static int Level(ReadOnlySpan<byte> row, int coordinate) => (row[coordinate / 2] >> (4 * (coordinate & 1))) & (Levels - 1);
    }

    /// <summary>
    /// A query prepared against the parameters of one graph; <see cref="Prepare"/> prepares it again for each query. Its buffers come from the allocator it is
    /// created with, so it must be disposed before the allocator is.
    /// </summary>
    internal sealed class CodeQuery : IDisposable
    {
        private readonly CodeParameters _parameters;
        // z, the rotated centered query, as doubles, then z in fixed point as shorts: the even coordinates, then the odd ones.
        private readonly ByteString _buffer;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _scope;
        private bool _hasDirection;
        // q·c, and the sum of z_j (7 + d_j).
        private double _offset;
        private double _zeroTerm;
        // The bound's multiplier times the centered norm of the query, rounded up.
        private double _errorFactor;
        // The value of one fixed-point unit, and what the rounding of z to fixed point may add to the dot of any row.
        private double _unit;
        private double _slack;

        public CodeQuery(CodeParameters parameters, ByteStringContext allocator)
        {
            _parameters = parameters;
            _scope = allocator.Allocate(parameters.Dimensions * sizeof(double) + 2 * parameters.CodeBytes * sizeof(short), out _buffer);
        }

        private Span<double> Rotated => MemoryMarshal.Cast<byte, double>(_buffer.ToSpan())[.._parameters.Dimensions];

        private Span<short> Halves => MemoryMarshal.Cast<byte, short>(_buffer.ToSpan()[(_parameters.Dimensions * sizeof(double))..]);

        public void Dispose() => _scope.Dispose();

        /// <summary>
        /// Prepares the query held in the tensor layout for <see cref="LowerBound"/> at the per-comparison allowance <paramref name="alpha"/>, the probability
        /// that one bound exceeds the exact distance.
        /// </summary>
        public CodeQuery Prepare(ReadOnlySpan<byte> tensor, double alpha)
        {
            if ((alpha > 0 && alpha < 1) == false)
                throw new ArgumentOutOfRangeException(nameof(alpha), alpha, "The allowance is a probability in the open interval (0, 1).");

            var z = Rotated;
            _hasDirection = CodeParameters.UnitVector(tensor, z);
            if (_hasDirection == false)
                return this;

            // z = P (q - c), the query relative to the center in the basis of the codes, and q·c.
            var center = _parameters.Center;
            double normSquared = 0, offset = 0;
            for (var k = 0; k < z.Length; k++)
            {
                offset += z[k] * center[k];
                z[k] -= center[k];
                normSquared += z[k] * z[k];
            }

            _parameters.Rotate(z);

            // PERF: z is rounded to shorts at a power-of-two step, the largest that keeps every short in range, so the kernel is an integer dot of the
            // levels and the shorts: exact in any order. Scaling by a power of two and by its inverse is exact, so the rounding of each coordinate is known
            // exactly, and it adds at most 15 times the sum of the absolute roundings to the dot of any row.
            var amplitude = Math.Abs(TensorPrimitives.MaxMagnitude<double>(z));
            var step = amplitude == 0 ? 1 : Math.ScaleB(1, Math.ILogB(short.MaxValue / amplitude));
            var halves = Halves;
            halves.Clear();
            var codeBytes = _parameters.CodeBytes;
            var dither = _parameters.Dither;
            double zeroTerm = 0, roundings = 0;
            for (var k = 0; k < z.Length; k++)
            {
                // A level decodes to (level - 7 - d_j) s, so the dot of z with a decoded code is s times the dot with the levels minus this sum.
                zeroTerm += z[k] * (CodeParameters.ZeroLevel + (double)dither[k]);
                var level = Math.Round(z[k] * step);
                roundings += Math.Abs(z[k] - level / step);
                halves[(k % 2 == 0 ? 0 : codeBytes) + k / 2] = (short)level;
            }

            // The distance that a code estimates errs by n z·e, with e the error of the encoding. A uniform error has a moment generating function of at most
            // exp(t²/24), so P(z·e < -m s |z|) <= exp(-6 m²) over the draw of the dither, which is alpha for m = sqrt(ln(1 / alpha) / 6). The bound subtracts
            // the stored n s times m |z|, with each factor rounded up.
            var multiplier = CodeParameters.RoundUp(Math.Sqrt(Math.Log(1 / alpha) / 6));
            _errorFactor = CodeParameters.RoundUp(multiplier * (double)CodeParameters.RoundUp(Math.Sqrt(normSquared)));
            _offset = offset;
            _zeroTerm = zeroTerm;
            _unit = 1 / step;
            _slack = CodeParameters.RoundUp((CodeParameters.Levels - 1) * roundings);
            return this;
        }

        /// <summary>
        /// A lower bound on the cosine distance between the query and the node that holds <paramref name="row"/>, which exceeds the exact distance
        /// with probability at most alpha over the draw of the graph's dither. Negative infinity when there is no bound: the query or the node has no direction.
        /// It does not cover the roundings that <see cref="CodeParameters.Margin"/> covers.
        /// </summary>
        public double LowerBound(ReadOnlySpan<byte> row)
        {
            Debug.Assert(row.Length == _parameters.RowBytes, "row.Length == _parameters.RowBytes");
            var bound = Distance(row, _slack, out var scale) - scale * _errorFactor;
            return double.IsFinite(bound) ? bound : double.NegativeInfinity;
        }

        /// <summary>The cosine distance that the code <paramref name="row"/> estimates, without the slack and the error term of <see cref="LowerBound"/>; NaN when the query or the node has no direction.</summary>
        public double Estimate(ReadOnlySpan<byte> row) => Distance(row, 0, out _);

        // NaN when the query or the node has no direction; the slack is added to the dot of the code and the query.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private double Distance(ReadOnlySpan<byte> row, double slack, out float scale)
        {
            var codeBytes = _parameters.CodeBytes;
            scale = CodeParameters.Scale(row, codeBytes);
            if (_hasDirection == false || float.IsNaN(scale))
                return double.NaN;

            // The node's unit vector is u = c + y with y = n P^T w, so q·u = q·c + c·y + n z·w. The estimate takes w as the decoded code; the slack of the
            // bound keeps the fixed-point dot at or above the exact one.
            var halves = Halves;
            var dot = Functions.DotProductNibbles(row[..codeBytes], halves[..codeBytes], halves[codeBytes..]) * _unit + slack;
            var centerTerm = BinaryPrimitives.ReadSingleLittleEndian(row[(codeBytes + sizeof(float))..]);
            return 1 - (_offset + centerTerm + scale * (dot - _zeroTerm));
        }
    }

    /// <summary>
    /// The prescreen of the queries of one SearchState, one query at a time, through its descent and its base-layer traversal. A filtered query screens: before
    /// the node record of a neighbor is resolved, the lower bound of the neighbor's code may reject it, and the query differs from the one that reads no code
    /// with probability at most <see cref="FailureBudget"/>. A query without a filter is steered by the <see cref="Estimate"/> of each code instead, which the
    /// budget does not cover. Its buffers come from the allocator it is created with, so it must be disposed before the allocator is.
    /// </summary>
    internal sealed unsafe class CodeScreen : IDisposable
    {
        internal const double FailureBudget = 0.01;

        private readonly CodeQuery _query;
        private readonly ByteStringContext _allocator;
        private long _maxNodeId;
        // PERF: One bit per node id of the snapshot, so a mark is one bit test and set, with no hashing: the neighbors that a screened query rejected, or those
        // that a steered query checked. Allocated at the first mark and kept across queries, which clear only the words that the query before set, so the
        // clearing follows the marks and not the graph.
        private ulong* _marks;
        private long _words;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _marksScope;
        private ContextBoundNativeList<int> _setWords;

        public CodeScreen(CodeParameters parameters, ByteStringContext allocator)
        {
            Margin = CodeParameters.Margin(parameters.Dimensions);
            _query = new CodeQuery(parameters, allocator);
            _allocator = allocator;
            _setWords = new ContextBoundNativeList<int>(allocator);
        }

        // A larger working set admits more neighbors before it settles. A traversal that spends the cap loses part of the saving, never correctness.
        public static int DefaultCap(int numberOfCandidates) => (int)Math.Min(Math.Max(512L, 4L * numberOfCandidates), int.MaxValue);

        public int Cap { get; private set; }
        public double Margin { get; }

        /// <summary>
        /// The admission events of the query so far: the moves of the descent, and the neighbors whose distance passes the threshold of full queues, so they
        /// enter the candidate queue. A node with an empty posting list counts too, although it stays out of the working set, because a screened query would not
        /// expand it.
        /// </summary>
        public int Admissions;

        /// <summary>The distinct neighbors rejected so far.</summary>
        public int Rejected { get; private set; }

        /// <summary>
        /// True while the descent screens. Its threshold is the distance of the current node, which is tighter than those of the base layer, so its rejections
        /// are not remembered.
        /// </summary>
        public bool Descending;

        /// <summary>
        /// Starts a query over a snapshot whose node ids end at <paramref name="maxNodeId"/>: prepares it and forgets the marks of the query before.
        /// </summary>
        public CodeScreen Begin(ReadOnlySpan<byte> tensor, int cap, long maxNodeId)
        {
            // Let U be the query without the screen, on the same graph: a mathematical object of this proof, not a query that runs. U reads no code, so which
            // neighbors it evaluates, in which order and against which thresholds is fixed before the dither is drawn. The screened query S evaluates the
            // neighbors of U in the same order minus the rejected ones, so it follows U until the first neighbor that S rejects and U admits. Rejecting an
            // admission event takes a bound above its distance by more than the margin, which has probability at most alpha. S screens an expansion only while
            // the events so far plus its neighbors fit under the cap, and the events so far are those of U up to the first difference, so every event that S
            // can reject is among the first cap events of U. Those are fixed, so by the union bound S differs from U with probability at most cap times alpha,
            // which this alpha makes the budget. Once the cap is spent the rest of the query runs unscreened.
            Cap = cap;
            Admissions = 0;
            Rejected = 0;
            Descending = false;
            _query.Prepare(tensor, FailureBudget / cap);

            foreach (var word in _setWords.ToSpan())
                _marks[word] = 0;
            _setWords.Clear();
            if (_marks != null && maxNodeId >> 6 >= _words)
            {
                _marksScope.Dispose();
                _marks = null;
            }

            _maxNodeId = maxNodeId;
            return this;
        }

        /// <summary>True when each of <paramref name="neighbors"/> checks can still stay inside the cap.</summary>
        public bool Screens(int neighbors) => (long)Admissions + neighbors <= Cap;

        /// <summary>
        /// Remembers a rejected neighbor of the base layer for the rest of the traversal: the threshold only tightens while the queues are full, so the
        /// rejection holds against every later threshold, and the id is never checked or resolved again. An id outside the snapshot is never remembered: the
        /// resolution of the node reports it.
        /// </summary>
        public void Reject(long nodeId)
        {
            if (Descending == false && (ulong)nodeId <= (ulong)_maxNodeId && Mark(nodeId))
                Rejected++;
        }

        public bool IsMarked(long nodeId) => _marks != null && (ulong)nodeId <= (ulong)_maxNodeId && (_marks[nodeId >> 6] & (1UL << (int)(nodeId & 63))) != 0;

        /// <summary>Marks the id; false when it was marked already. An id outside the snapshot is never marked, so it always reads as new: the resolution of the node reports it.</summary>
        public bool Mark(long nodeId)
        {
            if ((ulong)nodeId > (ulong)_maxNodeId)
                return true;

            if (_marks == null)
            {
                _words = (_maxNodeId >> 6) + 1;
                _marksScope = _allocator.Allocate(checked((int)_words * sizeof(ulong)), out ByteString bits);
                bits.ToSpan().Clear();
                _marks = (ulong*)bits.Ptr;
            }

            ref var word = ref _marks[nodeId >> 6];
            var bit = 1UL << (int)(nodeId & 63);
            if ((word & bit) != 0)
                return false;

            if (word == 0)
                _setWords.Add((int)(nodeId >> 6));
            word |= bit;
            return true;
        }

        public double Estimate(ReadOnlySpan<byte> row) => _query.Estimate(row);

        /// <summary>
        /// True when the code <paramref name="row"/> proves that its node is not admitted at <paramref name="threshold"/>: the bound exceeds it by the margin,
        /// strictly. In the descent the threshold is the distance of the current node. In the base layer the caller screens only while the working set is full,
        /// and so are the filter's nearest nodes of a filtered traversal, and the threshold is the distance of the farthest entry of the two.
        /// </summary>
        public bool Rejects(ReadOnlySpan<byte> row, double threshold) => Exceeds(_query.LowerBound(row), threshold, Margin);

        public static bool Exceeds(double bound, double threshold, double margin) => bound > threshold + margin;

        public void Dispose()
        {
            _query.Dispose();
            _setWords.Dispose();
            if (_marks != null)
                _marksScope.Dispose();
            _marks = null;
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
                    _codeParameters = _cache?.CodeParameters;
                    if (_codeParameters is null && ReadCodeParameters() is { } read)
                        _codeParameters = _cache?.PublishCodeParameters(read) ?? read;
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
        /// The stored row of the node, valid for the lifetime of the transaction; null for an id past the nodes of the graph. The row of a node that
        /// this transaction creates is stored by the commit of its registration.
        /// </summary>
        internal unsafe byte* CodeRow(long nodeId)
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

        // Reused by every traversal of this SearchState, which runs them one at a time.
        private CodeScreen _screen;

        /// <summary>The screen of the next traversal, which prepares the query once; null when the graph carries no codes.</summary>
        internal CodeScreen BeginScreen(ReadOnlySpan<byte> tensor, int numberOfCandidates)
        {
            if (_carriesCodes == false || Options.CountOfVectors == 0 || CodeParameters is not { } parameters)
                return null;

            _screen ??= new CodeScreen(parameters, Llt.Allocator);
            _screen.Begin(tensor, _forTestingPurposes?.Cap ?? CodeScreen.DefaultCap(numberOfCandidates), Options.CountOfVectors);
            if (_forTestingPurposes is not null)
                _forTestingPurposes.LastScreen = _screen;
            return _screen;
        }

        /// <summary>
        /// True when the screen drops an unresolved neighbor: it rejected the id earlier in the traversal, or <paramref name="screening"/> and the code
        /// <paramref name="row"/> of the id reject it against <paramref name="threshold"/> now.
        /// </summary>
        private unsafe bool ScreenRejects(CodeScreen screen, bool screening, long id, byte* row, double threshold)
        {
            if (screen.IsMarked(id))
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
