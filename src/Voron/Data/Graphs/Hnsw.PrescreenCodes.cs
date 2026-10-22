using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Numerics.Tensors;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Sparrow.Server.Tensors;

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
        /// </summary>
        public double LowerBound(ReadOnlySpan<byte> row)
        {
            Debug.Assert(row.Length == _parameters.RowBytes, "row.Length == _parameters.RowBytes");
            var codeBytes = _parameters.CodeBytes;
            var scale = CodeParameters.Scale(row, codeBytes);
            if (_hasDirection == false || float.IsNaN(scale))
                return double.NegativeInfinity;

            // The node's unit vector is u = c + y with y = n P^T w, so q·u = q·c + c·y + n z·w. The estimate takes w as the decoded code, and the slack keeps
            // the fixed-point dot at or above the exact one.
            var halves = Halves;
            var dot = Functions.DotProductNibbles(row[..codeBytes], halves[..codeBytes], halves[codeBytes..]) * _unit + _slack;
            var centerTerm = BinaryPrimitives.ReadSingleLittleEndian(row[(codeBytes + sizeof(float))..]);
            var estimate = 1 - (_offset + centerTerm + scale * (dot - _zeroTerm));
            var bound = estimate - scale * _errorFactor;
            return double.IsFinite(bound) ? bound : double.NegativeInfinity;
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
