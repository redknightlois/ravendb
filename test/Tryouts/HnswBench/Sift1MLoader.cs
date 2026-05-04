using System;
using System.IO;
using System.Numerics.Tensors;
using System.Runtime.InteropServices;

namespace Tryouts.HnswBench;

/// <summary>
/// Loader for SIFT-1M (Jegou et al., http://corpus-texmex.irisa.fr) fvecs files. Each record
/// is <c>int32 dim</c> followed by <c>dim × float32</c>; SIFT-1M is 1,000,000 × 128. Loads
/// up to <paramref name="count"/> records and normalises each to unit length so the cosine
/// similarity kernel produces a meaningful distance distribution. SIFT vectors are integer-
/// valued floats in [0, 218] before normalisation; their L2 norms cluster tightly so the
/// post-normalisation distribution is markedly more anisotropic than uniform-on-[-1,1].
/// </summary>
internal static class Sift1MLoader
{
    public const int VectorDim = 128;

    private const string BasePath = "/shared/disk2/datasets/sift1m/sift/sift_base.fvecs";
    private const string QueryPath = "/shared/disk2/datasets/sift1m/sift/sift_query.fvecs";
    private const string GroundTruthPath = "/shared/disk2/datasets/sift1m/sift/sift_groundtruth.ivecs";

    public static float[][] LoadBase(int count) => LoadFvecs(BasePath, count);
    public static float[][] LoadQueries(int count) => LoadFvecs(QueryPath, count);

    /// <summary>
    /// Loads the pre-computed top-100 nearest-neighbor ground-truth shipped with SIFT-1M.
    /// Each ivecs record is <c>int32 K</c> followed by <c>K × int32</c> base ids (0-indexed).
    /// SIFT's groundtruth ranks by L2; on unit-normalized vectors L2 and cosine produce the
    /// same ranking (||a-b||^2 = 2 - 2&lt;a,b&gt;), so this groundtruth is valid for the bench's
    /// cosine path. Returns ids translated into the bench's <c>entryId = (i + 1) * 100</c>
    /// convention; the resulting array has shape [queries][topK] truncated/padded to topK.
    /// </summary>
    public static long[][] LoadGroundTruth(int queryCount, int topK)
    {
        using var stream = new FileStream(GroundTruthPath, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 20, FileOptions.SequentialScan);
        var result = new long[queryCount][];

        Span<byte> dimBuf = stackalloc byte[sizeof(int)];
        for (int q = 0; q < queryCount; q++)
        {
            int read = stream.Read(dimBuf);
            if (read == 0)
            {
                Array.Resize(ref result, q);
                return result;
            }
            if (read != sizeof(int))
                throw new EndOfStreamException($"Truncated dim header at gt record {q}");
            int k = MemoryMarshal.Read<int>(dimBuf);
            if (k <= 0)
                throw new FormatException($"Invalid groundtruth K={k} at record {q}");
            if (topK > k)
                throw new InvalidOperationException($"Requested topK={topK} but groundtruth has only {k} entries per query");

            var raw = new int[k];
            int payloadBytes = k * sizeof(int);
            int got = stream.Read(MemoryMarshal.AsBytes(raw.AsSpan()));
            if (got != payloadBytes)
                throw new EndOfStreamException($"Truncated payload at gt record {q}: read {got}/{payloadBytes}");

            var top = new long[topK];
            for (int j = 0; j < topK; j++)
                top[j] = ((long)raw[j] + 1L) * 100L;
            result[q] = top;
        }
        return result;
    }

    private static float[][] LoadFvecs(string path, int count)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 20, FileOptions.SequentialScan);
        var result = new float[count][];

        Span<byte> dimBuf = stackalloc byte[sizeof(int)];
        for (int i = 0; i < count; i++)
        {
            int read = stream.Read(dimBuf);
            if (read == 0)
            {
                Array.Resize(ref result, i);
                return result;
            }
            if (read != sizeof(int))
                throw new EndOfStreamException($"Truncated dim header at record {i}");
            int d = MemoryMarshal.Read<int>(dimBuf);
            if (d != VectorDim)
                throw new FormatException($"Expected dim={VectorDim}, got {d} at record {i}");

            var vec = new float[VectorDim];
            int payloadBytes = VectorDim * sizeof(float);
            int got = stream.Read(MemoryMarshal.AsBytes(vec.AsSpan()));
            if (got != payloadBytes)
                throw new EndOfStreamException($"Truncated payload at record {i}: read {got}/{payloadBytes}");

            NormalizeInPlace(vec);
            result[i] = vec;
        }
        return result;
    }

    private static void NormalizeInPlace(float[] v)
    {
        float norm = MathF.Sqrt(TensorPrimitives.Dot<float>(v, v));
        if (norm <= 0f) return;
        float inv = 1f / norm;
        for (int i = 0; i < v.Length; i++)
            v[i] *= inv;
    }
}
