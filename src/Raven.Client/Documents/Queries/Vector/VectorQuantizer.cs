using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
#if NET9_0_OR_GREATER
using System.Runtime.Intrinsics;
#endif
using Sparrow;

namespace Raven.Client.Documents.Queries.Vector;

public static class VectorQuantizer
{
    private static ReadOnlySpan<byte> BinaryQuantizerLookup => [0b_1000_0000, 0b_0100_0000, 0b_0010_0000, 0b_0001_0000, 0b_0000_1000, 0b_0000_0100, 0b_0000_0010, 0b_0000_0001];

    internal static bool TryToInt8(ReadOnlySpan<float> rawEmbedding, ReadOnlySpan<sbyte> destination, out int usedBytes)
    {
        usedBytes = 0;
        if (destination.Length < rawEmbedding.Length + sizeof(float))
            return false;
        ref var sourceRef = ref MemoryMarshal.GetReference(rawEmbedding);
        var maxComponent = float.MinValue;
        var i = 0;
#if NET9_0_OR_GREATER
        if (Vector128.IsHardwareAccelerated)
        {
            var max = Vector128<float>.Zero;
            for (; i + Vector128<float>.Count <= rawEmbedding.Length; i += Vector128<float>.Count)
                max = Vector128.Max(max, Vector128.Abs(Vector128.LoadUnsafe(ref sourceRef, (nuint)i)));
            for (var lane = 0; lane < Vector128<float>.Count; lane++)
                maxComponent = Math.Max(maxComponent, max.GetElement(lane));
        }
#endif
        for (; i < rawEmbedding.Length; i++)
            maxComponent = Math.Max(maxComponent, Math.Abs(Unsafe.Add(ref sourceRef, i)));
        
        // A vector without a finite, positive scale (all zero, a tiny largest component, a NaN or infinite component) converts to zeros with magnitude 0.
        var scaleFactor = 127f / maxComponent;
        var hasScale = scaleFactor > 0 && float.IsInfinity(scaleFactor) == false;
        ref var destinationRef = ref MemoryMarshal.GetReference(destination);
        i = 0;
#if NET9_0_OR_GREATER
        // A step reads its sixteen components before it writes their bytes, so the destination may overlap the source from its start.
        if (hasScale && Vector128.IsHardwareAccelerated)
        {
            var scale = Vector128.Create(scaleFactor);
            for (; i + Vector128<sbyte>.Count <= rawEmbedding.Length; i += Vector128<sbyte>.Count)
            {
                var q0 = Quantize(Vector128.LoadUnsafe(ref sourceRef, (nuint)i), scale);
                var q1 = Quantize(Vector128.LoadUnsafe(ref sourceRef, (nuint)(i + 4)), scale);
                var q2 = Quantize(Vector128.LoadUnsafe(ref sourceRef, (nuint)(i + 8)), scale);
                var q3 = Quantize(Vector128.LoadUnsafe(ref sourceRef, (nuint)(i + 12)), scale);
                Vector128.Narrow(Vector128.Narrow(q0, q1), Vector128.Narrow(q2, q3)).StoreUnsafe(ref destinationRef, (nuint)i);
            }
        }

        // Rounds half to even, as Convert.ToSByte does.
        static Vector128<int> Quantize(Vector128<float> components, Vector128<float> scale) => Vector128.ConvertToInt32(Vector128.Round(components * scale));
#endif
        for (; i < rawEmbedding.Length; i++)
        {
            Unsafe.Add(ref destinationRef, i) = hasScale ? Convert.ToSByte(Unsafe.Add(ref sourceRef, i) * scaleFactor) : (sbyte)0;
        }
        
        // Store magnitude as four last sbytes
        Unsafe.As<sbyte, float>(ref Unsafe.Add(ref destinationRef, rawEmbedding.Length)) = hasScale ? maxComponent : 0;
        
        usedBytes = rawEmbedding.Length + sizeof(float);
        return true;
    }

    public static sbyte[] ToInt8(float[] rawEmbedding)
    {
        var mem = new sbyte[rawEmbedding.Length + sizeof(float)];
        TryToInt8(rawEmbedding, mem, out _);
        return mem;
    }
    
    public static byte[] ToInt1(ReadOnlySpan<float> rawEmbedding)
    {
        const byte dimensionsInOneByte = 8;
        var outputLength = ((int)rawEmbedding.Length / dimensionsInOneByte + (rawEmbedding.Length % dimensionsInOneByte != 0).ToInt32());
        var bytes = new byte[outputLength];
        TryToInt1(rawEmbedding, bytes, out _);
        return bytes;
    }
    
    internal static bool TryToInt1(ReadOnlySpan<float> source, ReadOnlySpan<byte> destination, out int usedBytes)
    {
        const byte dimensionsInOneByte = 8;
        var inputLength = (nuint)source.Length;
        var outputLength = (nuint)((int)inputLength / dimensionsInOneByte + ((int)inputLength % dimensionsInOneByte != 0).ToInt32());

        if ((int)outputLength > destination.Length)
        {
            usedBytes = 0;
            return false;
        }
        
        ref var resultRef = ref MemoryMarshal.GetReference(destination);
        ref var embeddingRef = ref MemoryMarshal.GetReference(source);
        ref var lookupTableRef = ref MemoryMarshal.GetReference(BinaryQuantizerLookup);
        nuint j = 0;
#if NET9_0_OR_GREATER
        // A step reads its eight components before it writes their byte, so the destination may overlap the source from its start.
        if (Vector128.IsHardwareAccelerated)
        {
            var high = Vector128.Create(0b_1000_0000, 0b_0100_0000, 0b_0010_0000, 0b_0001_0000);
            var low = Vector128.Create(0b_0000_1000, 0b_0000_0100, 0b_0000_0010, 0b_0000_0001);
            for (; j + dimensionsInOneByte <= inputLength; j += dimensionsInOneByte)
            {
                var first = Vector128.GreaterThanOrEqual(Vector128.LoadUnsafe(ref embeddingRef, j), Vector128<float>.Zero).AsInt32() & high;
                var second = Vector128.GreaterThanOrEqual(Vector128.LoadUnsafe(ref embeddingRef, j + 4), Vector128<float>.Zero).AsInt32() & low;
                Unsafe.AddByteOffset(ref resultRef, j / dimensionsInOneByte) = (byte)Vector128.Sum(first | second);
            }
        }
#endif
        for (; j < inputLength; j++)
        {
            var result = Unsafe.Add(ref embeddingRef, j) >= 0 
                ? Unsafe.AddByteOffset(ref lookupTableRef, j % dimensionsInOneByte) 
                : byte.MinValue;

            // In case when source and destination is the same memory we've to clean it first
            if (j % dimensionsInOneByte == 0)
                Unsafe.AddByteOffset(ref resultRef, j / dimensionsInOneByte) = 0;

            Unsafe.AddByteOffset(ref resultRef, j / dimensionsInOneByte) |= result;
        }

        usedBytes = (int)outputLength;
        return true;
    }
}
