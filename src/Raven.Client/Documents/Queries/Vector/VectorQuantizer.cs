using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;

namespace Raven.Client.Documents.Queries.Vector;

public static class VectorQuantizer
{
    private static ReadOnlySpan<byte> BinaryQuantizerLookup => [0b_1000_0000, 0b_0100_0000, 0b_0010_0000, 0b_0001_0000, 0b_0000_1000, 0b_0000_0100, 0b_0000_0010, 0b_0000_0001];

    internal static bool TryToInt8(ReadOnlySpan<float> rawEmbedding, ReadOnlySpan<sbyte> destination, out int usedBytes)
    {
        usedBytes = 0;
        if (destination.Length < rawEmbedding.Length)
            return false;
        ref var sourceRef = ref MemoryMarshal.GetReference(rawEmbedding);
        var maxComponent = float.MinValue;
        for (int i = 0; i < rawEmbedding.Length; i++)
            maxComponent = Math.Max(maxComponent, Math.Abs(Unsafe.Add(ref sourceRef, i)));
        
        // A vector without a finite, positive scale (all zero, a tiny largest component, a NaN or infinite component) converts to zeros with magnitude 0.
        var scaleFactor = 127f / maxComponent;
        var hasScale = scaleFactor > 0 && float.IsInfinity(scaleFactor) == false;
        ref var destinationRef = ref MemoryMarshal.GetReference(destination);
        
        for (var i = 0; i < rawEmbedding.Length; i++)
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
        for (nuint j = 0; j < inputLength; j++)
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
