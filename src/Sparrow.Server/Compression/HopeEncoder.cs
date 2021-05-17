﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Compression
{
    public sealed class HopeEncoder<TAlgorithm>
        where TAlgorithm : struct, IEncoderAlgorithm
    {
        private TAlgorithm _encoder;
        private int _maxSequenceLength;

        public HopeEncoder(TAlgorithm encoder = default)
        {
            _encoder = encoder;
            _maxSequenceLength = _encoder.MaxBitSequenceLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Train<TSampleEnumerator>(in TSampleEnumerator enumerator, int dictionarySize)
            where TSampleEnumerator : struct, IReadOnlySpanEnumerator
        {
            _encoder.Train(enumerator, dictionarySize);
            _maxSequenceLength = _encoder.MaxBitSequenceLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Encode<TSource, TDestination>(in TSource inputBuffers, in TDestination outputBuffers, Span<int> outputSizes)
            where TSource : struct, IReadOnlySpanEnumerator
            where TDestination : struct, ISpanEnumerator
        {
            if (outputBuffers.Length != outputSizes.Length)
                throw new ArgumentException($"'{nameof(outputBuffers)}' and '{nameof(outputSizes)}' must be of the same size.");

            if (outputBuffers.Length != inputBuffers.Length)
                throw new ArgumentException($"'{nameof(outputBuffers)}' and '{nameof(inputBuffers)}' must be of the same size.");

            _encoder.EncodeBatch(in inputBuffers, outputSizes, in outputBuffers);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Decode<TSource, TDestination>(in TSource inputBuffers, in TDestination outputBuffers, Span<int> outputSizes)
            where TSource : struct, IReadOnlySpanEnumerator
            where TDestination : struct, ISpanEnumerator
        {
            if (outputBuffers.Length != outputSizes.Length)
                throw new ArgumentException($"'{nameof(outputBuffers)}' and '{nameof(outputSizes)}' must be of the same size.");

            if (outputBuffers.Length != inputBuffers.Length)
                throw new ArgumentException($"'{nameof(outputBuffers)}' and '{nameof(inputBuffers)}' must be of the same size.");

            _encoder.DecodeBatch(in inputBuffers, outputSizes, in outputBuffers);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Encode(in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
        {
            return _encoder.Encode(data, outputBuffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Decode(in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
        {
            return _encoder.Decode(data, outputBuffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Decode(int bits, in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
        {
            return _encoder.Decode(bits, data, outputBuffer);
        }

        public int GetMaxEncodingBytes(int keySize)
        {
            if (_maxSequenceLength < 1)
                throw new InvalidOperationException("Cannot calculate without a trained dictionary");

            return (_maxSequenceLength * keySize) / 8 + 1;
        }
    }
}
