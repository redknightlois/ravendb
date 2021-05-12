using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Compression
{
    public sealed class HopeEncoder<TAlgorithm> where TAlgorithm : struct, IEncoderAlgorithm
    {
        private TAlgorithm _encoder;

        public HopeEncoder(TAlgorithm encoder = default)
        {
            _encoder = encoder;
        }

        public void Train<TEncoderState, TSampleEnumerator>(in TEncoderState state, in TSampleEnumerator enumerator, int dictionarySize)
            where TSampleEnumerator : struct, IReadOnlySpanEnumerator
            where TEncoderState : struct, IEncoderState
        {
            _encoder.Train(state, enumerator, dictionarySize);
        }

        public void Encode<TEncoderState, TSource, TDestination>(in TEncoderState state, in TSource inputBuffers, in TDestination outputBuffers, in Span<int> outputSizes)
            where TEncoderState : struct, IEncoderState
            where TSource : struct, IReadOnlySpanEnumerator
            where TDestination : struct, ISpanEnumerator
        {
            if (outputBuffers.Length != outputSizes.Length)
                throw new ArgumentException($"'{nameof(outputBuffers)}' and '{nameof(outputSizes)}' must be of the same size.");

            if (outputBuffers.Length != inputBuffers.Length)
                throw new ArgumentException($"'{nameof(outputBuffers)}' and '{nameof(inputBuffers)}' must be of the same size.");

            // FIXME: This is stupidly inefficient, but just doing so to showcase how it works.
            for (int i = 0; i < inputBuffers.Length; i++)
            {
                var input = inputBuffers[i];
                var output = outputBuffers[i];
                outputSizes[i] = _encoder.Encode(in state, in input, in output);
            }
        }

        public void Decode<TEncoderState, TSource, TDestination>(in TEncoderState state, in TSource inputBuffers, in TDestination outputBuffers, in Span<int> outputSizes)
            where TEncoderState : struct, IEncoderState
            where TSource : struct, IReadOnlySpanEnumerator
            where TDestination : struct, ISpanEnumerator
        {
            if (outputBuffers.Length != outputSizes.Length)
                throw new ArgumentException($"'{nameof(outputBuffers)}' and '{nameof(outputSizes)}' must be of the same size.");

            if (outputBuffers.Length != inputBuffers.Length)
                throw new ArgumentException($"'{nameof(outputBuffers)}' and '{nameof(inputBuffers)}' must be of the same size.");

            // FIXME: This is stupidly inefficient, but just doing so to showcase how it works.
            for (int i = 0; i < inputBuffers.Length; i++)
            {
                var input = inputBuffers[i];
                var output = outputBuffers[i];
                outputSizes[i] = _encoder.Decode(in state, in input, in output);
            }
        }

        public int Encode<TEncoderState>(in TEncoderState state, in ReadOnlySpan<byte> data, in Span<byte> outputBuffer) where TEncoderState : struct, IEncoderState
        {
            return _encoder.Encode(in state, in data, in outputBuffer);
        }

        public int Decode<TEncoderState>(in TEncoderState state, in ReadOnlySpan<byte> data, in Span<byte> outputBuffer) where TEncoderState : struct, IEncoderState
        {
            return _encoder.Decode(in state, in data, in outputBuffer);
        }

        public int Decode<TEncoderState>(in TEncoderState state, int bits, in ReadOnlySpan<byte> data, in Span<byte> outputBuffer) where TEncoderState : struct, IEncoderState
        {
            return _encoder.Decode(in state, bits, in data, in outputBuffer);
        }
    }
}
