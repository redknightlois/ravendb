using System;

namespace Sparrow.Server.Compression
{
    public interface IEncoderAlgorithm
    {
        void Train<TEncoderState, TSampleEnumerator>(in TEncoderState state, in TSampleEnumerator enumerator, int dictionarySize)
            where TEncoderState : struct, IEncoderState
            where TSampleEnumerator : struct, IReadOnlySpanEnumerator;

        int Encode<TEncoderState>(in TEncoderState state, in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
            where TEncoderState : struct, IEncoderState;

        int Decode<TEncoderState>(in TEncoderState state, in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
            where TEncoderState : struct, IEncoderState;

        int Decode<TEncoderState>(in TEncoderState state, int bits, in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
            where TEncoderState : struct, IEncoderState;
    }
}
