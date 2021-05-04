using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Collections;

namespace Sparrow.Server.Compression
{
    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct Interval3Gram
    {
        [FieldOffset(0)]
        private byte _startKey;
        [FieldOffset(3)]
        public byte PrefixLength;
        [FieldOffset(4)]
        public Code Code;

        public Span<byte> StartKey => new(Unsafe.AsPointer(ref _startKey), 3);
    }

    public struct Encoder3Gram : IEncoderAlgorithm
    {
        public void Train<TEncoderState, TSampleEnumerator>(in TEncoderState state, in TSampleEnumerator enumerator, int dictionarySize)
            where TEncoderState : struct, IEncoderState
            where TSampleEnumerator : struct, IReadOnlySpanEnumerator
        {
            var dict_ = new Encoder3GramDictionary<TEncoderState>(state);

            var symbolSelector = new Encoder3GramSymbolSelector<TSampleEnumerator>();
            var frequency_list = symbolSelector.SelectSymbols(enumerator, dictionarySize);

            var codeAssigner = new HuTuckerCodeAssigner();
            var symbolCodes = codeAssigner.AssignCodes(frequency_list);

            dict_.Build(symbolCodes);
        }

        public int Encode<TEncoderState>(in TEncoderState state, in ReadOnlySpan<byte> key, in Span<byte> outputBuffer)
            where TEncoderState : struct, IEncoderState
        {
            Debug.Assert(outputBuffer.Length % sizeof(long) == 0); // Ensure we can safely cast to int 64

            var dict_ = new Encoder3GramDictionary<TEncoderState>(state);

            var int_buf = MemoryMarshal.Cast<byte, long>(outputBuffer);

            int idx = 0;
            int_buf[0] = 0;
            int int_buf_len = 0;

            var key_str = key;
            int pos = 0;
            while (pos < key.Length)
            {
                int prefix_len = dict_.Lookup(key_str.Slice(pos), out Code code);
                long s_buf = code.Value;
                int s_len = code.Length;
                if (int_buf_len + s_len > 63)
                {
                    int num_bits_left = 64 - int_buf_len;
                    int_buf_len = s_len - num_bits_left;
                    int_buf[idx] <<= num_bits_left;
                    int_buf[idx] |= (s_buf >> int_buf_len);
                    int_buf[idx] = BinaryPrimitives.ReverseEndianness(int_buf[idx]);
                    int_buf[idx + 1] = s_buf;
                    idx++;
                }
                else
                {
                    int_buf[idx] <<= s_len;
                    int_buf[idx] |= s_buf;
                    int_buf_len += s_len;
                }

                pos += prefix_len;
            }

            int_buf[idx] <<= (64 - int_buf_len);
            int_buf[idx] = BinaryPrimitives.ReverseEndianness(int_buf[idx]);
            return ((idx << 6) + int_buf_len);
        }

        public int Decode<TEncoderState>(in TEncoderState state, in ReadOnlySpan<byte> data, in Span<byte> outputBuffer) 
            where TEncoderState : struct, IEncoderState
        {
            throw new NotImplementedException();
        }
    }
}
