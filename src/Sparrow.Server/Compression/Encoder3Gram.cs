using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Collections;
using Sparrow.Server.Binary;

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
        public static int GetDictionarySize<TEncoderState>(in TEncoderState state) where TEncoderState : struct, IEncoderState
        {
            var dictionary = new Encoder3GramDictionary<TEncoderState>(state);
            return dictionary.MemoryUse;
        }

        public void Train<TEncoderState, TSampleEnumerator>(in TEncoderState state, in TSampleEnumerator enumerator, int dictionarySize)
            where TEncoderState : struct, IEncoderState
            where TSampleEnumerator : struct, IReadOnlySpanEnumerator
        {
            var dictionary = new Encoder3GramDictionary<TEncoderState>(state);

            var symbolSelector = new Encoder3GramSymbolSelector<TSampleEnumerator>();
            var frequencyList = symbolSelector.SelectSymbols(enumerator, dictionarySize);

            var codeAssigner = new HuTuckerCodeAssigner();
            var symbolCodes = codeAssigner.AssignCodes(frequencyList);

            dictionary.Build(symbolCodes);
        }

        public int Encode<TEncoderState>(in TEncoderState state, in ReadOnlySpan<byte> key, in Span<byte> outputBuffer)
            where TEncoderState : struct, IEncoderState
        {
            Debug.Assert(outputBuffer.Length % sizeof(long) == 0); // Ensure we can safely cast to int 64

            var dictionary = new Encoder3GramDictionary<TEncoderState>(state);

            var intBuf = MemoryMarshal.Cast<byte, long>(outputBuffer);

            int idx = 0;
            intBuf[0] = 0;
            int intBufLen = 0;

            var keyStr = key;
            int pos = 0;
            while (pos < key.Length)
            {
                int prefixLen = dictionary.Lookup(keyStr.Slice(pos), out Code code);
                long sBuf = code.Value;
                int sLen = code.Length;
                // Console.WriteLine($"[{sBuf}|{sLen}|{Encoding.ASCII.GetString(keyStr.Slice(pos))}],");
                if (intBufLen + sLen > 63)
                {
                    int numBitsLeft = 64 - intBufLen;
                    intBufLen = sLen - numBitsLeft;
                    intBuf[idx] <<= numBitsLeft;
                    intBuf[idx] |= (sBuf >> intBufLen);
                    intBuf[idx] = BinaryPrimitives.ReverseEndianness(intBuf[idx]);
                    intBuf[idx + 1] = sBuf;
                    idx++;
                }
                else
                {
                    intBuf[idx] <<= sLen;
                    intBuf[idx] |= sBuf;
                    intBufLen += sLen;
                }

                pos += prefixLen;
            }

            //Console.WriteLine();
            intBuf[idx] <<= (64 - intBufLen);
            intBuf[idx] = BinaryPrimitives.ReverseEndianness(intBuf[idx]);
            return ((idx << 6) + intBufLen);
        }

        public int Decode<TEncoderState>(in TEncoderState state, in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
            where TEncoderState : struct, IEncoderState
        {
            Debug.Assert(outputBuffer.Length % sizeof(long) == 0); // Ensure we can safely cast to int 64

            var dictionary = new Encoder3GramDictionary<TEncoderState>(state);

            var buffer = outputBuffer;
            var reader = new BitReader(data);
            while (reader.Length > 0)
            {
                int length = dictionary.Lookup(reader, out var symbol);
                if (length < 0)
                    throw new IOException("Invalid data stream.");

                // Advance the reader.
                reader.Skip(length);

                symbol.CopyTo(buffer);
                buffer = buffer.Slice(symbol.Length);

                if (symbol[0] == 0)
                    break;
            }

            return outputBuffer.Length - buffer.Length;
        }

        public int Decode<TEncoderState>(in TEncoderState state, int bits, in ReadOnlySpan<byte> data, in Span<byte> outputBuffer)
            where TEncoderState : struct, IEncoderState
        {
            Debug.Assert(outputBuffer.Length % sizeof(long) == 0); // Ensure we can safely cast to int 64

            var dictionary = new Encoder3GramDictionary<TEncoderState>(state);

            var buffer = outputBuffer;
            var reader = new BitReader(data);
            while (bits > 0)
            {
                int length = dictionary.Lookup(reader, out var symbol);
                if (length < 0)
                    throw new IOException("Invalid data stream.");

                // Advance the reader.
                reader.Skip(length);

                symbol.CopyTo(buffer);
                buffer = buffer.Slice(symbol.Length);

                bits -= length;
            }

            return outputBuffer.Length - buffer.Length;
        }
    }
}
