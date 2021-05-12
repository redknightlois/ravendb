using System;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Collections;
using Sparrow.Server.Binary;
using Sparrow.Server.Collections.Persistent;

namespace Sparrow.Server.Compression
{
    internal unsafe struct Encoder3GramDictionary<TEncoderState>
        where TEncoderState : struct, IEncoderState
    {
        public readonly TEncoderState State;
        public int NumberOfEntries => _numberOfEntries[0];
        public int MemoryUse => _numberOfEntries[0] * sizeof(Interval3Gram);

        public Encoder3GramDictionary(in TEncoderState state)
        {
            State = state;
        }

        private readonly Span<int> _numberOfEntries => MemoryMarshal.Cast<byte, int>(State.EncodingTable.Slice(0, 4));
        private readonly Span<Interval3Gram> EncodingTable => MemoryMarshal.Cast<byte, Interval3Gram>(State.EncodingTable.Slice(4));
        private readonly BinaryTree<short> DecodingTable => BinaryTree<short>.Open(State.DecodingTable);

        public void Build(in FastList<SymbolCode> symbolCodeList)
        {
            var table = EncodingTable;

            int dictSize = symbolCodeList.Count;
            if (dictSize >= short.MaxValue)
                throw new NotSupportedException($"We do not support dictionaries with more items than {short.MaxValue - 1}");

            // We haven't stored it yet. So we are calculating it. 
            int numberOfEntries = (State.EncodingTable.Length - 4) / sizeof(Interval3Gram);
            if (numberOfEntries < dictSize)
                throw new ArgumentException("Not enough memory to store the dictionary");

            var tree = BinaryTree<short>.Create(State.DecodingTable);

            for (int i = 0; i < dictSize; i++)
            {
                var symbol = symbolCodeList[i].StartKey;
                int symbolLength = symbolCodeList[i].Length;

                ref var entry = ref table[i];

                var startKey = entry.StartKey;
                for (int j = 0; j < 3; j++)
                {
                    if (j < symbolLength)
                        startKey[j] = symbol[j];
                    else
                        startKey[j] = 0;
                }

                if (i < dictSize - 1)
                {
                    entry.PrefixLength = 0;
                    var nextSymbol = symbolCodeList[i + 1].StartKey;
                    int nextSymbolLength = symbolCodeList[i + 1].Length;

                    nextSymbol[nextSymbolLength - 1] -= 1;
                    int j = 0;
                    while (j < symbolLength && j < nextSymbolLength && symbol[j] == nextSymbol[j])
                    {
                        entry.PrefixLength++;
                        j++;
                    }
                }
                else
                {
                    entry.PrefixLength = (byte)symbolLength;
                }

                entry.Code = symbolCodeList[i].Code;

                int codeValue = BinaryPrimitives.ReverseEndianness(entry.Code.Value << (sizeof(int) * 8 - entry.Code.Length));
                var codeValueSpan = MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref codeValue, 1));
                var reader = new BitReader(codeValueSpan, entry.Code.Length);
                tree.Add(ref reader, (short)i);
            }

            _numberOfEntries[0] = dictSize;
         }

        public int Lookup(in ReadOnlySpan<byte> symbol, out Code code)
        {
            var table = EncodingTable;

            int l = 0;
            int r = _numberOfEntries[0];
            while (r - l > 1)
            {
                int m = (l + r) >> 1;

                // TODO: Check this is doing the right thing. If we need to improve this we can do it very easily (see SpanHelpers.Byte.cs file)
                ref var entry = ref table[m];
                int cmp = symbol.SequenceCompareTo(entry.StartKey.Slice(0, entry.PrefixLength));

                if (cmp < 0)
                    r = m;
                else 
                    l = m;

                if (cmp == 0)
                    break;
            }

            code = table[l].Code;
            return table[l].PrefixLength;
        }

        public int Lookup(in BitReader reader, out ReadOnlySpan<byte> symbol)
        {
            BitReader localReader = reader;
            if (DecodingTable.FindCommonPrefix(ref localReader, out var idx))
            {
                ref var entry = ref EncodingTable[idx];
                symbol = entry.StartKey.Slice(0, entry.PrefixLength);

                return reader.Length - localReader.Length;
            }

            symbol = ReadOnlySpan<byte>.Empty;
            return -1;
        }
    }
}
