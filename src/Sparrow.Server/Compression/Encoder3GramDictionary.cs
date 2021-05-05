using System;
using System.Runtime.InteropServices;
using Sparrow.Collections;

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


        private Span<int> _numberOfEntries => MemoryMarshal.Cast<byte, int>(State.Table.Slice(0, 4));
        private Span<Interval3Gram> _table => MemoryMarshal.Cast<byte, Interval3Gram>(State.Table.Slice(4));

        public void Build(in FastList<SymbolCode> symbol_code_list)
        {
            var table = _table;

            int dict_size = symbol_code_list.Count;
            int numberOfEntries = (State.Table.Length - 4) / sizeof(Interval3Gram);

            if (numberOfEntries < dict_size)
                throw new ArgumentException("Not enough memory to store the dictionary");

            for (int i = 0; i < dict_size; i++)
            {
                var symbol = symbol_code_list[i].StartKey;
                int symbol_len = symbol_code_list[i].Length;

                var startKey = table[i].StartKey;
                for (int j = 0; j < 3; j++)
                {
                    if (j < symbol_len)
                        startKey[j] = symbol[j];
                    else
                        startKey[j] = 0;
                }

                if (i < dict_size - 1)
                {
                    table[i].PrefixLength = 0;
                    var next_symbol = symbol_code_list[i + 1].StartKey;
                    int next_symbol_len = symbol_code_list[i + 1].Length;

                    next_symbol[next_symbol_len - 1] -= 1;
                    int j = 0;
                    while (j < symbol_len && j < next_symbol_len && symbol[j] == next_symbol[j])
                    {
                        table[i].PrefixLength++;
                        j++;
                    }
                }
                else
                {
                    table[i].PrefixLength = (byte)symbol_len;
                }

                table[i].Code = symbol_code_list[i].Code;
            }

            _numberOfEntries[0] = dict_size;
         }

        public int Lookup(in ReadOnlySpan<byte> symbol, out Code code)
        {
            var table = _table;

            int l = 0;
            int r = _numberOfEntries[0];
            while (r - l > 1)
            {
                int m = (l + r) >> 1;
                
                // TODO: Check this is doing the right thing. If we need to improve this we can do it very easily (see SpanHelpers.Byte.cs file)
                int cmp = symbol.SequenceCompareTo(table[m].StartKey);
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
    }
}
