using System;
using System.Runtime.InteropServices;
using Sparrow.Collections;

namespace Sparrow.Server.Compression
{
    internal unsafe struct Encoder3GramDictionary<TEncoderState>
        where TEncoderState : struct, IEncoderState
    {
        public readonly TEncoderState State;
        public readonly int NumberOfEntries;
        public int MemoryUse => NumberOfEntries * sizeof(Interval3Gram);

        public Encoder3GramDictionary(in TEncoderState state)
        {
            State = state;
            NumberOfEntries = state.Table.Length / sizeof(Interval3Gram);
        }

        private Span<Interval3Gram> Table => MemoryMarshal.Cast<byte, Interval3Gram>(State.Table);

        public void Build(in FastList<SymbolCode> symbol_code_list)
        {
            var table = Table;

            int dict_size = symbol_code_list.Count;
            if (NumberOfEntries < dict_size)
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
        }

        public int Lookup(in ReadOnlySpan<byte> symbol, out Code code)
        {
            var table = Table;

            int l = 0;
            int r = NumberOfEntries;
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
