using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Collections;
using Sparrow.Server.Binary;
using Sparrow.Server.Collections.Persistent;

namespace Sparrow.Server.Compression
{
    public static class StringEscaper
    {
        public static string EscapeForCSharp(this string str)
        {
            StringBuilder sb = new StringBuilder();
            foreach (char c in str)
                switch (c)
                {
                    case '\'':
                    case '"':
                    case '\\':
                        sb.Append(c.EscapeForCSharp());
                        break;
                    default:
                        if (char.IsControl(c))
                            sb.Append(c.EscapeForCSharp());
                        else
                            sb.Append(c);
                        break;
                }
            return sb.ToString();
        }
        public static string EscapeForCSharp(this char chr)
        {
            switch (chr)
            {//first catch the special cases with C# shortcut escapes.
                case '\'':
                    return @"\'";
                case '"':
                    return "\\\"";
                case '\\':
                    return @"\\";
                case '\0':
                    return @"\0";
                case '\a':
                    return @"\a";
                case '\b':
                    return @"\b";
                case '\f':
                    return @"\f";
                case '\n':
                    return @"\n";
                case '\r':
                    return @"\r";
                case '\t':
                    return @"\t";
                case '\v':
                    return @"\v";
                default:
                    //we need to escape surrogates with they're single chars,
                    //but in strings we can just use the character they produce.
                    if (char.IsControl(chr) || char.IsHighSurrogate(chr) || char.IsLowSurrogate(chr))
                        return @"\u" + ((int)chr).ToString("X4");
                    else
                        return new string(chr, 1);
            }
        }
    }

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
            table.Clear(); // Zero out the memory we are going to be using. 

            // Creating the Binary Tree. 
            var tree = BinaryTree<short>.Create(State.DecodingTable);

            int dictSize = symbolCodeList.Count;
            if (dictSize >= short.MaxValue)
                throw new NotSupportedException($"We do not support dictionaries with more items than {short.MaxValue - 1}");

            // We haven't stored it yet. So we are calculating it. 
            int numberOfEntries = (State.EncodingTable.Length - 4) / sizeof(Interval3Gram);
            if (numberOfEntries < dictSize)
                throw new ArgumentException("Not enough memory to store the dictionary");

            for (int i = 0; i < dictSize; i++)
            {
                var symbol = symbolCodeList[i];
                int symbolLength = symbol.Length;

                ref var entry = ref table[i];

                // We update the size first to avoid getting a zero size start key.
                entry.KeyLength = (byte)symbolLength;
                var startKey = entry.StartKey;
                
                for (int j = 0; j < 3; j++)
                {
                    if (j < symbolLength)
                        startKey[j] = symbol.StartKey[j];
                    else
                        startKey[j] = 0;
                }

                if (i < dictSize - 1)
                {
                    entry.PrefixLength = 0;

                    // We want a local copy of it. 
                    var nextSymbol = symbolCodeList[i + 1];
                    int nextSymbolLength = nextSymbol.Length;

                    nextSymbol.StartKey[nextSymbolLength - 1] -= 1;
                    int j = 0;
                    while (j < symbolLength && j < nextSymbolLength && symbol.StartKey[j] == nextSymbol.StartKey[j])
                    {
                        entry.PrefixLength++;
                        j++;
                    }
                }
                else
                {
                    entry.PrefixLength = (byte)symbolLength;
                }

                Debug.Assert(entry.PrefixLength > 0);

                entry.Code = symbolCodeList[i].Code;

                int codeValue = BinaryPrimitives.ReverseEndianness(entry.Code.Value << (sizeof(int) * 8 - entry.Code.Length));
                var codeValueSpan = MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref codeValue, 1));
                var reader = new BitReader(codeValueSpan, entry.Code.Length);

                //string aux = Encoding.ASCII.GetString(entry.StartKey.Slice(0, entry.PrefixLength));
                //Console.Write($"[{entry.PrefixLength},{Encoding.ASCII.GetString(entry.StartKey.Slice(0, entry.KeyLength)).EscapeForCSharp()}] ");

                tree.Add(ref reader, (short)i);
            }

            _numberOfEntries[0] = dictSize;
         }

        private static int CompareDictionaryEntry(in ReadOnlySpan<byte> s1, ReadOnlySpan<byte> s2)
        {
            for (int i = 0; i < 3; i++)
            {
                if (i >= s1.Length)
                {
                    if (s2[i] == 0)
                        return 0;
                    return -1;
                }

                if (s1[i] < s2[i]) return -1;
                if (s1[i] > s2[i]) return 1;
            }

            if (s1.Length > 3)
                return 1;
            return 0;
        }

        public int Lookup(in ReadOnlySpan<byte> symbol, out Code code)
        {
            var table = EncodingTable;

            int l = 0;
            int r = _numberOfEntries[0];
            while (r - l > 1)
            {
                int m = (l + r) >> 1;

                ref var entry = ref table[m];
                int cmp = CompareDictionaryEntry(symbol, entry.StartKey);

                //var comparisonString = cmp < 0 ? "<" : (cmp > 0 ? ">" : "=");
                //Console.WriteLine($"{Encoding.ASCII.GetString(symbol).EscapeForCSharp()} {comparisonString} {Encoding.ASCII.GetString(entry.StartKey).EscapeForCSharp()} [{m}]");

                if (cmp < 0)
                {
                    r = m;
                }
                else if (cmp == 0)
                {
                    l = m;
                    break;
                }
                else
                {
                    l = m;
                }
            }

            //Console.WriteLine($"{Encoding.ASCII.GetString(symbol).EscapeForCSharp()} -> {Encoding.ASCII.GetString(table[l].StartKey.Slice(0, table[l].KeyLength)).EscapeForCSharp()} [{l}]");

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

                //Console.Write($",bits={reader.Length - localReader.Length}");

                return reader.Length - localReader.Length;
            }

            symbol = ReadOnlySpan<byte>.Empty;
            return -1;
        }
    }
}
