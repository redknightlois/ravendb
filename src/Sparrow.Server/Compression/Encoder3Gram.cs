using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.SymbolStore;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Server.Binary;
using Sparrow.Server.Collections.Persistent;

namespace Sparrow.Server.Compression
{
    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct Interval3Gram
    {
        [FieldOffset(0)]
        public fixed byte KeyBuffer[3];
        [FieldOffset(3)]
        public byte _prefixAndKeyLength;
        [FieldOffset(4)]
        public Code Code;

        public byte PrefixLength
        {
            get { return (byte)(_prefixAndKeyLength & 0x0F); }
            set { _prefixAndKeyLength = (byte)(_prefixAndKeyLength & 0xF0 | value & 0x0F); }
        }

        public byte KeyLength
        {
            get { return (byte)(_prefixAndKeyLength >> 4); }
            set { _prefixAndKeyLength = (byte)(_prefixAndKeyLength & 0x0F | (value << 4)); }
        }
    }

    public unsafe struct Encoder3Gram<TEncoderState> : IEncoderAlgorithm
        where TEncoderState : struct, IEncoderState
    {
        private readonly TEncoderState _state;
        private int _entries;
        private fixed int _buffer[2];

        public Encoder3Gram(TEncoderState state)
        {
            _state = state;
            _entries = 0; // Zero means not trained.

            _entries = _numberOfEntries[0];
        }

        public static int GetDictionarySize(in TEncoderState state)
        {
            int entries = MemoryMarshal.Cast<byte, int>(state.EncodingTable.Slice(0, 4))[0];
            return ( entries * Unsafe.SizeOf<Interval3Gram>() + 8 ) * 2; // The encoding and the decoding table size.
        }

        public static int GetEntriesTableSize(in TEncoderState state)
        {
            return MemoryMarshal.Cast<byte, int>(state.EncodingTable.Slice(0, 4))[0];
        }

        public void Train<TSampleEnumerator>(in TSampleEnumerator enumerator, int dictionarySize)
            where TSampleEnumerator : struct, IReadOnlySpanEnumerator
        {
            var symbolSelector = new Encoder3GramSymbolSelector<TSampleEnumerator>();
            var frequencyList = symbolSelector.SelectSymbols(enumerator, dictionarySize);

            var codeAssigner = new HuTuckerCodeAssigner();            
            var symbolCodes = codeAssigner.AssignCodes(frequencyList);

            BuildDictionary(symbolCodes);
        }

        private string ToBinaryString(short sBuf, int sLen)
        {
            Span<short> value = stackalloc short[1];
            value[0] = BinaryPrimitives.ReverseEndianness((short)(sBuf << (sizeof(short) * 8 - sLen)));
            BitReader reader = new(MemoryMarshal.Cast<short, byte>(value), sLen);

            string result = string.Empty;
            while (reader.Length != 0)
            {
                if (reader.Read().IsSet)
                    result += "R";
                else
                    result += "L";
            }
            return result;
        }

        public void EncodeBatch<TSampleEnumerator, TOutputEnumerator>(in TSampleEnumerator data, Span<int> outputSizes, in TOutputEnumerator outputBuffers)
            where TSampleEnumerator : struct, IReadOnlySpanIndexer
            where TOutputEnumerator : struct, ISpanIndexer
        {
            var table = EncodingTable;
            var numberOfEntries = _entries;

            int length = data.Length;
            for (int i = 0; i < length; i++)
            {
                var intBuf = MemoryMarshal.Cast<byte, long>(outputBuffers[i]);

                int idx = 0;
                intBuf[0] = 0;
                int intBufLen = 0;

                var keyStr = data[i];
                int pos = 0;
                while (pos < keyStr.Length)
                {
                    int prefixLen = Lookup(keyStr.Slice(pos), table, numberOfEntries, out Code code);
                    long sBuf = code.Value;
                    int sLen = code.Length;
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

                intBuf[idx] <<= (64 - intBufLen);
                intBuf[idx] = BinaryPrimitives.ReverseEndianness(intBuf[idx]);
                outputSizes[i] = ((idx << 6) + intBufLen);
            }
        }

        public unsafe void DecodeBatch<TSampleEnumerator, TOutputEnumerator>(in TSampleEnumerator data, Span<int> outputSize, in TOutputEnumerator outputBuffers)
            where TSampleEnumerator : struct, IReadOnlySpanIndexer
            where TOutputEnumerator : struct, ISpanIndexer
        {
            fixed (Interval3Gram* table = EncodingTable)
            {
                var tree = BinaryTree<short>.Open(_state.DecodingTable);

                for (int i = 0; i < data.Length; i++)
                {
                    Span<byte> buffer =  outputBuffers[i];
                    var reader = new BitReader(data[i]);
                    int bits = reader.Length;
                    var endsWithNull = false; 
                    while (bits > 0 && endsWithNull == false)
                    {
                        int length = Lookup(reader, ref buffer, table, tree, out endsWithNull);
                        if (length < 0)
                            throw new IOException("Invalid data stream.");
                        // Advance the reader.
                        reader.Skip(length);
                        bits -= length;
                    }

                    outputSize[i] = buffer.Length - buffer.Length;
                }
            }
        }

        public unsafe void DecodeBatch<TSampleEnumerator, TOutputEnumerator>(ReadOnlySpan<int> dataBits, in TSampleEnumerator data, Span<int> outputSize, in TOutputEnumerator outputBuffers)
            where TSampleEnumerator : struct, IReadOnlySpanIndexer
            where TOutputEnumerator : struct, ISpanIndexer
        {
            fixed (Interval3Gram* table = EncodingTable)
            {
                var tree = BinaryTree<short>.Open(_state.DecodingTable);

                for (int i = 0; i < data.Length; i++)
                {
                    Span<byte> buffer = default;
                    var reader = new BitReader(data[i]);
                    int bits = dataBits[i];
                    var endsWithNull = false;
                    while (bits > 0 && endsWithNull == false)
                    {
                        buffer = outputBuffers[i];
                        int length = Lookup(reader, ref buffer, table, tree, out endsWithNull);
                        if (length < 0)
                            throw new IOException("Invalid data stream.");

                        // Advance the reader.
                        reader.Skip(length);

                        bits -= length;
                    }

                    outputSize[i] = buffer.Length - buffer.Length;
                }
            }
        }

        public int Encode(ReadOnlySpan<byte> key, Span<byte> outputBuffer)
        {
            Debug.Assert(outputBuffer.Length >= sizeof(long)); // Ensure we can safely cast to int 64

            var intBuf = MemoryMarshal.Cast<byte, long>(outputBuffer);

            int idx = 0;
            intBuf[0] = 0;
            int intBufLen = 0;

            var table = EncodingTable;
            var numberOfEntries = _entries;

            var symbol = key;
            while (symbol.Length != 0)
            {
                if (symbol[0] == 0 && symbol.Length > 1)
                    throw new InvalidDataException("The key cannot contain null bytes unless it is the last value.");

                int prefixLen = Lookup(symbol, table, numberOfEntries, out Code code);
                long sBuf = code.Value;
                int sLen = code.Length;
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

                symbol = symbol.Slice(prefixLen);
            }

            intBuf[idx] <<= (64 - intBufLen);
            intBuf[idx] = BinaryPrimitives.ReverseEndianness(intBuf[idx]);
            return ((idx << 6) + intBufLen);
        }

        public unsafe int DecodeStochasticBug(ReadOnlySpan<byte> data, Span<byte> outputBuffer)
        {
            Span<byte> buffer = outputBuffer;
            fixed (Interval3Gram* table = EncodingTable)
            {
                var tree = BinaryTree<short>.Open(_state.DecodingTable);

                var reader = new BitReader(data);
                var endsWithNull = false;
                while (reader.Length > 0 && endsWithNull == false)
                {
                    int length = Lookup(reader, ref buffer, table, tree, out endsWithNull);
                    if (length < 0)
                        throw new IOException("Invalid data stream.");

                    // Advance the reader.
                    reader.Skip(length);
                }

                return outputBuffer.Length - buffer.Length;
            }
        }

        public unsafe int Decode(ReadOnlySpan<byte> data, Span<byte> outputBuffer)
        {
            Span<byte> buffer = outputBuffer;
            fixed (Interval3Gram* table = EncodingTable)
            {
                var tree = BinaryTree<short>.Open(_state.DecodingTable);
                var reader = new BitReader(data);
                var endsWithNull = false;
                while (reader.Length > 0 && endsWithNull == false)
                {
                    int length = Lookup(reader, ref buffer, table, tree, out endsWithNull);
                    if (length < 0)
                        throw new IOException("Invalid data stream.");

                    // Advance the reader.
                    reader.Skip(length);
                }

                return outputBuffer.Length - buffer.Length;
            }
        }

        public unsafe int Decode(int bits, ReadOnlySpan<byte> data, Span<byte> outputBuffer)
        {
            fixed (Interval3Gram* table = EncodingTable)
            {
                var tree = BinaryTree<short>.Open(_state.DecodingTable);
                var buffer = outputBuffer;
                var reader = new BitReader(data);
                var endsWithNull = false;
                while (bits > 0 && endsWithNull == false)
                {
                    int length = Lookup(reader, ref buffer, table, tree, out endsWithNull);
                    if (length < 0)
                        throw new IOException("Invalid data stream.");

                    // Advance the reader.
                    reader.Skip(length);

                    bits -= length;
                }

                return outputBuffer.Length - buffer.Length;
            }
        }

        public int NumberOfEntries => _numberOfEntries[0];
        public int MemoryUse => _numberOfEntries[0] * Unsafe.SizeOf<Interval3Gram>();


        private Span<int> _numberOfEntries => MemoryMarshal.Cast<byte, int>(_state.EncodingTable.Slice(0, 4));

        public int MaxBitSequenceLength
        {
            get { return MemoryMarshal.Read<byte>(_state.EncodingTable.Slice(4, 1)); }
            set
            {
                byte valueAsByte = (byte)value;
                MemoryMarshal.Write(_state.EncodingTable.Slice(4, 1), ref valueAsByte);
            }
        }

        public int MinBitSequenceLength
        {
            get { return MemoryMarshal.Read<byte>(_state.EncodingTable.Slice(5, 1)); }
            set
            {
                byte valueAsByte = (byte)value;
                MemoryMarshal.Write(_state.EncodingTable.Slice(5, 1), ref valueAsByte);
            }
        }

        private Span<Interval3Gram> EncodingTable => MemoryMarshal.Cast<byte, Interval3Gram>(_state.EncodingTable.Slice(8));

        private unsafe void BuildDictionary(in FastList<SymbolCode> symbolCodeList)
        {
            EncodingTable.Clear(); // Zero out the memory we are going to be using. 

            // Creating the Binary Tree. 
            var tree = BinaryTree<short>.Create(_state.DecodingTable);

            int dictSize = symbolCodeList.Count;
            if (dictSize >= short.MaxValue)
                throw new NotSupportedException($"We do not support dictionaries with more items than {short.MaxValue - 1}");

            // We haven't stored it yet. So we are calculating it. 
            int numberOfEntriesInTable = (_state.EncodingTable.Length - 4) / Unsafe.SizeOf<Interval3Gram>();
            if (numberOfEntriesInTable < dictSize)
            {
                if (_state.CanGrow)
                    _state.Grow(dictSize);
                else
                    throw new InsufficientMemoryException("Not enough memory for the table and the table supplied does not support growing.");
            }                

            int maxBitSequenceLength = 1;
            int minBitSequenceLength = int.MaxValue;

            for (int i = 0; i < dictSize; i++)
            {
                var symbol = symbolCodeList[i];
                int symbolLength = symbol.Length;

                ref var entry = ref EncodingTable[i];

                // We update the size first to avoid getting a zero size start key.
                entry.KeyLength = (byte)symbolLength;
                for (int j = 0; j < 3; j++)
                {
                    if (j < symbolLength)
                        entry.KeyBuffer[j] = symbol.StartKey[j];
                    else
                        entry.KeyBuffer[j] = 0;
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

                uint codeValue = (uint)entry.Code.Value << (sizeof(int) * 8 - entry.Code.Length);
                
                maxBitSequenceLength = Math.Max(maxBitSequenceLength, entry.Code.Length);
                minBitSequenceLength = Math.Min(minBitSequenceLength, entry.Code.Length);

                var reader = new TypedBitReader<uint>(codeValue, entry.Code.Length);
                tree.Add(ref reader, (short)i);
            }

            _numberOfEntries[0] = dictSize;
            _entries = dictSize;
            MaxBitSequenceLength = maxBitSequenceLength;
            MinBitSequenceLength = minBitSequenceLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Lookup(ReadOnlySpan<byte> symbol, ReadOnlySpan<Interval3Gram> table, int numberOfEntries, out Code code)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int CompareDictionaryEntry(byte* s1, int size, byte* s2)
            {
                // PERF: This is probably the oldest trick in the book. When 2 pointers are going to be accessed in the
                // same pattern. We can avoid incrementing them by referencing the second pointer as an offset from the 
                // first pointer. Given this causes less registers writes we can iterate faster avoiding data hazards at
                // the CPU pipeline level. Furthermore, the access are done through relative addressing and 'lea' operations
                // which are faster as most CPUs are able to compute those addresses directly at the front-end of the
                // processor. 
                long offset = s2 - s1;
                byte* s1End = s1 + size;
                byte* s1LoopEnd = s1 + 3;
                while(s1 < s1LoopEnd)
                {
                    if (s1 >= s1End)
                    {
                        //s2[i] == 0 ? 0 : 1
                        return -(*(s1 + offset) != 0).ToInt32();
                    }

                    if (*s1 != *(s1 + offset))
                    {
                        // s1[i] < s2[i] ? -1 : s1[i] > s2[i] ? 1 : <can't happen>
                        return -(*s1 < *(s1 + offset)).ToInt32() + (*s1 > *(s1 + offset)).ToInt32();
                    }

                    s1++;
                }

                // s1.Length > 3 ? 1 : 0
                return (size > 3).ToInt32();
            }

            // PERF: We create aliases l and r to make the algorithm more readable. At the assembler level,
            // the mov operations work exactly the same as a value assignment.
            ref int l = ref _buffer[0];
            ref int r = ref _buffer[1];

            fixed (byte* symbolPtr = &MemoryMarshal.GetReference(symbol))
            {
                l = 0;
                r = numberOfEntries;
                while (r - l > 1)
                {
                    int m = (l + r) >> 1;

                    int cmp;
                    fixed (byte* p = table[m].KeyBuffer)
                    {
                        cmp = CompareDictionaryEntry(symbolPtr, symbol.Length, p);
                    }

                    // PERF: since we want to avoid unpredictable branches as much as possible but we need
                    // to track both r and l values. What we do is store that in sequence on a buffer and
                    // then perform a memory access through converting the binary value of the 'Cmp < 0'
                    // operation into an index without using a branching instruction. 

                    // Cmp < 0 = 1 -> value[1] = r
                    // This is the branchless statement: cmp < 0 ? r = m : l = m
                    _buffer[(cmp < 0).ToInt32()] = m;
                    if (cmp == 0)
                        break;
                }
            }
            
            code = table[l].Code;
            return table[l].PrefixLength;
        }

        private int Lookup(in BitReader reader, ref Span<byte> symbol, Interval3Gram* table, in BinaryTree<short> tree, out bool endsWithNull)
        {
            BitReader localReader = reader;
            if (tree.FindCommonPrefix(ref localReader, out var idx))
            {
                var p = table + idx;
                Span<byte> term = new(p->KeyBuffer, p->PrefixLength);
                term.CopyTo(symbol);
                symbol = symbol[p->PrefixLength..];

                endsWithNull = term[^1] == 0;

                return reader.Length - localReader.Length;
            }

            endsWithNull = false;
            symbol = Span<byte>.Empty;
            return -1;
        }

        public void Dispose()
        {
            _state.Dispose();
        }
    }
}
