using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow.Collections;

namespace Sparrow.Server.Compression
{
    internal unsafe class Encoder3GramSymbolSelector<TSampleEnumerator>
        where TSampleEnumerator : struct, IReadOnlySpanEnumerator
    {

        private struct ByFrequencyComparer : IComparer<SymbolFrequency>
        {
            public int Compare(SymbolFrequency x, SymbolFrequency y)
            {
                if (y.Frequency != x.Frequency)
                    return y.Frequency - x.Frequency;

                return y.StartKey.SequenceCompareTo(x.StartKey);
            }
        }

        private struct LexicographicComparer : IComparer<SymbolFrequency>
        {
            public int Compare(SymbolFrequency x, SymbolFrequency y)
            {
                return x.StartKey.SequenceCompareTo(y.StartKey);
            }
        }

        private readonly FastList<int> _intervalFrequencies = new(32);
        private readonly FastList<Symbol> _intervalPrefixes = new(32);
        private readonly FastList<Symbol> _intervalBoundaries = new(32);
        private readonly FastList<SymbolFrequency> _mostFrequentSymbols = new (32);

        public FastList<SymbolFrequency> SelectSymbols(TSampleEnumerator keys, int dictionarySize, FastList<SymbolFrequency> symbolFrequenciesList = null)
        {
            if ( symbolFrequenciesList == null )
                symbolFrequenciesList = new FastList<SymbolFrequency>();
            else
                symbolFrequenciesList.Clear();

            if (keys.Length == 0)
                return symbolFrequenciesList;

            CountSymbolFrequency(keys);

            int adjustedDictionarySize = dictionarySize;
            if (dictionarySize > _frequencyMap.Count() * 2)
            {
                // 3 Gram: Input dictionary Size is too big, changing the size
                adjustedDictionarySize = (_frequencyMap.Count() * 2 - 1) / 2;
            }

            PickMostFreqSymbols(adjustedDictionarySize);

            FillInGap();
            Debug.Assert(_intervalPrefixes.Count == _intervalBoundaries.Count);
            
            CountIntervalFreq(keys);
            Debug.Assert(_intervalPrefixes.Count == _intervalFrequencies.Count);
            for (int i = 0; i < (int)_intervalBoundaries.Count; i++)
            {
                symbolFrequenciesList.Add( new SymbolFrequency(_intervalBoundaries[i].StartKey, _intervalFrequencies[i]));
            }

            return symbolFrequenciesList;
        }

        private const int GramSize = 3;
        private readonly Dictionary<int, int> _frequencyMap = new (16);

        private void CountIntervalFreq(TSampleEnumerator keys)
        {
            _intervalFrequencies.Clear();
            for (int i = 0; i < _intervalPrefixes.Count(); i++)
                _intervalFrequencies.Add(1);

            for (int i = 0; i < keys.Length; i++)
            {
                int pos = 0;
                var key = keys[i];
                while (pos < key.Length)
                {
                    int idx = BinarySearch(key.Slice(pos, Math.Min(4, key.Length - pos)));
                    _intervalFrequencies[idx]++;
                    pos += _intervalPrefixes[idx].Length;
                }
            }
        }

        private int BinarySearch(ReadOnlySpan<byte> key)
        {
            int l = 0;
            int r = _intervalBoundaries.Count;
            while (r - l > 1)
            {
                int m = (l + r) >> 1;
                int cmp = key.SequenceCompareTo(_intervalBoundaries[m].StartKey);
                if (cmp < 0)
                {
                    r = m;
                }
                else if (cmp == 0)
                {
                    return m;
                }
                else
                {
                    l = m;
                }
            }
            return l;
        }

        private void FillInGap()
        {
            var localMostFrequentSymbols = _mostFrequentSymbols;
            var intervalPrefixes = _intervalPrefixes;
            var intervalBoundaries = _intervalBoundaries;

            intervalPrefixes.Clear();
            intervalBoundaries.Clear();

            // Include prefixes and boundaries for every case until we hit the most frequent start key first character. 
            FillInSingleChar(0, localMostFrequentSymbols[0].StartKey[0]);

            Span<byte> localAuxiliaryKey = stackalloc byte[GramSize];

            for (int i = 0; i < localMostFrequentSymbols.Count - 1; i++)
            {
                var key1 = new Symbol(localMostFrequentSymbols[i].StartKey);
                var key2 = new Symbol(localMostFrequentSymbols[i + 1].StartKey);

                intervalPrefixes.Add(key1);
                intervalBoundaries.Add(key1);

                var key1RightBound = key1;
                for (int j = 3 - 1; j >= 0; j--)
                {
                    if (key1RightBound.StartKey[j] < 255)
                    {
                        key1RightBound.StartKey[j] += 1;
                        key1RightBound = new Symbol(key1RightBound.StartKey.Slice(0, j + 1));
                        break;
                    }
                }

                if (key1RightBound.StartKey.SequenceCompareTo(key2.StartKey) != 0)
                {
                    intervalBoundaries.Add(new Symbol(key1RightBound.StartKey));
                    if (key1RightBound.StartKey[0] != key2.StartKey[0])
                    {
                        intervalPrefixes.Add(new Symbol(key1.StartKey.Slice(0, 1)));
                        FillInSingleChar(key1.StartKey[0] + 1, key2.StartKey[0]);
                    }
                    else
                    {
                        int length;
                        if (key1.StartKey[0] != key1RightBound.StartKey[0])
                        {
                            localAuxiliaryKey = key1RightBound.StartKey;
                            length = localAuxiliaryKey.Length;
                        }
                        else
                        {
                            length = CommonPrefix(localAuxiliaryKey, key1, key2);
                        }

                        Debug.Assert(length > 0);
                        intervalPrefixes.Add(new Symbol(localAuxiliaryKey.Slice(0, length)));
                    }
                }
            }

            var lastKey = new Symbol(localMostFrequentSymbols[^1].StartKey);
            intervalPrefixes.Add(lastKey);
            intervalBoundaries.Add(lastKey);

            var lastKeyRightBound = lastKey;
            for (int j = 3 - 1; j >= 0; j--)
            {
                if (lastKeyRightBound.StartKey[j] < 255)
                {
                    lastKeyRightBound.StartKey[j] += 1;
                    lastKeyRightBound = new Symbol(lastKeyRightBound.StartKey.Slice(0, j + 1));
                    break;
                }
            }

            intervalBoundaries.Add(new Symbol(lastKeyRightBound.StartKey));
            intervalPrefixes.Add(new Symbol(lastKeyRightBound.StartKey.Slice(0, 1)));

            if (lastKey.StartKey[0] < 255)
                FillInSingleChar(lastKey.StartKey[0] + 1, 255);
        }

        private int CommonPrefix(Span<byte> commonStr, Symbol s1, Symbol s2)
        {
            var str1 = s1.StartKey;
            var str2 = s2.StartKey;

            if (str1[0] != str2[0])
                return 0; 

            for (int i = 1; i < GramSize; i++)
            {
                if (str1.Length < i)
                {
                    str1.Slice(0, i).CopyTo(commonStr);
                    return i;
                }

                if (str2.Length < i)
                {
                    str2.Slice(0, i).CopyTo(commonStr);
                    return i;
                }

                if (str1[i] != str2[i])
                {
                    str1.Slice(0, i).CopyTo(commonStr);
                    return i;
                }
            }

            throw new ArgumentException();
        }

        private void FillInSingleChar(int start, int last)
        {
            Span<byte> key = stackalloc byte[1];
            for (int c = start; c <= last; c++)
            {
                // We update the local key to be pushed to the prefixes and boundaries.
                key[0] = (byte)c;

                // Create a symbol out of the single character and push it into the prefixes and boundaries list.
                var singleCharacterSymbol = new Symbol(key);
                _intervalPrefixes.Add(singleCharacterSymbol);
                _intervalBoundaries.Add(singleCharacterSymbol);
            }
        }

        private void PickMostFreqSymbols(int dictionarySize)
        {
            Debug.Assert(GramSize == 3);

            // PERF: Local reference to avoid having to access the field every time.
            var mostFrequentSymbols = _mostFrequentSymbols;

            mostFrequentSymbols.Clear();

            Span<byte> key = stackalloc byte[4];
            Span<byte> keySlice = key.Slice(0, 3);
            foreach (var tuple in _frequencyMap)
            {
                key[0] = (byte)(tuple.Key >> 16);
                key[1] = (byte)(tuple.Key >> 8);
                key[2] = (byte)(tuple.Key);

                mostFrequentSymbols.Add(new SymbolFrequency(keySlice, tuple.Value));
            }

            Sorter<SymbolFrequency, ByFrequencyComparer> sortByFrequency;
            mostFrequentSymbols.Sort(ref sortByFrequency);

            if (mostFrequentSymbols.Count > dictionarySize)
                mostFrequentSymbols.Trim(dictionarySize);

            Sorter<SymbolFrequency, LexicographicComparer> sortLexicographical;
            mostFrequentSymbols.Sort(ref sortLexicographical);
        }

        private void CountSymbolFrequency(TSampleEnumerator keys)
        {
            Debug.Assert(GramSize <= 3);

            // PERF: Local reference to avoid having to access the field every time.
            var frequencyMap = _frequencyMap;

            frequencyMap.Clear();

            for (int i = 0; i < keys.Length; i++)
            {
                var key = keys[i];
                for (int j = 0; j < key.Length - GramSize + 1; j++)
                {
                    var slice = key.Slice(j, GramSize);

                    int sliceDescriptor = slice[0] << 16 | slice[1] << 8 | slice[2];
                    if (!frequencyMap.TryGetValue(sliceDescriptor, out var frequency))
                        frequency = 0;

                    frequencyMap[sliceDescriptor] = frequency + 1;
                }
            }
        }
    }
}
