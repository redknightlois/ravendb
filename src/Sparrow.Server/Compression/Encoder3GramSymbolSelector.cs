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

        /// <summary>
        /// Comparer for comparing two keys, handling equality as being greater
        /// Use this Comparer e.g. with SortedLists or SortedDictionaries, that don't allow duplicate keys
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        private class DuplicateKeyComparer : IComparer<SymbolFrequency> 
        {
            #region IComparer<TKey> Members

            public int Compare(SymbolFrequency x, SymbolFrequency y)
            {
                if (x.Frequency != y.Frequency)
                    return x.Frequency - y.Frequency;

                int result = x.StartKey.SequenceCompareTo(y.StartKey);
                if (result == 0)
                    return 1;
                return result;
            }

            #endregion
        }

        private readonly FastList<int> interval_freqs_ = new(32);
        private readonly FastList<Symbol> interval_prefixes_ = new(32);
        private readonly FastList<Symbol> interval_boundaries_ = new(32);
        private readonly SortedList<SymbolFrequency, int> most_freq_symbols = new (32, new DuplicateKeyComparer());

        public FastList<SymbolFrequency> SelectSymbols(TSampleEnumerator keys, int num_limit, FastList<SymbolFrequency> symbol_freq_list = null)
        {
            if ( symbol_freq_list == null )
                symbol_freq_list = new FastList<SymbolFrequency>();
            else
                symbol_freq_list.Clear();

            if (keys.Length == 0)
                return symbol_freq_list;

            CountSymbolFrequence(keys);

            int adjust_num_limit = num_limit;
            if (num_limit > freq_map_.Count() * 2)
            {
                // 3 Gram: Input dictionary Size is too big, changing the size
                adjust_num_limit = freq_map_.Count() * 2 - 1;
            }

            PickMostFreqSymbols();

            FillInGap(adjust_num_limit / 2);
            Debug.Assert(interval_prefixes_.Count == interval_boundaries_.Count);
            
            CountIntervalFreq(keys);
            Debug.Assert(interval_prefixes_.Count == interval_freqs_.Count);
            for (int i = 0; i < (int)interval_boundaries_.Count; i++)
            {
                symbol_freq_list.Add( new SymbolFrequency(interval_boundaries_[i].StartKey, interval_freqs_[i]));
            }

            return symbol_freq_list;
        }

        private const int GramSize = 3;
        private readonly Dictionary<int, int> freq_map_ = new (16);

        private void CountIntervalFreq(TSampleEnumerator keys)
        {
            interval_freqs_.Clear();
            for (int i = 0; i < interval_prefixes_.Count(); i++)
                interval_freqs_.Add(1);

            for (int i = 0; i < keys.Length; i++)
            {
                int pos = 0;
                var key = keys[i];
                while (pos < key.Length)
                {
                    var cur_str = key.Slice(pos, Math.Min(3, key.Length - pos));
                    int idx = BinarySearch(cur_str);
                    interval_freqs_[idx]++;
                    pos += interval_prefixes_[idx].Length;
                }
            }
        }

        private int BinarySearch(ReadOnlySpan<byte> key)
        {
            int l = 0;
            int r = interval_boundaries_.Count;
            while (r - l > 1)
            {
                int m = (l + r) >> 1;
                var cur_key = interval_boundaries_[m].StartKey;
                int cmp = key.SequenceCompareTo(cur_key);
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

        private void FillInGap(int num_symbols)
        {
            interval_prefixes_.Clear();
            interval_boundaries_.Clear();

            var most_freq_symbols_value =  most_freq_symbols.Keys;

            // Include prefixes and boundaries for every case until we hit the most frequent start key first character. 
            FillInSingleChar(0, most_freq_symbols_value[0].StartKey[0]);

            Span<byte> common_str = stackalloc byte[GramSize];

            Debug.Assert(num_symbols <= most_freq_symbols_value.Count);
            for (int i = 0; i < num_symbols - 1; i++)
            {
                var str1 = new Symbol(most_freq_symbols_value[i].StartKey);
                var str2 = new Symbol(most_freq_symbols_value[i + 1].StartKey);

                interval_prefixes_.Add(str1);
                interval_boundaries_.Add(str1);

                if (str1.StartKey.SequenceCompareTo(str2.StartKey) != 0)
                {
                    interval_boundaries_.Add(new Symbol(str1.StartKey));
                    if (str1.StartKey[0] != str2.StartKey[0])
                    {
                        interval_prefixes_.Add(new Symbol(str1.StartKey.Slice(0, 1)));
                        FillInSingleChar(str1.StartKey[0] + 1, str2.StartKey[0]);
                    }
                    else
                    {
                        int length = CommonPrefix(common_str, str1, str2);

                        Debug.Assert(length > 0);
                        interval_prefixes_.Add(new Symbol(common_str.Slice(0, length)));
                    }
                }
            }

            var last_str = new Symbol(most_freq_symbols_value[num_symbols - 1].StartKey);
            interval_prefixes_.Add(last_str);
            interval_boundaries_.Add(last_str);

            //interval_boundaries_.Add(new Symbol(last_str.StartKey));
            //interval_prefixes_.Add(new Symbol(last_str.StartKey.Slice(0, 1)));

            FillInSingleChar(last_str.StartKey[0] + 1, 255);
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
                interval_prefixes_.Add(singleCharacterSymbol);
                interval_boundaries_.Add(singleCharacterSymbol);
            }
        }

        private void PickMostFreqSymbols()
        {
            Debug.Assert(GramSize == 3);

            most_freq_symbols.Clear();

            Span<byte> key = stackalloc byte[4];
            Span<byte> keySlice = key.Slice(0, 3);
            foreach (var tuple in freq_map_)
            {
                key[0] = (byte)(tuple.Key >> 16);
                key[1] = (byte)(tuple.Key >> 8);
                key[2] = (byte)(tuple.Key);

                most_freq_symbols.Add(new SymbolFrequency(keySlice, tuple.Value), 0);
            }
        }

        private void CountSymbolFrequence(TSampleEnumerator keys)
        {
            Debug.Assert(GramSize <= 3);

            freq_map_.Clear();

            for (int i = 0; i < keys.Length; i++)
            {
                var key = keys[i];
                for (int j = 0; j < key.Length - GramSize + 1; j++)
                {
                    var slice = key.Slice(j, GramSize);

                    int sliceDescriptor = slice[0] << 16 | slice[1] << 8 | slice[2];
                    if (!freq_map_.TryGetValue(sliceDescriptor, out var frequency))
                        frequency = 0;

                    freq_map_[sliceDescriptor] = frequency + 1;
                }
            }
        }
    }
}
