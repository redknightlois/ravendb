﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Voron;

namespace Corax.Queries
{
    unsafe partial struct UnaryMatch<TInner, TValueType>
    {
        [SkipLocalsInit]
        private static int FillFuncBetweenSequence(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
        {
            GreaterThanOrEqualMatchComparer greaterThan = default;
            LessThanOrEqualMatchComparer lessThan = default;

            var currentType1 = ((Slice)(object)match._value).AsReadOnlySpan();
            var currentType2 = ((Slice)(object)match._valueAux).AsReadOnlySpan();

            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;

            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            while (currentMatches.Length > maxUnusedMatchesSlots)
            {
                var results = match._inner.Fill(currentMatches);
                if (results == 0)
                    return totalResults;

                int currentIdx = 0;
                int storeIdx = 0;
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetReaderFor(currentMatches[i]);
                    var read = reader.Read(match._fieldId, out var resultX);
                    if (read && greaterThan.Compare(currentType1, resultX) && lessThan.Compare(currentType2, resultX))
                    {
                        // We found a match.
                        currentMatches[currentIdx] = currentMatches[storeIdx];
                        storeIdx++;
                    }
                    currentIdx++;
                }

                totalResults += storeIdx;
                if (totalResults > match._take)
                    break;

                currentMatches = currentMatches.Slice(storeIdx);
            }

            return totalResults;
        }

        [SkipLocalsInit]
        private static int FillFuncBetweenNumerical(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
        {
            GreaterThanOrEqualMatchComparer greaterThan = default;
            LessThanOrEqualMatchComparer lessThan = default;

            var currentType1 = match._value;
            var currentType2 = match._valueAux;

            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;

            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            while (currentMatches.Length > maxUnusedMatchesSlots)
            {
                var results = match._inner.Fill(currentMatches);
                if (results == 0)
                    return totalResults;

                int storeIdx = 0;
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetReaderFor(currentMatches[i]);

                    bool isMatch = false;
                    if (typeof(TValueType) == typeof(long))
                    {
                        var read = reader.Read<long>(match._fieldId, out var rx);
                        if (read)
                            isMatch = greaterThan.Compare((long)(object)currentType1, rx) && lessThan.Compare((long)(object)currentType2, rx);
                    }
                    else if (typeof(TValueType) == typeof(double))
                    {
                        var read = reader.Read<double>(match._fieldId, out var rx);
                        if (read)
                            isMatch = greaterThan.Compare((double)(object)currentType1, rx) && lessThan.Compare((double)(object)currentType2, rx);
                    }

                    if (isMatch)
                    {
                        // We found a match.
                        matches[storeIdx] = currentMatches[i];
                        storeIdx++;
                    }
                }

                totalResults += storeIdx;
                if (totalResults >= match._take)
                    break;

            }

            return totalResults;
        }

        public static UnaryMatch<TInner, TValueType> YieldBetweenMatch(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value1, TValueType value2, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                var vs1 = ((Slice)(object)value1).AsReadOnlySpan();
                var vs2 = ((Slice)(object)value2).AsReadOnlySpan();
                if (vs1.SequenceCompareTo(vs2) > 0)
                {
                    var aux = value1;
                    value2 = value1;
                    value1 = aux;
                }

                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.Between, 
                    searcher, fieldId, value1, value2, 
                    &FillFuncBetweenSequence, &AndWith, 
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
            else if (typeof(TValueType) == typeof(long))
            {
                var vs1 = (long)(object)value1;
                var vs2 = (long)(object)value2;
                if (vs1 > vs2)
                {
                    var aux = value1;
                    value2 = value1;
                    value1 = aux;
                }

                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.Between, 
                    searcher, fieldId, value1, value2, 
                    &FillFuncBetweenNumerical, &AndWith, 
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
            else
            {
                var vs1 = (double)(object)value1;
                var vs2 = (double)(object)value2;
                if (vs1 > vs2)
                {
                    var aux = value1;
                    value2 = value1;
                    value1 = aux;
                }
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotBetween, 
                    searcher, fieldId, value1, value2, 
                    &FillFuncBetweenNumerical, &AndWith, 
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
        }

        [SkipLocalsInit]
        private static int FillFuncNotBetweenSequence(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
        {
            GreaterThanOrEqualMatchComparer greaterThan = default;
            LessThanOrEqualMatchComparer lessThan = default;

            var currentType1 = ((Slice)(object)match._value).AsReadOnlySpan();
            var currentType2 = ((Slice)(object)match._valueAux).AsReadOnlySpan();

            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;

            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            while (currentMatches.Length > maxUnusedMatchesSlots)
            {
                var results = match._inner.Fill(currentMatches);
                if (results == 0)
                    return totalResults;

                int currentIdx = 0;
                int storeIdx = 0;
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetReaderFor(currentMatches[i]);
                    var read = reader.Read(match._fieldId, out var resultX);
                    if ((read && greaterThan.Compare(currentType1, resultX) && lessThan.Compare(currentType2, resultX)) == false)
                    {
                        // We found a match.
                        currentMatches[currentIdx] = currentMatches[storeIdx];
                        storeIdx++;
                    }
                    currentIdx++;
                }

                totalResults += storeIdx;
                if (totalResults >= match._take)
                    break;

                currentMatches = currentMatches.Slice(storeIdx);
            }

            return totalResults;
        }

        [SkipLocalsInit]
        private static int FillFuncNotBetweenNumerical(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
        {
            GreaterThanOrEqualMatchComparer greaterThan = default;
            LessThanOrEqualMatchComparer lessThan = default;

            var currentType1 = match._value;
            var currentType2 = match._valueAux;

            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;

            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            while (currentMatches.Length > maxUnusedMatchesSlots)
            {
                var results = match._inner.Fill(currentMatches);
                if (results == 0)
                    return totalResults;

                int storeIdx = 0;
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetReaderFor(currentMatches[i]);

                    bool isMatch = false;
                    if (typeof(TValueType) == typeof(long))
                    {
                        var read = reader.Read<long>(match._fieldId, out var rx);
                        if (read)
                            isMatch = greaterThan.Compare((long)(object)currentType1, rx) && lessThan.Compare((long)(object)currentType2, rx);
                    }
                    else if (typeof(TValueType) == typeof(double))
                    {
                        var read = reader.Read<double>(match._fieldId, out var rx);
                        if (read)
                            isMatch = greaterThan.Compare((double)(object)currentType1, rx) && lessThan.Compare((double)(object)currentType2, rx);
                    }

                    if (isMatch == false)
                    {
                        // We found a match.
                        matches[storeIdx] = currentMatches[i];
                        storeIdx++;
                    }
                }

                totalResults += storeIdx;
                if (totalResults >= match._take)
                    break;                    

            }

            matches = currentMatches;
            return totalResults;
        }

        public static UnaryMatch<TInner, TValueType> YieldNotBetweenMatch(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value1, TValueType value2, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                var vs1 = ((Slice)(object)value1).AsReadOnlySpan();
                var vs2 = ((Slice)(object)value2).AsReadOnlySpan();
                if (vs1.SequenceCompareTo(vs2) > 0)
                {
                    var aux = value1;
                    value2 = value1;
                    value1 = aux;
                }

                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotBetween, 
                    searcher, fieldId, value1, value2, 
                    &FillFuncNotBetweenSequence, &AndWith, 
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
            else if (typeof(TValueType) == typeof(long))
            {
                var vs1 = (long)(object)value1;
                var vs2 = (long)(object)value2;
                if (vs1 > vs2)
                {
                    var aux = value1;
                    value2 = value1;
                    value1 = aux;
                }

                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotBetween, 
                    searcher, fieldId, value1, value2, 
                    &FillFuncNotBetweenNumerical, &AndWith, 
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
            else
            {
                var vs1 = (double)(object)value1;
                var vs2 = (double)(object)value2;
                if (vs1 > vs2)
                {
                    var aux = value1;
                    value2 = value1;
                    value1 = aux;
                }
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotBetween, 
                    searcher, fieldId, value1, value2, 
                    &FillFuncNotBetweenNumerical, &AndWith, 
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
        }
    }
}
