﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
    public unsafe partial struct SortingMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal SortingMatch(IQueryMatch match, FunctionTable functionTable)
        {
            _inner = match;
            _functionTable = functionTable;
        }

        public long Count => _functionTable.CountFunc(ref this);

        public QueryCountConfidence Confidence => _inner.Confidence;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            return _functionTable.FillFunc(ref this, buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer)
        {
            throw new NotSupportedException($"{nameof(SortingMatch)} does not support the operation of {nameof(AndWith)}.");
        }

        internal class FunctionTable
        {
            public readonly delegate*<ref SortingMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref SortingMatch, long> CountFunc;

            public FunctionTable(
                delegate*<ref SortingMatch, Span<long>, int> fillFunc,
                delegate*<ref SortingMatch, long> countFunc)
            {
                FillFunc = fillFunc;
                CountFunc = countFunc;
            }
        }

        private static class StaticFunctionCache<TInner, TComparer>
            where TInner : IQueryMatch
            where TComparer : struct, IComparer<long>
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref SortingMatch match)
                {
                    return ((SortingMatch<TInner, TComparer>)match._inner).Count;
                }
                static int FillFunc(ref SortingMatch match, Span<long> matches)
                {
                    if (match._inner is SortingMatch<TInner, TComparer> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                FunctionTable = new FunctionTable(&FillFunc, &CountFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SortingMatch Create<TInner, TComparer>(in SortingMatch<TInner, TComparer> query)
            where TInner : IQueryMatch
            where TComparer : struct, IComparer<long>
        {
            return new SortingMatch(query, StaticFunctionCache<TInner, TComparer>.FunctionTable);
        }
    }
}
