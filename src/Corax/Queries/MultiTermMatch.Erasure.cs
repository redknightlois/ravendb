using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public unsafe struct MultiTermMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal MultiTermMatch(IQueryMatch match, FunctionTable functionTable)
        {
            _inner = match;
            _functionTable = functionTable;
        }

        public long Count => _functionTable.CountFunc(ref this);

        public long Current => _functionTable.CurrentFunc(ref this);


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SeekTo(long next = 0)
        {
            return _functionTable.SeekToFunc(ref this, next);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            return _functionTable.FillFunc(ref this, buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer)
        {
            return _functionTable.AndWithFunc(ref this, buffer);
        }

        internal class FunctionTable
        {
            public readonly delegate*<ref MultiTermMatch, long, bool> SeekToFunc;
            public readonly delegate*<ref MultiTermMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref MultiTermMatch, Span<long>, int> AndWithFunc;
            public readonly delegate*<ref MultiTermMatch, long> CountFunc;
            public readonly delegate*<ref MultiTermMatch, long> CurrentFunc;

            public FunctionTable(
                delegate*<ref MultiTermMatch, long, bool> seekToFunc,
                delegate*<ref MultiTermMatch, Span<long>, int> fillFunc,
                delegate*<ref MultiTermMatch, Span<long>, int> andWithFunc,
                delegate*<ref MultiTermMatch, long> countFunc,
                delegate*<ref MultiTermMatch, long> currentFunc)
            {
                SeekToFunc = seekToFunc;
                FillFunc = fillFunc;
                AndWithFunc = andWithFunc;
                CountFunc = countFunc;
                CurrentFunc = currentFunc;
            }
        }

        private static class StaticFunctionCache<TInner>            
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref MultiTermMatch match)
                {
                    return ((MultiTermMatch<TInner>)match._inner).Count;
                }
                static long CurrentFunc(ref MultiTermMatch match)
                {
                    return ((MultiTermMatch<TInner>)match._inner).Current;
                }
                static bool SeekToFunc(ref MultiTermMatch match, long v)
                {
                    if (match._inner is MultiTermMatch<TInner> inner)
                    {
                        var result = inner.SeekTo(v);
                        match._inner = inner;
                        return result;
                    }
                    return false;
                }

                static int FillFunc(ref MultiTermMatch match, Span<long> matches)
                {
                    if (match._inner is MultiTermMatch<TInner> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                static int AndWithFunc(ref MultiTermMatch match, Span<long> matches)
                {
                    if (match._inner is MultiTermMatch<TInner> inner)
                    {
                        var result = inner.AndWith(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                FunctionTable = new FunctionTable(&SeekToFunc, &FillFunc, &AndWithFunc, &CountFunc, &CurrentFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MultiTermMatch Create<TInner>(in MultiTermMatch<TInner> query)
        {
            return new MultiTermMatch(query, StaticFunctionCache<TInner>.FunctionTable);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MultiTermMatch CreateEmpty(CompactTree tree)
        {
            return new MultiTermMatch(MultiTermMatch<byte>.CreateEmpty(tree), StaticFunctionCache<MultiTermMatch<byte>>.FunctionTable);
        }
    }
}
