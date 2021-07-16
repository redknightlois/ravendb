using System;
using System.Runtime.CompilerServices;


namespace Corax.Queries
{
    public unsafe struct BinaryMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal BinaryMatch(IQueryMatch match, FunctionTable functionTable)
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
            public readonly delegate*<ref BinaryMatch, long, bool> SeekToFunc;
            public readonly delegate*<ref BinaryMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref BinaryMatch, Span<long>, int> AndWithFunc;
            public readonly delegate*<ref BinaryMatch, long> CountFunc;
            public readonly delegate*<ref BinaryMatch, long> CurrentFunc;

            public FunctionTable(
                delegate*<ref BinaryMatch, long, bool> seekToFunc,
                delegate*<ref BinaryMatch, Span<long>, int> fillFunc,
                delegate*<ref BinaryMatch, Span<long>, int> andWithFunc,
                delegate*<ref BinaryMatch, long> countFunc,
                delegate*<ref BinaryMatch, long> currentFunc)
            {
                SeekToFunc = seekToFunc;
                FillFunc = fillFunc;
                AndWithFunc = andWithFunc;
                CountFunc = countFunc;
                CurrentFunc = currentFunc;
            }
        }

        private static class StaticFunctionCache<TInner, TOuter>
            where TInner : IQueryMatch
            where TOuter : IQueryMatch
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref BinaryMatch match)
                {
                    return ((BinaryMatch<TInner, TOuter>)match._inner).Count;
                }
                static long CurrentFunc(ref BinaryMatch match)
                {
                    return ((BinaryMatch<TInner, TOuter>)match._inner).Current;
                }
                static bool SeekToFunc(ref BinaryMatch match, long v)
                {
                    if (match._inner is BinaryMatch<TInner, TOuter> inner)
                    {
                        var result = inner.SeekTo(v);
                        match._inner = inner;
                        return result;
                    }
                    return false;
                }
                static int FillFunc(ref BinaryMatch match, Span<long> matches)
                {
                    if (match._inner is BinaryMatch<TInner, TOuter> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                static int AndWithFunc(ref BinaryMatch match, Span<long> matches)
                {
                    if (match._inner is BinaryMatch<TInner, TOuter> inner)
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
        public static BinaryMatch Create<TInner, TOuter>(in BinaryMatch<TInner, TOuter> query)
            where TInner : IQueryMatch
            where TOuter : IQueryMatch
        {
            return new BinaryMatch(query, StaticFunctionCache<TInner, TOuter>.FunctionTable);
        }
    }
}
