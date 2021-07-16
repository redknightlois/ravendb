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
        public QueryMatchStatus MoveNext(out long v)
        {
            Span<long> buffer = stackalloc long[1];
            var result = _functionTable.MoveNextFunc(ref this, buffer, out var _);
            v = buffer[0];
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public QueryMatchStatus MoveNext(Span<long> buffer, out int read)
        {
            return _functionTable.MoveNextFunc(ref this, buffer, out read);
        }

        internal class FunctionTable
        {
            public readonly delegate*<ref MultiTermMatch, long, bool> SeekToFunc;
            public readonly delegate*<ref MultiTermMatch, Span<long>, out int, QueryMatchStatus> MoveNextFunc;
            public readonly delegate*<ref MultiTermMatch, long> CountFunc;
            public readonly delegate*<ref MultiTermMatch, long> CurrentFunc;

            public FunctionTable(
                delegate*<ref MultiTermMatch, long, bool> seekToFunc,
                delegate*<ref MultiTermMatch, Span<long>, out int, QueryMatchStatus> moveNextFunc,
                delegate*<ref MultiTermMatch, long> countFunc,
                delegate*<ref MultiTermMatch, long> currentFunc)
            {
                SeekToFunc = seekToFunc;
                MoveNextFunc = moveNextFunc;
                CountFunc = countFunc;
                CurrentFunc = currentFunc;
            }
        }

        private static class StaticFunctionCache<TInner>
            where TInner : IQueryMatch
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
                static QueryMatchStatus MoveNextFunc(ref MultiTermMatch match, Span<long> buffer, out int read)
                {
                    if (match._inner is MultiTermMatch<TInner> inner)
                    {
                        var result = inner.MoveNext(buffer, out read);
                        match._inner = inner;
                        return result;
                    }
                    Unsafe.SkipInit(out read);
                    return QueryMatchStatus.NoMore;
                }

                FunctionTable = new FunctionTable(&SeekToFunc, &MoveNextFunc, &CountFunc, &CurrentFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MultiTermMatch Create<TInner>(in MultiTermMatch<TInner> query)
            where TInner : IQueryMatch
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
