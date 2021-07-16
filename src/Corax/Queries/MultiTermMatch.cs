using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public unsafe struct MultiTermMatch<TInner> : IQueryMatch
    {
        private readonly CompactTree _tree;
        private readonly delegate*<ref MultiTermMatch<TInner>, long, bool> _seekToFunc;
        private readonly delegate*<ref MultiTermMatch<TInner>, Span<long>, out int, QueryMatchStatus> _moveNext;

        private long _totalResults;
        private long _current;
        private long _currentIdx;
        private TInner _inner;

        public long Count => _totalResults;
        public long Current => _currentIdx <= QueryMatch.Start ? _currentIdx : _current;
        tr
        public MultiTermMatch(CompactTree tree, TInner inner,
            delegate*<ref MultiTermMatch<TInner>, long, bool> seekToFunc,
            delegate*<ref MultiTermMatch<TInner>, Span<long>, out int, QueryMatchStatus> moveNextFunc)
        {
            _inner = inner;
            _totalResults = 0;
            _current = QueryMatch.Start;
            _currentIdx = QueryMatch.Start;

            _tree = tree;
            _seekToFunc = seekToFunc;
            _moveNext = moveNextFunc;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SeekTo(long next = 0)
        {
            return _seekToFunc(ref this, next);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public QueryMatchStatus MoveNext(out long v)
        {
            Span<long> value = stackalloc long[1];
            QueryMatchStatus hasMore = _moveNext(ref this, value, out var len);
            v = value[0];
            return hasMore;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public QueryMatchStatus MoveNext(Span<long> buffer, out int read)
        {
            return _moveNext(ref this, buffer, out read);
        }

        internal static MultiTermMatch<byte> CreateEmpty(CompactTree tree)
        {
            static bool SeekFunc(ref MultiTermMatch<byte> term, long next)
            {
                bool result = next == QueryMatch.Start;
                term._current = result ? QueryMatch.Start : QueryMatch.Invalid;
                return result;
            }

            static QueryMatchStatus MoveNext(ref MultiTermMatch<byte> term, Span<long> buffer, out int read)
            {
                term._currentIdx = QueryMatch.Invalid;
                term._current = QueryMatch.Invalid;
                read = 0;
                return QueryMatchStatus.NoMore;
            }

            return new MultiTermMatch<byte>(tree, 0, &SeekFunc, &MoveNext);
        }
    }
}
