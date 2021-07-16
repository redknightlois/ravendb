using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public unsafe struct MultiTermMatch<TInner> : IQueryMatch
    {
        private readonly delegate*<ref MultiTermMatch<TInner>, long, bool> _seekToFunc;
        private readonly delegate*<ref MultiTermMatch<TInner>, Span<long>, int> _fillFunc;
        private readonly delegate*<ref MultiTermMatch<TInner>, Span<long>, int> _andWith;

        internal long _totalResults;
        internal long _current;
        internal long _currentIdx;
        internal TInner _inner;

        public long Count => _totalResults;
        public long Current => _currentIdx <= QueryMatch.Start ? _currentIdx : _current;
       
        public MultiTermMatch(TInner inner,
            delegate*<ref MultiTermMatch<TInner>, long, bool> seekFunc,
            delegate*<ref MultiTermMatch<TInner>, Span<long>, int> fillFunc,
            delegate*<ref MultiTermMatch<TInner>, Span<long>, int> andWith)
        {
            _inner = inner;
            _totalResults = 0;
            _current = QueryMatch.Start;
            _currentIdx = QueryMatch.Start;

            _seekToFunc = seekFunc;
            _fillFunc = fillFunc;
            _andWith = andWith;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SeekTo(long next = 0)
        {
            return _seekToFunc(ref this, next);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            return _fillFunc(ref this, buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer)
        {
            return _andWith(ref this, buffer);
        }

        internal static MultiTermMatch<byte> CreateEmpty(CompactTree tree)
        {
            static bool SeekFunc(ref MultiTermMatch<byte> match, long next)
            {
                bool result = next == QueryMatch.Start;
                match._current = result ? QueryMatch.Start : QueryMatch.Invalid;
                return result;
            }

            static int FillFunc(ref MultiTermMatch<byte> match, Span<long> matches)
            {
                match._currentIdx = QueryMatch.Invalid;
                match._current = QueryMatch.Invalid;
                return 0;
            }

            return new MultiTermMatch<byte>(tree, 0, &SeekFunc, &FillFunc, &FillFunc);
        }
    }
}
