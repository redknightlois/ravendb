using System.Runtime.CompilerServices;
using Sparrow.Server.Compression;
using Voron.Data.Sets;
using Voron.Data.Containers;
using System;

namespace Corax.Queries
{
    public unsafe struct TermMatch : IQueryMatch
    {
        private readonly delegate*<ref TermMatch, long, bool> _seekToFunc;
        private readonly delegate*<ref TermMatch, out long, QueryMatchStatus> _moveNext;

        private long _totalResults;
        private long _currentIdx;
        private long _current;

        private Container.Item _container;
        private Set.Iterator _set;

        public long Count => _totalResults;
        public long Current => _currentIdx <= QueryMatch.Start ? _currentIdx : _current;

        private TermMatch(
            delegate*<ref TermMatch, long, bool> seekFunc, 
            delegate*<ref TermMatch, out long, QueryMatchStatus> moveNext, long totalResults)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;
            _currentIdx = QueryMatch.Start;
            _seekToFunc = seekFunc;
            _moveNext = moveNext;

            _container = default;
            _set = default;
        }

        public static TermMatch CreateEmpty()
        {
            static bool SeekFunc(ref TermMatch term, long next)
            {
                bool result = next == QueryMatch.Start;
                term._current = result ? QueryMatch.Start : QueryMatch.Invalid;
                return result;
            }

            static QueryMatchStatus MoveNextFunc(ref TermMatch term, out long v)
            {
                term._currentIdx = QueryMatch.Invalid;
                term._current = QueryMatch.Invalid;
                v = QueryMatch.Invalid;
                return QueryMatchStatus.NoMore;
            }

            return new TermMatch(&SeekFunc, &MoveNextFunc, 0);
        }

        public static TermMatch YieldOnce(long value)
        {
            static bool SeekFunc(ref TermMatch term, long next)
            {
                term._currentIdx = next > term._current ? QueryMatch.Invalid : QueryMatch.Start;
                return term._currentIdx == QueryMatch.Start;
            }

            static QueryMatchStatus MoveNextFunc(ref TermMatch term, out long v)
            {
                if (term._currentIdx == QueryMatch.Start)
                {
                    term._currentIdx = term._current;
                    v = term._current;
                    return QueryMatchStatus.InOrder;
                }

                v = QueryMatch.Invalid;
                term._currentIdx = QueryMatch.Invalid;
                return QueryMatchStatus.NoMore;
            }

            return new TermMatch(&SeekFunc, &MoveNextFunc, 1)
            {
                _current = value,
                _currentIdx = QueryMatch.Start
            };
        }

        public static TermMatch YieldSmall(Container.Item containerItem)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static bool SeekFunc(ref TermMatch term, long next)
            {
                var stream = term._container.ToSpan();

                while (term._currentIdx < stream.Length)
                {
                    var current = ZigZagEncoding.Decode<long>(stream, out var len, (int)term._currentIdx);
                    term._currentIdx += len;
                    if (current <= next)
                        continue;

                    // We found values bigger than next.
                    term._current = current;
                    return true;
                }

                term._currentIdx = QueryMatch.Invalid;

                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static QueryMatchStatus MoveNextFunc(ref TermMatch term, out long v)
            {
                var stream = term._container.ToSpan();
                if (term._currentIdx == QueryMatch.Invalid || term._currentIdx >= stream.Length)
                {
                    term._currentIdx = QueryMatch.Invalid;
                    v = QueryMatch.Invalid;
                    return QueryMatchStatus.NoMore;
                }

                term._current += ZigZagEncoding.Decode<long>(stream, out var len, (int)term._currentIdx);
                v = term._current;
                term._currentIdx += len;

                return QueryMatchStatus.InOrder;
            }

            var itemsCount = ZigZagEncoding.Decode<int>(containerItem.ToSpan(), out var len);
            return new TermMatch(&SeekFunc, &MoveNextFunc, itemsCount)
            {
                _container = containerItem,
                _currentIdx = len,
            };
        }

        public static TermMatch YieldSet(Set set)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static bool SeekFunc(ref TermMatch term, long next)
            {
                if (next == QueryMatch.Start)
                {
                    term._current = QueryMatch.Start;
                    term._currentIdx = QueryMatch.Start;

                    // We change it in order for the Set Seek operation to seek to the start. 
                    next = long.MinValue;
                }

                return term._set.Seek(next);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static QueryMatchStatus MoveNextFunc(ref TermMatch term, out long v)
            {
                bool hasMove = term._set.MoveNext();
                v = term._set.Current;
                term._currentIdx = hasMove ? v : QueryMatch.Invalid;
                term._current = v;

                return hasMove ? QueryMatchStatus.InOrder : QueryMatchStatus.NoMore;
            }

            return new TermMatch(&SeekFunc, &MoveNextFunc, set.State.NumberOfEntries)
            {
                _set = set.Iterate(),
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SeekTo(long next = QueryMatch.Start)
        {
            return _seekToFunc(ref this, next);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public QueryMatchStatus MoveNext(out long v)
        {
            return _moveNext(ref this, out v);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public QueryMatchStatus MoveNext(Span<long> buffer, out int read)
        {
            read = 0;
            while (read < buffer.Length && _moveNext(ref this, out var v) != QueryMatchStatus.NoMore)
                buffer[read++] = v;

            return read == buffer.Length ? QueryMatchStatus.InOrder : QueryMatchStatus.NoMore;
        }
    }
}
