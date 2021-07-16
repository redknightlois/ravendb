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
        private readonly delegate*<ref TermMatch, Span<long>, int> _fillFunc;
        private readonly delegate*<ref TermMatch, Span<long>, int> _andWithFunc;

        private readonly long _totalResults;
        private long _currentIdx;
        private long _baselineIdx;
        private long _current;

        private Container.Item _container;
        private Set.Iterator _set;

        public long Count => _totalResults;
        public long Current => _currentIdx <= QueryMatch.Start ? _currentIdx : _current;

        private TermMatch(
            delegate*<ref TermMatch, long, bool> seekFunc,
            delegate*<ref TermMatch, Span<long>, int> fillFunc,
            delegate*<ref TermMatch, Span<long>, int> andWithFunc,
            long totalResults)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;
            _currentIdx = QueryMatch.Start;
            _baselineIdx = QueryMatch.Start;
            _seekToFunc = seekFunc;
            _fillFunc = fillFunc;
            _andWithFunc = andWithFunc;

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

            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                term._currentIdx = QueryMatch.Invalid;
                term._current = QueryMatch.Invalid;
                return 0;
            }

            return new TermMatch(&SeekFunc, &FillFunc, &FillFunc, 0);
        }

        public static TermMatch YieldOnce(long value)
        {
            static bool SeekFunc(ref TermMatch term, long next)
            {
                term._currentIdx = next > term._current ? QueryMatch.Invalid : QueryMatch.Start;
                return term._currentIdx == QueryMatch.Start;
            }

            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                if (term._currentIdx == QueryMatch.Start)
                {
                    term._currentIdx = term._current;
                    matches[0] = term._current;
                    return 1;
                }

                term._currentIdx = QueryMatch.Invalid;
                return 0;
            }

            static int AndWithFunc(ref TermMatch term, Span<long> matches)
            {
                return matches[0] == term._current ? 1 : 0;
            }


            return new TermMatch(&SeekFunc, &FillFunc, &AndWithFunc, 1)
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
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                var stream = term._container.ToSpan();
                if (term._currentIdx == QueryMatch.Invalid || term._currentIdx >= stream.Length)
                {
                    term._currentIdx = QueryMatch.Invalid;
                    return 0;
                }

                int i = 0;
                for (; i < matches.Length; i++)
                {
                    term._current += ZigZagEncoding.Decode<long>(stream, out var len, (int)term._currentIdx);
                    term._currentIdx += len;
                    matches[i] = term._current;
                }

                return i;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc(ref TermMatch term, Span<long> matches)
            {
                var stream = term._container.ToSpan();

                if (term._current >= matches[0])
                {
                    // need to seek from strart
                    term._currentIdx = term._baselineIdx;
                }
                if (SeekFunc(ref term, matches[0] - 1) == false)
                {
                    return 0;
                }
                int i = 0;
                int matchedIdx = 0;
                while (term._currentIdx < stream.Length && i < matches.Length)
                {
                    term._current += ZigZagEncoding.Decode<long>(stream, out var len, (int)term._currentIdx);
                    term._currentIdx += len;
                    if (matches[i] == term._current)
                    {
                        matches[matchedIdx++] = term._current;
                        i++;
                    }
                }

                return matchedIdx;
            }


            var itemsCount = ZigZagEncoding.Decode<int>(containerItem.ToSpan(), out var len);
            return new TermMatch(&SeekFunc, &FillFunc, &AndWithFunc, itemsCount)
            {
                _container = containerItem,
                _currentIdx = len,
                _baselineIdx = len,
                _current = long.MinValue
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
            static int AndWithFunc(ref TermMatch term, Span<long> matches)
            {
                if (term._current >= matches[0])
                {
                    // need to seek from strart
                    term._set.Seek(matches[0] - 1);
                }
                //TODO: can probably optimize this if set is large and matches is small, better to Seek() to the next values
                int i = 0;
                int matchedIdx = 0;
                while (i < matches.Length && term._set.MoveNext())
                {
                    if (term._set.Current == matches[i])
                    {
                        matches[matchedIdx++] = term._set.Current;
                        i++;
                    }
                }

                return matchedIdx;
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                int i = 0;
                while (i < matches.Length && term._set.MoveNext())
                {
                    matches[i++] = term._set.Current;
                }
                return i;
            }


            return new TermMatch(&SeekFunc, &FillFunc, &AndWithFunc, set.State.NumberOfEntries)
            {
                _set = set.Iterate(),
                _current = long.MinValue
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SeekTo(long next = QueryMatch.Start)
        {
            return _seekToFunc(ref this, next);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            return _fillFunc(ref this, matches);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> matches)
        {
            return _andWithFunc(ref this, matches);
        }
    }
}
