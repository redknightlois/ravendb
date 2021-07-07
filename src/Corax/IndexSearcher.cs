using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Sparrow.Server.Compression;
using Voron;
using Voron.Impl;
using Voron.Data.Sets;
using Voron.Data.Containers;

namespace Corax
{
    public interface IIndexMatch
    {
        long Count { get; }
        long Current { get; }

        bool SeekTo(long next = 0);
//        bool IsMatch(long entry);
        bool MoveNext(out long v);
    }

    public static class QueryMatch
    {
        public const long Invalid = -1;
        public const long Start = 0;
    }

    public unsafe struct TermMatch : IIndexMatch
    {
        private readonly delegate*<ref TermMatch, long, bool> _seekToFunc;
        private readonly delegate*<ref TermMatch, out long, bool> _moveNext;

        private long _totalResults;
        private long _currentIdx;
        private long _current;
        
        private Container.Item _container;
        private Set.Iterator _set;

        public long Count => _totalResults;
        public long Current => _currentIdx <= QueryMatch.Start ? _currentIdx : _current;

        private TermMatch(delegate*<ref TermMatch, long, bool> seekFunc, delegate*<ref TermMatch, out long, bool> moveNext, long totalResults)
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
                term._current = next == QueryMatch.Start ? QueryMatch.Start : QueryMatch.Invalid;
                return false;
            }

            static bool MoveNextFunc(ref TermMatch term, out long v)
            {
                term._currentIdx = QueryMatch.Invalid;
                term._current = QueryMatch.Invalid;
                v = QueryMatch.Invalid;
                return false;
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

            static bool MoveNextFunc(ref TermMatch term, out long v)
            {
                if (term._currentIdx == QueryMatch.Start)
                {
                    term._currentIdx = term._current;
                    v = term._current;
                    return true;
                }

                v = QueryMatch.Invalid;
                term._currentIdx = QueryMatch.Invalid;
                return false;
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
            static bool MoveNextFunc(ref TermMatch term, out long v)
            {
                var stream = term._container.ToSpan();
                if (term._currentIdx == QueryMatch.Invalid || term._currentIdx >= stream.Length)
                {
                    term._currentIdx = QueryMatch.Invalid;
                    v = QueryMatch.Invalid;
                    return false;
                }

                term._current += ZigZagEncoding.Decode<long>(stream, out var len, (int)term._currentIdx);
                v = term._current;
                term._currentIdx += len;

                return true;
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
            static bool MoveNextFunc(ref TermMatch term, out long v)
            {
                bool hasMove = term._set.MoveNext();
                v = term._set.Current;
                term._currentIdx = hasMove ? v : QueryMatch.Invalid;
                term._current = v;

                return hasMove;
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
        public bool MoveNext(out long v)
        {
            return _moveNext(ref this, out v);
        }
    }

    public unsafe struct BinaryMatch<TInner, TOuter> : IIndexMatch
        where TInner : struct, IIndexMatch
        where TOuter : struct, IIndexMatch
    {
        private readonly delegate*<ref BinaryMatch<TInner, TOuter>, long, bool> _seekToFunc;
        private readonly delegate*<ref BinaryMatch<TInner, TOuter>, out long, bool> _moveNext;

        private TInner _inner;
        private TOuter _outer;

        private long _totalResults;
        private long _current;

        public long Count => _totalResults;
        public long Current => _current;

        private BinaryMatch(ref TInner inner, ref TOuter outer, delegate*<ref BinaryMatch<TInner, TOuter>, long, bool> seekFunc, delegate*<ref BinaryMatch<TInner, TOuter>, out long, bool> moveNext, long totalResults)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;
            _seekToFunc = seekFunc;
            _moveNext = moveNext;
            _inner = inner;
            _outer = outer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SeekTo(long next = 0)
        {
            return _seekToFunc(ref this, next);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext(out long v)
        {
            return _moveNext(ref this, out v);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BinaryMatch<TInner, TOuter> YieldAnd(ref TInner inner, ref TOuter outer)
        {
            static bool SeekToFunc(ref BinaryMatch<TInner, TOuter> match, long v)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                return inner.SeekTo(v) && outer.SeekTo(v);
            }

            static bool MoveNextFunc(ref BinaryMatch<TInner, TOuter> match, out long v)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                if (inner.Current == QueryMatch.Invalid || outer.Current == QueryMatch.Invalid)
                    goto Fail;

                // Last were equal, moving forward. 
                inner.MoveNext(out v);
                outer.MoveNext(out v);

                while (inner.Current != outer.Current)
                {
                    if (inner.Current < outer.Current)
                    {
                        if (inner.MoveNext(out v) == false)
                            goto Fail;
                    }
                    else
                    {
                        if (outer.MoveNext(out v) == false)
                            goto Fail;
                    }
                }

                // PERF: We dont need to check both as the equal later will take care of that. 
                if (inner.Current == QueryMatch.Invalid)
                    goto Fail;

                if (inner.Current == outer.Current)
                {
                    match._current = inner.Current;
                    return true;
                }

                Fail:  
                match._current = QueryMatch.Invalid;
                v = QueryMatch.Invalid;
                return false;
            }

            return new BinaryMatch<TInner, TOuter>(ref inner, ref outer, &SeekToFunc, &MoveNextFunc, Math.Min(inner.Count, outer.Count));
        }

        public static BinaryMatch<TInner, TOuter> YieldOr(ref TInner inner, ref TOuter outer)
        {
            static bool SeekToFunc(ref BinaryMatch<TInner, TOuter> match, long v)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                return inner.SeekTo(v) && outer.SeekTo(v);
            }

            static bool MoveNextFunc(ref BinaryMatch<TInner, TOuter> match, out long v)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;                
                
                // Nothing else left to add
                if (inner.Current == QueryMatch.Invalid && outer.Current == QueryMatch.Invalid)
                {
                    v = QueryMatch.Invalid;
                    goto Done;
                }
                else if (inner.Current == QueryMatch.Invalid)
                {
                    outer.MoveNext(out v);
                    goto Done;
                }
                else if (outer.Current == QueryMatch.Invalid)
                {
                    inner.MoveNext(out v);
                    goto Done;
                }

                long x, y;
                if (inner.Current == outer.Current)
                {
                    inner.MoveNext(out x);
                    outer.MoveNext(out y);
                }
                else if (inner.Current < outer.Current)
                {
                    inner.MoveNext(out x);
                    y = outer.Current;
                }
                else
                {
                    x = inner.Current;
                    outer.MoveNext(out y);
                }

                if (x == QueryMatch.Invalid && y == QueryMatch.Invalid)
                {
                    v = QueryMatch.Invalid;
                    match._current = QueryMatch.Invalid;
                    return false;
                }
                else if (x == QueryMatch.Invalid)
                {
                    v = y;
                }
                else if (y == QueryMatch.Invalid)
                {
                    v = x;
                }
                else
                {
                    v = x < y ? x : y;
                }

                Done: match._current = v;
                return v != QueryMatch.Invalid;
            }

            return new BinaryMatch<TInner, TOuter>(ref inner, ref outer, &SeekToFunc, &MoveNextFunc, inner.Count + outer.Count);
        }
    }

    public class IndexSearcher : IDisposable
    {
        private readonly StorageEnvironment _environment;
        private readonly Transaction _transaction;

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index searcher with opening semantics and also every new
        // searcher becomes essentially a unit of work which makes reusing assets tracking more explicit.
        public IndexSearcher(StorageEnvironment environment)
        {
            _environment = environment;
            _transaction = environment.ReadTransaction();
        }

        public string GetEntryById(long id)
        {
            var data = Container.Get(_transaction.LowLevelTransaction, id).ToSpan();
            int size = ZigZagEncoding.Decode<int>(data, out var len);
            return Encoding.UTF8.GetString(data.Slice(len, size));
        }

        public IIndexMatch Search(string q)
        {
            var parser = new QueryParser();
            parser.Init(q);
            var query = parser.Parse();
            return Search(query.Where);
        }

        private IIndexMatch Search(QueryExpression where)
        {
            // if (where.Compiled != null)
            //     return;
            
            switch (@where)
            {
                case TrueExpression _:
                case null:
                    return null; // all docs here
                case BinaryExpression be:
                    return (be.Operator, be.Left, be.Right) switch
                    {
                        (OperatorType.Equal, FieldExpression f, ValueExpression v) => TermQuery(f.FieldValue, v.Token.Value),
                        //(OperatorType.And , _, _) => BinaryMatch.YieldAnd(),
                        _ => throw new NotSupportedException()
                    };
                default:
                    return null;
            }
        }

        // foreach term in 2010 .. 2020
        //     yield return TermMatch(field, term)// <-- one term , not sorted

        // userid = UID and date between 2010 and 2020 <-- 100 million terms here 
        // foo = bar and published = true

        // foo = bar
        public TermMatch TermQuery(string field, string term)
        {
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null || terms.TryGetValue(term, out var value) == false)
                return TermMatch.CreateEmpty();
            
            TermMatch matches;
            if ((value & (long)TermIdMask.Set) != 0)
            {
                var setId = value & ~0b11;
                var setStateSpan = Container.Get(_transaction.LowLevelTransaction, setId).ToSpan();
                ref readonly var setState = ref MemoryMarshal.AsRef<SetState>(setStateSpan);
                var set = new Set(_transaction.LowLevelTransaction, Slices.Empty, setState);
                matches = TermMatch.YieldSet(set);                
            }
            else if ((value & (long)TermIdMask.Small) != 0)
            {
                var smallSetId = value & ~0b11;
                var small = Container.Get(_transaction.LowLevelTransaction, smallSetId);
                matches = TermMatch.YieldSmall(small);                
            }
            else
            {
                matches = TermMatch.YieldOnce(value);
            }
                
            return matches;
        }


        public BinaryMatch<TInner, TOuter> And<TInner, TOuter>(ref TInner set1, ref TOuter set2)
            where TInner : struct, IIndexMatch
            where TOuter : struct, IIndexMatch
        {
            return BinaryMatch<TInner, TOuter>.YieldAnd(ref set1, ref set2);
        }

        public BinaryMatch<TInner, TOuter> Or<TInner, TOuter>(ref TInner set1, ref TOuter set2)
            where TInner : struct, IIndexMatch
            where TOuter : struct, IIndexMatch
        {
            return BinaryMatch<TInner, TOuter>.YieldOr(ref set1, ref set2);
        }

        public void Dispose()
        {
            _transaction?.Dispose();
        }
    }
}
