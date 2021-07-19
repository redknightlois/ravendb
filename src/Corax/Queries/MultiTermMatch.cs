using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace Corax.Queries
{
    public interface ITermProvider
    {
        int TermsCount { get; }
        void Reset();
        bool Next(out TermMatch term);
        bool Evaluate(long id);
    }

    public struct InTermProvider : ITermProvider
    {
        private readonly IndexSearcher _searcher;
        private readonly string _field;
        private readonly int _fieldId;
        private readonly List<string> _terms;
        private int _termIndex;

        public InTermProvider(IndexSearcher searcher, string field, int fieldId, List<string> terms)
        {
            _searcher = searcher;
            _field = field;
            _fieldId = fieldId;
            _terms = terms;
            _termIndex = -1;
        }

        public int TermsCount => _terms.Count;

        public void Reset() => _termIndex = -1;

        public bool Next(out TermMatch term)
        {
            _termIndex++;
            if(_termIndex > _terms.Count)
            {
                term = default;
                return false;
            }
            term = _searcher.TermQuery(_field, _terms[_termIndex]);
            return true;
        }

        public bool Evaluate(long id)
        {
            var entry = _searcher.GetReaderFor(id);
            var fieldType = entry.GetFieldType(_fieldId);
            if (fieldType.HasFlag(IndexEntryFieldType.List))
            {
                // TODO: Federico fixme please
            }
            if (entry.Read(_fieldId, out var value) == false)
                return false;

            //TODO: fix me, allocations, O(N^2), etc
            return _terms.Contains(Encoding.UTF8.GetString(value));
        }
    }

    public unsafe struct MultiTermMatch<TTermProvider> : IQueryMatch
        where TTermProvider : ITermProvider
    {
        internal long _totalResults;
        internal long _current;
        internal long _currentIdx;
        internal TTermProvider _inner;
        private TermMatch _currentTerm;

        public long Count => _totalResults;
        public long Current => _currentIdx <= QueryMatch.Start ? _currentIdx : _current;
       
        public MultiTermMatch(TTermProvider inner)
        {
            _inner = inner;
            _totalResults = 0;
            _current = QueryMatch.Start;
            _currentIdx = QueryMatch.Start;
            _currentTerm = TermMatch.CreateEmpty();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            while (true)
            {
                var read = _currentTerm.Fill(buffer);
                if (read != 0)
                    return read;

                if (_inner.Next(out _currentTerm) == false)
                    return 0;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer)
        {
            if (_inner.TermsCount < 16) // TODO: consider the policy here
            {
                return AndWithSmall(buffer);
            }

            int matches = 0;
            int i = 0;
            for (; i < buffer.Length; i++)
            {
                if(_inner.Evaluate(buffer[i]))
                {
                    buffer[matches++] = buffer[i];
                }
            }

            return matches;
        }

        [SkipLocalsInit]
        private int AndWithSmall(Span<long> buffer)
        {            
            Span<long> tmp = stackalloc long[buffer.Length];
            Span<long> tmp2 = stackalloc long[buffer.Length];
            Span<long> results = stackalloc long[buffer.Length];

            _inner.Reset();
            int totalSize = 0;
            while (_inner.Next(out var current))
            {
                buffer.CopyTo(tmp);
                var read = current.AndWith(tmp);
                if (read == 0)
                    continue;

                results.Slice(0, totalSize).CopyTo(tmp2);
                totalSize = MergeHelper.Or(results, tmp2.Slice(0, totalSize), tmp);
            }
            results.Slice(0, totalSize).CopyTo(buffer);
            return totalSize;
        }
    }
}
