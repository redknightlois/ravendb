using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Mappings;
using Sparrow;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public unsafe struct ExistsTermProvider : ITermProvider
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;

        private CompactTree.Iterator _iterator;

        public ExistsTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field)
        {
            _tree = tree;
            _field = field;
            _searcher = searcher;
            _iterator = tree.Iterate();
            _iterator.Reset();
        }

        public void Reset()
        {            
            _iterator = _tree.Iterate();
            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {
            while (_iterator.MoveNext(out var keyScope, out var _))
            {
                term = _searcher.TermQuery(_field, _tree, keyScope.Key.Decoded());
                keyScope.Dispose();
                return true;
            }

            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public bool GetNextTerm(out ReadOnlySpan<byte> term)
        {
            while (_iterator.MoveNext(out var keyScope, out var _))
            {
                var key = keyScope.Key.Decoded();

                int termSize = key.Length;
                if (key.Length > 1)
                {
                    if (key[^1] == 0)
                        termSize--;
                }

                term = key.Slice(0, termSize);
                return true;
            }

            term = Span<byte>.Empty;
            return false;
        }
        
        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(ExistsTermProvider)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() }
                            });
        }
    }
}
