﻿using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Text.Unicode;
using Corax.Mappings;
using Voron;
using Voron.Data.CompactTrees;
using CompactTreeForwardIterator = Voron.Data.CompactTrees.CompactTree.Iterator<Voron.Data.Lookups.Lookup<Voron.Data.CompactTrees.CompactTree.CompactKeyLookup>.ForwardIterator>;

namespace Corax.Queries;

public struct RegexTermProvider : ITermProvider
{
    private readonly CompactTree _tree;
    private readonly IndexSearcher _searcher;
    private readonly FieldMetadata _field;
    private readonly Regex _regex;

    private CompactTreeForwardIterator _iterator;

    public bool IsOrdered => true;

    public RegexTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, Regex regex)
    {
        _searcher = searcher;
        _regex = regex;
        _tree = tree;
        _iterator = tree.Iterate();
        _iterator.Reset();
        _field = field;
    }


    public void Reset()
    {
        _iterator = _tree.Iterate();
        _iterator.Reset();
    }

    public bool Next(out TermMatch term)
    {
        while (_iterator.MoveNext(out var compactKey, out var _))
        {
            var key = compactKey.Decoded();
            if (_regex.IsMatch(Encoding.UTF8.GetString(key)) == false)
                continue;

            term = _searcher.TermQuery(_field, compactKey, _tree);
            return true;
        }

        term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
        return false;
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(RegexTermProvider)}",
            parameters: new Dictionary<string, string>()
            {
                { "Field", _field.ToString() },
                { "Regex", _regex.ToString()}
            });
    }
}
