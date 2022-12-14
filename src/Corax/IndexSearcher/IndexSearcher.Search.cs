using System;
using System.Data;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Corax.Pipeline;
using Corax.Queries;
using Sparrow.Server;
using Voron;

namespace Corax;

public partial class IndexSearcher
{
    public IQueryMatch SearchQuery(string field, string searchTerm, Constants.Search.Operator @operator, bool isNegated, int analyzerId)
    {
        return SearchQuery<NullScoreFunction>(field, searchTerm, default, @operator, analyzerId, isNegated);
    }

    public IQueryMatch SearchQuery<TScoreFunction>(string field, string searchTerm, TScoreFunction scoreFunction, Constants.Search.Operator @operator, int analyzerId,
        bool isNegated = false, bool manuallyCutWildcards = false)
        where TScoreFunction : IQueryScoreFunction
    {
        bool isDynamic = analyzerId == Constants.IndexWriter.DynamicField;
        ReadOnlySpan<byte> term = Encoding.UTF8.GetBytes(searchTerm).AsSpan();
        if (isNegated)
        {
            @operator = @operator switch
            {
                Constants.Search.Operator.Or => Constants.Search.Operator.And,
                Constants.Search.Operator.And => Constants.Search.Operator.Or,
                _ => throw new ArgumentOutOfRangeException(nameof(@operator), @operator, null)
            };
        }

        if ((_fieldMapping.TryGetByFieldId(analyzerId, out var indexFieldBinding) == false || indexFieldBinding.Analyzer is null) && isDynamic == false)
        {
            throw new InvalidOperationException($"{nameof(SearchQuery)} requires analyzer.");
        }

        var searchAnalyzer = isDynamic 
            ? _fieldMapping.SearchAnalyzer(field) 
            : indexFieldBinding?.Analyzer;
        
        var wildcardAnalyzer = Analyzer.Create<WhitespaceTokenizer, ExactTransformer>(this.Allocator);

        searchAnalyzer.GetOutputBuffersSize(term.Length, out var outputSize, out var tokenSize);
        wildcardAnalyzer.GetOutputBuffersSize(term.Length, out var wildcardSize, out var wildcardTokenSize);

        var tokenStructSize = Unsafe.SizeOf<Token>();
        using var __ = Allocator.Allocate(outputSize + wildcardSize + tokenStructSize * (tokenSize + wildcardTokenSize),  out var buffer);
        Span<byte> encodeBufferOriginal = buffer.ToSpan().Slice(0, outputSize);
        Span<Token> tokensBufferOriginal = MemoryMarshal.Cast<byte, Token>(buffer.ToSpan().Slice(outputSize, tokenSize * tokenStructSize));

        var wildcardAnalyzerBuffer = buffer.ToSpan().Slice(outputSize + tokenSize * tokenStructSize, wildcardSize);
        var wildcardTokenizerBuffer = MemoryMarshal.Cast<byte, Token>(buffer.ToSpan().Slice(outputSize + tokenSize * tokenStructSize + wildcardSize));

        var encodedWildcard = wildcardAnalyzerBuffer;
        var tokensWildcard = wildcardTokenizerBuffer;


        wildcardAnalyzer.Execute(term, ref encodedWildcard, ref tokensWildcard);
        using var _ = Slice.From(Allocator, field, ByteStringType.Immutable, out var fieldName);
        IQueryMatch match = null;
        foreach (var tokenWildcard in tokensWildcard)
        {
            var encoded = encodeBufferOriginal;
            var tokens = tokensBufferOriginal;
            var originalWord = term.Slice(tokenWildcard.Offset, (int)tokenWildcard.Length);
            searchAnalyzer.Execute(term.Slice(tokenWildcard.Offset, (int)tokenWildcard.Length), ref encoded, ref tokens);
            if (tokens.Length == 0)
                continue;


            Constants.Search.SearchMatchOptions mode = Constants.Search.SearchMatchOptions.TermMatch;
            if (originalWord[0] is Constants.Search.Wildcard)
                mode |= Constants.Search.SearchMatchOptions.EndsWith;
            if (originalWord[^1] is Constants.Search.Wildcard)
                mode |= Constants.Search.SearchMatchOptions.StartsWith;
            if (mode.HasFlag(Constants.Search.SearchMatchOptions.StartsWith | Constants.Search.SearchMatchOptions.EndsWith))
                mode = Constants.Search.SearchMatchOptions.Contains;

            int termStart = tokens[0].Offset;
            int termLength = (int)tokens[0].Length;
            if (manuallyCutWildcards)
            {
                if (mode != Constants.Search.SearchMatchOptions.TermMatch)
                {
                    if (mode is Constants.Search.SearchMatchOptions.Contains && termLength <= 2)
                        throw new InvalidDataException(
                            "You are looking for an empty term. Search doesn't support it. If you want to find empty strings, you have to write an equal inside WHERE clause.");
                    if (termLength == 1)
                        throw new InvalidDataException("You are looking for all matches. To retrieve all matches you have to write `true` in WHERE clause.");
                }

                (int startIncrement, int lengthIncrement) = mode switch
                {
                    Constants.Search.SearchMatchOptions.StartsWith => (1,-1),
                    Constants.Search.SearchMatchOptions.EndsWith => (0,-1),
                    Constants.Search.SearchMatchOptions.Contains => (1,-3),
                    Constants.Search.SearchMatchOptions.TermMatch => (0,0),
                    _ => throw new InvalidExpressionException("Unknown flag inside Search match.")
                };

                termStart += startIncrement;
                termLength += lengthIncrement;
            }

            _transaction.Allocator.Allocate(termLength, out ByteString encodedString);
            unsafe
            {
                Unsafe.CopyBlockUnaligned(ref Unsafe.AsRef<byte>(encodedString.Ptr), ref encoded[termStart], (uint)termLength);
            }

            BuildExpression(Allocator, mode, new Slice(encodedString));
        }

        return typeof(TScoreFunction) == typeof(NullScoreFunction)
            ? match
            : Boost(match, scoreFunction);

        void BuildExpression(ByteStringContext ctx, Constants.Search.SearchMatchOptions mode, Slice encodedString)
        {
            switch (mode)
            {
                case Constants.Search.SearchMatchOptions.TermMatch:
                    IQueryMatch exactMatch = isNegated
                        ? UnaryQuery(AllEntries(), analyzerId, encodedString, UnaryMatchOperation.NotEquals)
                        : TermQuery(field, encodedString);

                    if (match is null)
                    {
                        match = exactMatch;
                        return;
                    }

                    match = @operator is Constants.Search.Operator.Or
                        ? Or(match, exactMatch)
                        : And(match, exactMatch);
                    break;
                case Constants.Search.SearchMatchOptions.StartsWith:
                    if (match is null)
                    {
                        match = StartWithQuery(fieldName, encodedString, isNegated);
                        return;
                    }

                    match = @operator is Constants.Search.Operator.Or
                        ? Or(match, StartWithQuery(fieldName, encodedString, isNegated))
                        : And(match, StartWithQuery(fieldName, encodedString, isNegated));
                    break;
                case Constants.Search.SearchMatchOptions.EndsWith:
                    if (match is null)
                    {
                        match = EndsWithQuery(fieldName, encodedString, isNegated);
                        return;
                    }

                    match = @operator is Constants.Search.Operator.Or
                        ? Or(match, EndsWithQuery(fieldName, encodedString, isNegated))
                        : And(match, EndsWithQuery(fieldName, encodedString, isNegated));
                    break;
                case Constants.Search.SearchMatchOptions.Contains:
                    if (match is null)
                    {
                        match = ContainsQuery(fieldName, encodedString, isNegated);
                        return;
                    }

                    match = @operator is Constants.Search.Operator.Or
                        ? Or(match, ContainsQuery(fieldName, encodedString, isNegated))
                        : And(match, ContainsQuery(fieldName, encodedString, isNegated));
                    break;
                default:
                    throw new InvalidExpressionException("Unknown flag inside Search match.");
            }
        }
    }
}
