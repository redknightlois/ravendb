using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
#if DEBUG
using System.Text;
#endif
using Corax.Mappings;
using Corax.Queries;
using Corax.Utils;
using Sparrow.Compression;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.PostingLists;

namespace Corax;

public partial class IndexSearcher
{
    /// <summary>
    ///  Test API, should not be used anywhere else
    /// </summary>
    public TermMatch TermQuery(string field, string term, bool hasBoost = false) => TermQuery(FieldMetadataBuilder(field, hasBoost: hasBoost), term);
    public TermMatch TermQuery(string field, Slice term, bool hasBoost = false) => TermQuery(FieldMetadataBuilder(field, hasBoost: hasBoost), term);
    public TermMatch TermQuery(Slice field, Slice term, bool hasBoost = false) => TermQuery(FieldMetadata.Build(field, default, default, default, default, hasBoost: hasBoost), term);

    
    public TermMatch TermQuery(FieldMetadata field, string term, CompactTree termsTree = null)
    {
        var terms = termsTree ?? _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return TermMatch.CreateEmpty(this, Allocator);
        }

        var termSlice = term switch
        {
            Constants.NullValue => Constants.NullValueSlice,
            Constants.EmptyString => Constants.EmptyStringSlice,
            _ => EncodeAndApplyAnalyzer(field, term)
        };
        
        return TermQuery(field, terms, termSlice.AsReadOnlySpan());
    }
    
    //Should be already analyzed...
    public TermMatch TermQuery(FieldMetadata field, Slice term, CompactTree termsTree = null)
    {
        var terms = termsTree ?? _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return TermMatch.CreateEmpty(this, Allocator);
        }
        
        return TermQuery(field, terms, term);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal TermMatch TermQuery(in FieldMetadata field, CompactTree tree, Slice term)
    {
        return TermQuery(field, tree, term.AsReadOnlySpan());
    }
    
    public TermMatch TermQuery(FieldMetadata field, CompactKey term, CompactTree termsTree = null)
    {
        return TermQuery(field, termsTree, term.Decoded());
    }

    internal TermMatch TermQuery(in FieldMetadata field, CompactTree tree, ReadOnlySpan<byte> term)
    {
        if (tree.TryGetValue(term, out var value) == false)
            return TermMatch.CreateEmpty(this, Allocator);

        // Calculate bias for BM25 only when needed. There is no reason to calculate this in BM25 class because it would require to pass more information to primitive (and there is no reason to do so).
        double termRatioToWholeCollection = 1;
        if (field.HasBoost)
        {
            var totalTerms = tree.NumberOfEntries;
            var totalSum = _metadataTree.Read(field.TermLengthSumName)?.Reader.ReadLittleEndianInt64() ?? totalTerms;
            
            if (totalTerms == 0 || totalSum == 0)
                termRatioToWholeCollection = 1;
            else
                termRatioToWholeCollection = term.Length /  (totalSum / (double)totalTerms);
        }

        var matches = TermQuery(field, value, termRatioToWholeCollection);
        
        #if DEBUG
        matches.Term = Encoding.UTF8.GetString(term);
        #endif
        return matches;
    }

    internal TermMatch TermQuery(in FieldMetadata field, long containerId, double termRatioToWholeCollection)
    {
        TermMatch matches;
        if ((containerId & (long)TermIdMask.Set) != 0)
        {
            var setId = EntryIdEncodings.GetContainerId(containerId);
            var setStateSpan = Container.Get(_transaction.LowLevelTransaction, setId).ToSpan();

            ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
            var set = new PostingList(_transaction.LowLevelTransaction, Slices.Empty, setState);
            matches = TermMatch.YieldSet(this, Allocator, set, termRatioToWholeCollection, field.HasBoost, IsAccelerated);
        }
        else if ((containerId & (long)TermIdMask.Small) != 0)
        {
            var smallSetId = EntryIdEncodings.GetContainerId(containerId);
            var small = Container.Get(_transaction.LowLevelTransaction, smallSetId);
            matches = TermMatch.YieldSmall(this, Allocator, small, termRatioToWholeCollection, field.HasBoost);
        }
        else
        {

            matches = TermMatch.YieldOnce(this, Allocator, containerId, termRatioToWholeCollection, field.HasBoost);
        }

        return matches;
    }

    public long NumberOfDocumentsUnderSpecificTerm(FieldMetadata binding, string term)
    {
        var terms = _fieldsTree?.CompactTreeFor(binding.FieldName);
        if (terms == null)
            return 0;
        
        var termSlice = term switch
        {
            Constants.NullValue => Constants.NullValueSlice,
            Constants.EmptyString => Constants.EmptyStringSlice,
            _ => EncodeAndApplyAnalyzer(binding, term)
        };
        
        return NumberOfDocumentsUnderSpecificTerm(terms, termSlice);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal long NumberOfDocumentsUnderSpecificTerm(CompactTree tree, Slice term)
    {
        return NumberOfDocumentsUnderSpecificTerm(tree, term.AsReadOnlySpan());
    }

    private long NumberOfDocumentsUnderSpecificTerm(CompactTree tree, ReadOnlySpan<byte> term)
    {
        if (tree.TryGetValue(term, out var value) == false)
            return 0;

        
        if ((value & (long)TermIdMask.Set) != 0)
        {
            var setId = EntryIdEncodings.GetContainerId(value);
            var setStateSpan = Container.Get(_transaction.LowLevelTransaction, setId).ToSpan();
            ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
            return setState.NumberOfEntries;
        }
        
        if ((value & (long)TermIdMask.Small) != 0)
        {
            var smallSetId = EntryIdEncodings.GetContainerId(value);
            var small = Container.Get(_transaction.LowLevelTransaction, smallSetId);
            var itemsCount = ZigZagEncoding.Decode<int>(small.ToSpan(), out var len);

            return itemsCount;
        }

        return 1;
    }
}
