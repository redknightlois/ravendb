using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Server.Compression;
using Voron;
using Voron.Impl;
using Voron.Data.Containers;
using Sparrow;
using System.Runtime.Intrinsics.X86;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Queries;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.BTrees;
using Voron.Data.Fixed;

namespace Corax;

public sealed unsafe partial class IndexSearcher : IDisposable
{
    private readonly Transaction _transaction;
    private readonly IndexFieldsMapping _fieldMapping;
    private Tree _persistedDynamicTreeAnalyzer;


    private Page _lastPage = default;
    private long? _numberOfEntries;

    /// <summary>
    /// When true no SIMD instruction will be used. Useful for checking that optimized algorithms behave in the same
    /// way than reference algorithms. 
    /// </summary>
    public bool ForceNonAccelerated { get; set; }

    public bool IsAccelerated => Avx2.IsSupported && !ForceNonAccelerated;

    public long NumberOfEntries => _numberOfEntries ??= _metadataTree?.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;

    public ByteStringContext Allocator => _transaction.Allocator;

    internal Transaction Transaction => _transaction;

    public IndexFieldsMapping FieldMapping => _fieldMapping;

    private readonly bool _ownsTransaction;
    private readonly bool _ownsIndexMapping;

    private readonly Tree _metadataTree;
    private readonly Tree _fieldsTree;

    public bool DocumentsAreBoosted => GetDocumentBoostTree().NumberOfEntries > 0;

    // The reason why we want to have the transaction open for us is so that we avoid having
    // to explicitly provide the index searcher with opening semantics and also every new
    // searcher becomes essentially a unit of work which makes reusing assets tracking more explicit.
    public IndexSearcher(StorageEnvironment environment, IndexFieldsMapping fieldsMapping = null) : this(fieldsMapping)
    {
        _ownsTransaction = true;
        _transaction = environment.ReadTransaction();
        _fieldsTree = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        _metadataTree = _transaction.ReadTree(Constants.IndexMetadataSlice);
    }

    public IndexSearcher(Transaction tx, IndexFieldsMapping fieldsMapping = null) : this(fieldsMapping)
    {
        _ownsTransaction = false;
        _transaction = tx;
        _fieldsTree = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        _metadataTree = _transaction.ReadTree(Constants.IndexMetadataSlice);
    }

    private IndexSearcher(IndexFieldsMapping fieldsMapping)
    {
        if (fieldsMapping is null)
        {
            _ownsIndexMapping = true;
            using var builder = IndexFieldsMappingBuilder.CreateForReader();
            _fieldMapping = builder.Build();
        }
        else
        {
            _fieldMapping = fieldsMapping;
        }
    }

    public UnmanagedSpan GetIndexEntryPointer(long id)
    {
        var data = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref _lastPage, id);
        int size = ZigZagEncoding.Decode<int>(data.ToSpan(), out var len);
        return data.ToUnmanagedSpan().Slice(size + len);
    }

    public IndexEntryReader GetReaderFor(long id)
    {
        return GetReaderFor(_transaction, ref _lastPage, id, out _);
    }

    public static IndexEntryReader GetReaderFor(Transaction transaction, ref Page page, long id, out int rawSize)
    {
        var item = Container.MaybeGetFromSamePage(transaction.LowLevelTransaction, ref page, id);
        int size = ZigZagEncoding.Decode<int>(item.Address, out var len);

        rawSize = item.Length;
        int headerSize = size + len;
        return new IndexEntryReader(item.Address + headerSize, item.Length - headerSize);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IndexEntryReader GetReaderAndIdentifyFor(long id, out string key)
    {
        var item = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref _lastPage, id);

        int size = ZigZagEncoding.Decode<int>(item.Address, out var len);

        var idSpan = new ReadOnlySpan<byte>(item.Address + len, size);
        key = Encoding.UTF8.GetString(idSpan);
        
        int headerSize = size + len;
        return new(item.Address + headerSize, item.Length - headerSize);
    }

    public string GetIdentityFor(long id)
    {
        var data = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref _lastPage, id).ToSpan();
        int size = ZigZagEncoding.Decode<int>(data, out var len);
        return Encoding.UTF8.GetString(data.Slice(len, size));
    }

    public UnmanagedSpan GetRawIdentityFor(long id)
    {
        var data = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref _lastPage, id).ToUnmanagedSpan();
        int size = ZigZagEncoding.Decode<int>(data, out var len);
        return data.Slice(len, size);
    }

    //Function used to generate Slice from query parameters.
    //We cannot dispose them before the whole query is executed because they are an integral part of IQueryMatch.
    //We know that the Slices are automatically disposed when the transaction is closed so we don't need to track them.
    [SkipLocalsInit]
    internal Slice EncodeAndApplyAnalyzer(string term, int fieldId)
    {
        if (term is null)
            return default;

        if (term.Length == 0 || term == Constants.EmptyString)
            return Constants.EmptyStringSlice;

        if (term == Constants.NullValue)
            return Constants.NullValueSlice;

        ApplyAnalyzer(term, fieldId, out var encodedTerm);
        return encodedTerm;
    }

    internal ByteStringContext<ByteStringMemoryCache>.InternalScope ApplyAnalyzer(string originalTerm, int fieldId, out Slice value)
    {
        using (Slice.From(Allocator, originalTerm, ByteStringType.Immutable, out var originalTermSliced))
        {
            return ApplyAnalyzer(originalTermSliced, fieldId, out value);
        }
    }
    
    internal ByteStringContext<ByteStringMemoryCache>.InternalScope ApplyAnalyzer(ReadOnlySpan<byte> originalTerm, int fieldId, out Slice value)
    {
        Analyzer analyzer = null;
        IndexFieldBinding binding = null;
        
        if (fieldId == Constants.IndexWriter.DynamicField)
        {
            analyzer = _fieldMapping.DefaultAnalyzer;
        }
        else if (_fieldMapping.TryGetByFieldId(fieldId, out binding) == false
            || binding.FieldIndexingMode is FieldIndexingMode.Exact
            || binding.Analyzer is null)
        {
            var disposable = Allocator.AllocateDirect(originalTerm.Length, ByteStringType.Immutable, out var originalTermSliced);
            originalTerm.CopyTo(new Span<byte>(originalTermSliced._pointer->Ptr, originalTerm.Length));

            value = new Slice(originalTermSliced);
            return disposable;
        }

        analyzer ??= binding.FieldIndexingMode is FieldIndexingMode.Search 
            ? Analyzer.CreateLowercaseAnalyzer(_transaction.Allocator) // lowercase only when search is used in non-full-text-search match 
            : binding.Analyzer!;
        
        return AnalyzeTerm(analyzer, originalTerm, fieldId, out value);
    }

    [SkipLocalsInit]
    internal ByteStringContext<ByteStringMemoryCache>.InternalScope ApplyAnalyzer(Slice originalTerm, int fieldId, out Slice value)
    {
        return ApplyAnalyzer(originalTerm.AsSpan(), fieldId, out value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ByteStringContext<ByteStringMemoryCache>.InternalScope AnalyzeTerm(Analyzer analyzer, ReadOnlySpan<byte> originalTerm, int fieldId, out Slice value)
    {
        analyzer.GetOutputBuffersSize(originalTerm.Length, out int outputSize, out int tokenSize);

        Debug.Assert(outputSize < 1024 * 1024, "Term size is too big for analyzer.");
        Debug.Assert(Unsafe.SizeOf<Token>() * tokenSize < 1024 * 1024, "Analyzer wants to create too much tokens.");

        var buffer = Analyzer.BufferPool.Rent(outputSize);
        var tokens = Analyzer.TokensPool.Rent(tokenSize);

        Span<byte> bufferSpan = buffer.AsSpan();
        Span<Token> tokensSpan = tokens.AsSpan();
        analyzer.Execute(originalTerm, ref bufferSpan, ref tokensSpan);
        Debug.Assert(tokensSpan.Length == 1, $"{nameof(ApplyAnalyzer)} should create only 1 token as a result.");
        var disposable = Slice.From(Allocator, bufferSpan, ByteStringType.Immutable, out value);

        Analyzer.TokensPool.Return(tokens);
        Analyzer.BufferPool.Return(buffer);

        return disposable;
    }

    public AllEntriesMatch AllEntries()
    {
        return new AllEntriesMatch(this, _transaction);
    }
    
    public TermMatch EmptySet() => TermMatch.CreateEmpty(Allocator);

    public long GetEntriesAmountInField(string name)
    {
        var terms = _fieldsTree?.CompactTreeFor(name);

        return terms?.NumberOfEntries ?? 0;
    }

    public bool TryGetTermsOfField(string field, out ExistsTermProvider existsTermProvider)
    {
        using var _ = Slice.From(Allocator, field, ByteStringType.Immutable, out var fieldName);
        var terms = _fieldsTree?.CompactTreeFor(fieldName);

        if (terms == null)
        {
            existsTermProvider = default;
            return false;
        }

        existsTermProvider = new ExistsTermProvider(this, _transaction.Allocator, terms, fieldName);
        return true;
    }
    
    public List<string> GetFields()
    {
        List<string> fieldsInIndex = new();
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);

        //Return an empty set when the index doesn't contain any fields. Case when index has 0 entries.
        if (fields is null)
            return fieldsInIndex;
        
        
        using (var it = fields.Iterate(false))
        {
            if (it.Seek(Slices.BeforeAllKeys))
            {
                do
                {
                    if (it.CurrentKey.EndsWith(Constants.IndexWriter.DoubleTreeSuffix) || it.CurrentKey.EndsWith(Constants.IndexWriter.LongTreeSuffix))
                        continue;
                    
                    fieldsInIndex.Add(it.CurrentKey.ToString());
                } while (it.MoveNext());
            }
        }
       
        return fieldsInIndex;
    }
    
    
    public (Slice FieldName, Slice NumericTree) GetSliceForRangeQueries<T>(string name, T value)
    {
        Slice.From(Allocator, name, ByteStringType.Immutable, out var fieldName);
        Slice numericTree;
        switch (value)
        {
            case long l:
                Slice.From(Allocator, $"{name}-L", ByteStringType.Immutable, out numericTree);
                break;
            case double d:
                Slice.From(Allocator, $"{name}-D", ByteStringType.Immutable, out numericTree);
                break;
            default:
                numericTree = default;
                break;
        }

        return (fieldName, numericTree);
    }

    public FieldIndexingMode GetFieldIndexingModeForDynamic(Slice name)
    {
        _persistedDynamicTreeAnalyzer ??= _transaction.ReadTree(Constants.IndexWriter.DynamicFieldsAnalyzersSlice);
        var readResult = _persistedDynamicTreeAnalyzer?.Read(name);
        if (readResult == null)
            return FieldIndexingMode.Normal;

        var mode = (FieldIndexingMode)readResult.Reader.ReadByte();
        return mode;
    }

    internal FixedSizeTree GetDocumentBoostTree()
    {
        return _transaction.FixedTreeFor(Constants.DocumentBoostSlice, sizeof(float));
    }

    public void Dispose()
    {
        if (_ownsTransaction)
            _transaction?.Dispose();

        if (_ownsIndexMapping)
            _fieldMapping?.Dispose();
    }
}
