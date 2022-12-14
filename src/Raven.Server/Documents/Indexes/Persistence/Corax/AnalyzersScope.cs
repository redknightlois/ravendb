using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Mappings;
using Corax.Pipeline;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal unsafe class AnalyzersScope : IDisposable
{
    private readonly IndexFieldsMapping _knownFields;
    private readonly bool _hasDynamics;
    private readonly IndexSearcher _indexSearcher;
    private readonly Dictionary<Slice, Analyzer> _analyzersCache;

    private ByteStringContext<ByteStringMemoryCache>.InternalScope _tempBufferScope;
    private int _tempOutputBufferSize;
    private byte* _tempOutputBuffer;
    private int _tempOutputTokenSize;
    private Token* _tempTokenBuffer;

    public AnalyzersScope(IndexSearcher indexSearcher, IndexFieldsMapping fieldsMapping, bool hasDynamics)
    {
        _indexSearcher = indexSearcher;
        _knownFields = fieldsMapping;
        _hasDynamics = hasDynamics;
        _analyzersCache = new(SliceComparer.Instance);

        InitializeTemporaryBuffers(indexSearcher.Allocator);
    }

    private void InitializeTemporaryBuffers(ByteStringContext allocator)
    {
        _tempOutputBufferSize = Constants.Analyzers.DefaultBufferForAnalyzers;
        _tempOutputTokenSize = Constants.Analyzers.DefaultBufferForAnalyzers;

        _tempBufferScope = allocator.AllocateDirect(_tempOutputBufferSize + _tempOutputTokenSize * Unsafe.SizeOf<Token>(), out var tempBuffer);
        _tempOutputBuffer = tempBuffer.Ptr;
        _tempTokenBuffer = (Token*)(tempBuffer.Ptr + _tempOutputBufferSize);
    }

    private void UnlikelyGrowBuffers(int outputSize, int tokenSize)
    {
        _tempBufferScope.Dispose();

        _tempOutputBufferSize = outputSize;
        _tempOutputTokenSize = tokenSize;

        _tempBufferScope = _indexSearcher.Allocator.AllocateDirect(_tempOutputBufferSize + _tempOutputTokenSize * Unsafe.SizeOf<Token>(), out var tempBuffer);
        _tempOutputBuffer = tempBuffer.Ptr;
        _tempTokenBuffer = (Token*)(tempBuffer.Ptr + _tempOutputBufferSize);
    }

    public ByteStringContext<ByteStringMemoryCache>.InternalScope Execute(Slice fieldName, ReadOnlySpan<byte> source, out ReadOnlySpan<byte> buffer, out ReadOnlySpan<Token> tokens)
    {
        Analyzer analyzer = GetAnalyzer(fieldName);

        analyzer.GetOutputBuffersSize(source.Length, out var outputSize, out var tokensSize);

        if (outputSize > _tempOutputBufferSize || tokensSize > _tempOutputTokenSize)
            UnlikelyGrowBuffers(outputSize, tokensSize);

        var tokenSpace = new Span<Token>(_tempTokenBuffer, _tempOutputTokenSize);
        var wordSpace = new Span<byte>(_tempOutputBuffer, _tempOutputBufferSize);
        analyzer.Execute(source, ref wordSpace, ref tokenSpace);

        var scope = _indexSearcher.Allocator.AllocateDirect(tokenSpace.Length * sizeof(Token) + wordSpace.Length, out var outputMemory);

        var outputBuffer = new Span<byte>(outputMemory.Ptr, wordSpace.Length);
        var outputTokens = new Span<Token>(outputMemory.Ptr + outputBuffer.Length, tokenSpace.Length);

        wordSpace.CopyTo(outputBuffer);
        tokenSpace.CopyTo(outputTokens);

        buffer = outputBuffer;
        tokens = outputTokens;

        return scope;
    }

    private Analyzer GetAnalyzer(Slice fieldName)
    {
        Analyzer analyzer;
        if (_analyzersCache.ContainsKey(fieldName))
            return _analyzersCache[fieldName];

        if (_knownFields.TryGetByFieldName(fieldName, out var binding))
        {
            analyzer = binding.Analyzer;
        }
        else
        {
            if (_hasDynamics is false)
                ThrowWhenDynamicFieldNotFound(fieldName);
            var mode = _indexSearcher.GetFieldIndexingModeForDynamic(fieldName);
            
            analyzer = mode switch
            {
                FieldIndexingMode.Normal => _knownFields!.DefaultAnalyzer,
                FieldIndexingMode.Search => _knownFields!.SearchAnalyzer(fieldName.ToString()),
                FieldIndexingMode.No => Analyzer.CreateDefaultAnalyzer( _indexSearcher.Allocator),
                FieldIndexingMode.Exact => _knownFields!.ExactAnalyzer(fieldName.ToString()),
                _ => ThrowWhenAnalyzerModeNotFound(mode)
            };
        }

        _analyzersCache[fieldName] = analyzer;
        
        return analyzer;
    }

    private static Analyzer ThrowWhenAnalyzerModeNotFound(FieldIndexingMode mode)
    {
        throw new ArgumentOutOfRangeException($"{mode} is not implemented in {nameof(AnalyzersScope)}");
    }

    private static void ThrowWhenDynamicFieldNotFound(Slice fieldName)
    {
        throw new InvalidDataException(
            $"Cannot find field {fieldName.ToString()} inside known binding and also index doesn't contain dynamic fields. The field you try to read is not inside index.");
    }

    public void Dispose()
    {
        _tempBufferScope.Dispose();
    }
}
