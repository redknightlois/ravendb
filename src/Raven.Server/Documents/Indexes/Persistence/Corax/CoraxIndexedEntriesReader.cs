using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Amqp.Framing;
using Corax;
using Corax.Mappings;
using Corax.Pipeline;
using Sparrow;
using Sparrow.Json.Parsing;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public unsafe class CoraxIndexedEntriesReader : IDisposable
{
    private readonly IndexSearcher _indexSearcher;
    private readonly IndexFieldsMapping _fieldsMapping;

    private readonly Dictionary<string, byte[]> _dynamicMapping;
    private readonly HashSet<object> _container;

    private ByteStringContext<ByteStringMemoryCache>.InternalScope _tempBufferScope;
    private int _tempOutputBufferSize;
    private byte* _tempOutputBuffer;
    private int _tempOutputTokenSize;
    private Token* _tempTokenBuffer;

    public CoraxIndexedEntriesReader(IndexSearcher indexSearcher, IndexFieldsMapping fieldsMapping)
    {
        _indexSearcher = indexSearcher;
        _fieldsMapping = fieldsMapping;
        _dynamicMapping = new();

        foreach (var dynamicField in indexSearcher.GetFields())
            _dynamicMapping.Add(dynamicField, Encoding.UTF8.GetBytes(dynamicField));

        _container = new();

        InitializeTemporaryBuffers(indexSearcher.Allocator);
    }

    public DynamicJsonValue GetDocument(ref IndexEntryReader entryReader)
    {
        var doc = new DynamicJsonValue();
        foreach (var binding in _fieldsMapping)
        {
            var fieldReader = entryReader.GetReaderFor(binding.FieldId);

            if (fieldReader.Type == IndexEntryFieldType.Invalid)
                continue;

            doc[binding.FieldNameAsString] = GetValueForField(ref fieldReader, binding.Analyzer);
        }

        foreach (var (fieldAsString, fieldAsBytes) in _dynamicMapping)
        {
            var fieldReader = entryReader.GetReaderFor(fieldAsBytes);

            if (fieldReader.Type == IndexEntryFieldType.Invalid)
                continue;

            doc[fieldAsString] = GetValueForField(ref fieldReader, null);
        }

        return doc;
    }

    public object GetValueForField(ref IndexEntryReader.FieldReader fieldReader, Analyzer analyzer)
    {
        _container.Clear();
        switch (fieldReader.Type)
        {
            case IndexEntryFieldType.Empty:
                _container.Add(string.Empty);
                break;
            case IndexEntryFieldType.Null:
                _container.Add(null);
                break;
            case IndexEntryFieldType.TupleListWithNulls:
            case IndexEntryFieldType.TupleList:
                if (fieldReader.TryReadMany(out var iterator) == false)
                    break;

                while (iterator.ReadNext())
                {
                    if (iterator.IsNull)
                    {
                        _container.Add(null);
                    }
                    else if (iterator.IsEmptyString)
                    {
                        throw new InvalidDataException("Tuple list cannot contain an empty string (otherwise, where did the numeric came from!)");
                    }
                    else
                    {
                        
                        AddBytesAsItemToContainer(iterator.Sequence, analyzer);
                    }
                }

                break;
            case IndexEntryFieldType.Tuple:
                if (fieldReader.Read(out _, out long lVal, out double dVal, out Span<byte> valueInEntry) == false)
                    break;
                AddBytesAsItemToContainer(valueInEntry, analyzer);
                
                break;
            case IndexEntryFieldType.SpatialPointList:
                if (fieldReader.TryReadManySpatialPoint(out var spatialIterator) == false)
                    break;

                while (spatialIterator.ReadNext())
                {
                    for (int i = 1; i <= spatialIterator.Geohash.Length; ++i)
                        AddBytesAsItemToContainer(spatialIterator.Geohash.Slice(0, i), null);
                }

                break;

            case IndexEntryFieldType.SpatialPoint:
                if (fieldReader.Read(out valueInEntry) == false)
                    break;

                for (int i = 1; i <= valueInEntry.Length; ++i)
                    AddBytesAsItemToContainer(valueInEntry.Slice(0, i), null);

                break;

            case IndexEntryFieldType.ListWithNulls:
            case IndexEntryFieldType.List:
                if (fieldReader.TryReadMany(out iterator) == false)
                    break;

                while (iterator.ReadNext())
                {
                    Debug.Assert((fieldReader.Type & IndexEntryFieldType.Tuple) == 0, "(fieldType & IndexEntryFieldType.Tuple) == 0");

                    if (iterator.IsEmptyString || iterator.IsNull)
                    {
                        _container.Add(iterator.IsEmptyString ? string.Empty : null);
                    }
                    else
                    {
                        AddBytesAsItemToContainer(iterator.Sequence, analyzer);
                    }
                }

                break;
            case IndexEntryFieldType.Raw:
            case IndexEntryFieldType.RawList:
            case IndexEntryFieldType.Invalid:
                break;
            default:
                fieldReader.Read(out var value);
                AddBytesAsItemToContainer(value, analyzer);
                break;
        }

        return _container.Count switch
        {
            1 => _container.First(),
            > 1 => _container.ToArray(),
            _ => string.Empty
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddBytesAsItemToContainer(ReadOnlySpan<byte> value, Analyzer analyzer)
    {
        if (analyzer is null)
        {
            _container.Add(Encodings.Utf8.GetString(value));
            return;
        }

        analyzer.GetOutputBuffersSize(value.Length, out var outputSize, out var tokensSize);

        if (outputSize > _tempOutputBufferSize || tokensSize > _tempOutputTokenSize)
            UnlikelyGrowBuffers(outputSize, tokensSize);

        var tokenSpace = new Span<Token>(_tempTokenBuffer, _tempOutputTokenSize);
        var wordSpace = new Span<byte>(_tempOutputBuffer, _tempOutputBufferSize);
        analyzer.Execute(value, ref wordSpace, ref tokenSpace);

        foreach (var token in tokenSpace)
        {
            var analyzedTerm = wordSpace.Slice(token.Offset, (int)token.Length);
            _container.Add(Encoding.UTF8.GetString(analyzedTerm));
        }
    }


    private void InitializeTemporaryBuffers(ByteStringContext allocator)
    {
        _tempOutputBufferSize = Constants.Analyzers.DefaultBufferForAnalyzers;
        _tempOutputTokenSize = Constants.Analyzers.DefaultBufferForAnalyzers;

        _tempBufferScope = allocator.Allocate(_tempOutputBufferSize + _tempOutputTokenSize * Unsafe.SizeOf<Token>(), out var tempBuffer);
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

    public void Dispose()
    {
        _tempBufferScope.Dispose();
    }
}
