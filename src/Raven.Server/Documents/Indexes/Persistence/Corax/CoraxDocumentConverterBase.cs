using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Utils;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Exceptions;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using RavenConstants = Raven.Client.Constants;
using CoraxConstants = Corax.Constants;
using Encoding = System.Text.Encoding;
using System.Diagnostics;
using Raven.Server.NotificationCenter.Notifications;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public abstract class CoraxDocumentConverterBase : ConverterBase
{
    public static readonly Memory<byte> NullValue = Encoding.UTF8.GetBytes(RavenConstants.Documents.Indexing.Fields.NullValue);
    public static readonly Memory<byte> EmptyString = Encoding.UTF8.GetBytes(RavenConstants.Documents.Indexing.Fields.EmptyString);
    
    //todo maciej
    private static readonly Memory<byte> _trueLiteral = new Memory<byte>(Encoding.UTF8.GetBytes("true"));
    private static readonly Memory<byte> _falseLiteral = new Memory<byte>(Encoding.UTF8.GetBytes("false"));
    private static readonly StandardFormat _standardFormat = new StandardFormat('g');
    private static readonly StandardFormat _timeSpanFormat = new StandardFormat('c');

    private ConversionScope Scope;
    protected readonly IndexFieldsMapping _knownFields;
    protected readonly ByteStringContext _allocator;

    private const int InitialSizeOfEnumerableBuffer = 128;

    private bool EnumerableDataStructExist =>
        StringsListForEnumerableScope is not null && LongsListForEnumerableScope is not null && DoublesListForEnumerableScope is not null;

    public List<Memory<byte>> StringsListForEnumerableScope;
    public List<long> LongsListForEnumerableScope;
    public List<double> DoublesListForEnumerableScope;
    public List<BlittableJsonReaderObject> BlittableJsonReaderObjectsListForEnumerableScope;
    public List<CoraxSpatialPointEntry> CoraxSpatialPointEntryListForEnumerableScope;


    public abstract Span<byte> SetDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext,
        out LazyStringValue id, Span<byte> writerBuffer);

    protected CoraxDocumentConverterBase(Index index, bool storeValue, bool indexImplicitNull, bool indexEmptyEntries, int numberOfBaseFields, string keyFieldName,
        string storeValueFieldName, ICollection<IndexField> fields = null) : base(index, storeValue, indexImplicitNull, indexEmptyEntries, numberOfBaseFields,
        keyFieldName, storeValueFieldName, fields)
    {
        _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        _knownFields = GetKnownFields(_allocator, index);
        _knownFields.UpdateAnalyzersInBindings( CoraxIndexingHelpers.CreateCoraxAnalyzers(_allocator, index, index.Definition));
        Scope = new();
    }

    public static IndexFieldsMapping GetKnownFields(ByteStringContext allocator, Index index)
    {
        var knownFields = new IndexFieldsMapping(allocator);
        //todo maciej: perf
        Slice.From(allocator, index.Type.IsMapReduce()
            ? RavenConstants.Documents.Indexing.Fields.ReduceKeyValueFieldName
            : RavenConstants.Documents.Indexing.Fields.DocumentIdFieldName, ByteStringType.Immutable, out var value);

        knownFields = knownFields.AddBinding(0, value, null, hasSuggestion: false, fieldIndexingMode: FieldIndexingMode.Normal);
        foreach (var field in index.Definition.IndexFields.Values)
        {
            Slice.From(allocator, field.Name, ByteStringType.Immutable, out value);
            knownFields = knownFields.AddBinding(field.Id, value, null, 
                hasSuggestion: field.HasSuggestions, 
                fieldIndexingMode: TranslateRavenFieldIndexingIntoCoraxFieldIndexingMode(field.Indexing),
                field.Spatial is not null);
        }

        if (index.Type.IsMapReduce())
        {
            Slice.From(allocator, RavenConstants.Documents.Indexing.Fields.AllStoredFields, ByteStringType.Immutable, out var storedKey);
            knownFields = knownFields.AddBinding(knownFields.Count, storedKey, null, false, FieldIndexingMode.No);
        }

        return knownFields;
    }

    public IndexFieldsMapping GetKnownFields() => _knownFields;

    protected void InsertRegularField(IndexField field, object value, JsonOperationContext indexContext, ref IndexEntryWriter entryWriter,
        IWriterScope scope, bool nestedArray = false)
    {
        var valueType = GetValueType(value);
        long @long;
        double @double;

        switch (valueType)
        {
            case ValueType.Double:
                if (value is LazyNumberValue ldv)
                {
                    if (TryToTrimTrailingZeros(ldv, indexContext, out var doubleAsString) == false)
                        doubleAsString = ldv.Inner;
                    @long = (long)ldv;
                    @double = ldv.ToDouble(CultureInfo.InvariantCulture);
                    scope.Write(field.Id, doubleAsString.AsSpan(), @long, @double, ref entryWriter);
                    break;
                }
                else
                {
                    using (_allocator.Allocate(128, out var buffer))
                    {
                        var length = 0;
                        switch (value)
                        {
                            case double d:
                                if (Utf8Formatter.TryFormat(d, buffer.ToSpan(), out length, _standardFormat) == false)
                                    throw new Exception($"Cannot convert {field.Name} as double into bytes.");
                                break;

                            case decimal dm:
                                if (Utf8Formatter.TryFormat(dm, buffer.ToSpan(), out length, _standardFormat) == false)
                                    throw new Exception($"Cannot convert {field.Name} as decimal into bytes.");
                                break;

                            case float f:
                                if (Utf8Formatter.TryFormat(f, buffer.ToSpan(), out length, _standardFormat) == false)
                                    throw new Exception($"Cannot convert {field.Name} as float into bytes.");
                                break;
                        }

                        @long = Convert.ToInt64(value);
                        @double = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                        buffer.Truncate(length);
                        scope.Write(field.Id, buffer.ToSpan(), @long, @double, ref entryWriter);
                        return;
                    }
                }

            case ValueType.Numeric:
                var lazyNumber = value as LazyNumberValue;
                if (lazyNumber == null)
                {
                    scope.Write(field.Id, lazyNumber.Inner.AsSpan(), (long)value, Convert.ToDouble(value), ref entryWriter);
                    return;
                }

                @long = (long)lazyNumber;
                @double = lazyNumber.ToDouble(CultureInfo.InvariantCulture);

                scope.Write(field.Id, lazyNumber.Inner.AsSpan(), @long, @double, ref entryWriter);
                return;

            case ValueType.String:
                scope.Write(field.Id, value.ToString(), ref entryWriter);
                return;

            case ValueType.LazyCompressedString:
            case ValueType.LazyString:
                LazyStringValue lazyStringValue;
                if (valueType == ValueType.LazyCompressedString)
                    lazyStringValue = ((LazyCompressedStringValue)value).ToLazyStringValue();
                else
                    lazyStringValue = (LazyStringValue)value;
                scope.Write(field.Id, lazyStringValue.AsSpan(), ref entryWriter);
                return;

            case ValueType.Enum:
                scope.Write(field.Id, value.ToString(), ref entryWriter);
                return;

            case ValueType.Boolean:
                scope.Write(field.Id, ((bool)value ? _trueLiteral : _falseLiteral).Span, ref entryWriter);
                return;

            case ValueType.DateTime:
                var dateTime = (DateTime)value;
                var dateAsBytes = dateTime.GetDefaultRavenFormat();
                scope.Write(field.Id, dateAsBytes, ref entryWriter);
                return;

            case ValueType.DateTimeOffset:
                var dateTimeOffset = (DateTimeOffset)value;
                var dateTimeOffsetBytes = dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(isUtc: true);
                scope.Write(field.Id, dateTimeOffsetBytes, ref entryWriter);
                return;

            case ValueType.TimeSpan:
                var timeSpan = (TimeSpan)value;
                using (_allocator.Allocate(256, out var buffer))
                {
                    if (Utf8Formatter.TryFormat(timeSpan, buffer.ToSpan(), out var bytesWritten, _timeSpanFormat) == false)
                        throw new Exception($"Cannot convert {field.Name} as double into bytes.");
                    buffer.Truncate(bytesWritten);
                    scope.Write(field.Id, buffer.ToSpan(), ref entryWriter);
                }

                return;
            
            case ValueType.DateOnly:
                var dateOnly = ((DateOnly)value).ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture);
                scope.Write(field.Id, dateOnly, ref entryWriter);
                return;
            
            case ValueType.TimeOnly:
                var timeOnly = ((TimeOnly)value).ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture);
                scope.Write(field.Id, timeOnly, ref entryWriter);
                return;
            
            case ValueType.Convertible:
                var iConvertible = (IConvertible)value;
                @long = iConvertible.ToInt64(CultureInfo.InvariantCulture);
                @double = iConvertible.ToDouble(CultureInfo.InvariantCulture);

                scope.Write(field.Id, iConvertible.ToString(CultureInfo.InvariantCulture), @long, @double, ref entryWriter);
                return;

            case ValueType.Enumerable:
                RuntimeHelpers.EnsureSufficientExecutionStack();
                var iterator = (IEnumerable)value;
                if (EnumerableDataStructExist == false)
                {
                    StringsListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
                    LongsListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
                    DoublesListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
                    BlittableJsonReaderObjectsListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
                    CoraxSpatialPointEntryListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
                }

                var canFinishEnumerableWriting = false;
                if (scope is not EnumerableWriterScope enumerableWriterScope)
                {
                    canFinishEnumerableWriting = true;
                    enumerableWriterScope = new EnumerableWriterScope(StringsListForEnumerableScope, LongsListForEnumerableScope, DoublesListForEnumerableScope, CoraxSpatialPointEntryListForEnumerableScope, BlittableJsonReaderObjectsListForEnumerableScope, _allocator);
                }
               
                foreach (var item in iterator)
                {
                    InsertRegularField(field, item, indexContext, ref entryWriter, enumerableWriterScope);
                }

                if (canFinishEnumerableWriting)
                {
                    enumerableWriterScope.Finish(field.Id, ref entryWriter);
                }
                
                return;

            case ValueType.DynamicJsonObject:
                if (field.Indexing is not FieldIndexing.No) 
                    AssertOrAdjustIndexingOptionForComplexObject(field);

                var dynamicJson = (DynamicBlittableJson)value;
                scope.Write(field.Id, dynamicJson.BlittableJson, ref entryWriter);
                return;

            case ValueType.ConvertToJson:
                var val = TypeConverter.ToBlittableSupportedType(value);
                if (val is not DynamicJsonValue json)
                {
                    InsertRegularField(field, val, indexContext, ref entryWriter, scope, nestedArray);
                    return;
                }

                if (field.Indexing is not FieldIndexing.No) 
                    AssertOrAdjustIndexingOptionForComplexObject(field);

                var jsonScope = Scope.CreateJson(json, indexContext);
                scope.Write(field.Id, jsonScope, ref entryWriter);
                return;

            case ValueType.BlittableJsonObject:
                HandleObject((BlittableJsonReaderObject)value, field, indexContext, ref entryWriter, scope);
                return;

            case ValueType.DynamicNull:       
                var dynamicNull = (DynamicNullObject)value;
                if (dynamicNull.IsExplicitNull || _indexImplicitNull)
                {
                    scope.WriteNull(field.Id, ref entryWriter);
                }
                return;

            case ValueType.Null:
                scope.WriteNull(field.Id, ref entryWriter);
                return;
            case ValueType.BoostedValue:
                //todo maciej
                //https://issues.hibernatingrhinos.com/issue/RavenDB-18146
                throw new NotSupportedException("Boosting in index is not supported by Corax. You can do it during querying or change index type into Lucene.");
            case ValueType.EmptyString:
                scope.Write(field.Id, ReadOnlySpan<byte>.Empty, ref entryWriter);
                return;
            case ValueType.CoraxSpatialPointEntry:
                scope.Write(field.Id, (CoraxSpatialPointEntry)value, ref entryWriter);
                return;
            case ValueType.Stream:
                throw new NotImplementedException();
            case ValueType.Lucene:
                throw new NotSupportedException("The Lucene value type is not supportes by Corax. You can change index type into Lucene.");
            default:
                throw new NotImplementedException();
        }

    }

    private void AssertOrAdjustIndexingOptionForComplexObject(IndexField field)
    {
        Debug.Assert(field.Indexing != FieldIndexing.No, "field.Indexing != FieldIndexing.No");

        if (_index.GetIndexDefinition().Fields.TryGetValue(field.Name, out var fieldFromDefinition) &&
            fieldFromDefinition.Indexing != FieldIndexing.No)
        {
            ThrowIndexingComplexObjectNotSupported(field, _index.Type);
        }

        DisableIndexingForComplexObject(field);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void HandleObject(BlittableJsonReaderObject val, IndexField field, JsonOperationContext indexContext, ref IndexEntryWriter entryWriter,
        IWriterScope scope, bool nestedArray = false)
    {
        if (val.TryGetMember(RavenConstants.Json.Fields.Values, out var values) &&
            IsArrayOfTypeValueObject(val))
        {
            InsertRegularField(field, (IEnumerable)values, indexContext, ref entryWriter, scope, nestedArray);
            return;
        }
        
        if (field.Indexing is not FieldIndexing.No) 
            AssertOrAdjustIndexingOptionForComplexObject(field);

        _knownFields.GetByFieldId(field.Id).Analyzer = null;
        scope.Write(field.Id, val, ref entryWriter);
    }

    private void DisableIndexingForComplexObject(IndexField field)
    {
        field.Indexing = FieldIndexing.No;
        if (_knownFields.TryGetByFieldId(field.Id, out var binding))
        {
            binding.OverrideFieldIndexingMode(FieldIndexingMode.No);
        }
    }

    internal static void ThrowIndexingComplexObjectNotSupported(IndexField field, IndexType indexType)
    {
        var fieldName = field.OriginalName ?? field.Name;

        string exceptionMessage;

        if (indexType.IsStatic())
        {
            exceptionMessage =
                $"The value of '{fieldName}' field is a complex object. Indexing it as a text isn't supported and it's supposed to have \\\"Indexing\\\" option set to \\\"No\\\". " +
                $"Note that you can still store it and use it in projections.{Environment.NewLine}" +
                "If you need to use it for searching purposes, you have to call ToString() on the field value in the index definition.";
        }
        else
        {
            exceptionMessage =
                $"The value of '{fieldName}' field is a complex object. Indexing it as a text isn't supported. You should consider querying on individual fields of that object.";
        }
        throw new NotSupportedException(exceptionMessage);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static FieldIndexingMode TranslateRavenFieldIndexingIntoCoraxFieldIndexingMode(FieldIndexing indexing) => indexing switch
    {
        FieldIndexing.No => FieldIndexingMode.No,
        FieldIndexing.Exact => FieldIndexingMode.Exact,
        FieldIndexing.Search => FieldIndexingMode.Search,
        _ => FieldIndexingMode.Normal,
    };
    
    public override void Dispose()
    {
        Scope?.Dispose();
    }
}
