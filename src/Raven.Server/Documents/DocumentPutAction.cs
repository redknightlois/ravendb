using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Changes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;
using System.Linq;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Sparrow.Server;
using static Raven.Server.Documents.DocumentsStorage;
using Constants = Raven.Client.Constants;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using static Raven.Server.Documents.Schemas.Documents;

namespace Raven.Server.Documents
{
    public unsafe class DocumentPutAction
    {
        private readonly DocumentsStorage _documentsStorage;
        private readonly DocumentDatabase _documentDatabase;
        private readonly IRecreationType[] _recreationTypes;

        public DocumentPutAction(DocumentsStorage documentsStorage, DocumentDatabase documentDatabase)
        {
            _documentsStorage = documentsStorage;
            _documentDatabase = documentDatabase;
            _recreationTypes = new IRecreationType[]
            {
                new RecreateAttachments(documentsStorage),
                new RecreateCounters(documentsStorage),
                new RecreateTimeSeries(documentsStorage),
            };
        }

        public void Recreate<T>(DocumentsOperationContext context, string docId) where T : IRecreationType
        {
            var doc = _documentsStorage.Get(context, docId);
            if (doc?.Data == null)
                return;

            var type = _recreationTypes.Single(t => t.GetType() == typeof(T));

            using (doc.Data)
            {
                PutDocument(context, docId, expectedChangeVector: null, document: doc.Data, nonPersistentFlags: type.ResolveConflictFlag);
            }
        }

        private readonly struct CompareClusterTransactionId
        {
            private readonly ServerStore _serverStore;
            private readonly DocumentPutAction _parent;

            public CompareClusterTransactionId(DocumentPutAction parent)
            {
                _serverStore = parent._documentDatabase.ServerStore;
                _parent = parent;
            }

            public void ValidateAtomicGuard(string id, NonPersistentDocumentFlags nonPersistentDocumentFlags, string changeVector)
            {
                if (nonPersistentDocumentFlags != NonPersistentDocumentFlags.None) // replication or engine running an operation, we can skip checking it 
                    return;

                if (_parent._documentDatabase.ClusterTransactionId == null)
                    return;

                long indexFromChangeVector = ChangeVectorUtils.GetEtagById(changeVector, _parent._documentDatabase.ClusterTransactionId);
                if (indexFromChangeVector == 0)
                    return;

                using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext clusterContext))
                using (clusterContext.OpenReadTransaction())
                {
                    var guardId = CompareExchangeKey.GetStorageKey(_parent._documentDatabase.Name, ClusterTransactionCommand.GetAtomicGuardKey(id));
                    var (indexFromCluster, val) = _serverStore.Cluster.GetCompareExchangeValue(clusterContext, guardId);
                    if (indexFromChangeVector != indexFromCluster)
                    {
                        throw new ConcurrencyException(
                            $"Cannot PUT document '{id}' because its change vector's cluster transaction index is set to {indexFromChangeVector} " +
                            $"but the compare exchange guard ('{ClusterTransactionCommand.GetAtomicGuardKey(id)}') is {(val == null ? "missing" : $"set to {indexFromCluster}")}")
                        {
                            Id = id
                        };
                    }
                }
            }
        }

        public virtual void ValidateId(Slice lowerId)
        {

        }

        public PutOperationResults PutDocument(DocumentsOperationContext context, string id,
            string expectedChangeVector,
            BlittableJsonReaderObject document,
            long? lastModifiedTicks = null,
            ChangeVector changeVector = null,
            string oldChangeVectorForClusterTransactionIndexCheck = null,
            DocumentFlags flags = DocumentFlags.None,
            NonPersistentDocumentFlags nonPersistentFlags = NonPersistentDocumentFlags.None)
        {
            if (context.Transaction == null)
            {
                ThrowRequiresTransaction();
                return default; // never hit
            }

            var documentDebugHash = 0UL;
            ValidateDocument(id, document, ref documentDebugHash);

            var newEtag = _documentsStorage.GenerateNextEtag();
            var modifiedTicks = _documentsStorage.GetOrCreateLastModifiedTicks(lastModifiedTicks);

            var compareClusterTransaction = new CompareClusterTransactionId(this);
            if (oldChangeVectorForClusterTransactionIndexCheck != null)
            {
                compareClusterTransaction.ValidateAtomicGuard(id, nonPersistentFlags, oldChangeVectorForClusterTransactionIndexCheck);
            }

            id = BuildDocumentId(id, newEtag, out bool knownNewId);
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idPtr))
            {
                if (flags.HasFlag(DocumentFlags.FromResharding) == false)
                    ValidateId(lowerId);

                var collectionName = _documentsStorage.ExtractCollectionName(context, document);
                var table = context.Transaction.InnerTransaction.OpenTable(_documentDatabase.GetDocsSchemaForCollection(collectionName), collectionName.GetTableName(CollectionTableType.Documents));

                var oldValue = default(TableValueReader);
                if (knownNewId == false)
                {
                    // delete a tombstone if it exists, if it known that it is a new ID, no need, so we can skip it
                    DeleteTombstoneIfNeeded(context, collectionName, lowerId.Content.Ptr, lowerId.Size);

                    table.ReadByKey(lowerId, out oldValue);
                }

                BlittableJsonReaderObject oldDoc = null;
                string oldChangeVector = "";
                if (oldValue.Pointer == null)
                {
                    // expectedChangeVector being null means we don't care, 
                    // and empty means that it must be new
                    if (string.IsNullOrEmpty(expectedChangeVector) == false)
                        ThrowConcurrentExceptionOnMissingDoc(id, expectedChangeVector);
                }
                else
                {
                    // expectedChangeVector  has special meaning here
                    // null - means, don't care, don't check
                    // "" / empty - means, must be new
                    // anything else - must match exactly

                    oldChangeVector = TableValueToChangeVector(context, (int)DocumentsTable.ChangeVector, ref oldValue);
                    if (expectedChangeVector != null)
                    {
                        if (string.Compare(expectedChangeVector, oldChangeVector, StringComparison.Ordinal) != 0)
                            ThrowConcurrentException(id, expectedChangeVector, oldChangeVector);
                    }
                    if (oldChangeVectorForClusterTransactionIndexCheck == null)
                    {
                        compareClusterTransaction.ValidateAtomicGuard(id, nonPersistentFlags, oldChangeVector);
                    }

                    oldDoc = new BlittableJsonReaderObject(oldValue.Read((int)DocumentsTable.Data, out int oldSize), oldSize, context);
                    var oldCollectionName = _documentsStorage.ExtractCollectionName(context, oldDoc);
                    if (oldCollectionName != collectionName)
                        ThrowInvalidCollectionNameChange(id, oldCollectionName, collectionName);

                    var oldFlags = TableValueToFlags((int)DocumentsTable.Flags, ref oldValue);

                    if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication) == false)
                    {
                        flags = flags.Strip(DocumentFlags.FromReplication | DocumentFlags.FromResharding);

                        if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByAttachmentUpdate) ||
                            nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByCountersUpdate))
                            flags = flags.Strip(DocumentFlags.Reverted);

                        if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByAttachmentUpdate) == false &&
                            oldFlags.Contain(DocumentFlags.HasAttachments))
                        {
                            flags |= DocumentFlags.HasAttachments;
                        }

                        if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByCountersUpdate) == false &&
                            oldFlags.Contain(DocumentFlags.HasCounters))
                        {
                            flags |= DocumentFlags.HasCounters;
                        }

                        if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByTimeSeriesUpdate) == false &&
                            oldFlags.Contain(DocumentFlags.HasTimeSeries))
                        {
                            flags |= DocumentFlags.HasTimeSeries;
                        }

                        if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByEnforceRevisionConfiguration) == false &&
                            oldFlags.Contain(DocumentFlags.HasRevisions))
                        {
                            flags |= DocumentFlags.HasRevisions;
                        }
                    }
                }

                var result = BuildChangeVectorAndResolveConflicts(context, id, lowerId, newEtag, document, changeVector, expectedChangeVector, flags, oldValue);

                nonPersistentFlags |= result.NonPersistentFlags;

                if (UpdateLastDatabaseChangeVector(context, result.ChangeVector, flags, nonPersistentFlags))
                    changeVector = result.ChangeVector;

                if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.Resolved))
                {
                    flags |= DocumentFlags.Resolved;
                }

                if (collectionName.IsHiLo == false && flags.Contain(DocumentFlags.Artificial) == false)
                {
                    Recreate(context, id, oldDoc, ref document, ref flags, nonPersistentFlags, ref documentDebugHash);

                    var shouldVersion = _documentDatabase.DocumentsStorage.RevisionsStorage.ShouldVersionDocument(
                        collectionName, nonPersistentFlags, oldDoc, document, context, id, lastModifiedTicks, ref flags, out var configuration);

                    if (shouldVersion)
                    {
                        if (_documentDatabase.DocumentsStorage.RevisionsStorage.ShouldVersionOldDocument(context, flags, oldDoc, oldChangeVector, collectionName))
                        {
                            var oldFlags = TableValueToFlags((int)DocumentsTable.Flags, ref oldValue);
                            var oldTicks = TableValueToDateTime((int)DocumentsTable.LastModified, ref oldValue);

                            _documentDatabase.DocumentsStorage.RevisionsStorage.Put(context, id, oldDoc, oldFlags | DocumentFlags.HasRevisions | DocumentFlags.FromOldDocumentRevision, NonPersistentDocumentFlags.None,
                                oldChangeVector, oldTicks.Ticks, configuration, collectionName);
                        }

                        flags |= DocumentFlags.HasRevisions;
                        _documentDatabase.DocumentsStorage.RevisionsStorage.Put(context, id, document, flags, nonPersistentFlags, changeVector.AsString(), modifiedTicks, configuration, collectionName);
                    }
                }

                FlagsProperlySet(flags, changeVector.AsString());
                using (Slice.From(context.Allocator, changeVector.AsString(), out var cv))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(lowerId);
                    tvb.Add(Bits.SwapBytes(newEtag));
                    tvb.Add(idPtr);
                    tvb.Add(document.BasePointer, document.Size);
                    tvb.Add(cv.Content.Ptr, cv.Size);
                    tvb.Add(modifiedTicks);
                    tvb.Add((int)flags);
                    tvb.Add(context.GetTransactionMarker());

                    if (oldValue.Pointer == null)
                    {
                        table.Insert(tvb);
                    }
                    else
                    {
                        table.Update(oldValue.Id, tvb);
                    }
                }

                if (collectionName.IsHiLo == false)
                {
                    _documentsStorage.ExpirationStorage.Put(context, lowerId, document);
                }

                _documentDatabase.Metrics.Docs.PutsPerSec.MarkSingleThreaded(1);
                _documentDatabase.Metrics.Docs.BytesPutsPerSec.MarkSingleThreaded(document.Size);

                context.Transaction.AddAfterCommitNotification(new DocumentChange
                {
                    ChangeVector = changeVector.AsString(),
                    CollectionName = collectionName.Name,
                    Id = id,
                    Type = DocumentChangeTypes.Put,
                });

                ValidateDocumentHash(id, document, documentDebugHash);
                ValidateDocument(id, document, ref documentDebugHash);

                return new PutOperationResults
                {
                    Etag = newEtag,
                    Id = id,
                    Collection = collectionName,
                    ChangeVector = changeVector.AsString(),
                    Flags = flags,
                    LastModified = new DateTime(modifiedTicks, DateTimeKind.Utc)
                };
            }
        }

        [Conditional("DEBUG")]
        private static void ValidateDocument(string id, BlittableJsonReaderObject document, ref ulong documentDebugHash)
        {
            document.BlittableValidation();
            BlittableJsonReaderObject.AssertNoModifications(document, id, assertChildren: true);
            AssertMetadataWasFiltered(document);
            documentDebugHash = document.DebugHash;
        }

        [Conditional("DEBUG")]
        private static void ValidateDocumentHash(string id, BlittableJsonReaderObject document, ulong documentDebugHash)
        {
            if (document.DebugHash != documentDebugHash)
            {
                throw new InvalidDataException("The incoming document " + id + " has changed _during_ the put process, " +
                                               "this is likely because you are trying to save a document that is already stored and was moved");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (ChangeVector ChangeVector, NonPersistentDocumentFlags NonPersistentFlags) BuildChangeVectorAndResolveConflicts(
            DocumentsOperationContext context, string id, Slice lowerId, long newEtag,
            BlittableJsonReaderObject document, ChangeVector changeVector, string expectedChangeVector, DocumentFlags flags, TableValueReader oldValue)
        {
            var nonPersistentFlags = NonPersistentDocumentFlags.None;
            var fromReplication = flags.Contain(DocumentFlags.FromReplication);

            if (_documentsStorage.ConflictsStorage.ConflictsCount != 0)
            {
                // Since this document resolve the conflict we don't need to alter the change vector.
                // This way we avoid another replication back to the source

                _documentsStorage.ConflictsStorage.ThrowConcurrencyExceptionOnConflictIfNeeded(context, lowerId, expectedChangeVector);

                if (fromReplication)
                {
                    nonPersistentFlags = _documentsStorage.ConflictsStorage.DeleteConflictsFor(context, id, document).NonPersistentFlags;
                }
                else
                {
                    var result = _documentsStorage.ConflictsStorage.MergeConflictChangeVectorIfNeededAndDeleteConflicts(changeVector, context, id, newEtag, document);
                    nonPersistentFlags = result.NonPersistentFlags;

                    if (result.ChangeVector != null)
                        return (result.ChangeVector, nonPersistentFlags);
                }
            }

            if (changeVector != null)
                return (changeVector, nonPersistentFlags);

            var oldChangeVector = oldValue.Pointer != null ? TableValueToChangeVector(context, (int)DocumentsTable.ChangeVector, ref oldValue) : null;
            if (fromReplication == false)
            {
                context.LastDatabaseChangeVector ??= GetDatabaseChangeVector(context);
                oldChangeVector = oldChangeVector == null
                    ? context.GetChangeVector(context.LastDatabaseChangeVector)
                    : oldChangeVector.MergeWith(context.LastDatabaseChangeVector, context);
            }

            changeVector = SetDocumentChangeVectorForLocalChange(context, lowerId, oldChangeVector, newEtag);
            context.SkipChangeVectorValidation = changeVector.TryRemoveIds(_documentsStorage.UnusedDatabaseIds, context, out changeVector);
            return (changeVector, nonPersistentFlags);
        }

        private static bool UpdateLastDatabaseChangeVector(DocumentsOperationContext context, ChangeVector changeVector, DocumentFlags flags, NonPersistentDocumentFlags nonPersistentFlags)
        {
            // if arrived from replication we keep the document with its original change vector
            // in that case the updating of the global change vector should happened upper in the stack
            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication))
                return false;

            var currentGlobalChangeVector = context.LastDatabaseChangeVector ?? GetDatabaseChangeVector(context);

            var clone = context.GetChangeVector(changeVector);
            clone = clone.StripSinkTags(currentGlobalChangeVector, context);

            // this is raft created document, so it must contain only the RAFT element 
            if (flags.Contain(DocumentFlags.FromClusterTransaction))
            {
                context.LastDatabaseChangeVector = ChangeVector.Merge(currentGlobalChangeVector, clone.Order, context);
                return false;
            }

            // the resolved document must preserve the original change vector (without the global change vector) to avoid ping-pong replication.
            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromResolver))
            {
                context.LastDatabaseChangeVector = ChangeVector.Merge(currentGlobalChangeVector, clone.Order, context);
                return false;
            }

            context.LastDatabaseChangeVector = clone.Order;
            return true;
        }

        protected virtual void CalculateSuffixForIdentityPartsSeparator(string id, ref char* idSuffixPtr, ref int idSuffixLength, ref int idLength)
        {
        }

        protected virtual void WriteSuffixForIdentityPartsSeparator(ref char* valueWritePosition, char* idSuffixPtr, int idSuffixLength)
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string BuildDocumentId(string id, long newEtag, out bool knownNewId)
        {
            if (string.IsNullOrWhiteSpace(id))
            {
                knownNewId = true;
                id = Guid.NewGuid().ToString();
            }
            else
            {
                // We use if instead of switch so the JIT will better inline this method
                var lastChar = id[^1];
                if (lastChar == '|')
                {
                    ThrowInvalidDocumentId(id);
                }

                fixed (char* idPtr = id)
                {
                    var idSuffixPtr = idPtr;
                    var idLength = id.Length;
                    var idSuffixLength = 0;

                    if (lastChar == _documentDatabase.IdentityPartsSeparator)
                    {
                        CalculateSuffixForIdentityPartsSeparator(id, ref idSuffixPtr, ref idSuffixLength, ref idLength);

                        string nodeTag = _documentDatabase.ServerStore.NodeTag;

                        // PERF: we are creating an string and mutating it for performance reasons.
                        //       while nasty this shouldn't have any side effects because value didn't
                        //       escape yet the function, so while not pretty it works (and it's safe).      
                        //       
                        int valueLength = idLength + 1 + 19 + nodeTag.Length + idSuffixLength;
                        string value = new('0', valueLength);
                        fixed (char* valuePtr = value)
                        {
                            char* valueWritePosition = valuePtr + value.Length;

                            WriteSuffixForIdentityPartsSeparator(ref valueWritePosition, idSuffixPtr, idSuffixLength);

                            valueWritePosition -= nodeTag.Length;
                            for (int j = 0; j < nodeTag.Length; j++)
                                valueWritePosition[j] = nodeTag[j];

                            int i;
                            for (i = 0; i < idLength; i++)
                                valuePtr[i] = id[i];

                            i += 19;
                            valuePtr[i] = '-';

                            Format.Backwards.WriteNumber(valuePtr + i - 1, (ulong)newEtag);
                        }

                        id = value;

                        knownNewId = true;
                    }
                    else
                    {
                        knownNewId = false;
                    }
                }
            }

            // Intentionally have just one return statement here for better inlining
            return id;
        }

        private static void ThrowInvalidDocumentId(string id)
        {
            throw new NotSupportedException("Document ids cannot end with '|', but was called with " + id +
                                            ". Identities are only generated for external requests, not calls to PutDocument and such.");
        }

        private void Recreate(DocumentsOperationContext context, string id, BlittableJsonReaderObject oldDoc,
            ref BlittableJsonReaderObject document, ref DocumentFlags flags, NonPersistentDocumentFlags nonPersistentFlags, ref ulong documentDebugHash)
        {
            for (int i = 0; i < _recreationTypes.Length; i++)
            {
                var type = _recreationTypes[i];
                if (RecreateIfNeeded(context, id, oldDoc, document, ref flags, nonPersistentFlags, type))
                {
                    ValidateDocumentHash(id, document, documentDebugHash);

                    document = context.ReadObject(document, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    ValidateDocument(id, document, ref documentDebugHash);
#if DEBUG
                    type.Assert(id, document, flags);
#endif
                }
            }
        }


        private bool RecreateIfNeeded(DocumentsOperationContext context, string docId, BlittableJsonReaderObject oldDoc,
            BlittableJsonReaderObject document, ref DocumentFlags flags, NonPersistentDocumentFlags nonPersistentFlags, IRecreationType type)
        {
            BlittableJsonReaderObject metadata;
            BlittableJsonReaderArray current, old = null;

            if (nonPersistentFlags.Contain(type.ResolveConflictFlag))
            {
                TryGetMetadata(document, type, out metadata, out current);
                return RecreatePreserveCasing(current, ref flags);
            }

            if (flags.Contain(type.HasFlag) == false ||
                nonPersistentFlags.Contain(type.ByUpdateFlag) ||
                nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication))
                return false;

            if (oldDoc == null ||
                oldDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject oldMetadata) == false ||
                oldMetadata.TryGet(type.MetadataProperty, out old) == false)
                return false;

            // Make sure the user did not changed the value of @attachments in the @metadata
            // In most cases it won't be changed so we can use this value 
            // instead of recreating the document's blittable from scratch

            if (TryGetMetadata(document, type, out metadata, out current) == false ||
                current.Equals(old) == false)
            {
                return RecreatePreserveCasing(current, ref flags);
            }

            return false;

            bool RecreatePreserveCasing(BlittableJsonReaderArray currentMetadata, ref DocumentFlags documentFlags)
            {
                if ((type is RecreateTimeSeries || type is RecreateCounters) &&
                    currentMetadata == null && old != null)
                {
                    // use the '@counters'/'@timeseries' from old document's metadata

                    if (metadata == null)
                    {
                        document.Modifications = new DynamicJsonValue(document)
                        {
                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                            {
                                [type.MetadataProperty] = old
                            }
                        };
                    }
                    else
                    {
                        metadata.Modifications = new DynamicJsonValue(metadata)
                        {
                            [type.MetadataProperty] = old
                        };
                        document.Modifications = new DynamicJsonValue(document)
                        {
                            [Constants.Documents.Metadata.Key] = metadata
                        };
                    }

                    return true;
                }

                var values = type.GetMetadata(context, docId);

                if (values.Count == 0)
                {
                    if (metadata != null && metadata.TryGetMember(type.MetadataProperty, out _))
                    {
                        metadata.Modifications = new DynamicJsonValue(metadata);
                        metadata.Modifications.Remove(type.MetadataProperty);
                        document.Modifications = new DynamicJsonValue(document)
                        {
                            [Constants.Documents.Metadata.Key] = metadata
                        };
                    }

                    documentFlags &= ~type.HasFlag;
                    return true;
                }

                documentFlags |= type.HasFlag;

                if (metadata == null)
                {
                    document.Modifications = new DynamicJsonValue(document)
                    {
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [type.MetadataProperty] = values
                        }
                    };
                }
                else
                {
                    metadata.Modifications = new DynamicJsonValue(metadata)
                    {
                        [type.MetadataProperty] = values
                    };
                    document.Modifications = new DynamicJsonValue(document)
                    {
                        [Constants.Documents.Metadata.Key] = metadata
                    };
                }

                return true;
            }
        }

        private static bool TryGetMetadata(BlittableJsonReaderObject document, IRecreationType type, out BlittableJsonReaderObject metadata, out BlittableJsonReaderArray current)
        {
            current = null;
            metadata = null;

            return document.TryGet(Constants.Documents.Metadata.Key, out metadata) &&
                   metadata.TryGet(type.MetadataProperty, out current);
        }

        public interface IRecreationType
        {
            string MetadataProperty { get; }

            DynamicJsonArray GetMetadata(DocumentsOperationContext context, string id);

            DocumentFlags HasFlag { get; }

            NonPersistentDocumentFlags ResolveConflictFlag { get; }
            NonPersistentDocumentFlags ByUpdateFlag { get; }

            Action<string, BlittableJsonReaderObject, DocumentFlags> Assert { get; }
        }

        private class RecreateAttachments : IRecreationType
        {
            private readonly DocumentsStorage _storage;

            public RecreateAttachments(DocumentsStorage storage)
            {
                _storage = storage;
            }

            public string MetadataProperty => Constants.Documents.Metadata.Attachments;

            public DynamicJsonArray GetMetadata(DocumentsOperationContext context, string id)
            {
                return _storage.AttachmentsStorage.GetAttachmentsMetadataForDocument(context, id);
            }

            public DocumentFlags HasFlag => DocumentFlags.HasAttachments;

            public NonPersistentDocumentFlags ResolveConflictFlag => NonPersistentDocumentFlags.ResolveAttachmentsConflict;

            public NonPersistentDocumentFlags ByUpdateFlag => NonPersistentDocumentFlags.ByAttachmentUpdate;

            public Action<string, BlittableJsonReaderObject, DocumentFlags> Assert => (id, o, flags) =>
                _storage.AssertMetadataKey(id, o, flags, DocumentFlags.HasAttachments, Constants.Documents.Metadata.Attachments);
        }

        public class RecreateCounters : IRecreationType
        {
            private readonly DocumentsStorage _storage;

            public RecreateCounters(DocumentsStorage storage)
            {
                _storage = storage;
            }

            public string MetadataProperty => Constants.Documents.Metadata.Counters;

            public DynamicJsonArray GetMetadata(DocumentsOperationContext context, string id)
            {
                return _storage.CountersStorage.GetCountersForDocumentList(context, id);
            }

            public DocumentFlags HasFlag => DocumentFlags.HasCounters;

            public NonPersistentDocumentFlags ResolveConflictFlag => NonPersistentDocumentFlags.ResolveCountersConflict;

            public NonPersistentDocumentFlags ByUpdateFlag => NonPersistentDocumentFlags.ByCountersUpdate;

            public Action<string, BlittableJsonReaderObject, DocumentFlags> Assert => (id, o, flags) =>
                _storage.AssertMetadataKey(id, o, flags, DocumentFlags.HasCounters, Constants.Documents.Metadata.Counters);
        }

        private class RecreateTimeSeries : IRecreationType
        {
            private readonly DocumentsStorage _storage;

            public RecreateTimeSeries(DocumentsStorage storage)
            {
                _storage = storage;
            }

            public string MetadataProperty => Constants.Documents.Metadata.TimeSeries;

            public DynamicJsonArray GetMetadata(DocumentsOperationContext context, string id)
            {
                return _storage.TimeSeriesStorage.GetTimeSeriesNamesForDocument(context, id);
            }

            public DocumentFlags HasFlag => DocumentFlags.HasTimeSeries;

            public NonPersistentDocumentFlags ResolveConflictFlag => NonPersistentDocumentFlags.ResolveTimeSeriesConflict;

            public NonPersistentDocumentFlags ByUpdateFlag => NonPersistentDocumentFlags.ByTimeSeriesUpdate;

            public Action<string, BlittableJsonReaderObject, DocumentFlags> Assert => (id, o, flags) =>
                _storage.AssertMetadataKey(id, o, flags, DocumentFlags.HasTimeSeries, Constants.Documents.Metadata.TimeSeries);
        }

        public static void ThrowRequiresTransaction([CallerMemberName] string caller = null)
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentException("Context must be set with a valid transaction before calling " + caller, "context");
        }

        private static void ThrowConcurrentExceptionOnMissingDoc(string id, string expectedChangeVector)
        {
            throw new ConcurrencyException(
                $"Document {id} does not exist, but Put was called with change vector: {expectedChangeVector}. Optimistic concurrency violation, transaction will be aborted.")
            {
                Id = id,
                ExpectedChangeVector = expectedChangeVector
            };
        }

        private static void ThrowInvalidCollectionNameChange(string id, CollectionName oldCollectionName, CollectionName collectionName)
        {
            DocumentCollectionMismatchException.ThrowFor(id, oldCollectionName.Name, collectionName.Name);
        }

        private static void ThrowConcurrentException(string id, string expectedChangeVector, string oldChangeVector)
        {
            throw new ConcurrencyException(
                $"Document {id} has change vector {oldChangeVector}, but Put was called with {(expectedChangeVector.Length == 0 ? "expecting new document" : "change vector " + expectedChangeVector)}. Optimistic concurrency violation, transaction will be aborted.")
            {
                Id = id,
                ActualChangeVector = oldChangeVector,
                ExpectedChangeVector = expectedChangeVector
            };
        }

        private void DeleteTombstoneIfNeeded(DocumentsOperationContext context, CollectionName collectionName, byte* lowerId, int lowerSize)
        {
            var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(_documentsStorage.TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));
            if (tombstoneTable.NumberOfEntries == 0)
                return;

            using (Slice.External(context.Allocator, lowerId, lowerSize, out Slice id))
            {
                DeleteTombstone(tombstoneTable, id);
            }
        }

        public void DeleteTombstoneIfNeeded(DocumentsOperationContext context, CollectionName collectionName, Slice id)
        {
            var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(_documentsStorage.TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));
            if (tombstoneTable.NumberOfEntries == 0)
                return;

            DeleteTombstone(tombstoneTable, id);
        }

        private static void DeleteTombstone(Table tombstoneTable, Slice id)
        {
            foreach (var (tombstoneKey, tvh) in tombstoneTable.SeekByPrimaryKeyPrefix(id, Slices.Empty, 0))
            {
                if (IsTombstoneOfId(tombstoneKey, id) == false)
                    return;

                if (tombstoneTable.IsOwned(tvh.Reader.Id))
                {
                    tombstoneTable.Delete(tvh.Reader.Id);
                    return;
                }
            }
        }

        private ChangeVector SetDocumentChangeVectorForLocalChange(DocumentsOperationContext context, Slice lowerId, ChangeVector oldChangeVector, long newEtag)
        {
            if (oldChangeVector != null)
            {
                oldChangeVector = oldChangeVector.UpdateVersion(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, newEtag, context);
                oldChangeVector = oldChangeVector.UpdateOrder(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, newEtag, context);
                return oldChangeVector;
            }
            return context.GetChangeVector(_documentsStorage.ConflictsStorage.GetMergedConflictChangeVectorsAndDeleteConflicts(context, lowerId, newEtag));
        }

        [Conditional("DEBUG")]
        public static void AssertMetadataWasFiltered(BlittableJsonReaderObject data)
        {
            if (data == null)
                return;

            var originalNoCacheValue = data.NoCache;

            data.NoCache = true;

            try
            {
                if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                    return;

                var names = metadata.GetPropertyNames();
                if (names.Contains(Constants.Documents.Metadata.Id, StringComparer.OrdinalIgnoreCase) ||
                    names.Contains(Constants.Documents.Metadata.LastModified, StringComparer.OrdinalIgnoreCase) ||
                    names.Contains(Constants.Documents.Metadata.IndexScore, StringComparer.OrdinalIgnoreCase) ||
                    names.Contains(Constants.Documents.Metadata.ChangeVector, StringComparer.OrdinalIgnoreCase) ||
                    names.Contains(Constants.Documents.Metadata.Flags, StringComparer.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException("Document's metadata should filter properties on before put to storage." + Environment.NewLine + data);
                }
            }
            finally
            {
                data.NoCache = originalNoCacheValue;
            }
        }
    }
}
