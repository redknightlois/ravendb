using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using Voron.Data.RoaringBitmaps;
using Sparrow;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Graphs;
using Voron.Util;

namespace Corax.Querying;

public partial class IndexSearcher
{
    internal static class VectorSearchUtils
    {
        public static bool ShouldScan(IndexSearcher indexSearcher, long filterMatchesCount, bool isExact, IQueryMatch filterQuery, int scanningThreshold, int numberOfCandidates)
        {
            var shouldScan = filterQuery != null && (filterMatchesCount < scanningThreshold || isExact || filterMatchesCount * 0.5 < numberOfCandidates);
            if (indexSearcher._testingConfiguration is {DisableVectorSearchScanning: true})
                return false;
             
            return shouldScan;
        }

        public static RoaringBitmap LoadFilterMatches(IndexSearcher indexSearcher, ref IQueryMatch query, out bool owned)
        {
            if (query is IBitmapQueryMatch bitmapMatch) // fast path, when source already has a bitmap
            {
                owned = false;
                ref RoaringBitmap borrowed = ref bitmapMatch.BitmapState;
                borrowed.PrepareForReading();
                return borrowed;
            }

            owned = true;
            RoaringBitmap filter = new(indexSearcher.Allocator);

            using var _ = indexSearcher.Allocator.Allocate(4096, out Span<long> workingBuffer);
            int read;
            while ((read = query.Fill(workingBuffer)) > 0)
            {
                for (int i = 0; i < read; i++)
                    filter.Add(workingBuffer[i]);
            }

            filter.PrepareForReading();
            return filter;
        }

        public static bool TryConvertDocumentsIdsToNodesIds(IndexSearcher indexSearcher, in FieldMetadata metadata, ref RoaringBitmap filterResults, out ContextBoundNativeList<long> nodesIdsToScan)
        {
            var searchState = new Hnsw.SearchState(indexSearcher.Transaction.LowLevelTransaction, metadata.FieldName);
            indexSearcher._transaction.TryGetCompactTreeFor(Hnsw.VectorsIdByHashSlice, out var vectorsByHash);
            var nodesByVectorId = searchState.NodeIdsByVectorId;
            if (indexSearcher.TryGetRootPageByFieldName(metadata.FieldName, out var vectorRootPage) is false)
            {
                nodesIdsToScan = default;
                return false;
            }

            nodesIdsToScan = new ContextBoundNativeList<long>(indexSearcher.Allocator);

            Page p = default;
            using var iterator = filterResults.GetIterator();
            Span<long> batch = stackalloc long[QueryPrimitives.EntryScanBatchSize];
            int read;
            while ((read = iterator.Fill(ref filterResults, batch)) > 0)
            {
                for (int i = 0; i < read; i++)
                {
                    var entryTermsReader = indexSearcher.GetEntryTermsReader(batch[i], ref p);
                    while (entryTermsReader.FindNextStored(vectorRootPage))
                    {
                        var vectorHash = entryTermsReader.StoredField.Value;
                        if (vectorsByHash.TryGetValue(vectorHash, out var vectorId))
                        {
                            if (nodesByVectorId.TryGetValue(vectorId, out var nodeId))
                                nodesIdsToScan.Add(nodeId);
                        }
                    }
                }
            }
            // we get _all_ the node ids, so we can then use exact nearest neighbour to find the closest matches 
            var uniqueCount = Sorting.SortAndRemoveDuplicates(nodesIdsToScan.ToSpan());
            nodesIdsToScan.Count = uniqueCount;

            if (nodesIdsToScan.Count == 0)
            {
                nodesIdsToScan.Dispose();
                nodesIdsToScan = default;
                return false;
            }

            return nodesIdsToScan.Count > 0;
        }
    }


    public VectorSearchMatch VectorSearch(in FieldMetadata metadata, in VectorValue vectorValue, float minimumMatch, in int numberOfCandidates, bool isExact, bool sortByScore, IQueryMatch filterQuery = null, int scanningThreshold = 1024)
    {
        return new VectorSearchMatch(this, metadata, vectorValue, minimumMatch, numberOfCandidates, isExact, sortByScore, filterQuery, scanningThreshold);
    }

    public IQueryMatch VectorSearch(in FieldMetadata metadata, in string documentId, float minimumMatch, in int numberOfCandidates, bool isExact, bool sortByScore, IQueryMatch filterQuery = null, int scanningThreshold = 1024)
    {
        var idField = GetTermsFor(_fieldMapping.GetByFieldId(Constants.IndexWriter.PrimaryKeyFieldId).FieldName);
        string loweredDocumentId = documentId.ToLowerInvariant();
        if (idField == null ||
            idField.TryGetValue(loweredDocumentId, out var rawId) is false ||
            TryGetRootPageByFieldName(metadata.FieldName, out var vectorRootPage) is false ||
            _transaction.TryGetCompactTreeFor(Hnsw.VectorsIdByHashSlice, out var vectorsByHash) is false)
            return EmptyMatch();
        PortableExceptions.ThrowIf<InvalidOperationException>((rawId & (long)TermIdMask.EnsureIsSingleMask) != (long)TermIdMask.Single,
            "The provided id must be a document id mapped to a single value, but got: " + documentId + ", which maps to: " + rawId);

        Page page = default;
        var singleEntryId = EntryIdEncodings.GetContainerId(rawId);
        var reader = GetEntryTermsReader((long)singleEntryId, ref page);

        var searchState = new Hnsw.SearchState(_transaction.LowLevelTransaction, metadata.FieldName);

        List<VectorValue> vectors = [];
        while (reader.FindNextStored(vectorRootPage))
        {
            PortableExceptions.ThrowIf<InvalidOperationException>(reader.IsVectorHash is false, "Expected vector field, but got " + metadata.FieldName + ", which isn't a vector");

            if (vectorsByHash.TryGetValue(reader.StoredField.Value.ToSpan(), out var vectorId) is false)
                continue;

            var vectorSpan = Hnsw.NodeReader.ReadVector(vectorId, searchState);
            vectors.Add(new VectorValue(null, vectorSpan.AsMemory()));
        }

        return vectors.Count switch
        {
            0 => EmptyMatch(),
            1 => new VectorSearchMatch(this, metadata, vectors[0], minimumMatch, numberOfCandidates, isExact, sortByScore, filterQuery, scanningThreshold),
            _ => new MultiVectorSearchMatch(this, metadata, vectors.ToArray(), minimumMatch, numberOfCandidates, isExact, sortByScore, filterQuery, scanningThreshold)
        };
    }

    public MultiVectorSearchMatch MultiVectorSearch(in FieldMetadata metadata, in VectorValue[] vectorValues, float minimumMatch, in int numberOfCandidates, bool isExact, bool sortByScore, IQueryMatch filterQuery = null, int scanningThreshold = 1024)
        => new(this, metadata, vectorValues, minimumMatch, numberOfCandidates, isExact, sortByScore, filterQuery, scanningThreshold);
}
