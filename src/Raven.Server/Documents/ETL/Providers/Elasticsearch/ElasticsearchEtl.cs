﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
using Nest;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Enumerators;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Exceptions.ETL.ElasticSearch;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    public class ElasticSearchEtl : EtlProcess<ElasticSearchItem, ElasticSearchIndexWithRecords, ElasticSearchEtlConfiguration, ElasticSearchConnectionString, EtlStatsScope, EtlPerformanceOperation>
    {
        public readonly ElasticSearchEtlMetricsCountersManager ElasticSearchMetrics = new ElasticSearchEtlMetricsCountersManager();

        public ElasticSearchEtl(Transformation transformation, ElasticSearchEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
            : base(transformation, configuration, database, serverStore, ElasticSearchEtlTag)
        {
            Metrics = ElasticSearchMetrics;
        }

        public const string ElasticSearchEtlTag = "ElasticSearch ETL";

        public override EtlType EtlType => EtlType.ElasticSearch;

        public override bool ShouldTrackCounters() => false;

        public override bool ShouldTrackTimeSeries() => false;

        protected override bool ShouldTrackAttachmentTombstones() => false;

        private ElasticClient _client;

        protected override EtlStatsScope CreateScope(EtlRunStats stats)
        {
            return new EtlStatsScope(stats);
        }

        protected override bool ShouldFilterOutHiLoDocument() => true;

        protected override IEnumerator<ElasticSearchItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
        {
            return new DocumentsToElasticSearchItems(docs, collection);
        }

        protected override IEnumerator<ElasticSearchItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection,
            bool trackAttachments)
        {
            return new TombstonesToElasticSearchItems(tombstones, collection);
        }

        protected override IEnumerator<ElasticSearchItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones,
            List<string> collections)
        {
            throw new NotSupportedException("Attachment tombstones aren't supported by ElasticSearch ETL");
        }

        protected override IEnumerator<ElasticSearchItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters,
            string collection)
        {
            throw new NotSupportedException("Counters aren't supported by ElasticSearch ETL");
        }

        protected override IEnumerator<ElasticSearchItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries,
            string collection)
        {
            throw new NotSupportedException("Time series aren't supported by ElasticSearch ETL");
        }

        protected override IEnumerator<ElasticSearchItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context,
            IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
        {
            throw new NotSupportedException("Time series aren't supported by ElasticSearch ETL");
        }

        protected override EtlTransformer<ElasticSearchItem, ElasticSearchIndexWithRecords, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
        {
            return new ElasticSearchDocumentTransformer(Transformation, Database, context, Configuration);
        }

        protected override int LoadInternal(IEnumerable<ElasticSearchIndexWithRecords> records, DocumentsOperationContext context, EtlStatsScope scope)
        {
            int count = 0;

            _client ??= ElasticSearchHelper.CreateClient(Configuration.Connection);

            foreach (var index in records)
            {
                string indexName = index.IndexName.ToLower();

                if (_client.Indices.Exists(new IndexExistsRequest(Indices.Index(indexName))).Exists == false)
                {
                    var response = _client.Indices.Create(indexName, c => c
                        .Map(m => m
                            .Properties(p => p
                                .Keyword(t => t
                                    .Name(index.IndexIdProperty)))));

                    // TODO arek - check response status
                }

                if (index.InsertOnlyMode == false)
                {
                    count += DeleteByQueryOnIndexIdProperty(index);
                }

                foreach (ElasticSearchItem insert in index.Inserts)
                {
                    if (insert.Property == null) 
                        continue;

                    var response = _client.LowLevel.Index<StringResponse>(
                        index: indexName,
                        body: insert.Property.RawValue.ToString(), requestParameters: new IndexRequestParameters());

                    if (response.Success == false)
                    {
                        if (string.IsNullOrWhiteSpace(response.DebugInformation) == false)
                        {
                            throw new ElasticSearchLoadException($"Index {index} error: {response.DebugInformation}");
                        }

                        throw new ElasticSearchLoadException($"Index {index} error: {response.OriginalException}");
                    }

                    count++;
                }
            }

            return count;
        }

        private int DeleteByQueryOnIndexIdProperty(ElasticSearchIndexWithRecords index)
        {
            string indexName = index.IndexName.ToLower();

            var idsToDelete = new List<string>();

            foreach (ElasticSearchItem delete in index.Deletes)
            {
                idsToDelete.Add(delete.DocumentId);
            }

            // we are about to delete by query so we need to ensure that all documents are available for search
            // this way we won't skip just inserted documents that could not be indexed yet

            _client.Indices.Refresh(new RefreshRequest(Indices.Index(indexName)));

            var deleteResponse = _client.DeleteByQuery<string>(d => d
                .Index(indexName)
                .Query(q => q
                    .Terms(p => p
                        .Field(index.IndexIdProperty)
                        .Terms(idsToDelete))
                )
            );

            // The request made it to the server but something went wrong in ElasticSearch (query parsing exception, non-existent index, etc)
            if (deleteResponse.ServerError != null)
            {
                var message = $"Index {index}; Documents IDs: {string.Join(',', idsToDelete)}; Error: {deleteResponse.ServerError.Error}";

                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"{message}; Debug Information: {deleteResponse.DebugInformation}");
                }

                throw new ElasticSearchLoadException(message);
            }

            // ElasticSearch error occurred or a connection error (the server could not be reached, request timed out, etc)
            if (deleteResponse.OriginalException != null)
            {
                var message = $"Index {index}; Documents IDs: {string.Join(',', idsToDelete)}; Error: {deleteResponse.OriginalException}";

                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"{message}; Debug Information: {deleteResponse.DebugInformation}", deleteResponse.OriginalException);
                }

                throw new ElasticSearchLoadException(message);
            }

            return (int)deleteResponse.Deleted;
        }

        public ElasticSearchEtlTestScriptResult RunTest(IEnumerable<ElasticSearchIndexWithRecords> records)
        {
            var simulatedWriter = new ElasticSearchIndexWriterSimulator();
            var summaries = new List<IndexSummary>();
            
            foreach (var record in records)
            {
                var commands = simulatedWriter.SimulateExecuteCommandText(record);
                
                summaries.Add(new IndexSummary
                {
                    IndexName = record.IndexName.ToLower(),
                    Commands = commands.ToArray()
                });
            }
            
            return new ElasticSearchEtlTestScriptResult
            {
                TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList(),
                Summary = summaries
            };
        }
    }
}
