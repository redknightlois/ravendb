﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public sealed class GetDocumentsCommand : RavenCommand<GetDocumentsResult>
    {
        private readonly DocumentConventions _conventions;
        private readonly string _id;
        private readonly string[] _ids;
        private readonly string[] _includes;
        private readonly string[] _counters;
        private readonly bool _includeAllCounters;
        private TransactionMode _txMode;

        private readonly IEnumerable<AbstractTimeSeriesRange> _timeSeriesIncludes;
        private readonly IEnumerable<string> _revisionsIncludeByChangeVector;
        private readonly DateTime? _revisionsIncludeByDateTime;
        private readonly string[] _compareExchangeValueIncludes;
        private readonly bool _metadataOnly;

        private readonly string _startWith;
        private readonly string _matches;
        private readonly int? _start;
        private readonly int? _pageSize;
        private readonly string _exclude;
        private readonly string _startAfter;

        public GetDocumentsCommand(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public GetDocumentsCommand(DocumentConventions conventions, string id, string[] includes, bool metadataOnly)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _id = id ?? throw new ArgumentNullException(nameof(id));
            _includes = includes;
            _metadataOnly = metadataOnly;
        }

        public GetDocumentsCommand(DocumentConventions conventions, string[] ids, string[] includes, bool metadataOnly)
        {
            if (ids == null || ids.Length == 0)
                throw new ArgumentNullException(nameof(ids));

            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _ids = ids;
            _includes = includes;
            _metadataOnly = metadataOnly;
        }

        public GetDocumentsCommand(DocumentConventions conventions, string[] ids, string[] includes, string[] counterIncludes, IEnumerable<AbstractTimeSeriesRange> timeSeriesIncludes, string[] compareExchangeValueIncludes, bool metadataOnly)
            : this(conventions, ids, includes, metadataOnly)
        {
            _counters = counterIncludes;
            _timeSeriesIncludes = timeSeriesIncludes;
            _compareExchangeValueIncludes = compareExchangeValueIncludes;
        }
        
        public GetDocumentsCommand(DocumentConventions conventions, string[] ids, string[] includes, string[] counterIncludes, IEnumerable<string> revisionsIncludesByChangeVector, DateTime? revisionIncludeByDateTimeBefore, IEnumerable<AbstractTimeSeriesRange> timeSeriesIncludes, string[] compareExchangeValueIncludes, bool metadataOnly)
            : this(conventions, ids, includes, metadataOnly)
        {
            _counters = counterIncludes;
            _revisionsIncludeByChangeVector = revisionsIncludesByChangeVector;
            _revisionsIncludeByDateTime = revisionIncludeByDateTimeBefore;
            _timeSeriesIncludes = timeSeriesIncludes;
            _compareExchangeValueIncludes = compareExchangeValueIncludes;
        }

        public GetDocumentsCommand(DocumentConventions conventions, string[] ids, string[] includes, bool includeAllCounters, IEnumerable<AbstractTimeSeriesRange> timeSeriesIncludes, string[] compareExchangeValueIncludes, bool metadataOnly)
            : this(conventions, ids, includes, metadataOnly)
        {
            _includeAllCounters = includeAllCounters;
            _timeSeriesIncludes = timeSeriesIncludes;
            _compareExchangeValueIncludes = compareExchangeValueIncludes;
        }

        public GetDocumentsCommand(DocumentConventions conventions, string startWith, string startAfter, string matches, string exclude, int start, int pageSize, bool metadataOnly)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _startWith = startWith ?? throw new ArgumentNullException(nameof(startWith));
            _startAfter = startAfter;
            _matches = matches;
            _exclude = exclude;
            _start = start;
            _pageSize = pageSize;
            _metadataOnly = metadataOnly;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var pathBuilder = new StringBuilder(node.Url);
            pathBuilder.Append("/databases/")
                .Append(node.Database)
                .Append("/docs?");

            if (_txMode == TransactionMode.ClusterWide)
                pathBuilder.Append("&txMode=ClusterWide");
            if (_start.HasValue)
                pathBuilder.Append("&start=").Append(_start);
            if (_pageSize.HasValue)
                pathBuilder.Append("&pageSize=").Append(_pageSize);
            if (_metadataOnly)
                pathBuilder.Append("&metadataOnly=true");

            if (_startWith != null)
            {
                pathBuilder.Append("&startsWith=").Append(Uri.EscapeDataString(_startWith));

                if (_matches != null)
                    pathBuilder.Append("&matches=").Append(Uri.EscapeDataString(_matches));
                if (_exclude != null)
                    pathBuilder.Append("&exclude=").Append(Uri.EscapeDataString(_exclude));
                if (_startAfter != null)
                    pathBuilder.Append("&startAfter=").Append(Uri.EscapeDataString(_startAfter));
            }

            if (_includes != null)
            {
                foreach (var include in _includes)
                {
                    pathBuilder.Append("&include=").Append(Uri.EscapeDataString(include));
                }
            }

            if (_includeAllCounters)
            {
                pathBuilder.Append("&counter=").Append(Uri.EscapeDataString(Constants.Counters.All));
            }
            else if (_counters != null && _counters.Length > 0)
            {
                foreach (var counter in _counters)
                {
                    pathBuilder.Append("&counter=").Append(Uri.EscapeDataString(counter));
                }
            }

            if (_timeSeriesIncludes != null)
            {
                foreach (var tsInclude in _timeSeriesIncludes)
                {
                    switch (tsInclude)
                    {
                        case TimeSeriesRange range:
                            pathBuilder.Append("&timeseries=").Append(Uri.EscapeDataString(range.Name))
                                .Append("&from=").Append(range.From?.GetDefaultRavenFormat())
                                .Append("&to=").Append(range.To?.GetDefaultRavenFormat());
                            break;
                        case TimeSeriesTimeRange timeRange:
                            pathBuilder.Append("&timeseriestime=").Append(Uri.EscapeDataString(timeRange.Name))
                                .Append("&timeType=").Append(Uri.EscapeDataString(timeRange.Type.ToString()))
                                .Append("&timeValue=").Append(Uri.EscapeDataString($"{timeRange.Time.Value}"))
                                .Append("&timeUnit=").Append(Uri.EscapeDataString(timeRange.Time.Unit.ToString()));
                            break;
                        case TimeSeriesCountRange countRange:
                            pathBuilder.Append("&timeseriescount=").Append(Uri.EscapeDataString(countRange.Name))
                                .Append("&countType=").Append(Uri.EscapeDataString(countRange.Type.ToString()))
                                .Append("&countValue=").Append(Uri.EscapeDataString($"{countRange.Count}"));
                            break;
                        default:
                            throw new InvalidOperationException($"Unexpected TimesSeries range {tsInclude.GetType()}");
                    }
                }
            }

            if (_revisionsIncludeByChangeVector != null)
            {
                foreach (var changeVector in _revisionsIncludeByChangeVector)
                {
                    pathBuilder.Append("&revisions=").Append(Uri.EscapeDataString(changeVector));
                }
            }
            
            if (_revisionsIncludeByDateTime != null)
            {
                pathBuilder.Append("&revisionsBefore=").Append(Uri.EscapeDataString(_revisionsIncludeByDateTime.Value.GetDefaultRavenFormat()));
            }

            if (_compareExchangeValueIncludes != null)
            {
                foreach (var compareExchangeValue in _compareExchangeValueIncludes)
                    pathBuilder.Append("&cmpxchg=").Append(Uri.EscapeDataString(compareExchangeValue));
            }

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            if (_id != null)
            {
                pathBuilder.Append("&id=").Append(Uri.EscapeDataString(_id));
            }
            else if (_ids != null)
            {
                PrepareRequestWithMultipleIds(_conventions, pathBuilder, request, _ids, ctx);
            }

            url = pathBuilder.ToString();
            return request;
        }

        public static void PrepareRequestWithMultipleIds(DocumentConventions conventions, StringBuilder pathBuilder, HttpRequestMessage request, string[] ids, JsonOperationContext context)
        {
            var uniqueIds = new HashSet<string>(ids);

            // if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
            // we are fine with that, requests to load > 1024 items are going to be rare
            var isGet = uniqueIds.Sum(x => x?.Length ?? 0) < 1024;
            if (isGet)
            {
                uniqueIds.ApplyIfNotNull(id => pathBuilder.Append("&id=").Append(Uri.EscapeDataString(id ?? string.Empty)));
            }
            else
            {
                var calculateHash = CalculateHash(context, uniqueIds);
                pathBuilder.Append("&loadHash=").Append(calculateHash);
                request.Method = HttpMethod.Post;

                request.Content = new BlittableJsonContent(async stream =>
                {
                    using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
                    {
                        writer.WriteStartObject();
                        writer.WriteArray("Ids", uniqueIds);
                        writer.WriteEndObject();
                    }
                }, conventions);
            }
        }

        private static ulong CalculateHash(JsonOperationContext ctx, HashSet<string> uniqueIds)
        {
            using (var hasher = new HashCalculator(ctx))
            {
                foreach (var x in uniqueIds)
                    hasher.Write(x);

                return hasher.GetHash();
            }
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            if (fromCache)
            {
                // we have to clone the response here because  otherwise the cached item might be freed while
                // we are still looking at this result, so we clone it to the side
                response = response.Clone(context);
            }

            Result = JsonDeserializationClient.GetDocumentsResult(response);
        }

        public override bool IsReadRequest => true;

        internal void SetTransactionMode(TransactionMode mode)
        {
            _txMode = mode;
    }
}
}
