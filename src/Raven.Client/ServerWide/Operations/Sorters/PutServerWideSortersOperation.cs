﻿using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Sorters
{
    public sealed class PutServerWideSortersOperation : IServerOperation
    {
        private readonly SorterDefinition[] _sortersToAdd;

        public PutServerWideSortersOperation(params SorterDefinition[] sortersToAdd)
        {
            if (sortersToAdd == null || sortersToAdd.Length == 0)
                throw new ArgumentNullException(nameof(sortersToAdd));

            _sortersToAdd = sortersToAdd;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutServerWideSortersCommand(conventions, context, _sortersToAdd);
        }

        private sealed class PutServerWideSortersCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject[] _sortersToAdd;

            public PutServerWideSortersCommand(DocumentConventions conventions, JsonOperationContext context, SorterDefinition[] sortersToAdd)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (sortersToAdd == null)
                    throw new ArgumentNullException(nameof(sortersToAdd));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _conventions = conventions;

                _sortersToAdd = new BlittableJsonReaderObject[sortersToAdd.Length];

                for (var i = 0; i < sortersToAdd.Length; i++)
                {
                    if (sortersToAdd[i].Name == null)
                        throw new ArgumentNullException(nameof(SorterDefinition.Name));

                    _sortersToAdd[i] = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(sortersToAdd[i], context);
                }
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/sorters";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WriteArray("Sorters", _sortersToAdd);
                            writer.WriteEndObject();
                        }
                    }, _conventions)
                };

                return request;
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
