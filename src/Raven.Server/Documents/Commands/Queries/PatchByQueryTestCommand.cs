﻿using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Queries;

public sealed class PatchByQueryTestCommand : RavenCommand<PatchByQueryTestCommand.Response>
{
    private readonly DocumentConventions _conventions;
    private readonly string _id;
    private readonly IndexQueryServerSide _query;

    public sealed class Response : PatchResult
    {
        public List<string> Output { get; set; }

        public BlittableJsonReaderObject DebugActions { get; set; }
    }

    public PatchByQueryTestCommand(DocumentConventions conventions, string id, IndexQueryServerSide query)
    {
        _conventions = conventions ?? throw new ArgumentNullException(nameof(id));
        _id = id ?? throw new ArgumentNullException(nameof(id));
        _query = query ?? throw new ArgumentNullException(nameof(query));
    }

    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/queries/test?id={Uri.EscapeDataString(_id)}";

        return new HttpRequestMessage
        {
            Method = HttpMethods.Patch,
            Content = new BlittableJsonContent(async stream =>
            {
                using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Query");
                    writer.WriteIndexQuery(ctx, _query);

                    writer.WriteEndObject();
                }
            }, _conventions)
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            ThrowInvalidResponse();

        Result = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<Response>(response);
    }
}
