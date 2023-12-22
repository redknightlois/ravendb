﻿using System.Collections.Concurrent;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Changes;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Changes;

internal abstract class AbstractChangesHandlerProcessorForGetConnectionsDebugInfo<TRequestHandler, TOperationContext, TChangesClientConnection> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TChangesClientConnection : AbstractChangesClientConnection<TOperationContext>
{
    protected AbstractChangesHandlerProcessorForGetConnectionsDebugInfo([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ConcurrentDictionary<long, TChangesClientConnection> GetConnections();

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            var connectionValues = GetConnections().Values;

            writer.WritePropertyName("NumberOfConnections");
            writer.WriteInteger(connectionValues.Count);
            writer.WriteComma();

            writer.WritePropertyName("Connections");

            writer.WriteStartArray();
            var first = true;
            foreach (var connection in connectionValues)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;
                context.Write(writer, connection.GetDebugInfo());
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
        }
    }
}
