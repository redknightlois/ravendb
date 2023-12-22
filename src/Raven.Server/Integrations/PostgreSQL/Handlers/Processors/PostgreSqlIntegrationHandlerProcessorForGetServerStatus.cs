﻿using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal sealed class PostgreSqlIntegrationHandlerProcessorForGetServerStatus<TRequestHandler, TOperationContext> : AbstractPostgreSqlIntegrationHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    public PostgreSqlIntegrationHandlerProcessorForGetServerStatus([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        AssertCanUsePostgreSqlIntegration(RequestHandler);

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            var dto = new PostgreSqlServerStatus { Active = RequestHandler.Server.PostgresServer.Active };

            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(dto);
            writer.WriteObject(context.ReadObject(djv, "PostgreSqlServerStatus"));
        }
    }
}
