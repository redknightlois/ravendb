﻿using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

internal abstract class AbstractStudioDatabaseTasksHandlerProcessorForGetIndexDefaults<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractStudioDatabaseTasksHandlerProcessorForGetIndexDefaults([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected abstract RavenConfiguration GetDatabaseConfiguration();

    public override async ValueTask ExecuteAsync()
    {
        var configuration = GetDatabaseConfiguration();

        var autoIndexesDeploymentMode = configuration.Indexing.AutoIndexDeploymentMode;
        var staticIndexesDeploymentMode = configuration.Indexing.StaticIndexDeploymentMode;
        var staticIndexingEngineType = configuration.Indexing.StaticIndexingEngineType;

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(IndexDefaults.AutoIndexDeploymentMode));
            writer.WriteString(autoIndexesDeploymentMode.ToString());
            writer.WriteComma();
            writer.WritePropertyName(nameof(IndexDefaults.StaticIndexDeploymentMode));
            writer.WriteString(staticIndexesDeploymentMode.ToString());
            writer.WriteComma();
            writer.WritePropertyName(nameof(IndexDefaults.StaticIndexingEngineType));
            writer.WriteString(staticIndexingEngineType.ToString());
            writer.WriteEndObject();
        }
    }
}

public sealed class IndexDefaults
{
    public IndexDeploymentMode AutoIndexDeploymentMode { get; set; }
    public IndexDeploymentMode StaticIndexDeploymentMode { get; set; }
    public SearchEngineType StaticIndexingEngineType { get; set; }
}
