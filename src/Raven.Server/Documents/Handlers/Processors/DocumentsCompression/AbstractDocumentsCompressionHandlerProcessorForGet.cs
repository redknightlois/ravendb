﻿using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.DocumentsCompression
{
    internal abstract class AbstractDocumentsCompressionHandlerProcessorForGet<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractDocumentsCompressionHandlerProcessorForGet([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract DocumentsCompressionConfiguration GetDocumentsCompressionConfiguration();

        public override async ValueTask ExecuteAsync()
        {
            var compressionConfig = GetDocumentsCompressionConfiguration();

            if (compressionConfig != null)
            {
                using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, compressionConfig.ToJson());
                }
            }
            else
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            }
        }
    }
}
