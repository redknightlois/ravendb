﻿using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.DocumentsCompression
{
    internal abstract class AbstractDocumentsCompressionHandlerProcessorForPost<TRequestHandler> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler>
        where TRequestHandler : RequestHandler
    {
        protected AbstractDocumentsCompressionHandlerProcessorForPost([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            return RequestHandler.ServerStore.ModifyDocumentsCompression(context, databaseName, configuration, raftRequestId);
        }


    }
}
