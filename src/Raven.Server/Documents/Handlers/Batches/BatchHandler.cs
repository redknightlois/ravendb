using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.Batches
{
    public sealed class BatchHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_docs", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public Task BulkDocs()
        {
            var processor = new BatchHandlerProcessorForBulkDocs(this);
            var task = processor.ExecuteAsync();
            if (task.IsCompletedSuccessfully)
            {
                processor.Dispose();
                return Task.CompletedTask;
            }
            return HandleAsyncCompletion(processor, task);
        }
        
        private static async Task HandleAsyncCompletion(BatchHandlerProcessorForBulkDocs processor, ValueTask task)
        {
            using (processor)
                await task;
        }
    }
}
