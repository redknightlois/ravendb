using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Xunit;

namespace FastTests.Corax.Vectors;

public class VectorGraphsDebugOperationTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int Count = 200;

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public async Task ReportMatchesTheStoredGraph()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        await StoreVectorsAndIndex(store);

        var (status, report) = await Get(store, "Vectors");

        Assert.Equal(HttpStatusCode.OK, status);
        var graph = Assert.Single(report["Results"]);
        Assert.Equal("Vector", graph["Field"]);
        Assert.Equal(0, (int)graph["UnreachableNodes"]);

        var database = await GetDatabase(store.Database);
        using var tx = database.IndexStore.GetIndex("Vectors")._environment.ReadTransaction();
        var longest = Hnsw.IterateNodes(tx.LowLevelTransaction, "Vector").Max(node => node.EdgesByLevel.Max(level => level.Length));
        Assert.Equal(Count, (int)graph["NodeCount"]);
        Assert.Equal(longest, (int)graph["LongestList"]);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes | RavenTestCategory.Sharding)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public async Task ReportHasTheSameShapeOnAShard(Options options)
    {
        using var store = GetDocumentStore(options);
        await StoreVectorsAndIndex(store);

        var (status, report) = await Get(store, "Vectors", options.DatabaseMode == RavenDatabaseMode.Sharded ? "&shardNumber=0&nodeTag=A" : "&nodeTag=A");

        Assert.Equal(HttpStatusCode.OK, status);
        var graph = Assert.Single(report["Results"]);
        Assert.Equal("Vector", graph["Field"]);
        Assert.True((int)graph["NodeCount"] > 0);
        Assert.Equal(0, (int)graph["UnreachableNodes"]);
        Assert.True((int)graph["LongestList"] > 0);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task AnIndexWithoutVectorFieldsAnswersBadRequest(Options options)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition { Name = "Plain", Maps = { "from doc in docs.Items select new { doc.Id }" } }));

        var (status, report) = await Get(store, "Plain");

        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.NotNull(report["Error"]);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public async Task AnUnknownIndexAnswersNotFound()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        Assert.Equal(HttpStatusCode.NotFound, (await Get(store, "Missing")).Status);
    }

    private async Task StoreVectorsAndIndex(IDocumentStore store)
    {
        var random = new Random(3);
        using (var session = store.OpenAsyncSession())
        {
            for (var i = 0; i < Count; i++)
                await session.StoreAsync(new Item { Vector = Enumerable.Range(0, 8).Select(_ => random.NextSingle() * 2 - 1).ToArray() });
            await session.SaveChangesAsync();
        }

        await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Vectors",
            Maps = { "from doc in docs.Items select new { Vector = CreateVector(doc.Vector) }" }
        }));
        Indexes.WaitForIndexing(store);
    }

    private static async Task<(HttpStatusCode Status, JObject Body)> Get(IDocumentStore store, string index, string extra = null)
    {
        var url = $"{store.Urls.First()}/databases/{store.Database}/indexes/debug?name={index}&op=vector-graphs{extra}";
        using var client = new HttpClient();
        using var response = await client.GetAsync(url);
        var body = await response.Content.ReadAsStringAsync();
        return (response.StatusCode, body.Length > 0 ? JObject.Parse(body) : null);
    }

    private class Item
    {
        public string Id { get; set; }
        public float[] Vector { get; set; }
    }
}
