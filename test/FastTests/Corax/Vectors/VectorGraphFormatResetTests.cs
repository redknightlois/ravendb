using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics.Tensors;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Errors;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;
using VectorEmbeddingType = Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType;

namespace FastTests.Corax.Vectors;

public class VectorGraphFormatResetTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int Count = 300;
    private const string PlainIndex = "Plain";
    private const string VectorIndex = "Vectors";

    public enum IndexKind
    {
        Static,
        Auto,
        JavaScript
    }

    // The index is created by the current code, then the stored version of its graph is set below the current one, or above it as a newer build would write it.
    // A graph of another version is never read: the index opens faulty, as any index the server cannot open, and a reset rebuilds it. A downgrade behaves the same.
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [InlineData(IndexKind.Static, false)]
    [InlineData(IndexKind.Auto, false)]
    [InlineData(IndexKind.JavaScript, false)]
    [InlineData(IndexKind.Static, true)]
    [InlineData(IndexKind.JavaScript, true)]
    public async Task IndexWithAGraphOfAnotherVersionOpensFaultyAndIsRebuiltByAReset(IndexKind kind, bool newer)
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.RunInMemory = false;
        options.Path = NewDataPath();
        options.ModifyDatabaseRecord += record => record.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
        using var store = GetDocumentStore(options);

        var random = new Random(7);
        var vectors = Enumerable.Range(0, Count).Select(_ => Enumerable.Range(0, 8).Select(_ => random.NextSingle() * 2 - 1).ToArray()).ToArray();
        using (var session = store.OpenAsyncSession())
        {
            for (var i = 0; i < Count; i++)
                await session.StoreAsync(new Item { Id = $"items/{i}", Vector = vectors[i] });
            await session.SaveChangesAsync();
        }

        await store.Maintenance.SendAsync(new PutIndexesOperation(PlainDefinition()));
        var query = vectors[0].Select(x => x * 0.9f + 0.05f).ToArray();
        var vectorIndexName = await CreateVectorIndex(store, kind, query);
        Indexes.WaitForIndexing(store);

        var plainCreated = (await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(PlainIndex))).CreatedTimestamp;
        var definitionBefore = await store.Maintenance.SendAsync(new GetIndexOperation(vectorIndexName));
        var vectorCreated = (await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(vectorIndexName))).CreatedTimestamp;

        var database = await GetDatabase(store.Database);
        var storedVersion = newer
            ? (ushort)(global::Voron.Global.Constants.Graphs.HnswVersion.CurrentVersion + 1)
            : global::Voron.Global.Constants.Graphs.HnswVersion.SinglesWithL2Norm;
        using (var tx = database.IndexStore.GetIndex(vectorIndexName)._environment.WriteTransaction())
        {
            foreach (var (name, _) in Hnsw.GraphsOf(tx))
            {
                using var state = new Hnsw.SearchState(tx.LowLevelTransaction, name);
                state.Options.Version = storedVersion;
                state.FlushOptions();
            }

            tx.Commit();
        }

        await Restart(store);

        database = await GetDatabase(store.Database);
        var faulty = Assert.IsType<FaultyInMemoryIndex>(database.IndexStore.GetIndex(vectorIndexName));
        Assert.Contains("vector graph of another format version", Assert.Single(faulty.GetErrors()).Error);
        Assert.Equal(plainCreated, (await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(PlainIndex))).CreatedTimestamp);

        await store.Maintenance.SendAsync(new ResetIndexOperation(vectorIndexName));
        Indexes.WaitForIndexing(store);
        var rebuiltCreated = (await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(vectorIndexName))).CreatedTimestamp;
        Assert.True(rebuiltCreated > vectorCreated, "The index was not reset.");

        Assert.True(definitionBefore.Equals(await store.Maintenance.SendAsync(new GetIndexOperation(vectorIndexName))), "The stored definition changed by the reset.");
        AssertGraphIsCurrentAndExact(database.IndexStore.GetIndex(vectorIndexName));
        await AssertSearchesCorrectly(store, kind, vectorIndexName, vectors, query);

        // The rebuilt graph is at the current version, so the next open keeps the index.
        await Restart(store);
        Indexes.WaitForIndexing(store);
        Assert.Equal(rebuiltCreated, (await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(vectorIndexName))).CreatedTimestamp);
    }

    private async Task Restart(IDocumentStore store)
    {
        Assert.True((await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: true))).Success);
        Assert.True((await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: false))).Success);
    }

    private static IndexDefinition PlainDefinition() => new()
    {
        Name = PlainIndex,
        Maps = { "from doc in docs.Items select new { doc.Id }" }
    };

    private async Task<string> CreateVectorIndex(IDocumentStore store, IndexKind kind, float[] query)
    {
        if (kind == IndexKind.Auto)
        {
            using var session = store.OpenSession();
            _ = session.Query<Item>().VectorSearch(x => x.WithEmbedding(i => i.Vector), v => v.ByEmbedding(query)).Take(1).ToList();
            return (await GetDatabase(store.Database)).IndexStore.GetIndexes().Single(i => i.Type == IndexType.AutoMap).Name;
        }

        // The JavaScript field is not declared, so its graph is created at indexing time and only the stored graph shows it.
        var definition = kind == IndexKind.Static
            ? new IndexDefinition
            {
                Name = VectorIndex,
                Maps = { "from doc in docs.Items select new { Vector = CreateVector(doc.Vector) }" },
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    ["Vector"] = new() { Vector = new VectorOptions { SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single } }
                }
            }
            : new IndexDefinition
            {
                Name = VectorIndex,
                Maps = { "map('Items', function (item) { return { Vector: createVector(item.Vector) }; })" }
            };
        await store.Maintenance.SendAsync(new PutIndexesOperation(definition));
        return VectorIndex;
    }

    // Every node has an in-link from an older node and the stored count is exactly their number.
    private static void AssertGraphIsCurrentAndExact(Raven.Server.Documents.Indexes.Index index)
    {
        using var tx = index._environment.ReadTransaction();
        var graph = Assert.Single(Hnsw.GraphsOf(tx));
        Assert.Equal(global::Voron.Global.Constants.Graphs.HnswVersion.CurrentVersion, graph.Version);
        var report = Hnsw.Inspect(tx.LowLevelTransaction, graph.Name);
        Assert.Equal(Count, report.NodeCount);
        Assert.Empty(report.Unreachable);

        var olderInLinks = new long[Count + 1];
        foreach (var node in Hnsw.IterateNodes(tx.LowLevelTransaction, graph.Name.ToString()))
        {
            foreach (var (to, _) in node.EdgesByLevel[0].Where(edge => edge.NodeId > node.NodeId))
                olderInLinks[to]++;
        }

        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, graph.Name);
        Assert.Equal(index.Configuration.CoraxVectorDefaultNumberOfCandidatesForIndexing, state.Options.NumberOfCandidates);
        for (var id = 1; id <= Count; id++)
        {
            Assert.Equal(olderInLinks[id], state.GetNodeById(id).OlderInLinks);
            Assert.True(id == 1 || olderInLinks[id] > 0, $"node {id} has no older in-link");
        }
    }

    private async Task AssertSearchesCorrectly(IDocumentStore store, IndexKind kind, string indexName, float[][] vectors, float[] query)
    {
        var expected = Enumerable.Range(0, Count).OrderByDescending(i => TensorPrimitives.CosineSimilarity(vectors[i], query)).Take(10).Select(i => $"items/{i}").ToArray();
        foreach (var isExact in new[] { true, false })
        {
            using var session = store.OpenAsyncSession();
            var results = kind == IndexKind.Auto
                ? session.Query<Item>().VectorSearch(x => x.WithEmbedding(i => i.Vector), v => v.ByEmbedding(query), 0f, Count, isExact)
                : session.Query<Item>(indexName).VectorSearch(x => x.WithField(i => i.Vector), v => v.ByEmbedding(query), 0f, Count, isExact);
            Assert.Equal(expected, (await results.Take(10).ToListAsync()).Select(x => x.Id));
        }
    }

    private class Item
    {
        public string Id { get; set; }
        public float[] Vector { get; set; }
    }
}
