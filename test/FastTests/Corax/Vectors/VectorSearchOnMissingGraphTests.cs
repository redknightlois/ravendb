using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using VectorEmbeddingType = Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType;

namespace FastTests.Corax.Vectors;

public class VectorSearchOnMissingGraphTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string IndexName = "Items/ByVectors";
    private const int Count = 1100;

    // The index declares the field Vector, but no document has a value for it, so the field has no graph. Once Other holds values, the environment holds a graph and
    // the shared vector tables, which the missing graph of Vector must not depend on.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Vector)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SearchOfAFieldWithoutAGraphReturnsNoResults(bool otherHasValues)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var vectorOptions = new VectorOptions { SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single };
        await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
        {
            Name = IndexName,
            Maps = { "from doc in docs.Items select new { doc.Tag, Vector = CreateVector(doc.Vector), Other = CreateVector(doc.Other) }" },
            Fields = new Dictionary<string, IndexFieldOptions>
            {
                ["Vector"] = new() { Vector = vectorOptions },
                ["Other"] = new() { Vector = vectorOptions }
            }
        }));

        var random = new Random(1);
        using (var bulk = store.BulkInsert())
        {
            for (var i = 0; i < Count; i++)
                await bulk.StoreAsync(new Item { Id = $"items/{i}", Tag = i < 10 ? "scanned" : "approximate", Other = otherHasValues ? Enumerable.Range(0, 4).Select(_ => random.NextSingle()).ToArray() : null });
        }

        Indexes.WaitForIndexing(store);

        var raw = Convert.ToBase64String(MemoryMarshal.AsBytes(new[] { 0.1f, 0.2f, 0.3f, 0.4f }.AsSpan()));
        using var session = store.OpenAsyncSession();
        // Tag admits 10 documents, which is below the scanning threshold: the exact scan. Tag approximate admits 1090: the filtered approximate search.
        string[] queries =
        [
            "vector.search(Vector, embedding.Raw($raw))",
            "exact(vector.search(Vector, embedding.Raw($raw)))",
            "vector.search(Vector, embedding.Raw($raw)) and Tag = 'approximate'",
            "vector.search(Vector, embedding.Raw($raw)) and Tag = 'scanned'",
            "exact(vector.search(Vector, embedding.Raw($raw))) and Tag = 'scanned'",
            "vector.search(Vector, embedding.forDoc('items/0'))",
            "exact(vector.search(Vector, embedding.forDoc('items/0')))",
            "exact(vector.search(Vector, embedding.forDoc('items/0'))) and Tag = 'scanned'",
            "vector.search(Vector, embedding.forDoc('items/0')) and Tag = 'approximate'"
        ];
        foreach (var query in queries)
            Assert.Empty(await session.Advanced.AsyncRawQuery<Item>($"from index '{IndexName}' where {query}").AddParameter("raw", raw).ToListAsync());

        // The client emits embedding.forDoc for a search by document.
        Assert.Empty(await session.Query<Item>(IndexName).VectorSearch(f => f.WithField("Vector"), v => v.ForDocument("items/0")).ToListAsync());
    }

    private class Item
    {
        public string Id { get; set; }
        public string Tag { get; set; }
        public float[] Vector { get; set; }
        public float[] Other { get; set; }
    }
}
