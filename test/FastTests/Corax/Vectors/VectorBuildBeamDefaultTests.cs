using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Xunit;
using VectorEmbeddingType = Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType;

namespace FastTests.Corax.Vectors;

public class VectorBuildBeamDefaultTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int ExplicitBeam = 20;

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public async Task NewGraphsTakeTheServerDefaultBeamAndAnExplicitBeamIsHonored()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var definition = new IndexDefinition
        {
            Name = "Vectors",
            Maps = { "from doc in docs.Items select new { Default = CreateVector(doc.Vector), Explicit = CreateVector(doc.Vector) }" },
            Fields = new Dictionary<string, IndexFieldOptions>
            {
                ["Default"] = Field(null),
                ["Explicit"] = Field(ExplicitBeam)
            }
        };
        await store.Maintenance.SendAsync(new PutIndexesOperation(definition));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Item { Vector = [1f, 2f, 3f] });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        var stored = (await store.Maintenance.SendAsync(new GetIndexesOperation(0, 1))).Single();
        Assert.Equal(IndexDefinitionCompareDifferences.None, definition.Compare(stored));
        Assert.Null(stored.Fields["Default"].Vector.NumberOfCandidatesForIndexing);

        var database = await GetDatabase(store.Database);
        var index = database.IndexStore.GetIndex(definition.Name);
        var configured = index.Configuration.CoraxVectorDefaultNumberOfCandidatesForIndexing;
        Assert.Equal(64, configured);

        using var tx = index._environment.ReadTransaction();
        using var defaultGraph = new Hnsw.SearchState(tx.LowLevelTransaction, "Default");
        using var explicitGraph = new Hnsw.SearchState(tx.LowLevelTransaction, "Explicit");
        Assert.Equal(configured, defaultGraph.Options.NumberOfCandidates);
        Assert.Equal(ExplicitBeam, explicitGraph.Options.NumberOfCandidates);
    }

    private static IndexFieldOptions Field(int? candidates) => new()
    {
        Vector = new VectorOptions
        {
            SourceEmbeddingType = VectorEmbeddingType.Single,
            DestinationEmbeddingType = VectorEmbeddingType.Single,
            NumberOfCandidatesForIndexing = candidates
        }
    };

    private class Item
    {
        public string Id { get; set; }
        public float[] Vector { get; set; }
    }
}
