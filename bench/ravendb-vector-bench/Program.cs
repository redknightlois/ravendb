using System.Diagnostics;
using Parquet;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Vector;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

const int D = 1536;
const int EmbDataFieldIdx = 3;
const string DefaultUrl = "http://localhost:8080";
const string DbName = "vector-indexing-bench";

string url = DefaultUrl;
bool forceBigDataset = false;
string mode = "concurrent"; // or "deferred" — insert-all-then-index
int N = 100_000;
for (int ai = 0; ai < args.Length; ai++)
{
    if (args[ai] == "--url" && ai + 1 < args.Length) { url = args[++ai]; continue; }
    if (args[ai] == "--mode" && ai + 1 < args.Length) { mode = args[++ai]; continue; }
    if (args[ai] == "--big-dataset") { forceBigDataset = true; continue; }
    if (args[ai].StartsWith("--")) continue;
    if (int.TryParse(args[ai], out var nArg)) N = nArg;
}
if (mode != "concurrent" && mode != "deferred")
{
    Console.Error.WriteLine($"unknown --mode '{mode}'; expected 'concurrent' or 'deferred'");
    return 1;
}
bool useBigDataset = forceBigDataset || N > 100_000;

Console.WriteLine($"ravendb-vector-bench (D={D}): N={N}  mode={mode}  server={url}  db={DbName}");
Console.WriteLine();

Console.Write("Dataset: ");
Console.WriteLine(useBigDataset ? "3-large-1536-1M" : "3-small-1536-100K");
var loadSw = Stopwatch.StartNew();
Console.Write("Loading dataset... ");
var allVectors = useBigDataset ? LoadDatasetLarge(N) : LoadDataset(N);
N = allVectors.Length;
Console.WriteLine($"{loadSw.Elapsed.TotalSeconds:F1}s  ({N:N0} vectors)");

foreach (var v in allVectors) Normalize(v);

using var store = new DocumentStore
{
    Urls = new[] { url },
    Database = DbName,
    Conventions = new DocumentConventions { DisableTopologyUpdates = true }
}.Initialize();

// Recreate database fresh.
if (store.Maintenance.Server.Send(new GetDatabaseNamesOperation(0, 1024)).Contains(DbName))
    store.Maintenance.Server.Send(new DeleteDatabasesOperation(DbName, hardDelete: true));
store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(DbName)));

// Trigger auto-index creation up front via a probe query (indexes 1 seed doc).
// In 'deferred' mode we stop indexing immediately so the bulk insert below runs
// with no concurrent HNSW work; in 'concurrent' mode the auto-index keeps
// indexing as documents stream in (production default).
using (var seedSession = store.OpenSession())
{
    seedSession.Store(new VecDoc { Vector = new float[D] }, "VecDocs/seed");
    seedSession.SaveChanges();
}
string indexName;
using (var qSession = store.OpenSession())
{
    qSession.Query<VecDoc>()
        .VectorSearch(f => f.WithEmbedding(d => d.Vector), v => v.ByEmbedding(new float[D]))
        .Customize(c => c.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
        .Statistics(out var stats)
        .Take(1)
        .ToList();
    indexName = stats.IndexName!;
    Console.WriteLine($"Auto-index created: {indexName}");
}

if (mode == "deferred")
{
    store.Maintenance.Send(new StopIndexOperation(indexName));
    Console.WriteLine($"Indexing stopped on {indexName} (deferred mode).");
}

// Insert phase.
Console.Write($"BulkInsert {N:N0} docs... ");
var insertSw = Stopwatch.StartNew();
long inserted = 0;
using (var bulk = store.BulkInsert())
{
    for (int i = 0; i < N; i++)
    {
        var doc = new VecDoc { Vector = allVectors[i] };
        bulk.StoreAsync(doc, $"VecDocs/{i + 1}").GetAwaiter().GetResult();
        allVectors[i] = null!; // release as we go
        inserted++;
        if (inserted % 50_000 == 0)
        {
            long rssMb = Process.GetCurrentProcess().WorkingSet64 >> 20;
            Console.Error.WriteLine($"    bulk[{inserted / 1000}K] elapsed={insertSw.Elapsed.TotalSeconds:F1}s RSS={rssMb}MB");
        }
    }
}
double bulkSec = insertSw.Elapsed.TotalSeconds;
Console.WriteLine($"BulkInsert done in {bulkSec:F1}s ({(long)(N / bulkSec):N0} docs/s wire-only).");

if (mode == "deferred")
{
    store.Maintenance.Send(new StartIndexOperation(indexName));
    Console.WriteLine($"Indexing started on {indexName}.");
}

// Wait for the auto vector index to catch up — this is where HNSW build dominates.
Console.Write("Waiting for auto-index to finish... ");
var indexSw = Stopwatch.StartNew();
WaitForIndexNonStale(store, TimeSpan.FromMinutes(60));
double indexSec = indexSw.Elapsed.TotalSeconds;
Console.WriteLine($"index caught up in {indexSec:F1}s.");

double totalSec = bulkSec + indexSec;
double endToEndVps = N / totalSec;
Console.WriteLine($"[bulk={bulkSec:F1}s index={indexSec:F1}s total={totalSec:F1}s] {endToEndVps:F0} vec/s end-to-end ({mode})");

return 0;

static void WaitForIndexNonStale(IDocumentStore store, TimeSpan timeout)
{
    var deadline = DateTime.UtcNow + timeout;
    long lastDone = -1;
    while (DateTime.UtcNow < deadline)
    {
        var stats = store.Maintenance.Send(new GetStatisticsOperation());
        if (stats.StaleIndexes.Length == 0) return;
        long done = stats.CountOfDocuments;
        if (done != lastDone)
        {
            Console.Error.WriteLine($"    stale={string.Join(",", stats.StaleIndexes)} docs={done}");
            lastDone = done;
        }
        Thread.Sleep(500);
    }
    throw new TimeoutException($"Indexes still stale after {timeout}");
}

static void Normalize(float[] v)
{
    float n = 0f;
    for (int i = 0; i < v.Length; i++) n += v[i] * v[i];
    n = MathF.Sqrt(n);
    if (n > 0f) for (int i = 0; i < v.Length; i++) v[i] /= n;
}

static float[][] LoadDatasetLarge(int maxCount)
{
    var cacheDir = "/mnt/work/tq-datasets/dbpedia-openai3-1536-large";
    Directory.CreateDirectory(cacheDir);
    const string ShardUrlTemplate = "https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-large-1536-1M/resolve/refs%2Fconvert%2Fparquet/default/train/{0:D4}.parquet";
    const int MaxShards = 30;

    var result = new List<float[]>(Math.Min(maxCount, 1_000_000));
    for (int shard = 0; shard < MaxShards && result.Count < maxCount; shard++)
    {
        var localPath = Path.Combine(cacheDir, $"{shard:D4}.parquet");
        if (!File.Exists(localPath))
        {
            Console.Write($"\n  Downloading large shard {shard} (~367 MB)... ");
            using HttpClient client = new() { Timeout = TimeSpan.FromHours(1) };
            var fetchUrl = string.Format(ShardUrlTemplate, shard);
            using var response = client.GetAsync(fetchUrl, HttpCompletionOption.ResponseHeadersRead).GetAwaiter().GetResult();
            response.EnsureSuccessStatusCode();
            using var contentStream = response.Content.ReadAsStreamAsync().GetAwaiter().GetResult();
            using var fileStream = File.Create(localPath);
            contentStream.CopyTo(fileStream);
            Console.WriteLine("done.");
        }
        var reader = ParquetReader.CreateAsync(localPath).Result;
        var embField = reader.Schema.DataFields[EmbDataFieldIdx];
        for (int rg = 0; rg < reader.RowGroupCount && result.Count < maxCount; rg++)
        {
            var rgReader = reader.OpenRowGroupReader(rg);
            var col = rgReader.ReadColumnAsync(embField).Result;
            var doubles = (double[])col.DefinedData;
            int vecCount = doubles.Length / D;
            for (int i = 0; i < vecCount && result.Count < maxCount; i++)
            {
                var vec = new float[D];
                int baseIx = i * D;
                for (int j = 0; j < D; j++) vec[j] = (float)doubles[baseIx + j];
                result.Add(vec);
            }
        }
    }
    return result.ToArray();
}

static float[][] LoadDataset(int maxCount)
{
    var cacheDir = "/mnt/work/tq-datasets/dbpedia-openai3-1536";
    Directory.CreateDirectory(cacheDir);
    const string ShardUrlTemplate = "https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K/resolve/refs%2Fconvert%2Fparquet/default/train/{0:D4}.parquet";
    const int MaxShards = 4;

    var result = new List<float[]>(Math.Min(maxCount, 100_000));
    for (int shard = 0; shard < MaxShards && result.Count < maxCount; shard++)
    {
        var localPath = Path.Combine(cacheDir, $"{shard:D4}.parquet");
        if (!File.Exists(localPath))
        {
            Console.Write($"\n  Downloading shard {shard}/{MaxShards - 1}... ");
            using HttpClient client = new() { Timeout = TimeSpan.FromHours(1) };
            var fetchUrl = string.Format(ShardUrlTemplate, shard);
            using var response = client.GetAsync(fetchUrl, HttpCompletionOption.ResponseHeadersRead).GetAwaiter().GetResult();
            response.EnsureSuccessStatusCode();
            using var contentStream = response.Content.ReadAsStreamAsync().GetAwaiter().GetResult();
            using var fileStream = File.Create(localPath);
            contentStream.CopyTo(fileStream);
            Console.WriteLine("done.");
        }
        var reader = ParquetReader.CreateAsync(localPath).Result;
        var embField = reader.Schema.DataFields[EmbDataFieldIdx];
        for (int rg = 0; rg < reader.RowGroupCount && result.Count < maxCount; rg++)
        {
            var rgReader = reader.OpenRowGroupReader(rg);
            var col = rgReader.ReadColumnAsync(embField).Result;
            var floats = (float[])col.DefinedData;
            int vecCount = floats.Length / D;
            for (int i = 0; i < vecCount && result.Count < maxCount; i++)
            {
                var vec = new float[D];
                Array.Copy(floats, i * D, vec, 0, D);
                result.Add(vec);
            }
        }
    }
    return result.ToArray();
}

class VecDoc
{
    public float[] Vector { get; set; } = default!;
}
