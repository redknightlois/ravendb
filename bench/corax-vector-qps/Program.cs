using System.Diagnostics;
using System.Runtime.InteropServices;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Utils;
using Parquet;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.Graphs;
using VectorOptionsCorax = Corax.Mappings.VectorOptions;

// Corax-level HNSW QPS benchmark.
//
// Three query shapes are measured end-to-end through IndexSearcher:
//   [Single]      one VectorSearch call per query, fresh searcher per query
//   [SharedIS]    one VectorSearch call per query, searcher reused across all queries
//   [MultiVec]    one MultiVectorSearch call with NQ vectors (shared SearchState inside)
//   [Concurrent]  threaded per-thread searchers against the same env
//
// Data: DBpedia-openai3 1536d (same as tq-cohere). f32 / cosine only — that is the
// code path the L2 norm cache actually touches.
//
// CLI:
//   [N]                positional override for N (default 25000)
//   --full             N=100000, NQ=500
//   --skip-insert      reuse an existing index dir at /tmp/corax-vec-qps/idx
//   --threads=K        override concurrency for the Concurrent mode (default = ProcessorCount)
//   --evict-cold       drop page cache between warm and cold phases (Linux-only)

const int K = 10;
const int D = 1536;
const int M = 16, EfConstruction = 64;

int N = 25_000;
int NQ = 200;
bool skipInsert = false;
bool evictCold = false;
int threads = Environment.ProcessorCount;
// --profile=shared64 | shared512 | single64 — runs only that mode in a tight loop
// for a fixed wall-clock duration (default 20s) so dotnet-trace captures a dense profile.
string? profileMode = null;
int profileSeconds = 20;

foreach (var a in args)
{
    if (a == "--full") { N = 100_000; NQ = 500; }
    else if (a == "--skip-insert") skipInsert = true;
    else if (a == "--evict-cold") evictCold = true;
    else if (a.StartsWith("--threads=")) threads = int.Parse(a.Substring("--threads=".Length));
    else if (a.StartsWith("--profile=")) profileMode = a.Substring("--profile=".Length);
    else if (a.StartsWith("--profile-seconds=")) profileSeconds = int.Parse(a.Substring("--profile-seconds=".Length));
    else if (int.TryParse(a, out var nArg)) N = nArg;
}

const int EmbDataFieldIdx = 3;
const string IndexPath = "/tmp/corax-vec-qps/idx";

Console.WriteLine($"Corax-Vector-QPS  D={D}  N≤{N}  NQ={NQ}  k={K}  M={M}  efC={EfConstruction}");
Console.WriteLine($"  skipInsert={skipInsert}  threads={threads}  evictCold={evictCold}");
Console.WriteLine();

// ---- dataset ----
Console.Write("Loading dataset... ");
var sw = Stopwatch.StartNew();
var allVectors = LoadDataset(N);
N = allVectors.Length;
Console.WriteLine($"{sw.Elapsed.TotalSeconds:F1}s  ({N:N0} vectors)");

var rng = new Random(42);
var qIdx = Enumerable.Range(0, N).OrderBy(_ => rng.Next()).Take(NQ).ToArray();
var queries = qIdx.Select(i => allVectors[i].ToArray()).ToArray();

// For Corax write, raw floats are fine; WriteVector handles normalization.
// For brute-force ground truth we normalize a local copy.
Console.Write("Ground truth... ");
sw.Restart();
var queriesNorm = queries.Select(q => { var c = (float[])q.Clone(); Normalize(c); return c; }).ToArray();
var allVecsNorm = allVectors.Select(v => { var c = (float[])v.Clone(); Normalize(c); return c; }).ToArray();
var truth = new HashSet<long>[NQ];
Parallel.For(0, NQ, q =>
{
    var sims = new (float sim, long id)[N];
    var qv = queriesNorm[q];
    for (int i = 0; i < N; i++)
    {
        float dot = 0f;
        var av = allVecsNorm[i];
        for (int j = 0; j < D; j++) dot += qv[j] * av[j];
        sims[i] = (dot, i + 1);
    }
    Array.Sort(sims, (a, b) => b.sim.CompareTo(a.sim));
    var set = new HashSet<long>();
    for (int i = 0; i < K; i++) set.Add(sims[i].id);
    truth[q] = set;
});
Console.WriteLine($"{sw.Elapsed.TotalSeconds:F1}s");

// ---- index setup ----
var mapping = IndexFieldsMappingBuilder
    .CreateForWriter(false)
    .AddBinding(0, "id()")
    .AddBinding(1, "Vector", vectorOptions: new VectorOptionsCorax
    {
        NumberOfEdges = M,
        NumberOfCandidates = EfConstruction,
        VectorEmbeddingType = VectorEmbeddingType.Single
    })
    .Build();

if (skipInsert && Directory.Exists(IndexPath))
{
    Console.WriteLine($"Reusing existing index at {IndexPath}");
}
else
{
    if (Directory.Exists(IndexPath)) Directory.Delete(IndexPath, true);
    Directory.CreateDirectory(IndexPath);

    Console.Write($"Indexing {N:N0} vectors via Corax... ");
    sw.Restart();
    var envOpts = StorageEnvironmentOptions.ForPathForTests(IndexPath);
    using (var env = new StorageEnvironment(envOpts))
    using (var writer = new IndexWriter(env, mapping, SupportedFeatures.All))
    {
        for (int i = 0; i < N; i++)
        {
            var id = $"vec/{i + 1}";
            using var entry = writer.Index(id);
            entry.Write(0, System.Text.Encoding.UTF8.GetBytes(id));
            entry.WriteVector(1, "Vector", MemoryMarshal.AsBytes(allVectors[i].AsSpan()));
            entry.EndWriting();
        }
        writer.Commit();
    }
    Console.WriteLine($"{sw.Elapsed.TotalSeconds:F1}s  ({N / sw.Elapsed.TotalSeconds:F0} vps)");
}

Console.WriteLine();

if (profileMode != null)
{
    var opts = StorageEnvironmentOptions.ForPathForTests(IndexPath);
    using var env = new StorageEnvironment(opts);
    using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
    var queryBytes = queries.Select(q => MemoryMarshal.AsBytes(q.AsSpan()).ToArray()).ToArray();
    var profileCaches = BuildVectorNodeCaches(env, mapping);
    Warmup(env, mapping, bsc, queryBytes, profileCaches);

    int ef = profileMode.EndsWith("512") ? 512 : 64;
    bool single = profileMode.StartsWith("single");
    bool concurrent = profileMode.StartsWith("concurrent");
    Console.WriteLine($"Profiling mode={profileMode}  duration={profileSeconds}s");
    var deadline = Stopwatch.StartNew();
    long total = 0;
    if (concurrent)
    {
        var deadlineShared = deadline;
        long totalShared = 0;
        Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, t =>
        {
            using var localBsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var localSearcher = new IndexSearcher(env, mapping);
            var meta = mapping.GetByFieldId(1).Metadata;
            var localIds = new long[K * 2];
            long localTotal = 0;
            while (deadlineShared.Elapsed.TotalSeconds < profileSeconds)
            {
                for (int q = 0; q < queryBytes.Length; q++)
                {
                    using var vv = MakeVectorValue(localBsc, queryBytes[q]);
                    var m = localSearcher.VectorSearch(meta, vv, -1f, ef, false, true, null, scanningThreshold: 0);
                    m.Fill(localIds);
                    localTotal++;
                }
            }
            System.Threading.Interlocked.Add(ref totalShared, localTotal);
        });
        total = totalShared;
    }
    else
    {
        using var searcher = new IndexSearcher(env, mapping);
        var metadata = mapping.GetByFieldId(1).Metadata;
        var ids = new long[K * 2];
        while (deadline.Elapsed.TotalSeconds < profileSeconds)
        {
            for (int q = 0; q < queryBytes.Length; q++)
            {
                using var vv = MakeVectorValue(bsc, queryBytes[q]);
                if (single)
                {
                    using var localSearcher = new IndexSearcher(env, mapping);
                    var m2 = localSearcher.VectorSearch(mapping.GetByFieldId(1).Metadata,
                        vv, -1f, ef, false, true, null, scanningThreshold: 0);
                    m2.Fill(ids);
                }
                else
                {
                    var m = searcher.VectorSearch(metadata, vv, -1f, ef, false, true, null, scanningThreshold: 0);
                    m.Fill(ids);
                }
                total++;
            }
        }
    }
    Console.WriteLine($"Profile run: {total} queries in {deadline.Elapsed.TotalSeconds:F1}s  =>  {total / deadline.Elapsed.TotalSeconds:F0} QPS");
    mapping.Dispose();
    return;
}

Console.WriteLine($"{"Mode",-22} {"ef",5} {"QPS",10} {"R@10",8} {"p50ms",8} {"p95ms",8} {"p99ms",8}");
Console.WriteLine(new string('-', 78));

// Warm env (stays open across all modes; snapshot is consistent).
{
    var opts = StorageEnvironmentOptions.ForPathForTests(IndexPath);
    using var env = new StorageEnvironment(opts);
    using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
    var queryBytes = queries.Select(q => MemoryMarshal.AsBytes(q.AsSpan()).ToArray()).ToArray();

    // Build HNSW node caches once for this snapshot and share them across every searcher
    // below — this is how production runs (Corax builds per-field caches on commit).
    var nodeCaches = BuildVectorNodeCaches(env, mapping);

    // Warmup pass — trigger lazy init + page cache + any JIT of hot kernels.
    Warmup(env, mapping, bsc, queryBytes, nodeCaches);

    foreach (var ef in new[] { 16, 32, 64, 128, 256, 512 })
    {
        RunSingle(env, mapping, bsc, queryBytes, truth, ef, nodeCaches, label: "Single (fresh IS)");
        RunSharedSearcher(env, mapping, bsc, queryBytes, truth, ef, nodeCaches, label: "SharedIS");
        RunMultiVector(env, mapping, bsc, queryBytes, truth, ef, nodeCaches, label: "MultiVector");
        RunConcurrent(env, mapping, bsc, queryBytes, truth, ef, threads, nodeCaches, label: $"Concurrent x{threads}");
    }
}

if (evictCold)
{
    Console.WriteLine();
    var evicted = EvictPageCache(IndexPath);
    Console.WriteLine($"  evicted {evicted / (1024.0 * 1024):F1} MB from page cache");
    Console.WriteLine();
    Console.WriteLine($"{"Mode (cold)",-22} {"ef",5} {"QPS",10} {"R@10",8} {"p50ms",8} {"p95ms",8} {"p99ms",8}");
    Console.WriteLine(new string('-', 78));

    var opts = StorageEnvironmentOptions.ForPathForTests(IndexPath);
    using var env = new StorageEnvironment(opts);
    using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
    var queryBytes = queries.Select(q => MemoryMarshal.AsBytes(q.AsSpan()).ToArray()).ToArray();
    var nodeCaches = BuildVectorNodeCaches(env, mapping);
    foreach (var ef in new[] { 16, 32, 64, 128, 256, 512 })
    {
        RunSingle(env, mapping, bsc, queryBytes, truth, ef, nodeCaches, label: "Single (fresh IS)");
        RunSharedSearcher(env, mapping, bsc, queryBytes, truth, ef, nodeCaches, label: "SharedIS");
        RunMultiVector(env, mapping, bsc, queryBytes, truth, ef, nodeCaches, label: "MultiVector");
        RunConcurrent(env, mapping, bsc, queryBytes, truth, ef, threads, nodeCaches, label: $"Concurrent x{threads}");
    }
}

mapping.Dispose();

// ------- modes -------

static Dictionary<Slice, Hnsw.NodeCache> BuildVectorNodeCaches(StorageEnvironment env, IndexFieldsMapping mapping)
{
    // Match the production path in Corax: build a per-field node cache on the current
    // snapshot and thread it into each IndexSearcher. Int8 pre-screen data (if the index
    // is an f32 cosine graph) is computed inside NodeCache.Build — purely in-memory,
    // rebuilt per snapshot, no on-disk format changes.
    using var tx = env.ReadTransaction();
    var dict = new Dictionary<Slice, Hnsw.NodeCache>(SliceComparer.Instance);
    var fieldName = mapping.GetByFieldId(1).Metadata.FieldName;
    var cache = Hnsw.NodeCache.Build(tx.LowLevelTransaction, fieldName, maxNodes: 100_000);
    if (cache != null && cache.Count > 0)
        dict[fieldName] = cache;
    return dict;
}

static void Warmup(StorageEnvironment env, IndexFieldsMapping mapping, ByteStringContext bsc, byte[][] queryBytes, Dictionary<Slice, Hnsw.NodeCache> caches)
{
    using var searcher = new IndexSearcher(env, mapping);
    searcher.AttachVectorNodeCaches(caches);
    var metadata = mapping.GetByFieldId(1).Metadata;
    Span<long> ids = stackalloc long[K * 2];
    for (int q = 0; q < Math.Min(10, queryBytes.Length); q++)
    {
        using var vv = MakeVectorValue(bsc, queryBytes[q]);
        var m = searcher.VectorSearch(metadata, vv, -1f, 64, false, true, null, scanningThreshold: 0);
        m.Fill(ids);
    }
}

static void RunSingle(StorageEnvironment env, IndexFieldsMapping mapping, ByteStringContext bsc,
    byte[][] queryBytes, HashSet<long>[] truth, int ef, Dictionary<Slice, Hnsw.NodeCache> caches, string label)
{
    var per = new double[queryBytes.Length];
    int hits = 0, total = 0;
    var ids = new long[K * 2];
    var sw = Stopwatch.StartNew();
    for (int q = 0; q < queryBytes.Length; q++)
    {
        var qsw = Stopwatch.StartNew();
        using var searcher = new IndexSearcher(env, mapping);
        searcher.AttachVectorNodeCaches(caches);
        var metadata = mapping.GetByFieldId(1).Metadata;
        using var vv = MakeVectorValue(bsc, queryBytes[q]);
        var match = searcher.VectorSearch(metadata, vv, -1f, ef, false, true, null, scanningThreshold: 0);
        int c = match.Fill(ids);
        per[q] = qsw.Elapsed.TotalMilliseconds;
        hits += CountHits(ids, c, truth[q]);
        total += Math.Min(c, K);
    }
    ReportRow(label, ef, queryBytes.Length, sw.Elapsed.TotalSeconds, hits, total, per);
}

static void RunSharedSearcher(StorageEnvironment env, IndexFieldsMapping mapping, ByteStringContext bsc,
    byte[][] queryBytes, HashSet<long>[] truth, int ef, Dictionary<Slice, Hnsw.NodeCache> caches, string label)
{
    var per = new double[queryBytes.Length];
    int hits = 0, total = 0;
    var ids = new long[K * 2];
    using var searcher = new IndexSearcher(env, mapping);
    searcher.AttachVectorNodeCaches(caches);
    var metadata = mapping.GetByFieldId(1).Metadata;
    var sw = Stopwatch.StartNew();
    for (int q = 0; q < queryBytes.Length; q++)
    {
        var qsw = Stopwatch.StartNew();
        using var vv = MakeVectorValue(bsc, queryBytes[q]);
        var match = searcher.VectorSearch(metadata, vv, -1f, ef, false, true, null, scanningThreshold: 0);
        int c = match.Fill(ids);
        per[q] = qsw.Elapsed.TotalMilliseconds;
        hits += CountHits(ids, c, truth[q]);
        total += Math.Min(c, K);
    }
    ReportRow(label, ef, queryBytes.Length, sw.Elapsed.TotalSeconds, hits, total, per);
}

static void RunMultiVector(StorageEnvironment env, IndexFieldsMapping mapping, ByteStringContext bsc,
    byte[][] queryBytes, HashSet<long>[] truth, int ef, Dictionary<Slice, Hnsw.NodeCache> caches, string label)
{
    // MultiVectorSearch returns a combined match; we can't cleanly slice per-query recall without
    // inspecting scores, so recall is reported as "aggregate: did the top-N contain any truth item".
    // QPS is reported as (NQ / elapsed) — one MultiVectorSearch represents NQ query vectors.
    using var searcher = new IndexSearcher(env, mapping);
    searcher.AttachVectorNodeCaches(caches);
    var metadata = mapping.GetByFieldId(1).Metadata;
    var vvs = new VectorValue[queryBytes.Length];
    for (int i = 0; i < queryBytes.Length; i++)
        vvs[i] = MakeVectorValue(bsc, queryBytes[i]);

    var buf = new long[queryBytes.Length * K * 2];
    var sw = Stopwatch.StartNew();
    var match = searcher.MultiVectorSearch(metadata, vvs, -1f, ef, false, true, null, scanningThreshold: 0);
    int c = 0, read;
    int offset = 0;
    while ((read = match.Fill(buf.AsSpan(offset))) > 0)
    {
        offset += read;
        c += read;
        if (offset + K > buf.Length) break;
    }
    var elapsed = sw.Elapsed.TotalSeconds;

    // MultiVectorSearch returns a combined match mixing IDs from every query vector.
    // Per-query recall attribution requires inspecting Score(), not done here —
    // rely on Single/SharedIS for recall sanity; this row is throughput-only.
    double qps = queryBytes.Length / elapsed;
    Console.WriteLine($"{label,-22} {ef,5} {qps,10:F0} {"n/a",7}  {"-",7} {"-",7} {"-",7}  (returned {c})");

    foreach (var v in vvs) v.Dispose();
}

static void RunConcurrent(StorageEnvironment env, IndexFieldsMapping mapping, ByteStringContext bsc,
    byte[][] queryBytes, HashSet<long>[] truth, int ef, int threads, Dictionary<Slice, Hnsw.NodeCache> caches, string label)
{
    // Per-thread searcher + per-thread ByteStringContext. Env is shared.
    var per = new double[queryBytes.Length];
    int hits = 0, total = 0;
    var sw = Stopwatch.StartNew();
    Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, t =>
    {
        using var localBsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var searcher = new IndexSearcher(env, mapping);
        searcher.AttachVectorNodeCaches(caches);
        var metadata = mapping.GetByFieldId(1).Metadata;
        var ids = new long[K * 2];
        int localHits = 0, localTotal = 0;
        for (int q = t; q < queryBytes.Length; q += threads)
        {
            var qsw = Stopwatch.StartNew();
            using var vv = MakeVectorValue(localBsc, queryBytes[q]);
            var match = searcher.VectorSearch(metadata, vv, -1f, ef, false, true, null, scanningThreshold: 0);
            int c = match.Fill(ids);
            per[q] = qsw.Elapsed.TotalMilliseconds;
            localHits += CountHits(ids, c, truth[q]);
            localTotal += Math.Min(c, K);
        }
        System.Threading.Interlocked.Add(ref hits, localHits);
        System.Threading.Interlocked.Add(ref total, localTotal);
    });
    ReportRow(label, ef, queryBytes.Length, sw.Elapsed.TotalSeconds, hits, total, per);
}

// ------- helpers -------

static VectorValue MakeVectorValue(ByteStringContext bsc, byte[] bytes)
{
    var scope = bsc.Allocate(bytes.Length, out Memory<byte> mem);
    bytes.CopyTo(mem.Span);
    return new VectorValue(scope, mem, bytes.Length);
}

static int CountHits(long[] ids, int c, HashSet<long> truth)
{
    int hits = 0;
    for (int i = 0; i < Math.Min(c, K); i++)
        if (truth.Contains(ids[i])) hits++;
    return hits;
}

static void ReportRow(string label, int ef, int nq, double elapsed, int hits, int total, double[] per)
{
    Array.Sort(per);
    double p50 = per[(int)(per.Length * 0.50)];
    double p95 = per[(int)(per.Length * 0.95)];
    double p99 = per[(int)(per.Length * 0.99)];
    double qps = nq / elapsed;
    double recall = total > 0 ? (double)hits / total : 0;
    Console.WriteLine($"{label,-22} {ef,5} {qps,10:F0} {recall,7:P1} {p50,7:F2} {p95,7:F2} {p99,7:F2}");
}

static void Normalize(float[] v)
{
    float n = 0f;
    for (int i = 0; i < v.Length; i++) n += v[i] * v[i];
    n = MathF.Sqrt(n);
    if (n > 0f) for (int i = 0; i < v.Length; i++) v[i] /= n;
}

static float[][] LoadDataset(int maxCount)
{
    var cacheDir = Path.Combine(Path.GetTempPath(), "tq-datasets", "dbpedia-openai3-1536");
    Directory.CreateDirectory(cacheDir);
    // The HF repo publishes the 100K dataset as 4 shards (0000..0003.parquet), 25K rows each.
    const string ShardUrlTemplate = "https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K/resolve/refs%2Fconvert%2Fparquet/default/train/{0:D4}.parquet";
    const int MaxShards = 4;

    var result = new List<float[]>(Math.Min(maxCount, 100_000));
    for (int shard = 0; shard < MaxShards && result.Count < maxCount; shard++)
    {
        var localPath = Path.Combine(cacheDir, $"{shard:D4}.parquet");
        if (!File.Exists(localPath))
        {
            Console.Write($"\n  Downloading dbpedia-openai3-1536 shard {shard}/{MaxShards - 1} (~393 MB)... ");
            using HttpClient client = new() { Timeout = TimeSpan.FromHours(1) };
            var url = string.Format(ShardUrlTemplate, shard);
            using var response = client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead).GetAwaiter().GetResult();
            response.EnsureSuccessStatusCode();
            using var contentStream = response.Content.ReadAsStreamAsync().GetAwaiter().GetResult();
            using var fileStream = File.Create(localPath);
            contentStream.CopyTo(fileStream);
            Console.WriteLine("done.");
        }

        var reader = ParquetReader.CreateAsync(localPath).Result;
        var schema = reader.Schema;
        var embField = schema.DataFields[EmbDataFieldIdx];
        for (int rg = 0; rg < reader.RowGroupCount && result.Count < maxCount; rg++)
        {
            var rgReader = reader.OpenRowGroupReader(rg);
            var col = rgReader.ReadColumnAsync(embField).Result;
            var floats = (float[])col.DefinedData;
            int vecCount = floats.Length / D;
            for (int i = 0; i < vecCount && result.Count < maxCount; i++)
            {
                var v = new float[D];
                Array.Copy(floats, i * D, v, 0, D);
                result.Add(v);
            }
        }
    }
    return result.ToArray();
}

static long EvictPageCache(string dirPath)
{
    if (!OperatingSystem.IsLinux()) return 0;
    long total = 0;
    foreach (var file in Directory.EnumerateFiles(dirPath, "*", SearchOption.AllDirectories))
    {
        try
        {
            var fi = new FileInfo(file);
            using var fs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            int fd = (int)fs.SafeFileHandle.DangerousGetHandle();
            if (posix_fadvise(fd, 0, 0, 4) == 0) total += fi.Length;
        }
        catch { }
    }
    return total;

    [DllImport("libc", EntryPoint = "posix_fadvise", SetLastError = true)]
    static extern int posix_fadvise(int fd, long offset, long len, int advice);
}
