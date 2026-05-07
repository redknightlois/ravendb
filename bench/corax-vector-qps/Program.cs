using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Utils;
using Parquet;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.Graphs;
using VectorOptionsCorax = Corax.Mappings.VectorOptions;

// HNSW insertion-campaign benchmark.
// Derived from the QPS-graduation harness (corax-vector-qps); insert is now primary,
// QPS matrix is a regression gate at ef ∈ {16,32,128,512}, Int8 variant is gated behind
// --verify / --with-int8. The database directory is always wiped per run.
//
// Modes (OBJECTIVES two-tier N policy):
//   default           → STANDARD dev    (N=25000,  NQ=200,   Single only)
//   --verify          → STANDARD verify (N=100000, NQ=500,   Single + Int8)
//   --big             → BIG dev         (N=100000, NQ=500,   Single only)
//   --big --verify    → BIG verify      (N=250000, NQ=1000,  Single + Int8)
//   --with-int8       → force Int8 on in any dev run
//   [N] positional    → custom N (dev mode, NQ=max(NQ, 500))
//
// Data: DBpedia-OpenAI3 1536d (HuggingFace Qdrant shards), f32 cosine, normalized.

const int K = 10;
const int D = 1536;
const int M = 16, EfConstruction = 64;
int[] EfValues = { 16, 32, 128, 512 };

bool big     = args.Contains("--big");
bool verify  = args.Contains("--verify");
bool withInt8 = verify || args.Contains("--with-int8");
bool skipQps = args.Contains("--no-qps");
bool forceBigDataset = args.Contains("--big-dataset");
bool reuse   = args.Contains("--reuse");
bool cycle   = args.Contains("--cycle"); // cache-friendly: workers cycle through queryBytes (queries repeat); off = partitioned (each query executed once)
int commitEvery = int.MaxValue;
int nqOverride = -1;
int workersOverride = -1;
for (int ai = 0; ai < args.Length; ai++)
{
    if (args[ai] == "--commit-every" && ai + 1 < args.Length && int.TryParse(args[ai + 1], out var ce))
        commitEvery = ce;
    if (args[ai] == "--nq" && ai + 1 < args.Length && int.TryParse(args[ai + 1], out var nq))
        nqOverride = nq;
    if (args[ai] == "--workers" && ai + 1 < args.Length && int.TryParse(args[ai + 1], out var w))
        workersOverride = w;
}
int Workers = workersOverride > 0 ? workersOverride : Math.Max(1, Environment.ProcessorCount);

int nodeCacheSize = 0;
for (int ai = 0; ai < args.Length; ai++)
    if (args[ai] == "--node-cache" && ai + 1 < args.Length && int.TryParse(args[ai + 1], out var nc))
        nodeCacheSize = nc;

int N, NQ;
if (big) { N = verify ? 250_000 : 100_000; NQ = verify ? 1000 : 500; }
else     { N = verify ? 100_000 :  25_000; NQ = verify ?  500 : 200; }

for (int ai = 0; ai < args.Length; ai++)
{
    if (args[ai] == "--commit-every") { ai++; continue; } // skip value
    if (args[ai] == "--nq") { ai++; continue; }
    if (args[ai] == "--workers") { ai++; continue; }
    if (args[ai] == "--node-cache") { ai++; continue; }
    if (args[ai].StartsWith("--")) continue;              // bool flag
    if (int.TryParse(args[ai], out var nArg)) { N = nArg; NQ = Math.Max(NQ, 500); }
}

if (nqOverride > 0) NQ = nqOverride;

const int EmbDataFieldIdx = 3;
const string BenchRoot = "/mnt/work/tq-bench";
const double QueryGateSeconds = 10.0;

Console.WriteLine($"TQ-Insert DBpedia-OpenAI3 (D={D}): N≤{N} NQ={NQ} k={K}  M={M} efC={EfConstruction}");
Console.WriteLine($"Mode: {(big ? "BIG" : "STD")} {(verify ? "verify" : "dev")}  Int8: {(withInt8 ? "yes" : "no")}");
Console.WriteLine($"EF:   {string.Join(", ", EfValues)}  query-gate: {QueryGateSeconds:F0}s per ef  workers: {Workers}  query-mode: {(cycle ? "cycle (cache-friendly)" : "partition (cache-hostile)")}  node-cache: {(nodeCacheSize > 0 ? nodeCacheSize.ToString("N0") + " nodes" : "disabled")}");
Console.WriteLine();

// ---- dataset ----
// BIG verify (N=250K) exceeds the 100K-small-dataset cap, so it uses the
// 1M-large-1536 sibling. Other modes stay on the 100K-small dataset for
// continuity with the QPS campaign's recall baselines.
bool useBigDataset = forceBigDataset || (big && verify) || N > 100_000;
string datasetLabel = useBigDataset ? "3-large-1536-1M" : "3-small-1536-100K";
Console.WriteLine($"Dataset: {datasetLabel}");
Console.Write("Loading dataset... ");
var sw = Stopwatch.StartNew();
var allVectors = useBigDataset ? LoadDatasetLarge(N) : LoadDataset(N);
N = allVectors.Length;
Console.WriteLine($"{sw.Elapsed.TotalSeconds:F1}s  ({N:N0} vectors)");

// Normalize in place for cosine
foreach (var v in allVectors) Normalize(v);

var rng = new Random(42);
var qIdx = Enumerable.Range(0, N).OrderBy(_ => rng.Next()).Take(NQ).ToArray();
var queries = qIdx.Select(i => (float[])allVectors[i].Clone()).ToArray();

// Recall ground truth scales O(NQ × N), so we cap it: only the first
// `truthCap` queries get a real top-K reference set; the rest get an
// empty set (CountHits returns 0 for those, so they don't move recall).
// QPS, however, is measured across ALL NQ queries.
int truthCap = Math.Min(NQ, 2000);
var truth = new HashSet<long>[NQ];
for (int q = truthCap; q < NQ; q++) truth[q] = new HashSet<long>();
if (!skipQps)
{
    Console.Write($"Computing ground truth (brute force, first {truthCap:N0} of {NQ:N0})... ");
    sw.Restart();
    Parallel.For(0, truthCap, q =>
    {
        var sims = new (float sim, long id)[N];
        var qv = queries[q];
        for (int i = 0; i < N; i++)
        {
            float dot = 0f;
            var av = allVectors[i];
            for (int j = 0; j < D; j++) dot += qv[j] * av[j];
            sims[i] = (dot, i + 1);
        }
        Array.Sort(sims, (a, b) => b.sim.CompareTo(a.sim));
        var set = new HashSet<long>();
        for (int i = 0; i < K; i++) set.Add(sims[i].id);
        truth[q] = set;
    });
    Console.WriteLine($"{sw.Elapsed.TotalSeconds:F1}s");
}
else
{
    for (int q = 0; q < truthCap; q++) truth[q] = new HashSet<long>();
    Console.WriteLine("(skipping ground truth — --no-qps)");
}
Console.WriteLine();

// ---- results header ----
string header = $"{"Variant",-14} {"Insert v/s",11} {"Insert s",9}";
foreach (var ef in EfValues) header += $" {$"QPS@{ef}",9} {$"R@{ef}",8}";
Console.WriteLine(header);
Console.WriteLine(new string('-', header.Length));

// ---- Single (f32) — primary ----
RunVariant("Single (f32)", VectorEmbeddingType.Single, quantize: null);

// ---- Int8 — gate ----
if (withInt8)
    RunVariant("Int8", VectorEmbeddingType.Int8, quantize: QuantizeInt8);

return;

// ===================================================================
// runner
// ===================================================================

void RunVariant(string label, VectorEmbeddingType embType, Func<float[], byte[]>? quantize)
{
    string slug = label.Replace(" ", "_").Replace("(", "").Replace(")", "");
    string indexPath = Path.Combine(BenchRoot, slug);
    if (!reuse)
    {
        if (Directory.Exists(indexPath))
            Directory.Delete(indexPath, true);
        Directory.CreateDirectory(indexPath);
    }

    var mapping = IndexFieldsMappingBuilder
        .CreateForWriter(false)
        .AddBinding(0, "id()")
        .AddBinding(1, "Vector", vectorOptions: new VectorOptionsCorax
        {
            NumberOfEdges = M,
            NumberOfCandidates = EfConstruction,
            VectorEmbeddingType = embType,
        })
        .Build();

    // Precompute vector byte payloads so the insert loop measures the HNSW path,
    // not quantization cost (quantization is I/O-free and always amortized).
    // Skip when --reuse: we won't insert, no need for the 6 GB byte buffer.
    var vecBytes = reuse ? null! : new byte[N][];
    if (!reuse)
    {
        for (int i = 0; i < N; i++)
        {
            vecBytes[i] = quantize != null
                ? quantize(allVectors[i])
                : MemoryMarshal.AsBytes<float>(allVectors[i]).ToArray();
            // Release per-vector float buffer as we go — at N=1M each float[] is ~6 KB,
            // total ~6 GB; holding both allVectors and vecBytes is the usual OOM trigger.
            // Queries are cloned earlier (qIdx) so they survive this nulling.
            if (skipQps)
                allVectors[i] = null!;
        }
        if (skipQps)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }

    var queryBytes = new byte[NQ][];
    for (int i = 0; i < NQ; i++)
        queryBytes[i] = quantize != null
            ? quantize(queries[i])
            : MemoryMarshal.AsBytes<float>(queries[i]).ToArray();

    // -- INSERT --
    // Batched commits mirror production RavenDB indexing: a single uncommitted
    // transaction retains ~32KB scratch per vector (big overflow pages), so
    // holding 500K+ uncommitted thrashes the memory-mapped scratch buffer.
    // Commit every `commitEvery` records to bound scratch working set.
    double insertSec = 0;
    double insertVps = 0;
    if (!reuse)
    {
        Console.Write($"  [{label}] inserting {N:N0} vectors... ");
        var insertSw = Stopwatch.StartNew();
        {
            var envOpts = StorageEnvironmentOptions.ForPathForTests(indexPath);
            using var env = new StorageEnvironment(envOpts);
            long regMsTotal = 0, commitMsTotal = 0;
            long lastMs = 0;
            long regStep = skipQps ? 50_000 : long.MaxValue;
            var regSw = new Stopwatch();
            var commitSw2 = new Stopwatch();

            int batchStart = 0;
            int batchIdx = 0;
            while (batchStart < N)
            {
                int batchEnd = (int)Math.Min((long)batchStart + commitEvery, N);
                var writer = new IndexWriter(env, mapping, SupportedFeatures.All);
                regSw.Restart();
                for (int i = batchStart; i < batchEnd; i++)
                {
                    var id = $"vec/{i + 1}";
                    using var entry = writer.Index(id);
                    entry.Write(0, System.Text.Encoding.UTF8.GetBytes(id));
                    entry.WriteVector(1, "Vector", vecBytes[i]);
                    entry.EndWriting();
                    if ((i + 1) % regStep == 0)
                    {
                        long now = insertSw.ElapsedMilliseconds;
                        long step = now - lastMs;
                        long workingSetMb = Process.GetCurrentProcess().WorkingSet64 >> 20;
                        long gen2 = GC.CollectionCount(2);
                        Console.Error.WriteLine($"    reg[{(i + 1) / 1000}K] step={step}ms total={now}ms RSS={workingSetMb}MB gen2={gen2}");
                        lastMs = now;
                    }
                }
                long batchRegMs = regSw.ElapsedMilliseconds;
                regMsTotal += batchRegMs;
                commitSw2.Restart();
                writer.Commit();
                long batchCommitMs = commitSw2.ElapsedMilliseconds;
                commitMsTotal += batchCommitMs;
                writer.Dispose();
                long rssMb = Process.GetCurrentProcess().WorkingSet64 >> 20;
                Console.Error.WriteLine($"    batch[{batchIdx,3}] {batchStart / 1000}K..{batchEnd / 1000}K  reg={batchRegMs,5}ms commit={batchCommitMs,5}ms  RSS={rssMb}MB");
                batchStart = batchEnd;
                batchIdx++;
            }
            Console.Write($"[reg={regMsTotal}ms commit={commitMsTotal}ms commitEvery={(commitEvery == int.MaxValue ? "∞" : commitEvery.ToString())}] ");
        }
        insertSec = insertSw.Elapsed.TotalSeconds;
        insertVps = N / insertSec;
        Console.WriteLine($"{insertSec:F1}s ({insertVps:F0} vec/s)");
    }
    else
    {
        Console.WriteLine($"  [{label}] --reuse: skipping insert, querying existing graph at {indexPath}");
    }

    // -- QUERIES --
    var qps = new double[EfValues.Length];
    var recall = new double[EfValues.Length];
    if (!skipQps)
    {
        var envOpts = StorageEnvironmentOptions.ForPathForTests(indexPath);
        using var env = new StorageEnvironment(envOpts);
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        // Build a single NodeCache for the "Vector" field if requested. Mirrors
        // CoraxIndexPersistence.BuildVectorCacheSnapshot: keyed by the field-name
        // slice via SliceComparer, immutable after construction so all workers
        // can share it.
        Dictionary<Slice, Hnsw.NodeCache> caches = null;
        Slice fieldNameSlice = default;
        IDisposable fieldNameScope = null;
        if (nodeCacheSize > 0)
        {
            fieldNameScope = Slice.From(bsc, "Vector", out fieldNameSlice);
            using var rtx = env.ReadTransaction();
            var cacheBuildSw = Stopwatch.StartNew();
            var cache = Hnsw.NodeCache.Build(rtx.LowLevelTransaction, fieldNameSlice, nodeCacheSize);
            cacheBuildSw.Stop();
            if (cache != null && cache.Count > 0)
            {
                caches = new Dictionary<Slice, Hnsw.NodeCache>(SliceComparer.Instance) { [fieldNameSlice] = cache };
                Console.WriteLine($"  [{label}] node-cache: built {cache.Count:N0} nodes in {cacheBuildSw.Elapsed.TotalSeconds:F1}s (budget {nodeCacheSize:N0})");
            }
            else
            {
                Console.WriteLine($"  [{label}] node-cache: empty (budget {nodeCacheSize:N0}) — disabled");
            }
        }

        Warmup(env, mapping, bsc, queryBytes);

        for (int e = 0; e < EfValues.Length; e++)
        {
            (qps[e], recall[e]) = RunQueriesGated(env, mapping, queryBytes, truth, EfValues[e], Workers, cycle, caches);
        }

        fieldNameScope?.Dispose();
    }

    // -- row --
    var row = $"{label,-14} {insertVps,11:F0} {insertSec,9:F1}";
    for (int e = 0; e < EfValues.Length; e++)
        row += $" {qps[e],9:F0} {recall[e],8:P2}";
    Console.WriteLine(row);
    Console.WriteLine();

    mapping.Dispose();
}

static (double qps, double recall) RunQueriesGated(
    StorageEnvironment env, IndexFieldsMapping mapping,
    byte[][] queryBytes, HashSet<long>[] truth, int ef, int parallelism, bool cycle,
    Dictionary<Slice, Hnsw.NodeCache> nodeCaches)
{
    long totalHits = 0, totalCount = 0, totalQueries = 0;
    var sw = Stopwatch.StartNew();

    // Two modes:
    //   cycle=true  (cache-friendly): every worker round-robins through the
    //                full queryBytes, so the same query vectors are
    //                re-executed many times within the gate. L3 stays hot
    //                across iterations — gives "best-case" QPS.
    //   cycle=false (cache-hostile, default): workers partition queryBytes
    //                into disjoint slices and walk their slice once. No
    //                query is executed twice across all workers — gives
    //                "worst-case / production-traffic-shape" QPS.
    var po = new ParallelOptions { MaxDegreeOfParallelism = parallelism };
    int chunk = queryBytes.Length / parallelism;
    Parallel.For(0, parallelism, po, t =>
    {
        using var searcher = new IndexSearcher(env, mapping);
        if (nodeCaches != null) searcher.AttachVectorNodeCaches(nodeCaches);
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var metadata = mapping.GetByFieldId(1).Metadata;
        var ids = new long[K * 2];
        long localHits = 0, localCount = 0, localQueries = 0;

        if (cycle)
        {
            int idx = t;
            while (sw.Elapsed.TotalSeconds < QueryGateSeconds)
            {
                int q = idx % queryBytes.Length;
                using var vv = MakeVectorValue(bsc, queryBytes[q]);
                var match = searcher.VectorSearch(metadata, vv, -1f, ef, false, true, null, scanningThreshold: 0);
                int c = match.Fill(ids);
                if (truth[q].Count > 0)
                {
                    localHits += CountHits(ids, c, truth[q]);
                    localCount += Math.Min(c, K);
                }
                localQueries++;
                idx++;
            }
        }
        else
        {
            int start = t * chunk;
            int end = (t == parallelism - 1) ? queryBytes.Length : start + chunk;

            for (int q = start; q < end && sw.Elapsed.TotalSeconds < QueryGateSeconds; q++)
            {
                using var vv = MakeVectorValue(bsc, queryBytes[q]);
                var match = searcher.VectorSearch(metadata, vv, -1f, ef, false, true, null, scanningThreshold: 0);
                int c = match.Fill(ids);
                if (truth[q].Count > 0)
                {
                    localHits += CountHits(ids, c, truth[q]);
                    localCount += Math.Min(c, K);
                }
                localQueries++;
            }
        }

        Interlocked.Add(ref totalHits, localHits);
        Interlocked.Add(ref totalCount, localCount);
        Interlocked.Add(ref totalQueries, localQueries);
    });

    double elapsed = sw.Elapsed.TotalSeconds;
    double qps = totalQueries / elapsed;
    double recall = totalCount > 0 ? (double)totalHits / totalCount : 0;
    return (qps, recall);
}

// ===================================================================
// helpers
// ===================================================================

static void Warmup(StorageEnvironment env, IndexFieldsMapping mapping, ByteStringContext bsc,
    byte[][] queryBytes)
{
    using var searcher = new IndexSearcher(env, mapping);
    var metadata = mapping.GetByFieldId(1).Metadata;
    Span<long> ids = stackalloc long[K * 2];
    for (int q = 0; q < Math.Min(10, queryBytes.Length); q++)
    {
        using var vv = MakeVectorValue(bsc, queryBytes[q]);
        var m = searcher.VectorSearch(metadata, vv, -1f, 64, false, true, null, scanningThreshold: 0);
        m.Fill(ids);
    }
}

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

static void Normalize(float[] v)
{
    float n = 0f;
    for (int i = 0; i < v.Length; i++) n += v[i] * v[i];
    n = MathF.Sqrt(n);
    if (n > 0f) for (int i = 0; i < v.Length; i++) v[i] /= n;
}

// Int8 quantization: symmetric per-vector max-abs scale, magnitude stored as trailing float.
// Layout: [D sbytes] [float magnitude]. Same contract as VectorQuantizer.TryToInt8.
static byte[] QuantizeInt8(float[] v)
{
    int dims = v.Length;
    var buf = new byte[dims + sizeof(float)];
    float maxAbs = 0f;
    for (int i = 0; i < dims; i++)
    {
        float a = MathF.Abs(v[i]);
        if (a > maxAbs) maxAbs = a;
    }
    float scale = maxAbs > 0f ? 127f / maxAbs : 0f;
    for (int i = 0; i < dims; i++)
    {
        int s = (int)MathF.Round(v[i] * scale);
        if (s > 127) s = 127; else if (s < -128) s = -128;
        buf[i] = (byte)(sbyte)s;
    }
    MemoryMarshal.Write(buf.AsSpan(dims), in maxAbs);
    return buf;
}

// 1M dataset: text-embedding-3-large, D=1536, float64 embeddings, 26 shards ~38K rows each.
// Only used for BIG verify (N=250K) — roughly 7 shards, ~2.6 GB one-time download.
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
            var url = string.Format(ShardUrlTemplate, shard);
            using var response = client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead).GetAwaiter().GetResult();
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
            // Large dataset stores float64 — convert per row to float32.
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
    // HF publishes as 4 shards (0000..0003.parquet), 25K rows each.
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
            var url = string.Format(ShardUrlTemplate, shard);
            using var response = client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead).GetAwaiter().GetResult();
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
