using System.Diagnostics;
using System.Runtime.InteropServices;
using Parquet;
using Voron;
using Voron.Data.Graphs;

const int D = 1536;
const int M = 16;
const int EfConstruction = 64;
const int EmbDataFieldIdx = 3;
const string BenchRoot = "/mnt/work/tq-bench";

bool skipQps = args.Contains("--no-qps");                 // accepted for arg-compat; this bench has no QPS phase
bool forceBigDataset = args.Contains("--big-dataset");
bool int8 = args.Contains("--int8");
int commitEvery = int.MaxValue;
for (int ai = 0; ai < args.Length; ai++)
    if (args[ai] == "--commit-every" && ai + 1 < args.Length && int.TryParse(args[ai + 1], out var ce))
        commitEvery = ce;
// Env-var fallback so wrappers that strip "--" flags can still set this.
var ceEnv = Environment.GetEnvironmentVariable("COMMIT_EVERY");
if (!string.IsNullOrEmpty(ceEnv) && int.TryParse(ceEnv, out var ceFromEnv))
    commitEvery = ceFromEnv;

int N = 100_000;
for (int ai = 0; ai < args.Length; ai++)
{
    if (args[ai] == "--commit-every") { ai++; continue; }
    if (args[ai].StartsWith("--")) continue;
    if (int.TryParse(args[ai], out var nArg)) N = nArg;
}

bool useBigDataset = forceBigDataset || N > 100_000;
_ = skipQps; // silence unused (this bench is insert-only by construction)

Console.WriteLine($"voron-hnsw-bench (D={D}): N={N}  M={M} efC={EfConstruction}  Int8={(int8 ? "yes" : "no")}");
Console.WriteLine();

Console.Write("Dataset: ");
Console.WriteLine(useBigDataset ? "3-large-1536-1M" : "3-small-1536-100K");
var loadSw = Stopwatch.StartNew();
Console.Write("Loading dataset... ");
var allVectors = useBigDataset ? LoadDatasetLarge(N) : LoadDataset(N);
N = allVectors.Length;
Console.WriteLine($"{loadSw.Elapsed.TotalSeconds:F1}s  ({N:N0} vectors)");

foreach (var v in allVectors) Normalize(v);

// Quantize to byte payload, free float[] buffers as we go to keep peak RSS bounded.
int vecBytesLen = int8 ? D + sizeof(float) : D * sizeof(float);
var vecBytes = new byte[N][];
for (int i = 0; i < N; i++)
{
    vecBytes[i] = int8
        ? QuantizeInt8(allVectors[i])
        : MemoryMarshal.AsBytes<float>(allVectors[i]).ToArray();
    allVectors[i] = null!;
}
GC.Collect();
GC.WaitForPendingFinalizers();

string indexPath = Path.Combine(BenchRoot, "voron-hnsw");
if (Directory.Exists(indexPath))
    Directory.Delete(indexPath, true);
Directory.CreateDirectory(indexPath);

var insertSw = Stopwatch.StartNew();
{
    var envOpts = StorageEnvironmentOptions.ForPathForTests(indexPath);
    using var env = new StorageEnvironment(envOpts);

    using (var txw = env.WriteTransaction())
    {
        Hnsw.Create(txw.LowLevelTransaction, "vectors", vecBytesLen, M, EfConstruction,
            int8 ? VectorEmbeddingType.Int8 : VectorEmbeddingType.Single);
        txw.Commit();
    }

    long regMsTotal = 0, commitMsTotal = 0;
    long lastMs = 0;
    long regStep = 50_000;
    var regSw = new Stopwatch();
    var commitSw = new Stopwatch();

    int batchStart = 0;
    while (batchStart < N)
    {
        int batchEnd = (int)Math.Min((long)batchStart + commitEvery, N);
        using var txw = env.WriteTransaction();
        using var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "vectors");
        registration.Random = new Random(454);

        regSw.Restart();
        for (int i = batchStart; i < batchEnd; i++)
        {
            // Hnsw expects entry ids with the bottom 2 bits clear; vec ids start at 1
            // and are shifted left by 2 to satisfy the contract.
            registration.Register(((long)(i + 1)) << 2, vecBytes[i]);
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
        regMsTotal += regSw.ElapsedMilliseconds;

        commitSw.Restart();
        registration.Commit(CancellationToken.None);
        txw.Commit();
        commitMsTotal += commitSw.ElapsedMilliseconds;

        batchStart = batchEnd;
    }

    double insertSec = insertSw.Elapsed.TotalSeconds;
    double insertVps = N / insertSec;
    Console.WriteLine($"[reg={regMsTotal}ms commit={commitMsTotal}ms commitEvery={(commitEvery == int.MaxValue ? "∞" : commitEvery.ToString())}] {insertSec:F1}s ({insertVps:F0} vec/s)");
}

return;

static void Normalize(float[] v)
{
    float n = 0f;
    for (int i = 0; i < v.Length; i++) n += v[i] * v[i];
    n = MathF.Sqrt(n);
    if (n > 0f) for (int i = 0; i < v.Length; i++) v[i] /= n;
}

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
