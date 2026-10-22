using System.Numerics;
using System.Threading;

namespace Voron.Data.Graphs;

/// <summary>
/// What every transaction on one graph shares: the decoded code parameters and the page of each committed block of code rows and of record locations.
/// One instance serves one graph for the lifetime of its index. Every entry is set once and never changes, so no commit updates it.
/// </summary>
public sealed class HnswIndexCache
{
    // Immutable once fixed, so concurrent readers share it. Null until a reader finds the graph's parameters; it never records their absence.
    private Hnsw.CodeParameters _codeParameters;

    internal Hnsw.CodeParameters CodeParameters => Volatile.Read(ref _codeParameters);

    /// <summary>Keeps the first decode of the graph's parameters and returns the one that every reader shares.</summary>
    internal Hnsw.CodeParameters PublishCodeParameters(Hnsw.CodeParameters parameters) =>
        Interlocked.CompareExchange(ref _codeParameters, parameters, null) ?? parameters;

    // The first page of each block of code rows and of record locations, 0 until a reader publishes it.
    internal readonly BlockPages CodeBlocks = new(), LocationBlocks = new();

    /// <summary>A committed block keeps its pages, so a set entry never changes.</summary>
    internal sealed class BlockPages
    {
        private long[] _pages = [];

        public long Get(long block)
        {
            var pages = Volatile.Read(ref _pages);
            return block < pages.Length ? pages[block] : 0;
        }

        /// <summary>Callers publish only committed blocks. A publish that races a growth of the array can be lost, which costs a later lookup.</summary>
        public void Publish(long block, long page)
        {
            var pages = Volatile.Read(ref _pages);
            if (block >= pages.Length)
            {
                var grown = new long[BitOperations.RoundUpToPowerOf2((ulong)block + 1)];
                pages.CopyTo(grown, 0);
                Interlocked.CompareExchange(ref _pages, grown, pages);
                pages = Volatile.Read(ref _pages);
                if (block >= pages.Length)
                    return;
            }

            pages[block] = page;
        }
    }
}
