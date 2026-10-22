using System;
using System.Collections.Generic;
using Voron.Data.Lookups;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    // A block of record locations is one page.
    private const int LocationsPerBlock = (Global.Constants.Storage.PageSize - PageHeader.SizeOf) / sizeof(long);

    /// <summary>The container location of the record of each node, 0 for a node without one, in the blocks that <paramref name="pages"/> locates.</summary>
    internal static NodeRows RecordLocations(Lookup<Int64LookupKey> pages, HnswIndexCache memo) => new(pages, sizeof(long), LocationsPerBlock, memo?.LocationBlocks);

    /// <summary>Whether the values of the nested lookup of a graph are the first pages of row blocks.</summary>
    internal static bool IsBlockLookup(Slice key) => SliceComparer.AreEqual(key, LocationPagesSlice) || SliceComparer.AreEqual(key, CodePagesSlice);

    /// <summary>Every page of the row blocks that <paramref name="blocks"/> maps.</summary>
    internal static List<long> BlockPages(Lookup<Int64LookupKey> blocks)
    {
        var pages = new List<long>();
        var it = blocks.Iterate();
        it.Reset();
        while (it.MoveNext(out long first))
        {
            var count = Paging.GetNumberOfOverflowPages(blocks.Llt.GetPage(first).OverflowSize);
            for (var page = first; page < first + count; page++)
                pages.Add(page);
        }

        return pages;
    }

    /// <summary>
    /// Rows of one size addressed by node id: node n holds row n % R of block n / R, one overflow allocation that the lookup of the blocks maps n / R to.
    /// A row that was never written reads as zeros. A committed block keeps its page in every later snapshot.
    /// </summary>
    internal readonly unsafe struct NodeRows
    {
        private readonly Lookup<Int64LookupKey> _blocks;
        private readonly int _rowBytes;
        private readonly int _rowsPerBlock;
        private readonly HnswIndexCache.BlockPages _memo;
        // 2^64 / R rounded up; zero for one row per block, whose reciprocal does not fit.
        private readonly ulong _reciprocal;

        public NodeRows(Lookup<Int64LookupKey> blocks, int rowBytes, int rowsPerBlock, HnswIndexCache.BlockPages memo)
        {
            _blocks = blocks;
            _rowBytes = rowBytes;
            _rowsPerBlock = rowsPerBlock;
            _memo = memo;
            _reciprocal = rowsPerBlock > 1 ? ulong.MaxValue / (ulong)rowsPerBlock + 1 : 0;
        }

        // PERF: The block is the node id divided by R, and R is fixed for the life of the rows. Instead of a division, the id is multiplied by the reciprocal
        // of R and the high 64 bits of the 128-bit product are the quotient, exact for every id and every R below 2^32; a multiply is far cheaper than a
        // 64-bit division. Larger ids and a single row per block divide.
        private long Block(long nodeId) =>
            _reciprocal != 0 && (ulong)nodeId <= uint.MaxValue ? (long)Math.BigMul(_reciprocal, (ulong)nodeId, out _) : nodeId / _rowsPerBlock;

        /// <summary>The row of the node, valid for the lifetime of the transaction; null when the snapshot holds no block for it.</summary>
        public byte* Read(LowLevelTransaction llt, long nodeId)
        {
            if (nodeId < 0)
                return null;

            // PERF: The row of a node is found by arithmetic on its id and one page read, with no search by node id.
            var block = Block(nodeId);
            var pageNumber = _memo?.Get(block) ?? 0;
            if (pageNumber == 0)
            {
                if (_blocks.TryGetValue(block, out pageNumber) == false)
                    return null;
                // A write transaction may hold a block that its rollback discards.
                if (llt.Flags is TransactionFlags.Read)
                    _memo?.Publish(block, pageNumber);
            }

            return llt.GetPagePointer(pageNumber) + PageHeader.SizeOf + (nodeId - block * _rowsPerBlock) * _rowBytes;
        }

        /// <summary>The value of an eight byte row, 0 when the snapshot holds no block for it.</summary>
        public long ReadInt64(LowLevelTransaction llt, long nodeId)
        {
            var row = Read(llt, nodeId);
            return row == null ? 0 : *(long*)row;
        }

        /// <summary>The row of the node in a page that the write transaction modifies.</summary>
        public byte* Write(LowLevelTransaction llt, long nodeId)
        {
            var block = Block(nodeId);
            Page page;
            if (_blocks.TryGetValue(block, out var pageNumber))
                page = llt.ModifyPage(pageNumber);
            else
            {
                page = llt.AllocateOverflowRawPage((long)_rowsPerBlock * _rowBytes, out _);
                _blocks.Add(block, page.PageNumber);
            }

            return page.DataPointer + (nodeId - block * _rowsPerBlock) * _rowBytes;
        }
    }
}
