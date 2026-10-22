using Voron.Data.Lookups;
using Voron.Impl;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    /// <summary>
    /// Rows of one size addressed by node id: node n holds row n % R of block n / R, one overflow allocation that the lookup of the blocks maps n / R to.
    /// A row that was never written reads as zeros.
    /// </summary>
    internal readonly unsafe struct NodeRows
    {
        private readonly Lookup<Int64LookupKey> _blocks;
        private readonly int _rowBytes;
        private readonly int _rowsPerBlock;

        public NodeRows(Lookup<Int64LookupKey> blocks, int rowBytes, int rowsPerBlock)
        {
            _blocks = blocks;
            _rowBytes = rowBytes;
            _rowsPerBlock = rowsPerBlock;
        }

        /// <summary>The row of the node, valid for the lifetime of the transaction; null when the snapshot holds no block for it.</summary>
        public byte* Read(LowLevelTransaction llt, long nodeId)
        {
            if (nodeId < 0)
                return null;

            if (_blocks.TryGetValue(nodeId / _rowsPerBlock, out var pageNumber) == false)
                return null;

            return llt.GetPage(pageNumber).DataPointer + nodeId % _rowsPerBlock * _rowBytes;
        }

        /// <summary>The row of the node in a page that the write transaction modifies.</summary>
        public byte* Write(LowLevelTransaction llt, long nodeId)
        {
            var block = nodeId / _rowsPerBlock;
            Page page;
            if (_blocks.TryGetValue(block, out var pageNumber))
                page = llt.ModifyPage(pageNumber);
            else
            {
                page = llt.AllocateOverflowRawPage((long)_rowsPerBlock * _rowBytes, out _);
                _blocks.Add(block, page.PageNumber);
            }

            return page.DataPointer + nodeId % _rowsPerBlock * _rowBytes;
        }
    }
}
