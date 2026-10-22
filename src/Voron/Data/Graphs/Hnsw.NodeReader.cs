using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.Containers;
using Voron.Global;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public unsafe ref struct NodeReader(ByteStringContext allocator, Span<byte> buffer)
    {
        public long PostingListId;
        public long VectorId;
        public int CountOfLevels;
        public int OlderInLinks;

        private int _offset;
        private readonly Span<byte> _buffer = buffer;

        public void LoadInto(ref Node node)
        {
            node.VectorId = VectorId;
            node.PostingListId = PostingListId;
            node.EdgesPerLevel.EnsureCapacityFor(allocator, CountOfLevels);
            node.OlderInLinks = OlderInLinks;
            for (var level = 0; level < CountOfLevels; level++)
            {
                node.EdgesPerLevel.AddUnsafe(ReadEdges());
            }
        }

        /// <summary>Replaces the content of <paramref name="edges"/> with the edge ids of the next level, without allocating native memory.</summary>
        public void ReadEdges(List<long> edges)
        {
            edges.Clear();
            var count = VariableSizeEncoding.Read<int>(_buffer, out int offset, _offset);
            _offset += offset;
            long prev = 0;
            for (int i = 0; i < count; i++)
            {
                prev += VariableSizeEncoding.Read<long>(_buffer, out offset, _offset);
                _offset += offset;
                edges.Add(prev);
            }
        }

        private NativeList<long> ReadEdges()
        {
            var count = VariableSizeEncoding.Read<int>(_buffer, out int offset, _offset);
            _offset += offset;
            var list = new NativeList<long>();
            list.EnsureCapacityFor(allocator, count);
            long prev = 0;
            for (int i = 0; i < count; i++)
            {
                prev += VariableSizeEncoding.Read<long>(_buffer, out offset, _offset);
                _offset += offset;
                Debug.Assert(prev >= 0, "prev >= 0");
                list.AddUnsafe(prev);
            }

            return list;
        }

        public UnmanagedSpan ReadVector(in SearchState state) => ReadVector(VectorId, in state);

        public static UnmanagedSpan ReadVector(long vectorId, in SearchState state)
        {
            if ((vectorId & Constants.Graphs.VectorStorage.VectorContainerInternalIndexer) == 0)
            {
                Container.Get(state.Llt, new ContainerEntryId(vectorId), out var item);
                var vectorSpan = new UnmanagedSpan(item.Address, item.Length);
                Debug.Assert(state.Options.VectorSizeBytes == vectorSpan.Length, "state.Options.VectorSizeBytes == vectorSpan.Length");
                return vectorSpan;
            }

            var count = (byte)(vectorId >> 1);
            var containerId = vectorId & Node.VectorIdMask;
            Container.Get(state.Llt, new ContainerEntryId(containerId), out var container);
            var offset = count * state.Options.VectorSizeBytes;
            Debug.Assert(offset >= 0 && offset + state.Options.VectorSizeBytes <= container.Length, "offset >= 0 && offset + state.Options.VectorSizeBytes <= container.Length");
            return new UnmanagedSpan(container.Address + offset, state.Options.VectorSizeBytes);
        }
    }
}
