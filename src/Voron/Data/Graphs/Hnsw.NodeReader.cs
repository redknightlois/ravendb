using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.Containers;
using Voron.Global;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public unsafe ref struct NodeReader
    {
        public readonly long PostingListId;
        public readonly long VectorId;
        public readonly int CountOfLevels;
        public readonly int OlderInLinks;

        private readonly ByteStringContext _allocator;
        private readonly Span<byte> _buffer;
        private int _offset;

        /// <summary>
        /// Reads the header of a stored node record. The record is untrusted: a record that ends inside a varint, or a count
        /// larger than the bytes left to hold its items, throws <see cref="InvalidDataException"/> before anything is allocated for it.
        /// </summary>
        public NodeReader(ByteStringContext allocator, Span<byte> record)
        {
            // Every record ends with a complete varint, so a read that starts inside the record ends inside it.
            if (record.IsEmpty || record[^1] >= 0x80)
                ThrowCorruptRecord();

            _allocator = allocator;
            _buffer = record;
            PostingListId = Read<long>();
            VectorId = Read<long>();
            CountOfLevels = ReadCount();
            OlderInLinks = Read<int>();
        }

        public void LoadInto(ref Node node)
        {
            var edgesPerLevel = new NativeList<NativeList<long>>();
            edgesPerLevel.EnsureCapacityFor(_allocator, CountOfLevels);
            for (var level = 0; level < CountOfLevels; level++)
            {
                edgesPerLevel.AddUnsafe(ReadEdges());
            }
            node.Load(VectorId, PostingListId, OlderInLinks, edgesPerLevel);
        }

        /// <summary>Replaces the content of <paramref name="edges"/> with the edge ids of the next level, without allocating native memory.</summary>
        public void ReadEdges(List<long> edges)
        {
            edges.Clear();
            var count = ReadCount();
            long prev = 0;
            for (int i = 0; i < count; i++)
            {
                prev += Read<long>();
                edges.Add(prev);
            }
        }

        private NativeList<long> ReadEdges()
        {
            var count = ReadCount();
            var list = new NativeList<long>();
            list.EnsureCapacityFor(_allocator, count);
            long prev = 0;
            for (int i = 0; i < count; i++)
            {
                prev += Read<long>();
                Debug.Assert(prev >= 0, "prev >= 0");
                list.AddUnsafe(prev);
            }

            return list;
        }

        private T Read<T>() where T : unmanaged
        {
            if (_offset >= _buffer.Length)
                ThrowCorruptRecord();

            var value = VariableSizeEncoding.Read<T>(_buffer, out var size, _offset);
            _offset += size;
            return value;
        }

        // Each counted item takes at least one byte of the record.
        private int ReadCount()
        {
            var count = Read<int>();
            if ((uint)count > (uint)(_buffer.Length - _offset))
                ThrowCorruptRecord();
            return count;
        }

        [DoesNotReturn]
        private static void ThrowCorruptRecord() => throw new InvalidDataException("The HNSW node record is corrupted.");

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
