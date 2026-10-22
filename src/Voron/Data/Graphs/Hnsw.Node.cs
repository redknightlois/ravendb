using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron.Data.Containers;
using Voron.Global;
using Voron.Impl;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public unsafe struct Node
    {
        internal const long VectorIdMask = ~0xFFF;

        // Hot fields read by the search inner loop are colocated at the start of the struct so a
        // single cache line covers PostingListId (tombstone check), _vectorSpan (kernel input),
        // Visited (RW per candidate), and the QueryDistance memo. VectorId / NodeId follow to keep
        // line 0 saturated; the resolve-only fields (edges and their indexes) land on line 1.
        public long PostingListId;
        internal UnmanagedSpan _vectorSpan;
        public int Visited;
        public int QueryDistanceVersion;
        // Cached distance between this node and the current query vector. Valid only when
        // QueryDistanceVersion == SearchState._queryVectorVersion. SearchState bumps that
        // version whenever the query vector changes (see OnQueryVector), which in turn
        // invalidates this entry without touching the node.
        public float QueryDistanceValue;
        public long VectorId;
        public long NodeId;

        // Written only through the methods that mark the node changed, so a commit persists every node whose edges it changed.
        private NativeList<NativeList<long>> _edgesPerLevel;
        public NativeList<NativeList<int>> EdgesIndexesPerLevel;

        // Number of distinct nodes with a smaller id whose base-layer list holds this node.
        internal int OlderInLinks;

        // The values of the stored record that the node was loaded from. A commit rewrites only the nodes that differ from it.
        private bool _stored;
        private bool _edgesUpdated;
        private int _storedOlderInLinks;
        private long _storedPostingListId;

        internal bool DiffersFromStoredRecord => _stored == false || _edgesUpdated || OlderInLinks != _storedOlderInLinks || PostingListId != _storedPostingListId;

        public int CountOfLevels => _edgesPerLevel.Count;

        public ReadOnlySpan<long> Edges(int level) => _edgesPerLevel[level].ToSpan();

        internal ref NativeList<long> UpdateEdges(int level)
        {
            _edgesUpdated = true;
            return ref _edgesPerLevel[level];
        }

        internal void EnsureLevels(ByteStringContext allocator, int count)
        {
            if (_edgesPerLevel.Count >= count)
                return;
            _edgesUpdated = true;
            _edgesPerLevel.SetCapacity(allocator, count);
        }

        internal void Load(long vectorId, long postingListId, int olderInLinks, NativeList<NativeList<long>> edgesPerLevel)
        {
            VectorId = vectorId;
            PostingListId = _storedPostingListId = postingListId;
            OlderInLinks = _storedOlderInLinks = olderInLinks;
            _edgesPerLevel = edgesPerLevel;
            _stored = true;
        }

        internal void CountOlderInLink(long fromNodeId, int change)
        {
            if (fromNodeId >= NodeId)
                return;
            OlderInLinks += change;
            Debug.Assert(OlderInLinks >= 0, "OlderInLinks >= 0");
        }

        // True when an edge from the node fromNodeId would be this node's only in-link from an older node.
        internal bool NeedsOlderInLinkFrom(long fromNodeId) => fromNodeId < NodeId && OlderInLinks == 0;

        public bool VectorLoaded => _vectorSpan.Length > 0;

        /// <summary>
        /// Returns the vector's memory address and length without triggering I/O.
        /// Returns false if the vector hasn't been loaded yet.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetVectorAddress(out byte* address, out int length)
        {
            if (_vectorSpan.Length > 0)
            {
                address = _vectorSpan.Address;
                length = _vectorSpan.Length;
                return true;
            }

            address = null;
            length = 0;
            return false;
        }

        public long GetVectorContainerId()
        {
            if ((VectorId & Constants.Graphs.VectorStorage.VectorContainerInternalIndexer) == 0)
                return VectorId;

            return VectorId & VectorIdMask;
        }

        public static NodeReader Decode(LowLevelTransaction llt, long id)
        {
            var span = Container.GetReadOnly(llt, new ContainerEntryId(id));
            return Decode(llt, span);
        }

        public static NodeReader Decode(LowLevelTransaction llt, Span<byte> span)
        {
            var postingListId = VariableSizeEncoding.Read<long>(span, out var pos);
            var offset = pos;
            var vectorId = VariableSizeEncoding.Read<long>(span, out pos, offset);
            offset += pos;
            var countOfLevels = VariableSizeEncoding.Read<int>(span, out pos, offset);
            offset += pos;
            var olderInLinks = VariableSizeEncoding.Read<int>(span, out pos, offset);
            offset += pos;

            return new NodeReader(llt.Allocator, span[offset..]) { PostingListId = postingListId, VectorId = vectorId, CountOfLevels = countOfLevels, OlderInLinks = olderInLinks };
        }

        public Span<byte> Encode(ref ContextBoundNativeList<byte> buffer)
        {
            int countOfLevels = _edgesPerLevel.Count;

            // posting list id, vector id, count of levels, older in-link count
            var maxSize = 4 * VariableSizeEncoding.MaximumSizeOf<long>();
            for (int i = 0; i < countOfLevels; i++)
            {
                maxSize += _edgesPerLevel[i].Count * VariableSizeEncoding.MaximumSizeOf<long>();
            }

            buffer.EnsureCapacityFor(maxSize);

            var bufferSpan = buffer.ToFullCapacitySpan();

            var pos = VariableSizeEncoding.Write(bufferSpan, PostingListId);
            pos += VariableSizeEncoding.Write(bufferSpan, VectorId, pos);
            pos += VariableSizeEncoding.Write(bufferSpan, countOfLevels, pos);
            pos += VariableSizeEncoding.Write(bufferSpan, OlderInLinks, pos);

            for (int i = 0; i < countOfLevels; i++)
            {
                Span<long> span = _edgesPerLevel[i].ToSpan();
                int len = Sorting.SortAndRemoveDuplicates(span);
                span = span[..len];
                long prev = 0;
                pos += VariableSizeEncoding.Write(bufferSpan, span.Length, pos);
                for (int j = 0; j < span.Length; j++)
                {
                    var delta = span[j] - prev;
                    prev = span[j];
                    pos += VariableSizeEncoding.Write(bufferSpan, delta, pos);
                }
            }

            return bufferSpan[..pos];
        }

        public UnmanagedSpan GetVectorUnmanagedSpan(SearchState state)
        {
            if (_vectorSpan.Length > 0)
                return _vectorSpan;

            _vectorSpan = NodeReader.ReadVector(VectorId, in state);
            return _vectorSpan;
        }

        public Span<byte> GetVector(SearchState state)
        {
            return GetVectorUnmanagedSpan(state).ToSpan();
        }

        internal void SetVector(in Options options, UnmanagedSpan span)
        {
            if ((VectorId & Constants.Graphs.VectorStorage.VectorContainerInternalIndexer) == 0)
            {
                _vectorSpan = span;
                return;
            }

            var count = (byte)(VectorId >> 1);
            var offset = count * options.VectorSizeBytes;
            _vectorSpan = new UnmanagedSpan(span.Address + offset, options.VectorSizeBytes);
        }
    }
}
