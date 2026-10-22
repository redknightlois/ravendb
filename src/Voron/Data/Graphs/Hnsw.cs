using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Threading;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Tensors;
using Sparrow.Server.Utils;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Global;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;
using Container = Voron.Data.Containers.Container;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    public static void Create(LowLevelTransaction llt, string name, int vectorSizeBytes, int numberOfEdges, int numberOfCandidates, VectorEmbeddingType embeddingType)
    {
        using var _ = Slice.From(llt.Allocator, name, out var slice);
        Create(llt, slice, vectorSizeBytes, numberOfEdges, numberOfCandidates, embeddingType);
    }

    public static void Create(LowLevelTransaction llt, Slice name, int vectorSizeBytes, int numberOfEdges, int numberOfCandidates, VectorEmbeddingType embeddingType)
    {
        var tree = llt.Transaction.CreateTree(name);
        ref readonly var stateHeader = ref tree.ReadHeader();
        if (stateHeader.NumberOfEntries is not 0)
            return; // already created

        // global creation for all HNSWs in the database
        var vectorsContainerId = CreateHnswGlobalState(llt);
        ContainerId storage = Container.Create(llt);
        tree.LookupFor<Int64LookupKey>(LocationPagesSlice);
        tree.LookupFor<Int64LookupKey>(NodesByVectorIdSlice);

        var similarityMethod = embeddingType switch
        {
            VectorEmbeddingType.Single => SimilarityMethod.CosineSimilaritySingles,
            VectorEmbeddingType.Int8 => SimilarityMethod.CosineSimilarityI8,
            VectorEmbeddingType.Binary => SimilarityMethod.HammingDistance,
            _ => throw new InvalidOperationException($"Unexpected value of {nameof(VectorEmbeddingType)}: {embeddingType}")
        };

        // VectorEmbeddingType.Single is stored in the NormalizedTensor layout: unit vector followed by a trailing float L2 norm. Callers pass the raw payload size (dims * sizeof(float)); the trailing-norm bytes are added here so callers do not need to know about the layout.
        if (embeddingType == VectorEmbeddingType.Single)
            vectorSizeBytes += MagnitudeSizeInBytes;

        var options = new Options
        {
            Version = Constants.Graphs.HnswVersion.CurrentVersion,
            VectorSizeBytes = vectorSizeBytes,
            CountOfVectors = 0,
            Container = storage,
            NumberOfEdges = numberOfEdges,
            NumberOfCandidates = numberOfCandidates,
            SimilarityMethod = similarityMethod
        };
        if (similarityMethod == SimilarityMethod.CosineSimilaritySingles)
        {
            options.CodeRowsPerBlock = CodeRowsPerBlock(CodeParameters.RowBytesOf((vectorSizeBytes - MagnitudeSizeInBytes) / sizeof(float)));
            tree.LookupFor<Int64LookupKey>(CodePagesSlice);
        }

        using (tree.DirectAdd(OptionsSlice, sizeof(Options), out var output))
        {
            Unsafe.Write(output, options);
        }
    }

    /// <summary>
    /// Reads the options of a graph tree. Every open of a graph goes through here, so a graph at another
    /// format version, older or newer, is refused before any of its node records is decoded.
    /// </summary>
    internal static Options ReadOptions(Tree tree)
    {
        var options = Unsafe.Read<Options>(tree.DirectRead(OptionsSlice));
        if (options.Version != Constants.Graphs.HnswVersion.CurrentVersion)
            throw new NotSupportedException($"The vector graph '{tree.Name}' has format version {options.Version}, but version {Constants.Graphs.HnswVersion.CurrentVersion} is required. Reset the index to rebuild the graph.");
        return options;
    }

    public static bool HasGraphOfOtherVersion(Transaction tx) => GraphsOf(tx).Exists(g => g.Version != Constants.Graphs.HnswVersion.CurrentVersion);

    /// <summary>
    /// Lists every vector graph tree of the environment with its stored format version. Reads only the options of each tree,
    /// so it works on a graph of any version.
    /// </summary>
    public static List<(Slice Name, ushort Version)> GraphsOf(Transaction tx)
    {
        var graphs = new List<(Slice, ushort)>();
        using var it = tx.LowLevelTransaction.RootObjects.Iterate(false);
        if (it.Seek(Slices.BeforeAllKeys) == false)
            return graphs;

        do
        {
            var name = it.CurrentKey.Clone(tx.Allocator);
            if (tx.GetRootObjectType(name) != RootObjectType.VariableSizeTree)
                continue;

            var tree = tx.ReadTree(name);
            if (IsGraph(tree))
                graphs.Add((name, Unsafe.Read<Options>(tree.DirectRead(OptionsSlice)).Version));
        } while (it.MoveNext());

        return graphs;
    }

    // A Corax tree can hold a user-named key, but never a value of the size of the options.
    internal static bool IsGraph(Tree tree) => tree.GetDataSize(OptionsSlice) == sizeof(Options);

    /// <summary>
    /// A graph tree holds raw values beside its nested lookups, so a walker of the graph tree reads a root object header only under these keys.
    /// </summary>
    internal static bool IsNestedLookup(Slice key) =>
        SliceComparer.AreEqual(key, LocationPagesSlice) || SliceComparer.AreEqual(key, NodesByVectorIdSlice) || SliceComparer.AreEqual(key, CodePagesSlice);

    /// <summary>
    /// Translates a minimum similarity on the [0, 1] scale into the maximum distance the similarity method reports for
    /// it. Cosine distance is <c>1 - cos</c> over [0, 2] and similarity is <c>(cos + 1) / 2</c>, so the bound is
    /// <c>2 * (1 - minimumSimilarity)</c>. Hamming distance counts differing bits and similarity is the fraction of
    /// matching bits, so the bound is the number of bits that may differ: <c>numberOfBits * (1 - minimumSimilarity)</c>.
    /// </summary>
    internal static float MinimumSimilarityToDistance(SimilarityMethod similarityMethod, int vectorSizeBytes, float minimumSimilarity)
    {
        switch (similarityMethod)
        {
            case SimilarityMethod.CosineSimilaritySingles:
            case SimilarityMethod.CosineSimilarityI8:
                return 2f * (1.0f - minimumSimilarity);
            case SimilarityMethod.HammingDistance:
                return vectorSizeBytes * 8 * (1f - minimumSimilarity); // number_of_bits * (1 - minimum_similarity) = allowed differing bits
            default:
                throw new InvalidDataException($"Unknown similarity method {similarityMethod}");
        }
    }

    /// <summary>
    /// Returns the distance kernel to use for a given HNSW <see cref="Options"/>. The operands of
    /// <see cref="SimilarityMethod.CosineSimilaritySingles"/> are pre-normalized (NormalizedTensor layout),
    /// so the dot-only kernel applies.
    /// </summary>
    internal static delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> GetDistanceKernel(in Options options)
    {
        return options.SimilarityMethod switch
        {
            SimilarityMethod.CosineSimilaritySingles => &CosineDistanceSinglesNormalized,
            SimilarityMethod.CosineSimilarityI8 => &CosineDistanceI8,
            SimilarityMethod.HammingDistance => &HammingDistance,
            _ => throw new ArgumentOutOfRangeException(nameof(options.SimilarityMethod), options.SimilarityMethod, null)
        };
    }

    private static ContainerId ReadGlobalVectorsContainerId(LowLevelTransaction llt)
    {
        var config = llt.Transaction.ReadTree(HnswGlobalConfigSlice);
        var read = config.DirectRead(VectorsContainerIdSlice);
        return new ContainerId(Unsafe.Read<long>(read));
    }

    private static ContainerId CreateHnswGlobalState(LowLevelTransaction llt)
    {
        llt.Transaction.CompactTreeFor(VectorsIdByHashSlice);
        var config = llt.Transaction.CreateTree(HnswGlobalConfigSlice);
        var read = config.DirectRead(VectorsContainerIdSlice);
        if (read is not null)
            return new ContainerId(Unsafe.Read<long>(read));

        ContainerId vectorsContainerId = Container.Create(llt);
        config.Add(VectorsContainerIdSlice, (long)vectorsContainerId);
        return vectorsContainerId;
    }

    public partial class SearchState : IDisposable
    {
        private readonly PriorityQueue<int, float> _candidatesQ = new();
        private readonly PriorityQueue<int, float> _nearestEdgesQ = new();
        private readonly Dictionary<long, int> _nodeIdToIdx = new();
        private NativeList<Node> _nodes = default;
        private NativeList<int> _newNodes = default;
        private readonly Tree _tree;
        private readonly NodeRows _locations;
        public readonly LowLevelTransaction Llt;
        // _visitsCounter versions Node.Visited; every traversal start bumps it so the visited
        // set is reset in O(1). _queryVectorVersion versions Node.QueryDistanceVersion; it is
        // bumped by OnQueryVector only when the query vector changes, so a node's cached distance
        // survives across traversals that reuse the same query vector. Both start at 1 so
        // freshly-loaded nodes (fields default to 0) never spuriously match.
        private int _visitsCounter = 1;
        private int _queryVectorVersion = 1;
        private Memory<byte> _lastQueryVector;
        public readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> SimilarityCalc;
        public readonly bool IsEmpty;
        // Every float32 graph carries a code for each of its nodes (see CodeParameters); the other graphs carry none.
        private readonly bool _carriesCodes;
        private readonly int _codeRowBytes;
        private readonly NodeRows _codeRows;
        private readonly HnswIndexCache _cache;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope? _queryNormalizationScope;

        public Span<Node> Nodes => _nodes.ToSpan();
        public Tree Tree => _tree;

        public int CreatedNodes => _newNodes.Count;

        private List<NativeList<Node>> _retiredNodeStores;

        // Grow the node array by copying into a fresh buffer and keeping the old one alive (released on
        // dispose) instead of freeing it, so a parallel-placement worker dereferencing a ref into the
        // array never observes a buffer freed by a grow mid-dispatch (RavenDB-26809).
        private void GrowNodesRetainingOldBuffer()
        {
            var grown = new NativeList<Node>();
            grown.Initialize(Llt.Allocator, Math.Max(InitialNodesCapacityWithCache, _nodes.Capacity * 2));
            grown.AddRangeUnsafe(_nodes.ToSpan());
            (_retiredNodeStores ??= []).Add(_nodes);
            // Publish the copied buffer before the new pointer becomes observable, so a concurrent
            // reader that sees the new pointer also sees the copied contents (weak-memory archs).
            Thread.MemoryBarrier();
            _nodes = grown;
        }

        // Force a grow of the node array backing store; returns true if the buffer moved. Test-only.
        internal bool ForceGrowNodesForTesting()
        {
            var before = (nint)_nodes.RawItems;
            GrowNodesRetainingOldBuffer();
            return (nint)_nodes.RawItems != before;
        }

        // Test-only: grow the node array, then claim a buffer of the vacated capacity and fill it with a
        // poison pattern. If the grow freed the old buffer (the RavenDB-26809 bug) the allocator reuses
        // that same memory and a dangling reader sees poison; if it was retained (the fix) the reader is
        // unaffected. The poison buffer is retained so it occupies the region past the reader's resume.
        internal bool GrowNodesAndPoisonVacatedForTesting()
        {
            var before = (nint)_nodes.RawItems;
            int vacatedCapacity = _nodes.Capacity;
            GrowNodesRetainingOldBuffer();

            var poison = new NativeList<Node>();
            poison.Initialize(Llt.Allocator, vacatedCapacity);
            poison.ToFullCapacitySpan().Fill(new Node { NodeId = -1, VectorId = -1 });
            (_retiredNodeStores ??= []).Add(poison);

            return (nint)_nodes.RawItems != before;
        }

        // Number of grown-from node buffers kept alive (un-disposed) until dispose.
        internal int RetiredNodeStorageCountForTesting => _retiredNodeStores?.Count ?? 0;

        public int GetCreatedNodeIndex(int index) => _newNodes[index];

        public Options Options;

        public SearchState(LowLevelTransaction llt, string name) : this(llt, SliceFromString(llt, name))
        {
        }

        private static Slice SliceFromString(LowLevelTransaction llt, string name)
        {
            Slice.From(llt.Allocator, name, out var slice);
            return slice;
        }
        
        public Lookup<Int64LookupKey> NodeIdsByVectorId => _tree.LookupFor<Int64LookupKey>(Hnsw.NodesByVectorIdSlice);

        public SearchState(LowLevelTransaction llt, Slice name) : this(llt, name, null)
        {
        }

        public SearchState(LowLevelTransaction llt, Slice name, ref Memory<byte> queryVector) : this(llt, name, null)
        {
            if (IsEmpty)
                return;
            if (TryNormalizeQueryVector(Options, Llt.Allocator, ref queryVector, out var scope))
                _queryNormalizationScope = scope;
        }

        public SearchState(LowLevelTransaction llt, Slice name, HnswIndexCache cache)
        {
            Llt = llt;
            _cache = cache;
            _tree = llt.Transaction.ReadTree(name);

            if (_tree is null || _tree.TryGetLookupFor(LocationPagesSlice, out Lookup<Int64LookupKey> locationPages) == false)
            {
                IsEmpty = true;
                return;
            }

            Options = ReadOptions(_tree);
            _locations = RecordLocations(locationPages, _cache);
            _carriesCodes = Options.SimilarityMethod == SimilarityMethod.CosineSimilaritySingles;
            if (_carriesCodes)
            {
                _codeRowBytes = CodeParameters.RowBytesOf((Options.VectorSizeBytes - MagnitudeSizeInBytes) / sizeof(float));
                _codeRows = new NodeRows(_tree.LookupFor<Int64LookupKey>(CodePagesSlice), _codeRowBytes, Options.CodeRowsPerBlock, _cache?.CodeBlocks);
            }
            SimilarityCalc = GetDistanceKernel(Options);

            if (_cache != null)
            {
                // Skip ~7 NativeList<Node>.Grow doublings (and Dictionary rehashes) for the typical
                // per-query visit set when a shared cache is attached. ByteStringMemoryCache.Allocate
                // showed 36.5s of 165s wall in NativeList<Node>.Grow under load; pre-sizing kills it.
                _nodes.Initialize(llt.Allocator, InitialNodesCapacityWithCache);
                _nodeIdToIdx.EnsureCapacity(InitialNodesCapacityWithCache);
            }
        }

        private const int InitialNodesCapacityWithCache = 256;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float DistanceToScore(float score)
        {
            switch (Options.SimilarityMethod)
            {
                case SimilarityMethod.CosineSimilaritySingles:
                case SimilarityMethod.CosineSimilarityI8:
                    return 1 - score;
                case SimilarityMethod.HammingDistance:
                    return ((Options.VectorSizeBytes * 8) - score) / (8f * Options.VectorSizeBytes); // number_of_bits * minimum_similarity
                default:
                    throw new InvalidDataException($"Unknown similarity method {Options.SimilarityMethod}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DistancesToScores(Span<float> distances)
        {
            switch (Options.SimilarityMethod)
            {
                case SimilarityMethod.CosineSimilaritySingles:
                case SimilarityMethod.CosineSimilarityI8:
                    DistanceToScoreCosine(distances);
                    break;
                case SimilarityMethod.HammingDistance:
                    DistanceToScoreHamming(distances, Options.VectorSizeBytes);
                    break;
                default:
                    throw new InvalidDataException($"Unknown similarity method {Options.SimilarityMethod}");
            }
        }

        public void FlushOptions()
        {
            using (_tree.DirectAdd(OptionsSlice, sizeof(Options), out var dst))
            {
                Unsafe.Write(dst, Options);
            }
        }

        public int RegisterVectorNode(long newNodeId, long vectorId)
        {
            int nodeIndex = AllocateNodeIndex(newNodeId);

            _newNodes.Add(Llt.Allocator, nodeIndex);
            _nodes[nodeIndex].VectorId = vectorId;

            _nodeIdToIdx[newNodeId] = nodeIndex;
            return nodeIndex;
        }

        private int AllocateNodeIndex(long nodeId)
        {
            if (_nodes.HasCapacityFor(1) == false)
                GrowNodesRetainingOldBuffer();
            int nodeIndex = _nodes.Count;
            _nodes.AddUnsafe(new Node { NodeId = nodeId });
            return nodeIndex;
        }

        public bool TryGetLocationForNode(long nodeId, out long locationId) =>
            (locationId = _locations.ReadInt64(Llt, nodeId)) != 0;

        public unsafe void RegisterNodeLocation(long nodeId, long locationId) =>
            *(long*)_locations.Write(Llt, nodeId) = locationId;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref Node GetNodeByIndex(int index)
        {
            ref var n = ref _nodes[index];
            Debug.Assert(n.NodeId is not 0, "n.NodeId is not 0");
            return ref n;
        }

        public void ReadNode(long nodeId, out NodeReader n)
        {
            if (TryGetLocationForNode(nodeId, out var nodeLocation) is false)
                throw new InvalidOperationException($"Unable to find node id {nodeId}");
            n = Node.Decode(Llt, nodeLocation);
        }

        /// <summary>
        /// Resolves edge indexes for a node at the specified level, caching the result in
        /// <see cref="Node.EdgesIndexesPerLevel"/>. On cache hit the expensive
        /// <see cref="LoadNodeIndexes"/> dictionary lookups are skipped entirely.
        /// With a <paramref name="screen"/>, the neighbors that it rejects against <paramref name="threshold"/> are left out and stay unresolved.
        /// May grow the nodes array — callers must re-fetch any outstanding node refs afterward.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ResolveEdgeIndexes(int nodeIndex, int level, CodeScreen screen, double threshold, ref NativeList<long> nodeIds, ref NativeList<int> indexes)
        {
            ref var node = ref GetNodeByIndex(nodeIndex);

            // Fast path: the resolved indexes for this level are already cached on the node.
            // The slow path (miss) is in a NoInlining helper so only this copy is inlined into
            // callers inside the search loop.
            int edgeCount = node.Edges(level).Length;
            if (node.EdgesIndexesPerLevel.Count > level &&
                node.EdgesIndexesPerLevel[level].Count == edgeCount)
            {
                indexes.ResetAndCopyFrom(Llt.Allocator, node.EdgesIndexesPerLevel[level].ToSpan());
                return;
            }

            ResolveEdgeIndexesSlow(nodeIndex, level, screen, threshold, ref nodeIds, ref indexes);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ResolveEdgeIndexesSlow(int nodeIndex, int level, CodeScreen screen, double threshold, ref NativeList<long> nodeIds, ref NativeList<int> indexes)
        {
            ref var node = ref GetNodeByIndex(nodeIndex);
            nodeIds.ResetAndCopyFrom(Llt.Allocator, node.Edges(level));
            var edgeCount = nodeIds.Count;
            LoadNodeIndexes(ref nodeIds, ref indexes, screen, threshold);

            // A list without the rejected neighbors is never the node's list.
            if (indexes.Count != edgeCount)
                return;

            node = ref GetNodeByIndex(nodeIndex);
            node.EdgesIndexesPerLevel.SetCapacity(Llt.Allocator, level + 1);
            node.EdgesIndexesPerLevel[level].ResetAndCopyFrom(Llt.Allocator, indexes.ToSpan());
        }

        /// <summary>
        /// This accepts a list of node ids (mutable, we do destructive updates to it) and translate
        /// that to a list of the indexes in the nodes array. If needed, it will load the nodes
        /// from the disk in a batch oriented manner.
        /// The indexes are listed in the order of the given ids, whatever resolved each node before, so the order of an evaluation never depends
        /// on the nodes that a screened traversal left unresolved.
        /// With a <paramref name="screen"/>, the ids that it rejects against <paramref name="threshold"/>, now or earlier in the traversal, are dropped.
        /// An id that this SearchState holds is resolved and is never checked.
        /// </summary>
        private unsafe void LoadNodeIndexes(ref NativeList<long> nodeIds, ref NativeList<int> indexes, CodeScreen screen, double threshold)
        {
            var screening = screen is not null && screen.Screens(nodeIds.Count);
            if (screening && _forTestingPurposes is not null)
                _forTestingPurposes.Commitment = Math.Max(_forTestingPurposes.Commitment, screen.Admissions + nodeIds.Count);

            // One probe per id. The code rows of the ids to check start loading before the first one is read, so their cache misses overlap.
            Buffers.Get(nodeIds.Count, out var rows, out _, out _, out _, out _);
            indexes.ResetAndEnsureCapacity(Llt.Allocator, nodeIds.Count);
            for (int i = 0; i < nodeIds.Count; i++)
            {
                var resolved = _nodeIdToIdx.TryGetValue(nodeIds[i], out var index);
                indexes.AddUnsafe(resolved ? index : -1);
                rows[i] = resolved == false && screening && screen.IsRejected(nodeIds[i]) == false ? (long)PrefetchCode(nodeIds[i]) : 0;
            }

            var unresolved = 0;
            var kept = 0;
            for (int i = 0; i < nodeIds.Count; i++)
            {
                var id = nodeIds[i];
                var index = indexes[i];
                if (index < 0 && screen is not null && ScreenRejects(screen, screening, id, (byte*)rows[i], threshold))
                    continue;

                // The slot of an unresolved id holds -1 until its record is read below.
                indexes[kept] = index;
                nodeIds[kept++] = id;
                if (index < 0)
                    unresolved++;
            }

            nodeIds.Shrink(kept);
            indexes.Shrink(kept);
            if (unresolved == 0)
                return;

            Buffers.Get(unresolved, out var locations, out _, out var spans, out var targets, out _);
            var loading = 0;
            for (int i = 0; i < kept; i++)
            {
                if (indexes[i] >= 0)
                    continue;

                var nodeIdx = AllocateNodeIndex(nodeIds[i]);
                _nodeIdToIdx[nodeIds[i]] = nodeIdx;
                indexes[i] = nodeIdx;
                locations[loading] = _locations.ReadInt64(Llt, nodeIds[i]);
                targets[loading++] = nodeIdx;
            }

            Container.GetAll(Llt, locations[..loading], spans, 0, Llt.PageLocator);
            for (int i = 0; i < loading; i++)
                Node.Decode(Llt, spans[i].ToSpan()).LoadInto(ref _nodes[targets[i]]);
        }

        public int GetNodeIndexById(long nodeId)
        {
            ref var nodeIdx = ref CollectionsMarshal.GetValueRefOrAddDefault(_nodeIdToIdx, nodeId, out var exists);
            if (exists)
                return nodeIdx;

            if (TryGetLocationForNode(nodeId, out var nodeLocation) is false)
                throw new InvalidOperationException($"Unable to find node id {nodeId}");

            nodeIdx = AllocateNodeIndex(nodeId);
            var reader = Node.Decode(Llt, nodeLocation);
            ref var n = ref GetNodeByIndex(nodeIdx);
            reader.LoadInto(ref n);
            return nodeIdx;
        }

        public ref Node GetNodeById(long nodeId)
        {
            int idx = GetNodeIndexById(nodeId);
            return ref GetNodeByIndex(idx);
        }

        public float Distance(UnmanagedSpan src, UnmanagedSpan dst)
        {
            return SimilarityCalc(src.ToSpan(), dst.ToSpan());
        }

        public float Distance(ReadOnlySpan<byte> vector, int fromIdx, int toIdx)
        {
            if (vector.IsEmpty)
            {
                ref var from = ref GetNodeByIndex(fromIdx);
                vector = from.GetVector(this); // Note: we have to make a copy here since we cannot pass this as ref into a ref value
            }

            ref var to = ref GetNodeByIndex(toIdx);
            if (to.QueryDistanceVersion == _queryVectorVersion)
                return to.QueryDistanceValue;

            Span<byte> v2 = to.GetVector(this);
            var distance = SimilarityCalc(vector, v2);

            return distance;
        }

        // Allows storing cached distance to the queried vector. Should be used only in the querying part!
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float QueryDistance(ReadOnlySpan<byte> vector, int toIdx, ref long vectorReadCounter)
        {
            // Fast path: the distance for this (query vector, node) pair was already computed during
            // the current traversal and is still valid (version equals _visitsCounter). The miss path
            // — loading the stored vector and running SimilarityCalc — lives in a NoInlining helper so
            // its register footprint does not leak into the search loop.
            ref var to = ref GetNodeByIndex(toIdx);
            if (to.QueryDistanceVersion == _queryVectorVersion)
                return to.QueryDistanceValue;

            return QueryDistanceMiss(vector, ref to, ref vectorReadCounter);
        }

        // Same fast/slow split as the index-taking overload. Callers in the search inner loop
        // already hold a ref to the Node so this avoids the second GetNodeByIndex lookup.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float QueryDistance(ReadOnlySpan<byte> vector, ref Node to, ref long vectorReadCounter)
        {
            if (to.QueryDistanceVersion == _queryVectorVersion)
                return to.QueryDistanceValue;

            return QueryDistanceMiss(vector, ref to, ref vectorReadCounter);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private float QueryDistanceMiss(ReadOnlySpan<byte> vector, ref Node to, ref long vectorReadCounter)
        {
            Span<byte> v2 = to.GetVector(this);
            vectorReadCounter++;
            var distance = SimilarityCalc(vector, v2);
            to.QueryDistanceValue = distance;
            to.QueryDistanceVersion = _queryVectorVersion;
            return distance;
        }

        /// <summary>
        /// Records the query vector a searcher is about to use. Bumps <see cref="_queryVectorVersion"/>
        /// iff the vector differs from the previously recorded one (compared by Memory identity, i.e.
        /// same underlying buffer and range). A bump invalidates every cached QueryDistance; a no-op
        /// keeps them valid.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void OnQueryVector(Memory<byte> queryVector)
        {
            if (queryVector.Equals(_lastQueryVector))
                return;
            _lastQueryVector = queryVector;
            ++_queryVectorVersion;
        }

        public void ReadPostingList(ContainerEntryId rawPostingListId, ref ContextBoundNativeList<long> listBuffer, ref FastPForDecoder pforDecoder, out int postingListSize)
        {
            ReadPostingList(Llt, rawPostingListId, ref listBuffer, ref pforDecoder, out postingListSize);
        }

        public static void ReadPostingList(LowLevelTransaction llt, ContainerEntryId rawPostingListId, ref ContextBoundNativeList<long> listBuffer, ref FastPForDecoder pforDecoder, out int postingListSize)
        {
            Container.Get(llt, rawPostingListId, out var smallPostingList);
            var count = VariableSizeEncoding.Read<int>(smallPostingList.Address, out var offset);

            var requiredSize = Math.Max(256, 256 * (int)Math.Ceiling((count + listBuffer.Count) / 256f));
            listBuffer.EnsureCapacityFor(requiredSize);
            Debug.Assert(listBuffer.Capacity > 0 && listBuffer.Capacity % 256 == 0, "The buffer must be multiple of 256 for PForDecoder.Read");

            pforDecoder.Init(smallPostingList.Address + offset, smallPostingList.Length - offset);
            listBuffer.Count += pforDecoder.Read(listBuffer.RawItems + listBuffer.Count, listBuffer.Capacity - listBuffer.Count);
            postingListSize = smallPostingList.Length;
        }

        [Flags]
        public enum NearestEdgesFlags
        {
            None = 0,
            StartingPointAsEdge = 1 << 1,
            FilterNodesWithEmptyPostingLists = 1 << 2
        }
        
        public IHnswSearcher NearestSearch(int startingPointIndex, Memory<byte> vector,
            int level, int numberOfCandidates,
            ContextBoundNativeList<int> candidates,
            NearestEdgesFlags flags,
            bool hasFilterMatch)
        {
            return new NearestSearcher(this, startingPointIndex, vector, level , numberOfCandidates, candidates, flags, hasFilterMatch,
                BeginScreen(vector.Span, numberOfCandidates, hasFilterMatch));
        }

        /// <summary>The descent from the entry point and the base-layer traversal from where it ends, with one screen for both.</summary>
        internal IHnswSearcher ApproximateSearch(Memory<byte> vector, int numberOfCandidates, bool hasFilterMatch)
        {
            var nearestNodesByLevel = new ContextBoundNativeList<int>(Llt.Allocator);
            nearestNodesByLevel.EnsureCapacityFor(Options.MaxLevel + 1);
            var screen = BeginScreen(vector.Span, numberOfCandidates, hasFilterMatch);
            SearchNearestAcrossLevels(vector.Span, -1, Options.MaxLevel, ref nearestNodesByLevel, screen);
            var nearest = nearestNodesByLevel[0];
            nearestNodesByLevel.Clear();
            return new NearestSearcher(this, nearest, vector, 0, numberOfCandidates, nearestNodesByLevel,
                NearestEdgesFlags.StartingPointAsEdge | NearestEdgesFlags.FilterNodesWithEmptyPostingLists, hasFilterMatch, screen);
        }

        public IHnswSearcher EmptySearch() => new EmptySearcher();
        
        public IHnswSearcher ExactSearch(Memory<byte> vector, bool hasFilterMatch, int numberOfCandidates, ContextBoundNativeList<long>? nodesToScan) => new ExactSearcher(this, vector, hasFilterMatch, numberOfCandidates, nodesToScan);


        public void SearchNearestAcrossLevels(ReadOnlySpan<byte> vector, int dstIdx, int maxLevel, ref ContextBoundNativeList<int> nearestIndexes) =>
            SearchNearestAcrossLevels(vector, dstIdx, maxLevel, ref nearestIndexes, null);

        internal void SearchNearestAcrossLevels(ReadOnlySpan<byte> vector, int dstIdx, int maxLevel, ref ContextBoundNativeList<int> nearestIndexes, CodeScreen screen)
        {
            if (screen is not null)
                screen.Descending = true;
            var visitCounter = ++_visitsCounter;
            var currentNodeIndex = GetNodeIndexById(EntryPointId);
            var level = maxLevel;
            ref var entry = ref GetNodeByIndex(currentNodeIndex);
            entry.EnsureLevels(Llt.Allocator, maxLevel + 1);
            var distance = Distance(vector, dstIdx, currentNodeIndex);
            var indexes = new NativeList<int>();
            var nodeIds = new NativeList<long>();

            while (level >= 0)
            {
                bool moved;
                do
                {
                    moved = false;
                    Debug.Assert(GetNodeByIndex(currentNodeIndex).CountOfLevels > level, "n.CountOfLevels > level");
                    ResolveEdgeIndexes(currentNodeIndex, level, screen, distance, ref nodeIds, ref indexes);
                    for (var i = 0; i < indexes.Count; i++)
                    {
                        var edgeIdx = indexes[i];
                        ref var edge = ref GetNodeByIndex(edgeIdx);
                        if (edge.Visited == visitCounter)
                            continue; // already checked it
                        edge.Visited = visitCounter;
                        var curDist = Distance(vector, dstIdx, edgeIdx);
                        if (curDist >= distance || double.IsNaN(curDist))
                            continue;

                        moved = true;
                        distance = curDist;
                        currentNodeIndex = edgeIdx;
                        if (screen is not null)
                            screen.Admissions++;
                    }
                } while (moved);

                nearestIndexes.AddUnsafe(currentNodeIndex);
                level--;
            }

            indexes.Dispose(Llt.Allocator);
            nodeIds.Dispose(Llt.Allocator);
            nearestIndexes.Inner.Reverse();
            if (screen is not null)
                screen.Descending = false;
        }


        private static class Buffers
        {
            [ThreadStatic]
            private static long[] IdsToLoad;
            [ThreadStatic]
            private static int[] IndexesOfIds;
            [ThreadStatic]
            private static long[] VectorIdsToLoad;
            [ThreadStatic]
            private static UnmanagedSpan[] SpansToLoad;
            [ThreadStatic]
            private static int[] NodeIndexes;

            public static void Get(int count, out Span<long> idsToLoad, out Span<long> vectorIdsToLoad, out Span<UnmanagedSpan> spansToLoad, out Span<int> nodeIndexes, out Span<int> indexesOfIds)
            {
                if (IdsToLoad is null || count > IdsToLoad.Length)
                {
                    Allocate(count);
                }

                idsToLoad = IdsToLoad;
                vectorIdsToLoad = VectorIdsToLoad;
                spansToLoad = SpansToLoad;
                nodeIndexes = NodeIndexes;
                indexesOfIds = IndexesOfIds;
            }

            private static void Allocate(int count)
            {
                count = Bits.NextAllocationSize(count);

                IndexesOfIds = new int[count];
                IdsToLoad = new long[count];
                VectorIdsToLoad = new long[count];
                SpansToLoad = new UnmanagedSpan[count];
                NodeIndexes = new int[count];
            }
        }

        /// <summary>
       /// Load all the _vectors_ associated with the provided node ids.
       /// Note that this actually requires us to first look up the nodes by id (loading them via batch)
       /// then load any yet unloaded vector (again using a batch) 
       /// </summary>
       public void PreloadNodesVectors(Span<long> nodeIds)
       {
           Buffers.Get(nodeIds.Length, 
               out var idsToLoad, 
               out var vectorIdsToLoad, 
               out var spans, 
               out var nodeIndexes,
               out var indexesOfIds);
           int idsToLoadIdx = 0;
           var vectorsToLoadIdx = 0;
           foreach (var nodeId in nodeIds)
           {
               if (_nodeIdToIdx.TryGetValue(nodeId, out var existingIdx))
               {
                   ref var n = ref _nodes[existingIdx];
                   if (n.VectorLoaded)
                       continue;
                   nodeIndexes[vectorsToLoadIdx] = existingIdx;
                   vectorIdsToLoad[vectorsToLoadIdx++] = n.GetVectorContainerId();
                   continue;
               }

               var nodeIdx = AllocateNodeIndex(nodeId);
               _nodes[nodeIdx].NodeId = nodeId;
               _nodeIdToIdx[nodeId] = nodeIdx;
               indexesOfIds[idsToLoadIdx] = nodeIdx;
               idsToLoad[idsToLoadIdx++] = nodeId;
           }

           if (idsToLoadIdx is not 0)
           {
               idsToLoad = idsToLoad[..idsToLoadIdx];
               indexesOfIds = indexesOfIds[..idsToLoadIdx];
           
               for (int i = 0; i < idsToLoad.Length; i++)
                   idsToLoad[i] = _locations.ReadInt64(Llt, idsToLoad[i]);
               idsToLoad.Sort(indexesOfIds);
               Container.GetAll(Llt, idsToLoad, spans, 0, Llt.PageLocator);
               for (int i = 0; i < indexesOfIds.Length; i++)
               {
                   var buf = spans[i].ToSpan();
                   var reader = Node.Decode(Llt, buf);
                   var nodeIndex = indexesOfIds[i];
                   ref var n = ref _nodes[nodeIndex];
                   reader.LoadInto(ref n);
                   nodeIndexes[vectorsToLoadIdx] = nodeIndex;
                   vectorIdsToLoad[vectorsToLoadIdx++] = n.GetVectorContainerId();
               }
           }
           if (vectorsToLoadIdx is 0)
               return;

           vectorIdsToLoad = vectorIdsToLoad[..vectorsToLoadIdx];
           nodeIndexes = nodeIndexes[..vectorsToLoadIdx];
           vectorIdsToLoad.Sort(nodeIndexes);
           Container.GetAll(Llt, vectorIdsToLoad, spans, -1, Llt.PageLocator);
           for (int i = 0; i < vectorIdsToLoad.Length; i++)
           {
               // note, small vectors (where multiple can fit in a single page), will be 
               // partition inside the SetVector if needed
               _nodes[nodeIndexes[i]].SetVector(Options, spans[i]);
           }
       }

       public bool TryGetNodeById(long nodeId, out int nodeIndex)
       {
           return _nodeIdToIdx.TryGetValue(nodeId, out nodeIndex);
       }

       /// <summary>
       /// Debug-only check that the candidate and nearest-edges priority queues owned by this
       /// SearchState are empty. A searcher that reuses a shared SearchState must leave these
       /// queues empty on Dispose; this method catches a searcher that fails to do so before the
       /// next one starts.
       /// </summary>
       [Conditional("DEBUG")]
       public void AssertSharedQueuesClean()
       {
           Debug.Assert(_candidatesQ.Count == 0, "_candidatesQ must be empty between sub-queries on a shared SearchState");
           Debug.Assert(_nearestEdgesQ.Count == 0, "_nearestEdgesQ must be empty between sub-queries on a shared SearchState");
       }

       public void Dispose()
       {
           _candidatesQ.Clear();
           _nearestEdgesQ.Clear();
           _newNodes.Dispose(Llt.Allocator);

           _nodes.Dispose(Llt.Allocator);

           if (_retiredNodeStores is not null)
           {
               foreach (var store in _retiredNodeStores)
               {
                   var retired = store;
                   retired.Dispose(Llt.Allocator);
               }
           }

           if (_queryNormalizationScope is { } scope)
           {
               scope.Dispose();
               _queryNormalizationScope = null;
           }
       }

    }

    public partial class Registration : IDisposable
    {
        public bool IsCommited { get; private set; }
        private readonly Dictionary<ByteString, (ByteString Key, int NodeIndex, NativeList<long> PostingList)> _vectorHashCache = new(ByteStringContentComparer.Instance);

        private readonly Lookup<Int64LookupKey> _nodesByVectorId;
        private SearchState _searchState;
        public Random Random;
        private readonly CompactTree _vectorsByHash;
        private readonly int _vectorBatchSizeInPages;
        private readonly ContainerId _globalVectorsContainerId;
        private PostingList _largePostingListSet;

        // Lazy scratch used by Register to normalize raw-sized inputs in-place; one allocation per Registration.
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _normalizationScope;
        private ByteString _normalizationBuffer;

        public int AmountOfModifiedVectorsInTransaction => _vectorHashCache.Count;

        public Options Options => _searchState.Options;

        internal TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly() => _forTestingPurposes ??= new TestingStuff();

        // Deterministically reproduces the RavenDB-26809 interleaving for tests: the LLT thread picks a
        // victim (node, level) in PrepareEdgesOnLLT, the worker dispatched for it parks, and the LLT thread
        // then moves both that edge buffer and the node array before releasing it. With the fix in place
        // the worker reads a private edge snapshot and the grown-from node buffer is retained, so the build
        // completes and the graph stays queryable; reverting either guard turns this into a use-after-free.
        internal sealed class TestingStuff
        {
            internal bool SimulateConcurrentRealloc;

            // LLT thread: level and top level of the inserted node, and the number of nodes its insertion beam starts from at that level.
            internal Action<int, int, int> OnBeamStarted;

            // LLT thread, around the rewrite of an overfull list from its dispatch snapshot: state, node index and level.
            internal Action<SearchState, int, int> BeforeEdgesReplaced;
            internal Action<SearchState, int, int> AfterEdgesReplaced;

            // LLT thread, once every placement is done.
            internal Action<SearchState> BeforeOlderInLinksRepaired;

            // Draws the code parameters of the graph in place of Random.Shared.
            internal Random CodeRandom;

            // Latched once the matching buffer was actually moved while the worker was parked, so a test
            // can fail loudly if the race window was never exercised.
            internal volatile bool VictimSelected;
            internal volatile bool InnerEdgeBufferMovedWhileWorkerParked;
            internal volatile bool NodesArrayMovedWhileWorkerParked;

            // False if a node reference captured before the move read poisoned/freed data after it — i.e.
            // the node array was not retained across the grow (the RavenDB-26809 bug).
            internal volatile bool StaleNodeReferenceStillValid = true;

            // Set by the runner so the parked worker can wake the dispatch loop; without it the loop may
            // not iterate again when the only outstanding work is the parked worker itself.
            internal Action WakeLltLoop;

            private int _victim = -1;
            private int _victimLevel;
            private nint _victimNodePtr;
            private long _victimNodeIdExpected;
            private readonly ManualResetEventSlim _workerParked = new(false);
            private readonly ManualResetEventSlim _lltMoved = new(false);

            // LLT thread: pick the victim here, where the node and its edge lists are stable because we
            // own the allocator. Capturing it on a worker instead races the LLT thread's own SetCapacity /
            // ResetAndEnsureCapacity on that node and dereferences a freed edge buffer (RavenDB-27393).
            // Refs are re-derived here rather than passed in: PrepareEdgesOnLLT's own ref into the node
            // array can be pointing at a retired buffer by the time it calls us, since its mirror-rebuild
            // loop can grow that array through GetNodeIndexById.
            internal void OnLltPreparedEdges(SearchState searchState, int nodeIndex, int level)
            {
                if (SimulateConcurrentRealloc == false || _victim >= 0)
                    return;

                ref var node = ref searchState.GetNodeByIndex(nodeIndex);
                if (node.EdgesIndexesPerLevel[level].Count == 0)
                    return;

                _victimLevel = level;
                _victimNodePtr = (nint)Unsafe.AsPointer(ref node);
                _victimNodeIdExpected = node.NodeId;
                // Published last: a worker that observes _victim also observes everything above it.
                Volatile.Write(ref _victim, nodeIndex);
            }

            // Worker thread: the worker dispatched for the victim (node, level) parks here until the LLT
            // thread has moved that storage and the node array. Reads no shared native state.
            internal void OnWorkerAboutToConsumeEdges(int nodeIndex, int level)
            {
                if (SimulateConcurrentRealloc == false || _lltMoved.IsSet)
                    return;
                if (Volatile.Read(ref _victim) != nodeIndex || _victimLevel != level)
                    return;

                VictimSelected = true;
                _workerParked.Set();
                WakeLltLoop?.Invoke();
                _lltMoved.Wait(TimeSpan.FromMinutes(2));

                // Read through the reference captured before the move, exactly as a racing worker would.
                // Retaining the grown-from buffer keeps this valid; freeing it (the bug) reads poison.
                if (((Node*)_victimNodePtr)->NodeId != _victimNodeIdExpected)
                    StaleNodeReferenceStillValid = false;
            }

            // LLT thread, at the top of each dispatch round: once the victim worker has parked, move its
            // edge buffer (restoring contents) and the node array, then release the worker.
            internal void OnLltRunRound(SearchState searchState)
            {
                if (SimulateConcurrentRealloc == false || _lltMoved.IsSet || _workerParked.IsSet == false)
                    return;

                ref var inner = ref searchState.GetNodeByIndex(_victim).EdgesIndexesPerLevel[_victimLevel];
                var saved = inner.ToSpan().ToArray();
                var before = (nint)inner.RawItems;
                inner.ResetAndEnsureCapacity(searchState.Llt.Allocator, Math.Max(inner.Capacity * 2, saved.Length + 1));
                foreach (var v in saved)
                    inner.AddUnsafe(v);
                if ((nint)inner.RawItems != before)
                    InnerEdgeBufferMovedWhileWorkerParked = true;

                if (searchState.GrowNodesAndPoisonVacatedForTesting())
                    NodesArrayMovedWhileWorkerParked = true;

                _lltMoved.Set();
            }
        }

        public Registration(LowLevelTransaction llt, Slice name, Random random = null)
        {
            Random = random ?? Random.Shared;
            _searchState = new SearchState(llt, name);
            _vectorBatchSizeInPages = _searchState.Options.VectorBatchInPages;
            _globalVectorsContainerId = ReadGlobalVectorsContainerId(llt);
            _nodesByVectorId = _searchState.Tree.LookupFor<Int64LookupKey>(NodesByVectorIdSlice);
            _vectorsByHash = llt.Transaction.CompactTreeFor(VectorsIdByHashSlice);
        }

        /// <summary>
        /// Removes a vector from the graph.
        /// </summary>
        /// <param name="entryId">The ID of the document.</param>
        /// <param name="vectorHash">The hash of the vector to remove.</param>
        public void Remove(long entryId, ReadOnlySpan<byte> vectorHash)
        {
            entryId = EntryIdToInternalEntryId(entryId);
            const long RemovalMask = 1;

            PortableExceptions.ThrowIfOnDebug<ArgumentOutOfRangeException>((entryId & Constants.Graphs.VectorId.EnsureIsSingleMask) != 0, "Entry ids must have the first two bits cleared, we are using those");

            _searchState.Llt.Allocator.AllocateDirect(Sodium.GenericHashSize, out var hashBuffer);
            vectorHash.CopyTo(hashBuffer.ToSpan());

            ref var postingList = ref CollectionsMarshal.GetValueRefOrAddDefault(_vectorHashCache, hashBuffer, out var exists);
            if (exists)
            {
                ref var l = ref postingList.PostingList;
                l.Add(_searchState.Llt.Allocator, entryId | RemovalMask);
                _searchState.Llt.Allocator.Release(ref hashBuffer);
                return;
            }

            if (_vectorsByHash.TryGetValue(vectorHash, out var vectorId) is false)
                PortableExceptions.Throw<InvalidOperationException>($"Unable to find the vector corresponding to the provided vector hash: base64({Convert.ToBase64String(vectorHash)}).");

            if (_nodesByVectorId.TryGetValue(vectorId, out var nodeId) is false)
                PortableExceptions.Throw<InvalidOperationException>($"Unable to find the node corresponding to the provided vector hash: base64({Convert.ToBase64String(vectorHash)}) and VectorId({vectorId}).");

            int nodeIndex = _searchState.GetNodeIndexById(nodeId);
            postingList = (hashBuffer, nodeIndex, NativeList<long>.Create(_searchState.Llt.Allocator, entryId | RemovalMask));
        }

        /// <summary>
        /// The two lowest bits must be cleared for mask purposes.
        /// </summary>
        /// <param name="entryId">Original entryId.</param>
        /// <returns>Internal Hnsw entryId</returns>
        internal static long EntryIdToInternalEntryId(long entryId)
        {
            Debug.Assert(entryId > 0 && (~(long.MaxValue >> 2) & entryId) == 0, "entryId > 0 && (~(long.MaxValue >> 2) & entryId) == 0");
            return entryId << 2;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long InternalEntryIdToEntryId(long entryId) => entryId >> 2;

        /// <summary>
        /// During indexing, we're shifting each ID 2 bits to the left to use the two lowest bits as a mask placeholder. This is for querying decoding.
        /// </summary>
        /// <param name="entries">Array of internal ids.</param>
        internal static void InternalEntryIdToEntryId(Span<long> entries)
        {
            var entriesPos = 0;
            ref var entriesRef = ref MemoryMarshal.GetReference(entries);

            if (AdvInstructionSet.IsAcceleratedVector512)
            {
                var N = Vector512<long>.Count;

                for (; entriesPos + N < entries.Length; entriesPos += N)
                {
                    ref var currentMemory = ref Unsafe.Add(ref entriesRef, entriesPos);
                    var current = Vector512.LoadUnsafe(ref currentMemory);
                    Vector512.ShiftRightLogical(current, 2).StoreUnsafe(ref currentMemory);
                }
            }

            if (AdvInstructionSet.IsAcceleratedVector256)
            {
                var N = Vector256<long>.Count;

                for (; entriesPos + N < entries.Length; entriesPos += N)
                {
                    ref var currentMemory = ref Unsafe.Add(ref entriesRef, entriesPos);
                    var current = Vector256.LoadUnsafe(ref currentMemory);
                    Vector256.ShiftRightLogical(current, 2).StoreUnsafe(ref currentMemory);
                }
            }

            for (; entriesPos < entries.Length; entriesPos++)
                Unsafe.Add(ref entriesRef, entriesPos) >>= 2;
        }

        /// <summary>
        /// Adds a vector to the graph.
        /// </summary>
        /// <param name="entryId">The ID of the document (source).</param>
        /// <param name="vector">The vector's data.</param>
        /// <returns>The CompactKey address of the hash calculated from the vector, which will be required for removal.</returns>
        public ByteString Register(long entryId, ReadOnlySpan<byte> vector)
        {
            entryId = EntryIdToInternalEntryId(entryId);
            PortableExceptions.ThrowIfOnDebug<ArgumentOutOfRangeException>((entryId & Constants.Graphs.VectorId.EnsureIsSingleMask) != 0, "Entry ids must have the first two bits cleared, we are using those");

            if (vector.Length != _searchState.Options.VectorSizeBytes)
                vector = NormalizeIntoScratch(vector);

            var hashBuffer = ComputeHashFor(vector);
            ref (ByteString Hash, int NodeIndex, NativeList<long> PostingList) postingList = ref CollectionsMarshal.GetValueRefOrAddDefault(_vectorHashCache, hashBuffer, out var exists);
            if (exists)
            {
                // already added this in the current batch
                ref var l = ref postingList.PostingList;
                l.Add(_searchState.Llt.Allocator, entryId);
                _searchState.Llt.Allocator.Release(ref hashBuffer);
                return postingList.Hash;
            }

            var vectorHash = hashBuffer.ToReadOnlySpan();
            long vectorId;
            if (_vectorsByHash.TryGetValue(vectorHash, out _, out vectorId) is false)
            {
                var vectorEntryId = RegisterVector(vector);
                vectorId = (long)vectorEntryId;
                _vectorsByHash.Add(vectorHash, vectorId);
            }

            if (_nodesByVectorId.TryGetValue(vectorId, out var nodeId))
            {
                int nodeIndex = _searchState.GetNodeIndexById(nodeId);
                postingList = (hashBuffer, nodeIndex, NativeList<long>.Create(_searchState.Llt.Allocator, entryId));
                return hashBuffer;
            }

            long newNodeId = ++_searchState.Options.CountOfVectors;
            int nodeIdx = _searchState.RegisterVectorNode(newNodeId, vectorId);
            _nodesByVectorId.Add(vectorId, newNodeId);

            postingList = (hashBuffer, nodeIdx, ToPostingListTuple(entryId));
            return hashBuffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        NativeList<long> ToPostingListTuple(long entryId)
        {
            var list = new NativeList<long>();
            list.Add(_searchState.Llt.Allocator, entryId);
            return list;
        }
        
        private ContainerEntryId RegisterVector(ReadOnlySpan<byte> vector)
        {
            if (_searchState.Options.LastUsedContainerId.IsEmpty)
            {
                if (_vectorBatchSizeInPages is 1)
                {
                    // here we allocate a small value, directly from the container
                    var vectorId = Container.Allocate(_searchState.Llt, _globalVectorsContainerId,
                        vector.Length, out var singleVectorStorage);

                    vector.CopyTo(singleVectorStorage);
                    return vectorId;
                }

                var sizeInBytes = _vectorBatchSizeInPages * Constants.Storage.PageSize - PageHeader.SizeOf;
                var batchId = Container.Allocate(_searchState.Llt, _globalVectorsContainerId,
                    sizeInBytes, out var vectorStorage);

                Debug.Assert(vectorStorage.Length / _searchState.Options.VectorSizeBytes <= byte.MaxValue, "vectorStorage.Length / _searchState.Options.VectorSizeBytes <= byte.MaxValue");
                Debug.Assert(((long)batchId & 0xFFF) == 0, "We allocate > 1 page, so we get the full page container id");
                _searchState.Options.LastUsedContainerId = new ContainerId((long)batchId);
                _searchState.Options.VectorBatchIndex = 1;
                vector.CopyTo(vectorStorage);
                //container id | index    | marker
                return GetVectorId(new ContainerId((long)batchId), 0);
            }
            var span = Container.GetMutable(_searchState.Llt, new ContainerEntryId((long)_searchState.Options.LastUsedContainerId));
            var count = _searchState.Options.VectorBatchIndex++;
            Debug.Assert(((count) * vector.Length) < span.Length, "((count) * vector.Length) < span.Length");
            var offset = count * vector.Length;
            vector.CopyTo(span[offset..]);
            offset += vector.Length;
            var id = GetVectorId(_searchState.Options.LastUsedContainerId, count);
            if (offset + vector.Length > span.Length)
            {
                // no more room for the _next_ vector
                _searchState.Options.LastUsedContainerId = new ContainerId(0);
                _searchState.Options.VectorBatchIndex = 0;
            }

            return id;

            ContainerEntryId GetVectorId(ContainerId containerId, int index)
            {
                Debug.Assert(((long)containerId & Constants.Graphs.VectorId.EnsureIsSingleMask) == 0, $"Container id {containerId}");
                //container id | index    | marker
                return new ContainerEntryId((long)containerId | (uint)(index << 1) | Constants.Graphs.VectorStorage.VectorContainerInternalIndexer);
            }
        }

        private ReadOnlySpan<byte> NormalizeIntoScratch(ReadOnlySpan<byte> vector)
        {
            var expected = _searchState.Options.VectorSizeBytes;
            var floats = MemoryMarshal.Cast<byte, float>(vector);
            PortableExceptions.ThrowIf<ArgumentOutOfRangeException>(
                VectorNormalizationRequired(_searchState.Options) == false || TensorSizeBytes<float>(floats.Length) != expected,
                $"Vector size {vector.Length} does not match expected size: {expected}");

            if (_normalizationBuffer.HasValue == false)
                _normalizationScope = _searchState.Llt.Allocator.Allocate(expected, out _normalizationBuffer);

            var dst = _normalizationBuffer.ToSpan();
            WriteNormalizedTensor(floats, dst);
            return dst;
        }

        private ByteString ComputeHashFor(ReadOnlySpan<byte> vector)
        {
            _searchState.Llt.Allocator.AllocateDirect(Sodium.GenericHashSize, out var hashBuffer);
            Sodium.GenericHash(vector, hashBuffer.ToSpan());
            return hashBuffer;
        }

        public void Commit(CancellationToken token)
        {
            PortableExceptions.ThrowIfOnDebug<InvalidOperationException>(_searchState.Llt.Committed);

            var pforEncoder = new FastPForEncoder(_searchState.Llt.Allocator);
            var pforDecoder = new FastPForDecoder(_searchState.Llt.Allocator);
            var listBuffer = new ContextBoundNativeList<long>(_searchState.Llt.Allocator);
            var byteBuffer = new ContextBoundNativeList<byte>(_searchState.Llt.Allocator);
            byteBuffer.EnsureCapacityFor(128);

            var nodes = _searchState.Nodes;
            foreach (var (_, (_, nodeIndex, modifications)) in _vectorHashCache)
            {
                ref var node = ref nodes[nodeIndex];
                node.PostingListId = MergePostingList(node.PostingListId, modifications);
            }

            // Intentionally zeroing the nodes var, we may realloc the underlying array in the insert vector phase
            nodes = Span<Node>.Empty;
            _ = nodes;

            InsertVectorsToGraph(ref byteBuffer, token);

            nodes = _searchState.Nodes;
            for (int i = 0; i < nodes.Length; i++)
                PersistNode(ref nodes[i], ref byteBuffer);

            WriteCodes();

            // flush the local modifications
            _searchState.FlushOptions();

            listBuffer.Dispose();
            byteBuffer.Dispose();
            pforEncoder.Dispose();
            pforDecoder.Dispose();

            IsCommited = true;

            long MergePostingList(long postingList, NativeList<long> modifications)
            {
                // We may have duplicates in the list.
                // Scenarios:
                // 1) additions: when the source document contains a list of the same vectors
                // 2) removals: when the deleted document contained a list of the same vectors
                // However, the HNSW does not use frequency. So we want to have information in buffer about:
                // - There was an addition
                // - There was a removal
                // In such case, we can just sort and remove duplicates to have unique list.
                modifications.Shrink(Sorting.SortAndRemoveDuplicates(modifications.ToSpan()));

                listBuffer.Clear();
                listBuffer.AddRange(modifications.ToSpan());

                int currentSize = 0;
                bool hasSmallPostingList = false;
                ContainerEntryId rawPostingListId = new ContainerEntryId(postingList & Constants.Graphs.VectorId.ContainerType);

                switch (postingList & Constants.Graphs.VectorId.EnsureIsSingleMask)
                {
                    case Constants.Graphs.VectorId.Tombstone: // nothing there
                        break;
                    case Constants.Graphs.VectorId.Single: // single value, just add it
                        listBuffer.Add((long)rawPostingListId);
                        break;
                    case Constants.Graphs.VectorId.SmallPostingList:
                        hasSmallPostingList = true;
                        _searchState.ReadPostingList(rawPostingListId, ref listBuffer, ref pforDecoder, out currentSize);
                        break;
                    case Constants.Graphs.VectorId.PostingList:
                        return UpdatePostingList(rawPostingListId, in modifications, pforEncoder, ref pforDecoder, ref listBuffer);
                }

                // Due to deduplication performed before reading the posting list from disk, we can now have the following scenarios:
                // 1) 2x Additions + 1x Removal
                // There was an update of the document. So, we have 1x addition and 1x removal from indexing, plus the loaded entry id from disk -> the ID will remain in the buffer
                // 2) 1x Addition + 1x Removal:
                // There was a delete operation during indexing, plus the loaded entry id from disk: id will be removed from buffer
                // 3) 1x Addition: New document 
                // INFO: All other scenarios are invalid.
                PostingList.SortModificationsAndRemoveDuplicates(ref listBuffer);

                if (listBuffer.Count is 0 or 1)
                {
                    if (hasSmallPostingList)
                    {
                        Container.Delete(_searchState.Llt, _searchState.Options.Container, rawPostingListId);
                    }

                    if (listBuffer.Count is 0)
                        return 0;

                    Debug.Assert((listBuffer[0] & Constants.Graphs.VectorId.PostingList) == 0, "(listBuffer[0] & 0b11) == 0");
                    return listBuffer[0] | Constants.Graphs.VectorId.Single;
                }

                int size = pforEncoder.Encode(listBuffer.RawItems, listBuffer.Count);
                if (size > Container.MaxSizeInsideContainerPage)
                {
                    DeleteOldSmallPostingListIfNeeded();
                    return CreateNewPostingList(pforEncoder);
                }

                byteBuffer.EnsureCapacityFor(size + 5);
                var offset = VariableSizeEncoding.Write(byteBuffer.RawItems, listBuffer.Count);
                (int itemsCount, int sizeUsed) = pforEncoder.Write(byteBuffer.RawItems + offset, byteBuffer.Capacity - offset);
                byteBuffer.Count = sizeUsed + offset;
                Debug.Assert(itemsCount == listBuffer.Count && sizeUsed == size, "itemsCount == listBuffer.Count && sizeUsed == size");
                Span<byte> mutable;
                if (currentSize == byteBuffer.Count)
                {
                    mutable = Container.GetMutable(_searchState.Llt, rawPostingListId);
                }
                else
                {
                    DeleteOldSmallPostingListIfNeeded();
                    rawPostingListId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, byteBuffer.Count, out mutable);
                }

                Span<byte> span = byteBuffer.ToSpan();
                span.CopyTo(mutable);

                Debug.Assert(((long)rawPostingListId & Constants.Graphs.VectorId.PostingList) == 0, "(rawPostingListId & 0b11) == 0");
                return (long)rawPostingListId | Constants.Graphs.VectorId.SmallPostingList;


                void DeleteOldSmallPostingListIfNeeded()
                {
                    if (hasSmallPostingList)
                    {
                        Container.Delete(_searchState.Llt, _searchState.Options.Container, rawPostingListId);
                    }
                }
            }
        }

        public void Dispose()
        {
            if (_normalizationBuffer.HasValue)
                _normalizationScope.Dispose();
        }

        private long UpdatePostingList(ContainerEntryId postingListId, in NativeList<long> modifications, FastPForEncoder pForEncoder, ref FastPForDecoder pForDecoder, ref ContextBoundNativeList<long> tempListBuffer)
        {
            var setSpace = Container.GetMutable(_searchState.Llt, postingListId);
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);

            var lists = stackalloc long*[2];
            var indexes = stackalloc int[2];

            using var _1 = _searchState.Llt.Allocator.Allocate(modifications.Count * sizeof(long), out var bs1);
            using var _2 = _searchState.Llt.Allocator.Allocate(modifications.Count * sizeof(long), out var bs2);
            lists[0] = (long*)bs1.Ptr;
            lists[1] = (long*)bs2.Ptr;

            for (int i = 0; i < modifications.Count; i++)
            {
                var cur = modifications[i];
                var listIdx = cur & 1;
                var curIndex = indexes[listIdx]++;
                lists[listIdx][curIndex] = cur;
            }

            var numberOfEntries = PostingList.Update(_searchState.Llt, ref postingListState, lists[0], indexes[0],
                lists[1], indexes[1], pForEncoder, ref tempListBuffer, ref pForDecoder);

            if (numberOfEntries is 0)
            {
                _largePostingListSet ??= _searchState.Llt.Transaction.OpenPostingList(Constants.PostingList.PostingListRegister);
                _largePostingListSet.Remove((long)postingListId);
                Container.Delete(_searchState.Llt, _searchState.Options.Container, postingListId);
                return 0;
            }

            return (long)postingListId | Constants.Graphs.VectorId.PostingList;
        }

        private long CreateNewPostingList(FastPForEncoder pforEncoder)
        {
            var setId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, sizeof(PostingListState), out var setSpace);

            _largePostingListSet ??= _searchState.Llt.Transaction.OpenPostingList(Constants.PostingList.PostingListRegister);
            _largePostingListSet.Add((long)setId);
            
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);
            PostingList.Create(_searchState.Llt, ref postingListState, pforEncoder);
            return (long)setId | Constants.Graphs.VectorId.PostingList;
        }


        // The code of a node is written in the transaction that creates the node. The parameters are fixed by the first commit that creates a node.
        private void WriteCodes()
        {
            var state = _searchState;
            if (state.Options.SimilarityMethod != SimilarityMethod.CosineSimilaritySingles || state.CreatedNodes == 0)
                return;

            var parameters = state.ReadCodeParameters() ?? FixCodeParameters();
            var scratch = new double[parameters.Dimensions];
            for (var i = 0; i < state.CreatedNodes; i++)
            {
                ref var node = ref state.GetNodeByIndex(state.GetCreatedNodeIndex(i));
                state.WriteCode(node.NodeId, parameters, node.GetVector(state), scratch);
            }
        }

        // The center is the mean of the unit vectors that this commit creates that have a direction; it is zero when there is none.
        private CodeParameters FixCodeParameters()
        {
            var state = _searchState;
            var sum = new double[(state.Options.VectorSizeBytes - MagnitudeSizeInBytes) / sizeof(float)];
            var count = 0;
            for (var i = 0; i < state.CreatedNodes; i++)
            {
                var tensor = state.GetNodeByIndex(state.GetCreatedNodeIndex(i)).GetVector(state);
                if (CodeParameters.HasDirection(tensor) == false)
                    continue;

                var unit = ReadTensorVector<float>(tensor);
                for (var k = 0; k < sum.Length; k++)
                    sum[k] += unit[k];
                count++;
            }

            var center = new float[sum.Length];
            for (var k = 0; k < center.Length; k++)
                center[k] = (float)(sum[k] / Math.Max(count, 1));

            // Never the level Random: a draw from it would shift the level of every later node.
            var parameters = CodeParameters.Draw(center, _forTestingPurposes?.CodeRandom ?? Random.Shared);
            using (state.Tree.DirectAdd(CodeParametersSlice, parameters.StoredBytes, out var destination))
                parameters.Write(new Span<byte>(destination, parameters.StoredBytes));
            return parameters;
        }

        // A node that matches the record it was loaded from is skipped, and a stored record that already holds the encoded bytes is left unmodified, and so is its page.
        void PersistNode(ref Node node, ref ContextBoundNativeList<byte> byteBuffer)
        {
            if (node.DiffersFromStoredRecord == false)
                return;

            var encoded = node.Encode(ref byteBuffer);
            if (_searchState.TryGetLocationForNode(node.NodeId, out var locationId))
            {
                var id = new ContainerEntryId(locationId);
                Container.Get(_searchState.Llt, id, out var stored);
                if (stored.ToSpan().SequenceEqual(encoded))
                    return;

                if (stored.Length == encoded.Length)
                {
                    encoded.CopyTo(Container.GetMutable(_searchState.Llt, id));
                    return;
                }

                Container.Delete(_searchState.Llt, _searchState.Options.Container, id);
            }

            var allocatedId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, encoded.Length, out var storage);
            locationId = (long)allocatedId;
            _searchState.RegisterNodeLocation(node.NodeId, locationId);
            encoded.CopyTo(storage);
        }
    }

    public static Registration RegistrationFor(LowLevelTransaction llt, string name, Random random = null)
    {
        Slice.From(llt.Allocator, name, out var slice);
        return RegistrationFor(llt, slice, random);
    }

    public static Registration RegistrationFor(LowLevelTransaction llt, Slice name, Random random = null)
    {
        return new Registration(llt, name, random);
    }
    
    public static VectorSearchRetriever ExactNearest(LowLevelTransaction llt, Slice name, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity, bool hasFilterMatch, ContextBoundNativeList<long>? nodesToScan = null)
    {
        var searchState = new SearchState(llt, name, ref vector);
        var results = searchState.ExactSearch(vector, hasFilterMatch, numberOfCandidates, nodesToScan);
        return new VectorSearchRetriever(searchState,  results, vector, minimumSimilarity);
    }

    public static VectorSearchRetriever ApproximateNearest(LowLevelTransaction llt, Slice name, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity, bool hasFilterMatch = false)
    {
        var searchState = new SearchState(llt, name, ref vector);
        if (searchState.Options.CountOfVectors == 0)
            return new VectorSearchRetriever(searchState, searchState.EmptySearch(), vector, minimumSimilarity);

        return new VectorSearchRetriever(searchState, searchState.ApproximateSearch(vector, numberOfCandidates, hasFilterMatch), vector, minimumSimilarity);
    }

    /// <summary>
    /// Approximate nearest neighbor search using a caller-owned <see cref="SearchState"/>. The
    /// caller keeps the SearchState alive across multiple queries and is responsible for
    /// disposing it; node data loaded by earlier queries remains in SearchState for later ones,
    /// so repeated traversals avoid the node-index resolution and container reads already paid
    /// for on the first pass.
    /// </summary>
    public static VectorSearchRetriever ApproximateNearest(SearchState searchState, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity, bool hasFilterMatch = false)
    {
        if (searchState.IsEmpty)
            return EmptyRetriever(searchState, vector, minimumSimilarity);

        TryNormalizeQueryVector(searchState.Options, searchState.Llt.Allocator, ref vector, out var queryVectorScope);
        // Bind the per-node QueryDistance cache to this query vector before SearchNearestAcrossLevels
        // reads it. Without this, a shared SearchState reuses cached distances from a prior sub-query
        // and the upper-level descent latches onto the wrong starting point.
        searchState.OnQueryVector(vector);

        if (searchState.Options.CountOfVectors == 0)
            return new VectorSearchRetriever(searchState, searchState.EmptySearch(), vector, minimumSimilarity, ownsSearchState: false, queryVectorScope);

        return new VectorSearchRetriever(searchState, searchState.ApproximateSearch(vector, numberOfCandidates, hasFilterMatch), vector, minimumSimilarity,
            ownsSearchState: false, queryVectorScope);
    }

    /// <summary>
    /// Exact nearest neighbor search using a caller-owned SearchState.
    /// The caller is responsible for disposing the SearchState.
    /// </summary>
    public static VectorSearchRetriever ExactNearest(SearchState searchState, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity, bool hasFilterMatch, ContextBoundNativeList<long>? nodesToScan = null)
    {
        if (searchState.IsEmpty)
            return EmptyRetriever(searchState, vector, minimumSimilarity);

        TryNormalizeQueryVector(searchState.Options, searchState.Llt.Allocator, ref vector, out var queryVectorScope);
        searchState.OnQueryVector(vector);
        var results = searchState.ExactSearch(vector, hasFilterMatch, numberOfCandidates, nodesToScan);
        return new VectorSearchRetriever(searchState, results, vector, minimumSimilarity, ownsSearchState: false, queryVectorScope);
    }

    // A graph with no stored options has no vector size to check the query against.
    private static VectorSearchRetriever EmptyRetriever(SearchState searchState, Memory<byte> vector, float minimumSimilarity)
        => new(searchState, searchState.EmptySearch(), vector, minimumSimilarity, ownsSearchState: false);

    public static VectorSearchRetriever EmptySearch(LowLevelTransaction llt, Slice name, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity)
    {
        var searchState = new SearchState(llt, name);
        return new VectorSearchRetriever(searchState, searchState.EmptySearch(), vector, minimumSimilarity);
    }
}
