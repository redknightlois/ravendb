using System;
using System.Collections.Generic;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Graphs;

namespace Raven.Server.Indexing
{
    public sealed class IndexTransactionCache
    {
        public sealed class CollectionEtags
        {
            public long LastIndexedEtag;
            public long LastProcessedTombstoneEtag;
            public long LastProcessedTimeSeriesDeletedRangeEtag;
            public Dictionary<string, ReferenceCollectionEtags> LastReferencedEtags;
            public ReferenceCollectionEtags LastReferencedEtagsForCompareExchange;
        }

        public sealed class ReferenceCollectionEtags
        {
            public long LastEtag;
            public long LastProcessedTombstoneEtag;
        }

        public sealed class DirectoryFiles
        {
            public Dictionary<string, Tree.ChunkDetails[]> ChunksByName = new Dictionary<string, Tree.ChunkDetails[]>(StringComparer.OrdinalIgnoreCase);
        }

        public Dictionary<string, DirectoryFiles> DirectoriesByName = new Dictionary<string, DirectoryFiles>(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, CollectionEtags> Collections = new Dictionary<string, CollectionEtags>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Per-field HNSW node caches for vector search, keyed by field name. Populated by Corax
        /// indexes only (null or empty for Lucene indexes and for Corax indexes without vector
        /// fields). Attached to a transaction via
        /// <see cref="Voron.Impl.LowLevelTransaction.ImmutableExternalState"/>, so a read tx
        /// always sees the cache that matches its snapshot; old caches become unreachable and
        /// are reclaimed by GC once no transaction still references them.
        /// </summary>
        public Dictionary<Slice, Hnsw.NodeCache> VectorNodeCaches;
    }
}
