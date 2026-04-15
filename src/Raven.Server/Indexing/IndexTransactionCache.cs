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
        /// Per-field pre-warmed HNSW node caches for vector search. Populated by Corax indexes
        /// only; null or empty for Lucene indexes and for Corax indexes without vector fields.
        /// The cache is keyed by vector field name. Because this is attached to
        /// <see cref="Voron.Impl.LowLevelTransaction.ImmutableExternalState"/>, every read tx
        /// sees exactly the cache that matches its snapshot — no explicit holder or ref-counting
        /// is needed; the GC reclaims old caches once all referencing transactions have disposed.
        /// </summary>
        public Dictionary<Slice, Hnsw.NodeCache> VectorNodeCaches;
    }
}
