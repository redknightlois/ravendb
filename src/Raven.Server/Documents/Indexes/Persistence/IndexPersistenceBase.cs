﻿using System;
using Raven.Server.Documents.Queries;
using Raven.Server.Indexing;
using Sparrow.Json;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class IndexPersistenceBase : IDisposable
    {
        protected readonly Index _index;

        protected IndexPersistenceBase(Index index, IIndexReadOperationFactory indexReadOperationFactory)
        {
            IndexReadOperationFactory = indexReadOperationFactory;
            _index = index;
        }

        protected IIndexReadOperationFactory IndexReadOperationFactory { get; }

        public abstract bool HasWriter { get; }

        public abstract void CleanWritersIfNeeded();

        public abstract void Clean(IndexCleanup mode);

        public abstract void Initialize(StorageEnvironment environment);

        public abstract void OnInitializeComplete();

        public abstract void PublishIndexCacheToNewTransactions(IndexTransactionCache transactionCache);

        internal abstract IndexTransactionCache BuildStreamCacheAfterTx(Transaction tx);

        public abstract IndexWriteOperationBase OpenIndexWriter(Transaction writeTransaction, JsonOperationContext indexContext);

        public abstract IndexReadOperationBase OpenIndexReader(Transaction readTransaction, IndexQueryServerSide query = null);

        public abstract bool ContainsField(string field);
        public abstract IndexFacetReadOperationBase OpenFacetedIndexReader(Transaction readTransaction);
        public abstract SuggestionIndexReaderBase OpenSuggestionIndexReader(Transaction readTransaction, string field);
        internal abstract void RecreateSearcher(Transaction asOfTx);
        internal abstract void RecreateSuggestionsSearchers(Transaction asOfTx);
        public abstract void DisposeWriters();
        public abstract void Dispose();
    }


}
