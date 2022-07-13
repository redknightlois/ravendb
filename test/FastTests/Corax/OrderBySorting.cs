﻿using System;
using System.Collections.Generic;
using System.Text;
using Corax.Queries;
using Corax;
using FastTests.Voron;
using Sparrow.Server;
using Voron;
using Xunit.Abstractions;
using Xunit;
using Sparrow.Threading;

namespace FastTests.Corax
{
    public class OrderBySortingTests : StorageTest
    {
        private readonly List<IndexSingleNumericalEntry<long>> longList = new();
        private const int IndexId = 0, ContentId = 1;

        public OrderBySortingTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void OrderByNumber()
        {
            PrepareData();
            IndexEntries();
            longList.Sort(CompareDescending);
            using var searcher = new IndexSearcher(Env);
            {
                var allEntries = searcher.AllEntries();
                var match1 = searcher.StartWithQuery("Id", "l");
                var concat = searcher.And(allEntries, match1);
                var match = searcher.OrderByDescending(concat, ContentId, MatchCompareFieldType.Integer);

                List<string> sortedByCorax = new();
                Span<long> ids = stackalloc long[2048];
                int read = 0;
                do
                {
                    read = match.Fill(ids);
                    for (int i = 0; i < read; ++i)
                        sortedByCorax.Add(searcher.GetIdentityFor(ids[i]));
                }
                while (read != 0);

                for (int i = 0; i < longList.Count; ++i)
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);
                
                Assert.Equal(1000, sortedByCorax.Count);
            }
        }

        private static int CompareAscending(IndexSingleNumericalEntry<long> value1, IndexSingleNumericalEntry<long> value2)
        {
            return value1.Content.CompareTo(value2.Content);
        }

        private static int CompareDescending(IndexSingleNumericalEntry<long> value1, IndexSingleNumericalEntry<long> value2)
        {
            return value2.Content.CompareTo(value1.Content);
        }
        private void PrepareData()
        {
            for (int i = 0; i < 1000; ++i)
            {
                longList.Add(new IndexSingleNumericalEntry<long>
                {
                    Id = $"list/{i}",
                    Content = i
                });
            }
        }

        private void IndexEntries()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var knownFields = CreateKnownFields(bsc);

            {
                using var indexWriter = new IndexWriter(Env, knownFields);
                var entryWriter = new IndexEntryWriter(bsc, knownFields);
                foreach (var entry in longList)
                {
                    using (var _ = CreateIndexEntry(ref entryWriter, entry, out var data))
                    {
                        indexWriter.Index(entry.Id, data.ToSpan());
                    }
                }
                indexWriter.Commit();
            }
        }

        private ByteStringContext<ByteStringMemoryCache>.InternalScope CreateIndexEntry(ref IndexEntryWriter entryWriter, IndexSingleNumericalEntry<long> entry, out ByteString output)
        {
            entryWriter.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
            entryWriter.Write(ContentId, Encoding.UTF8.GetBytes(entry.Content.ToString()), entry.Content, Convert.ToDouble(entry.Content));
            return entryWriter.Finish(out output);
        }

        private IndexFieldsMapping CreateKnownFields(ByteStringContext bsc)
        {
            Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(bsc, "Content", ByteStringType.Immutable, out Slice contentSlice);

            return new IndexFieldsMapping(bsc)
                .AddBinding(IndexId, idSlice)
                .AddBinding(ContentId, contentSlice);
        }

        private class IndexSingleNumericalEntry<T>
        {
            public string Id { get; set; }
            public T Content { get; set; }
        }
    }
}
