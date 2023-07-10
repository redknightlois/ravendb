﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Utils;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_19283 : StorageTest
{
    public RavenDB_19283(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public unsafe void CanReadAndWriteLargeEntries()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        
        using var _ = StorageEnvironment.GetStaticContext(out var ctx);
        Slice.From(ctx, "Items", ByteStringType.Immutable, out Slice itemsSlice);
        Slice.From(ctx, "id()", ByteStringType.Immutable, out Slice idSlice);
        
        // The idea is that GetField will return an struct we can use later on a loop (we just get it once).
        
        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, idSlice)
            .AddBinding(1, itemsSlice, shouldStore:true);
        using var knownFields = builder.Build();

        long entryId;
        using (var indexWriter = new IndexWriter(Env, knownFields))
        {
            var options = new[] { "one", "two", "three" };
            using (var writer = indexWriter.Index("users/1"))
            {
                writer.Write(0, Encoding.UTF8.GetBytes("users/1"));
                var tags = Enumerable.Range(0, 10000).Select(x => options[x % options.Length]);

                entryId = writer.EntryId;
                
                writer.IncrementList();
                {
                    foreach (string tag in tags)
                    {
                        writer.Write(1, Encoding.UTF8.GetBytes(tag));
                    }
                }
                writer.DecrementList();
            }
            indexWriter.PrepareAndCommit();
        }

        using (var indexSearcher = new IndexSearcher(Env, knownFields))
        {
            Page p = default;
            var reader = indexSearcher.GetEntryTermsReader(entryId, ref p);
            long fieldRootPage = indexSearcher.FieldCache.GetLookupRootPage(itemsSlice);
            long i = 0;
            while (reader.FindNextStored(fieldRootPage))
            {
                i++;
            }
            Assert.Equal(10000, i);
        }
    }
}
