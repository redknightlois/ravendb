﻿using Corax;
using Corax.Mappings;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class IndexEntryReaderBigDoc : StorageTest
{
    private readonly ITestOutputHelper _testOutputHelper;

    public IndexEntryReaderBigDoc(ITestOutputHelper output, ITestOutputHelper testOutputHelper) : base(output)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public unsafe void CanCreateAndReadBigDocument()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, "id()")
            .AddBinding(1, "Badges", shouldStore: true);
        using var knownFields = builder.Build();
        
        
        long entryId;
        using (var indexWriter = new IndexWriter(Env, knownFields))
        {
            var options = new[] { "one", "two", "three" };
            using (var writer = indexWriter.Index("users/1"))
            {
                writer.Write(0, "users/1"u8);

                entryId = writer.EntryId;
                
                writer.IncrementList();
                {
                    for (int i = 0; i < 7500; i++)
                    {
                        writer.Write(1, "Nice Answer"u8);
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
            long fieldRootPage = indexSearcher.FieldCache.GetLookupRootPage("Badges");
            long i = 0;
            while (reader.FindNextStored(fieldRootPage))
            {
                i++;
            }
            Assert.Equal(7500, i);
        }
    }
}
