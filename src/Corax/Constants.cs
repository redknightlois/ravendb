using System;
using Sparrow.Server;
using Voron;

namespace Corax
{
    public static class Constants
    {
        public const string NullValue = "NULL_VALUE";
        public static readonly ReadOnlyMemory<char> NullValueCharSpan = new(Constants.NullValue.ToCharArray());
        
        
        public const string EmptyString = "EMPTY_STRING";
        public static readonly ReadOnlyMemory<char> EmptyStringCharSpan = new(Constants.EmptyString.ToCharArray());
        
        public const string IndexMetadata = "@index_metadata";
        public const string IndexTimeFields = "@index_time_fields";
        public const string DocumentBoost = "@document_boost";
        
        public static readonly Slice NullValueSlice, EmptyStringSlice, IndexMetadataSlice, DocumentBoostSlice, IndexTimeFieldsSlice;

        static Constants()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, NullValue, ByteStringType.Immutable, out NullValueSlice);
                Slice.From(ctx, EmptyString, ByteStringType.Immutable, out EmptyStringSlice);
                Slice.From(ctx, IndexMetadata, ByteStringType.Immutable, out IndexMetadataSlice);
                Slice.From(ctx, DocumentBoost, ByteStringType.Immutable, out DocumentBoostSlice);
                Slice.From(ctx, IndexTimeFields, ByteStringType.Immutable, out IndexTimeFieldsSlice);
            }
        }
        
        public static class Terms
        {
            public const int MaxLength = 512;
        }

        public static class Boosting
        {
            public static float ScoreEpsilon = 1e-8F;
        }

        public static class IndexSearcher
        {
            public const int InvalidId = -1;
            public const int TakeAll = -1;
            public const int NonAnalyzer = -1;
        }

        public static class IndexWriter
        {
            public static ReadOnlySpan<byte> DoubleTreeSuffix => DoubleTreeSuffixBytes.AsSpan();
            private static readonly byte[] DoubleTreeSuffixBytes = new byte[]  { (byte)'-', (byte)'D' };

            public static ReadOnlySpan<byte> LongTreeSuffix => LongTreeSuffixBytes.AsSpan();
            private static readonly byte[] LongTreeSuffixBytes = new byte[]  { (byte)'-', (byte)'L' };

            
            public static readonly Slice LargePostingListsSetSlice, PostingListsSlice,  EntryIdToLocationSlice, LastEntryIdSlice, StoreFieldsSlice, EntriesTermsContainerSlice, FieldsSlice, NumberOfEntriesSlice, EntriesToSpatialSlice, EntriesToTermsSlice, DynamicFieldsAnalyzersSlice, TimeFieldsSlice, NumberOfTermsInIndex;            public const int IntKnownFieldMask = unchecked((int)0x80000000);
            public const short ShortKnownFieldMask = unchecked((short)0x8000);
            public const byte ByteKnownFieldMask = unchecked((byte)0x80);
            public const int DynamicField = -2;
            static IndexWriter()
            {
                using (StorageEnvironment.GetStaticContext(out var ctx))
                {
                    Slice.From(ctx, "Fields", ByteStringType.Immutable, out FieldsSlice);
                    Slice.From(ctx, "PostingLists", ByteStringType.Immutable, out PostingListsSlice);
                    Slice.From(ctx, "LargePostingListsSet", ByteStringType.Immutable, out LargePostingListsSetSlice);
                    Slice.From(ctx, "StoreFields", ByteStringType.Immutable, out StoreFieldsSlice);
                    Slice.From(ctx, "EntriesTerms", ByteStringType.Immutable, out EntriesTermsContainerSlice);
                    Slice.From(ctx, "EntryIdToLocation", ByteStringType.Immutable, out EntryIdToLocationSlice);
                    Slice.From(ctx, "NumberOfEntries", ByteStringType.Immutable, out NumberOfEntriesSlice);
                    Slice.From(ctx, "LastEntryId", ByteStringType.Immutable, out LastEntryIdSlice);
                    Slice.From(ctx, "EntriesToTerms", ByteStringType.Immutable, out EntriesToTermsSlice);
                    Slice.From(ctx, "EntriesToSpatial", ByteStringType.Immutable, out EntriesToSpatialSlice);
                    Slice.From(ctx, "DynamicFieldsAnalyzers", ByteStringType.Immutable, out DynamicFieldsAnalyzersSlice);
                    Slice.From(ctx, "NumberOfTermsInIndex", ByteStringType.Immutable, out NumberOfTermsInIndex);
                }
            }
        }

        public static class StorageMask
        {
            public const long ContainerType = ~0b11;
        }
        
        public static class Search
        {
            public const byte Wildcard = (byte)'*';

            [Flags]
            public enum SearchMatchOptions
            {
                TermMatch = 0,
                StartsWith = 1,
                EndsWith = 2,
                Contains = StartsWith | EndsWith
            }

            public enum Operator
            {
                Or,
                And
            }
        }

        public static class Analyzers
        {
            public const int DefaultBufferForAnalyzers = 4 * Sparrow.Global.Constants.Size.Kilobyte;
        }

        public static class Primitives
        {
            internal const int DefaultBufferSize = 4 * Sparrow.Global.Constants.Size.Kilobyte;
        }

        public static class Suggestions
        {
            public const int DefaultNGramSize = 4;

            public const float DefaultAccuracy = 0.7f;

            public enum Algorithm
            {
                NGram,
                JaroWinkler,
                Levenshtein 
            }
        }
    }
}
