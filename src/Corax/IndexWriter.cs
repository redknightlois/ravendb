using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Pipeline;
using Corax.Utils;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Compression;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Sets;
using Voron.Impl;

namespace Corax
{
    // container ids are guaranteed to be aligned on 
    // 4 bytes boundary, we're using this to store metadata
    // about the data
    public enum TermIdMask : long
    {
        Single = 0,
        Small = 1,
        Set = 2
    }

    public class IndexWriter : IDisposable // single threaded, controlled by caller
    {
        public const int MaxTermLength = 1024;

        private readonly IndexFieldsMapping _fieldsMapping;

        private readonly StorageEnvironment _environment;

        private readonly bool _ownsTransaction;
        private JsonOperationContext _jsonOperationContext;
        public readonly Transaction Transaction;
        private readonly TransactionPersistentContext _transactionPersistentContext;


        // CPU bound - embarassingly parallel
        // 
        // private readonly ConcurrentDictionary<Slice, Dictionary<Slice, ConcurrentQueue<long>>> _bufferConcurrent =
        //     new ConcurrentDictionary<Slice, ConcurrentDictionary<Slice, ConcurrentQueue<long>>>(SliceComparer.Instance);

        private readonly Dictionary<Slice, Dictionary<Slice, List<long>>> _buffer =
            new Dictionary<Slice, Dictionary<Slice, List<long>>>(SliceComparer.Instance);

        private readonly List<long> _entriesToDelete = new List<long>();

        private readonly long _postingListContainerId, _entriesContainerId;

        private const string SuggestionsTreePrefix = "__Suggestion_";
        private Dictionary<int, Dictionary<Slice, int>> _suggestionsAccumulator;

#pragma warning disable CS0169
        private Queue<long> _lastEntries; // keep last 256 items
#pragma warning restore CS0169

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index writer with opening semantics and also every new
        // writer becomes essentially a unit of work which makes reusing assets tracking more explicit.
        public IndexWriter([NotNull] StorageEnvironment environment, IndexFieldsMapping fieldsMapping = null)
        {
            _environment = environment;
            _transactionPersistentContext = new TransactionPersistentContext(true);
            Transaction = _environment.WriteTransaction(_transactionPersistentContext);

            _ownsTransaction = true;
            _postingListContainerId = Transaction.OpenContainer(Constants.IndexWriter.PostingListsSlice);
            _entriesContainerId = Transaction.OpenContainer(Constants.IndexWriter.EntriesContainerSlice);
            _jsonOperationContext = JsonOperationContext.ShortTermSingleUse();
            _fieldsMapping = fieldsMapping ?? IndexFieldsMapping.Instance;
        }

        public IndexWriter([NotNull] Transaction tx, IndexFieldsMapping fieldsMapping = null)
        {
            Transaction = tx;

            _ownsTransaction = false;
            _postingListContainerId = Transaction.OpenContainer(Constants.IndexWriter.PostingListsSlice);
            _entriesContainerId = Transaction.OpenContainer(Constants.IndexWriter.EntriesContainerSlice);

            _fieldsMapping = fieldsMapping ?? IndexFieldsMapping.Instance;
        }


        public long Index(string id, Span<byte> data, IndexFieldsMapping knownFields)
        {
            using var _ = Slice.From(Transaction.Allocator, id, out var idSlice);
            return Index(idSlice, data, knownFields);
        }

        public long Index(Slice id, Span<byte> data, IndexFieldsMapping knownFields)
        {
            long entriesCount = Transaction.LowLevelTransaction.RootObjects.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;
            Transaction.LowLevelTransaction.RootObjects.Add(Constants.IndexWriter.NumberOfEntriesSlice, entriesCount + 1);

            Span<byte> buf = stackalloc byte[10];
            var idLen = ZigZagEncoding.Encode(buf, id.Size);
            var entryId = Container.Allocate(Transaction.LowLevelTransaction, _entriesContainerId, idLen + id.Size + data.Length, out var space);
            buf.Slice(0, idLen).CopyTo(space);
            space = space.Slice(idLen);
            id.CopyTo(space);
            space = space.Slice(id.Size);
            data.CopyTo(space);

            var context = Transaction.Allocator;
            var entryReader = new IndexEntryReader(data);

            foreach (var binding in knownFields)
            {
                if (binding.FieldIndexingMode is FieldIndexingMode.No)
                    continue;

                var key = binding.FieldName;
                if (_buffer.TryGetValue(key, out var field) == false)
                {
                    //PERF: avoid creating dictionary
                    _buffer[key] = field = new Dictionary<Slice, List<long>>(SliceComparer.Instance);
                }

                if (binding.IsAnalyzed)
                {
                    InsertAnalyzedToken(context, ref entryReader, field, entryId, binding);
                }
                else
                {
                    InsertToken(context, ref entryReader, field, entryId, binding);
                }
            }

            return entryId;
        }

        public long GetNumberOfEntries()
        {
            return Transaction.LowLevelTransaction.RootObjects.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0L;
        }

        private unsafe void AddSuggestions(IndexFieldBinding binding, Slice slice)
        {
            _suggestionsAccumulator ??= new Dictionary<int, Dictionary<Slice, int>>();

            if (_suggestionsAccumulator.TryGetValue(binding.FieldId, out var suggestionsToAdd) == false)
            {
                suggestionsToAdd = new Dictionary<Slice, int>();
                _suggestionsAccumulator[binding.FieldId] = suggestionsToAdd;
            }

            var keys = SuggestionsKeys.Generate(Transaction.Allocator, Constants.Suggestions.DefaultNGramSize, slice.AsSpan(), out int keysCount);
            int keySizes = keys.Length / keysCount;

            var bsc = Transaction.Allocator;

            int idx = 0;
            while (idx < keysCount)
            {
                var key = new Slice(bsc.Slice(keys, idx * keySizes, keySizes, ByteStringType.Immutable));
                if (suggestionsToAdd.TryGetValue(key, out int counter) == false)
                    counter = 0;

                counter++;
                suggestionsToAdd[key] = counter;
                idx++;
            }
        }

        private void RemoveSuggestions(IndexFieldBinding binding, ReadOnlySpan<byte> sequence)
        {
            if (_suggestionsAccumulator == null)
                _suggestionsAccumulator = new Dictionary<int, Dictionary<Slice, int>>();

            if (_suggestionsAccumulator.TryGetValue(binding.FieldId, out var suggestionsToAdd) == false)
            {
                suggestionsToAdd = new Dictionary<Slice, int>();
                _suggestionsAccumulator[binding.FieldId] = suggestionsToAdd;
            }

            var keys = SuggestionsKeys.Generate(Transaction.Allocator, Constants.Suggestions.DefaultNGramSize, sequence, out int keysCount);
            int keySizes = keys.Length / keysCount;

            var bsc = Transaction.Allocator;

            int idx = 0;
            while (idx < keysCount)
            {
                var key = new Slice(bsc.Slice(keys, idx * keySizes, keySizes, ByteStringType.Immutable));
                if (suggestionsToAdd.TryGetValue(key, out int counter) == false)
                    counter = 0;

                counter--;
                suggestionsToAdd[key] = counter;
                idx++;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void AddMaybeAvoidDuplicate(List<long> term, long entryId)
        {
            // TODO: Do we want to index nulls? If so, how do we do that?
            if (term.Count > 0 && term[^1] == entryId)
                return;

            term.Add(entryId);
        }

        [SkipLocalsInit]
        private unsafe void InsertAnalyzedToken(ByteStringContext context, ref IndexEntryReader entryReader, Dictionary<Slice, List<long>> field, long entryId,
            IndexFieldBinding binding)
        {
            Analyzer analyzer = binding.Analyzer;
            analyzer.GetOutputBuffersSize(Analyzer.MaximumSingleTokenLength, out int bufferSize, out int tokenSize);

            byte* tempWordsSpace = stackalloc byte[bufferSize];
            Token* tempTokenSpace = stackalloc Token[tokenSize];

            int tokenField = binding.FieldId;
            var fieldType = entryReader.GetFieldType(tokenField, out var intOffset);
            if (fieldType == IndexEntryFieldType.Null || fieldType.HasFlag(IndexEntryFieldType.Empty))
            {
                var fieldName = fieldType == IndexEntryFieldType.Null ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                if (field.TryGetValue(fieldName, out var term) == false)
                    field[fieldName] = term = new List<long>();

                AddMaybeAvoidDuplicate(term, entryId);
                return;
            }
            else if (fieldType.HasFlag(IndexEntryFieldType.Raw))
            {
            }
            else if (fieldType.HasFlag(IndexEntryFieldType.List))
            {
                // TODO: For performance we can retrieve the whole thing and execute the analyzer many times in a loop for each token
                //       that will ensure faster turnaround and more efficient execution. 
                if (fieldType.HasFlag(IndexEntryFieldType.SpatialPoint))
                {
                    var iterator = entryReader.ReadManySpatialPoint(binding.FieldId);
                    while (iterator.ReadNext())
                    {
                        for (int i = 1; i <= iterator.Geohash.Length; ++i)
                            ProcessTerm(iterator.Geohash.Slice(0, i));
                    }
                }
                else
                {
                    var iterator = entryReader.ReadMany(tokenField);
                    while (iterator.ReadNext())
                    {                        
                        // If null, we just add it and be done with it. 
                        if (iterator.IsNull || iterator.IsEmpty)
                        {
                            var fieldName = iterator.IsNull ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                            if (field.TryGetValue(fieldName, out var term) == false)
                                field[fieldName] = term = new List<long>();

                            AddMaybeAvoidDuplicate(term, entryId);
                            continue;
                        }

                        ProcessTerm(iterator.Sequence);
                    }
                }
            }
            else
            {                
                if (entryReader.Read(binding.FieldId, out Span<byte> valueInEntry))
                {                   
                    if (valueInEntry.Length == 0)
                    {
                        var fieldName = Constants.EmptyStringSlice;
                        if (field.TryGetValue(fieldName, out var term) == false)
                            field[fieldName] = term = new List<long>();

                        AddMaybeAvoidDuplicate(term, entryId);
                    }
                    else if (fieldType.HasFlag(IndexEntryFieldType.SpatialPoint))
                    {
                        for (int i = 1; i <= valueInEntry.Length; ++i)
                            ProcessTerm(valueInEntry.Slice(0, i));
                    }
                    else if (fieldType.HasFlag(IndexEntryFieldType.Tuple) || fieldType.HasFlag(IndexEntryFieldType.Invalid) == false)
                    {
                        ProcessTerm(valueInEntry);
                    }
                }
            }

            void ProcessTerm(ReadOnlySpan<byte> value)
            {
                var words = new Span<byte>(tempWordsSpace, bufferSize);
                var tokens = new Span<Token>(tempTokenSpace, tokenSize);
                analyzer.Execute(value, ref words, ref tokens);

                for (int i = 0; i < tokens.Length; i++)
                {
                    ref var token = ref tokens[i];

                    using var _ = Slice.From(context, words.Slice(token.Offset, (int)token.Length), ByteStringType.Mutable, out var slice);
                    if (field.TryGetValue(slice, out var term) == false)
                    {
                        var fieldName = slice.Clone(context);
                        field[fieldName] = term = new List<long>();
                    }

                    AddMaybeAvoidDuplicate(term, entryId);

                    if (binding.HasSuggestions)
                        AddSuggestions(binding, slice);
                }
            }
        }

        [SkipLocalsInit]
        private void InsertToken(ByteStringContext context, ref IndexEntryReader entryReader, Dictionary<Slice, List<long>> field, long entryId,
            IndexFieldBinding binding)
        {
            int fieldId = binding.FieldId;
            var fieldType = entryReader.GetFieldType(fieldId, out var _);
            if (fieldType == IndexEntryFieldType.Null || fieldType.HasFlag(IndexEntryFieldType.Empty))
            {
                InsertNullOrEmpty(field, entryId, fieldType);
            }
            else if (fieldType.HasFlag(IndexEntryFieldType.Raw))
            {
            }
            else if (fieldType.HasFlag(IndexEntryFieldType.List))
            {
                // TODO: For performance we can retrieve the whole thing and execute the analyzer many times in a loop for each token
                //       that will ensure faster turnaround and more efficient execution. 

                if (fieldType.HasFlag(IndexEntryFieldType.SpatialPoint))
                {
                    var iterator = entryReader.ReadManySpatialPoint(binding.FieldId);
                    while (iterator.ReadNext())
                    {
                        for (int i = 1; i <= iterator.Geohash.Length; ++i)
                            ProcessTerm(iterator.Geohash.Slice(0, i));
                    }
                }
                else
                {
                    var iterator = entryReader.ReadMany(fieldId);
                    while (iterator.ReadNext())
                    {
                        if (iterator.IsNull || iterator.IsEmpty)
                        {
                            InsertNullOrEmpty(field, entryId, fieldType);
                            continue;
                        }

                        ProcessTerm(iterator.Sequence);
                    }
                }
            }
            else
            {
                entryReader.Read(binding.FieldId, out Span<byte> valueInEntry);
                if (fieldType.HasFlag(IndexEntryFieldType.SpatialPoint))
                {
                    for (int i = 1; i <= valueInEntry.Length; ++i)
                        ProcessTerm(valueInEntry.Slice(0, i));
                }
                else if (fieldType.HasFlag(IndexEntryFieldType.Tuple) || fieldType.HasFlag(IndexEntryFieldType.Invalid) == false)
                {
                    Debug.Assert(!fieldType.HasFlag(IndexEntryFieldType.Empty));
                    ProcessTerm(valueInEntry);
                }
            }


            void ProcessTerm(ReadOnlySpan<byte> value)
            {
                using var _ = Slice.From(context, value, ByteStringType.Immutable, out var slice);
                if (field.TryGetValue(slice, out var term) == false)
                {
                    var fieldName = slice.Clone(context);
                    field[fieldName] = term = new List<long>();
                }

                AddMaybeAvoidDuplicate(term, entryId);

                if (binding.HasSuggestions)
                    AddSuggestions(binding, slice);
            }

            void InsertNullOrEmpty(Dictionary<Slice, List<long>> field, long entryId, IndexEntryFieldType fieldType)
            {
                var fieldName = fieldType == IndexEntryFieldType.Null ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                if (field.TryGetValue(fieldName, out var term) == false)
                    field[fieldName] = term = new List<long>();

                AddMaybeAvoidDuplicate(term, entryId);
            }
        }

        private unsafe void DeleteCommit(Span<byte> tmpBuf, Tree fieldsTree)
        {
            if (_entriesToDelete.Count == 0)
                return;

            Page page = default;
            var llt = Transaction.LowLevelTransaction;

            List<long> ids = null;
            int maxWordsSpaceLength = 0;
            int maxTokensSpaceLength = 0;
            foreach (var binding in _fieldsMapping)
            {
                if (binding.IsAnalyzed == false)
                    continue;

                binding.Analyzer.GetOutputBuffersSize(Analyzer.MaximumSingleTokenLength, out var tempOutputSize, out var tempTokenSize);
                maxWordsSpaceLength = Math.Max(maxWordsSpaceLength, tempOutputSize);
                maxTokensSpaceLength = Math.Max(maxTokensSpaceLength, tempTokenSize);
            }

            Span<byte> tempWordsSpace = stackalloc byte[maxWordsSpaceLength];
            Span<Token> tempTokenSpace = stackalloc Token[maxTokensSpaceLength];

            foreach (var id in _entriesToDelete)
            {
                var entryReader = IndexSearcher.GetReaderFor(Transaction, ref page, id);

                foreach (var binding in _fieldsMapping) // todo maciej: this part needs to be rebuilt after implementing DynamicFields
                {
                    if (binding.IsIndexed == false)
                        continue;

                    var fieldType = entryReader.GetFieldType(binding.FieldId, out var intOffset);
                    if (fieldType.HasFlag(IndexEntryFieldType.List))
                    {
                        if (fieldType.HasFlag(IndexEntryFieldType.SpatialPoint))
                        {
                            var iterator = entryReader.ReadManySpatialPoint(binding.FieldId);
                            while (iterator.ReadNext())
                            {
                                for (int i = 1; i <= iterator.Count; ++i)
                                    DeleteToken(iterator.Geohash.Slice(0, i), tmpBuf, binding, tempWordsSpace, tempTokenSpace);
                            }
                        }
                        else
                        {
                            var it = entryReader.ReadMany(binding.FieldId);
                            while (it.ReadNext())
                            {
                                if (it.IsNull)
                                    DeleteToken(Constants.NullValueSlice.AsSpan(), tmpBuf, binding, tempWordsSpace, tempTokenSpace);
                                else
                                    DeleteToken(it.Sequence, tmpBuf, binding, tempWordsSpace, tempTokenSpace);
                            }
                        }
                    }
                    else
                    {
                        entryReader.Read(binding.FieldId, out var termValue);
                        DeleteToken(termValue, tmpBuf, binding, tempWordsSpace, tempTokenSpace);
                    }
                }

                ids?.Clear();
                Container.Delete(llt, _entriesContainerId, id);
                llt.RootObjects.Increment(Constants.IndexWriter.NumberOfEntriesSlice, -1);
                var numberOfEntries = llt.RootObjects.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;
                Debug.Assert(numberOfEntries >= 0);

                void DeleteToken(ReadOnlySpan<byte> termValue, Span<byte> tmpBuf, IndexFieldBinding binding, Span<byte> wordSpace, Span<Token> tokenSpace)
                {
                    if (binding.IsAnalyzed == false)
                    {
                        DeleteField(id, binding.FieldName, tmpBuf, termValue);
                        if (binding.HasSuggestions)
                            RemoveSuggestions(binding, termValue);

                        return;
                    }

                    var analyzer = binding.Analyzer;
                    analyzer.Execute(termValue, ref wordSpace, ref tokenSpace);

                    for (int i = 0; i < tokenSpace.Length; i++)
                    {
                        ref var token = ref tokenSpace[i];

                        var term = wordSpace.Slice(token.Offset, (int)token.Length);
                        DeleteField(id, binding.FieldName, tmpBuf, term);

                        if (binding.HasSuggestions)
                            RemoveSuggestions(binding, termValue);
                    }
                }
            }

            void DeleteField(long id, Slice fieldName, Span<byte> tmpBuffer, ReadOnlySpan<byte> termValue)
            {
                var fieldTree = fieldsTree.CompactTreeFor(fieldName);
                if (termValue.Length == 0 || fieldTree.TryGetValue(termValue, out var containerId) == false)
                    return;

                if ((containerId & (long)TermIdMask.Set) != 0)
                {
                    var setId = containerId & ~0b11;
                    var setStateSpan = Container.GetMutable(llt, setId);
                    ref var setState = ref MemoryMarshal.AsRef<SetState>(setStateSpan);
                    var set = new Set(llt, fieldName, in setState);
                    set.Remove(id);
                    setState = set.State;
                }
                else if ((containerId & (long)TermIdMask.Small) != 0)
                {
                    var smallSetId = containerId & ~0b11;
                    var buffer = Container.GetMutable(llt, smallSetId);

                    //get first item into ids.
                    var itemsCount = ZigZagEncoding.Decode<int>(buffer, out var len);
                    ids ??= new List<long>(itemsCount);
                    ids.Clear();


                    long pos = len;
                    var currentId = 0L;

                    while (pos < buffer.Length)
                    {
                        var delta = ZigZagEncoding.Decode<long>(buffer, out len, (int)pos);
                        pos += len;
                        currentId += delta;
                        if (currentId == id)
                            continue;
                        ids.Add(currentId);
                    }

                    Container.Delete(llt, _postingListContainerId, smallSetId);
                    fieldTree.TryRemove(termValue, out _);
                    AddNewTerm(ids, fieldTree, termValue, tmpBuffer);
                }
                else
                {
                    fieldTree.TryRemove(termValue, out var _);
                }
            }
        }

        public bool TryDeleteEntry(string key, string term)
        {
            var fieldsTree = Transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
            if (fieldsTree == null)
                return false;

            var fieldTree = fieldsTree.CompactTreeFor(key);
            var entriesCount = Transaction.LowLevelTransaction.RootObjects.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;
            Debug.Assert(entriesCount - _entriesToDelete.Count >= 0);

            if (fieldTree.TryGetValue(term, out long id) == false)
                return false;

            if ((id & (long)TermIdMask.Set) != 0 || (id & (long)TermIdMask.Small) != 0)
                throw new InvalidDataException($"Cannot delete {term} in {key} because it's not {nameof(TermIdMask.Single)}.");

            _entriesToDelete.Add(id);
            return true;
        }

        public void Commit()
        {
            using var _ = Transaction.Allocator.Allocate(Container.MaxSizeInsideContainerPage, out Span<byte> tmpBuf);
            Tree fieldsTree = Transaction.CreateTree(Constants.IndexWriter.FieldsSlice);

            if (_fieldsMapping.Count != 0)
                DeleteCommit(tmpBuf, fieldsTree);

            foreach (var (field, terms) in _buffer)
            {
                var fieldTree = fieldsTree.CompactTreeFor(field);
                var llt = Transaction.LowLevelTransaction;
                var sortedTerms = terms.Keys.ToArray();
                // CPU bounded - embarssingly parallel
                Array.Sort(sortedTerms, SliceComparer.Instance);
                foreach (var term in sortedTerms)
                {
                    var entries = terms[term];
                    ReadOnlySpan<byte> termsSpan = term.AsSpan();

                    // TODO: For now if the term is null (termsSpan.Length == 0) we will not do anything... this happens
                    //       because we are not explicitly handling the case of explicit NULL values (instead of unsetted). 
                    if (termsSpan.Length == 0)
                        continue;

                    if (fieldTree.TryGetValue(termsSpan, out var existing) == false)
                    {
                        AddNewTerm(entries, fieldTree, termsSpan, tmpBuf);
                        continue;
                    }

                    if ((existing & (long)TermIdMask.Set) != 0)
                    {
                        var id = existing & ~0b11;
                        var setSpace = Container.GetMutable(llt, id);
                        ref var setState = ref MemoryMarshal.AsRef<SetState>(setSpace);
                        var set = new Set(llt, Slices.Empty, setState);
                        entries.Sort();
                        set.Add(entries);
                        setState = set.State;
                    }
                    else if ((existing & (long)TermIdMask.Small) != 0)
                    {
                        var id = existing & ~0b11;
                        var smallSet = Container.Get(llt, id).ToSpan();
                        // combine with existing value
                        var cur = 0L;
                        ZigZagEncoding.Decode<int>(smallSet, out var pos); // discard the first entry, the count
                        while (pos < smallSet.Length)
                        {
                            var value = ZigZagEncoding.Decode<long>(smallSet, out var len, pos);
                            pos += len;
                            cur += value;
                            entries.Add(cur);
                        }

                        Container.Delete(llt, _postingListContainerId, id);
                        AddNewTerm(entries, fieldTree, termsSpan, tmpBuf);
                    }
                    else // single
                    {
                        // Same element to add, nothing to do here. 
                        if (entries.Count == 1 && entries[0] == existing)
                            continue;

                        entries.Add(existing);
                        AddNewTerm(entries, fieldTree, termsSpan, tmpBuf);
                    }
                }
            }

            // Check if we have suggestions to deal with. 
            if (_suggestionsAccumulator != null)
            {
                foreach (var (fieldId, values) in _suggestionsAccumulator)
                {
                    Slice.From(Transaction.Allocator, $"{SuggestionsTreePrefix}{fieldId}", out var treeName);
                    var tree = Transaction.CompactTreeFor(treeName);
                    foreach (var (key, counter) in values)
                    {
                        if (tree.TryGetValue(key, out var storedCounter) == false)
                            storedCounter = 0;

                        long finalCounter = storedCounter + counter;
                        if (finalCounter > 0)
                            tree.Add(key, finalCounter);
                        else
                            tree.TryRemove(key, out storedCounter);
                    }
                }
            }


            if (_ownsTransaction)
                Transaction.Commit();
        }

        private unsafe void AddNewTerm(List<long> entries, CompactTree fieldTree, ReadOnlySpan<byte> termsSpan, Span<byte> tmpBuf)
        {
            // common for unique values (guid, date, etc)
            if (entries.Count == 1)
            {
                Debug.Assert(fieldTree.TryGetValue(termsSpan, out var _) == false);

                // just a single entry, store the value inline
                fieldTree.Add(termsSpan, entries[0] | (long)TermIdMask.Single);
                return;
            }

            entries.Sort();

            // try to insert to container value
            //TODO: using simplest delta encoding, need to do better here
            int pos = ZigZagEncoding.Encode(tmpBuf, entries.Count);
            pos += ZigZagEncoding.Encode(tmpBuf, entries[0], pos);
            var llt = Transaction.LowLevelTransaction;
            for (int i = 1; i < entries.Count; i++)
            {
                if (pos + ZigZagEncoding.MaxEncodedSize < tmpBuf.Length)
                {
                    long entry = entries[i] - entries[i - 1];
                    if (entry == 0)
                        continue; // we don't need to store duplicates

                    pos += ZigZagEncoding.Encode(tmpBuf, entry, pos);
                    continue;
                }

                // too big, convert to a set
                var setId = Container.Allocate(llt, _postingListContainerId, sizeof(SetState), out var setSpace);
                ref var setState = ref MemoryMarshal.AsRef<SetState>(setSpace);
                Set.Initialize(llt, ref setState);
                var set = new Set(llt, Slices.Empty, setState);
                entries.Sort();
                set.Add(entries);
                setState = set.State;
                fieldTree.Add(termsSpan, setId | (long)TermIdMask.Set);
                return;
            }

            var termId = Container.Allocate(llt, _postingListContainerId, pos, out var space);
            tmpBuf.Slice(0, pos).CopyTo(space);
            fieldTree.Add(termsSpan, termId | (long)TermIdMask.Small);
        }

        public void Dispose()
        {
            _jsonOperationContext?.Dispose();
            if (_ownsTransaction)
                Transaction?.Dispose();
        }
    }
}
