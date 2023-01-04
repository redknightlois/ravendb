using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Utils;
using Corax.Utils.Spatial;
using Newtonsoft.Json.Linq;
using Sparrow;
using Sparrow.Server;
using static Corax.Queries.SortingMatch;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct SortingMatch<TInner, TComparer> : IQueryMatch
        where TInner : IQueryMatch
        where TComparer : struct, IMatchComparer
    {
        private readonly IndexSearcher _searcher;
        private readonly IQueryMatch _inner;        
        private readonly TComparer _comparer;
        private readonly int _take;
        private readonly bool _isScoreComparer;
        private readonly delegate*<ref SortingMatch<TInner, TComparer>, Span<long>, int> _fillFunc;

        private const int NotStarted = -1;
        private int _currentIdx;

        internal long* _buffer;
        internal int _bufferSize;
        internal IDisposable _bufferHandler;

        public long TotalResults;

        public SortingMatch(IndexSearcher searcher, in TInner inner, in TComparer comparer, int take = -1)
        {
            _searcher = searcher;
            _inner = inner;
            _take = take;
            _comparer = comparer;
            _isScoreComparer = typeof(TComparer) == typeof(BoostingComparer);
            _currentIdx = NotStarted;

            TotalResults = 0;

            if (typeof(TComparer) == typeof(BoostingComparer))
            {
                _fillFunc = &FillNoPagingScore;
            }
            else
            {
                _fillFunc = _comparer.FieldType switch
                {
                    MatchCompareFieldType.Sequence or MatchCompareFieldType.Alphanumeric => &Fill<SequenceItem>,
                    MatchCompareFieldType.Integer => &Fill<NumericalItem<long>>,
                    MatchCompareFieldType.Floating => &Fill<NumericalItem<double>>,
                    MatchCompareFieldType.Spatial => &Fill<NumericalItem<double>>,
                    MatchCompareFieldType.Score => &FillNoPagingScore,
                    _ => throw new ArgumentOutOfRangeException(_comparer.FieldType.ToString())
                };
            }
        }

        public long Count => _inner.Count;

        public QueryCountConfidence Confidence => throw new NotSupportedException();

        public bool IsBoosting => _inner.IsBoosting || _isScoreComparer;

        public int AndWith(Span<long> buffer, int matches)
        {
            throw new NotSupportedException($"{nameof(SortingMatch<TInner, TComparer>)} does not support the operation of {nameof(AndWith)}.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            return _fillFunc(ref this, matches);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool Get<TOut, TIn>(IndexSearcher searcher, FieldMetadata binding, long entryId, out TOut storedValue, in TIn comparer) 
            where TOut : struct
            where TIn : IMatchComparer
        {
            var reader = searcher.GetEntryReaderFor(entryId);
    
            if (typeof(TIn) == typeof(SpatialAscendingMatchComparer))
            {
                if (comparer is not SpatialAscendingMatchComparer spatialAscendingMatchComparer)
                    goto Failed;

                var readX = reader.GetFieldReaderFor(binding).Read(out (double lat, double lon) coordinates);
                var distance = SpatialUtils.GetGeoDistance(in coordinates, in spatialAscendingMatchComparer);
                
                storedValue = (TOut)(object)new NumericalItem<double>(distance);
                return readX;
            }
            else if (typeof(TIn) == typeof(SpatialDescendingMatchComparer))
            {
                if (comparer is not SpatialDescendingMatchComparer spatialDescendingMatchComparer)
                    goto Failed;
                
                var readX = reader.GetFieldReaderFor(binding).Read( out (double lat, double lon) coordinates);
                var distance = SpatialUtils.GetGeoDistance(in coordinates, in spatialDescendingMatchComparer);

                storedValue = (TOut)(object)new NumericalItem<double>(distance);
                return readX; 
            }
            else if (typeof(TOut) == typeof(SequenceItem))
            {
                var readX = reader.GetFieldReaderFor(binding).Read( out var sv);
                fixed (byte* svp = sv)
                {
                    storedValue = (TOut)(object)new SequenceItem(svp, sv.Length);
                }
                return readX;
            }
            else if (typeof(TOut) == typeof(NumericalItem<long>))
            {
                var readX = reader.GetFieldReaderFor(binding).Read<long>(out var value);
                storedValue = (TOut)(object)new NumericalItem<long>(value);
                return readX;
            }
            else if (typeof(TOut) == typeof(NumericalItem<double>))
            {
                var readX = reader.GetFieldReaderFor(binding).Read<double>(out var value);
                storedValue = (TOut)(object)new NumericalItem<double>(value);
                return readX;
            }
            
            Failed:
            Unsafe.SkipInit(out storedValue);
            return false;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void UnlikelyGrowBuffer(int currentlyUsed)
        {
            // Calculate the new size. 
            int size = (int)(currentlyUsed * (currentlyUsed > 16 * Voron.Global.Constants.Size.Megabyte ? 1.5 : 2));

            // Allocate the new buffer
            var bufferHandler = _searcher.Allocator.Allocate(size * sizeof(long), out var buffer);

            // In case there exist already buffers in place, we copy the content.
            if (_buffer != null)
            {
                // Ensure we copy the content and then switch the buffers. 
                new Span<long>(_buffer, currentlyUsed).CopyTo(new Span<long>(buffer.Ptr, size));
                _bufferHandler.Dispose();
            }

            _bufferSize = size;
            _buffer = (long*)buffer.Ptr;
            _bufferHandler = bufferHandler;
        }


        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FillNoPagingScore(ref SortingMatch<TInner, TComparer> match, Span<long> matches)
        {
            // Important: If you are going to request a massive take like 20K you need to pass at least a 20K size buffer to work with.
            //            The rationale for such behavior is that sorting has to find among the candidates the order between elements,
            //            and it can't do so without checking every single element found. If you fail to do so, your results may not be
            //            correct. 
            Debug.Assert(match._take <= matches.Length);

            int totalMatches = match._inner.Fill(matches);
            if (totalMatches <= 1)
            {
                // In those cases, no need to sort or anything. 
                return totalMatches;
            }

            int take = match._take <= 0 ? matches.Length : Math.Min(matches.Length, match._take);

            int arraySize = 4 * matches.Length;
            using var _ = match._searcher.Allocator.Allocate(arraySize * sizeof(float) + arraySize * sizeof(long), out var wholeBuffer);

            // We need to work with spans from now on, to avoid the creation of new arrays by slicing. 
            long* matchesSpanPtr = (long*)wholeBuffer.Ptr;
            var matchesSpan = new Span<long>(matchesSpanPtr, arraySize);
            float* scoresSpanPtr = (float*)(wholeBuffer.Ptr + arraySize * sizeof(long));
            var scoresSpan = new Span<float>(scoresSpanPtr, arraySize);

            // We will copy the first batch to a temporary location which we will use to work.
            // However, given that we already filled the scores, we are not going to copy those.
            matches.CopyTo(matchesSpan);

            var aScoresSpan = scoresSpan[0..totalMatches];
            aScoresSpan.Fill(1);
            match._inner.Score(matchesSpan[0..totalMatches], aScoresSpan);

            var sorter = new Sorter<float, long, NumericDescendingComparer>();

            bool isSorted = false;

            // We are going to slowly and painstakingly moving one batch after another to score
            // and select the appropriate boosted matches. For that we use a temporary buffer that
            // it has enough space for us to work with without the risk of overworking. 
            int temporaryTotalMatches;
            while (true)
            {
                // We get a new batch
                int bTotalMatches = match._inner.Fill(matchesSpan[totalMatches..]);
                temporaryTotalMatches = totalMatches + bTotalMatches;

                // When we don't have any new batch, we are done.
                if (bTotalMatches == 0)
                    break;

                isSorted = false;

                // We will score the new batch
                var bMatchesScores = scoresSpan[totalMatches..temporaryTotalMatches];
                bMatchesScores.Fill(1);
                match._inner.Score(matchesSpan[totalMatches..temporaryTotalMatches], bMatchesScores);

                if (temporaryTotalMatches > 3 * matches.Length)
                {
                    // We need to first sort by match to remove the duplicates.
                    temporaryTotalMatches = Sorting.SortAndRemoveDuplicates(matchesSpanPtr, scoresSpanPtr, temporaryTotalMatches);

                    // Then sort again to select the appropriate matches.
                    sorter.Sort(scoresSpan[0..temporaryTotalMatches], matchesSpan[0..temporaryTotalMatches]);
                    totalMatches = Math.Min(matches.Length, temporaryTotalMatches);
                    isSorted = true;
                }
                else
                {
                    totalMatches = temporaryTotalMatches;
                }
            }

            bool documentBoostIsAdded = false;
            if (isSorted == false)
            {
                AddDocumentBoost(ref match, matchesSpan, scoresSpan, temporaryTotalMatches);
                documentBoostIsAdded = true;
                
                // We need to first sort by match to remove the duplicates.
                temporaryTotalMatches = Sorting.SortAndRemoveDuplicates(matchesSpanPtr, scoresSpanPtr, temporaryTotalMatches);

                // Then sort again to select the appropriate matches.
                sorter.Sort(scoresSpan[0..temporaryTotalMatches], matchesSpan[0..temporaryTotalMatches]);
            }
            totalMatches = Math.Min(take, temporaryTotalMatches);

            if (documentBoostIsAdded == false)
            {
                AddDocumentBoost(ref match, matchesSpan, scoresSpan, totalMatches);
                sorter.Sort(scoresSpan[0..totalMatches], matchesSpan[0..totalMatches]);
            }

            // Copy must happen before we return the backing arrays.
            matchesSpan[..totalMatches].CopyTo(matches);
            match.TotalResults = totalMatches;

            return totalMatches;

            void AddDocumentBoost(ref SortingMatch<TInner, TComparer> match, Span<long> matchesSpan, Span<float> scoresSpan, int limit)
            {
                if (match._searcher.DocumentsAreBoosted == false) 
                    return;
                var tree = match._searcher.GetDocumentBoostTree();
                if (tree == null || tree.NumberOfEntries == 0)
                    return;
                
                for (int bIdx = 0; bIdx < limit; ++bIdx)
                {
                    using var __ = tree.Read(matchesSpan[bIdx], out var slice);
                    if (slice.HasValue == false)
                        continue;
                    var boostFactor = MemoryMarshal.Cast<byte, float>(slice.AsSpan());
                    scoresSpan[bIdx] *= boostFactor[0];
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ExecuteSorting<TOut>(ref SortingMatch<TInner, TComparer> match, Span<long> matches)
            where TOut : struct
        {
            // PERF: This method assumes that we need to perform sorting of the whole set (even if we have a take statement).
            // However, there is space to improve this further doing iterative sorting with element discarding in cases where
            // take statements requires us to remove lots of elements. While we are not going to perform this optimization yet
            // sorting is a costly primitive that if we can avoid it, would provide us a great opportunity for improvement. 

            int totalMatches = (int)match.TotalResults;

            int itemArraySize = Unsafe.SizeOf<MatchComparer<TComparer, TOut>.Item>() * totalMatches;
            using var _ = match._searcher.Allocator.Allocate(itemArraySize, out var bufferHolder);

            var itemKeys = MemoryMarshal.Cast<byte, MatchComparer<TComparer, TOut>.Item>(bufferHolder.ToSpan().Slice(0, itemArraySize));
            Debug.Assert(itemKeys.Length == totalMatches);

            var searcher = match._searcher;
            var field = match._comparer.Field;
            var comparer = new MatchComparer<TComparer, TOut>(match._comparer);
            for (int i = 0; i < totalMatches; i++)
            {
                var read = Get(searcher, field, matches[i], out itemKeys[i].Value, match._comparer);
                itemKeys[i].Key = read ? matches[i] : -matches[i];
            }

            // We sort the the set
            var sorter = new Sorter<MatchComparer<TComparer, TOut>.Item, long, MatchComparer<TComparer, TOut>>(comparer);
            sorter.Sort(itemKeys, matches[0..totalMatches]);

            // We have a take statement so we are only going to care about the highest priority elements. 
            if (match._take > 0)
                totalMatches = Math.Min(totalMatches, match._take);

            return totalMatches;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Fill<TOut>(ref SortingMatch<TInner, TComparer> match, Span<long> matches)
            where TOut : struct
        {
            // We will try to use the matches buffer for as long as we can, if it is not enough we will switch to a more complex 
            // behavior. This method should also be re-entrant for the case where we have already pre-sorted everything and 
            // we will just need to acquire via pages the totality of the results. 

            if (match._currentIdx != NotStarted)
                goto ReturnMatches;

            // If there is nothing or just 1 element to return, we are done as there is no need to sort anything.
            int totalMatches = match._inner.Fill(matches);
            if (totalMatches is 0 or 1)
            {
                match.TotalResults = totalMatches;
                match._currentIdx = totalMatches;
                return totalMatches;
            }

            // Here comes the most important part on why https://issues.hibernatingrhinos.com/issue/RavenDB-19718 
            // requires special handling of the situation of returning multiple pages. Sorting requires that we
            // guarantee a global ordering, which cannot be done unless we have the whole set available already.
            // Therefore, if the results are smaller than the buffer we were given to work with, everything
            // would be fine. However, there might exist the case where the total amount of hits is bigger than
            // the buffer we were given to work with. In those cases we have to recruit external memory to be able
            // to execute multiple `.Fill()` calls. 

            // To start with, we will try to get all the hits to sort and assume that the whole result-set will be
            // able to be stored in the matches buffer until we figure out that is not the case.

            bool isNotDone = true;
            if (totalMatches < matches.Length)
            {
                while (isNotDone)
                {
                    // We get a new batch
                    int bTotalMatches = match._inner.Fill(matches.Slice(totalMatches));
                    totalMatches += bTotalMatches;

                    // When we don't have any new batch, we are done.
                    if (bTotalMatches == 0)
                        isNotDone = false;

                    if (totalMatches < matches.Length / 8)
                        break; // We are not done, therefore we will go to get extra temporary buffers.
                }
            }

            if (isNotDone == false)
            {
                // If we are done (the expected outcome), we know that we can do this on a single call. Therefore,
                // we will sort and return the whole buffer.
                match.TotalResults = totalMatches;
                match._currentIdx = totalMatches;
                totalMatches = SortingMatch<TInner, TComparer>.ExecuteSorting<TOut>(ref match, matches);
                return totalMatches;
            }

            // However, it might happen that we are not actually done which means that we will need to recruit
            // an external buffer memory to temporarily store the sorted data and for that we need to estimate 
            // how much memory to recruit for doing so. 

            if (match._inner.Confidence >= QueryCountConfidence.Normal && match._inner.Count < Constants.Primitives.DefaultBufferSize)
            {
                // Since we expect to find less than the default buffer size, we just ask for that much. We divide by 2 because
                // the grow buffer function will adjust the used size to double the size. 
                match.UnlikelyGrowBuffer(Constants.Primitives.DefaultBufferSize / 2);
            }
            else
            {
                // Since confidence is not good recruiting four times the size of the current matches in external memory is a sensible
                // tradeoff, and we will just hit other grow sequences over time if needed. 
                match.UnlikelyGrowBuffer(Math.Max(128, 4 * matches.Length));
            }

            // Copy to the buffer the matches that we already have.
            matches[..totalMatches].CopyTo(new Span<long>(match._buffer, totalMatches));

            long* buffer = match._buffer;
            while (true)
            {
                // We will ensure we have enough space to fill matches.
                int excessSpace = match._bufferSize - totalMatches;
                if (excessSpace < 128)
                {
                    match.UnlikelyGrowBuffer(match._bufferSize);
                    excessSpace = match._bufferSize - totalMatches;
                }

                // We will get more batches until we are done getting matches.
                int bTotalMatches = match._inner.Fill(new Span<long>(buffer + totalMatches, excessSpace));
                if (bTotalMatches == 0)
                {
                    match.TotalResults = totalMatches;
                    totalMatches = SortingMatch<TInner, TComparer>.ExecuteSorting<TOut>(ref match, new Span<long>(match._buffer, totalMatches));

                    match._currentIdx = 0;
                    goto ReturnMatches;
                }

                totalMatches += bTotalMatches;
            }

            ReturnMatches:

            if (match._currentIdx < match.TotalResults)
            {
                Debug.Assert(match._currentIdx != NotStarted);

                // We will just copy as many already sorted elements into the output buffer.
                int leftovers = Math.Min((int)(match.TotalResults - match._currentIdx), matches.Length);
                new Span<long>(match._buffer + match._currentIdx, leftovers).CopyTo(matches);
                match._currentIdx += leftovers;
                return leftovers;
            }

            // There are no more matches to return.
            return 0;
        }


        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FillNoPaging<TOut>(ref SortingMatch<TInner, TComparer> match, Span<long> matches)
            where TOut : struct
        {
            // Important: If you are going to request a massive take like 20K you need to pass at least a 20K size buffer to work with.
            //            The rationale for such behavior is that sorting has to find among the candidates the order between elements,
            //            and it can't do so without checking every single element found. If you fail to do so, your results may not be
            //            correct. 
            Debug.Assert(match._take <= matches.Length);

            int totalMatches = match._inner.Fill(matches);
            if (totalMatches == 0)
                return 0;

            int matchesArraySize = sizeof(long) * matches.Length;
            int itemArraySize = 2 * Unsafe.SizeOf<MatchComparer<TComparer, TOut>.Item>() * matches.Length;
            using var _ = match._searcher.Allocator.Allocate(itemArraySize + matchesArraySize, out var bufferHolder);

            var itemKeys = MemoryMarshal.Cast<byte, MatchComparer<TComparer, TOut>.Item>(bufferHolder.ToSpan().Slice(0, itemArraySize));
            Debug.Assert(itemKeys.Length == 2 * matches.Length);

            // PERF: We want to avoid to share cache lines, that's why the second array will move toward the end of the array. 
            var matchesKeys = itemKeys.Slice(0, matches.Length);
            Debug.Assert(matchesKeys.Length == matches.Length);
            var bKeys = itemKeys.Slice(matches.Length, matches.Length);
            Debug.Assert(bKeys.Length == matches.Length);

            int take = match._take <= 0 ? matches.Length : Math.Min(matches.Length, match._take);

            match.TotalResults += totalMatches;

            var searcher = match._searcher;
            var field = match._comparer.Field;
            var comparer = new MatchComparer<TComparer, TOut>(match._comparer);
            for (int i = 0; i < totalMatches; i++)
            {
                var read = Get(searcher, field, matches[i], out matchesKeys[i].Value, match._comparer);
                matchesKeys[i].Key = read ? matches[i] : -matches[i];
            }

            // We sort the first batch
            var sorter = new Sorter<MatchComparer<TComparer, TOut>.Item, long, MatchComparer<TComparer, TOut>>(comparer);
            sorter.Sort(matchesKeys[0..totalMatches], matches);

            Span<long> bValues = MemoryMarshal.Cast<byte, long>(bufferHolder.ToSpan().Slice(itemArraySize, matchesArraySize));
            Debug.Assert(bValues.Length == matches.Length);
            while (true)
            {
                // We get a new batch
                int bTotalMatches = match._inner.Fill(bValues);
                match.TotalResults += bTotalMatches;

                // When we don't have any new batch, we are done.
                if (bTotalMatches == 0)
                {
                    return totalMatches;
                }

                // We get the keys to sort.
                for (int i = 0; i < bTotalMatches; i++)
                {
                    var read = Get(searcher, field, bValues[i], out bKeys[i].Value, match._comparer);
                    bKeys[i].Key = read ? bValues[i] : -bValues[i];
                }

                int bIdx = 0;
                int kIdx = 0;

                // Get rid of all the elements that are bigger than the last one.
                ref var lastElement = ref matchesKeys[take - 1];
                for (; bIdx < bTotalMatches; bIdx++)
                {
                    if (comparer.Compare(lastElement, bKeys[bIdx]) >= 0)
                        bKeys[kIdx++] = bKeys[bIdx];
                }
                bTotalMatches = kIdx;

                // We sort the new batch
                sorter.Sort(bKeys[0..bTotalMatches], bValues);                
                
                // We merge both batches. 
                int aTotalMatches = Math.Min(totalMatches, take);

                int aIdx = aTotalMatches;
                bIdx = 0;
                kIdx = 0;

                while (aIdx > 0 && aIdx >= aTotalMatches / 8)
                {
                    // If the 'bigger' of what we had is 'bigger than'
                    if (comparer.Compare(matchesKeys[aIdx-1], bKeys[0]) <= 0)
                        break;

                    aIdx /= 2;
                }

                // This is the new start location on the matches. 
                kIdx = aIdx; 

                // If we bailed on the first check, nothing to do here. 
                if (aIdx == aTotalMatches - 1 || kIdx >= take)
                    goto End;

                // PERF: This can be improved with TimSort like techniques (Galloping) but given the amount of registers and method calls
                //       involved requires careful timing to understand if we are able to gain vs a more compact code and predictable
                //       memory access patterns. 

                while (aIdx < aTotalMatches && bIdx < bTotalMatches && kIdx < take)
                {
                    var result = comparer.Compare(matchesKeys[aIdx], bKeys[bIdx]) < 0;

                    if (result)
                    {
                        matches[kIdx] = matchesKeys[aIdx].Key;
                        aIdx++;
                    }
                    else
                    {
                        matches[kIdx] = bKeys[bIdx].Key;
                        matchesKeys[kIdx] = bKeys[bIdx];
                        bIdx++;
                    }
                    kIdx++;
                }
                    
                // If there is no more space in the buffer, discard everything else.
                if (kIdx >= take)
                    goto End;

                // PERF: We could improve this with a CopyTo (won't do that for now). 

                // Copy the rest, given that we have failed on one of the other 2 only a single one will execute.
                while (aIdx < aTotalMatches && kIdx < take)
                {
                    matches[kIdx++] = matchesKeys[aIdx++].Key;
                }
                    
                while (bIdx < bTotalMatches && kIdx < take)
                {
                    matches[kIdx] = bKeys[bIdx].Key;
                    matchesKeys[kIdx] = bKeys[bIdx]; // We are using a new key, therefore we have to update it. 
                    kIdx++;
                    bIdx++;
                }
                    
                End:
                totalMatches = kIdx;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores) 
        {
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(SortingMatch)} [{typeof(TComparer).Name}]",
                children: new List<QueryInspectionNode> { _inner.Inspect()},
                parameters: new Dictionary<string, string>()
                {
                        { nameof(IsBoosting), IsBoosting.ToString() },
                });
        }

        string DebugView => Inspect().ToString();
    }
}
