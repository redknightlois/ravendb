using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Server;

namespace Corax.Queries
{
    public unsafe struct MultiTermMatch<TTermProvider> : IQueryMatch
        where TTermProvider : ITermProvider
    {
        internal long _totalResults;
        internal long _current;
        internal long _currentIdx;
        private QueryCountConfidence _confidence;
        
        internal TTermProvider _inner;
        private TermMatch _currentTerm;        
        private readonly ByteStringContext _context;
        private ByteString _cachedResult;

        public long Count => _totalResults;
        public long Current => _currentIdx <= QueryMatch.Start ? _currentIdx : _current;

        public QueryCountConfidence Confidence => _confidence;

        public MultiTermMatch(ByteStringContext context, TTermProvider inner, long totalResults = 0, QueryCountConfidence confidence = QueryCountConfidence.Low)
        {
            _inner = inner;
            _context = context;
            _cachedResult = new ByteString();
            _current = QueryMatch.Start;
            _currentIdx = QueryMatch.Start;
            _totalResults = totalResults;
            _confidence = confidence;

            _inner.Next(out _currentTerm);
        }

        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            if (_current == QueryMatch.Invalid)
                return 0;

            int count = 0;
            var bufferState = buffer;
            bool requiresSort = false;
            while (bufferState.Length > 0)
            {
                var read = _currentTerm.Fill(bufferState);
                if (read == 0)
                {
                    if (_inner.Next(out _currentTerm) == false)
                    {
                        _current = QueryMatch.Invalid;
                        goto End;
                    }

                    // We can prove that we need sorting and deduplication in the end. 
                    requiresSort |= count != buffer.Length;
                }

                count += read;
                bufferState = bufferState.Slice(read);
            }

            _current = count != 0 ? buffer[count - 1] : QueryMatch.Invalid;

        End:
            if (requiresSort && count > 1)
            {
                // We need to sort and remove duplicates.
                var bufferBasePtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buffer));

                MemoryExtensions.Sort(buffer.Slice(0, count));

                // We need to fill in the gaps left by removing deduplication process.
                // If there are no duplicated the writes at the architecture level will execute
                // way faster than if there are.
                
                var outputBufferPtr = bufferBasePtr;

                var bufferPtr = bufferBasePtr;
                var bufferEndPtr = bufferBasePtr + count - 1;                
                while (bufferPtr < bufferEndPtr)
                {
                    outputBufferPtr += bufferPtr[1] != bufferPtr[0] ? 1 : 0;
                    *outputBufferPtr = bufferPtr[1];
                    
                    bufferPtr++;
                }

                count = (int) (outputBufferPtr - bufferBasePtr + 1);
            }

            return count;
        }

        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer)
        {
            // We should consider the policy where it makes sense to actually implement something different to avoid
            // the N^2 check here. While could be interesting to do now we still dont know what are the conditions
            // that would trigger such optimization or if they are even necessary. The reason why is that buffer size
            // may offset some of the requirements for such a scan operation. Interesting approaches to consider include
            // evaluating directly, construct temporary data structures like bloom filters on subsequent iterations when
            // the statistics guarrant those approaches, etc. Currently we apply memoization but without any limit to 
            // size of the result and it's subsequent usage of memory. 

            var bufferHolder = QueryContext.MatchesPool.Rent(3 * sizeof(long) * buffer.Length);
            var longBuffer = MemoryMarshal.Cast<byte, long>(bufferHolder);

            // PERF: We want to avoid to share cache lines, that's why the third array will move toward the end of the array. 
            Span<long> results = longBuffer[0..buffer.Length];
            Span<long> tmp = longBuffer.Slice(buffer.Length, buffer.Length);
            Span<long> tmp2 = longBuffer[^buffer.Length..];

            _inner.Reset();
            
            int totalSize = 0;

            bool hasData = _inner.Next(out var current);
            long totalRead = current.Count;
            while (totalSize < buffer.Length && hasData)
            {                
                buffer.CopyTo(tmp);
                var read = current.AndWith(tmp);
                if (read == 0)
                    continue;

                results[0..totalSize].CopyTo(tmp2);
                totalSize = MergeHelper.Or(results, tmp2[0..totalSize], tmp[0..read]);

                hasData = _inner.Next(out current);
                totalRead += current.Count;
            }

            // We will check if we can make a better decision next time. 
            if (!hasData)
            {
                _totalResults = totalRead;
                _confidence = QueryCountConfidence.High;
            }
            
            results[0..totalSize].CopyTo(buffer);

            QueryContext.MatchesPool.Return(bufferHolder);
            return totalSize;
        }
    }
}
