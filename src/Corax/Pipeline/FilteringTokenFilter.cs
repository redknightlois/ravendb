﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Corax.Pipeline
{
    public interface ITokenFilter
    {
        bool Accept(ReadOnlySpan<byte> source, in Token token);
    }

    public struct FilteringTokenFilter<TFilter> : IFilter
        where TFilter : struct, ITokenFilter
    {
        private readonly TFilter _filter;

        public FilteringTokenFilter(TFilter filter)
        {
            _filter = filter;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Filter(ReadOnlySpan<byte> source, ref Span<Token> tokens)
        {
            int storeIdx = 0;
            for(int i = 0; i< tokens.Length; i++ )
            {
                ref var token = ref tokens[i];

                if (_filter.Accept(source.Slice(token.Offset, (int)token.Length), token))
                {
                    tokens[storeIdx] = token;
                    storeIdx++;
                }                
            }
            
            tokens = tokens.Slice(0, storeIdx);
            return source.Length;
        }

        public void Dispose() {}
    }
}
