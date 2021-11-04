﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Pipeline
{
    public struct LowerCaseTransformer : ITransformer
    {
        public void Dispose() { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Transform(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            for (int i = 0; i < source.Length; i++)
            {
                byte value = source[i];
                if (value >= 'A' && value <= 'Z')
                {
                    dest[i] = (byte)(value + ('a' - 'A'));
                }             
            }

            // TODO: If they are the same input and output, dont do this. 
            tokens.CopyTo(destTokens);


            // We need to shrink the tokens and bytes output. 
            destTokens = destTokens.Slice(0, tokens.Length);
            dest = dest.Slice(0, source.Length);

            return source.Length;
        }
    }
}
