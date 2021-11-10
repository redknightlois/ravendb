using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Utf8
{
    public static unsafe partial class UtfTranscoder
    {
        // Port from Lemire et al C++ version at: https://github.com/simdutf/simdutf/blob/master/src/westmere/sse_convert_utf16_to_utf8.cpp
        // Licensed under Apache License 2.0 as well as the MIT license.

        /*
            The vectorized algorithm works on single SSE register i.e., it
            loads eight 16-bit words.
            We consider three cases:
            1. an input register contains no surrogates and each value
               is in range 0x0000 .. 0x07ff.
            2. an input register contains no surrogates and values are
               is in range 0x0000 .. 0xffff.
            3. an input register contains surrogates --- i.e. codepoints
               can have 16 or 32 bits.
            Ad 1.
            When values are less than 0x0800, it means that a 16-bit words
            can be converted into: 1) single UTF8 byte (when it's an ASCII
            char) or 2) two UTF8 bytes.
            For this case we do only some shuffle to obtain these 2-byte
            codes and finally compress the whole SSE register with a single
            shuffle.
            We need 256-entry lookup table to get a compression pattern
            and the number of output bytes in the compressed vector register.
            Each entry occupies 17 bytes.
            Ad 2.
            When values fit in 16-bit words, but are above 0x07ff, then
            a single word may produce one, two or three UTF8 bytes.
            We prepare data for all these three cases in two registers.
            The first register contains lower two UTF8 bytes (used in all
            cases), while the second one contains just the third byte for
            the three-UTF8-bytes case.
            Finally these two registers are interleaved forming eight-element
            array of 32-bit values. The array spans two SSE registers.
            The bytes from the registers are compressed using two shuffles.
            We need 256-entry lookup table to get a compression pattern
            and the number of output bytes in the compressed vector register.
            Each entry occupies 17 bytes.
            To summarize:
            - We need two 256-entry tables that have 8704 bytes in total.
        */
        public static bool SseFromUtf16(ReadOnlySpan<char> source, ref Span<byte> dest)
        {
            int N = Vector128<ushort>.Count;

            var v_0000 = Vector128.Create((byte)0);
            var v_f800 = Vector128.Create((ushort)0xf800);
            var v_d800 = Vector128.Create((ushort)0xd800);
            var v_c080 = Vector128.Create((ushort)0xc080);

            int len = source.Length;

            var buf = (ushort*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(source));
            var utf8Output = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dest));

            var end = buf + source.Length;
            while (buf + 16 <= end)
            {
                var @in = Sse3.LoadDquVector128(buf);

                // a single 16-bit UTF-16 word can yield 1, 2 or 3 UTF-8 bytes
                var v_ff80 = Vector128.Create((ushort)0xff80);

                if (Sse41.TestZ(@in, v_ff80))
                {
                    // ASCII fast path!!!!
                    var nextin = Sse3.LoadDquVector128(buf + N);
                    if (!Sse41.TestZ(nextin, v_ff80))
                    {
                        // 1. pack the bytes
                        // obviously suboptimal.
                        var utf8Packed = Sse2.PackUnsignedSaturate(@in.AsInt16(), @in.AsInt16());
                        // 2. store (16 bytes)
                        Sse2.Store((byte*)utf8Output, utf8Packed);
                        // 3. adjust pointers
                        buf += 8;
                        utf8Output += 8;
                        @in = nextin;
                    }
                    else
                    {
                        // 1. pack the bytes
                        // obviously suboptimal.
                        var utf8Packed = Sse2.PackUnsignedSaturate(@in.AsInt16(), nextin.AsInt16());
                        // 2. store (16 bytes)
                        Sse2.Store(utf8Output, utf8Packed);
                        // 3. adjust pointers
                        buf += 16;
                        utf8Output += 16;
                        continue; // we are done for this round!
                    }
                }
                // no bits set above 7th bit
                var one_byte_bytemask = Sse2.CompareEqual(Sse2.And(@in, v_ff80).AsByte(), v_0000);
                ushort one_byte_bitmask = (ushort)Sse2.MoveMask(one_byte_bytemask);

                // no bits set above 11th bit
                var one_or_two_bytes_bytemask = Sse2.CompareEqual(Sse2.And(@in, v_f800).AsByte(), v_0000);
                ushort one_or_two_bytes_bitmask = (ushort)Sse2.MoveMask(one_or_two_bytes_bytemask);

                if (one_or_two_bytes_bitmask == 0xffff)
                {
                    // 1. prepare 2-byte values
                    // input 16-bit word : [0000|0aaa|aabb|bbbb] x 8
                    // expected output   : [110a|aaaa|10bb|bbbb] x 8
                    var v_1f00 = Vector128.Create((ushort)0x1f00);
                    var v_003f = Vector128.Create((ushort)0x003f);

                    // t0 = [000a|aaaa|bbbb|bb00]
                    var t0 = Sse2.ShiftLeftLogical(@in.AsInt16(), 2).AsUInt16();
                    // t1 = [000a|aaaa|0000|0000]
                    var t1 = Sse2.And(t0, v_1f00);
                    // t2 = [0000|0000|00bb|bbbb]
                    var t2 = Sse2.And(@in, v_003f);
                    // t3 = [000a|aaaa|00bb|bbbb]
                    var t3 = Sse2.Or(t1, t2);
                    // t4 = [110a|aaaa|10bb|bbbb]
                    var t4 = Sse2.Or(t3, v_c080);

                    // 2. merge ASCII and 2-byte codewords
                    var utf8_unpacked = Sse41.BlendVariable(t4.AsByte(), @in.AsByte(), one_byte_bytemask);

                    // 3. prepare bitmask for 8-bit lookup
                    //    one_byte_bitmask = hhggffeeddccbbaa -- the bits are doubled (h - MSB, a - LSB)
                    ushort m0 = (ushort)(one_byte_bitmask & 0x5555);    // m0 = 0h0g0f0e0d0c0b0a
                    ushort m1 = (ushort)(m0 >> 7);                      // m1 = 00000000h0g0f0e0
                    byte m2 = (byte)((m0 | m1) & 0xff);                 // m2 =         hdgcfbea
                    
                    // 4. pack the bytes
                    byte* row = (byte*)Unsafe.AsPointer(ref pack_1_2_utf8_bytes[17 * m2 + 0]);
                    var shuffle = Sse2.LoadVector128(row + 1);
                    var utf8_packed = Ssse3.Shuffle(utf8_unpacked, shuffle);

                    // 5. store bytes
                    Sse2.Store(utf8Output, utf8_packed);

                    // 6. adjust pointers
                    buf += 8;
                    utf8Output += row[0];
                    continue;
                }

                // 1. Check if there are any surrogate word in the input chunk.
                //    We have also deal with situation when there is a suggogate word
                //    at the end of a chunk.
                var surrogates_bytemask = Sse2.CompareEqual(Sse2.And(@in, v_f800).AsByte(), v_d800.AsByte());

                // bitmask = 0x0000 if there are no surrogates
                //         = 0xc000 if the last word is a surrogate
                var surrogates_bitmask = (ushort)Sse2.MoveMask(surrogates_bytemask);

                // It might seem like checking for surrogates_bitmask == 0xc000 could help. However,
                // it is likely an uncommon occurrence.
                if (surrogates_bitmask == 0x0000)
                {
                    // case: words from register produce either 1, 2 or 3 UTF-8 bytes
                    var dup_even = Vector128.Create((ushort)0x0000, 0x0202, 0x0404, 0x0606,
                                                    0x0808, 0x0a0a, 0x0c0c, 0x0e0e);

                    /* In this branch we handle three cases:
                       1. [0000|0000|0ccc|cccc] => [0ccc|cccc]                           - single UFT-8 byte
                       2. [0000|0bbb|bbcc|cccc] => [110b|bbbb], [10cc|cccc]              - two UTF-8 bytes
                       3. [aaaa|bbbb|bbcc|cccc] => [1110|aaaa], [10bb|bbbb], [10cc|cccc] - three UTF-8 bytes
                      We expand the input word (16-bit) into two words (32-bit), thus
                      we have room for four bytes. However, we need five distinct bit
                      layouts. Note that the last byte in cases #2 and #3 is the same.
                      We precompute byte 1 for case #1 and the common byte for cases #2 & #3
                      in register t2.
                      We precompute byte 1 for case #3 and -- **conditionally** -- precompute
                      either byte 1 for case #2 or byte 2 for case #3. Note that they
                      differ by exactly one bit.
                      Finally from these two words we build proper UTF-8 sequence, taking
                      into account the case (i.e, the number of bytes to write).
                    */
                    /**
                     * Given [aaaa|bbbb|bbcc|cccc] our goal is to produce:
                     * t2 => [0ccc|cccc] [10cc|cccc]
                     * s4 => [1110|aaaa] ([110b|bbbb] OR [10bb|bbbb])
                     */

                    // [aaaa|bbbb|bbcc|cccc] => [bbcc|cccc|bbcc|cccc]
                    var t0 = Ssse3.Shuffle(@in.AsByte(), dup_even.AsByte()).AsUInt16();
                    // [bbcc|cccc|bbcc|cccc] => [00cc|cccc|0bcc|cccc]
                    var t1 = Sse2.And(t0, Vector128.Create((ushort)0b0011111101111111));
                    // [00cc|cccc|0bcc|cccc] => [10cc|cccc|0bcc|cccc]
                    var t2 = Sse2.Or(t1, Vector128.Create((ushort)0b1000000000000000));

                    // [aaaa|bbbb|bbcc|cccc] =>  [0000|aaaa|bbbb|bbcc]
                    var s0 = Sse2.ShiftRightLogical(@in, 4);
                    // [0000|aaaa|bbbb|bbcc] => [0000|aaaa|bbbb|bb00]
                    var s1 = Sse2.And(s0, Vector128.Create((ushort)0b0000111111111100));
                    // [0000|aaaa|bbbb|bb00] => [00bb|bbbb|0000|aaaa]
                    var s2 = Sse2.MultiplyAddAdjacent(s1.AsInt16(), Vector128.Create((ushort)0x0140).AsInt16()).AsUInt16();
                    // [00bb|bbbb|0000|aaaa] => [11bb|bbbb|1110|aaaa]
                    var s3 = Sse2.Or(s2, Vector128.Create((ushort)0b1100000011100000));
                    var m0 = Sse2.AndNot(one_or_two_bytes_bytemask.AsUInt16(), Vector128.Create((ushort)0b0100000000000000));
                    var s4 = Sse2.Xor(s3, m0);


                    // 4. expand words 16-bit => 32-bit
                    var out0 = Sse2.UnpackLow(t2, s4);
                    var out1 = Sse2.UnpackHigh(t2, s4);

                    // 5. compress 32-bit words into 1, 2 or 3 bytes -- 2 x shuffle
                    var mask = (one_byte_bitmask & 0x5555) |
                                  (one_or_two_bytes_bitmask & 0xaaaa);

                    Vector128<byte> utf8_0;
                    Vector128<byte> utf8_1;

                    if (mask == 0)
                    {
                        // We only have three-byte words. Use fast path.
                        var shuffle = Vector128.Create(2, 3, 1, 6, 7, 5, 10, 11, 9, 14, 15, 13, -1, -1, -1, -1).AsByte();
                        utf8_0 = Ssse3.Shuffle(out0.AsByte(), shuffle);
                        utf8_1 = Ssse3.Shuffle(out1.AsByte(), shuffle);
                        Sse2.Store(utf8Output, utf8_0);
                        utf8Output += 12;
                        Sse2.Store(utf8Output, utf8_1);
                        utf8Output += 12;
                        buf += 8;
                        continue;
                    }

                    var mask0 = (byte)mask;
                    byte* row0 = (byte*)Unsafe.AsPointer(ref pack_1_2_utf8_bytes[17 * mask0 + 0]);
                    var shuffle0 = Sse2.LoadVector128(row0 + 1);
                    utf8_0 = Sse42.Shuffle(out0.AsByte(), shuffle0);

                    byte mask1 = (byte)(mask >> 8);
                    byte* row1 = (byte*)Unsafe.AsPointer(ref pack_1_2_utf8_bytes[17 * mask1 + 0]);
                    var shuffle1 = Sse2.LoadVector128(row1 + 1);
                    utf8_1 = Sse42.Shuffle(out1.AsByte(), shuffle1);

                    Sse2.Store(utf8Output, utf8_0);
                    utf8Output += row0[0];
                    Sse2.Store(utf8Output, utf8_1);
                    utf8Output += row1[0];

                    buf += 8;
                    // surrogate pair(s) in a register
                }
                else
                {
                    // Let us do a scalar fallback.
                    // It may seem wasteful to use scalar code, but being efficient with SIMD
                    // in the presence of surrogate pairs may require non-trivial tables.
                    int forward = 15;
                    int k = 0;
                    if (end - buf < forward + 1)
                    {
                        forward = (int)(end - buf - 1);
                    }

                    for (; k < forward; k++)
                    {
                        ushort word = buf[k];
                        if ((word & 0xFF80) == 0)
                        {
                            *utf8Output++ = (byte)word;
                        }
                        else if ((word & 0xF800) == 0)
                        {
                            *utf8Output++ = (byte)((word >> 6) | 0b11000000);
                            *utf8Output++ = (byte)((word & 0b111111) | 0b10000000);
                        }
                        else if ((word & 0xF800) != 0xD800)
                        {
                            *utf8Output++ = (byte)((word >> 12) | 0b11100000);
                            *utf8Output++ = (byte)(((word >> 6) & 0b111111) | 0b10000000);
                            *utf8Output++ = (byte)((word & 0b111111) | 0b10000000);
                        }
                        else
                        {
                            // must be a surrogate pair
                            ushort diff = (ushort)(word - 0xD800);
                            ushort next_word = buf[k + 1];
                            k++;

                            short diff2 = (short)(next_word - 0xDC00);
                            if ((diff | diff2) > 0x3FF)
                                goto FAIL;

                            uint value = (uint)((diff << 10) + diff2 + 0x10000);
                            *utf8Output++ = (byte)((value >> 18) | 0b11110000);
                            *utf8Output++ = (byte)(((value >> 12) & 0b111111) | 0b10000000);
                            *utf8Output++ = (byte)(((value >> 6) & 0b111111) | 0b10000000);
                            *utf8Output++ = (byte)((value & 0b111111) | 0b10000000);
                        }
                    }
                    buf += k;
                }
            }

            ushort* bufStart = (ushort*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(source));
            var start = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dest));

            int saved_bytes = (int)(utf8Output - start);
            if (buf != bufStart + len)
            {
                var destRemainder = dest[saved_bytes..];
                var sourceRemainder = source[(int)(buf - bufStart)..];
                if (!ScalarFromUtf16(sourceRemainder, ref destRemainder))
                    goto FAIL;
                saved_bytes += destRemainder.Length;
            }

            
            dest = dest.Slice(0, saved_bytes);
            return true;

            FAIL:
            dest = dest.Slice(0, 0);
            return false;
        }

        public static bool SseToUtf16(ReadOnlySpan<byte> source, ref Span<char> dest)
        {
            int len = source.Length;

            var data = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(source));
            var utf16output = (char*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dest));

            int pos = 0;
            while (pos < len)
            {
                // try to convert the next block of 16 ASCII bytes
                if (pos + 16 <= len) // if it is safe to read 16 more bytes, check that they are ascii
                {
                    ulong v1 = *(ulong*)(data + pos);
                    ulong v2 = *(ulong*)(data + pos + sizeof(ulong));
                    ulong v = v1 | v2;
                    if ((v & 0x8080_8080_8080_8080) == 0)
                    {
                        int finalPos = pos + 16;
                        while (pos < finalPos)
                        {
                            *utf16output++ = (char)source[pos];
                            pos++;
                        }
                        continue;
                    }
                }

                byte leading_byte = data[pos]; // leading byte
                if (leading_byte < 0b10000000)
                {
                    // converting one ASCII byte !!!
                    *utf16output++ = (char)leading_byte;
                    pos++;
                }
                else if ((leading_byte & 0b11100000) == 0b11000000)
                {
                    // We have a two-byte UTF-8, it should become
                    // a single UTF-16 word.
                    if (pos + 1 >= len)
                        goto FAIL; // minimal bound checking
                    if ((data[pos + 1] & 0b11000000) != 0b10000000)
                        goto FAIL;
                    // range check
                    int code_point = (leading_byte & 0b00011111) << 6 | (data[pos + 1] & 0b00111111);
                    if (code_point < 0x80 || 0x7ff < code_point)
                        goto FAIL;

                    *utf16output++ = (char)(code_point);
                    pos += 2;
                }
                else if ((leading_byte & 0b11110000) == 0b11100000)
                {
                    // We have a three-byte UTF-8, it should become
                    // a single UTF-16 word.
                    if (pos + 2 >= len)
                        goto FAIL; // minimal bound checking
                    if ((data[pos + 1] & 0b11000000) != 0b10000000)
                        goto FAIL;
                    if ((data[pos + 2] & 0b11000000) != 0b10000000)
                        goto FAIL;
                    // range check
                    int code_point = (leading_byte & 0b00001111) << 12 |
                                     (data[pos + 1] & 0b00111111) << 6 |
                                     (data[pos + 2] & 0b00111111);
                    if (code_point < 0x800 || 0xffff < code_point || (0xd7ff < code_point && code_point < 0xe000))
                        goto FAIL;

                    *utf16output++ = (char)(code_point);
                    pos += 3;
                }
                else if ((leading_byte & 0b11111000) == 0b11110000)
                {
                    // 0b11110000
                    // we have a 4-byte UTF-8 word.
                    if (pos + 3 >= len)
                        goto FAIL; // minimal bound checking
                    if ((data[pos + 1] & 0b11000000) != 0b10000000)
                        goto FAIL;
                    if ((data[pos + 2] & 0b11000000) != 0b10000000)
                        goto FAIL;
                    if ((data[pos + 3] & 0b11000000) != 0b10000000)
                        goto FAIL;

                    // range check
                    int code_point =
                        (leading_byte & 0b00000111) << 18 | (data[pos + 1] & 0b00111111) << 12 |
                        (data[pos + 2] & 0b00111111) << 6 | (data[pos + 3] & 0b00111111);
                    if (code_point <= 0xffff || 0x10ffff < code_point)
                        goto FAIL;

                    code_point -= 0x10000;
                    *utf16output++ = (char)(0xD800 + (code_point >> 10));
                    *utf16output++ = (char)(0xDC00 + (code_point & 0x3FF));
                    pos += 4;
                }
                else
                {
                    return false;
                }
            }

            var start = (char*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dest));
            dest = dest.Slice(0, (int)(uint)(utf16output - start));
            return true;

        FAIL:
            dest = dest.Slice(0, 0);
            return false;
        }


        private static readonly byte[] pack_1_2_utf8_bytes = 
        {
            16,1,0,3,2,5,4,7,6,9,8,11,10,13,12,15,14,
            15,0,3,2,5,4,7,6,9,8,11,10,13,12,15,14,0x80,
            15,1,0,3,2,5,4,7,6,8,11,10,13,12,15,14,0x80,
            14,0,3,2,5,4,7,6,8,11,10,13,12,15,14,0x80,0x80,
            15,1,0,2,5,4,7,6,9,8,11,10,13,12,15,14,0x80,
            14,0,2,5,4,7,6,9,8,11,10,13,12,15,14,0x80,0x80,
            14,1,0,2,5,4,7,6,8,11,10,13,12,15,14,0x80,0x80,
            13,0,2,5,4,7,6,8,11,10,13,12,15,14,0x80,0x80,0x80,
            15,1,0,3,2,5,4,7,6,9,8,10,13,12,15,14,0x80,
            14,0,3,2,5,4,7,6,9,8,10,13,12,15,14,0x80,0x80,
            14,1,0,3,2,5,4,7,6,8,10,13,12,15,14,0x80,0x80,
            13,0,3,2,5,4,7,6,8,10,13,12,15,14,0x80,0x80,0x80,
            14,1,0,2,5,4,7,6,9,8,10,13,12,15,14,0x80,0x80,
            13,0,2,5,4,7,6,9,8,10,13,12,15,14,0x80,0x80,0x80,
            13,1,0,2,5,4,7,6,8,10,13,12,15,14,0x80,0x80,0x80,
            12,0,2,5,4,7,6,8,10,13,12,15,14,0x80,0x80,0x80,0x80,
            15,1,0,3,2,4,7,6,9,8,11,10,13,12,15,14,0x80,
            14,0,3,2,4,7,6,9,8,11,10,13,12,15,14,0x80,0x80,
            14,1,0,3,2,4,7,6,8,11,10,13,12,15,14,0x80,0x80,
            13,0,3,2,4,7,6,8,11,10,13,12,15,14,0x80,0x80,0x80,
            14,1,0,2,4,7,6,9,8,11,10,13,12,15,14,0x80,0x80,
            13,0,2,4,7,6,9,8,11,10,13,12,15,14,0x80,0x80,0x80,
            13,1,0,2,4,7,6,8,11,10,13,12,15,14,0x80,0x80,0x80,
            12,0,2,4,7,6,8,11,10,13,12,15,14,0x80,0x80,0x80,0x80,
            14,1,0,3,2,4,7,6,9,8,10,13,12,15,14,0x80,0x80,
            13,0,3,2,4,7,6,9,8,10,13,12,15,14,0x80,0x80,0x80,
            13,1,0,3,2,4,7,6,8,10,13,12,15,14,0x80,0x80,0x80,
            12,0,3,2,4,7,6,8,10,13,12,15,14,0x80,0x80,0x80,0x80,
            13,1,0,2,4,7,6,9,8,10,13,12,15,14,0x80,0x80,0x80,
            12,0,2,4,7,6,9,8,10,13,12,15,14,0x80,0x80,0x80,0x80,
            12,1,0,2,4,7,6,8,10,13,12,15,14,0x80,0x80,0x80,0x80,
            11,0,2,4,7,6,8,10,13,12,15,14,0x80,0x80,0x80,0x80,0x80,
            15,1,0,3,2,5,4,7,6,9,8,11,10,12,15,14,0x80,
            14,0,3,2,5,4,7,6,9,8,11,10,12,15,14,0x80,0x80,
            14,1,0,3,2,5,4,7,6,8,11,10,12,15,14,0x80,0x80,
            13,0,3,2,5,4,7,6,8,11,10,12,15,14,0x80,0x80,0x80,
            14,1,0,2,5,4,7,6,9,8,11,10,12,15,14,0x80,0x80,
            13,0,2,5,4,7,6,9,8,11,10,12,15,14,0x80,0x80,0x80,
            13,1,0,2,5,4,7,6,8,11,10,12,15,14,0x80,0x80,0x80,
            12,0,2,5,4,7,6,8,11,10,12,15,14,0x80,0x80,0x80,0x80,
            14,1,0,3,2,5,4,7,6,9,8,10,12,15,14,0x80,0x80,
            13,0,3,2,5,4,7,6,9,8,10,12,15,14,0x80,0x80,0x80,
            13,1,0,3,2,5,4,7,6,8,10,12,15,14,0x80,0x80,0x80,
            12,0,3,2,5,4,7,6,8,10,12,15,14,0x80,0x80,0x80,0x80,
            13,1,0,2,5,4,7,6,9,8,10,12,15,14,0x80,0x80,0x80,
            12,0,2,5,4,7,6,9,8,10,12,15,14,0x80,0x80,0x80,0x80,
            12,1,0,2,5,4,7,6,8,10,12,15,14,0x80,0x80,0x80,0x80,
            11,0,2,5,4,7,6,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            14,1,0,3,2,4,7,6,9,8,11,10,12,15,14,0x80,0x80,
            13,0,3,2,4,7,6,9,8,11,10,12,15,14,0x80,0x80,0x80,
            13,1,0,3,2,4,7,6,8,11,10,12,15,14,0x80,0x80,0x80,
            12,0,3,2,4,7,6,8,11,10,12,15,14,0x80,0x80,0x80,0x80,
            13,1,0,2,4,7,6,9,8,11,10,12,15,14,0x80,0x80,0x80,
            12,0,2,4,7,6,9,8,11,10,12,15,14,0x80,0x80,0x80,0x80,
            12,1,0,2,4,7,6,8,11,10,12,15,14,0x80,0x80,0x80,0x80,
            11,0,2,4,7,6,8,11,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            13,1,0,3,2,4,7,6,9,8,10,12,15,14,0x80,0x80,0x80,
            12,0,3,2,4,7,6,9,8,10,12,15,14,0x80,0x80,0x80,0x80,
            12,1,0,3,2,4,7,6,8,10,12,15,14,0x80,0x80,0x80,0x80,
            11,0,3,2,4,7,6,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            12,1,0,2,4,7,6,9,8,10,12,15,14,0x80,0x80,0x80,0x80,
            11,0,2,4,7,6,9,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,4,7,6,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,4,7,6,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,0x80,
            15,1,0,3,2,5,4,6,9,8,11,10,13,12,15,14,0x80,
            14,0,3,2,5,4,6,9,8,11,10,13,12,15,14,0x80,0x80,
            14,1,0,3,2,5,4,6,8,11,10,13,12,15,14,0x80,0x80,
            13,0,3,2,5,4,6,8,11,10,13,12,15,14,0x80,0x80,0x80,
            14,1,0,2,5,4,6,9,8,11,10,13,12,15,14,0x80,0x80,
            13,0,2,5,4,6,9,8,11,10,13,12,15,14,0x80,0x80,0x80,
            13,1,0,2,5,4,6,8,11,10,13,12,15,14,0x80,0x80,0x80,
            12,0,2,5,4,6,8,11,10,13,12,15,14,0x80,0x80,0x80,0x80,
            14,1,0,3,2,5,4,6,9,8,10,13,12,15,14,0x80,0x80,
            13,0,3,2,5,4,6,9,8,10,13,12,15,14,0x80,0x80,0x80,
            13,1,0,3,2,5,4,6,8,10,13,12,15,14,0x80,0x80,0x80,
            12,0,3,2,5,4,6,8,10,13,12,15,14,0x80,0x80,0x80,0x80,
            13,1,0,2,5,4,6,9,8,10,13,12,15,14,0x80,0x80,0x80,
            12,0,2,5,4,6,9,8,10,13,12,15,14,0x80,0x80,0x80,0x80,
            12,1,0,2,5,4,6,8,10,13,12,15,14,0x80,0x80,0x80,0x80,
            11,0,2,5,4,6,8,10,13,12,15,14,0x80,0x80,0x80,0x80,0x80,
            14,1,0,3,2,4,6,9,8,11,10,13,12,15,14,0x80,0x80,
            13,0,3,2,4,6,9,8,11,10,13,12,15,14,0x80,0x80,0x80,
            13,1,0,3,2,4,6,8,11,10,13,12,15,14,0x80,0x80,0x80,
            12,0,3,2,4,6,8,11,10,13,12,15,14,0x80,0x80,0x80,0x80,
            13,1,0,2,4,6,9,8,11,10,13,12,15,14,0x80,0x80,0x80,
            12,0,2,4,6,9,8,11,10,13,12,15,14,0x80,0x80,0x80,0x80,
            12,1,0,2,4,6,8,11,10,13,12,15,14,0x80,0x80,0x80,0x80,
            11,0,2,4,6,8,11,10,13,12,15,14,0x80,0x80,0x80,0x80,0x80,
            13,1,0,3,2,4,6,9,8,10,13,12,15,14,0x80,0x80,0x80,
            12,0,3,2,4,6,9,8,10,13,12,15,14,0x80,0x80,0x80,0x80,
            12,1,0,3,2,4,6,8,10,13,12,15,14,0x80,0x80,0x80,0x80,
            11,0,3,2,4,6,8,10,13,12,15,14,0x80,0x80,0x80,0x80,0x80,
            12,1,0,2,4,6,9,8,10,13,12,15,14,0x80,0x80,0x80,0x80,
            11,0,2,4,6,9,8,10,13,12,15,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,4,6,8,10,13,12,15,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,4,6,8,10,13,12,15,14,0x80,0x80,0x80,0x80,0x80,0x80,
            14,1,0,3,2,5,4,6,9,8,11,10,12,15,14,0x80,0x80,
            13,0,3,2,5,4,6,9,8,11,10,12,15,14,0x80,0x80,0x80,
            13,1,0,3,2,5,4,6,8,11,10,12,15,14,0x80,0x80,0x80,
            12,0,3,2,5,4,6,8,11,10,12,15,14,0x80,0x80,0x80,0x80,
            13,1,0,2,5,4,6,9,8,11,10,12,15,14,0x80,0x80,0x80,
            12,0,2,5,4,6,9,8,11,10,12,15,14,0x80,0x80,0x80,0x80,
            12,1,0,2,5,4,6,8,11,10,12,15,14,0x80,0x80,0x80,0x80,
            11,0,2,5,4,6,8,11,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            13,1,0,3,2,5,4,6,9,8,10,12,15,14,0x80,0x80,0x80,
            12,0,3,2,5,4,6,9,8,10,12,15,14,0x80,0x80,0x80,0x80,
            12,1,0,3,2,5,4,6,8,10,12,15,14,0x80,0x80,0x80,0x80,
            11,0,3,2,5,4,6,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            12,1,0,2,5,4,6,9,8,10,12,15,14,0x80,0x80,0x80,0x80,
            11,0,2,5,4,6,9,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,5,4,6,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,5,4,6,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,0x80,
            13,1,0,3,2,4,6,9,8,11,10,12,15,14,0x80,0x80,0x80,
            12,0,3,2,4,6,9,8,11,10,12,15,14,0x80,0x80,0x80,0x80,
            12,1,0,3,2,4,6,8,11,10,12,15,14,0x80,0x80,0x80,0x80,
            11,0,3,2,4,6,8,11,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            12,1,0,2,4,6,9,8,11,10,12,15,14,0x80,0x80,0x80,0x80,
            11,0,2,4,6,9,8,11,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,4,6,8,11,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,4,6,8,11,10,12,15,14,0x80,0x80,0x80,0x80,0x80,0x80,
            12,1,0,3,2,4,6,9,8,10,12,15,14,0x80,0x80,0x80,0x80,
            11,0,3,2,4,6,9,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,3,2,4,6,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            10,0,3,2,4,6,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,4,6,9,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,4,6,9,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,0x80,
            10,1,0,2,4,6,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,0x80,
            9,0,2,4,6,8,10,12,15,14,0x80,0x80,0x80,0x80,0x80,0x80,0x80,
            15,1,0,3,2,5,4,7,6,9,8,11,10,13,12,14,0x80,
            14,0,3,2,5,4,7,6,9,8,11,10,13,12,14,0x80,0x80,
            14,1,0,3,2,5,4,7,6,8,11,10,13,12,14,0x80,0x80,
            13,0,3,2,5,4,7,6,8,11,10,13,12,14,0x80,0x80,0x80,
            14,1,0,2,5,4,7,6,9,8,11,10,13,12,14,0x80,0x80,
            13,0,2,5,4,7,6,9,8,11,10,13,12,14,0x80,0x80,0x80,
            13,1,0,2,5,4,7,6,8,11,10,13,12,14,0x80,0x80,0x80,
            12,0,2,5,4,7,6,8,11,10,13,12,14,0x80,0x80,0x80,0x80,
            14,1,0,3,2,5,4,7,6,9,8,10,13,12,14,0x80,0x80,
            13,0,3,2,5,4,7,6,9,8,10,13,12,14,0x80,0x80,0x80,
            13,1,0,3,2,5,4,7,6,8,10,13,12,14,0x80,0x80,0x80,
            12,0,3,2,5,4,7,6,8,10,13,12,14,0x80,0x80,0x80,0x80,
            13,1,0,2,5,4,7,6,9,8,10,13,12,14,0x80,0x80,0x80,
            12,0,2,5,4,7,6,9,8,10,13,12,14,0x80,0x80,0x80,0x80,
            12,1,0,2,5,4,7,6,8,10,13,12,14,0x80,0x80,0x80,0x80,
            11,0,2,5,4,7,6,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            14,1,0,3,2,4,7,6,9,8,11,10,13,12,14,0x80,0x80,
            13,0,3,2,4,7,6,9,8,11,10,13,12,14,0x80,0x80,0x80,
            13,1,0,3,2,4,7,6,8,11,10,13,12,14,0x80,0x80,0x80,
            12,0,3,2,4,7,6,8,11,10,13,12,14,0x80,0x80,0x80,0x80,
            13,1,0,2,4,7,6,9,8,11,10,13,12,14,0x80,0x80,0x80,
            12,0,2,4,7,6,9,8,11,10,13,12,14,0x80,0x80,0x80,0x80,
            12,1,0,2,4,7,6,8,11,10,13,12,14,0x80,0x80,0x80,0x80,
            11,0,2,4,7,6,8,11,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            13,1,0,3,2,4,7,6,9,8,10,13,12,14,0x80,0x80,0x80,
            12,0,3,2,4,7,6,9,8,10,13,12,14,0x80,0x80,0x80,0x80,
            12,1,0,3,2,4,7,6,8,10,13,12,14,0x80,0x80,0x80,0x80,
            11,0,3,2,4,7,6,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            12,1,0,2,4,7,6,9,8,10,13,12,14,0x80,0x80,0x80,0x80,
            11,0,2,4,7,6,9,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,4,7,6,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,4,7,6,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            14,1,0,3,2,5,4,7,6,9,8,11,10,12,14,0x80,0x80,
            13,0,3,2,5,4,7,6,9,8,11,10,12,14,0x80,0x80,0x80,
            13,1,0,3,2,5,4,7,6,8,11,10,12,14,0x80,0x80,0x80,
            12,0,3,2,5,4,7,6,8,11,10,12,14,0x80,0x80,0x80,0x80,
            13,1,0,2,5,4,7,6,9,8,11,10,12,14,0x80,0x80,0x80,
            12,0,2,5,4,7,6,9,8,11,10,12,14,0x80,0x80,0x80,0x80,
            12,1,0,2,5,4,7,6,8,11,10,12,14,0x80,0x80,0x80,0x80,
            11,0,2,5,4,7,6,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,
            13,1,0,3,2,5,4,7,6,9,8,10,12,14,0x80,0x80,0x80,
            12,0,3,2,5,4,7,6,9,8,10,12,14,0x80,0x80,0x80,0x80,
            12,1,0,3,2,5,4,7,6,8,10,12,14,0x80,0x80,0x80,0x80,
            11,0,3,2,5,4,7,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,
            12,1,0,2,5,4,7,6,9,8,10,12,14,0x80,0x80,0x80,0x80,
            11,0,2,5,4,7,6,9,8,10,12,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,5,4,7,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,5,4,7,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            13,1,0,3,2,4,7,6,9,8,11,10,12,14,0x80,0x80,0x80,
            12,0,3,2,4,7,6,9,8,11,10,12,14,0x80,0x80,0x80,0x80,
            12,1,0,3,2,4,7,6,8,11,10,12,14,0x80,0x80,0x80,0x80,
            11,0,3,2,4,7,6,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,
            12,1,0,2,4,7,6,9,8,11,10,12,14,0x80,0x80,0x80,0x80,
            11,0,2,4,7,6,9,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,4,7,6,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,4,7,6,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            12,1,0,3,2,4,7,6,9,8,10,12,14,0x80,0x80,0x80,0x80,
            11,0,3,2,4,7,6,9,8,10,12,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,3,2,4,7,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,3,2,4,7,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,4,7,6,9,8,10,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,4,7,6,9,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            10,1,0,2,4,7,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            9,0,2,4,7,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,0x80,
            14,1,0,3,2,5,4,6,9,8,11,10,13,12,14,0x80,0x80,
            13,0,3,2,5,4,6,9,8,11,10,13,12,14,0x80,0x80,0x80,
            13,1,0,3,2,5,4,6,8,11,10,13,12,14,0x80,0x80,0x80,
            12,0,3,2,5,4,6,8,11,10,13,12,14,0x80,0x80,0x80,0x80,
            13,1,0,2,5,4,6,9,8,11,10,13,12,14,0x80,0x80,0x80,
            12,0,2,5,4,6,9,8,11,10,13,12,14,0x80,0x80,0x80,0x80,
            12,1,0,2,5,4,6,8,11,10,13,12,14,0x80,0x80,0x80,0x80,
            11,0,2,5,4,6,8,11,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            13,1,0,3,2,5,4,6,9,8,10,13,12,14,0x80,0x80,0x80,
            12,0,3,2,5,4,6,9,8,10,13,12,14,0x80,0x80,0x80,0x80,
            12,1,0,3,2,5,4,6,8,10,13,12,14,0x80,0x80,0x80,0x80,
            11,0,3,2,5,4,6,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            12,1,0,2,5,4,6,9,8,10,13,12,14,0x80,0x80,0x80,0x80,
            11,0,2,5,4,6,9,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,5,4,6,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,5,4,6,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            13,1,0,3,2,4,6,9,8,11,10,13,12,14,0x80,0x80,0x80,
            12,0,3,2,4,6,9,8,11,10,13,12,14,0x80,0x80,0x80,0x80,
            12,1,0,3,2,4,6,8,11,10,13,12,14,0x80,0x80,0x80,0x80,
            11,0,3,2,4,6,8,11,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            12,1,0,2,4,6,9,8,11,10,13,12,14,0x80,0x80,0x80,0x80,
            11,0,2,4,6,9,8,11,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,4,6,8,11,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,4,6,8,11,10,13,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            12,1,0,3,2,4,6,9,8,10,13,12,14,0x80,0x80,0x80,0x80,
            11,0,3,2,4,6,9,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,3,2,4,6,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,3,2,4,6,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,4,6,9,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,4,6,9,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            10,1,0,2,4,6,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            9,0,2,4,6,8,10,13,12,14,0x80,0x80,0x80,0x80,0x80,0x80,0x80,
            13,1,0,3,2,5,4,6,9,8,11,10,12,14,0x80,0x80,0x80,
            12,0,3,2,5,4,6,9,8,11,10,12,14,0x80,0x80,0x80,0x80,
            12,1,0,3,2,5,4,6,8,11,10,12,14,0x80,0x80,0x80,0x80,
            11,0,3,2,5,4,6,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,
            12,1,0,2,5,4,6,9,8,11,10,12,14,0x80,0x80,0x80,0x80,
            11,0,2,5,4,6,9,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,5,4,6,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,5,4,6,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            12,1,0,3,2,5,4,6,9,8,10,12,14,0x80,0x80,0x80,0x80,
            11,0,3,2,5,4,6,9,8,10,12,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,3,2,5,4,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,3,2,5,4,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,5,4,6,9,8,10,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,5,4,6,9,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            10,1,0,2,5,4,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            9,0,2,5,4,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,0x80,
            12,1,0,3,2,4,6,9,8,11,10,12,14,0x80,0x80,0x80,0x80,
            11,0,3,2,4,6,9,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,
            11,1,0,3,2,4,6,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,3,2,4,6,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            11,1,0,2,4,6,9,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,2,4,6,9,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            10,1,0,2,4,6,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            9,0,2,4,6,8,11,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,0x80,
            11,1,0,3,2,4,6,9,8,10,12,14,0x80,0x80,0x80,0x80,0x80,
            10,0,3,2,4,6,9,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            10,1,0,3,2,4,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            9,0,3,2,4,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,0x80,
            10,1,0,2,4,6,9,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,
            9,0,2,4,6,9,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,0x80,
            9,1,0,2,4,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,0x80,
            8,0,2,4,6,8,10,12,14,0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x80
            };
    }
}
