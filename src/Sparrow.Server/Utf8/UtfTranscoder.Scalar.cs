using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Utf8
{
    public static unsafe partial class UtfTranscoder
    {
        public static bool ScalarFromUtf16(ReadOnlySpan<char> source, ref Span<byte> dest)
        {
            int len = source.Length;

            var data = (char*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(source));
            var utf8Output = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dest));

            int pos = 0;
            while (pos < len)
            {
                // try to convert the next block of 8 ASCII characters
                if (pos + 4 <= len)
                {
                    // if it is safe to read 8 more bytes, check that they are ascii
                    ulong v = *(ulong*)(data + pos);
                    if ((v & 0xFF80_FF80_FF80_FF80) == 0)
                    {
                        int finalPos = pos + 4;
                        while (pos < finalPos)
                        {
                            *utf8Output++ = (byte)source[pos];
                            pos++;
                        }
                        continue;
                    }
                }

                char word = data[pos];
                if ((word & 0xFF80) == 0)
                {
                    // will generate one UTF-8 bytes
                    *utf8Output++ = (byte)word;
                    pos++;
                }
                else if ((word & 0xF800) == 0)
                {
                    // will generate two UTF-8 bytes
                    // we have 0b110XXXXX 0b10XXXXXX
                    *utf8Output++ = (byte)((word >> 6) | 0b11000000);
                    *utf8Output++ = (byte)((word & 0b111111) | 0b10000000);
                    pos++;
                }
                else if ((word & 0xF800) != 0xD800)
                {
                    // will generate three UTF-8 bytes
                    // we have 0b1110XXXX 0b10XXXXXX 0b10XXXXXX
                    *utf8Output++ = (byte)((word >> 12) | 0b11100000);
                    *utf8Output++ = (byte)(((word >> 6) & 0b111111) | 0b10000000);
                    *utf8Output++ = (byte)((word & 0b111111) | 0b10000000);
                    pos++;
                }
                else
                {
                    // must be a surrogate pair
                    if (pos + 1 >= len)
                        goto FAIL;
                    char diff = (char)(word - 0xD800);
                    if (diff > 0x3FF)
                        goto FAIL;
                    char nextWord = data[pos + 1];
                    char diff2 = (char)(nextWord - 0xDC00);
                    if (diff2 > 0x3FF)
                        goto FAIL;
                    int value = (diff << 10) + diff2 + 0x10000;

                    // will generate four UTF-8 bytes
                    // we have 0b11110XXX 0b10XXXXXX 0b10XXXXXX 0b10XXXXXX
                    *utf8Output++ = (byte)((value >> 18) | 0b11110000);
                    *utf8Output++ = (byte)(((value >> 12) & 0b111111) | 0b10000000);
                    *utf8Output++ = (byte)(((value >> 6) & 0b111111) | 0b10000000);
                    *utf8Output++ = (byte)((value & 0b111111) | 0b10000000);
                    pos += 2;
                }
            }

            var start = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dest));
            dest = dest.Slice(0, (int)(utf8Output - start));
            return true;

        FAIL:
            dest = dest.Slice(0, 0);
            return false;
        }

        public static bool ScalarToUtf16(ReadOnlySpan<byte> source, ref Span<char> dest)
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
    }
}
