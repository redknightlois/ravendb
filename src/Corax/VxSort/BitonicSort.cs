using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace VxSort
{
    static unsafe partial class BitonicSort
    {
        // * We might read the last 4 bytes into a 128-bit vector for 64-bit element masking
        // * We might read the last 8 bytes into a 128-bit vector for 32-bit element masking
        // This mostly applies to debug mode, since without optimizations, most compilers
        // actually execute the instruction stream _mm256_cvtepi8_epiNN + _mm_loadu_si128 as they are given.
        // In contract, release/optimizing compilers, turn that very specific instruction pair to
        // a more reasonable: vpmovsxbq ymm0, dword [rax*4 + mask_table_4], eliminating the 128-bit
        // load completely and effectively reading 4/8 (depending if the instruction is vpmovsxb[q,d]

        public static ReadOnlySpan<byte> mask_table_4 => new byte[]{
            0xFF, 0xFF, 0xFF, 0xFF,  // 0b0000 (0)
            0xFF, 0x00, 0x00, 0x00,  // 0b0001 (1)
            0xFF, 0xFF, 0x00, 0x00,  // 0b0011 (3)
            0xFF, 0xFF, 0xFF, 0x00,  // 0b0111 (7)
            0xCC, 0xCC, 0xCC, 0xCC,  // Garbage to make ASAN happy
            0xCC, 0xCC, 0xCC, 0xCC,  // Garbage to make ASAN happy
            0xCC, 0xCC, 0xCC, 0xCC,  // Garbage to make ASAN happy
        };

        public static ReadOnlySpan<byte> mask_table_8 => new byte[]{
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 0b00000000 (  0)
            0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 0b00000001 (  1)
            0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 0b00000011 (  3)
            0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, // 0b00000111 (  7)
            0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, // 0b00001111 ( 15)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, // 0b00011111 ( 31)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, // 0b00111111 ( 63)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, // 0b01111111 (127)
            0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, // Garbage to make ASAN happy
        };
    }
}
