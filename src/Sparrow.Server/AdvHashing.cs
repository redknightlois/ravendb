using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using Vector256 = System.Runtime.Intrinsics.Vector256;

namespace Sparrow.Server
{
    public static class AdvHashing
    {
        [StructLayout(LayoutKind.Sequential)]
        public unsafe ref struct AvxHash3Processor
        {
            private static Vector256<ulong> Rotate256(Vector256<ulong> h)
            {
                var a = Vector256.Create(6, 5, 4, 3, 2, 1, 0, 7);
                return Vector256.Shuffle(h.AsInt32(), a).AsUInt64();
            }

            private static Vector256<ulong> Mix256(Vector256<ulong> h)
            {
                var m1 = Vector256.Create(0xcc9e2d51db873593, 0xcc9e2d51db873593, 0xcc9e2d51db873593, 0xcc9e2d51db873593);
                var a = Vector256.Xor(h, m1);
                var b = Rotate256(a);
                return Vector256.Multiply(a, b);
            }

            private static Vector256<ulong> Round256(Vector256<ulong> h, Vector256<ulong> x)
            {
                var a = Vector256.Xor(h, x);
                a = Mix256(a);

                return a;
            }

            private static Vector256<uint> MultiplyLow(Vector256<uint> a, Vector256<uint> b)
            {
                if (Avx2.IsSupported)
                {
                    return Avx2.MultiplyLow(a, b);
                }
                if (Sse42.IsSupported)
                {
                    var al = Sse41.MultiplyLow(a.GetLower(), b.GetLower());
                    var au = Sse41.MultiplyLow(a.GetUpper(), b.GetUpper());
                    return Vector256.Create(al, au);
                }
                if (AdvSimd.IsSupported)
                {
                    var al = AdvSimd.Multiply(a.GetLower().AsInt32(), b.GetLower().AsInt32());
                    var au = AdvSimd.Multiply(a.GetUpper().AsInt32(), b.GetUpper().AsInt32());
                    return Vector256.Create(al.AsUInt32(), au.AsUInt32());
                }
                
                // Fallback.
                return Vector256.Create(b[0] * a[0], b[1] * a[1], b[2] * a[2], b[3] * a[3], b[4] * a[4], b[5] * a[5], b[6] * a[6], b[7] * a[7]);
            }

            private static Vector256<ulong> FMix256(Vector256<ulong> h)
            {
                var m1 = Vector256.Create(0xcc9e2d51db873593, 0xcc9e2d51db873593, 0xcc9e2d51db873593, 0xcc9e2d51db873593).AsUInt32();

                var a = Vector256.Xor(h.AsUInt32(), Vector256.ShiftRightLogical(h.AsUInt32(), 16));
                a = MultiplyLow(a, m1.AsUInt32());
                a = Vector256.Xor(a, Vector256.ShiftRightLogical(a, 16));
                
                return a.AsUInt64();
            }

            private static Vector256<ulong> Combine256(Vector256<ulong> h, Vector256<ulong> x)
            {
                var m = Vector256.CreateScalar((uint)3);
                var a = MultiplyLow(h.AsUInt32(), m);
                return Vector256.Add(a.AsUInt64(), x);
            }

            private static ReadOnlySpan<byte> Padding => new byte[256]
            {
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,  // 16
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,  // 32
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,  // 64
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,  // 128
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80   // 256
            };

            private Vector256<ulong> X1;
            private Vector256<ulong> X2;
            private Vector256<ulong> X3;
            private Vector256<ulong> X4;
            private Vector256<ulong> X5;
            private Vector256<ulong> X6;
            private Vector256<ulong> X7;
            private Vector256<ulong> X8;

            private uint Count;
            private uint BlockSize;
            private ulong TotalLength;

            private fixed byte Buffer[256];

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public AvxHash3Processor(Vector256<ulong> seed)
            {
                X1 = Vector256.Xor(Vector256.Create(0xc05bbd6b47c57574ul, 0xf83c1e9e4a934534ul, 0xc05bbd6b47c57574ul, 0xf83c1e9e4a934534ul), seed);
                X2 = Vector256.Xor(Vector256.Create(0xa224e4507e645d91ul, 0xe7a9131a4b842813ul, 0xa224e4507e645d91ul, 0xe7a9131a4b842813ul), seed);
                X3 = Vector256.Xor(Vector256.Create(0x58f082e3580f347eul, 0xceb5795349196afful, 0x58f082e3580f347eul, 0xceb5795349196afful), seed);
                X4 = Vector256.Xor(Vector256.Create(0xb549200b5168588ful, 0xfd5c07200540ada5ul, 0xb549200b5168588ful, 0xfd5c07200540ada5ul), seed);
                X5 = Vector256.Xor(Vector256.Create(0x526b8e90108029d5ul, 0x46e634784c1350f0ul, 0x526b8e90108029d5ul, 0x46e634784c1350f0ul), seed);
                X6 = Vector256.Xor(Vector256.Create(0x4e9a8c21921fc259ul, 0x8f5d6730b44e65c3ul, 0x4e9a8c21921fc259ul, 0x8f5d6730b44e65c3ul), seed);
                X7 = Vector256.Xor(Vector256.Create(0xac0120817de73b56ul, 0xa5be99de5ff769ddul, 0xac0120817de73b56ul, 0xa5be99de5ff769ddul), seed);
                X8 = Vector256.Xor(Vector256.Create(0x7541811329b4609ful, 0x4c4cfa866064ec93ul, 0x7541811329b4609ful, 0x4c4cfa866064ec93ul), seed);

                BlockSize = 256;
                Count = 0;
                TotalLength = 0;
            }

            public static void Start(out AvxHash3Processor processor)
            {
                processor = new AvxHash3Processor(Vector256<ulong>.Zero);
            }

            public static void Start(out AvxHash3Processor processor, long seed)
            {
                processor = new AvxHash3Processor(Vector256.Create((ulong)seed));
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void ProcessInternal(ref byte data, int length)
            {
                var N = Vector256<byte>.Count;

                if (length == 0)
                    return;

                nuint len = (nuint)length;

                TotalLength += len;

                ref var dataStart = ref data;
                ref var dataCurrent = ref dataStart;

                if (Count + len < BlockSize)
                {
                    Unsafe.CopyBlockUnaligned(ref Buffer[Count], ref dataStart, (uint)len);
                    Count += (uint)len;
                    return;
                }

                var llen = len;

                if (Count > 0)
                {
                    var left = BlockSize - Count;
                    Unsafe.CopyBlockUnaligned(ref Buffer[Count], ref dataStart, (uint)left);
                    dataCurrent = ref Unsafe.AddByteOffset(ref dataCurrent, left);
                    llen -= left;
                }

                var x1 = X1;
                var x2 = X2;
                var x3 = X3;
                var x4 = X4;
                var x5 = X5;
                var x6 = X6;
                var x7 = X7;
                var x8 = X8;

                if (Count > 0)
                {
                    x1 = Round256(x1, Vector256.LoadUnsafe(ref Buffer[0 * N]).AsUInt64());
                    x2 = Round256(x2, Vector256.LoadUnsafe(ref Buffer[1 * N]).AsUInt64());
                    x3 = Round256(x3, Vector256.LoadUnsafe(ref Buffer[2 * N]).AsUInt64());
                    x4 = Round256(x4, Vector256.LoadUnsafe(ref Buffer[3 * N]).AsUInt64());
                    x5 = Round256(x5, Vector256.LoadUnsafe(ref Buffer[4 * N]).AsUInt64());
                    x6 = Round256(x6, Vector256.LoadUnsafe(ref Buffer[5 * N]).AsUInt64());
                    x7 = Round256(x7, Vector256.LoadUnsafe(ref Buffer[6 * N]).AsUInt64());
                    x8 = Round256(x8, Vector256.LoadUnsafe(ref Buffer[7 * N]).AsUInt64());

                    Count = 0;
                }

                while (llen >= BlockSize)
                {
                    x1 = Round256(x1, Vector256.LoadUnsafe(ref Unsafe.AddByteOffset(ref dataCurrent, 0 * N)).AsUInt64());
                    x2 = Round256(x2, Vector256.LoadUnsafe(ref Unsafe.AddByteOffset(ref dataCurrent, 1 * N)).AsUInt64());
                    x3 = Round256(x3, Vector256.LoadUnsafe(ref Unsafe.AddByteOffset(ref dataCurrent, 2 * N)).AsUInt64());
                    x4 = Round256(x4, Vector256.LoadUnsafe(ref Unsafe.AddByteOffset(ref dataCurrent, 3 * N)).AsUInt64());
                    x5 = Round256(x5, Vector256.LoadUnsafe(ref Unsafe.AddByteOffset(ref dataCurrent, 4 * N)).AsUInt64());
                    x6 = Round256(x6, Vector256.LoadUnsafe(ref Unsafe.AddByteOffset(ref dataCurrent, 5 * N)).AsUInt64());
                    x7 = Round256(x7, Vector256.LoadUnsafe(ref Unsafe.AddByteOffset(ref dataCurrent, 6 * N)).AsUInt64());
                    x8 = Round256(x8, Vector256.LoadUnsafe(ref Unsafe.AddByteOffset(ref dataCurrent, 7 * N)).AsUInt64());

                    dataCurrent = ref Unsafe.AddByteOffset(ref dataCurrent, BlockSize);
                    llen -= BlockSize;
                }

                X1 = x1;
                X2 = x2;
                X3 = x3;
                X4 = x4;
                X5 = x5;
                X6 = x6;
                X7 = x7;
                X8 = x8;

                if (llen > 0)
                {
                    Unsafe.CopyBlockUnaligned(ref Buffer[Count], ref dataCurrent, (uint)llen);
                    Count = (uint)llen;
                }
            }

            public void Process(ReadOnlySpan<byte> data)
            {
                ProcessInternal(ref MemoryMarshal.GetReference(data), data.Length);
            }

            public Vector256<ulong> End()
            {
                var len = Count < 248 ? 248 - Count : 504 - Count;
                ProcessInternal(ref MemoryMarshal.GetReference(Padding), (int)len);
                ProcessInternal(ref Unsafe.As<uint, byte>(ref Unsafe.AsRef(len)), 8);

                var x1 = X1;
                var x2 = X2;
                var x3 = X3;
                var x4 = X4;
                var x5 = X5;
                var x6 = X6;
                var x7 = X7;
                var x8 = X8;

                for (int i = 0; i < 4; i++)
                {
                    x1 = Mix256(x1);
                    x2 = Mix256(x2);
                    x3 = Mix256(x3);
                    x4 = Mix256(x4);
                    x5 = Mix256(x5);
                    x6 = Mix256(x6);
                    x7 = Mix256(x7);
                    x8 = Mix256(x8);
                }

                x1 = FMix256(x1);
                x2 = FMix256(x2);
                x3 = FMix256(x3);
                x4 = FMix256(x4);
                x5 = FMix256(x5);
                x6 = FMix256(x6);
                x7 = FMix256(x7);
                x8 = FMix256(x8);

                var h = Combine256(x1, x2);
                h = Combine256(h, x3);
                h = Combine256(h, x4);
                h = Combine256(h, x5);
                h = Combine256(h, x6);
                h = Combine256(h, x7);
                h = Combine256(h, x8);

                return h;
            }
        }
    }
}
