using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text;
using FastTests.Voron.FixedSize;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class HashingTests : NoDisposalNeeded
    {
        public HashingTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void XXHash64_UseActualValues()
        {
            var r1 = Hashing.XXHash64.CalculateRaw("Public");
            var r2 = Hashing.XXHash64.CalculateRaw(new string("Public".ToCharArray()));

            Assert.Equal(r1, r2);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void XXHash32_UseActualValues()
        {
            var r1 = Hashing.XXHash32.CalculateRaw("Public");
            var r2 = Hashing.XXHash32.CalculateRaw(new string("Public".ToCharArray()));
            var r3 = Hashing.XXHash32.Calculate("Public");

            Assert.Equal(r1, r2);
            Assert.Equal(r2, r3);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Marvin32_UseActualValues()
        {
            byte[] value = { (byte)'A', 0, (byte)'b', 0, (byte)'c', 0, (byte)'d', 0, (byte)'e', 0, (byte)'f', 0, (byte)'g', 0, }; /* "Abcdefg" in UTF-16-LE */

            var r1 = Hashing.Marvin32.Calculate(value);
            var r2 = Hashing.Marvin32.Calculate("Abcdefg");

            Assert.Equal(r1, r2);
        }


        [RavenFact(RavenTestCategory.Core)]
        public void XXHash32_UseLongActualValues()
        {
            var r1 = Hashing.XXHash32.CalculateRaw("PublicPublicPublicPublic");
            var r2 = Hashing.XXHash32.CalculateRaw(new string("PublicPublicPublicPublic".ToCharArray()));
            var r3 = Hashing.XXHash32.Calculate("PublicPublicPublicPublic");

            Assert.Equal(r1, r2);
            Assert.Equal(r2, r3);
        }


        [RavenFact(RavenTestCategory.Core)]
        public unsafe void XXHash32_EqualityImplementationPointerAndSpan()
        {
            var rng = new Random();
            for (int it = 0; it < 1000; it++)
            {
                byte[] values = new byte[it];
                rng.NextBytes(values);

                for (int i = 0; i < 100; i++)
                {
                    fixed (byte* ptr = values)
                    {
                        uint h1 = Hashing.XXHash32.CalculateInline(values.AsSpan());
                        uint h2 = Hashing.XXHash32.CalculateInline(ptr, values.Length);

                        Assert.Equal(h1, h2);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void XXHash32()
        {
            string value = "abcd";
            uint result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 0);
            uint expected = 0xA3643705;
            Assert.Equal(expected, result);

            value = "abc";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 0);
            expected = 0x32D153FF;
            Assert.Equal(expected, result);

            value = "heiå";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 0);
            expected = 0xDB5ABCCC;
            Assert.Equal(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 0);
            expected = 0xD855F606;
            Assert.Equal(expected, result);

            value = "asdfasdfasdfasdfasdfasdfasdfuqrewpuqpuqruepoiqiwoerpuqowieruqwlekjrqwernq/wemnrq.,wemrn";
            var bytes = Encoding.UTF8.GetBytes(value);
            result = Hashing.XXHash32.Calculate(bytes);
            expected = 3571373779;
            Assert.Equal(expected, result);
        }


        [RavenFact(RavenTestCategory.Core)]
        public void Marvin32()
        {
            byte[] test = { (byte)'A', 0, (byte)'b', 0, (byte)'c', 0, (byte)'d', 0, (byte)'e', 0, (byte)'f', 0, (byte)'g', 0, }; /* "Abcdefg" in UTF-16-LE */
            uint r = Hashing.Marvin32.Calculate(test);
            Assert.Equal(r, 0xba627c81);
        }

        [RavenFact(RavenTestCategory.Core)]
        public unsafe void Marvin32_IntArrayEquivalence()
        {
            int[] test = { 32, 5, 11588, 5 }; /* "Abcdefg" in UTF-16-LE */
            
            fixed (int* ptr = test)
            {
                uint r = Hashing.Marvin32.CalculateInline(test);
                uint x = Hashing.Marvin32.CalculateInline((byte*)ptr, test.Length * sizeof(int));

                Assert.Equal(r, x);
            }                        
        }


        [RavenFact(RavenTestCategory.Core)]
        public void XXHash32_EquivalenceInDifferentMemoryLocations()
        {
            string value = "abcd";
            uint result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            uint expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);

            value = "abc";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void XXHash64_EquivalenceInDifferentMemoryLocationsXXHash64()
        {
            string value = "abcd";
            ulong result = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            ulong expected = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);

            value = "abc";
            result = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            expected = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            expected = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void XXHash32_NotEquivalenceOfBytesWithString()
        {
            string value = "abcd";
            uint result = Hashing.XXHash32.CalculateRaw(value, seed: 10);
            uint expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);

            value = "abc";
            result = Hashing.XXHash32.CalculateRaw(value, seed: 10);
            expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash32.CalculateRaw(value, seed: 10);
            expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);
        }


        [RavenFact(RavenTestCategory.Core)]
        public void XXHash64_NotEquivalenceOfBytesWithString()
        {
            string value = "abcd";
            ulong result = Hashing.XXHash64.CalculateRaw(value, seed: 10);
            ulong expected = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);

            value = "abc";
            result = Hashing.XXHash64.CalculateRaw(value, seed: 10);
            expected = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash64.CalculateRaw(value, seed: 10);
            expected = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);
        }

        [RavenFact(RavenTestCategory.Core)]
        public unsafe void EnsureZeroLengthStringIsAValidHash()
        {
            byte[] zeroLength = Array.Empty<byte>();
            byte[] nonZeroLength = "abcd"u8.ToArray();

            fixed (byte* zeroPtr = zeroLength)
            fixed (byte* nonZeroPtr = nonZeroLength)
            {
                uint zeroHash = Hashing.XXHash32.Calculate(zeroLength);
                Assert.Equal(zeroHash, Hashing.XXHash32.Calculate(zeroPtr, 0));
                Assert.Equal(zeroHash, Hashing.XXHash32.Calculate(nonZeroLength, 0));
                Assert.Equal(zeroHash, Hashing.XXHash32.Calculate(nonZeroPtr, 0));

                ulong zeroHashLong = Hashing.XXHash64.Calculate(zeroLength);
                Assert.Equal(zeroHashLong, Hashing.XXHash64.Calculate(zeroPtr, 0));
                Assert.Equal(zeroHashLong, Hashing.XXHash64.Calculate(nonZeroLength, 0));
                Assert.Equal(zeroHashLong, Hashing.XXHash64.Calculate(nonZeroPtr, 0));

                var marvinHash = Hashing.Marvin32.Calculate(zeroLength);
                Assert.Equal(marvinHash, Hashing.Marvin32.CalculateInline(zeroPtr, 0));
                Assert.Equal(marvinHash, Hashing.Marvin32.CalculateInline(nonZeroPtr, 0));
                Assert.Equal(marvinHash, Hashing.Marvin32.CalculateInline(new List<int>()));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Combine()
        {
            int h1 = Hashing.HashCombiner.CombineInline(1991, 13);
            int h2 = Hashing.HashCombiner.CombineInline(1991, 12);
            Assert.NotEqual(h1, h2);
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public unsafe void XXHash64_StreamedHashingEquivalence(int randomSeed)
        {
            var rnd = new Random(randomSeed);
            var bufferSize = rnd.Next(1, 1000);
            var seed = (ulong)rnd.Next();

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            int blockSize;
            int iteration = 1;
            do
            {
                blockSize = Hashing.Streamed.XXHash64.Alignment * iteration;

                var context = new Hashing.Streamed.XXHash64Context { Seed = seed };
                Hashing.Streamed.XXHash64.Begin(ref context);
                fixed (byte* buffer = values)
                {
                    byte* current = buffer;
                    byte* bEnd = buffer + bufferSize;
                    do
                    {
                        int block = Math.Min(blockSize, (int)(bEnd - current));
                        Hashing.Streamed.XXHash64.Process(ref context, current, block);
                        current += block;
                    }
                    while (current < bEnd);
                }

                iteration++;

                var result = Hashing.Streamed.XXHash64.End(ref context);
                var expected = Hashing.XXHash64.Calculate(values, -1, seed);

                Assert.Equal(expected, result);
            }
            while (blockSize <= bufferSize);
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public unsafe void XXHash64_HashingEquivalenceWithReference(int randomSeed)
        {
            var rnd = new Random(randomSeed);
            var bufferSize = rnd.Next(1, 1000);
            var seed = (ulong)rnd.Next();

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            fixed (byte* valuePtr = values)
            {
                for (int i = 1; i < values.Length; i++)
                {
                    var expected = XXHash64Reference(valuePtr, (ulong)i, seed: seed);
                    var result = Hashing.XXHash64.CalculateInline(values.AsSpan().Slice(0, i), seed);

                    Assert.Equal(expected, result);
                }
            }
        }

        private static unsafe ulong XXHash64Reference(byte* buffer, ulong len, ulong seed = 0)
        {
            ulong h64;

            byte* bEnd = buffer + len;

            if (len >= 32)
            {
                byte* limit = bEnd - 32;

                ulong v1 = seed + Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_2;
                ulong v2 = seed + Hashing.XXHash64Constants.PRIME64_2;
                ulong v3 = seed + 0;
                ulong v4 = seed - Hashing.XXHash64Constants.PRIME64_1;

                do
                {
                    v1 += ((ulong*)buffer)[0] * Hashing.XXHash64Constants.PRIME64_2;
                    v2 += ((ulong*)buffer)[1] * Hashing.XXHash64Constants.PRIME64_2;
                    v3 += ((ulong*)buffer)[2] * Hashing.XXHash64Constants.PRIME64_2;
                    v4 += ((ulong*)buffer)[3] * Hashing.XXHash64Constants.PRIME64_2;

                    buffer += 4 * sizeof(ulong);

                    v1 = Bits.RotateLeft64(v1, 31);
                    v2 = Bits.RotateLeft64(v2, 31);
                    v3 = Bits.RotateLeft64(v3, 31);
                    v4 = Bits.RotateLeft64(v4, 31);

                    v1 *= Hashing.XXHash64Constants.PRIME64_1;
                    v2 *= Hashing.XXHash64Constants.PRIME64_1;
                    v3 *= Hashing.XXHash64Constants.PRIME64_1;
                    v4 *= Hashing.XXHash64Constants.PRIME64_1;
                }
                while (buffer <= limit);

                h64 = Bits.RotateLeft64(v1, 1) + Bits.RotateLeft64(v2, 7) + Bits.RotateLeft64(v3, 12) + Bits.RotateLeft64(v4, 18);

                v1 *= Hashing.XXHash64Constants.PRIME64_2;
                v2 *= Hashing.XXHash64Constants.PRIME64_2;
                v3 *= Hashing.XXHash64Constants.PRIME64_2;
                v4 *= Hashing.XXHash64Constants.PRIME64_2;

                v1 = Bits.RotateLeft64(v1, 31);
                v2 = Bits.RotateLeft64(v2, 31);
                v3 = Bits.RotateLeft64(v3, 31);
                v4 = Bits.RotateLeft64(v4, 31);

                v1 *= Hashing.XXHash64Constants.PRIME64_1;
                v2 *= Hashing.XXHash64Constants.PRIME64_1;
                v3 *= Hashing.XXHash64Constants.PRIME64_1;
                v4 *= Hashing.XXHash64Constants.PRIME64_1;

                h64 ^= v1;
                h64 = h64 * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;

                h64 ^= v2;
                h64 = h64 * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;

                h64 ^= v3;
                h64 = h64 * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;

                h64 ^= v4;
                h64 = h64 * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;
            }
            else
            {
                h64 = seed + Hashing.XXHash64Constants.PRIME64_5;
            }

            h64 += (ulong)len;


            while (buffer + 8 <= bEnd)
            {
                ulong k1 = *((ulong*)buffer);
                k1 *= Hashing.XXHash64Constants.PRIME64_2;
                k1 = Bits.RotateLeft64(k1, 31);
                k1 *= Hashing.XXHash64Constants.PRIME64_1;
                h64 ^= k1;
                h64 = Bits.RotateLeft64(h64, 27) * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;
                buffer += 8;
            }

            if (buffer + 4 <= bEnd)
            {
                h64 ^= *(uint*)buffer * Hashing.XXHash64Constants.PRIME64_1;
                h64 = Bits.RotateLeft64(h64, 23) * Hashing.XXHash64Constants.PRIME64_2 + Hashing.XXHash64Constants.PRIME64_3;
                buffer += 4;
            }

            while (buffer < bEnd)
            {
                h64 ^= ((ulong)*buffer) * Hashing.XXHash64Constants.PRIME64_5;
                h64 = Bits.RotateLeft64(h64, 11) * Hashing.XXHash64Constants.PRIME64_1;
                buffer++;
            }

            h64 ^= h64 >> 33;
            h64 *= Hashing.XXHash64Constants.PRIME64_2;
            h64 ^= h64 >> 29;
            h64 *= Hashing.XXHash64Constants.PRIME64_3;
            h64 ^= h64 >> 32;

            return h64;
        }


        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public unsafe void XXHash32_HashingEquivalenceWithReference(int randomSeed)
        {
            var rnd = new Random(randomSeed);
            var bufferSize = rnd.Next(1, 1000);
            var seed = (uint)rnd.Next();

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            fixed (byte* valuePtr = values)
            {
                for (int i = 1; i < values.Length; i++)
                {
                    var expected = XXHash32Reference(valuePtr, i, seed: seed);
                    var result = Hashing.XXHash32.CalculateInline(values.AsSpan().Slice(0, i), seed);

                    Assert.Equal(expected, result);
                }
            }
        }

        private static unsafe uint XXHash32Reference(byte* buffer, int len, uint seed = 0)
        {
            unchecked
            {
                uint h32;

                byte* bEnd = buffer + len;

                if (len >= 16)
                {
                    byte* limit = bEnd - 16;

                    uint v1 = seed + Hashing.XXHash32Constants.PRIME32_1 + Hashing.XXHash32Constants.PRIME32_2;
                    uint v2 = seed + Hashing.XXHash32Constants.PRIME32_2;
                    uint v3 = seed + 0;
                    uint v4 = seed - Hashing.XXHash32Constants.PRIME32_1;

                    do
                    {
                        v1 += ((uint*)buffer)[0] * Hashing.XXHash32Constants.PRIME32_2;
                        v2 += ((uint*)buffer)[1] * Hashing.XXHash32Constants.PRIME32_2;
                        v3 += ((uint*)buffer)[2] * Hashing.XXHash32Constants.PRIME32_2;
                        v4 += ((uint*)buffer)[3] * Hashing.XXHash32Constants.PRIME32_2;

                        buffer += 4 * sizeof(uint);

                        v1 = Bits.RotateLeft32(v1, 13);
                        v2 = Bits.RotateLeft32(v2, 13);
                        v3 = Bits.RotateLeft32(v3, 13);
                        v4 = Bits.RotateLeft32(v4, 13);

                        v1 *= Hashing.XXHash32Constants.PRIME32_1;
                        v2 *= Hashing.XXHash32Constants.PRIME32_1;
                        v3 *= Hashing.XXHash32Constants.PRIME32_1;
                        v4 *= Hashing.XXHash32Constants.PRIME32_1;
                    }
                    while (buffer <= limit);

                    h32 = Bits.RotateLeft32(v1, 1) + Bits.RotateLeft32(v2, 7) + Bits.RotateLeft32(v3, 12) + Bits.RotateLeft32(v4, 18);
                }
                else
                {
                    h32 = seed + Hashing.XXHash32Constants.PRIME32_5;
                }

                h32 += (uint)len;

                while (buffer + 4 <= bEnd)
                {
                    h32 += *((uint*)buffer) * Hashing.XXHash32Constants.PRIME32_3;
                    h32 = Bits.RotateLeft32(h32, 17) * Hashing.XXHash32Constants.PRIME32_4;
                    buffer += 4;
                }

                while (buffer < bEnd)
                {
                    h32 += (uint)(*buffer) * Hashing.XXHash32Constants.PRIME32_5;
                    h32 = Bits.RotateLeft32(h32, 11) * Hashing.XXHash32Constants.PRIME32_1;
                    buffer++;
                }

                h32 ^= h32 >> 15;
                h32 *= Hashing.XXHash32Constants.PRIME32_2;
                h32 ^= h32 >> 13;
                h32 *= Hashing.XXHash32Constants.PRIME32_3;
                h32 ^= h32 >> 16;

                return h32;
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(0, 13260023361988245289, 5069547415044450459, 18147346671579169581, 17057179469276629209)]
        [InlineData(1, 2230426092371548845, 4350138951518009054, 17651194012308130073, 6455872321791968642)]
        [InlineData(8, 10379630594295810139, 18406055579461580707, 11690924229451562600, 16371210486626029159)]
        [InlineData(127, 16797027344594192929, 694832513189815045, 2522907568797044855, 2561093604431397521)]
        [InlineData(128, 4795591985723416652, 6355843798816121102, 11775536190875556031, 14302802362311004465)]
        [InlineData(129, 15625596227218635967, 18109289740430552306, 8226597735891562253, 11057416134933533959)]
        [InlineData(255, 7041073115101331792, 7656328491956222156, 3604937293468875266, 5922493902073507716)]
        [InlineData(256, 421087964126876859, 9921628540918254164, 15832661750652342892, 12684435052037196729)]
        [InlineData(257, 563882931424226363, 6435403922130708820, 14582845796558047813, 18238228886697782956)]
        [InlineData(389, 10955151904838313245, 8426815227591195014, 10117284461105261649, 8350394782998917055)]
        [InlineData(511, 11684002115584893207, 4489466968517564350, 12109475670462321816, 10496476265755677810)]
        [InlineData(512, 6437450569622665391, 18279784609715013086, 1724887424391053956, 7076580505828885637)]
        [InlineData(513, 5653296409674678664, 3102400713564180505, 10327594772298807805, 3693502979329925657)]
        [InlineData(1024, 2857459740629528508, 9723063089059054045, 16838253329923706379, 11762302998859726172)]
        [InlineData(1025, 15126720552513900612, 16792120546869112487, 11128366263533068195, 17331891763721056766)]
        [InlineData(4095, 14421310098847514701, 11326763412858189950, 17965297135265620014, 246366116961809572)]
        [InlineData(4096, 6521826151345010457, 1517911174873890588, 11222044164473421521, 7488946297568388237)]
        [InlineData(4097, 11788821607224371465, 11543652943026641977, 15819773834900040640, 14123616650812483576)]
        [InlineData(65536, 11203777016773248169, 12638266180091144249, 14794312563532408379, 10818318105339971323)]
        public void AvxHash3_Sanity(int bufferSize, ulong h1, ulong h2, ulong h3, ulong h4)
        {
            byte[] values = new byte[bufferSize];
            for (int i = 0; i < bufferSize; i++)
                values[i] = (byte)i;

            int seed = 1337;

            var processor = new AdvHashing.AvxHash3Processor(Vector256.Create<ulong>((ulong)seed));
            processor.Process(bufferSize > 0 ? values.AsSpan(0, bufferSize) : ReadOnlySpan<byte>.Empty);

            var hash = processor.End();
            Assert.Equal(h1, hash[0]);
            Assert.Equal(h2, hash[1]);
            Assert.Equal(h3, hash[2]);
            Assert.Equal(h4, hash[3]);
        }
    }
}
