﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Server.Compression;
using Xunit;

namespace FastTests.Sparrow
{
    public class Encoder3GramTests
    {
        public struct State : IEncoderState
        {
            private readonly byte[] _value;

            public State(int size)
            {
                _value = new byte[size];
            }

            public Span<byte> EncodingTable => new Span<byte>(_value).Slice(0, _value.Length / 2);
            public Span<byte> DecodingTable => new Span<byte>(_value).Slice(_value.Length / 2);
        }

        public struct StringKeys : IReadOnlySpanEnumerator, ISpanEnumerator
        {
            private readonly byte[][] _values;

            public int Length => _values.Length;

            public ReadOnlySpan<byte> this[int i] => new(_values[i]);

            Span<byte> ISpanEnumerator.this[int i] => new(_values[i]);


            public StringKeys(string[] keys)
            {
                _values = new byte[keys.Length][];
                for (int i = 0; i < keys.Length; i++)
                {
                    var value = UTF8Encoding.ASCII.GetBytes(keys[i]);

                    var nullTerminated = new byte[value.Length + 1];
                    nullTerminated[value.Length] = 0;
                    value.AsSpan().CopyTo(nullTerminated);

                    _values[i] = nullTerminated;
                }
            }

            public StringKeys(byte[][] keys)
            {
                _values = keys;
            }
        }

        [Fact]
        public void SingleKeyEncoding()
        {
            var encoder = new HopeEncoder<Encoder3Gram>();
            State state = new(64000);

            int rawLength = 0;
            string[] keysAsStrings = new string[10000];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                keysAsStrings[i] = $"companies/{i:000000000}";
                rawLength += keysAsStrings[i].Length;
            }

            int dictSize = 128;
            StringKeys keys = new(keysAsStrings);
            encoder.Train(state, keys, dictSize);

            StringKeys encodingValue = new(new[] { Encoding.ASCII.GetBytes("companies/000000182\0") });

            Span<byte> value = new byte[128];
            Span<byte> decoded = new byte[128];

            var encodedBitLength = encoder.Encode(state, encodingValue[0], value);
            var decodedBytes = encoder.Decode(state, value, decoded);

            Assert.Equal(0, encodingValue[0].SequenceCompareTo(decoded.Slice(0, decodedBytes)));
        }

        public static IEnumerable<object[]> RandomSeed
        {
            get { yield return new object[] { new Random().Next(100000) }; }
        }

        [Theory]
        [MemberData("RandomSeed")]
        public void VerifyOrderPreservation(int randomSeed = 3117)
        {
            var encoder = new HopeEncoder<Encoder3Gram>();
            State state = new(64000);

            var rgn = new Random(randomSeed);

            const int size = 10000;
            int dictSize = rgn.Next(512);

            int rawLength = 0;
            string[] keysAsStrings = new string[size];
            byte[][] outputBuffers = new byte[size][];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                int id = rgn.Next(100000000);
                keysAsStrings[i] = $"companies/{id:000000000}";
                outputBuffers[i] = new byte[128];
                rawLength += keysAsStrings[i].Length;
            }

            StringKeys keys = new(keysAsStrings);
            encoder.Train(state, keys, dictSize);

            // Encode all keys.
            StringKeys inputValues = new(keysAsStrings);
            StringKeys outputValues = new(outputBuffers);
            Span<int> outputValuesSizeInBits = new int[keysAsStrings.Length];
            encoder.Encode(state, inputValues, outputValues, outputValuesSizeInBits);

            for (int i = 0; i < keysAsStrings.Length * 2; i++)
            {
                var value1Idx = rgn.Next(keysAsStrings.Length - 1);
                var value2Idx = rgn.Next(keysAsStrings.Length - 1);

                var value1 = inputValues[value1Idx];
                var value2 = inputValues[value2Idx];

                var encoded1SizeInBytes = outputValuesSizeInBits[value1Idx] / 8 + (outputValuesSizeInBits[value1Idx] % 8 == 0 ? 0 : 1);
                var encoded2SizeInBytes = outputValuesSizeInBits[value2Idx] / 8 + (outputValuesSizeInBits[value2Idx] % 8 == 0 ? 0 : 1);

                var encodedValue1 = outputValues[value1Idx].Slice(0, encoded1SizeInBytes);
                var encodedValue2 = outputValues[value2Idx].Slice(0, encoded2SizeInBytes);

                var originalOrder = value1.SequenceCompareTo(value2);
                var encodedOrder = encodedValue1.SequenceCompareTo(encodedValue2);

                // Normalize to (-1,0,1)
                originalOrder = (originalOrder < 0) ? -1 : (originalOrder > 0) ? 1 : 0;
                encodedOrder = (encodedOrder < 0) ? -1 : (encodedOrder > 0) ? 1 : 0;

                Assert.Equal(originalOrder, encodedOrder);
            }
        }

        [Theory]
        [MemberData("RandomSeed")]
        public void VerifyCorrectDecoding(int randomSeed)
        {
            var encoder = new HopeEncoder<Encoder3Gram>();
            State state = new(64000);

            var rgn = new Random(randomSeed);
            const int size = 10000;
            int dictSize = rgn.Next(512);

            int rawLength = 0;
            string[] keysAsStrings = new string[size];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                keysAsStrings[i] = $"companies/{i:000000000}";
                rawLength += keysAsStrings[i].Length;
            }

            StringKeys keys = new(keysAsStrings);

            encoder.Train(state, keys, dictSize);

            Span<byte> value = new byte[128];
            Span<byte> decoded = new byte[128];

            //int totalLength = 0;
            for (int i = 0; i < keys.Length; i++)
            {
                var encodedBitLength = encoder.Encode(state, keys[i], value);
                var decodedBytes = encoder.Decode(state, value, decoded);

                if (keys[i].SequenceCompareTo(decoded.Slice(0, decodedBytes)) != 0)
                {
                    encodedBitLength = encoder.Encode(state, keys[i], value);
                    decodedBytes = encoder.Decode(state, value, decoded);
                }

                //if (keys[i].SequenceCompareTo(decoded.Slice(0, decodedBytes)) != 0)
                //{
                //    Console.WriteLine($"{dictSize},{Encoding.ASCII.GetString(keys[i])}");
                //}

                Assert.Equal(0, keys[i].SequenceCompareTo(decoded.Slice(0, decodedBytes)));

                //totalLength += encodedBitLength;
            }

            // totalLength /= 8; // Convert to bytes
            // Console.WriteLine($"{dictSize}: {totalLength / 8:f2}, {(float)totalLength / rawLength:f2}x, {Encoder3Gram.GetDictionarySize(state)} bytes");


            //Console.WriteLine($"Memory Usage: {Encoder3Gram.GetDictionarySize(state)}.");
            //Console.WriteLine($"Key Size: {rawLength} (raw) vs {totalLength / 8:f2} (encoded).");
        }
    }
}
