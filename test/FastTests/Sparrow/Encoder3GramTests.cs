using System;
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

        public struct StringKeys : IReadOnlySpanEnumerator
        {
            private readonly byte[][] _values;

            public int Length => _values.Length;

            public ReadOnlySpan<byte> this[int i] => new (_values[i]);


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
            State state = new(32000);

            int rawLength = 0;
            string[] keysAsStrings = new string[10000];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                keysAsStrings[i] = $"companies/{i:000000000}";
                rawLength += keysAsStrings[i].Length;
            }

            int dictSize = 16;
            StringKeys keys = new(keysAsStrings);
            encoder.Train(state, keys, dictSize);

            StringKeys encodingValue = new(new[] { Encoding.ASCII.GetBytes("companies/000000011\0") });

            Span<byte> value = new byte[128];
            Span<byte> decoded = new byte[128];

            var encodedBitLength = encoder.Encode(state, encodingValue[0], value);
            var decodedBytes = encoder.Decode(state, value, decoded);

            Assert.Equal(0, encodingValue[0].SequenceCompareTo(decoded.Slice(0, decodedBytes)));
        }

        [Fact]
        public void SimpleTraining()
        {
            var encoder = new HopeEncoder<Encoder3Gram>();
            State state = new(32000);
            //StringKeys keys = new(new[] { "Captain-", "Captain----Obvious", "Captain---Caveman", "Captain---Crazy", "Captain---Redundant", "Captain--0bvious", "Captain--Awesome", "Captain--Canada", "Captain--Captain", "Captain--Cocaine", "Captain--Fabulous", "Captain--Fawcett", "Captain--Hindsight", "Captain--Insano", "Captain--Morgan", "Captain--Oblivious" });
            //StringKeys keys = new(new[] { "abc", "def", "companies/1" });

            //string[] keysAsStrings = new string[5000];
            //for (int i = 0; i < keysAsStrings.Length; i++)
            //    keysAsStrings[i] = Guid.NewGuid().ToString();
            //StringKeys keys = new(keysAsStrings);

            int rawLength = 0;
            string[] keysAsStrings = new string[10000];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                keysAsStrings[i] = $"companies/{i:000000000}";
                rawLength += keysAsStrings[i].Length;
            }

            StringKeys keys = new(keysAsStrings);

            for (int dictSize = 16; dictSize < 1024; dictSize *= 2)
            {
                encoder.Train(state, keys, dictSize);

                Span<byte> value = new byte[128];
                Span<byte> decoded = new byte[128];

                int totalLength = 0;
                for (int i = 0; i < keys.Length; i++)
                {
                    var encodedBitLength = encoder.Encode(state, keys[i], value);
                    var decodedBytes = encoder.Decode(state, value, decoded);

                    if (keys[i].SequenceCompareTo(decoded.Slice(0, decodedBytes)) != 0)
                    {
                        encodedBitLength = encoder.Encode(state, keys[i], value);
                        decodedBytes = encoder.Decode(state, value, decoded);
                    }

                    Assert.Equal(0, keys[i].SequenceCompareTo(decoded.Slice(0, decodedBytes)));

                    totalLength += encodedBitLength;
                }

                totalLength /= 8; // Convert to bytes

                Console.WriteLine($"{dictSize}: {totalLength / 8:f2}, {(float)totalLength / rawLength:f2}x, {Encoder3Gram.GetDictionarySize(state)} bytes");
            }

            //Console.WriteLine($"Memory Usage: {Encoder3Gram.GetDictionarySize(state)}.");
            //Console.WriteLine($"Key Size: {rawLength} (raw) vs {totalLength / 8:f2} (encoded).");
        }
    }
}
