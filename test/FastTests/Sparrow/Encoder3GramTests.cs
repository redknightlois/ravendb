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

            public Span<byte> Table => new (_value);
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
                    _values[i] = UTF8Encoding.ASCII.GetBytes(keys[i]);
            }
        }

        [Fact]
        public void SimpleTraining()
        {
            var encoder = new HopeEncoder<Encoder3Gram>();
            State state = new (32000);
            //StringKeys keys = new(new[] { "Captain-", "Captain----Obvious", "Captain---Caveman", "Captain---Crazy", "Captain---Redundant", "Captain--0bvious", "Captain--Awesome", "Captain--Canada", "Captain--Captain", "Captain--Cocaine", "Captain--Fabulous", "Captain--Fawcett", "Captain--Hindsight", "Captain--Insano", "Captain--Morgan", "Captain--Oblivious" });
            //StringKeys keys = new(new[] { "abc", "def", "companies/1" });

            //string[] keysAsStrings = new string[5000];
            //for (int i = 0; i < keysAsStrings.Length; i++)
            //    keysAsStrings[i] = Guid.NewGuid().ToString();
            //StringKeys keys = new(keysAsStrings);

            string[] keysAsStrings = new string[600];
            for (int i = 0; i < keysAsStrings.Length; i++)
            {
                keysAsStrings[i] = $"companies/{i:000000000}";
            }

            StringKeys keys = new(keysAsStrings);

            encoder.Train(state, keys, 256);

            Span<byte> value = new byte[64];

            StringKeys data = new(new[] { "compan1ies/000001234" });
            var len = encoder.Encode(state, data[0], value);

            Console.WriteLine($"Memory Usage: {Encoder3Gram.GetDictionarySize(state)}.");
            Console.WriteLine($"Key Size: {data[0].Length} (raw) vs {len / 8:f2} (encoded).");
        }
    }
}
