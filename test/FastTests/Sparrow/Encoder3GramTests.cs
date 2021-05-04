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
            State state = new (8096);
            StringKeys keys = new(new[] { "Captain-", "Captain----Obvious", "Captain---Caveman", "Captain---Crazy", "Captain---Redundant", "Captain--0bvious", "Captain--Awesome", "Captain--Canada", "Captain--Captain", "Captain--Cocaine", "Captain--Fabulous", "Captain--Fawcett", "Captain--Hindsight", "Captain--Insano", "Captain--Morgan", "Captain--Oblivious" });

            encoder.Train(state, keys, 128);

            Span<byte> value = new byte[64];

            StringKeys data = new(new[] { "Captain---Crazy" });
            var len = encoder.Encode(state, data[0], value);
        }
    }
}
