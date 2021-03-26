using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Server;

namespace Corax.Filters
{
    public sealed class LowerCaseFilter<TSource> : ITokenFilter
        where TSource : ITokenizer
    {
        private TSource _source = default;
        private readonly TokenSpanStorageContext _storage;

        public LowerCaseFilter([NotNull] TokenSpanStorageContext storage, [NotNull] TSource tokenizer)
        {
            _storage = storage;
            _source = tokenizer;
        }

        public struct Enumerator : IEnumerator<TokenSpan>
        {
            private readonly TSource _source;
            private readonly IEnumerator _sourceEnumerable;
            private readonly TokenSpanStorageContext _storage;
            private TokenSpan _current;

            public Enumerator([NotNull] TSource source, [NotNull] TokenSpanStorageContext storage)
            {
                _source = source;
                _sourceEnumerable = source.GetEnumerator();
                _storage = storage;
                _current = TokenSpan.Null;

                Reset();
            }

            public bool MoveNext()
            {
                bool moveNext = _sourceEnumerable.MoveNext();
                if (moveNext)
                {
                    // TODO: Check if the cast is gonna be evicted.
                    _current = (TokenSpan)_sourceEnumerable.Current;

                    var buffer = _storage.RequestWriteAccess(_current);
                    DoLowercase(buffer);
                }
                return moveNext;
            }

            private void DoLowercase(Span<byte> buffer)
            {
                for (int i = 0; i < buffer.Length; i++)
                {
                    if (buffer[i] >= 'A' && buffer[i] <= 'Z')
                    {
                        byte value = buffer[i];
                        value += (byte)('a' - 'A');
                        buffer[i] = value;
                    }
                }
            }

            public void Reset()
            {
                _current = TokenSpan.Null;
                _sourceEnumerable.Reset();
            }

            public TokenSpan Current => _current;

            object IEnumerator.Current => _current;

            public void Dispose() { }
        }

        public Enumerator GetEnumerator()
        {
            return new(_source, _storage);
        }

        IEnumerator<TokenSpan> IEnumerable<TokenSpan>.GetEnumerator()
        {
            return new Enumerator(_source, _storage);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new Enumerator(_source, _storage);
        }
    }
}
