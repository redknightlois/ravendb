using System;
using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace Sparrow.Server.Collections
{
    public sealed class WeakSmallSet<TKey, TValue> : IDisposable 
        where TKey : unmanaged
    {
        private const int Invalid = -1;

        private readonly int _length;
        private readonly TKey[] _keys;
        private readonly TValue[] _values;
        private int _currentIdx;

        public WeakSmallSet(int size = 0)
        {
            _length = size > Vector<TKey>.Count ? size - size % Vector<TKey>.Count : Vector<TKey>.Count;
            _keys = ArrayPool<TKey>.Shared.Rent(_length);
            _values = ArrayPool<TValue>.Shared.Rent(_length);
            _currentIdx = -1;
        }


        public void Add(TKey key, TValue value)
        {
            int idx = FindKey(key);
            if (idx == Invalid)
                idx = RequestWritableBucket();

            _keys[idx] = key;
            _values[idx] = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int FindKey(TKey key)
        {
            int elementIdx = Math.Min(_currentIdx, _length - 1);

            var keys = _keys;
            if (Vector.IsHardwareAccelerated && elementIdx > Vector256<TKey>.Count)
            {
                Vector<TKey> chunk;
                var keyVector = new Vector<TKey>(key);
                while (elementIdx >= Vector256<TKey>.Count)
                {
                    // We subtract because we are going to use that even in the case when there are differences.
                    elementIdx -= Vector256<TKey>.Count;
                    chunk = new Vector<TKey>(keys, elementIdx + 1);
                    chunk = Vector.Equals(keyVector, chunk);
                    if (chunk == Vector<TKey>.Zero)
                        continue;

                    elementIdx = Math.Min(elementIdx + Vector256<TKey>.Count, _length - 1);
                    goto Found; 
                }

                chunk = new Vector<TKey>(keys);
                chunk = Vector.Equals(keyVector, chunk);
                if (chunk == Vector<TKey>.Zero)
                    return Invalid;

                elementIdx = Vector256<TKey>.Count - 1;

                Found:
                elementIdx = Math.Min(elementIdx, _currentIdx);
            }

            while (elementIdx >= 0)
            {
                ref TKey current = ref keys[elementIdx];
                if (current.Equals(key))
                    return elementIdx;

                elementIdx--;
            }

            return Invalid;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int RequestWritableBucket()
        {
            _currentIdx++;
            return _currentIdx % _length;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            int idx = FindKey(key);
            if (idx == Invalid)
            {
                Unsafe.SkipInit(out value);
                return false;
            }
                
            value = _values[idx];
            return true;
        }

        public void Clear()
        {
            Array.Fill(_keys, (TKey)(object)-1);
            Array.Fill(_values, default);
            _currentIdx = -1;
        }

        public void Dispose()
        {
            ArrayPool<TKey>.Shared.Return(_keys);
            ArrayPool<TValue>.Shared.Return(_values);
        }
    }
}
