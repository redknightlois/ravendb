using System;
using System.Buffers;
using System.Collections.Generic;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Sparrow.Collections
{
    public sealed class WeakSmallSet<TKey, TValue> : IDisposable 
        where TKey : unmanaged
    {
        private const int Invalid = -1;

        private readonly int _length;
        private readonly TKey[] _keys;
        private readonly TValue[] _values;
        private int _currentIdx;

        public WeakSmallSet(int size)
        {
            _length = size - size % Vector<TKey>.Count;
            _keys = ArrayPool<TKey>.Shared.Rent(_length);
            _values = ArrayPool<TValue>.Shared.Rent(_length);
            _currentIdx = -1;
        }


        public void Add(TKey key, TValue value)
        {
            int idx = SelectBucketForWrite();
            _keys[idx] = key;
            _values[idx] = value;
        }

        private static ReadOnlySpan<int> LoadTable => new int[] { -1, 0, -3, -2, -5, -4, -7, -6 };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe int SelectBucketForRead(TKey key)
        {
            int elementIdx = Math.Min(_currentIdx, _length - 1);

            if (Vector.IsHardwareAccelerated)
            {
                var keys = _keys.AsSpan();

                Vector<TKey> chunk;
                var keyVector = new Vector<TKey>(key);
                while (elementIdx >= Vector256<TKey>.Count)
                {
                    // We subtract because we are going to use that even in the case when there are differences.
                    int higherIdx = elementIdx;
                    elementIdx -= Vector256<TKey>.Count;
                    chunk = new Vector<TKey>(keys[elementIdx..higherIdx]);
                    chunk = Vector.Equals(keyVector, chunk);
                    if (chunk == Vector<TKey>.Zero)
                        continue;

                    goto Found; 
                }

                elementIdx = 0;
                chunk = new Vector<TKey>(keys);
                chunk = Vector.Equals(keyVector, chunk);
                if (chunk == Vector<TKey>.Zero)
                    return Invalid;

                Found:
                ref TKey first = ref keys[elementIdx];

                if (Unsafe.SizeOf<TKey>() > 8)
                    goto SequenceCompareTo;

                TKey lookUp0 = Unsafe.Add(ref first, 0);
                if (lookUp0.Equals(key))
                    goto Done;

                if (Unsafe.SizeOf<TKey>() > 1)
                {
                    lookUp0 = Unsafe.Add(ref first, 1);
                    if (lookUp0.Equals(key))
                        goto Done;
                }

                if (Unsafe.SizeOf<TKey>() > 2)
                {
                    lookUp0 = Unsafe.Add(ref first, 2);
                    if (lookUp0.Equals(key))
                        goto Done;
                }

                if (Unsafe.SizeOf<TKey>() > 3)
                {
                    lookUp0 = Unsafe.Add(ref first, 3);
                    if (lookUp0.Equals(key))
                        goto Done;
                }

                if (Unsafe.SizeOf<TKey>() > 4)
                {
                    lookUp0 = Unsafe.Add(ref first, 4);
                    if (lookUp0.Equals(key))
                        goto Done;
                }

                if (Unsafe.SizeOf<TKey>() > 5)
                {
                    lookUp0 = Unsafe.Add(ref first, 5);
                    if (lookUp0.Equals(key))
                        goto Done;
                }

                if (Unsafe.SizeOf<TKey>() > 6)
                {
                    lookUp0 = Unsafe.Add(ref first, 6);
                    if (!lookUp0.Equals(key))
                        goto Done;
                }

                if (Unsafe.SizeOf<TKey>() > 7)
                {
                    lookUp0 = Unsafe.Add(ref first, 7);
                }

                Done:

                long difference = Unsafe.ByteOffset(ref first, ref lookUp0).ToInt64();
                return elementIdx + (int)(difference / Unsafe.SizeOf<TKey>());
            }

            SequenceCompareTo:
            while (elementIdx >= 0)
            {
                ref TKey current = ref _keys[elementIdx];
                if (current.Equals(key))
                    return elementIdx;

                elementIdx--;
            }

            return Invalid;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int SelectBucketForWrite()
        {
            _currentIdx++;
            return _currentIdx % _length;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            int idx = SelectBucketForRead(key);
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
