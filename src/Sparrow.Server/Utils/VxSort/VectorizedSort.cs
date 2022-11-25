using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using V = System.Runtime.Intrinsics.Vector256<int>;

namespace VxSort
{
    // We will use type erasure to ensure that we can create specific variants of this same algorithm. 
    public static unsafe class Sort
    {
        internal const ulong ALIGN = 32;
        internal const ulong ALIGN_MASK = ALIGN - 1;

        internal const long REALIGN_LEFT = 0x666;
        internal const long REALIGN_RIGHT = 0x66600000000;
        internal const long REALIGN_BOTH = REALIGN_LEFT | REALIGN_RIGHT;

        static int FloorLog2PlusOne(uint n)
        {
            var result = 0;
            while (n >= 1)
            {
                result++;
                n /= 2;
            }
            return result;
        }

        public static void Run<T>([NotNull] T[] array)
            where T : unmanaged
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            fixed ( T* arrayPtr = array )
            {
                T* left = arrayPtr;
                T* right = arrayPtr + array.Length - 1;
                Run(left, right);
            }
        }

        public static void Run<T>([NotNull] Span<T> array)
            where T : unmanaged
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            // TODO: Improve this. 
            fixed (T* arrayPtr = array)
            {
                T* left = arrayPtr;
                T* right = arrayPtr + array.Length - 1;
                Run(left, right);
            }
        }

        public static void Run<T>(T* start, int count)
            where T : unmanaged
        {
            if (start == null)
                throw new ArgumentNullException(nameof(start));

            Run(start, start + count - 1);
        }

        [SkipLocalsInit]
        public static void Run<T>(T* left, T* right)
            where T : unmanaged
        {
            if (typeof(T) == typeof(int))
            {                
                int* il = (int*)left;
                int* ir = (int*)right;
                uint length = (uint)(ir - il) + 1;

                Debug.Assert(Avx2VectorizedSort.Int32Config.Unroll >= 1);
                Debug.Assert(Avx2VectorizedSort.Int32Config.Unroll <= 12);

                if (length < Avx2VectorizedSort.Int32Config.SmallSortThresholdElements) 
                {
                    BitonicSort.Sort(il, (int)length);
                    return;
                }
                    

                var depthLimit = 2 * FloorLog2PlusOne(length);
                var buffer = stackalloc byte[Avx2VectorizedSort.Int32Config.PartitionTempSizeInBytes];
                var sorter = new Avx2VectorizedSort(il, ir, buffer, Avx2VectorizedSort.Int32Config.PartitionTempSizeInBytes);
                sorter.sort(il, ir, int.MinValue, int.MaxValue, REALIGN_BOTH, depthLimit);
                return;
            }
            if (typeof(T) == typeof(long))
            {
                long* il = (long*)left;
                long* ir = (long*)right;
                int length = (int)(ir - il) + 1;

                Debug.Assert(Avx2VectorizedSort.Int64Config.Unroll >= 1);
                Debug.Assert(Avx2VectorizedSort.Int64Config.Unroll <= 12);

                if (length < Avx2VectorizedSort.Int64Config.SmallSortThresholdElements) 
                {
                    BitonicSort.Sort(il, (int)length);
                    return;
                }

                var depthLimit = 2 * FloorLog2PlusOne((uint)length);
                var buffer = stackalloc byte[Avx2VectorizedSort.Int64Config.PartitionTempSizeInBytes];
                var sorter = new Avx2VectorizedSort(il, ir, buffer, Avx2VectorizedSort.Int64Config.PartitionTempSizeInBytes);
                sorter.sort(il, ir, long.MinValue, long.MaxValue, REALIGN_BOTH, depthLimit);
                return;
            }
            throw new NotSupportedException();
        }
    }
}
