using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Server.Utils.VxSort;

namespace Sparrow.Server.Utils
{
    internal static class Sorting
    {
        public static unsafe int SortAndMergeDuplicates<T, W>(Span<T> values, Span<W> itemsAssociated)
            where T : unmanaged, IBinaryNumber<T>
            where W : unmanaged, IAdditionOperators<W, W, W>
        {
            if (values.Length <= 1)
                return values.Length;
            
            values.Sort(itemsAssociated);

            // Set up references for efficient span traversal
            ref var valuesStartRef = ref MemoryMarshal.GetReference(values);
            ref var valuesEndRef = ref Unsafe.Add(ref valuesStartRef, values.Length);
            ref var outputValuesRef = ref valuesStartRef;
            ref var outputItemRef = ref MemoryMarshal.GetReference(itemsAssociated);
            ref var currentValuesRef = ref valuesStartRef;
            ref var currentItemRef = ref outputItemRef;

            // Iterate through the sorted span to merge duplicates
            while (Unsafe.IsAddressLessThan(ref currentValuesRef, ref valuesEndRef))
            {
                // Advance to the next element for comparison, since in the naive version we
                // start at one, it is the same as pre incrementing the starting reference.
                currentValuesRef = ref Unsafe.Add(ref currentValuesRef, 1);
                currentItemRef = ref Unsafe.Add(ref currentItemRef, 1);

                // Check if current value is a duplicate of the last output value
                if (currentValuesRef != outputValuesRef)
                {
                    // Advance output position since this is a new value.
                    outputValuesRef = ref Unsafe.Add(ref outputValuesRef, 1);
                    outputItemRef = ref Unsafe.Add(ref outputItemRef, 1);

                    outputValuesRef = currentValuesRef;
                    outputItemRef = currentItemRef;
                }
                else
                {
                    // Duplicate value: merge associated items by adding them
                    outputItemRef += currentItemRef;
                }
            }

            // Calculate the number of unique elements based on the final output position
            int outputIdx = (int)Unsafe.ByteOffset(ref valuesStartRef, ref outputValuesRef).ToInt32() / Unsafe.SizeOf<T>();
            return outputIdx;
        }
        
        public static int SortAndRemoveDuplicates<T, W>(Span<T> valuesToDeduplicate, Span<W> itemsAssociated)
            where T : unmanaged, IBinaryNumber<T>
        {
            if (valuesToDeduplicate.Length <= 1)
                return valuesToDeduplicate.Length;
            
            valuesToDeduplicate.Sort(itemsAssociated);

            // We need to fill in the gaps left by removing deduplication process.
            // If there are no duplicated the writes at the architecture level will execute
            // way faster than if there are.

            int nextI = 0;
            int outputIdx = 0;
            while (nextI < valuesToDeduplicate.Length - 1)
            {
                int i = nextI;
                nextI++;

                outputIdx += (valuesToDeduplicate[nextI] != valuesToDeduplicate[i]).ToInt32();
                valuesToDeduplicate[outputIdx] = valuesToDeduplicate[nextI];
                itemsAssociated[outputIdx] = itemsAssociated[nextI];
            }

            outputIdx++;
            if (outputIdx != valuesToDeduplicate.Length)
            {
                valuesToDeduplicate[outputIdx] = valuesToDeduplicate[^1];
                itemsAssociated[outputIdx] = itemsAssociated[^1];
            }

            return outputIdx;
        }
        
        public static unsafe int SortAndRemoveDuplicates<T>(Span<T> values)
            where T : unmanaged, IBinaryNumber<T>
        {
            fixed (T* basePtr = values)
                return SortAndRemoveDuplicates(basePtr, values.Length);
        }

        public static unsafe int SortAndRemoveDuplicates<T, W>(T* bufferBasePtr, W* itemsBasePtr, int count)
            where T : unmanaged, IBinaryNumber<T>
            where W : unmanaged
        {
            new Span<T>(bufferBasePtr, count).Sort(new Span<W>(itemsBasePtr, count));

            // We need to fill in the gaps left by removing deduplication process.
            // If there are no duplicated the writes at the architecture level will execute
            // way faster than if there are.

            int index = 0;
            int runningIndex = 0;

            count--;
            while (runningIndex < count)
            {
                index += (bufferBasePtr[runningIndex + 1] != bufferBasePtr[runningIndex]).ToInt32();

                bufferBasePtr[index] = bufferBasePtr[runningIndex + 1];
                itemsBasePtr[index] = itemsBasePtr[runningIndex + 1];

                runningIndex++;
            }

            return index + 1;
        }

        public static unsafe int SortAndRemoveDuplicates<T>(T* bufferBasePtr, int count)
            where T : unmanaged, IBinaryNumber<T>
        {
            if (count == 0)
                return 0;
            Debug.Assert(count > 0);
            
            Sort.Run(bufferBasePtr, count);

            // We need to fill in the gaps left by removing deduplication process.
            // If there are no duplicated the writes at the architecture level will execute
            // way faster than if there are.

            var outputBufferPtr = bufferBasePtr;

            var bufferPtr = bufferBasePtr;
            var bufferEndPtr = bufferBasePtr + count - 1;
            while (bufferPtr < bufferEndPtr)
            {
                outputBufferPtr += (bufferPtr[1] != bufferPtr[0]).ToInt32();
                *outputBufferPtr = bufferPtr[1];

                bufferPtr++;
            }

            count = (int)(outputBufferPtr - bufferBasePtr + 1);
            return count;
        }
    }
}
