using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    /// <summary>
    /// A specialized binary min-heap for HNSW search operations.
    /// Optimized vs BCL PriorityQueue: combined (element, priority) storage,
    /// direct float comparison (no IComparer indirection), no version tracking.
    /// </summary>
    internal sealed class FastMinHeap
    {
        private (int Element, float Priority)[] _heap;
        private int _count;

        public int Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _count;
        }

        public FastMinHeap(int initialCapacity = 16)
        {
            _heap = new (int, float)[initialCapacity];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Enqueue(int element, float priority)
        {
            if (_count == _heap.Length)
                Grow();

            _heap[_count] = (element, priority);
            SiftUp(_count);
            _count++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue(out int element, out float priority)
        {
            if (_count == 0)
            {
                element = default;
                priority = default;
                return false;
            }

            (element, priority) = _heap[0];
            _count--;
            if (_count > 0)
            {
                _heap[0] = _heap[_count];
                SiftDown(0);
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPeek(out int element, out float priority)
        {
            if (_count == 0)
            {
                element = default;
                priority = default;
                return false;
            }

            (element, priority) = _heap[0];
            return true;
        }

        /// <summary>
        /// If the new priority is greater than the root's priority, replaces the root
        /// and sifts down. Otherwise does nothing.
        /// This is the hot path for bounded-result-set updates in HNSW search.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EnqueueDequeue(int element, float priority)
        {
            Debug.Assert(_count > 0);
            if (priority <= _heap[0].Priority)
                return;

            _heap[0] = (element, priority);
            SiftDown(0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            _count = 0;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void Grow()
        {
            var newCapacity = _heap.Length < 64 ? _heap.Length * 4 : _heap.Length * 2;
            var newHeap = new (int, float)[newCapacity];
            Array.Copy(_heap, newHeap, _count);
            _heap = newHeap;
        }

        private void SiftUp(int index)
        {
            var node = _heap[index];
            while (index > 0)
            {
                int parentIndex = (index - 1) >> 1;
                ref var parent = ref _heap[parentIndex];
                if (node.Priority < parent.Priority)
                {
                    _heap[index] = parent;
                    index = parentIndex;
                }
                else
                    break;
            }
            _heap[index] = node;
        }

        private void SiftDown(int index)
        {
            var node = _heap[index];
            int half = _count >> 1;
            while (index < half)
            {
                int left = (index << 1) + 1;
                int right = left + 1;
                int minChild = left;
                if (right < _count && _heap[right].Priority < _heap[left].Priority)
                    minChild = right;

                ref var child = ref _heap[minChild];
                if (node.Priority <= child.Priority)
                    break;

                _heap[index] = child;
                index = minChild;
            }
            _heap[index] = node;
        }
    }
}
