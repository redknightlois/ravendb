using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Threading;

namespace Sparrow.Server.Collections.Persistent
{
    /// <summary>
    /// The SingleWriterLockFreeTree is a 32-way SIMD optimized BTree using the path-replacement
    /// concept introduced in "Efficient Single Writer Concurrency" by Ben-David et al.
    /// https://arxiv.org/pdf/1803.08617v1
    /// The idea behind this data structure is to allow multiple active snapshots while
    /// a single writer modifies the tree in a write transaction.
    /// </summary>
    /// <typeparam name="T">The underlying type we are going to be storing</typeparam>
    public class SingleWriterLockFreeTree<T>
    {
        private const int StartingSpace = 64;  // Initial size for node and value arrays.
        private const int InvalidIndex = -1;   // Sentinel value indicating an invalid array index.
        private const int NotInUse = 0;        // Flag indicating that a resource is not currently in use.
        private const int InUse = 1;           // Flag indicating that a resource is in use.


        /// <summary>
        /// Represents a snapshot of the tree at a given transaction point, 
        /// encapsulating the state of the tree at that moment.
        /// </summary>
        public class Snapshot()
        {
            /// <summary>
            /// The root node index of the snapshot.
            /// </summary>
            public int Root { get; internal set; }

            /// <summary>
            /// Transaction identifier that uniquely identifies this snapshot.
            /// </summary>
            public long TransactionId { get; internal set; }

            /// <summary>
            /// The number of elements in the tree when this snapshot was created.
            /// </summary>
            public int Count { get; internal set; }

            /// <summary>
            /// Links to the previous snapshot in the chain. This enables tracking the history of versions
            /// and provides a mechanism to eventually prune old versions when they are no longer needed.
            /// </summary>
            internal Snapshot Previous;

            /// <summary>
            /// An array of values that corresponds to tree nodes, stored separately for quick access during read operations.
            /// It may be resized during operations that require more space or during compaction processes.
            /// </summary>
            internal readonly T[] _values;

            /// <summary>
            /// An array of nodes that form the tree structure. This array is typically reused across snapshots but may
            /// be resized in response to growth operations or compaction.
            /// </summary>
            internal readonly Node[] _treeNodes;

            internal Snapshot(long transactionId, Node[] treeNodes, T[] values, int count, Snapshot previous = null, int root = InvalidIndex) : this()
            {
                _treeNodes = treeNodes;
                _values = values;

                Root = root;
                Count = count;
                Previous = previous;
                TransactionId = transactionId;
            }
        }

        /// <summary>
        /// Holds the current snapshot of the tree. It is updated atomically to ensure all readers see a consistent version
        /// of the tree. Updates are made visible via volatile writes rather than CAS to simplify the logic and ensure that
        /// the most recent completed write transaction is visible to all readers.
        /// </summary>
        private Snapshot _current;

        /// <summary>
        /// Transaction ID used during write operations to identify the current modification session uniquely.
        /// This is updated for each new write transaction, allowing the system to maintain a clear history of changes.
        /// </summary>
        private long _currentWriteTransactionId;

        /// <summary>
        /// Index of the new root node after modifications made by the current write transaction. This value is
        /// committed to the current snapshot upon completion of the transaction.
        /// </summary>
        private int _currentRoot;

        /// <summary>
        /// Reflects the number of items in the tree, updated dynamically as items are added or removed during the current transaction.
        /// This count is committed to the snapshot once the transaction is completed.
        /// </summary>
        private int _count;

        internal int Count => _count;

        /// <summary>
        /// Manages an array of values corresponding to tree nodes, allowing resizing when necessary due to tree expansion
        /// or during a compaction process initiated by a write transaction. Compaction helps in reclaiming unused space and 
        /// optimizing memory usage.
        /// </summary>
        internal T[] _values = new T[StartingSpace];

        /// <summary>
        /// Tracks up to which transaction ID each value in the _values array is valid. This helps in avoiding unnecessary
        /// duplications and reducing memory usage by efficiently managing the lifecycle of each value based on transaction visibility.
        /// </summary>
        internal long[] _valuesValidity = new long[StartingSpace];

        /// <summary>
        /// Free-list used to manage the recycling of value slots in the _values array, enhancing memory efficiency
        /// by reusing slots that are no longer valid for any active transactions.
        /// </summary>
        private Queue<int> _valuesFreeList = new();

        /// <summary>
        /// We will never use the value at index 0, because we need to be able to define a null value. To avoid
        /// complexity we will waste a single value and use it as a guard. This value is used to 'extend' the
        /// usage when there are no more values in the free list but we still have available space in the array
        /// </summary>
        private int _valuesUsageCount = 1;

        /// <summary>
        /// Similar to _values, this array stores the tree nodes. It is also subject to resizing and is tightly managed
        /// to ensure memory efficiency and optimal performance during tree navigation.
        /// </summary>
        internal Node[] _treeNodes = new Node[StartingSpace];

        /// <summary>
        /// Tracks up to which transaction ID each node in the _treeNodes array is valid. This helps in avoiding unnecessary
        /// duplications and reducing memory usage by efficiently managing the lifecycle of each node based on transaction visibility.
        /// </summary>
        internal long[] _treeNodesValidity = new long[StartingSpace];

        /// <summary>
        /// We will never use the tree node at index 0, because we need to be able to define a null value. To avoid
        /// complexity we will waste a single value and use it as a guard.  This value is used to 'extend' the
        /// usage when there are no more values in the free list but we still have available space in the array
        /// </summary>
        private int _treeNodesUsageCount = 1;

        /// <summary>
        /// The free-list for the node indexes.
        /// </summary>
        private Queue<int> _treeNodesFreeList = new();

        /// <summary>
        /// The Node struct represents the nodes in the BTree. It is designed to be cache-aligned to avoid false sharing
        /// and to leverage SIMD operations for efficient processing.
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Pack = 0, Size = 128)]
        internal struct Node
        {
            /// <summary>
            /// When all keys are -1, the node is considered empty. The first index then points to the next node in the free list.
            /// </summary>
            [FieldOffset(0)]
            internal Vector512<uint> Keys;

            /// <summary>
            /// The indexes to the next nodes in the list of values. If the node is free, the first index points to the next node in the free list.
            /// Negatives are leafs and positives are branch. 
            /// </summary>
            [FieldOffset(64)]
            internal Vector512<int> Indexes;

            /// <summary>
            /// Determines if the node is a branch by checking if all index values are non-negative.
            /// </summary>
            internal bool IsBranch => Vector512.GreaterThanOrEqualAll(Indexes, Vector512<int>.Zero);

            /// <summary>
            /// Determines if the node is a leaf by checking if all index values are non-positive.
            /// </summary>
            internal bool IsLeaf => Vector512.LessThanOrEqualAll(Indexes, Vector512<int>.Zero);
        }
        
        public SingleWriterLockFreeTree()
        {
            _current = new Snapshot(InvalidIndex, _treeNodes, _values, 0);

            // Initializing the nodes as 'empty'
            for (int i = 0; i < _treeNodes.Length; i++)
            {
                ref var node = ref _treeNodes[i];
                node.Keys = Vector512<uint>.AllBitsSet;
                node.Indexes = Vector512<int>.Zero;
            }
        }

        public readonly struct ReadContext(SingleWriterLockFreeTree<T> tree, Snapshot snapshot)
        {
            public int Count => snapshot.Count;

            /// <summary>
            /// Tries to retrieve the value associated with the specified key.
            /// </summary>
            /// <param name="key">The key of the value to retrieve.</param>
            /// <param name="value">The output value if the key is found.</param>
            /// <returns>True if the value is found, otherwise false.</returns>
            public bool TryGet(long key, out T value)
            {
                return tree.TryGet(snapshot, key, out value);
            }
        }

        public readonly struct WriteContext(SingleWriterLockFreeTree<T> tree) : IDisposable
        {
            public int Count => tree.Count;

            public void Dispose()
            {
                // Commit the transaction
                tree.Commit();
                
                if (Interlocked.CompareExchange(ref tree._inUseFlag, NotInUse, InUse) == NotInUse)
                    throw new InvalidOperationException("There cannot be more than a single writer. This method cannot be called multiple times.");
            }

            /// <summary>
            /// Tries to retrieve the value associated with the specified key.
            /// </summary>
            /// <param name="key">The key of the value to retrieve.</param>
            /// <param name="value">The output value if the key is found.</param>
            /// <returns>True if the value is found, otherwise false.</returns>
            internal bool TryGet(long key, out T value)
            {
                return tree.TryGet(key, out value);
            }

            /// <summary>
            /// Inserts or updates the value associated with the specified key.
            /// </summary>
            /// <param name="key">The key of the value to insert or update.</param>
            /// <param name="value">The value to be inserted or updated.</param>
            internal void Put(long key, T value)
            {
                tree.Put(key, value);
            }

            /// <summary>
            /// Removes the value associated with the specified key.
            /// </summary>
            /// <param name="key">The key of the value to delete.</param>
            /// <returns>True if the value is removed, otherwise false.</returns>
            internal bool TryRemove(long key)
            {
                return tree.TryRemove(key);
            }
        }

        private int _inUseFlag = NotInUse;

        /// <summary>
        /// Acquires a writer context for a new transaction. Only one writer is allowed at a time.
        /// </summary>
        /// <param name="transactionId">The transaction ID for the new transaction.</param>
        /// <returns>A WriteContext for performing write operations.</returns>
        public WriteContext Writer(long transactionId)
        {
            if (Interlocked.CompareExchange(ref _inUseFlag, InUse, NotInUse) == InUse)
                throw new InvalidOperationException("There cannot be more than a single writer. This method cannot be called multiple times.");

            _currentWriteTransactionId = transactionId;
            return new WriteContext(this);
        }

        /// <summary>
        /// Acquires a reader context for the current snapshot.
        /// </summary>
        public ReadContext Reader()
        {
            return new ReadContext(this, Volatile.Read(ref _current));
        }

        /// <summary>
        /// Allocates a new node based on an existing node for the current transaction.
        /// </summary>
        /// <param name="from">The index of the node to copy from.</param>
        /// <returns>The index of the newly allocated node.</returns>
        protected int AllocateNode(int from)
        {
            int newIndex;
            if (_treeNodesFreeList.TryPeek(out int maybe) & _treeNodesValidity[maybe] < this._oldestTransactionId)
            {
                newIndex = _treeNodesFreeList.Dequeue();
            }
            else
            {
                // If no free nodes are available, grow the array
                if (_treeNodesUsageCount >= _treeNodes.Length)
                {
                    // Expand the node array and validity array
                    int newSize = _values.Length * 2;
                    Array.Resize(ref _treeNodes, newSize);
                    Array.Resize(ref _treeNodesValidity, newSize);
                }
                newIndex = _treeNodesUsageCount++;
            }

            // Copy the node from the existing index, depending on the platform this can be just assembler 2 instructions.
            _treeNodes[newIndex] = _treeNodes[from];
            _treeNodesFreeList.Enqueue(newIndex);
            
            return newIndex;
        }

        /// <summary>
        /// Allocates a new value based on an existing value for the current transaction.
        /// </summary>
        /// <param name="from">The index of the value to copy from.</param>
        /// <returns>The index of the newly allocated value.</returns>
        protected int AllocateValue(int from)
        {
            int newIndex;
            if (_valuesFreeList.TryPeek(out int maybe) & _valuesValidity[maybe] < this._oldestTransactionId)
            {
                newIndex = _valuesFreeList.Dequeue();
            }
            else
            {
                // If no free values are available, grow the array
                if (_valuesUsageCount >= _values.Length)
                {
                    // Expand the value array and validity array
                    int newSize = _values.Length * 2;
                    Array.Resize(ref _values, newSize);
                    Array.Resize(ref _valuesValidity, newSize);
                }
                newIndex = _valuesUsageCount++;
            }

            _values[newIndex] = _values[from];
            _valuesFreeList.Enqueue(newIndex);
            return newIndex;
        }

        /// <summary>
        /// Tries to retrieve the value associated with the specified key from the given snapshot.
        /// </summary>
        /// <param name="snapshot">The snapshot to search in.</param>
        /// <param name="key">The key of the value to retrieve.</param>
        /// <param name="value">The output value if the key is found.</param>
        /// <returns>True if the value is found, otherwise false.</returns>
        protected bool TryGet(Snapshot snapshot, long key, out T value)
        {
            // Traverse the tree from the root to find the key
            ref var root = ref snapshot._treeNodes[snapshot.Root];

            // From the snapshot root, if it is a leaf, return the appropriate value if available
            // If it is a branch move down until we find a leaf and return the value if available.

            // Placeholder implementation
            throw new NotImplementedException();
        }

        /// <summary>
        /// Tries to retrieve the value associated with the specified key from the current mutable state.
        /// </summary>
        /// <param name="key">The key of the value to retrieve.</param>
        /// <param name="value">The output value if the key is found.</param>
        /// <returns>True if the value is found, otherwise false.</returns>
        protected bool TryGet(long key, out T value)
        {
            // During the execution of this method, we dont know if nodes and values are mutated or not.
            // For that reason we use the local mutable copy. 
            ref var root = ref _treeNodes[_currentRoot];

            // From the snapshot root, if it is a leaf, return the appropriate value if available
            // If it is a branch move down until we find a leaf and return the value if available.

            // Placeholder implementation
            throw new NotImplementedException();
        }

        /// <summary>
        /// Inserts or updates the value associated with the specified key.
        /// </summary>
        /// <param name="key">The key of the value to insert or update.</param>
        /// <param name="value">The value to be inserted or updated.</param>
        protected void Put(long key, T value)
        {
            // Use the local mutable copy to make changes
            _currentRoot = GetMutableNode(_currentRoot);

            int currentNodeIndex = _currentRoot;
            ref var node = ref _treeNodes[currentNodeIndex];
            while (node.IsBranch)
            {
                // Find the branch index for the new node to find
                int index = SearchNode(in node, key);

                // It is important to get a mutable node reference to ensure we do not mutate the original state
                currentNodeIndex = GetMutableNode(index);

                // Go down to the next tree node
                node = ref _treeNodes[currentNodeIndex];
            }

            // Now we are at a leaf node

            // If we have enough space, we insert or update the value
            // If we don't have enough space, we split the node and insert a branch instead

            // Placeholder implementation
            throw new NotImplementedException();
        }

        /// <summary>
        /// Searches for the appropriate index in the node for the given key.
        /// </summary>
        /// <param name="node">The node to search in.</param>
        /// <param name="key">The key to search for.</param>
        /// <returns>The index where the key should be.</returns>
        private int SearchNode(in Node node, long key)
        {
            // Find the branch index for the new node to find and return the new index

            // Placeholder implementation
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets a mutable copy of the node from the specified index for the current transaction.
        /// </summary>
        /// <param name="from">The index of the node to copy from.</param>
        /// <returns>The index of the newly allocated node.</returns>
        private int GetMutableNode(int from)
        {
            // If the current node validity is already known to be the current write transaction
            // then it means that a call to ModifyNode for that node has already happened and the
            // node can be reused as it is already dirty
            if (_treeNodesValidity[from] == _currentWriteTransactionId)
                return from;

            // Allocate a new node for this particular node
            return AllocateNode(from);
        }

        /// <summary>
        /// Gets a mutable copy of the value from the specified index for the current transaction.
        /// </summary>
        /// <param name="from">The index of the node to copy from.</param>
        /// <returns>The index of the newly allocated node.</returns>
        private int GetMutableValue(int from)
        {
            // If the current node validity is already known to be the current write transaction
            // then it means that a call to ModifyNode for that node has already happened and the
            // node can be reused as it is already dirty
            if (_valuesValidity[from] == _currentWriteTransactionId)
                return from;

            // Allocate a new value for this particular node
            return AllocateValue(from);
        }

        /// <summary>
        /// Removes the value associated with the specified key.
        /// </summary>
        /// <param name="key">The key of the value to delete.</param>
        /// <returns>True if the value is removed, otherwise false.</returns>
        protected bool TryRemove(long key)
        {
            // Use the local mutable copy to make changes
            _currentRoot = GetMutableNode(_currentRoot);

            int currentNodeIndex = _currentRoot;
            ref var node = ref _treeNodes[currentNodeIndex];
            while (node.IsBranch)
            {
                // Find the branch index for the new node to find
                int index = SearchNode(in node, key);

                // It is important to get a mutable node reference to ensure we do not mutate the original state
                currentNodeIndex = GetMutableNode(index);

                // Go down to the next tree node
                node = ref _treeNodes[currentNodeIndex];
            }

            // Remove the actual value if available
            int itemIndex = SearchNode(in node, key);

            // Placeholder implementation
            throw new NotImplementedException();
        }

        protected void Commit()
        {
            var snapshot = new Snapshot(_currentWriteTransactionId, _treeNodes, _values, _count, _current, _currentRoot);

            // Publish the new snapshot with a memory barrier to ensure visibility.
            Volatile.Write(ref _current, snapshot);
        }

        private long _oldestTransactionId;

        /// <summary>
        /// Sets the oldest transaction ID that is still valid and prunes the snapshot chain accordingly.
        /// </summary>
        /// <param name="transactionId">The oldest valid transaction ID.</param>
        public void SetOldestTransactionId(long transactionId)
        {
            _oldestTransactionId = transactionId;

            // Prune the snapshot chain to remove old versions no longer needed
            var current = _current;
            while (current.Previous != null && current.TransactionId > transactionId)
                current = current.Previous;

            Debug.Assert(current == null || current.TransactionId <= transactionId);
            current.Previous = null;
        }
    }
}
