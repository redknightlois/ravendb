﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using System.Diagnostics;
using Sparrow.Json;
using Sparrow.Server;
using Voron.Data.RawData;
using Constants = Voron.Global.Constants;

namespace Voron.Impl
{
    public unsafe class Transaction : IDisposable
    {
        private Dictionary<Tuple<Tree, Slice>, Tree> _multiValueTrees;
        private Dictionary<long, ByteString> _cachedDecompressedBuffersByStorageId;

        internal Dictionary<long, ByteString> CachedDecompressedBuffersByStorageId =>
            _cachedDecompressedBuffersByStorageId ??= new Dictionary<long, ByteString>();

        private LowLevelTransaction _lowLevelTransaction;

        public LowLevelTransaction LowLevelTransaction
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _lowLevelTransaction; }
        }

        public ByteStringContext Allocator => _lowLevelTransaction.Allocator;

        private Dictionary<Slice, Table> _tables;

        private Dictionary<Slice, Tree> _trees;

        private Dictionary<Slice, FixedSizeTree> _globalFixedSizeTree;

        public IEnumerable<Tree> Trees => _trees?.Values ?? Enumerable.Empty<Tree>();

        public IEnumerable<Table> Tables => _tables?.Values ?? Enumerable.Empty<Table>();

        public bool IsWriteTransaction => _lowLevelTransaction.Flags == TransactionFlags.ReadWrite;

        public Transaction(LowLevelTransaction lowLevelTransaction)
        {
            _lowLevelTransaction = lowLevelTransaction;
            _lowLevelTransaction.Transaction = this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureTrees()
        {
            if (_trees != null)
                return;
            _trees = new Dictionary<Slice, Tree>(SliceStructComparer.Instance);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public Tree ReadTree(string treeName, RootObjectType type = RootObjectType.VariableSizeTree, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            Slice.From(Allocator, treeName, ByteStringType.Immutable, out var treeNameSlice);
            return ReadTree(treeNameSlice, type, isIndexTree, newPageAllocator);
        }

        public Tree ReadTree(Slice treeName, RootObjectType type = RootObjectType.VariableSizeTree, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            EnsureTrees();

            if (_trees.TryGetValue(treeName, out var tree))
            {
                if (tree == null)
                    return null;

                if (newPageAllocator == null)
                    return tree;

                if (tree.HasNewPageAllocator == false)
                    tree.SetNewPageAllocator(newPageAllocator);

                return tree;
            }

            var header = (TreeRootHeader*)_lowLevelTransaction.RootObjects.DirectRead(treeName);
            if (header != null)
            {
                if (header->RootObjectType != type)
                    ThrowInvalidTreeType(treeName, type, header);

                tree = Tree.Open(_lowLevelTransaction, this, treeName, header, type, isIndexTree, newPageAllocator);

                _trees.Add(treeName, tree);

                return tree;
            }

            _trees.Add(treeName, null);
            return null;
        }

        private static void ThrowInvalidTreeType(Slice treeName, RootObjectType type, TreeRootHeader* header)
        {
            throw new InvalidOperationException($"Tried to open {treeName} as a {type}, but it is actually a " +
                                                header->RootObjectType);
        }

        public void Commit()
        {
            if (_lowLevelTransaction.Flags != TransactionFlags.ReadWrite)
                return; // nothing to do

            PrepareForCommit();
            _lowLevelTransaction.Commit();
        }

        public Transaction BeginAsyncCommitAndStartNewTransaction(TransactionPersistentContext persistentContext)
        {
            if (_lowLevelTransaction.Flags != TransactionFlags.ReadWrite)
                ThrowInvalidAsyncCommitOnRead();

            PrepareForCommit();
            var tx = _lowLevelTransaction.BeginAsyncCommitAndStartNewTransaction(persistentContext);
            return new Transaction(tx);
        }

        private static void ThrowInvalidAsyncCommitOnRead()
        {
            throw new InvalidOperationException("Cannot call begin async commit on read tx");
        }

        public void EndAsyncCommit()
        {
            _lowLevelTransaction.EndAsyncCommit();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public Table OpenTable(TableSchema schema, string name)
        {
            Slice nameSlice;
            using (Slice.From(Allocator, name, ByteStringType.Immutable, out nameSlice))
            {
                return OpenTable(schema, nameSlice);
            }
        }

        public Table OpenTable(TableSchema schema, Slice name)
        {
            _tables ??= new Dictionary<Slice, Table>(SliceStructComparer.Instance);

            if (_tables.TryGetValue(name, out Table value))
                return value;

            var clonedName = name.Clone(Allocator);

            var tableTree = ReadTree(clonedName, RootObjectType.Table);

            if (tableTree == null)
                return null;


            value = new Table(schema, clonedName, this, tableTree, schema.TableType);
            _tables[clonedName] = value;
            return value;
        }

        internal void PrepareForCommit()
        {
            if (_multiValueTrees != null)
            {
                foreach (var multiValueTree in _multiValueTrees)
                {
                    var parentTree = multiValueTree.Key.Item1;
                    var key = multiValueTree.Key.Item2;
                    var childTree = multiValueTree.Value;

                    byte* ptr;
                    using (parentTree.DirectAdd(key, sizeof(TreeRootHeader), TreeNodeFlags.MultiValuePageRef, out ptr))
                        childTree.State.CopyTo((TreeRootHeader*)ptr);
                }
            }

            foreach (var tree in Trees)
            {
                if (tree == null)
                    continue;

                var treeState = tree.State;
                if (treeState.IsModified)
                {
                    byte* ptr;
                    using (_lowLevelTransaction.RootObjects.DirectAdd(tree.Name, sizeof(TreeRootHeader), out ptr))
                        treeState.CopyTo((TreeRootHeader*)ptr);
                }
            }

            if (_tables != null)
            {
                foreach (var participant in _tables.Values)
                {
                    participant.PrepareForCommit();
                }
            }
        }

        internal void AddMultiValueTree(Tree tree, Slice key, Tree mvTree)
        {
            if (_multiValueTrees == null)
                _multiValueTrees = new Dictionary<Tuple<Tree, Slice>, Tree>(new TreeAndSliceComparer());
            mvTree.IsMultiValueTree = true;
            _multiValueTrees.Add(Tuple.Create(tree, key.Clone(_lowLevelTransaction.Allocator, ByteStringType.Immutable)), mvTree);
        }

        internal bool TryGetMultiValueTree(Tree tree, Slice key, out Tree mvTree)
        {
            mvTree = null;
            if (_multiValueTrees == null)
                return false;
            return _multiValueTrees.TryGetValue(Tuple.Create(tree, key), out mvTree);
        }

        internal bool TryRemoveMultiValueTree(Tree parentTree, Slice key)
        {
            var keyToRemove = Tuple.Create(parentTree, key);
            if (_multiValueTrees == null || !_multiValueTrees.ContainsKey(keyToRemove))
                return false;

            return _multiValueTrees.Remove(keyToRemove);
        }


        [MethodImpl(MethodImplOptions.NoInlining)]
        internal void AddTree(string name, Tree tree)
        {
            Slice treeName;
            Slice.From(Allocator, name, ByteStringType.Immutable, out treeName);
            AddTree(treeName, tree);
        }

        internal void AddTree(Slice name, Tree tree)
        {
            EnsureTrees();

            if (_trees.TryGetValue(name, out Tree value) && value != null)
            {
                throw new InvalidOperationException("Tree already exists: " + name);
            }

            // Either we haven't added this tree, or we added it as null (meaning it didn't exist)
            _trees[name] = tree;
        }


        [MethodImpl(MethodImplOptions.NoInlining)]
        public void DeleteTree(string name)
        {
            Slice nameSlice;
            Slice.From(Allocator, name, ByteStringType.Immutable, out nameSlice);
            DeleteTree(nameSlice);
        }

        public void DeleteFixedTree(string name)
        {
            var tree = FixedTreeFor(name);

            DeleteFixedTree(tree);
        }

        public void DeleteFixedTree(FixedSizeTree tree, bool isInRoot = true)
        {
            while (true)
            {
                using (var it = tree.Iterate())
                {
                    if (it.Seek(long.MinValue) == false)
                        break;

                    var currentKey = it.CurrentKey;
                    tree.Delete(currentKey);
                }
            }

            if (isInRoot)
                _lowLevelTransaction.RootObjects.Delete(tree.Name);
        }


        public void DeleteTree(Slice name)
        {
            if (_lowLevelTransaction.Flags == TransactionFlags.ReadWrite == false)
                throw new ArgumentException("Cannot create a new newRootTree with a read only transaction");

            Tree tree = ReadTree(name);
            if (tree == null)
                return;

            DeleteTree(tree);

            if (_multiValueTrees != null)
            {
                var toRemove = new List<Tuple<Tree, Slice>>();

                foreach (var valueTree in _multiValueTrees)
                {
                    var multiTree = valueTree.Key.Item1;

                    if (SliceComparer.Equals(multiTree.Name, name))
                    {
                        toRemove.Add(valueTree.Key);
                    }
                }

                foreach (var recordToRemove in toRemove)
                {
                    _multiValueTrees.Remove(recordToRemove);
                }
            }
            // already created in ReadTree
            _trees.Remove(name);
        }

        private void DeleteTree(Tree tree, bool isInRoot = true)
        {
            foreach (var page in tree.AllPages())
            {
                _lowLevelTransaction.FreePage(page);
            }

            if (isInRoot)
                _lowLevelTransaction.RootObjects.Delete(tree.Name);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void RenameTree(string fromName, string toName)
        {
            Slice.From(Allocator, fromName, ByteStringType.Immutable, out Slice fromNameSlice);
            Slice.From(Allocator, toName, ByteStringType.Immutable, out Slice toNameSlice);
            RenameTree(fromNameSlice, toNameSlice);
        }

        public void RenameTree(Slice fromName, Slice toName)
        {
            if (_lowLevelTransaction.Flags == TransactionFlags.ReadWrite == false)
                throw new ArgumentException("Cannot rename a new tree with a read only transaction");

            if (SliceComparer.Equals(toName, Constants.RootTreeNameSlice))
                throw new InvalidOperationException("Cannot create a tree with reserved name: " + toName);

            if (ReadTree(toName) != null)
                throw new ArgumentException("Cannot rename a tree with the name of an existing tree: " + toName);

            Tree fromTree = ReadTree(fromName);
            if (fromTree == null)
                throw new ArgumentException("Tree " + fromName + " does not exists");

            _lowLevelTransaction.RootObjects.Delete(fromName);

            byte* ptr;
            using (_lowLevelTransaction.RootObjects.DirectAdd(toName, sizeof(TreeRootHeader), out ptr))
                fromTree.State.CopyTo((TreeRootHeader*)ptr);

            fromTree.Rename(toName);
            fromTree.State.IsModified = true;

            // _trees already ensrued already created in ReadTree
            _trees.Remove(fromName);
            _trees.Remove(toName);

            AddTree(toName, fromTree);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public Tree CreateTree(string name, RootObjectType type = RootObjectType.VariableSizeTree, TreeFlags flags = TreeFlags.None, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            Slice.From(Allocator, name, ByteStringType.Immutable, out var treeNameSlice);
            return CreateTree(treeNameSlice, type, flags, isIndexTree, newPageAllocator);
        }

        public Tree CreateTree(Slice name, RootObjectType type = RootObjectType.VariableSizeTree, TreeFlags flags = TreeFlags.None, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            Tree tree = ReadTree(name, type, isIndexTree, newPageAllocator);
            if (tree != null)
                return tree;

            if (_lowLevelTransaction.Flags == TransactionFlags.ReadWrite == false)
                throw new InvalidOperationException("No such tree: '" + name +
                                                    "' and cannot create trees in read transactions");

            tree = Tree.Create(_lowLevelTransaction, this, name, flags, type, isIndexTree, newPageAllocator);
            tree.State.RootObjectType = type;

            using (_lowLevelTransaction.RootObjects.DirectAdd(name, sizeof(TreeRootHeader), out byte* ptr))
                tree.State.CopyTo((TreeRootHeader*)ptr);

            tree.State.IsModified = true;
            AddTree(name, tree);

            return tree;
        }
        
        public void Dispose()
        {
            if (_trees != null)
            {
                foreach (var tree in _trees)
                {
                    tree.Value?.Dispose();
                }
            }

            if (_multiValueTrees != null)
            {
                foreach (var item in _multiValueTrees)
                {
                    item.Value?.Dispose();
                    item.Key.Item1?.Dispose();
                }
            }

            if (_tables != null)
            {
                foreach (var table in _tables)
                {
                    table.Value?.Dispose();
                }
            }

            if (_globalFixedSizeTree != null)
            {
                foreach (var tree in _globalFixedSizeTree)
                {
                    tree.Value?.Dispose();
                }
            }

            _lowLevelTransaction?.Dispose();

            _lowLevelTransaction = null;
        }

        public FixedSizeTree FixedTreeFor(string treeName)
        {
            Slice.From(Allocator, treeName, ByteStringType.Immutable, out var treeNameSlice);
            return FixedTreeFor(treeNameSlice);
        }

        public FixedSizeTree FixedTreeFor(string treeName, ushort valSize)
        {
            Slice.From(Allocator, treeName, ByteStringType.Immutable, out var treeNameSlice);
            return FixedTreeFor(treeNameSlice, valSize);
        }

        public FixedSizeTree FixedTreeFor(Slice treeName)
        {
            var valueSize = FixedSizeTree.GetValueSize(LowLevelTransaction, LowLevelTransaction.RootObjects, treeName);
            return FixedTreeFor(treeName, valueSize);
        }

        public FixedSizeTree FixedTreeFor(Slice treeName, ushort valSize)
        {
            return new FixedSizeTree(LowLevelTransaction, LowLevelTransaction.RootObjects, treeName, valSize);
        }

        public RootObjectType GetRootObjectType(Slice name)
        {
            var val = (RootHeader*)_lowLevelTransaction.RootObjects.DirectRead(name);
            if (val == null)
                return RootObjectType.None;

            return val->RootObjectType;
        }

        public FixedSizeTree GetGlobalFixedSizeTree(Slice name, ushort valSize, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            if (_globalFixedSizeTree == null)
                _globalFixedSizeTree = new Dictionary<Slice, FixedSizeTree>(SliceStructComparer.Instance);

            FixedSizeTree tree;
            if (_globalFixedSizeTree.TryGetValue(name, out tree) == false)
            {
                tree = new FixedSizeTree(LowLevelTransaction, LowLevelTransaction.RootObjects, name, valSize, isIndexTree: isIndexTree,
                    newPageAllocator: newPageAllocator);
                _globalFixedSizeTree[tree.Name] = tree;
            }
            else if (newPageAllocator != null && tree.HasNewPageAllocator == false)
                tree.SetNewPageAllocator(newPageAllocator);

            return tree;
        }

        [Conditional("DEBUG")]
        public static void DebugDisposeReaderAfterTransaction(Transaction tx, BlittableJsonReaderObject reader)
        {
            if (reader == null)
                return;
            Debug.Assert(tx != null);
            // this method is called to ensure that after the transaction is completed, all the readers are disposed
            // so we won't have read-after-tx use scenario, which can in rare case corrupt memory. This is a debug
            // helper that is used across the board, but it is meant to assert stuff during debug only
            tx.LowLevelTransaction.OnDispose += state => reader.Dispose();
        }

        public void DeleteTable(string name)
        {
            var tableTree = ReadTree(name, RootObjectType.Table);
            
            var writtenSchemaData = tableTree.DirectRead(TableSchema.SchemasSlice);
            var writtenSchemaDataSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
            var schema = TableSchema.ReadFrom(Allocator, writtenSchemaData, writtenSchemaDataSize);

            var table = OpenTable(schema, name);

            // delete table data

            table.DeleteByPrimaryKey(Slices.BeforeAllKeys, x =>
            {
                if (schema.Key.IsGlobal)
                {
                    return table.IsOwned(x.Reader.Id);
                }

                return true;
            });

            if (schema.Key.IsGlobal == false)
            {
                var pkTree = table.GetTree(schema.Key);

                DeleteTree(pkTree, isInRoot: false);

                tableTree.Delete(pkTree.Name);
            }

            // index trees should be already removed but just in case let's go over them and ensure they're really deleted

            foreach (var indexDef in schema.Indexes.Values)
            {
                if (indexDef.IsGlobal) // must not delete global indexes
                    continue;

                if (tableTree.Read(indexDef.Name) == null)
                    continue;

                var indexTree = table.GetTree(indexDef);

                DeleteTree(indexTree, isInRoot: false);

                tableTree.Delete(indexTree.Name);
            }

            foreach (var indexDef in schema.FixedSizeIndexes.Values)
            {
                if (indexDef.IsGlobal)  // must not delete global indexes
                    continue;

                if (tableTree.Read(indexDef.Name) == null)
                    continue;

                var index = table.GetFixedSizeTree(indexDef);

                DeleteFixedTree(index, isInRoot: false);

                tableTree.Delete(index.Name);
            }

            // raw data sections

            table.ActiveDataSmallSection.FreeRawDataSectionPages();

            if (tableTree.Read(TableSchema.ActiveCandidateSectionSlice) != null)
            {
                using (var it = table.ActiveCandidateSection.Iterate())
                {
                    if (it.Seek(long.MinValue))
                    {
                        var sectionPageNumber = it.CurrentKey;
                        var section = new ActiveRawDataSmallSection(this, sectionPageNumber);

                        section.FreeRawDataSectionPages();
                    }
                }

                DeleteFixedTree(table.ActiveCandidateSection, isInRoot: false);
            }

            if (tableTree.Read(TableSchema.InactiveSectionSlice) != null)
                DeleteFixedTree(table.InactiveSections, isInRoot: false);

            DeleteTree(name);

            using (Slice.From(Allocator, name, ByteStringType.Immutable, out var nameSlice))
            {
                _tables.Remove(nameSlice);
            }
        }

        public void ForgetAbout(in long storageId)
        {
            if (_cachedDecompressedBuffersByStorageId == null)
                return;

            if (_cachedDecompressedBuffersByStorageId.Remove(storageId, out var t))
                Allocator.Release(ref t);
        }
    }
}
