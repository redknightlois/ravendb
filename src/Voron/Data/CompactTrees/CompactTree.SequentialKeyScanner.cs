﻿using System;
using Sparrow;

namespace Voron.Data.CompactTrees;



unsafe partial class CompactTree
{
    // The sequential key scanner would scan every single key available in the tree in sequential order.
    // It is important to note that given we are returning a read only reference, and we cannot know what
    // the caller is gonna do, the keys must survive until struct is collected. 
    public struct SequentialKeyScanner : IReadOnlySpanEnumerator
    {
        private readonly CompactTree _tree;
        private ForwardIterator _forwardIterator;

        public SequentialKeyScanner(CompactTree tree)
        {
            _tree = tree;
            _forwardIterator = _tree.Iterate<ForwardIterator>();
            _forwardIterator.Reset();
        }

        public bool MoveNext(out ReadOnlySpan<byte> key)
        {
            // Obtain the new key and return the corresponding span. 
            bool operationResult = _forwardIterator.MoveNext(out var scope, out long _);
            key = operationResult ? scope.Key.Decoded() : ReadOnlySpan<byte>.Empty;
            scope.Dispose();

            // We won't dispose the scope. 
            return operationResult;
        }

        public void Reset()
        {
            _forwardIterator = _tree.Iterate<ForwardIterator>();
            _forwardIterator.Reset();
        }
    }
}
