using System.Collections;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Test;

public class TestIndexItemEnumerator : IIndexedItemEnumerator
{
    private readonly IIndexedItemEnumerator _inner;
    private int _count;
    private readonly int _maxOutputsPerCollection;

    public TestIndexItemEnumerator(IIndexedItemEnumerator inner, int collections, int maxDocumentsPerIndex)
    {
        _maxOutputsPerCollection = maxDocumentsPerIndex / collections;
        _inner = inner;
        _count = 0;
    }
        
    public void Dispose() => _inner.Dispose();
        
    public bool MoveNext(DocumentsOperationContext ctx, out IEnumerable resultsOfCurrentDocument, out long? etag)
    {
        if (_count < _maxOutputsPerCollection)
        {
            _count += 1;
            return _inner.MoveNext(ctx, out resultsOfCurrentDocument, out etag);
        }
            
        resultsOfCurrentDocument = default;
        etag = null;
            
        return false;
    }

    public void OnError()
    {
        _inner.OnError();
    }

    public IndexItem Current => _inner.Current;
}
