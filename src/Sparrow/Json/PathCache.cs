using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Sparrow.Json
{
    public class PathCache
    {
        private int _used = -1;
        private readonly ObjectPathCache[] _items = new ObjectPathCache[512];


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AcquirePathCache(out ObjectPathCache pathCache)
        {
            // PERF: Avoids allocating gigabytes in FastDictionary instances on high traffic RW operations like indexing.
            if (_used >= 0)
            {
                var cache = _items[_used--];
                Debug.Assert(cache != null);

                pathCache = cache;
                return;
            }
            pathCache = new ObjectPathCache(256); 
        }

        public void ReleasePathCache(ObjectPathCache pathCache)
        {
            if (_used >= _items.Length - 1) return;

            pathCache.Reset();

            _items[++_used] = pathCache;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ClearUnreturnedPathCache()
        {
            for (var i = _used + 1; i < _items.Length; i++)
            {
                var cache = _items[i];

                //never allocated, no reason to continue seeking
                if (cache == null)
                    break;

                //idly there shouldn't be unreleased path cache but we do have placed where we don't dispose of blittable object readers
                //and rely on the context.Reset to clear unwanted memory, but it didn't take care of the path cache.

                //Clear references for allocated cache paths so the GC can collect them.
                cache.Reset();
            }
        }
    }
}
