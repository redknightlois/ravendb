using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
    public unsafe struct MultiMatch : IQueryMatch
    {
        public long Count => throw new NotImplementedException();

        public long Current => throw new NotImplementedException();

        public bool MoveNext(out long v)
        {
            throw new NotImplementedException();
        }

        public bool SeekTo(long next = 0)
        {
            throw new NotImplementedException();
        }

        public static MultiMatch CreateEmpty()
        {
            throw new NotImplementedException();
        }

        public bool MoveNext(Span<long> buffer, out int read)
        {
            throw new NotImplementedException();
        }
    }
}
