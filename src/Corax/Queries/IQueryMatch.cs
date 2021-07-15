using System;

namespace Corax.Queries
{
    public static class QueryMatch
    {
        public const long Invalid = -1;
        public const long Start = 0;
    }

    [Flags]
    public enum QueryMatchStatus
    {
        NoMore = 0,
        InOrder = 1,
        NotInOrder = 2
    }

    public interface IQueryMatch
    {
        long Count { get; }
        
        long Current { get; }

        bool SeekTo(long next = 0);
        QueryMatchStatus MoveNext(out long v);

        // TODO: Don't know if 'MoveNext' is the right name for this method. For now I will leave it as-is in order to
        // see how the caller code feels like. 
        QueryMatchStatus MoveNext(Span<long> buffer, out int read);
    }
}
