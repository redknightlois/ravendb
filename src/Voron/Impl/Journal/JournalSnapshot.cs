using System;
using System.Collections.Generic;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public sealed class JournalSnapshot : IComparable<JournalSnapshot>
    {
        public long Number;
        public long Available4Kbs;
        public JournalFile FileInstance;
        public long WritePosIn4KbPosition;
        public long LastTransaction;

        public int CompareTo(JournalSnapshot other)
        {
            return Number.CompareTo(other.Number);
        }
    }
}
