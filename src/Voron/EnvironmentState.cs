using System.Collections.Frozen;
using System.Collections.Generic;
using Voron.Impl.Paging;

namespace Voron;

public class EnvironmentState
{
    public Pager2.State DataDate;
    public List<Pager2.State> ScratchesState = [];
    public FrozenDictionary<long, Page> ScratchPagesTable;
}
