using System.Runtime.InteropServices;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    /// <summary>
    /// Per-vector unit of work for the ring-based parallel builder. Producer (LLT) populates
    /// these into a <see cref="Sparrow.Collections.LockFreeRingBuffer{T}"/>; workers claim a
    /// task, run the full insertion pipeline (search + neighbor selection + edge writes),
    /// then signal completion. Layout is constrained to fit within a single cache line so the
    /// ring's wait-free contract holds (the ring's static check enforces sizeof &lt;= 64 B).
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct InsertTask
    {
        public int CurrentNodeIndex;
        public int Level;
        public long NodeId;

        public long EdgesPerLevelOffset;
        public long EdgesIndexesOffset;
    }
}
