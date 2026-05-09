using System.Runtime.CompilerServices;
using Sparrow.Collections;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Xunit;

namespace FastTests.Voron.Graphs;

public class HnswRingBuild(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public unsafe void InsertTaskFitsInRingCell()
    {
        // The LockFreeRingBuffer<T> static constructor throws if sizeof(T) > 64 B.
        // Constructing a ring of InsertTask exercises that contract; if InsertTask
        // ever grows past one cache line this test fails before any HNSW code runs.
        Assert.True(Unsafe.SizeOf<Hnsw.InsertTask>() <= 64,
            $"InsertTask is {Unsafe.SizeOf<Hnsw.InsertTask>()} B; ring cell budget is 64 B.");

        Hnsw.InsertTask task = new()
        {
            CurrentNodeIndex = 7,
            Level = 3,
            NodeId = 0x1234_5678_9ABC_DEF0L,
            EdgesPerLevelOffset = 1024,
            EdgesIndexesOffset = 2048
        };

        LockFreeRingBuffer<Hnsw.InsertTask> ring = new(capacity: 8);

        Assert.True(ring.TryEnqueue(task));
        Assert.True(ring.TryDequeue(out Hnsw.InsertTask roundTripped));

        Assert.Equal(task.CurrentNodeIndex, roundTripped.CurrentNodeIndex);
        Assert.Equal(task.Level, roundTripped.Level);
        Assert.Equal(task.NodeId, roundTripped.NodeId);
        Assert.Equal(task.EdgesPerLevelOffset, roundTripped.EdgesPerLevelOffset);
        Assert.Equal(task.EdgesIndexesOffset, roundTripped.EdgesIndexesOffset);
        Assert.True(ring.IsEmpty);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void RingPreservesFifoUnderPowerOfTwoCapacity()
    {
        const int capacity = 16;
        LockFreeRingBuffer<Hnsw.InsertTask> ring = new(capacity);

        for (int i = 0; i < capacity; i++)
            Assert.True(ring.TryEnqueue(new Hnsw.InsertTask { CurrentNodeIndex = i, Level = i & 3, NodeId = i * 17L }));

        Assert.True(ring.IsFull);
        Assert.False(ring.TryEnqueue(default));

        for (int i = 0; i < capacity; i++)
        {
            Assert.True(ring.TryDequeue(out Hnsw.InsertTask t));
            Assert.Equal(i, t.CurrentNodeIndex);
            Assert.Equal(i & 3, t.Level);
            Assert.Equal(i * 17L, t.NodeId);
        }

        Assert.True(ring.IsEmpty);
        Assert.False(ring.TryDequeue(out _));
    }
}
