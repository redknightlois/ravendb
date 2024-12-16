using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Threading;

namespace Sparrow.Utils
{

    public sealed class NativeMemoryCleaner<TPooledItem> : IDisposable
        where TPooledItem : PooledItem
    {
        private static readonly IRavenLogger Logger = RavenLogManager.Instance.GetLoggerForSparrow(typeof(NativeMemoryCleaner<TPooledItem>));

        private readonly LockFreeRingBuffer<TPooledItem> _ringBuffer;
        private readonly SharedMultipleUseFlag _lowMemoryFlag;
        private readonly TimeSpan _idleTime;
        private readonly Timer _timer;

        private bool _disposed;

        public NativeMemoryCleaner(LockFreeRingBuffer<TPooledItem> ringBuffer, SharedMultipleUseFlag lowMemoryFlag, TimeSpan period, TimeSpan idleTime)
        {
            _ringBuffer = ringBuffer ?? throw new ArgumentNullException(nameof(ringBuffer));
            _lowMemoryFlag = lowMemoryFlag ?? throw new ArgumentNullException(nameof(lowMemoryFlag));
            _idleTime = idleTime;
            _timer = new Timer(CleanNativeMemory, null, period, period);
        }

        private const int MinItemsInQueue = 2;

        /// <summary>
        /// Called periodically by the timer to clean up old or unneeded items.
        /// This method no longer takes a global lock, as the ring buffer is thread-safe.
        /// We aggressively and optimistically dequeue items, check their conditions, and either dispose or re-enqueue them.
        /// </summary>
        public void CleanNativeMemory(object state)
        {
            if (_disposed)
                return;

            try
            {

                int maxIterations = _ringBuffer.Count;
                if (maxIterations <= MinItemsInQueue)
                    return; // We effectively have nothing to do, we want to still have at least 2 items in there. 

                DateTime now = DateTime.UtcNow;
                for (int i = 0; i < maxIterations; i++)
                {
                    if (_ringBuffer.TryDequeue(out var item) == false)
                        continue;

                    if (item == null)
                        continue;

                    Debug.Assert(item.InUse.IsRaised() == false, "Item should not be in use when in the pool.");

                    // TODO: Since this is a global ring buffer, time in pool is always going to be somewhat low; recheck this assumption with data.
                    var timeInPool = now - item.InPoolSince;
                    bool shouldDispose = _lowMemoryFlag.IsRaised() || timeInPool >= _idleTime;
                    if (shouldDispose == false)
                    {
                        // Item is still fresh and we are not under memory pressure.
                        // Attempt to return item back to the pool for reuse.
                        if (_ringBuffer.TryEnqueue(item) == false)
                        {
                            // Fallback mechanism: if the pool is full, we have no choice but to dispose the item
                            // to avoid memory leaks and uncontrolled growth.
                            item.Dispose();
                        }

                        if (_ringBuffer.Count <= MinItemsInQueue)
                            break;

                        continue;
                    }

                    // Under memory pressure or item is too old, we will attempt to dispose.
                    // but need to protect from races if the owner thread will just pick it up
                    if (!item.InUse.Raise())
                        continue;

                    try
                    {
                        item.Dispose();
                    }
                    catch (ObjectDisposedException)
                    {
                        // it is possible that this has already been disposed
                    }
                }
            }
            catch (Exception e)
            {
                Debug.Assert(e is OutOfMemoryException, $"Expecting OutOfMemoryException but got: {e}");
                if (Logger.IsErrorEnabled)
                    Logger.Error("Error during cleanup.", e);
            }
        }

        public void Dispose()
        {
#if !NETSTANDARD1_3
            using (var waitHandle = new ManualResetEvent(false))
            {
                _disposed = true;
                if (_timer.Dispose(waitHandle))
                {
                    waitHandle.WaitOne();
                }
            }
#else
            lock (_lock) // prevent from running the callback _after_ dispose
            {
                _disposed = true;
                _timer.Dispose();
            }
#endif
        }
    }
}
