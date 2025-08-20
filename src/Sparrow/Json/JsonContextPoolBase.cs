using System;
using System.Diagnostics;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public abstract class JsonContextPoolBase<T> : ILowMemoryHandler, IMemoryContextPool
        where T : JsonOperationContext
    {
        private readonly IRavenLogger _logger;
        private readonly object _locker = new object();

        private bool _disposed;

        protected SharedMultipleUseFlag LowMemoryFlag = new SharedMultipleUseFlag();
        private readonly MultipleUseFlag _isExtremelyLowMemory = new MultipleUseFlag();
        private long _generation;

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly long _maxContextSizeToKeepInBytes;

        private readonly LockFreeRingBuffer<T> _contextBuffer = new LockFreeRingBuffer<T>(PlatformDetails.Is32Bits ? 4 * 1024 : 64 * 1024);
        private readonly Timer _cleanupTimer;

        protected JsonContextPoolBase(IRavenLogger logger)
        {
            _logger = logger;
            _cleanupTimer = new Timer(Cleanup, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
            _maxContextSizeToKeepInBytes = long.MaxValue;
        }

        protected JsonContextPoolBase(Size? maxContextSizeToKeep, IRavenLogger logger)
            : this(logger)
        {
            if (maxContextSizeToKeep.HasValue)
                _maxContextSizeToKeepInBytes = maxContextSizeToKeep.Value.GetValue(SizeUnit.Bytes);
        }


        public IDisposable AllocateOperationContext(out JsonOperationContext context)
        {
            var disposable = AllocateOperationContext(out T ctx);
            context = ctx;

            return disposable;
        }

        public void Clean()
        {
            // we are expecting to be called here when there is no
            // more work to be done, and we want to release resources
            // to the system

            // Drain the buffer and dispose all contexts
            while (_contextBuffer.TryDequeue(out var context))
            {
                context.Dispose();
            }
        }

        public IDisposable AllocateOperationContext(out T context)
        {
            _cts.Token.ThrowIfCancellationRequested();

            // Try to get a context from the ring buffer
            while (_contextBuffer.TryDequeue(out context))
            {
                if (context.InUse.Raise() == false)
                    continue;
                // This what ensures that we work correctly with races from other threads
                // if there is a context switch at the wrong time
                context.Renew();
                return new ReturnRequestContext
                {
                    Parent = this,
                    Context = context
                };
            }

            // no choice, got to create it
            context = CreateContext();
            context.PoolGeneration = _generation;
            return new ReturnRequestContext
            {
                Parent = this,
                Context = context
            };
        }

        protected abstract T CreateContext();

        private sealed class ReturnRequestContext : IDisposable
        {
            public T Context;
            public JsonContextPoolBase<T> Parent;

            public void Dispose()
            {
                var parent = Parent;
                if (parent == null)
                    return; // disposed already

                Parent = null;

                if (Context.DoNotReuse)
                {
                    Context.Dispose();
                    return;
                }

                if (Context.AllocatedMemory > parent._maxContextSizeToKeepInBytes)
                {
                    Context.Dispose();
                    return;
                }

                if (parent.LowMemoryFlag.IsRaised() && Context.PoolGeneration < parent._generation)
                {
                    // releasing all the contexts which were created before we got the low memory event
                    Context.Dispose();
                    return;
                }

                Context.Reset();
                // These contexts are reused, so we don't want to use LowerOrDie here.
                Context.InUse.Lower();
                Context.InPoolSince = DateTime.UtcNow;

                parent.Push(Context);

                Context = null;
            }
        }


        private void Cleanup(object _)
        {
            if (Monitor.TryEnter(_locker) == false)
                return;

            try
            {
                var currentTime = DateTime.UtcNow;
                var idleTime = TimeSpan.FromMinutes(5);
                
                // Get count estimate for the ring buffer to avoid infinite loops
                var itemsToCheck = _contextBuffer.Count;

                // Age-based cleanup using TryDequeue/TryEnqueue pattern
                for (int i = 0; i < itemsToCheck; i++)
                {
                    if (_contextBuffer.TryDequeue(out var context) == false)
                        break; // Buffer is empty, no need to continue. 
                    
                    // If context is old or the buffer is full, we will dispose. 
                    var timeInPool = currentTime - context.InPoolSince;
                    if (timeInPool > idleTime || _contextBuffer.TryEnqueue(context) == false)
                        context.Dispose();
                }
            }
            catch (Exception e)
            {
                Debug.Assert(e is OutOfMemoryException, $"Expecting OutOfMemoryException but got: {e}");
                if (_logger.IsErrorEnabled)
                    _logger.Error("Error during cleanup.", e);
            }
            finally
            {
                Monitor.Exit(_locker);
            }
        }

        private void Push(T context)
        {
            if (LowMemoryFlag.IsRaised())
            {
                context.Dispose();
                return;
            }

            // Try to enqueue the context back into the ring buffer
            if (_contextBuffer.TryEnqueue(context) == false)
            {
                // Ring buffer is full, dispose the context
                context.Dispose();
            }
        }

        public virtual void Dispose()
        {
            if (_disposed)
                return;

            lock (_locker)
            {
                if (_disposed)
                    return;

                _cts.Cancel();
                _disposed = true;
                _cleanupTimer.Dispose();

                // Clear all contexts from the ring buffer
                while (_contextBuffer.TryDequeue(out var context))
                {
                    context.Dispose();
                }
            }
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            if (LowMemoryFlag.Raise())
            {
                Interlocked.Increment(ref _generation);
            }

            if (lowMemorySeverity != LowMemorySeverity.ExtremelyLow)
                return;

            if (_isExtremelyLowMemory.Raise() == false)
                return;

            // Clear all contexts from the ring buffer during extremely low memory
            while (_contextBuffer.TryDequeue(out var context))
            {
                if (context.InUse.Raise())
                    context.Dispose();
            }
        }

        public void LowMemoryOver()
        {
            LowMemoryFlag.Lower();
            _isExtremelyLowMemory.Lower();
        }
    }
}
