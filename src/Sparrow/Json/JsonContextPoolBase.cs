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

        // Dual-buffer architecture for background renewal
        private readonly LockFreeRingBuffer<T> _readyBuffer;
        private readonly LockFreeRingBuffer<T> _shadowBuffer;
        
        // Background renewer thread infrastructure
        private readonly ManualResetEventSlim _workAvailable = new ManualResetEventSlim(false);
        private readonly Thread _renewerThread;
        private volatile bool _shutdownRequested;

        protected JsonContextPoolBase(IRavenLogger logger)
        {
            _logger = logger;
            
            // Initialize dual buffers with core-based capacity scaling
            int baseCapacityPerCore = 4 * 1024; // 4K contexts per core
            int perBufferCapacity = Math.Max(8, ProcessorInfo.ProcessorCount * baseCapacityPerCore);
            
            int readyCapacity = perBufferCapacity;
            int shadowCapacity = perBufferCapacity; // Same size as ready buffer since we can reclaim from shadow too
            
            _readyBuffer = new LockFreeRingBuffer<T>(readyCapacity);
            _shadowBuffer = new LockFreeRingBuffer<T>(shadowCapacity);
            
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
            _maxContextSizeToKeepInBytes = long.MaxValue;
            
            // Start the background renewer thread
            _renewerThread = new Thread(RenewLoop)
            {
                Name = "JsonContextPool.Renewer",
                IsBackground = true
            };
            _renewerThread.Start();
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
            // we are expecting to be called here when there is no more work to be done, and we want to release resources
            // to the system

            // Drain and dispose contexts from shadow buffer, we do this one first because we want to ensure we do not
            // create a race condition with the renewer thread to put more in the ready buffer after we finished on it already.
            while (_shadowBuffer.TryDequeue(out var context))
            {
                if (context.InUse.Raise())
                    context.Dispose();
            }
            
            // Drain and dispose contexts from ready buffer
            while (_readyBuffer.TryDequeue(out var context))
            {
                if (context.InUse.Raise())
                    context.Dispose();
            }
        }

        public IDisposable AllocateOperationContext(out T context)
        {
            _cts.Token.ThrowIfCancellationRequested();

            // Fast path: Try to get a pre-renewed context from ready buffer
            while (_readyBuffer.TryDequeue(out context))
            {
                if (context.InUse.Raise() == false)
                    continue;
                
                return new ReturnRequestContext
                {
                    Parent = this,
                    Context = context
                };
            }

            // Second chance: Steal from shadow buffer and renew inline (bounded latency)
            while (_shadowBuffer.TryDequeue(out context))
            {
                if (context.InUse.Raise() == false)
                    continue;
                
                // Pay the renewal cost now to maintain bounded latency
                context.Renew();
                
                return new ReturnRequestContext
                {
                    Parent = this,
                    Context = context
                };
            }

            // Cold path: Create a brand new context
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

                parent.EnqueueForRenew(Context);

                Context = null;
            }
        }
        
        private void EnqueueForRenew(T context)
        {
            if (LowMemoryFlag.IsRaised())
            {
                context.Dispose();
                return;
            }

            // Try to enqueue the context to shadow buffer for background renewal
            if (_shadowBuffer.TryEnqueue(context))
            {
                _workAvailable.Set();
                return;
            }

            // Shadow buffer is full - do inline renewal and push directly to ready buffer
            context.Renew();
            if (_readyBuffer.TryEnqueue(context) == false)
                context.Dispose();
        }

        private void RenewLoop()
        {
            // Main renewer thread loop - continues until shutdown is requested
            while (_shutdownRequested == false)
            {
                try
                {
                    // Check if cancellation was requested (dispose, shutdown, etc.)
                    if (_cts.Token.IsCancellationRequested)
                        break;
                        
                    // Try to get work from shadow buffer (contexts waiting for renewal)
                    if (_shadowBuffer.TryDequeue(out var context) == false)
                    {
                        // No work available - reset event and wait for signal or timeout
                        _workAvailable.Reset();
                        _workAvailable.Wait(100, _cts.Token);
                        continue;
                    }
                    
                    // Check if context should be disposed instead of renewed:
                    // - DoNotReuse: context marked as corrupted/problematic
                    // - Generation fence: context is from before low memory event
                    if (context.DoNotReuse || (LowMemoryFlag.IsRaised() && context.PoolGeneration < _generation))
                    {
                        context.Dispose();
                        continue;
                    }

                    // Perform the expensive renewal operation off the request hot path
                    context.Renew();
                    
                    // Try to enqueue renewed context to ready buffer for fast allocation
                    // If ready buffer is full, dispose context to prevent memory buildup
                    if (_readyBuffer.TryEnqueue(context) == false)
                        context.Dispose();
                }
                catch (OperationCanceledException)
                {
                    // Expected during shutdown - exit cleanly
                    break;
                }
                catch
                {
                    // Any other exception (renewal failure, disposal failure, etc.)
                    // Swallow and continue - this thread must never die
                    // Individual context failures should not kill the renewer
                }
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

                // Signal shutdown and cancel operations
                _shutdownRequested = true;
                _cts.Cancel();
                _disposed = true;
                
                // Wake up the renewer thread and wait for clean shutdown
                _workAvailable.Set();
                if (_renewerThread is { IsAlive: true })
                {
                    if (_renewerThread.Join((int)TimeSpan.FromSeconds(30).TotalMilliseconds) == false)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("JsonContextPool renewer thread did not shut down within timeout, continuing with disposal");
                    }
                }
                
                _workAvailable?.Dispose();

                // Now we drain the pool. 
                Clean();
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

            // Clear all contexts from both buffers during extremely low memory
            // The renewer thread will also honor the LowMemoryFlag and dispose old contexts
            while (_readyBuffer.TryDequeue(out var context))
            {
                if (context.InUse.Raise())
                    context.Dispose();
            }
            
            while (_shadowBuffer.TryDequeue(out var context))
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
