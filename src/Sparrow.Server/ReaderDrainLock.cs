using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Sparrow.Server
{
    /// <summary>
    /// Many-reader / rare-writer lock specialized for the "drain readers
    /// before maintenance" contract. Optimized for an uncontended reader
    /// hot path: one volatile read plus one CAS, zero allocations, and the
    /// returned read handle is a stack-allocated struct.
    ///
    /// Contract:
    ///   - Readers are non-exclusive among themselves.
    ///   - A pending or active writer blocks new readers (writer cannot starve).
    ///   - Writers are serialized; a writer must wait until the active reader
    ///     count drains to zero before entering.
    ///   - Reentrancy is not supported. Callers that need reentry must track
    ///     it externally (e.g. an AsyncLocal flag).
    ///
    /// State layout (single 64-bit word, manipulated atomically):
    ///   bit 63       WriterPending - set while any writer is waiting or
    ///                holding the lock; blocks new readers.
    ///   bits 0..31   active reader count.
    ///
    /// Reader fast path uses an unconditional <see cref="Interlocked.Add"/>
    /// followed by an inspect-and-undo if the writer-pending bit is set in
    /// the post-add state. This bounds the reader hot path at one atomic
    /// op per acquire even under extreme contention - a CAS-retry loop
    /// would devolve into a cache-line storm at high reader counts because
    /// readers would force each other to retry without any actual conflict.
    /// The invariant preserved is "while a writer holds the lock, no
    /// reader executes work against the protected resource"; the reader
    /// counter can briefly bump above zero during an aborted acquire, but
    /// the aborting reader decrements before doing any protected work, so
    /// the writer's exclusivity is intact. The writer's drain decision is
    /// anchored on the count snapshot that <see cref="Interlocked.Or"/>
    /// observes when publishing the writer-pending flag, so the writer
    /// never depends on the transient bumps either.
    /// </summary>
    public sealed class ReaderDrainLock : IDisposable
    {
        private const long WriterPendingBit = 1L << 63;
        private const long ReaderMask = 0xFFFFFFFFL;

        private long _state;

        // Set when (a) no readers are active OR (b) no writer is pending.
        // The writer waits on this; readers signal it on the 1->0 transition
        // when a writer is pending.
        private readonly ManualResetEventSlim _drained = new ManualResetEventSlim(initialState: true, spinCount: 0);

        // Set when no writer is pending. Readers wait on this in the slow
        // path; a writer resets it on entry and sets it on exit.
        private readonly ManualResetEventSlim _writerCleared = new ManualResetEventSlim(initialState: true, spinCount: 0);

        // Serializes writers. Held from EnterWrite to WriteHandle.Dispose.
        private readonly object _writerGate = new object();

        private bool _disposed;

        /// <summary>
        /// Try to take the read lock without blocking. Returns false if a
        /// writer is currently pending or holding the lock.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryEnterRead(out ReadHandle handle)
        {
            long s = Interlocked.Add(ref _state, 1);
            if ((s & WriterPendingBit) == 0)
            {
                handle = new ReadHandle(this);
                return true;
            }

            // Writer is pending. Undo our increment and report failure.
            AbortAfterAdd();
            handle = default;
            return false;
        }

        /// <summary>
        /// Take the read lock, blocking until granted or the token is
        /// cancelled. Throws <see cref="OperationCanceledException"/> when
        /// the token fires (typically a timeout cts wrapping the call).
        /// </summary>
        public ReadHandle EnterRead(CancellationToken token)
        {
            while (true)
            {
                long s = Interlocked.Add(ref _state, 1);
                if ((s & WriterPendingBit) == 0)
                    return new ReadHandle(this);

                // Aborted: a writer beat us to publishing. Undo our
                // increment, then wait for the writer to clear before
                // retrying. Throws OCE on cancellation.
                AbortAfterAdd();
                _writerCleared.Wait(token);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AbortAfterAdd()
        {
            long s = Interlocked.Decrement(ref _state);
            // If our undo just emptied the reader count and a writer is
            // still pending, wake it - we may have been the last reader
            // it was waiting on.
            if ((s & ReaderMask) == 0 && (s & WriterPendingBit) != 0)
                _drained.Set();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ExitRead()
        {
            long s = Interlocked.Decrement(ref _state);
            // If the reader count just hit zero AND a writer is pending,
            // wake the writer. The mask check is necessary because of the
            // 64-bit decrement: only the low 32 bits hold the count.
            if ((s & ReaderMask) == 0 && (s & WriterPendingBit) != 0)
                _drained.Set();
        }

        /// <summary>
        /// Take the write lock, blocking until all active readers have
        /// drained or the token fires. Writers are serialized.
        /// </summary>
        public IDisposable EnterWrite(CancellationToken token)
        {
            // Serialize writers. Held until WriteHandle.Dispose or until we
            // throw on timeout/cancel.
            Monitor.Enter(_writerGate);
            bool publishedFlag = false;
            try
            {
                _drained.Reset();
                _writerCleared.Reset();

                // Publish writer-pending and atomically observe the reader
                // count at the moment the flag becomes visible.
                long s = Interlocked.Or(ref _state, WriterPendingBit);
                publishedFlag = true;

                long readers = s & ReaderMask;
                if (readers == 0)
                {
                    // No active readers at the moment we published the flag.
                    // Any reader that races us will see WriterPending in its
                    // post-Add result and undo before doing protected work.
                    return new WriteHandle(this);
                }

                _drained.Wait(token);
                Debug.Assert((Volatile.Read(ref _state) & ReaderMask) == 0,
                    "writer woke with non-zero reader count");
                return new WriteHandle(this);
            }
            catch
            {
                if (publishedFlag)
                {
                    // Roll back the writer-pending flag so readers can resume.
                    Interlocked.And(ref _state, ~WriterPendingBit);
                    _writerCleared.Set();
                    _drained.Set();
                }
                Monitor.Exit(_writerGate);
                throw;
            }
        }

        internal void ExitWrite()
        {
            // Clear writer-pending. By the lock invariant the reader count
            // is zero here, so we can simply zero the high bit.
            Interlocked.And(ref _state, ~WriterPendingBit);
            _writerCleared.Set();
            _drained.Set();
            Monitor.Exit(_writerGate);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            _drained.Dispose();
            _writerCleared.Dispose();
        }

        /// <summary>
        /// Stack-allocated read handle. Releases the read lock on Dispose.
        /// Safe to ignore the return value of <see cref="Dispose"/> being
        /// called more than once - the handle becomes inert after the first
        /// release because the parent reference is captured by value.
        /// </summary>
        public struct ReadHandle : IDisposable
        {
            private ReaderDrainLock _parent;

            internal ReadHandle(ReaderDrainLock parent)
            {
                _parent = parent;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose()
            {
                ReaderDrainLock parent = _parent;
                if (parent == null)
                    return;
                _parent = null;
                parent.ExitRead();
            }
        }

        private sealed class WriteHandle : IDisposable
        {
            private ReaderDrainLock _parent;

            internal WriteHandle(ReaderDrainLock parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                ReaderDrainLock parent = _parent;
                if (parent == null)
                    return;
                _parent = null;
                parent.ExitWrite();
            }
        }
    }
}
