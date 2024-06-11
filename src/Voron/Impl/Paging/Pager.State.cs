#nullable enable

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Reflection.Metadata;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron.Platform.Win32;

namespace Voron.Impl.Paging;

public unsafe partial class Pager2
{
    public class State: IDisposable
    {
        public readonly Pager2 Pager;

        public readonly WeakReference<State> WeakSelf;
        public State(Pager2 pager)
        {
            Pager = pager;
            WeakSelf = new WeakReference<State>(this);
        }

        public State Clone()
        {
            State cloned = (State)MemberwiseClone();
            cloned.Cleanup = new()
            {
                cloned.Handle,
                cloned.FileStream
            };
            return cloned;
        }

        public void ReplaceInstance()
        {
            for (int i = 0; i < Cleanup.Count; i++)
            {
                var cur = Cleanup[i];
                if (ReferenceEquals(cur, Handle) ||
                    ReferenceEquals(cur, FileStream))
                {
                    Cleanup[i] = null;
                }
            }

            Handle = null;
            FileStream = null;
            MemoryMappedFile = null;
        }
        
        public byte* BaseAddress;
        public long NumberOfAllocatedPages;
        public long TotalAllocatedSize;
        public MemoryMappedFile? MemoryMappedFile;

        public bool Disposed;

        public Win32NativeFileAccess FileAccess;
        public Win32NativeFileAttributes FileAttributes;
        public MemoryMappedFileAccess MemAccess;
        public SafeFileHandle? Handle;
        public FileStream? FileStream;

        public List<IDisposable?> Cleanup = new();

        public void Dispose()
        {
            if (Disposed)
                return;
            // we may call this via a weak reference, so we need to ensure that 
            // we aren't racing through the finalizer and explicit dispose
            lock (WeakSelf)
            {
                if (Disposed)
                    return;
            
                Disposed = true;
                
                Pager._states.TryRemove(WeakSelf);
                // dispose any resources for this state
                for (int index = 0; index < Cleanup.Count; index++)
                {
                    Cleanup[index]?.Dispose();
                    Cleanup[index] = null;
                }
            }
            
            GC.SuppressFinalize(this);

        }

        ~State()
        {
            try
            {
                Dispose();
            }
            catch (Exception e)
            {
                try
                {
                    // cannot throw an exception from here, just log it
                    var entry = new LogEntry
                    {
                        At = DateTime.UtcNow,
                        Logger = nameof(State),
                        Exception = e,
                        Message = "Failed to dispose the pager state from the finalizer",
                        Source = "PagerState Finalizer",
                        Type = LogMode.Operations
                    };
                    if (LoggingSource.Instance.IsOperationsEnabled)
                    {
                        LoggingSource.Instance.Log(ref entry);
                    }
                }
                catch
                {
                    // nothing we can do here
                }
            }
        }
    }

}
