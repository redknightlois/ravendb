#nullable enable

using System;
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Server.Platform;
using Sparrow.Server.Platform.Win32;
using Sparrow.Utils;
using Voron.Platform.Win32;
using Voron.Global;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Voron.Impl.Paging;

public unsafe partial class Pager2
{
    public class Functions
    {
        public delegate* <Pager2, OpenFileOptions, State> Init;
        public delegate* <Pager2, long, State, byte*> AcquirePagePointer;
        public delegate* <Pager2, long, int, State, byte*> AcquirePagePointerForNewPage;
        public delegate* <Pager2, long, ref State, void> AllocateMorePages;
        public delegate* <Pager2, State, void> Sync;
        public delegate* <byte*, ulong, void> ProtectPageRange;
        public delegate* <byte*, ulong, void> UnprotectPageRange;
    }

    public static class Windows
    {
        private const int AllocationGranularity = 64 * Constants.Size.Kilobyte;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long NearestSizeToAllocationGranularity(long size)
        {
            return ((size / AllocationGranularity) + 1) * AllocationGranularity;
        }

        public static readonly Functions Functions = new()
        {
            Init = &Init,
            AcquirePagePointer = &AcquirePagePointer,
            AcquirePagePointerForNewPage = &AcquirePagePointerForNewPage,
            AllocateMorePages = &AllocateMorePages,
            Sync = &Sync,
            ProtectPageRange = &ProtectPageRange,
            UnprotectPageRange = &UnprotectPageRange,
        };

        public static void ProtectPageRange(byte* start, ulong size)
        {
            var rc = Win32MemoryProtectMethods.VirtualProtect(start, new UIntPtr(size), Win32MemoryProtectMethods.MemoryProtection.READONLY, out _);
            if (!rc)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }

        public static void UnprotectPageRange(byte* start, ulong size)
        {
            bool rc = Win32MemoryProtectMethods.VirtualProtect(start, new UIntPtr(size), Win32MemoryProtectMethods.MemoryProtection.READWRITE, out _);
            if (!rc)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }


        private static void Sync(Pager2 pager, State state)
        {
            if (Win32MemoryMapNativeMethods.FlushViewOfFile(state.BaseAddress, new IntPtr(state.TotalAllocatedSize)) == false ||
                Win32MemoryMapNativeMethods.FlushFileBuffers(state.Handle) == false)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }

        private static void AllocateMorePages(Pager2 pager, long newLength, ref State state)
        {
            var newLengthAfterAdjustment = NearestSizeToAllocationGranularity(newLength);

            if (newLengthAfterAdjustment <= state.TotalAllocatedSize)
                return;

            var allocationSize = newLengthAfterAdjustment - state.TotalAllocatedSize;

            Win32NativeFileMethods.SetFileLength(state.Handle, state.TotalAllocatedSize + allocationSize, pager.FileName);

            var newState = state.Clone();
            newState.TotalAllocatedSize = state.TotalAllocatedSize + allocationSize;
            newState.NumberOfAllocatedPages = newState.TotalAllocatedSize / Constants.Storage.PageSize;
            MapFile(newState);
            
            pager.InstallState(newState);
            
            state.MoveFileOwnership();
            
            state = newState;
        }

        private static byte* AcquirePagePointerForNewPage(Pager2 pager, long pageNumber, int numberOfPages, State state)
        {
            _ = numberOfPages;
            return AcquirePagePointer(pager, pageNumber, state);
        }

        private static byte* AcquirePagePointer(Pager2 pager, long pageNumber, State state)
        {
            if (pager._canPrefetch && pager._prefetchState.ShouldPrefetchSegment(pageNumber, out long offsetFromFileBase, out long bytes))
            {
                var command = new PalDefinitions.PrefetchRanges(state.BaseAddress + offsetFromFileBase, bytes);
                GlobalPrefetchingBehavior.GlobalPrefetcher.Value.CommandQueue.TryAdd(command, 0);
            }

            if (state.Disposed == false)
                return state.BaseAddress + pageNumber * Constants.Storage.PageSize;

            throw new ObjectDisposedException("PagerState was already disposed");
        }

        private static State Init(Pager2 pager, OpenFileOptions openFileOptions)
        {
            var copyOnWriteMode = pager.Options.CopyOnWriteMode && openFileOptions.File.EndsWith(Constants.DatabaseFilename);

            var state = new State(pager, null)
            {
                FileAccess =
                    openFileOptions.ReadOnly || copyOnWriteMode
                        ? Win32NativeFileAccess.GenericRead
                        : Win32NativeFileAccess.GenericRead | Win32NativeFileAccess.GenericWrite,
                FileAttributes = (openFileOptions.Temporary
                                     ? Win32NativeFileAttributes.Temporary | Win32NativeFileAttributes.DeleteOnClose
                                     : Win32NativeFileAttributes.Normal) |
                                 (openFileOptions.SequentialScan ? Win32NativeFileAttributes.SequentialScan : Win32NativeFileAttributes.RandomAccess) |
                                 (copyOnWriteMode ? Win32NativeFileAttributes.Readonly : Win32NativeFileAttributes.None),
                MemAccess = copyOnWriteMode
                    ? MemoryMappedFileAccess.CopyOnWrite
                    : (openFileOptions.ReadOnly ? MemoryMappedFileAccess.Read : MemoryMappedFileAccess.ReadWrite)
            };

            state.Handle = Win32NativeFileMethods.CreateFile(openFileOptions.File,
                state.FileAccess,
                Win32NativeFileShare.Read | Win32NativeFileShare.Write | Win32NativeFileShare.Delete,
                IntPtr.Zero,
                Win32NativeFileCreationDisposition.OpenAlways,
                state.FileAttributes,
                IntPtr.Zero);
            if (state.Handle.IsInvalid)
                ThrowInvalidHandle();

            try
            {
                var fileInfo = new FileInfo(openFileOptions.File);
                var drive = fileInfo.Directory?.Root.Name.TrimEnd('\\');

                try
                {
                    if (drive is not null && AbstractPager.PhysicalDrivePerMountCache.TryGetValue(drive, out pager.UniquePhysicalDriveId) == false)
                        pager.UniquePhysicalDriveId = WindowsMemoryMapPager.GetPhysicalDriveId(drive);

                    if (pager._logger.IsInfoEnabled)
                        pager._logger.Info($"Physical drive '{drive}' unique id = '{pager.UniquePhysicalDriveId}' for file '{openFileOptions.File}'");
                }
                catch (Exception ex)
                {
                    pager.UniquePhysicalDriveId = 0;
                    if (pager._logger.IsInfoEnabled)
                        pager._logger.Info($"Failed to determine physical drive Id for drive letter '{drive}', file='{openFileOptions.File}'", ex);
                }

                var streamAccessType = openFileOptions.ReadOnly
                    ? FileAccess.Read
                    : FileAccess.ReadWrite;
                state.FileStream = SafeFileStream.Create(state.Handle, streamAccessType);

                if (openFileOptions.ReadOnly == false)
                {
                    var fileLength = state.FileStream.Length;
                    state.TotalAllocatedSize = fileLength;
                    if (fileLength == 0)
                    {
                        var newFileLength = NearestSizeToAllocationGranularity(openFileOptions.InitializeFileSize ?? fileLength);
                        if (newFileLength != fileLength)
                        {
                            Win32NativeFileMethods.SetFileLength(state.Handle, newFileLength, openFileOptions.File);
                            state.TotalAllocatedSize = newFileLength;
                        }
                    }
                }
                else
                {
                    state.TotalAllocatedSize = state.FileStream.Length;
                }

                state.NumberOfAllocatedPages = state.TotalAllocatedSize / Constants.Storage.PageSize;

                MapFile(state);
                if (openFileOptions.Temporary && pager.Options.DiscardVirtualMemory)
                {
                    // RavenDB-22465 - TBD
                    // var result = Pal.rvn_discard_virtual_memory(state.BaseAddress, state.TotalAllocatedSize, out int errorCode);
                    // if (result != PalFlags.FailCodes.Success)
                    //     PalHelper.ThrowLastError(result, errorCode, $"Attempted to discard file memory. Path: {openFileOptions.File} Size: {state.TotalAllocatedSize}.");
                }
            }
            catch
            {
                try
                {
                    state.Dispose();
                }
                catch
                {
                    // ignored
                }

                throw;
            }

            return state;

            void ThrowInvalidHandle()
            {
                string message = $"Failed to open file storage of {nameof(Pager2)} for {openFileOptions.File}";

                int lastWin32ErrorCode = Marshal.GetLastWin32Error();

                if (lastWin32ErrorCode is (int)Win32NativeFileErrors.ERROR_SHARING_VIOLATION or (int)Win32NativeFileErrors.ERROR_LOCK_VIOLATION)
                {
                    try
                    {
                        message += $". {WhoIsLocking.ThisFile(openFileOptions.File)}";
                    }
                    catch
                    {
                        // ignored
                    }
                }

                throw new IOException(message, new Win32Exception(lastWin32ErrorCode));
            }
        }

        
        private static void MapFile(State state)
        {
            Win32MemoryMapNativeMethods.NativeFileMapAccessType mmFileAccessType = state.MemAccess switch
            {
                MemoryMappedFileAccess.CopyOnWrite => Win32MemoryMapNativeMethods.NativeFileMapAccessType.Copy,
                _ when state.FileAccess == Win32NativeFileAccess.GenericRead => Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read,
                _ => Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read | Win32MemoryMapNativeMethods.NativeFileMapAccessType.Write
            };

            long mappedSize = state.TotalAllocatedSize;

            state.MemoryMappedFile = MemoryMappedFile.CreateFromFile(state.FileStream!, null, mappedSize,
                state.MemAccess, HandleInheritability.None, true);

            var fileMappingHandle = state.MemoryMappedFile.SafeMemoryMappedFileHandle.DangerousGetHandle();

            state.BaseAddress = Win32MemoryMapNativeMethods.MapViewOfFileEx(fileMappingHandle,
                mmFileAccessType,
                0, 0,
                UIntPtr.Zero, //map all what was "reserved" in CreateFileMapping on previous row
                null);

            if (state.BaseAddress == (byte*)0) //system didn't succeed in mapping the address where we wanted
            {
                var innerException = new Win32Exception(Marshal.GetLastWin32Error(), "Failed to MapView of file " + state.Pager.FileName);
                var errorMessage =
                    $"Unable to allocate more pages - unsuccessfully tried to allocate continuous block of virtual memory with size = {new Size(mappedSize, SizeUnit.Bytes)}";
                throw new OutOfMemoryException(errorMessage, innerException);
            }

            // We don't need to manage size updates, we'll register a new allocation, instead
            NativeMemory.RegisterFileMapping(state.Pager.FileName, (nint)(state.BaseAddress), mappedSize, null);

            // If we are working on memory validation mode, then protect the pages by default.
            if (state.Pager._usePageProtection)
            {
                Win32MemoryProtectMethods.VirtualProtect(state.BaseAddress, new UIntPtr((ulong)mappedSize),
                    Win32MemoryProtectMethods.MemoryProtection.READONLY, out _);
            }
        }
    }
}
