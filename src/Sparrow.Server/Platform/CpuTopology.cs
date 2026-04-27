using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Global;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix.macOS;

namespace Sparrow.Server.Platform
{
    public static class CpuTopology
    {
        // Conservative default for boxes where probing fails or returns nothing useful
        // (older kernels, ARM64 Linux without sysfs cache nodes, Apple Silicon's unified
        // memory hierarchy that does not expose a classical L3, Windows on ARM64, etc.).
        private const long DefaultL3CacheSize = 8L * Constants.Size.Megabyte;

        private static readonly Lazy<long> L3CacheSizeLazy = new(ProbeL3CacheSize, LazyThreadSafetyMode.PublicationOnly);

        public static long L3CacheSize => L3CacheSizeLazy.Value;

        private static long ProbeL3CacheSize()
        {
            try
            {
                if (PlatformDetails.RunningOnLinux)
                    return ProbeLinux();
                if (PlatformDetails.RunningOnMacOsx)
                    return ProbeMacOs();
                if (PlatformDetails.RunningOnWindows)
                    return ProbeWindows();
            }
            catch
            {
                // best-effort: fall through to the default
            }

            return DefaultL3CacheSize;
        }

        private static long ProbeLinux()
        {
            // sysfs is the only path that works consistently across x86_64 and AArch64 Linux.
            // sysconf(_SC_LEVEL3_CACHE_SIZE) returns 0 on ARM64 kernels in practice.
            const string path = "/sys/devices/system/cpu/cpu0/cache/index3/size";
            if (File.Exists(path) == false)
                return DefaultL3CacheSize;

            return ParseSizeWithUnit(File.ReadAllText(path));
        }

        private static long ParseSizeWithUnit(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw))
                return DefaultL3CacheSize;

            var s = raw.Trim();
            long multiplier = 1;
            var suffix = s[^1];
            if (suffix is 'K' or 'k') { multiplier = Constants.Size.Kilobyte; s = s[..^1]; }
            else if (suffix is 'M' or 'm') { multiplier = Constants.Size.Megabyte; s = s[..^1]; }
            else if (suffix is 'G' or 'g') { multiplier = Constants.Size.Gigabyte; s = s[..^1]; }

            return long.TryParse(s, out var value) && value > 0
                ? value * multiplier
                : DefaultL3CacheSize;
        }

        private static unsafe long ProbeMacOs()
        {
            long value = 0;
            int len = sizeof(long);
            int rc = macSyscall.sysctlbyname("hw.l3cachesize", &value, &len, null, UIntPtr.Zero);
            if (rc == 0 && value > 0)
                return value;

            // Apple Silicon reports 0 here because the SoC has a system-level cache
            // instead of a per-die L3. Fall back to the conservative default rather
            // than guessing at SLC sizes that vary by chip.
            return DefaultL3CacheSize;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct CacheDescriptor
        {
            public byte Level;
            public byte Associativity;
            public ushort LineSize;
            public uint Size;
            public int Type;
        }

        [StructLayout(LayoutKind.Explicit)]
        private struct ProcessorInfoUnion
        {
            [FieldOffset(0)] public CacheDescriptor Cache;
            [FieldOffset(0)] public ulong Reserved0;
            [FieldOffset(8)] public ulong Reserved1;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct SystemLogicalProcessorInformation
        {
            public nuint ProcessorMask;
            public int Relationship;
            public ProcessorInfoUnion Info;
        }

        private const int RelationCache = 2;

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetLogicalProcessorInformation(IntPtr buffer, ref uint returnedLength);

        private static unsafe long ProbeWindows()
        {
            uint length = 0;
            GetLogicalProcessorInformation(IntPtr.Zero, ref length);
            if (length == 0)
                return DefaultL3CacheSize;

            var buffer = Marshal.AllocHGlobal((int)length);
            try
            {
                if (GetLogicalProcessorInformation(buffer, ref length) == false)
                    return DefaultL3CacheSize;

                int entrySize = sizeof(SystemLogicalProcessorInformation);
                int count = (int)length / entrySize;
                long best = 0;
                var ptr = (SystemLogicalProcessorInformation*)buffer;
                for (int i = 0; i < count; i++)
                {
                    ref var entry = ref ptr[i];
                    if (entry.Relationship != RelationCache)
                        continue;
                    if (entry.Info.Cache.Level != 3)
                        continue;
                    // Multiple entries can describe the same L3 (one per processor mask group);
                    // they share a size, so picking the max is safe.
                    if (entry.Info.Cache.Size > best)
                        best = entry.Info.Cache.Size;
                }

                return best > 0 ? best : DefaultL3CacheSize;
            }
            finally
            {
                Marshal.FreeHGlobal(buffer);
            }
        }
    }
}
