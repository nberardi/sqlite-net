using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace SQLite.Tests
{
    public static class NativeLibraryLoader
    {
        [DllImport("kernel32", SetLastError = true)]
        private static extern IntPtr LoadLibraryEx(string lpFileName, IntPtr hFile, uint dwFlags);

        [DllImport("kernel32", SetLastError = true)]
        private static extern IntPtr GetModuleHandle(string lpModuleName);

        [DllImport("kernel32")]
        private static extern void GetSystemInfo(out SYSTEM_INFO lpSystemInfo);

        private static readonly object _lock = new object();

        public static bool TryLoad(string dllName, bool throwWhenNotExists = false)
        {
            var found = false;
            var loaded = false;
            var loadExc = default(Exception);

            lock (_lock)
            {
                if (IsLoaded(dllName))
                    return true;

                string dllPath;
                found = TryFind(dllName, out dllPath);

                // only try to load if it was found
                if (found)
                {
                    var handle = LoadLibraryEx(dllPath, IntPtr.Zero, LOAD_WITH_ALTERED_SEARCH_PATH);
                    var hr = Marshal.GetLastWin32Error();
#if !PROD
                    if (Debugger.IsAttached && hr != 0)
                        Debugger.Break();
#endif
                    loadExc = Marshal.GetExceptionForHR(hr, new IntPtr(-1));
                    loaded = handle != IntPtr.Zero;
                }

                if (!found || !loaded)
                {
                    var msg = $"unable to load '{dllName}' from '{dllPath}' which {(found ? "exists" : "does not exit")}";

                    if (throwWhenNotExists)
                        throw new DllNotFoundException(msg, loadExc);
                    else
                        Trace.Fail(msg, loadExc?.ToString());
                }

                return found && loaded;
            }
        }

        private static bool TryFind(string dllName, out string dllPath)
        {
            if (!dllName.EndsWith(".dll"))
                dllName += ".dll";

            dllPath = GetSearchPaths()
                .Select(d => Path.Combine(d, dllName))
                .FirstOrDefault(File.Exists);

            return dllPath != null;
        }

        private static IEnumerable<string> GetSearchPaths()
        {
            yield return AppDomain.CurrentDomain.BaseDirectory;
            yield return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, GetArchitecture());

            var privateBinPath = AppDomain.CurrentDomain.RelativeSearchPath;
            if (!string.IsNullOrEmpty(privateBinPath))
            {
                foreach (var path in privateBinPath.Split(new[] {Path.PathSeparator}, StringSplitOptions.RemoveEmptyEntries))
                {
                    yield return path;
                    yield return Path.Combine(path, GetArchitecture());
                }
            }

            var assemblyDir = Path.GetDirectoryName(typeof(NativeLibraryLoader).Assembly.Location);
            if (!string.IsNullOrEmpty(assemblyDir))
            {
                yield return assemblyDir;
                yield return Path.Combine(assemblyDir, GetArchitecture());
            }
        }

        private static bool IsLoaded(string dllName)
        {
            lock (_lock)
            {
                if (IsMono())
                    return true;

                var handle = GetModuleHandle(dllName);

                var hr = Marshal.GetLastWin32Error();

                if (hr != 0 || handle == IntPtr.Zero)
                    Console.WriteLine(String.Format("IsLoaded got a handle of {0} and a HR of {1} for {2}", handle, hr, dllName));

                return handle != IntPtr.Zero;
            }
        }

        private static bool IsMono() => Type.GetType("Mono.Runtime") != null;

        private static string GetArchitecture()
        {
            SYSTEM_INFO systemInfo;
            GetSystemInfo(out systemInfo);

            switch (systemInfo.wProcessorArchitecture)
            {
                case PROCESSOR_ARCHITECTURE_AMD64:
                    return "x64";

                case PROCESSOR_ARCHITECTURE_ARM:
                    return "arm";

                default:
                    return "x86";
            }
        }

        [SuppressMessage("ReSharper", "InconsistentNaming")]
        private struct SYSTEM_INFO
        {
#pragma warning disable 649, 169
            public short wProcessorArchitecture;
            public short wReserved;
            public int dwPageSize;
            public IntPtr lpMinimumApplicationAddress;
            public IntPtr lpMaximumApplicationAddress;
            public IntPtr dwActiveProcessorMask;
            public int dwNumberOfProcessors;
            public int dwProcessorType;
            public int dwAllocationGranularity;
            public short wProcessorLevel;
            public short wProcessorRevision;
#pragma warning restore 649, 169
        }

        // ReSharper disable InconsistentNaming
        private const uint LOAD_WITH_ALTERED_SEARCH_PATH = 8;
        private const short PROCESSOR_ARCHITECTURE_INTEL = 0;
        private const short PROCESSOR_ARCHITECTURE_ARM = 5;
        private const short PROCESSOR_ARCHITECTURE_AMD64 = 9;
        // ReSharper restore InconsistentNaming
    }
}
