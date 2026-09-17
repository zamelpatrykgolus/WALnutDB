using System.ComponentModel;
using System.Runtime.InteropServices;

namespace WalnutDb.Storage;

// Publication primitives. Linux requires fsync of the containing directory;
// Windows uses a write-through rename. Never turn a failed barrier into success.
internal static class DurableFile
{
    public static void Move(string source, string destination)
    {
        if (OperatingSystem.IsWindows())
        {
            if (File.Exists(destination))
            {
                // ReplaceFile preserves old file identities held by scanners;
                // MoveFileEx(REPLACE_EXISTING) can reject those open handles.
                File.Replace(source, destination, null);
                using var published = new FileStream(destination, FileMode.Open, FileAccess.ReadWrite,
                    FileShare.ReadWrite | FileShare.Delete);
                published.Flush(true);
            }
            else if (!MoveFileEx(Path.GetFullPath(source), Path.GetFullPath(destination), 0x8))
                throw new IOException("Durable rename failed.", new Win32Exception(Marshal.GetLastWin32Error()));
        }
        else
        {
            File.Move(source, destination, overwrite: true);
            SyncDirectory(Path.GetDirectoryName(destination)!);
        }
    }

    public static void SyncDirectory(string path)
    {
        if (OperatingSystem.IsWindows()) return; // publication uses MoveFileEx WRITE_THROUGH
        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS())
            throw new PlatformNotSupportedException("Durable publication requires Windows or a supported POSIX filesystem.");
        int fd = Open(path, 0);
        if (fd < 0) throw new IOException("Cannot open directory for fsync.", new Win32Exception(Marshal.GetLastWin32Error()));
        try
        {
            if (Fsync(fd) != 0)
                throw new IOException("Directory fsync failed.", new Win32Exception(Marshal.GetLastWin32Error()));
        }
        finally { Close(fd); }
    }

    [DllImport("kernel32.dll", EntryPoint = "MoveFileExW", CharSet = CharSet.Unicode, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool MoveFileEx(string source, string destination, int flags);
    [DllImport("libc", EntryPoint = "open", SetLastError = true)] private static extern int Open(string path, int flags);
    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)] private static extern int Fsync(int fd);
    [DllImport("libc", EntryPoint = "close")] private static extern int Close(int fd);
}
