#nullable enable
using System.Text;
using WalnutDb.Storage;

namespace WalnutDb;

/// <summary>
/// Implementacja IManifestStore korzystająca z prostego pliku CURRENT wskazującego aktywny MANIFEST.
/// - Windows: używa File.Replace, jeśli to możliwe (atomowa wymiana); przy pierwszym zapisie File.Move.
/// - Linux/macOS: używa File.Move(overwrite: true), które jest atomowe w obrębie tego samego FS.
/// 
/// Plik CURRENT zawiera jedną linię: nazwę aktywnego pliku manifestu (np. "MANIFEST-000042\n").
/// </summary>
public sealed class FileSystemManifestStore : IManifestStore
{
    private readonly string _directory;
    private readonly string _currentPath;

    public FileSystemManifestStore(string directory)
    {
        _directory = directory;
        bool existed = Directory.Exists(directory);
        Directory.CreateDirectory(directory);
        if (!existed)
            DurableFile.SyncDirectory(Path.GetDirectoryName(Path.GetFullPath(directory))!);
        _currentPath = Path.Combine(directory, "CURRENT");
    }

    public async ValueTask<string?> ReadCurrentAsync(CancellationToken ct = default)
    {
        if (!File.Exists(_currentPath)) return null;
        using var fs = new FileStream(_currentPath, new FileStreamOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.Read,
            Share = FileShare.ReadWrite,
            Options = FileOptions.SequentialScan
        });
        using var sr = new StreamReader(fs, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 256, leaveOpen: false);
        var line = await sr.ReadLineAsync().WaitAsync(ct).ConfigureAwait(false);
        return string.IsNullOrWhiteSpace(line) ? null : line.Trim();
    }

    public async ValueTask WriteCurrentAsync(string manifestName, CancellationToken ct = default)
    {
        if (await ReadCurrentAsync(ct).ConfigureAwait(false) == manifestName) return;
        var tmp = _currentPath + ".tmp";

        // Zapisz do pliku tymczasowego i utrwal na dysku
        await using (var fs = new FileStream(tmp, new FileStreamOptions
        {
            Mode = FileMode.Create,
            Access = FileAccess.ReadWrite,
            Share = FileShare.None,
            Options = FileOptions.SequentialScan
        }))
        {
            var bytes = Encoding.UTF8.GetBytes(manifestName + "\n");
            await fs.WriteAsync(bytes.AsMemory(0, bytes.Length), ct).ConfigureAwait(false);
            fs.Flush(true); // trwałe zapisanie zawartości TMP
        }

        DurableFile.Move(tmp, _currentPath);
    }

    public ValueTask<bool> ValidateManifestAsync(string path, CancellationToken ct = default)
    {
        // MVP: sprawdź czy plik istnieje i ma jakąś treść. Docelowo: weryfikacja CRC w trailerze.
        try
        {
            var fi = new FileInfo(path);
            return new(fi.Exists && fi.Length > 0);
        }
        catch
        {
            return new(false);
        }
    }
}
