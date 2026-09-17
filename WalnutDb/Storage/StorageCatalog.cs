#nullable enable
using System.Text.Json;
using System.Security.Cryptography;

namespace WalnutDb.Storage;

internal sealed class UnsupportedStorageVersionException : IOException
{
    public UnsupportedStorageVersionException(string message) : base(message) { }
}

internal sealed record SegmentFile(string FileName, int Format, long Bytes, bool IsBase = false);

internal sealed record StorageCatalog
{
    public const int CurrentStorageVersion = 1;
    public const int CurrentManifestVersion = 1;
    public const string ManifestFileName = "MANIFEST-000001.json";

    public int ManifestVersion { get; init; } = CurrentManifestVersion;
    public int StorageVersion { get; init; } = CurrentStorageVersion;
    public string CreatedWith { get; init; } = "WalnutDb-2.0.0";
    public string WalFormat { get; init; } = "WALv1";
    public string SstFormat { get; init; } = "SSTv1";
    public long LastSequence { get; init; }
    public Dictionary<string, string> TableFiles { get; init; } = new(StringComparer.Ordinal);
    // Oldest first; a generation always represents a complete checkpoint.
    public Dictionary<string, List<SegmentFile>> TableSegments { get; init; } = new(StringComparer.Ordinal);
    public Guid CoveredWalEpoch { get; init; }
    public long CoveredWalOffset { get; init; }
    public CompactionStatus? Compaction { get; init; }
    public List<string> PendingDeletes { get; init; } = new();
    public List<string> StagedFiles { get; init; } = new();
}

internal static class StorageCatalogStore
{
    // Keep version fields at the outer level too: legacy JSON readers must
    // reject v2, not deserialize unknown envelope fields as an empty v1 catalog.
    private sealed record Envelope(byte[] Payload, string Sha256, int StorageVersion = 2, int ManifestVersion = 2);
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true
    };

    public static StorageCatalog? Load(string directory, IManifestStore manifestStore)
    {
        var current = manifestStore.ReadCurrentAsync().AsTask().GetAwaiter().GetResult();
        if (string.IsNullOrWhiteSpace(current))
        {
            if (Directory.EnumerateFiles(directory, "MANIFEST-v2-*").Any())
                throw new UnsupportedStorageVersionException("Storage v2 has no valid CURRENT. Explicit recovery is required; segment discovery is unsafe.");
            return null;
        }

        if (!string.Equals(Path.GetFileName(current), current, StringComparison.Ordinal))
            throw new InvalidDataException("CURRENT contains an invalid manifest path.");

        var path = Path.Combine(directory, current);
        if (!manifestStore.ValidateManifestAsync(path).AsTask().GetAwaiter().GetResult())
        {
            if (current.StartsWith("MANIFEST-v2-", StringComparison.Ordinal))
                throw new UnsupportedStorageVersionException("The active v2 manifest is missing; refusing legacy recovery.");
            throw new InvalidDataException($"Manifest '{current}' is missing or invalid.");
        }

        var json = File.ReadAllBytes(path);
        StorageCatalog catalog;
        try
        {
            if (current.StartsWith("MANIFEST-v2-", StringComparison.Ordinal))
            {
                var envelope = JsonSerializer.Deserialize<Envelope>(json, JsonOptions)
                    ?? throw new InvalidDataException("Missing v2 manifest envelope.");
                if (envelope.StorageVersion != 2 || envelope.ManifestVersion != 2)
                    throw new UnsupportedStorageVersionException("Unsupported manifest envelope version.");
                if (!string.Equals(Convert.ToHexString(SHA256.HashData(envelope.Payload)), envelope.Sha256, StringComparison.Ordinal))
                    throw new InvalidDataException("Manifest checksum mismatch.");
                json = envelope.Payload;
            }
            catalog = JsonSerializer.Deserialize<StorageCatalog>(json, JsonOptions)
                ?? throw new InvalidDataException($"Manifest '{current}' could not be deserialized.");
        }
        catch (Exception ex) when (ex is JsonException or InvalidDataException or ArgumentException)
        {
            if (current.StartsWith("MANIFEST-v2-", StringComparison.Ordinal))
                throw new UnsupportedStorageVersionException($"Damaged v2 manifest; refusing legacy recovery: {ex.Message}");
            throw new InvalidDataException($"Manifest '{current}' contains invalid JSON.", ex);
        }

        if (catalog.StorageVersion > 2)
            throw new UnsupportedStorageVersionException($"Database storage version {catalog.StorageVersion} is newer than this WalnutDb build supports.");
        if (catalog.ManifestVersion is not (1 or 2) || catalog.ManifestVersion != catalog.StorageVersion)
            throw new UnsupportedStorageVersionException($"Unsupported manifest version {catalog.ManifestVersion} for storage {catalog.StorageVersion}.");
        if (catalog.StorageVersion < 1)
            throw new UnsupportedStorageVersionException($"Database storage version {catalog.StorageVersion} is not supported by this WalnutDb build.");
        if (catalog.StorageVersion == 1 && (!string.Equals(catalog.WalFormat, "WALv1", StringComparison.Ordinal) ||
            !string.Equals(catalog.SstFormat, "SSTv1", StringComparison.Ordinal)))
            throw new UnsupportedStorageVersionException($"Unsupported storage formats: WAL='{catalog.WalFormat}', SST='{catalog.SstFormat}'.");

        if (catalog.StorageVersion == 2)
        {
            if (!current.StartsWith("MANIFEST-v2-", StringComparison.Ordinal) || catalog.WalFormat != "WALv1+epochs" || catalog.SstFormat != "SSTv1+SSTv2")
                throw new UnsupportedStorageVersionException("Unsupported v2 format or missing checksummed envelope.");
            if (catalog.TableSegments is null || catalog.PendingDeletes is null || catalog.StagedFiles is null || catalog.CoveredWalOffset < 0)
                throw new UnsupportedStorageVersionException("Invalid v2 manifest fields.");
            var files = catalog.TableSegments.Values.SelectMany(x => x).ToArray();
            var paths = files.Select(f => f.FileName).Concat(catalog.PendingDeletes).Concat(catalog.StagedFiles);
            if (paths.Any(p => string.IsNullOrEmpty(p) || Path.GetFileName(p) != p || p.Contains('/') || p.Contains('\\') || p is "." or "..") ||
                files.Any(f => f.Format is not (1 or 2) || f.Bytes < 12) || files.Select(f => f.FileName).Distinct(StringComparer.OrdinalIgnoreCase).Count() != files.Length)
                throw new UnsupportedStorageVersionException("Invalid or duplicate segment paths in v2 manifest.");
        }

        return catalog;
    }

    public static async ValueTask SaveAsync(string directory, IManifestStore manifestStore, StorageCatalog catalog, CancellationToken ct = default, Action<string>? fault = null)
    {
        var previous = await manifestStore.ReadCurrentAsync(ct).ConfigureAwait(false);
        var name = catalog.StorageVersion == 2 ? $"MANIFEST-v2-{Guid.NewGuid():N}.json" : StorageCatalog.ManifestFileName;
        var path = Path.Combine(directory, name);
        var tmp = path + ".tmp";
        var bytes = JsonSerializer.SerializeToUtf8Bytes(catalog, JsonOptions);
        if (catalog.StorageVersion == 2)
            bytes = JsonSerializer.SerializeToUtf8Bytes(new Envelope(bytes, Convert.ToHexString(SHA256.HashData(bytes))), JsonOptions);

        await using (var fs = new FileStream(tmp, new FileStreamOptions
        {
            Mode = FileMode.Create,
            Access = FileAccess.Write,
            Share = FileShare.None,
            Options = FileOptions.Asynchronous
        }))
        {
            fault?.Invoke("manifest.before-write");
            await fs.WriteAsync(bytes, ct).ConfigureAwait(false);
            fault?.Invoke("manifest.before-sync");
            fs.Flush(true);
            fault?.Invoke("manifest.after-sync");
        }

        DurableFile.Move(tmp, path);

        fault?.Invoke("manifest.before-publish");
        await manifestStore.WriteCurrentAsync(name, ct).ConfigureAwait(false);
        DurableFile.SyncDirectory(directory);
        fault?.Invoke("manifest.after-publish");
        if (previous is not null && previous != name && previous.StartsWith("MANIFEST-v2-", StringComparison.Ordinal))
        {
            File.Delete(Path.Combine(directory, previous));
            DurableFile.SyncDirectory(directory);
        }
    }
}
