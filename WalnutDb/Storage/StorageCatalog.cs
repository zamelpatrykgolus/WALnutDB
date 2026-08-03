#nullable enable
using System.Text.Json;

namespace WalnutDb.Storage;

internal sealed class UnsupportedStorageVersionException : IOException
{
    public UnsupportedStorageVersionException(string message) : base(message) { }
}

internal sealed class StorageCatalog
{
    public const int CurrentStorageVersion = 1;
    public const int CurrentManifestVersion = 1;
    public const string ManifestFileName = "MANIFEST-000001.json";

    public int ManifestVersion { get; init; } = CurrentManifestVersion;
    public int StorageVersion { get; init; } = CurrentStorageVersion;
    public string CreatedWith { get; init; } = "WalnutDb-1.0.19";
    public string WalFormat { get; init; } = "WALv1";
    public string SstFormat { get; init; } = "SSTv1";
    public long LastSequence { get; init; }
    public Dictionary<string, string> TableFiles { get; init; } = new(StringComparer.Ordinal);
}

internal static class StorageCatalogStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true
    };

    public static StorageCatalog? Load(string directory, IManifestStore manifestStore)
    {
        var current = manifestStore.ReadCurrentAsync().AsTask().GetAwaiter().GetResult();
        if (string.IsNullOrWhiteSpace(current))
            return null;

        if (!string.Equals(Path.GetFileName(current), current, StringComparison.Ordinal))
            throw new InvalidDataException("CURRENT contains an invalid manifest path.");

        var path = Path.Combine(directory, current);
        if (!manifestStore.ValidateManifestAsync(path).AsTask().GetAwaiter().GetResult())
            throw new InvalidDataException($"Manifest '{current}' is missing or invalid.");

        var json = File.ReadAllBytes(path);
        StorageCatalog catalog;
        try
        {
            catalog = JsonSerializer.Deserialize<StorageCatalog>(json, JsonOptions)
                ?? throw new InvalidDataException($"Manifest '{current}' could not be deserialized.");
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException($"Manifest '{current}' contains invalid JSON.", ex);
        }

        if (catalog.ManifestVersion != StorageCatalog.CurrentManifestVersion)
            throw new UnsupportedStorageVersionException($"Unsupported manifest version {catalog.ManifestVersion}.");
        if (catalog.StorageVersion > StorageCatalog.CurrentStorageVersion)
            throw new UnsupportedStorageVersionException($"Database storage version {catalog.StorageVersion} is newer than this WalnutDb build supports.");
        if (catalog.StorageVersion < StorageCatalog.CurrentStorageVersion)
            throw new UnsupportedStorageVersionException($"Database storage version {catalog.StorageVersion} is not supported by this WalnutDb build.");
        if (!string.Equals(catalog.WalFormat, "WALv1", StringComparison.Ordinal) ||
            !string.Equals(catalog.SstFormat, "SSTv1", StringComparison.Ordinal))
            throw new UnsupportedStorageVersionException($"Unsupported storage formats: WAL='{catalog.WalFormat}', SST='{catalog.SstFormat}'.");

        return catalog;
    }

    public static async ValueTask SaveAsync(string directory, IManifestStore manifestStore, StorageCatalog catalog, CancellationToken ct = default)
    {
        var path = Path.Combine(directory, StorageCatalog.ManifestFileName);
        var tmp = path + ".tmp";
        var bytes = JsonSerializer.SerializeToUtf8Bytes(catalog, JsonOptions);

        await using (var fs = new FileStream(tmp, new FileStreamOptions
        {
            Mode = FileMode.Create,
            Access = FileAccess.Write,
            Share = FileShare.None,
            Options = FileOptions.WriteThrough | FileOptions.Asynchronous
        }))
        {
            await fs.WriteAsync(bytes, ct).ConfigureAwait(false);
            fs.Flush(true);
        }

        if (File.Exists(path))
            File.Replace(tmp, path, destinationBackupFileName: null, ignoreMetadataErrors: true);
        else
            File.Move(tmp, path);

        await manifestStore.WriteCurrentAsync(StorageCatalog.ManifestFileName, ct).ConfigureAwait(false);
    }
}
