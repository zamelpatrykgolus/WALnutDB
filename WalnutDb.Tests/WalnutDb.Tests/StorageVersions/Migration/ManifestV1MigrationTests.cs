using System.Security.Cryptography;
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests.StorageVersions.Migration;

file sealed class MigrationDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    public int Value { get; set; }
}

[Trait("StorageVersion", "Migration")]
public sealed class ManifestV1MigrationTests
{
    [Fact]
    public async Task DamagedV1Manifest_IsRebuiltFromExistingSstWithoutDataRewrite()
    {
        var dir = NewDir();
        var options = new DatabaseOptions { CheckpointOnDispose = false };
        await using (var db = Open(dir, options))
        {
            var table = await db.OpenTableAsync("repair", new TableOptions<MigrationDoc>());
            await table.UpsertAsync(new MigrationDoc { Id = "row", Value = 42 });
            await db.CheckpointAsync();
        }

        var manifest = Directory.GetFiles(dir, "MANIFEST-*").Single();
        await File.WriteAllTextAsync(manifest, "{ damaged");

        await using var repaired = Open(dir, options);
        var repairedTable = await repaired.OpenTableAsync("repair", new TableOptions<MigrationDoc>());
        Assert.Equal(42, (await repairedTable.GetAsync("row"))?.Value);
        Assert.Contains("\"StorageVersion\"", await File.ReadAllTextAsync(manifest));
    }

    [Fact]
    public async Task NewerStorageVersion_IsRejectedInsteadOfBeingReinterpretedAsLegacy()
    {
        var dir = NewDir();
        var options = new DatabaseOptions { CheckpointOnDispose = false };
        await using (var db = Open(dir, options))
        {
            var table = await db.OpenTableAsync("future", new TableOptions<MigrationDoc>());
            await table.UpsertAsync(new MigrationDoc { Id = "row", Value = 1 });
            await db.CheckpointAsync();
        }

        var manifest = Directory.GetFiles(dir, "MANIFEST-*").Single();
        var json = await File.ReadAllTextAsync(manifest);
        Assert.Contains("\"StorageVersion\": 1", json);
        await File.WriteAllTextAsync(manifest, json.Replace("\"StorageVersion\": 1", "\"StorageVersion\": 999", StringComparison.Ordinal));

        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        var error = Assert.ThrowsAny<IOException>(() =>
            new WalnutDatabase(dir, options, new FileSystemManifestStore(dir), wal));
        Assert.Contains("newer", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task LegacySst_Migration_WritesOnlyMetadata_AndDoesNotRewriteDataFile()
    {
        var dir = NewDir();
        var options = new DatabaseOptions { CheckpointOnDispose = false };

        await using (var db = Open(dir, options))
        {
            var table = await db.OpenTableAsync("devices", new TableOptions<MigrationDoc>());
            for (var i = 0; i < 32; i++)
                await table.UpsertAsync(new MigrationDoc { Id = $"d-{i}", Value = i });
            await db.CheckpointAsync();
        }

        var sst = Directory.GetFiles(Path.Combine(dir, "sst"), "*.sst").Single();
        var beforeHash = await SHA256.HashDataAsync(File.OpenRead(sst));
        var beforeLength = new FileInfo(sst).Length;
        DeleteManifest(dir);

        await using (var migrated = Open(dir, options))
        {
            var table = await migrated.OpenTableAsync("devices", new TableOptions<MigrationDoc>());
            Assert.Equal(17, (await table.GetAsync("d-17"))?.Value);
            Assert.True(File.Exists(Path.Combine(dir, "CURRENT")));

            var afterHash = await SHA256.HashDataAsync(File.OpenRead(sst));
            Assert.Equal(beforeHash, afterHash);
            Assert.Equal(beforeLength, new FileInfo(sst).Length);

            var version = await migrated.GetStorageVersionAsync();
            Assert.Equal(1, version.StorageVersion);
            Assert.Contains("WALv1", version.FeatureFlags);
            Assert.Contains("SSTv1", version.FeatureFlags);
        }
    }

    private static WalnutDatabase Open(string dir, DatabaseOptions options) =>
        new(dir, options, new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));

    private static string NewDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "manifest-migration", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void DeleteManifest(string dir)
    {
        File.Delete(Path.Combine(dir, "CURRENT"));
        foreach (var file in Directory.EnumerateFiles(dir, "MANIFEST-*")) File.Delete(file);
    }
}
