using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Diagnostics;
using WalnutDb.Wal;
using Xunit;

namespace WalnutDb.Tests;

public sealed class StorageDiagnosticsTests
{
    private static string NewTempDir(string suffix)
    {
        var path = Path.Combine(Path.GetTempPath(), "WalnutDbTests", suffix + "-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed class HealDoc
    {
        public required string Id { get; init; }
        public string? Payload { get; init; }
    }

    [Fact]
    public async Task ScanSstDirectory_ReturnsHealthyFile()
    {
        var dir = NewTempDir("diag-ok");
        var walPath = Path.Combine(dir, "wal.log");

        await using (var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var table = await db.OpenTableAsync(new TableOptions<HealDoc> { GetId = d => d.Id });
            await table.UpsertAsync(new HealDoc { Id = "a", Payload = "one" });
            await table.UpsertAsync(new HealDoc { Id = "b", Payload = "two" });
            await db.CheckpointAsync();
        }

        var sstDir = Path.Combine(dir, "sst");
        var result = StorageDiagnostics.ScanSstDirectory(sstDir);

        Assert.NotEmpty(result.Files);
        Assert.Empty(result.Corruptions);

        var info = Assert.Single(result.Files);
        Assert.True(info.ObservedRowCount > 0);
        Assert.Equal(info.ObservedRowCount, info.DeclaredRowCount);
    }

    [Fact]
    public async Task ScanSstDirectory_FlagsTruncatedRecord()
    {
        var dir = NewTempDir("diag-corrupt");
        var walPath = Path.Combine(dir, "wal.log");

        string sstPath;
        await using (var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var table = await db.OpenTableAsync(new TableOptions<HealDoc> { GetId = d => d.Id });
            for (int i = 0; i < 8; i++)
                await table.UpsertAsync(new HealDoc { Id = $"row-{i}", Payload = new string('x', i + 1) });
            await db.CheckpointAsync();
            sstPath = Directory.GetFiles(Path.Combine(dir, "sst"), "*.sst").Single();
        }

        using (var fs = new FileStream(sstPath, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
        {
            fs.SetLength(fs.Length - 2); // truncate payload
        }

        var result = StorageDiagnostics.ScanSstDirectory(Path.Combine(dir, "sst"));
        Assert.NotEmpty(result.Corruptions);
        Assert.Contains(result.Corruptions, c => string.Equals(c.Path, sstPath, StringComparison.OrdinalIgnoreCase));
    }
}
