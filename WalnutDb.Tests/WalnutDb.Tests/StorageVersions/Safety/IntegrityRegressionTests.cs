using System.Buffers.Binary;
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests.StorageVersions.Safety;

file sealed class IntegrityDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    public string Value { get; set; } = "";
}

file sealed class NullableIndexDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Email", Unique = true)] public string? Email { get; set; }
}

[Trait("StorageVersion", "Safety")]
public sealed class IntegrityRegressionTests
{
    private static readonly DatabaseOptions KeepWal = new() { CheckpointOnDispose = false };

    [Fact]
    public async Task CorruptedSst_IsRejectedInsteadOfBeingReportedAsMissingData()
    {
        var dir = NewDir();
        await using (var db = Open(dir))
        {
            var table = await db.OpenTableAsync("rows", new TableOptions<IntegrityDoc>());
            await table.UpsertAsync(new IntegrityDoc { Id = "one", Value = "value" });
            await db.CheckpointAsync();
        }

        var sst = Directory.GetFiles(Path.Combine(dir, "sst"), "*.sst").Single();
        using (var fs = new FileStream(sst, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            fs.Position = fs.Length - 4;
            var trailer = new byte[4];
            Assert.Equal(4, fs.Read(trailer));
            BinaryPrimitives.WriteUInt32LittleEndian(trailer, 999);
            fs.Position = fs.Length - 4;
            fs.Write(trailer);
            fs.Flush(true);
        }

        Assert.Throws<InvalidDataException>(() => Open(dir));
    }

    [Fact]
    public async Task CorruptionInsideWal_IsRejectedAndWalIsNotTruncated()
    {
        var dir = NewDir();
        var walPath = Path.Combine(dir, "wal.log");
        await using (var db = Open(dir))
        {
            var table = await db.OpenTableAsync("rows", new TableOptions<IntegrityDoc>());
            await table.UpsertAsync(new IntegrityDoc { Id = "one", Value = "1" });
            await table.UpsertAsync(new IntegrityDoc { Id = "two", Value = "2" });
            await db.FlushAsync();
        }

        long before = new FileInfo(walPath).Length;
        using (var fs = new FileStream(walPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            var len = new byte[4];
            Assert.Equal(4, fs.Read(len));
            uint payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(len);
            fs.Position = 4 + payloadLength;
            int original = fs.ReadByte();
            Assert.True(original >= 0);
            fs.Position--;
            fs.WriteByte((byte)(original ^ 0xFF));
            fs.Flush(true);
        }

        Assert.Throws<InvalidDataException>(() => Open(dir));
        Assert.Equal(before, new FileInfo(walPath).Length);
    }

    [Fact]
    public async Task LegacySstOnly_Base64LookingLiteralName_RemainsLiteral()
    {
        var dir = NewDir();
        await using (var db = Open(dir))
        {
            var table = await db.OpenTableAsync("YWJj", new TableOptions<IntegrityDoc>());
            await table.UpsertAsync(new IntegrityDoc { Id = "one", Value = "literal" });
            await db.CheckpointAsync();
        }
        DeleteManifest(dir);

        await using var reopened = Open(dir);
        var literal = await reopened.OpenTableAsync("YWJj", new TableOptions<IntegrityDoc>());
        Assert.Equal("literal", (await literal.GetAsync("one"))?.Value);
        var decoded = await reopened.OpenTableAsync("abc", new TableOptions<IntegrityDoc>());
        Assert.Null(await decoded.GetAsync("one"));
    }

    [Fact]
    public async Task UniqueNullableIndex_ReleasesOldValueAndAcceptsNullToValueTransition()
    {
        var dir = NewDir();
        await using var db = Open(dir);
        var table = await db.OpenTableAsync("users", new TableOptions<NullableIndexDoc>());

        await table.UpsertAsync(new NullableIndexDoc { Id = "one", Email = "shared@example" });
        await table.UpsertAsync(new NullableIndexDoc { Id = "one", Email = null });
        await table.UpsertAsync(new NullableIndexDoc { Id = "two", Email = "shared@example" });
        await table.UpsertAsync(new NullableIndexDoc { Id = "one", Email = "other@example" });

        Assert.Equal("other@example", (await table.GetAsync("one"))?.Email);
        Assert.Equal("shared@example", (await table.GetAsync("two"))?.Email);
    }

    [Fact]
    public async Task TransactionFromAnotherDatabase_IsRejectedWithoutMutation()
    {
        var firstDir = NewDir();
        var secondDir = NewDir();
        await using var first = Open(firstDir);
        await using var second = Open(secondDir);
        var secondTable = await second.OpenTableAsync("rows", new TableOptions<IntegrityDoc>());
        await using var foreignTransaction = await first.BeginTransactionAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            secondTable.UpsertAsync(new IntegrityDoc { Id = "bad", Value = "bad" }, foreignTransaction).AsTask());
        await foreignTransaction.CommitAsync();

        Assert.Null(await secondTable.GetAsync("bad"));
    }

    [Fact]
    public async Task ValidManifest_DoesNotAdoptOrphanedSst()
    {
        var dir = NewDir();
        await using (var db = Open(dir))
        {
            var table = await db.OpenTableAsync("live", new TableOptions<IntegrityDoc>());
            await table.UpsertAsync(new IntegrityDoc { Id = "one", Value = "live" });
            await db.CheckpointAsync();
        }

        var liveSst = Directory.GetFiles(Path.Combine(dir, "sst"), "*.sst").Single();
        File.Copy(liveSst, Path.Combine(dir, "sst", "ghost.sst"));

        await using var reopened = Open(dir);
        Assert.DoesNotContain("ghost", reopened.EnumerateTableNames());
    }

    [Fact]
    public async Task Backup_RemovesStaleArtifactsAndIncompleteBackupCannotBeOpened()
    {
        var dir = NewDir();
        var backup = NewDir();
        Directory.CreateDirectory(Path.Combine(backup, "sst"));
        await File.WriteAllTextAsync(Path.Combine(backup, "sst", "stale.sst"), "stale");

        await using (var db = Open(dir))
        {
            var table = await db.OpenTableAsync("live", new TableOptions<IntegrityDoc>());
            await table.UpsertAsync(new IntegrityDoc { Id = "one", Value = "backup" });
            await db.CheckpointAsync();
            await db.CreateBackupAsync(backup);
        }

        Assert.False(File.Exists(Path.Combine(backup, "sst", "stale.sst")));
        Assert.False(File.Exists(Path.Combine(backup, ".walnutdb-backup-incomplete")));
        await using (var restored = Open(backup))
        {
            var table = await restored.OpenTableAsync("live", new TableOptions<IntegrityDoc>());
            Assert.Equal("backup", (await table.GetAsync("one"))?.Value);
        }

        var incomplete = NewDir();
        await File.WriteAllTextAsync(Path.Combine(incomplete, ".walnutdb-backup-incomplete"), "incomplete");
        Assert.Throws<InvalidDataException>(() => Open(incomplete));
    }

    private static WalnutDatabase Open(string dir) =>
        new(dir, KeepWal, new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));

    private static string NewDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "integrity-regression", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void DeleteManifest(string dir)
    {
        File.Delete(Path.Combine(dir, "CURRENT"));
        foreach (var file in Directory.EnumerateFiles(dir, "MANIFEST-*"))
            File.Delete(file);
    }
}
