using WalnutDb.Core;
using WalnutDb.Indexing;
using WalnutDb.Wal;

namespace WalnutDb.Tests.StorageVersions.Safety;

file sealed class SafetyDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    public int Value { get; set; }
}

file sealed class UniqueSafetyDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Email", Unique = true)] public string Email { get; set; } = "";
}

[Trait("StorageVersion", "Safety")]
public sealed class MaintenanceSafetyTests
{
    [Fact]
    public async Task ConcurrentCheckpoint_PreservesEveryCommittedRowAfterRestart()
    {
        var dir = NewDir();
        var options = new DatabaseOptions { CheckpointOnDispose = false };
        await using (var db = Open(dir, options))
        {
            var table = await db.OpenTableAsync("rows", new TableOptions<SafetyDoc>());
            var writers = Enumerable.Range(0, 4).Select(worker => Task.Run(async () =>
            {
                for (var i = 0; i < 150; i++)
                    await table.UpsertAsync(new SafetyDoc { Id = $"{worker}-{i}", Value = i });
            })).ToArray();

            var checkpoint = Task.Run(async () =>
            {
                await Task.Delay(10);
                await db.CheckpointAsync();
            });

            await Task.WhenAll(writers.Append(checkpoint));
            await db.FlushAsync();
        }

        await using var reopened = Open(dir, options);
        var reopenedTable = await reopened.OpenTableAsync("rows", new TableOptions<SafetyDoc>());
        var count = 0;
        await foreach (var _ in reopenedTable.GetAllAsync()) count++;
        Assert.Equal(600, count);
    }

    [Fact]
    public async Task RebuildOneTable_DoesNotLoseOtherTableWalData()
    {
        var dir = NewDir();
        var options = new DatabaseOptions { CheckpointOnDispose = false };
        await using (var db = Open(dir, options))
        {
            var a = await db.OpenTableAsync("a", new TableOptions<SafetyDoc>());
            var b = await db.OpenTableAsync("b", new TableOptions<SafetyDoc>());
            await a.UpsertAsync(new SafetyDoc { Id = "a", Value = 1 });
            await b.UpsertAsync(new SafetyDoc { Id = "b", Value = 2 });
            await db.RebuildTableAsync("a");
        }

        await using var reopened = Open(dir, options);
        var tableB = await reopened.OpenTableAsync("b", new TableOptions<SafetyDoc>());
        Assert.Equal(2, (await tableB.GetAsync("b"))?.Value);
    }

    [Fact]
    public async Task Defragment_DoesNotResurrectTombstone_WithEncryption()
    {
        var dir = NewDir();
        var key = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();
        var options = new DatabaseOptions { CheckpointOnDispose = false, Encryption = new AesGcmEncryption(key) };
        await using (var db = Open(dir, options))
        {
            var table = await db.OpenTableAsync("secure", new TableOptions<SafetyDoc>());
            await table.UpsertAsync(new SafetyDoc { Id = "deleted", Value = 1 });
            await table.UpsertAsync(new SafetyDoc { Id = "kept", Value = 2 });
            await db.CheckpointAsync();
            await table.DeleteAsync("deleted");
            await db.DefragmentAsync(DefragMode.Compact);
        }

        var reopenOptions = new DatabaseOptions { CheckpointOnDispose = false, Encryption = new AesGcmEncryption(key) };
        await using var reopened = Open(dir, reopenOptions);
        var secure = await reopened.OpenTableAsync("secure", new TableOptions<SafetyDoc>());
        Assert.Null(await secure.GetAsync("deleted"));
        Assert.Equal(2, (await secure.GetAsync("kept"))?.Value);
    }

    [Fact]
    public async Task SecondDatabaseInstance_IsRejected()
    {
        var dir = NewDir();
        var options = new DatabaseOptions { CheckpointOnDispose = false };
        await using var first = Open(dir, options);
        await using var secondWal = new WalWriter(Path.Combine(dir, "wal.log"));
        Assert.Throws<IOException>(() =>
            new WalnutDatabase(dir, options, new FileSystemManifestStore(dir), secondWal));
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            secondWal.AppendTransactionAsync(new[] { (ReadOnlyMemory<byte>)new byte[] { 1 } }, Durability.Safe).AsTask());
    }

    [Fact]
    public async Task RepeatedKeyInsideTransaction_UpdatesUniqueReservationAndCommitIsSingleUse()
    {
        var dir = NewDir();
        var options = new DatabaseOptions { CheckpointOnDispose = false };
        await using var db = Open(dir, options);
        var table = await db.OpenTableAsync("users", new TableOptions<UniqueSafetyDoc>());

        await using var tx = await db.BeginTransactionAsync();
        await table.UpsertAsync(new UniqueSafetyDoc { Id = "one", Email = "old@example" }, tx);
        await table.UpsertAsync(new UniqueSafetyDoc { Id = "one", Email = "new@example" }, tx);
        await tx.CommitAsync();

        await table.UpsertAsync(new UniqueSafetyDoc { Id = "two", Email = "old@example" });
        Assert.Equal("new@example", (await table.GetAsync("one"))?.Email);
        Assert.Equal("old@example", (await table.GetAsync("two"))?.Email);
        await Assert.ThrowsAsync<InvalidOperationException>(() => tx.CommitAsync().AsTask());
    }

    [Fact]
    public async Task Backup_ContainsConsistentManifestSstAndWalView()
    {
        var dir = NewDir();
        var backup = NewDir();
        Directory.Delete(backup);
        var options = new DatabaseOptions { CheckpointOnDispose = false };

        await using (var db = Open(dir, options))
        {
            var table = await db.OpenTableAsync("backup", new TableOptions<SafetyDoc>());
            await table.UpsertAsync(new SafetyDoc { Id = "sst", Value = 1 });
            await db.CheckpointAsync();
            await table.UpsertAsync(new SafetyDoc { Id = "wal", Value = 2 });
            await db.CreateBackupAsync(backup);
        }

        await using var restored = Open(backup, options);
        var restoredTable = await restored.OpenTableAsync("backup", new TableOptions<SafetyDoc>());
        Assert.Equal(1, (await restoredTable.GetAsync("sst"))?.Value);
        Assert.Equal(2, (await restoredTable.GetAsync("wal"))?.Value);
    }

    [Fact]
    public async Task EncryptedTableWithIndex_RecoversFromWalAndSst()
    {
        var dir = NewDir();
        var key = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();

        await using (var db = Open(dir, EncryptedOptions(key)))
        {
            var table = await db.OpenTableAsync("encrypted-users", new TableOptions<UniqueSafetyDoc>());
            await table.UpsertAsync(new UniqueSafetyDoc { Id = "one", Email = "one@example" });
            await db.FlushAsync();
        }

        await using (var recoveredFromWal = Open(dir, EncryptedOptions(key)))
        {
            var table = await recoveredFromWal.OpenTableAsync("encrypted-users", new TableOptions<UniqueSafetyDoc>());
            var found = await table.GetFirstAsync(
                x => x.Email == "one@example",
                IndexHint.FromPrefix("Email", "one@example"));
            Assert.Equal("one", found?.Id);
            await recoveredFromWal.CheckpointAsync();
        }

        await using var recoveredFromSst = Open(dir, EncryptedOptions(key));
        var sstTable = await recoveredFromSst.OpenTableAsync("encrypted-users", new TableOptions<UniqueSafetyDoc>());
        Assert.Equal("one@example", (await sstTable.GetAsync("one"))?.Email);
    }

    [Fact]
    public async Task CheckpointOnDispose_WritesSstAndTruncatesWal()
    {
        var dir = NewDir();
        await using (var db = Open(dir, new DatabaseOptions { CheckpointOnDispose = true }))
        {
            var table = await db.OpenTableAsync("dispose-checkpoint", new TableOptions<SafetyDoc>());
            await table.UpsertAsync(new SafetyDoc { Id = "persisted", Value = 7 });
        }

        Assert.NotEmpty(Directory.GetFiles(Path.Combine(dir, "sst"), "*.sst"));
        Assert.Equal(0, new FileInfo(Path.Combine(dir, "wal.log")).Length);

        await using var reopened = Open(dir, new DatabaseOptions { CheckpointOnDispose = false });
        var reopenedTable = await reopened.OpenTableAsync("dispose-checkpoint", new TableOptions<SafetyDoc>());
        Assert.Equal(7, (await reopenedTable.GetAsync("persisted"))?.Value);
    }

    [Fact]
    public async Task Dispose_DrainsFastCommitToWal_WhenCheckpointIsDisabled()
    {
        var dir = NewDir();
        var options = new DatabaseOptions { CheckpointOnDispose = false };
        await using (var db = Open(dir, options))
        {
            var table = await db.OpenTableAsync("fast", new TableOptions<SafetyDoc>());
            await using var tx = await db.BeginTransactionAsync();
            await table.UpsertAsync(new SafetyDoc { Id = "queued", Value = 9 }, tx);
            await tx.CommitAsync(Durability.Fast);
        }

        await using var reopened = Open(dir, options);
        var reopenedTable = await reopened.OpenTableAsync("fast", new TableOptions<SafetyDoc>());
        Assert.Equal(9, (await reopenedTable.GetAsync("queued"))?.Value);
    }

    private static DatabaseOptions EncryptedOptions(byte[] key) => new()
    {
        CheckpointOnDispose = false,
        Encryption = new AesGcmEncryption(key)
    };

    private static WalnutDatabase Open(string dir, DatabaseOptions options) =>
        new(dir, options, new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));

    private static string NewDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "maintenance-safety", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }
}
