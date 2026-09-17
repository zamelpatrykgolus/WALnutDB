using System.Security.Cryptography;
using WalnutDb.Core;
using WalnutDb.Indexing;
using WalnutDb.Storage;
using WalnutDb.Wal;

namespace WalnutDb.Tests.V2;

public sealed class Doc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    public string Value { get; set; } = "";
}
public sealed class IndexedDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Email", Unique = true)] public string? Email { get; set; }
}
public sealed record Sample(string Series, DateTime Time, int Value);

internal static class Fixture
{
    public static string Directory()
    {
        var path = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "v2", Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(path);
        return path;
    }
    public static WalnutDatabase Open(string path, DatabaseOptions? options = null) => new(path,
        options ?? new DatabaseOptions { CheckpointOnDispose = false, AllowStorageV2Upgrade = true },
        new FileSystemManifestStore(path), new WalWriter(Path.Combine(path, "wal.log")));
    public static Task<ITable<Doc>> Table(WalnutDatabase db, string name = "rows") => db.OpenTableAsync(name, new TableOptions<Doc>()).AsTask();
    public static async Task<List<T>> All<T>(IAsyncEnumerable<T> source)
    { var list = new List<T>(); await foreach (var row in source) list.Add(row); return list; }
}

[Trait("StorageVersion", "V1+V2")]
public sealed class SharedFormatTests
{
    [Theory][InlineData(1)][InlineData(2)]
    public async Task UniqueOwnerTransfer_WaitsForSlowDurableCommit(int version)
    {
        var dir = Fixture.Directory();
        await using var db = new WalnutDatabase(dir,
            new DatabaseOptions { CheckpointOnDispose = false, AllowStorageV2Upgrade = true },
            new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log"), TimeSpan.FromMilliseconds(500)));
        if (version == 2) await db.UpgradeStorageAsync();
        var table = await db.OpenTableAsync("users", new TableOptions<IndexedDoc>());
        await table.UpsertAsync(new() { Id = "a", Email = "old" });
        await using var tx = await db.BeginTransactionAsync();
        await table.UpsertAsync(new() { Id = "a", Email = "new" }, tx);
        var commit = tx.CommitAsync(Durability.Group).AsTask();
        var take = table.UpsertAsync(new() { Id = "b", Email = "old" }).AsTask();
        await Task.WhenAll(commit, take);
        await db.CheckpointAsync();
        Assert.Equal("new", (await table.GetAsync("a"))?.Email);
        Assert.Equal("old", (await table.GetAsync("b"))?.Email);
    }

    [Theory][InlineData(1)][InlineData(2)]
    public async Task ValidEmptyNullableIndex_IsNotRebuiltOnEveryOpen(int version)
    {
        var dir = Fixture.Directory();
        await using (var db = Fixture.Open(dir))
        {
            if (version == 2) await db.UpgradeStorageAsync();
            var table = await db.OpenTableAsync("users", new TableOptions<IndexedDoc>());
            await table.UpsertAsync(new() { Id = "one", Email = "old" }); await db.CheckpointAsync();
            await table.UpsertAsync(new() { Id = "one", Email = null }); await db.CheckpointAsync();
        }
        string current = File.ReadAllText(Path.Combine(dir, "CURRENT"));
        var files = System.IO.Directory.GetFiles(Path.Combine(dir, "sst"), "*.sst").ToDictionary(p => p, File.GetLastWriteTimeUtc);
        await using (var db = Fixture.Open(dir, new DatabaseOptions()))
        {
            var table = await db.OpenTableAsync("users", new TableOptions<IndexedDoc>());
            Assert.Null((await table.GetAsync("one"))!.Email);
            await db.CheckpointAsync();
            Assert.Equal(0, db.GetWriteStatistics().SstBytesWritten);
        }
        Assert.Equal(current, File.ReadAllText(Path.Combine(dir, "CURRENT")));
        Assert.All(files, entry => Assert.Equal(entry.Value, File.GetLastWriteTimeUtc(entry.Key)));
    }

    [Theory][InlineData(1)][InlineData(2)]
    public async Task NoOpCheckpointAndReadOnlyReopen_DoNotRewriteDataOrManifest(int version)
    {
        var dir = Fixture.Directory();
        await using (var db = Fixture.Open(dir))
        {
            if (version == 2) await db.UpgradeStorageAsync();
            var table = await Fixture.Table(db);
            await table.UpsertAsync(new() { Id = "one", Value = "original" });
            await db.CheckpointAsync();
            long before = db.GetWriteStatistics().SstBytesWritten;
            await db.CheckpointAsync();
            Assert.Equal(before, db.GetWriteStatistics().SstBytesWritten);
        }
        var files = System.IO.Directory.GetFiles(dir, "*", SearchOption.AllDirectories)
            .Where(p => p.EndsWith(".sst") || Path.GetFileName(p).StartsWith("MANIFEST") || Path.GetFileName(p) == "CURRENT")
            .ToDictionary(p => p, p => (File.GetLastWriteTimeUtc(p), Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(p)))));
        await using (var reopened = Fixture.Open(dir, new DatabaseOptions()))
            Assert.Equal("original", (await (await Fixture.Table(reopened)).GetAsync("one"))?.Value);
        foreach (var entry in files)
        {
            Assert.Equal(entry.Value.Item1, File.GetLastWriteTimeUtc(entry.Key));
            Assert.Equal(entry.Value.Item2, Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(entry.Key))));
        }
    }

    [Theory][InlineData(1)][InlineData(2)]
    public async Task OnlyDirtyTableIsWritten_AndDeletesSurviveRestart(int version)
    {
        var dir = Fixture.Directory();
        await using (var db = Fixture.Open(dir))
        {
            if (version == 2) await db.UpgradeStorageAsync();
            var a = await Fixture.Table(db, "a"); var b = await Fixture.Table(db, "b");
            await a.UpsertAsync(new() { Id = "one", Value = "a" });
            await b.UpsertAsync(new() { Id = "one", Value = "b" });
            await db.CheckpointAsync();
            var old = System.IO.Directory.GetFiles(Path.Combine(dir, "sst"), "*.sst")
                .ToDictionary(p => p, File.GetLastWriteTimeUtc);
            await a.DeleteAsync("one");
            await db.CheckpointAsync();
            Assert.True(old.Count(p => File.Exists(p.Key) && File.GetLastWriteTimeUtc(p.Key) == p.Value) >= 1);
        }
        await using var reopened = Fixture.Open(dir);
        Assert.Null(await (await Fixture.Table(reopened, "a")).GetAsync("one"));
        Assert.Equal("b", (await (await Fixture.Table(reopened, "b")).GetAsync("one"))?.Value);
    }

    [Theory][InlineData(1)][InlineData(2)]
    public async Task EncryptedIndexesAndNullableUpdates_SurviveMultipleCheckpoints(int version)
    {
        var dir = Fixture.Directory();
        DatabaseOptions Options() => new() { CheckpointOnDispose = false, AllowStorageV2Upgrade = true,
            Encryption = new AesGcmEncryption(Enumerable.Range(0, 32).Select(i => (byte)i).ToArray()) };
        await using (var db = Fixture.Open(dir, Options()))
        {
            if (version == 2) await db.UpgradeStorageAsync();
            var table = await db.OpenTableAsync("users", new TableOptions<IndexedDoc>());
            await table.UpsertAsync(new() { Id = "one", Email = "old" }); await db.CheckpointAsync();
            await table.UpsertAsync(new() { Id = "one", Email = null }); await db.CheckpointAsync();
            await table.UpsertAsync(new() { Id = "two", Email = "old" });
            await table.UpsertAsync(new() { Id = "one", Email = "new" }); await db.CheckpointAsync();
        }
        await using var reopened = Fixture.Open(dir, Options());
        var users = await reopened.OpenTableAsync("users", new TableOptions<IndexedDoc>());
        Assert.Equal("new", (await users.GetAsync("one"))?.Email);
        Assert.Equal("two", (await users.GetFirstAsync(_ => true, IndexHint.FromPrefix("Email", "old")))?.Id);
        await Assert.ThrowsAsync<InvalidOperationException>(() => users.UpsertAsync(new() { Id = "three", Email = "old" }).AsTask());
    }

    [Theory][InlineData(1)][InlineData(2)]
    public async Task TimeSeriesRangeAndTail_AcrossSegments(int version)
    {
        var dir = Fixture.Directory();
        var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var opt = new TimeSeriesOptions<Sample> { GetSeriesId = x => x.Series, GetUtcTimestamp = x => x.Time };
        await using (var db = Fixture.Open(dir))
        {
            if (version == 2) await db.UpgradeStorageAsync();
            var table = await db.OpenTimeSeriesAsync("samples", opt);
            for (int n = 0; n < 6; n++)
            { await table.AppendAsync(new("s", start.AddMinutes(n), n)); if (n % 2 == 1) await db.CheckpointAsync(); }
        }
        await using var reopened = Fixture.Open(dir);
        var ts = await reopened.OpenTimeSeriesAsync("samples", opt);
        Assert.Equal(new[] { 2, 3, 4 }, (await Fixture.All(ts.QueryAsync("s", start.AddMinutes(2), start.AddMinutes(5)))).Select(x => x.Value));
        Assert.Equal(new[] { 5, 4 }, (await Fixture.All(ts.QueryTailAsync("s", 2))).Select(x => x.Value));
    }
}

[Trait("StorageVersion", "V2")]
public sealed class V2StorageTests
{
    [Fact]
    public async Task EnvelopeRejectsLegacyReaders_AndFutureVersionDoesNotRepairWal()
    {
        var dir = Fixture.Directory();
        await using (var db = Fixture.Open(dir))
        { await db.UpgradeStorageAsync(); await (await Fixture.Table(db)).UpsertAsync(new() { Id = "one", Value = "wal" }); }
        string manifest = Path.Combine(dir, File.ReadAllText(Path.Combine(dir, "CURRENT")).Trim());
        string json = File.ReadAllText(manifest);
        using (var doc = System.Text.Json.JsonDocument.Parse(json))
        {
            Assert.Equal(2, doc.RootElement.GetProperty("StorageVersion").GetInt32());
            Assert.Equal(2, doc.RootElement.GetProperty("ManifestVersion").GetInt32());
        }
        File.WriteAllText(manifest, json.Replace("\"StorageVersion\": 2", "\"StorageVersion\": 999", StringComparison.Ordinal));
        string wal = Path.Combine(dir, "wal.log");
        using (var stream = new FileStream(wal, FileMode.Append)) stream.Write(new byte[] { 1, 2, 3 });
        var before = File.ReadAllBytes(wal);
        Assert.ThrowsAny<IOException>(() => Fixture.Open(dir));
        Assert.Equal(before, File.ReadAllBytes(wal));
    }

    [Fact]
    public async Task CompactionAtSegmentLimit_PreservesDirtyWal_AndDoesNotRewriteBase()
    {
        var dir = Fixture.Directory();
        await using var db = Fixture.Open(dir, new DatabaseOptions { CheckpointOnDispose = false, AllowStorageV2Upgrade = true, MaxSegmentsPerTable = 3 });
        await db.UpgradeStorageAsync(); var table = await Fixture.Table(db);
        await table.UpsertAsync(new() { Id = "one", Value = "base" }); await db.CheckpointAsync();
        await table.UpsertAsync(new() { Id = "one", Value = "delta1" }); await db.CheckpointAsync();
        await table.DeleteAsync("one"); await db.CheckpointAsync();
        await table.UpsertAsync(new() { Id = "dirty", Value = "WAL" });
        long walBefore = new FileInfo(Path.Combine(dir, "wal.log")).Length;
        await Assert.ThrowsAsync<IOException>(() => db.CheckpointAsync().AsTask());
        Assert.Equal(CompactionState.Completed, (await db.CompactAsync("rows")).State);
        Assert.Equal(walBefore, new FileInfo(Path.Combine(dir, "wal.log")).Length);
        Assert.Null(await table.GetAsync("one"));
        Assert.Equal("WAL", (await table.GetAsync("dirty"))?.Value);
        await db.CheckpointAsync();
    }

    [Fact]
    public async Task CompactionBudgetFailure_DoesNotReportSuccessOrRemoveInputs()
    {
        var dir = Fixture.Directory();
        await using (var db = Fixture.Open(dir))
        {
            await db.UpgradeStorageAsync(); var table = await Fixture.Table(db);
            await table.UpsertAsync(new() { Id = "one", Value = new string('x', 1000) }); await db.CheckpointAsync();
            var files = System.IO.Directory.GetFiles(Path.Combine(dir, "sst"), "*.sst");
            await Assert.ThrowsAsync<IOException>(() => db.CompactAsync("rows", new() { IncludeBaseSegments = true, MaxOutputBytes = 100 }).AsTask());
            Assert.NotEqual(CompactionState.Completed, db.GetCompactionStatus()?.State);
            Assert.All(files, file => Assert.True(File.Exists(file)));
        }
        await using var reopened = Fixture.Open(dir);
        Assert.Equal(CompactionState.Failed, reopened.GetCompactionStatus()?.State);
        Assert.Equal(1000, (await (await Fixture.Table(reopened)).GetAsync("one"))?.Value.Length);
    }

    [Fact]
    public async Task DropWithOtherDirtyTable_PreservesItsWal()
    {
        var dir = Fixture.Directory();
        await using (var db = Fixture.Open(dir))
        {
            await db.UpgradeStorageAsync(); var a = await Fixture.Table(db, "a"); var b = await Fixture.Table(db, "b");
            await a.UpsertAsync(new() { Id = "one", Value = "old" }); await db.CheckpointAsync();
            await b.UpsertAsync(new() { Id = "two", Value = "dirty" });
            await db.DropTableAsync("a");
        }
        await using var reopened = Fixture.Open(dir);
        Assert.Null(await (await Fixture.Table(reopened, "a")).GetAsync("one"));
        Assert.Equal("dirty", (await (await Fixture.Table(reopened, "b")).GetAsync("two"))?.Value);
    }

    [Fact]
    public async Task CorruptedEpochHeader_IsRejectedWithoutTailTruncation()
    {
        var dir = Fixture.Directory();
        await using (var db = Fixture.Open(dir))
        { await db.UpgradeStorageAsync(); await (await Fixture.Table(db)).UpsertAsync(new() { Id = "one", Value = "safe" }); await db.CheckpointAsync(); }
        var path = Path.Combine(dir, "wal.log"); var bytes = File.ReadAllBytes(path); bytes[10] ^= 1; File.WriteAllBytes(path, bytes);
        Assert.Throws<InvalidDataException>(() => Fixture.Open(dir));
        Assert.Equal(bytes, File.ReadAllBytes(path));
    }

    [Theory][InlineData("manifest.before-write")][InlineData("manifest.after-sync")][InlineData("manifest.after-publish")]
    public async Task InterruptedMigration_LeavesLegacyDataReadable(string point)
    {
        var dir = Fixture.Directory();
        await using (var db = Fixture.Open(dir))
        { await (await Fixture.Table(db)).UpsertAsync(new() { Id = "one", Value = "legacy" }); await db.CheckpointAsync(); }
        var before = SHA256.HashData(File.ReadAllBytes(Path.Combine(dir, "sst", "rows.sst")));
        bool armed = false;
        await using (var db = Fixture.Open(dir, new DatabaseOptions { CheckpointOnDispose = false, AllowStorageV2Upgrade = true,
            StorageFault = stage => { if (armed && stage == point) throw new IOException("Injected migration fault"); } }))
        { armed = true; await Assert.ThrowsAsync<IOException>(() => db.UpgradeStorageAsync().AsTask()); }
        await using var reopened = Fixture.Open(dir);
        Assert.Equal("legacy", (await (await Fixture.Table(reopened)).GetAsync("one"))?.Value);
        Assert.Equal(before, SHA256.HashData(File.ReadAllBytes(Path.Combine(dir, "sst", "rows.sst"))));
    }

    [Fact]
    public async Task MigrationIsExplicit_MetadataOnly_AndDeltaDoesNotRewriteLegacyBase()
    {
        var dir = Fixture.Directory();
        await using (var db = Fixture.Open(dir))
        {
            var table = await Fixture.Table(db);
            await using var tx = await db.BeginTransactionAsync();
            for (int n = 0; n < 200; n++) await table.UpsertAsync(new() { Id = $"{n:D4}", Value = new string('x', 4096) }, tx);
            await tx.CommitAsync(); await db.CheckpointAsync();
        }
        string legacy = Path.Combine(dir, "sst", "rows.sst");
        var hash = SHA256.HashData(File.ReadAllBytes(legacy));
        await using (var gated = Fixture.Open(dir, new DatabaseOptions { CheckpointOnDispose = false }))
            await Assert.ThrowsAsync<InvalidOperationException>(() => gated.UpgradeStorageAsync().AsTask());
        await using (var db = Fixture.Open(dir))
        {
            Assert.Equal(0, (await db.PlanStorageUpgradeAsync()).DataBytesToRewrite);
            await db.UpgradeStorageAsync();
            Assert.Equal(hash, SHA256.HashData(File.ReadAllBytes(legacy)));
            var table = await Fixture.Table(db);
            await table.UpsertAsync(new() { Id = "0001", Value = "changed" });
            await table.DeleteAsync("0002");
            await db.CheckpointAsync();
            Assert.InRange(db.GetWriteStatistics().SstBytesWritten, 1, 1024);
            Assert.Equal(hash, SHA256.HashData(File.ReadAllBytes(legacy)));
        }
        await using var reopened = Fixture.Open(dir);
        var read = await Fixture.Table(reopened);
        Assert.Equal("changed", (await read.GetAsync("0001"))?.Value);
        Assert.Null(await read.GetAsync("0002"));
        Assert.Equal(199, (await Fixture.All(read.GetAllAsync())).Count);
    }

    [Theory]
    [InlineData("segment.before-write")][InlineData("segment.after-record")]
    [InlineData("segment.before-sync")][InlineData("segment.after-sync")]
    [InlineData("segment.before-rename")][InlineData("segment.after-rename")]
    [InlineData("manifest.before-write")][InlineData("manifest.before-sync")]
    [InlineData("manifest.after-sync")][InlineData("manifest.before-publish")]
    [InlineData("manifest.after-publish")][InlineData("wal.before-rotate")][InlineData("wal.after-rotate")]
    public async Task InterruptedCheckpoint_RecoversAllCommittedChanges(string point)
    {
        var dir = Fixture.Directory(); bool armed = false;
        var options = new DatabaseOptions { CheckpointOnDispose = false, AllowStorageV2Upgrade = true,
            StorageFault = stage => { if (armed && stage == point) throw new IOException("Injected I/O failure"); } };
        await using (var db = Fixture.Open(dir, options))
        {
            await db.UpgradeStorageAsync();
            var a = await Fixture.Table(db, "a"); var b = await Fixture.Table(db, "b");
            await a.UpsertAsync(new() { Id = "delete", Value = "old" }); await db.CheckpointAsync();
            await using var tx = await db.BeginTransactionAsync();
            await a.DeleteAsync("delete", tx);
            await a.UpsertAsync(new() { Id = "new", Value = "A" }, tx);
            await b.UpsertAsync(new() { Id = "new", Value = "B" }, tx);
            await tx.CommitAsync(); armed = true;
            await Assert.ThrowsAsync<IOException>(() => db.CheckpointAsync().AsTask());
        }
        await using var reopened = Fixture.Open(dir);
        Assert.Null(await (await Fixture.Table(reopened, "a")).GetAsync("delete"));
        Assert.Equal("A", (await (await Fixture.Table(reopened, "a")).GetAsync("new"))?.Value);
        Assert.Equal("B", (await (await Fixture.Table(reopened, "b")).GetAsync("new"))?.Value);
        await reopened.CheckpointAsync();
    }

    [Theory][InlineData("cleanup.before-delete")][InlineData("cleanup.after-delete")][InlineData("cleanup.before-complete")]
    public async Task Compaction_IsNotCompleteUntilCleanupIsDurable_AndResumes(string point)
    {
        var dir = Fixture.Directory(); bool armed = false;
        var options = new DatabaseOptions { CheckpointOnDispose = false, AllowStorageV2Upgrade = true,
            StorageFault = stage => { if (armed && stage == point) throw new IOException("Injected cleanup failure"); } };
        await using (var db = Fixture.Open(dir, options))
        {
            await db.UpgradeStorageAsync(); var table = await Fixture.Table(db);
            await table.UpsertAsync(new() { Id = "one", Value = "old" }); await db.CheckpointAsync();
            await table.DeleteAsync("one"); await db.CheckpointAsync();
            armed = true;
            await Assert.ThrowsAsync<IOException>(() => db.CompactAsync("rows", new() { IncludeBaseSegments = true }).AsTask());
            Assert.Equal(CompactionState.PublishedCleanupPending, db.GetCompactionStatus()?.State);
        }
        await using var reopened = Fixture.Open(dir);
        Assert.Equal(CompactionState.Completed, reopened.GetCompactionStatus()?.State);
        Assert.Null(await (await Fixture.Table(reopened)).GetAsync("one"));
    }

    [Fact]
    public async Task Compaction_WaitsForActiveScannerBeforeDeletingInputs()
    {
        var dir = Fixture.Directory();
        await using var db = Fixture.Open(dir); await db.UpgradeStorageAsync(); var table = await Fixture.Table(db);
        await table.UpsertAsync(new() { Id = "a", Value = "first" });
        await table.UpsertAsync(new() { Id = "b", Value = "second" }); await db.CheckpointAsync();
        await using var scan = table.GetAllAsync().GetAsyncEnumerator();
        Assert.True(await scan.MoveNextAsync());
        var compact = db.CompactAsync("rows", new() { IncludeBaseSegments = true }).AsTask();
        for (int i = 0; i < 100 && db.GetCompactionStatus()?.State != CompactionState.PublishedCleanupPending; i++) await Task.Delay(10);
        Assert.False(compact.IsCompleted);
        Assert.True(await scan.MoveNextAsync());
        await scan.DisposeAsync();
        Assert.Equal(CompactionState.Completed, (await compact.WaitAsync(TimeSpan.FromSeconds(5))).State);
    }

    [Fact]
    public async Task OutOfSpace_PreservesWalAndDirtyData_AndCanRetry()
    {
        var dir = Fixture.Directory(); long space = long.MaxValue;
        await using var db = Fixture.Open(dir, new DatabaseOptions { CheckpointOnDispose = false, AllowStorageV2Upgrade = true, AvailableSpaceOverride = () => space });
        await db.UpgradeStorageAsync(); var table = await Fixture.Table(db);
        await table.UpsertAsync(new() { Id = "one", Value = "safe" });
        var before = new FileInfo(Path.Combine(dir, "wal.log")).Length; space = 0;
        await Assert.ThrowsAsync<IOException>(() => db.CheckpointAsync().AsTask());
        Assert.Equal(before, new FileInfo(Path.Combine(dir, "wal.log")).Length);
        Assert.Equal("safe", (await table.GetAsync("one"))?.Value);
        space = long.MaxValue; await db.CheckpointAsync();
    }

    [Fact]
    public async Task DropRecreateAndBackup_AcrossEpochs()
    {
        var dir = Fixture.Directory(); var backup = Fixture.Directory();
        await using (var db = Fixture.Open(dir))
        {
            await db.UpgradeStorageAsync(); var table = await Fixture.Table(db);
            await table.UpsertAsync(new() { Id = "old", Value = "gone" }); await db.CheckpointAsync();
            await db.DropTableAsync("rows");
            await table.UpsertAsync(new() { Id = "new", Value = "kept" }); await db.CheckpointAsync();
            await db.CreateBackupAsync(backup);
        }
        foreach (var path in new[] { dir, backup })
        {
            await using var db = Fixture.Open(path); var table = await Fixture.Table(db);
            Assert.Null(await table.GetAsync("old")); Assert.Equal("kept", (await table.GetAsync("new"))?.Value);
        }
    }

    [Fact]
    public async Task CorruptedSegmentAndManifest_FailClosedWithoutTruncatingWal()
    {
        var dir = Fixture.Directory();
        await using (var db = Fixture.Open(dir))
        {
            await db.UpgradeStorageAsync(); var table = await Fixture.Table(db);
            await table.UpsertAsync(new() { Id = "one", Value = "safe" }); await db.CheckpointAsync();
        }
        var segment = System.IO.Directory.GetFiles(Path.Combine(dir, "sst"), "*.sst").Single();
        var bytes = File.ReadAllBytes(segment); bytes[20] ^= 1; File.WriteAllBytes(segment, bytes);
        var wal = File.ReadAllBytes(Path.Combine(dir, "wal.log"));
        Assert.ThrowsAny<Exception>(() => Fixture.Open(dir));
        Assert.Equal(wal, File.ReadAllBytes(Path.Combine(dir, "wal.log")));
        string manifest = Path.Combine(dir, File.ReadAllText(Path.Combine(dir, "CURRENT")).Trim());
        File.WriteAllText(manifest, "{}");
        Assert.ThrowsAny<IOException>(() => Fixture.Open(dir));
        Assert.Equal(wal, File.ReadAllBytes(Path.Combine(dir, "wal.log")));
    }
}
