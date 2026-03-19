#nullable enable
using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

public sealed class DropTableTests
{
    private static string NewTempDir(string name)
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", name, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private sealed class UxUser
    {
        [DatabaseObjectId] public string Id { get; set; } = "";
        [DbIndex("Email", Unique = true)] public string? Email { get; set; }
    }

    [Fact]
    public async Task DropTable_Removes_Data_SST_Indexes_And_UniqueGuards()
    {
        var dir = NewTempDir("drop-unique");
        var sstDir = Path.Combine(dir, "sst");
        const string Tbl = "drop-users";

        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var tbl = await db.OpenTableAsync<UxUser>(Tbl, new TableOptions<UxUser> { GetId = u => u.Id });

        // 1) Insert z unikalnym mailem
        await tbl.UpsertAsync(new UxUser { Id = "A", Email = "x@example.com" });

        // 2) Duplikat powinien rzucić
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await tbl.UpsertAsync(new UxUser { Id = "B", Email = "x@example.com" }));

        // 3) Checkpoint -> w katalogu sst pojawią się pliki
        await db.CheckpointAsync();
        Assert.True(Directory.EnumerateFiles(sstDir, "*.sst").Any());

        // 4) Drop całej tabeli (łącznie z indeksami i guardami)
        await db.DropTableAsync(Tbl);

        // 4a) sst powinno być puste (w tych testach tworzona jest tylko ta jedna tabela)
        Assert.False(Directory.EnumerateFiles(sstDir, "*.sst").Any());

        // 4b) EnumerateTableNames nie zwraca tabeli ani indeksów
        Assert.DoesNotContain(Tbl, db.EnumerateTableNames(includeIndexes: false));
        Assert.DoesNotContain(db.EnumerateTableNames(includeIndexes: true),
            n => n.StartsWith($"__index__{Tbl}__", StringComparison.Ordinal));

        // 5) Re-open -> guard unikalności MUSI być zdjęty
        var tbl2 = await db.OpenTableAsync<UxUser>(Tbl, new TableOptions<UxUser> { GetId = u => u.Id });

        // Teraz B z tym samym mailem powinno PRZEJŚĆ (stary guard nie może wisieć)
        await tbl2.UpsertAsync(new UxUser { Id = "B", Email = "x@example.com" });

        // A z tym samym mailem ponownie powinno rzucić (nowy indeks działa)
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await tbl2.UpsertAsync(new UxUser { Id = "A", Email = "x@example.com" }));

        // sanity: w tabeli żyje tylko B
        var all = await tbl2.QueryAsync(_ => true).ToListAsync();
        Assert.Single(all);
        Assert.Equal("B", all[0].Id);
    }

    [Fact]
    public async Task DropTable_Clears_InMemory_State_For_Existing_Table_Handle()
    {
        var dir = NewTempDir("drop-handle");
        const string Tbl = "drop-users-handle";

        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var tbl = await db.OpenTableAsync<UxUser>(Tbl, new TableOptions<UxUser> { GetId = u => u.Id });

        // Seed with a record that exercises the unique index.
        await tbl.UpsertAsync(new UxUser { Id = "seed", Email = "support@support" });

        // Drop the table – consumer code may still hold on to the old table instance.
        await db.DropTableAsync(Tbl);

        // Using the same table handle, inserting the same unique value should succeed because
        // the in-memory memtables and index caches are cleared during DropTableAsync.
        await tbl.UpsertAsync(new UxUser { Id = "after", Email = "support@support" });

        var docs = await tbl.QueryAsync(_ => true).ToListAsync();
        Assert.Single(docs);
        Assert.Equal("after", docs[0].Id);
    }

    [Fact]
    public async Task DropTable_Old_Table_Handle_Can_Persist_New_Data_Across_Reopen()
    {
        var dir = NewTempDir("drop-handle-reopen");
        const string Tbl = "drop-users-handle-reopen";
        var walPath = Path.Combine(dir, "wal.log");

        await using (var wal = new WalWriter(walPath))
        {
            await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

            var tbl = await db.OpenTableAsync<UxUser>(Tbl, new TableOptions<UxUser> { GetId = u => u.Id });
            await tbl.UpsertAsync(new UxUser { Id = "seed", Email = "support@support" });

            await db.DropTableAsync(Tbl);

            // Simulates application code that keeps the original handle after deleting the table.
            await tbl.UpsertAsync(new UxUser { Id = "after", Email = "support@support" });
            await db.CheckpointAsync();
        }

        await using var wal2 = new WalWriter(walPath);
        await using var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal2);
        var tbl2 = await db2.OpenTableAsync<UxUser>(Tbl, new TableOptions<UxUser> { GetId = u => u.Id });

        var docs = await tbl2.QueryAsync(_ => true).ToListAsync();
        Assert.Single(docs);
        Assert.Equal("after", docs[0].Id);
    }

    [Fact]
    public async Task DropTable_Persists_Across_Reopen()
    {
        var dir = NewTempDir("drop-reopen");
        const string Tbl = "drop-users-reopen";
        var walPath = Path.Combine(dir, "wal.log");

        await using (var wal = new WalWriter(walPath))
        {
            await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
            var tbl = await db.OpenTableAsync<UxUser>(Tbl, new TableOptions<UxUser> { GetId = u => u.Id });
            await tbl.UpsertAsync(new UxUser { Id = "seed", Email = "support@support" });
            await db.DropTableAsync(Tbl);
        }

        await using var wal2 = new WalWriter(walPath);
        await using var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal2);
        var tbl2 = await db2.OpenTableAsync<UxUser>(Tbl, new TableOptions<UxUser> { GetId = u => u.Id });

        var existing = await tbl2.QueryAsync(_ => true).ToListAsync();
        Assert.Empty(existing);

        await tbl2.UpsertAsync(new UxUser { Id = "after", Email = "support@support" });
        var docs = await tbl2.QueryAsync(_ => true).ToListAsync();
        Assert.Single(docs);
        Assert.Equal("after", docs[0].Id);
    }

    [Fact]
    public async Task LegacyDrop_WithMissingPrimarySst_IsRepairedOnStartup()
    {
        var dir = NewTempDir("drop-legacy-clean");
        const string Tbl = "legacy-users";
        var walPath = Path.Combine(dir, "wal.log");

        await using (var wal = new WalWriter(walPath))
        {
            await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
            var tbl = await db.OpenTableAsync<UxUser>(Tbl, new TableOptions<UxUser> { GetId = u => u.Id });
            await tbl.UpsertAsync(new UxUser { Id = "legacy", Email = "support@support" });
            await db.CheckpointAsync();
        }

        var sstDir = Path.Combine(dir, "sst");
        var primarySst = Path.Combine(sstDir, $"{Tbl}.sst");
        if (File.Exists(primarySst)) File.Delete(primarySst);
        if (File.Exists(primarySst + ".sxi")) File.Delete(primarySst + ".sxi");

        Assert.True(File.Exists(Path.Combine(sstDir, $"__index__{Tbl}__Email.sst")), "legacy index SST should remain to simulate stale state");

        await using var wal2 = new WalWriter(walPath);
        await using var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal2);
        var tbl2 = await db2.OpenTableAsync<UxUser>(Tbl, new TableOptions<UxUser> { GetId = u => u.Id });

        await tbl2.UpsertAsync(new UxUser { Id = "after", Email = "support@support" });

        var docs = await tbl2.QueryAsync(_ => true).ToListAsync();
        Assert.Single(docs);
        Assert.Equal("after", docs[0].Id);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await tbl2.UpsertAsync(new UxUser { Id = "dup", Email = "support@support" }));
    }
}

internal static class AsyncEnumUtil
{
    public static async Task<System.Collections.Generic.List<T>> ToListAsync<T>(this IAsyncEnumerable<T> src)
    {
        var list = new System.Collections.Generic.List<T>();
        await foreach (var x in src) list.Add(x);
        return list;
    }
}
