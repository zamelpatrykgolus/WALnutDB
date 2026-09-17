#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class UxStressUser
{
    [DatabaseObjectId] public string Id { get; set; } = "";

    [DbIndex("Email", Unique = true)]
    public string? Email { get; set; }
}

public sealed class UniqueIndexStressTests
{
    private static string NewTempDir(string name)
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", name, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Unique_Index_No_Duplicates_Under_Contention()
    {
        var dir = NewTempDir("stress-ux");
        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        Diag.UniqueTrace = true; // włącz w teście: Diag.UniqueTrace = true;
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var tbl = await db.OpenTableAsync(new TableOptions<UxStressUser> { GetId = u => u.Id });

        const int userCount = 200;
        const int writers = 8;
        var duration = TimeSpan.FromSeconds(2.5);

        var ids = Enumerable.Range(0, userCount).Select(i => $"U{i:D4}").ToArray();
        var emails = Enumerable.Range(0, userCount).Select(i => $"e{i:D4}@ex.com").ToArray();

        using var cts = new CancellationTokenSource(duration);

        var tasks = Enumerable.Range(0, writers).Select(async w =>
        {
            var rnd = new Random(unchecked(Environment.TickCount * 61 + w * 17));
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    var id = ids[rnd.Next(ids.Length)];
                    var action = rnd.NextDouble();

                    try
                    {
                        if (action < 0.10) // 10% delete
                        {
                            var res = await tbl.DeleteAsync(id); // ⟵ bez tokena – op kończy się w całości
                            Console.WriteLine($"Deleted user {id}: {res}");
                        }
                        else // 90% upsert
                        {
                            var email = emails[rnd.Next(emails.Length)];
                            var res = await tbl.UpsertAsync(new UxStressUser { Id = id, Email = email }); // ⟵ bez tokena
                            Console.WriteLine($"Upserted user {id} with email {email}: {res}");
                        }
                    }
                    catch (InvalidOperationException ex)
                    {
                        Console.WriteLine($"Error for user {id}, kolizja unikalności: {ex.Message}");
                        // spodziewane kolizje unikalności – ignorujemy
                    }
                    await Task.Yield();
                }
            }
            catch (OperationCanceledException ex) { Console.WriteLine(ex); }
        }).ToArray();

        var chkTask = Task.Run(async () =>
        {
            while (true)
            {
                try { await Task.Delay(200, cts.Token); }
                catch (OperationCanceledException ex)
                {
                    Console.WriteLine(ex);
                    break;
                }

                try { await db.CheckpointAsync(); } // ⟵ bez tokena
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }
        }, cts.Token);

        await Task.WhenAll(tasks.Append(chkTask));

        await db.CheckpointAsync();

        var dupCheck = new Dictionary<string, int>(StringComparer.Ordinal);
        await foreach (var u in tbl.ScanByIndexAsync("Email", default, default))
        {
            if (u.Email is null)
            {
                Console.WriteLine($"User {u.Id} has null email, skipping.");
                continue;
            }
            if (dupCheck.ContainsKey(u.Email) && dupCheck[u.Email] > 0)
            {
                Console.WriteLine($"Duplicate email found: {u.Email} for user {u.Id}");
            }
            dupCheck.TryGetValue(u.Email, out var c);
            dupCheck[u.Email] = c + 1;
        }

        foreach (var kv in dupCheck)
        {
            Console.WriteLine($"Email: {kv.Key}, Count: {kv.Value}");
        }

        foreach (var kv in dupCheck)
            Assert.True(kv.Value <= 1, $"Duplicate email in index: {kv.Key} count={kv.Value}");
    }

    [Fact]
    public async Task Unique_TwoDifferentPk_SameEmail_OnlyOneWins()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "uniq-race", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        Diag.UniqueTrace = true;

        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var tbl = await db.OpenTableAsync(new TableOptions<UxStressUser> { GetId = u => u.Id });

        var email = "e9999@ex.com";
        var t1 = tbl.UpsertAsync(new UxStressUser { Id = "A", Email = email }).AsTask();
        var t2 = tbl.UpsertAsync(new UxStressUser { Id = "B", Email = email }).AsTask();

        bool ok1 = false, ok2 = false;

        try
        {
            await t1;
            ok1 = true;
        }
        catch (InvalidOperationException)
        {
        }

        try
        {
            await t2;
            ok2 = true;
        }
        catch (InvalidOperationException)
        {
        }

        await db.CheckpointAsync();

        int cnt = 0;
        await foreach (var u in tbl.ScanByIndexAsync("Email", default, default))
            if (u.Email == email) cnt++;

        Assert.True(cnt == 1, $"expected exactly one owner of {email}, got {cnt} (ok1={ok1}, ok2={ok2})");
    }

    [Fact]
    public async Task Unique_RotateAndTake_OldOwnerReleases_NewOwnerTakes()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "uniq-rotate", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        Diag.UniqueTrace = true;

        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var tbl = await db.OpenTableAsync(new TableOptions<UxStressUser> { GetId = u => u.Id });

        await tbl.UpsertAsync(new UxStressUser { Id = "A", Email = "E" });

        // A zmienia E→F, równolegle B próbuje wziąć E
        var tA = tbl.UpsertAsync(new UxStressUser { Id = "A", Email = "F" }).AsTask();
        var tB = tbl.UpsertAsync(new UxStressUser { Id = "B", Email = "E" }).AsTask();

        await Task.WhenAll(tA, tB);
        await db.CheckpointAsync();

        var a = await tbl.GetAsync("A");
        var b = await tbl.GetAsync("B");

        Assert.Equal("F", a?.Email);
        Assert.Equal("E", b?.Email);
    }

}
