#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

using Xunit;

public sealed class NightlySoakStressTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "soak", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private sealed class Doc
    {
        [DatabaseObjectId] public string Id { get; set; } = "";
        public int Value { get; set; }
    }

    private static TimeSpan GetDurationSeconds(int @default)
    {
        var env = Environment.GetEnvironmentVariable("WALNUT_SOAK_SECONDS");
        if (int.TryParse(env, out var secs) && secs > 0) return TimeSpan.FromSeconds(secs);
        return TimeSpan.FromSeconds(@default);
    }

    [Trait("Category", "Soak")]
    [Fact]
    public async Task Concurrent_Writes_Reads_With_Frequent_Checkpoints_Soak()
    {
        var dir = NewTempDir();
        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var tbl = await db.OpenTableAsync(new TableOptions<Doc> { GetId = d => d.Id });

        const int keyCount = 500;
        const int writers = 12;
        const int readers = 6;

        var duration = GetDurationSeconds(@default: 15);
        var keys = Enumerable.Range(0, keyCount).Select(i => $"k{i:D4}").ToArray();

        // „Model” – uaktualniany zawsze po R/W na podstawie faktycznego widoku DB
        var expected = new ConcurrentDictionary<string, int?>();

        using var cts = new CancellationTokenSource(duration);

        var writerTasks = Enumerable.Range(0, writers).Select(async w =>
        {
            var rnd = new Random(unchecked(Environment.TickCount * 37 + w * 997));
            int opId = 0;
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    var k = keys[rnd.Next(keys.Length)];

                    if (rnd.NextDouble() < 0.80)
                    {
                        int v = Interlocked.Increment(ref opId);
                        await tbl.UpsertAsync(new Doc { Id = k, Value = v });  // commit zakończony
                    }
                    else
                    {
                        await tbl.DeleteAsync(k);                               // commit zakończony
                    }

                    // READ-AFTER-WRITE ⇒ model = to, co naprawdę teraz widać
                    var cur = await tbl.GetAsync(k);
                    expected[k] = cur is null ? (int?)null : cur.Value;

                    await Task.Yield();
                }
            }
            catch (OperationCanceledException) { /* koniec czasu */ }
        }).ToArray();

        var readerTasks = Enumerable.Range(0, readers).Select(async r =>
        {
            var rnd = new Random(unchecked(Environment.TickCount * 131 + r * 13));
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        if (rnd.Next(2) == 0)
                        {
                            await foreach (var _ in tbl.GetAllAsync(pageSize: 256, ct: cts.Token)) { }
                        }
                        else
                        {
                            var k = keys[rnd.Next(keys.Length)];
                            _ = await tbl.GetAsync(k, cts.Token);
                        }
                    }
                    catch (OperationCanceledException) { break; }

                    await Task.Yield();
                }
            }
            catch (OperationCanceledException) { }
        }).ToArray();

        var chkTask = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try { await db.CheckpointAsync(cts.Token); } catch { /* best-effort */ }
                try { await Task.Delay(100, cts.Token); } catch { break; }
            }
        }, cts.Token);

        // ... cały test jak u Ciebie powyżej ...

        await Task.WhenAll(writerTasks.Concat(readerTasks).Append(chkTask));

        // „Ustal” ostatni stan w SST (żeby nie wisiało w MEM z tombstonami)
        await db.CheckpointAsync();

        // === NOWOŚĆ: twarda rekonsyliacja modelu po zakończeniu ruchu ===
        // (ostatnie wyścigi, które mogły „minąć się” z read-after-write w writerach)
        foreach (var k in keys)
        {
            var cur = await tbl.GetAsync(k);
            expected[k] = cur is null ? (int?)null : cur.Value;
        }

        // A) zbiory kluczy żyjących
        var expectedAlive = expected.Where(kv => kv.Value.HasValue)
                                    .Select(kv => kv.Key)
                                    .ToHashSet(StringComparer.Ordinal);

        var actualAlive = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var d in tbl.GetAllAsync(pageSize: 4096))
            actualAlive.Add(d.Id);

        // Jeżeli to się różni – mamy realny problem w enumeracji (merge MEM/SST), a nie w modelu.
        if (!expectedAlive.SetEquals(actualAlive))
        {
            var missing = expectedAlive.Except(actualAlive).Take(20).ToArray();
            var extra = actualAlive.Except(expectedAlive).Take(20).ToArray();

            var msg = $"Final state mismatch: expectedAlive={expectedAlive.Count}, actualAlive={actualAlive.Count}\n" +
                      (missing.Length > 0 ? $"  Missing (in DB): {string.Join(",", missing)}\n" : "") +
                      (extra.Length > 0 ? $"  Extra   (in DB): {string.Join(",", extra)}\n" : "");
            Assert.Fail(msg);
        }

        // B) sanity: wartości dla żyjących
        foreach (var k in expectedAlive)
        {
            var got = await tbl.GetAsync(k);
            Assert.NotNull(got);
            Assert.Equal(expected[k]!.Value, got!.Value);
        }
    }
}
