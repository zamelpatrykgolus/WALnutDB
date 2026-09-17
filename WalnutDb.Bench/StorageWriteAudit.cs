using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Bench;

internal static class StorageWriteAudit
{
    private sealed record Row(string Id, byte[] Bytes);
    public static async Task RunAsync(string output)
    {
        var results = new List<object>();
        foreach (int version in new[] { 1, 2 })
        {
            string dir = Path.Combine(Path.GetTempPath(), "WalnutDb-write-audit", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            await using var db = new WalnutDatabase(dir,
                new DatabaseOptions { CheckpointOnDispose = false, AllowStorageV2Upgrade = true },
                new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));
            var table = await db.OpenTableAsync("audit", new TableOptions<Row>
            { GetId = r => r.Id, Serialize = r => r.Bytes, Deserialize = b => new("", b.ToArray()) });
            for (int offset = 0; offset < 10240; offset += 128)
            {
                await using var tx = await db.BeginTransactionAsync();
                for (int i = offset; i < offset + 128; i++)
                    await table.UpsertAsync(new($"{i:D8}", new byte[10240]), tx);
                await tx.CommitAsync();
            }
            await db.CheckpointAsync();
            string baseFile = Path.Combine(dir, "sst", "audit.sst");
            byte[] baseHash = SHA256.HashData(File.ReadAllBytes(baseFile));
            if (version == 2) await db.UpgradeStorageAsync();
            foreach (int changed in new[] { 1, 10, 100, 0 })
            {
                long walBefore = new FileInfo(Path.Combine(dir, "wal.log")).Length;
                if (changed > 0)
                {
                    await using var tx = await db.BeginTransactionAsync();
                    for (int i = 0; i < changed; i++)
                    {
                        var bytes = new byte[10240]; Array.Fill(bytes, (byte)changed);
                        await table.UpsertAsync(new($"{i:D8}", bytes), tx);
                    }
                    await tx.CommitAsync();
                }
                long walWritten = new FileInfo(Path.Combine(dir, "wal.log")).Length - walBefore;
                var before = db.GetWriteStatistics();
                long allocated = GC.GetTotalAllocatedBytes(true);
                var clock = Stopwatch.StartNew();
                await db.CheckpointAsync();
                clock.Stop();
                long sstWritten = db.GetWriteStatistics().SstBytesWritten - before.SstBytesWritten;
                var result = new
                {
                    storageVersion = version, logicalBytes = changed * 10240L, walWritten, sstWritten,
                    dataWriteAmplification = changed == 0 ? (double?)null : (double)(walWritten + sstWritten) / (changed * 10240L),
                    checkpointMilliseconds = clock.Elapsed.TotalMilliseconds,
                    allocatedBytes = GC.GetTotalAllocatedBytes(true) - allocated,
                    legacyBaseUnchanged = version == 2 ? (bool?)baseHash.SequenceEqual(SHA256.HashData(File.ReadAllBytes(baseFile))) : null
                };
                results.Add(result); Console.WriteLine(JsonSerializer.Serialize(result));
            }
            if (version == 2)
            {
                long before = db.GetWriteStatistics().CompactionBytesWritten;
                var watch = Stopwatch.StartNew();
                var status = await db.CompactAsync("audit");
                watch.Stop();
                var compact = new { storageVersion = 2, scenario = "compact deltas only", status = status.State.ToString(),
                    compactionBytesWritten = db.GetWriteStatistics().CompactionBytesWritten - before,
                    milliseconds = watch.Elapsed.TotalMilliseconds,
                    legacyBaseUnchanged = baseHash.SequenceEqual(SHA256.HashData(File.ReadAllBytes(baseFile))) };
                results.Add(compact); Console.WriteLine(JsonSerializer.Serialize(compact));
            }
        }
        await File.WriteAllTextAsync(output, JsonSerializer.Serialize(new
        {
            utc = DateTime.UtcNow, runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
            note = "100 MiB payload. Single local run. WAL+SST counters exclude small manifest/epoch/SXI overhead and are not physical NAND writes.",
            results
        }, new JsonSerializerOptions { WriteIndented = true }));
    }
}
