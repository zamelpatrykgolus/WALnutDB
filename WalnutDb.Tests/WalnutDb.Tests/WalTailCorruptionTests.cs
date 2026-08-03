#nullable enable
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class CorruptDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
}

public sealed class WalTailCorruptionTests
{
    private static readonly DatabaseOptions KeepWal = new() { CheckpointOnDispose = false };
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "wal_tail_corrupt", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Recovery_Ignores_Trailing_Garbage()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        // 1) Napisz kilka ramek
        await using (var db = new WalnutDatabase(dir, KeepWal, new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t = await db.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            for (int i = 0; i < 50; i++)
                await t.UpsertAsync(new CorruptDoc { Id = $"x{i}" });
            await db.FlushAsync();
        }

        // 2) Zepsuj ogon: dopisz kilka bajtów
        await File.AppendAllTextAsync(walPath, "XYZ");

        // 3) Otwórz ponownie – nie powinno rzucać
        await using (var db2 = new WalnutDatabase(dir, KeepWal, new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t2 = await db2.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            int count = 0; await foreach (var _ in t2.GetAllAsync()) count++;
            Assert.Equal(50, count);
        }
    }

    [Fact]
    public async Task Recovery_Truncates_Torn_Frame_And_Emits_Diagnostic()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using (var db = new WalnutDatabase(dir, KeepWal, new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t = await db.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            for (int i = 0; i < 5; i++)
                await t.UpsertAsync(new CorruptDoc { Id = $"g{i}" });
            await db.FlushAsync();
        }

        var goodLength = new FileInfo(walPath).Length;

        // Append an incomplete frame (length without payload)
        await using (var fs = new FileStream(walPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
        {
            var lenBuf = new byte[4];
            WriteUInt32LE(lenBuf, 1024u);
            await fs.WriteAsync(lenBuf, 0, lenBuf.Length);
            await fs.FlushAsync();
        }

        var warnings = new List<string>();
        void Handler(string _, string message) => warnings.Add(message);
        var prevDebug = WalnutLogger.Debug;
        WalnutLogger.Debug = true;
        WalnutLogger.OnWarning += Handler;
        try
        {
            await using (var db2 = new WalnutDatabase(dir, KeepWal, new FileSystemManifestStore(dir), new WalWriter(walPath)))
            {
                var t2 = await db2.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
                int count = 0; await foreach (var _ in t2.GetAllAsync()) count++;
                Assert.Equal(5, count);
            }
        }
        finally
        {
            WalnutLogger.OnWarning -= Handler;
            WalnutLogger.Debug = prevDebug;
        }

        Assert.Equal(goodLength, new FileInfo(walPath).Length);
        Assert.Contains(warnings, m => m.Contains("Truncating WAL tail", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Recovery_Truncates_Frame_With_Corrupted_Crc()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using (var db = new WalnutDatabase(dir, KeepWal, new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t = await db.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            for (int i = 0; i < 10; i++)
                await t.UpsertAsync(new CorruptDoc { Id = $"good{i}" });
            await db.FlushAsync();
        }

        var baselineLength = new FileInfo(walPath).Length;

        await using (var db2 = new WalnutDatabase(dir, KeepWal, new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t2 = await db2.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            await t2.UpsertAsync(new CorruptDoc { Id = "corrupted" });
            await db2.FlushAsync();
        }

        var extendedLength = new FileInfo(walPath).Length;
        Assert.True(extendedLength > baselineLength);

        // Corrupt the CRC of the last frame
        using (var fs = new FileStream(walPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
        {
            fs.Seek(-4, SeekOrigin.End);
            var crcBuf = new byte[4];
            int read = fs.Read(crcBuf, 0, crcBuf.Length);
            Assert.Equal(4, read);
            crcBuf[0] ^= 0xFF; // flip a few bits to make CRC invalid
            fs.Seek(-4, SeekOrigin.End);
            fs.Write(crcBuf, 0, crcBuf.Length);
            fs.Flush();
        }

        await using (var db3 = new WalnutDatabase(dir, KeepWal, new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t3 = await db3.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            var ids = new HashSet<string>();
            await foreach (var doc in t3.GetAllAsync()) ids.Add(doc.Id);
            Assert.Equal(10, ids.Count);
            Assert.DoesNotContain("corrupted", ids);
        }

        var finalLength = new FileInfo(walPath).Length;
        Assert.Equal(baselineLength, finalLength);
    }

    [Fact]
    public async Task Recovery_Truncation_Repositions_Writer_For_New_Appends()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using (var db = new WalnutDatabase(dir, KeepWal, new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t = await db.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            await t.UpsertAsync(new CorruptDoc { Id = "before" });
            await db.FlushAsync();
        }

        var baseline = new FileInfo(walPath).Length;

        // Append a torn frame so recovery trims the WAL.
        await using (var fs = new FileStream(walPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
        {
            var lenBuf = new byte[4];
            WriteUInt32LE(lenBuf, 512u);
            await fs.WriteAsync(lenBuf, 0, lenBuf.Length);
            await fs.FlushAsync();
        }

        await using (var db2 = new WalnutDatabase(dir, KeepWal, new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t2 = await db2.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            var docs = await MaterializeAsync(t2.GetAllAsync());
            Assert.Single(docs);
            Assert.Equal("before", docs[0].Id);

            await t2.UpsertAsync(new CorruptDoc { Id = "after" });
            await db2.FlushAsync();
        }

        Assert.True(new FileInfo(walPath).Length > baseline);

        await using (var db3 = new WalnutDatabase(dir, KeepWal, new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t3 = await db3.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            var ids = (await MaterializeAsync(t3.GetAllAsync())).Select(d => d.Id).OrderBy(id => id).ToArray();
            Assert.Equal(new[] { "after", "before" }, ids);
        }
    }

    private static void WriteUInt32LE(byte[] buffer, uint value)
    {
        buffer[0] = (byte)(value & 0xFF);
        buffer[1] = (byte)((value >> 8) & 0xFF);
        buffer[2] = (byte)((value >> 16) & 0xFF);
        buffer[3] = (byte)((value >> 24) & 0xFF);
    }

    private static async Task<List<T>> MaterializeAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var item in source)
            list.Add(item);
        return list;
    }
}
