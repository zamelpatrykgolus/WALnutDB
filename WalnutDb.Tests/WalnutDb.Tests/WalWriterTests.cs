#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;

using WalnutDb;
using WalnutDb.Wal;

using Xunit;

namespace WalnutDb.Tests;

public sealed class WalWriterTests
{
    [Fact]
    public async Task ReopenedWal_FirstBarrierSyncsExistingData_ThenNoOpFlushSkipsRedundantSync()
    {
        var dir = NewTempDir();
        var path = Path.Combine(dir, "wal.log");
        await using (var first = new WalWriter(path))
        {
            var handle = await first.AppendTransactionAsync(SimpleTx(), Durability.Fast);
            await handle.WhenCommitted;
        }
        await using var reopened = new WalWriter(path);
        await reopened.FlushAsync();
        Assert.Equal(1, reopened.DurableFlushCount);
        await reopened.FlushAsync();
        Assert.Equal(1, reopened.DurableFlushCount);
    }

    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static ReadOnlyMemory<byte> Frame(params byte[] bytes) => new(bytes);

    private static List<ReadOnlyMemory<byte>> SimpleTx()
    {
        return new()
        {
            Frame((byte)WalOp.Begin),
            Frame((byte)WalOp.Put, 0x01, 0x02, 0x03),
            Frame((byte)WalOp.Commit)
        };
    }

    [Fact]
    public async Task WalWriter_WritesFramesWithCommit_AndFlushes()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using (var writer = new WalWriter(walPath, groupWindow: TimeSpan.FromMilliseconds(5), maxBatch: 64))
        {
            var tx1 = await writer.AppendTransactionAsync(SimpleTx(), Durability.Safe);
            var tx2 = await writer.AppendTransactionAsync(SimpleTx(), Durability.Safe);

            await tx1.WhenCommitted;
            await tx2.WhenCommitted;

            // dodatkowy flush (no-op jeśli już flushnięte)
            await writer.FlushAsync();
        }

        // WAL powinien zawierać 6 ramek (2x3). Zweryfikujmy strukturę i ostatni COMMIT.
        var frames = ReadWalFrames(walPath);
        Assert.Equal(6, frames.Count);
        Assert.Equal((byte)WalOp.Commit, frames[^1].Span[0]);
    }

    [Fact]
    public async Task WalWriter_FlushAsync_DoesNotThrow_AndDataReadable()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using (var writer = new WalWriter(walPath))
        {
            var tx = await writer.AppendTransactionAsync(SimpleTx(), Durability.Safe);
            await tx.WhenCommitted;
            await writer.FlushAsync();
        }

        var frames = ReadWalFrames(walPath);
        Assert.True(frames.Count >= 3);
        Assert.Equal((byte)WalOp.Begin, frames[0].Span[0]);
        Assert.Equal((byte)WalOp.Commit, frames[frames.Count - 1].Span[0]);
    }

    [Fact]
    public async Task WalWriter_FaultedLoop_FailsFlushInsteadOfWaitingForever()
    {
        var dir = NewTempDir();
        await using var writer = new WalWriter(Path.Combine(dir, "wal.log"));
        var field = typeof(WalWriter).GetField("_fs", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        ((FileStream)field!.GetValue(writer)!).Dispose();

        var handle = await writer.AppendTransactionAsync(SimpleTx(), Durability.Safe);
        await Assert.ThrowsAnyAsync<Exception>(() => handle.WhenCommitted.AsTask().WaitAsync(TimeSpan.FromSeconds(2)));
        await Assert.ThrowsAsync<IOException>(() => writer.FlushAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2)));
    }

    private static List<ReadOnlyMemory<byte>> ReadWalFrames(string path)
    {
        var list = new List<ReadOnlyMemory<byte>>();
        using var fs = File.OpenRead(path);
        Span<byte> lenBuf = stackalloc byte[4];
        Span<byte> crcBuf = stackalloc byte[4];
        while (fs.Position + 8 <= fs.Length)
        {
            // len
            var r1 = fs.Read(lenBuf);
            if (r1 != 4) break;
            uint len = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(lenBuf);
            if (len > (fs.Length - fs.Position - 4)) break; // niepełna ramka (buforowany ogon)

            var payload = new byte[len];
            var r2 = fs.Read(payload, 0, payload.Length);
            if (r2 != payload.Length) break;

            // crc (ignorujemy, ale przesuwamy)
            var r3 = fs.Read(crcBuf);
            if (r3 != 4) break;

            list.Add(payload);
        }
        return list;
    }
}
