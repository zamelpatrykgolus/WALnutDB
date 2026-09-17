#nullable enable
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using WalnutDb;

namespace WalnutDb.Core;

/// <summary>
/// Proste odtworzenie z WAL: czyta wal.log, weryfikuje CRC każdej ramki,
/// zbiera operacje per TxId i aplikuje je do MemTable dopiero po COMMIT.
/// Na pierwszej niepełnej/zepsutej ramce – kończy replay (symulacja „crash tail”).
/// </summary>
internal static class WalRecovery
{
    public static void Replay(string walPath,
                              ConcurrentDictionary<string, MemTable> tables,
                              ISet<string> droppedTables,
                              IEncryption? encryption = null,
                              Wal.WalPosition? covered = null)
    {
        if (!File.Exists(walPath)) return;

        using var fs = new FileStream(walPath, new FileStreamOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.ReadWrite,
            Share = FileShare.ReadWrite,          // współdziel z WalWriterem i pozwól na przycięcie
            Options = FileOptions.SequentialScan
        });

        var epoch = Wal.WalEpoch.Read(fs);
        if (covered is not null && epoch.Epoch == Guid.Empty && fs.Length > 0)
        {
            // During metadata-only migration a v2 catalog can still reference a
            // legacy WAL. It must start with an intact legacy BEGIN, not with a
            // damaged epoch header reinterpreted as a discardable torn tail.
            fs.Position = 0;
            var begin = new byte[25];
            if (fs.ReadAtLeast(begin, begin.Length, throwOnEndOfStream: false) != begin.Length ||
                BinaryPrimitives.ReadInt32LittleEndian(begin) != 17 || begin[4] != (byte)Wal.WalOp.Begin ||
                BinaryPrimitives.ReadUInt32LittleEndian(begin.AsSpan(21)) != new Crc32().Compute(begin.AsSpan(4, 17)))
                throw new InvalidDataException("Unknown or damaged WAL header in storage v2; refusing destructive tail repair.");
        }
        fs.Position = epoch.Offset;
        if (covered is { } boundary && boundary.Epoch == epoch.Epoch)
        {
            if (boundary.Offset < epoch.Offset || boundary.Offset > fs.Length)
                throw new InvalidDataException("WAL is shorter than its durable checkpoint boundary.");
            fs.Position = boundary.Offset;
        }
        var crc = new Crc32();

        // Bufory przeniesione poza pętlę (CA2014)
        Span<byte> lenBuf = stackalloc byte[4];
        Span<byte> crcBuf = stackalloc byte[4];

        // TxId -> lista operacji do zastosowania przy commit
        var pending = new Dictionary<ulong, List<Action>>();
        long lastGoodPosition = fs.Position;
        bool truncateTail = false;
        bool issueMayBeTail = true;

        string? truncateReason = null;
        while (fs.Position + 8 <= fs.Length) // min: len(4)+crc(4)
        {
            long frameStart = fs.Position;
            // len
            if (!TryReadExactly(fs, lenBuf))
            {
                truncateReason = $"unexpected EOF while reading frame length at offset {frameStart}";
                truncateTail = true;
                break;
            }
            uint len = BinaryPrimitives.ReadUInt32LittleEndian(lenBuf);
            if (len == 0 || len > int.MaxValue || len > fs.Length - fs.Position - 4)
            {
                truncateReason = $"frame length {len} at offset {frameStart} exceeds remaining file size";
                truncateTail = true;
                issueMayBeTail = !ContainsValidFrameAfter(fs, frameStart + 1, crc);
                break; // niepełna ramka → przerwij
            }

            // payload
            var payload = new byte[(int)len];
            if (!TryReadExactly(fs, payload))
            {
                truncateReason = $"unexpected EOF while reading payload (len={len}) at offset {frameStart + 4}";
                truncateTail = true;
                break;
            }

            // crc
            if (!TryReadExactly(fs, crcBuf))
            {
                truncateReason = $"unexpected EOF while reading CRC at offset {frameStart + 4 + len}";
                truncateTail = true;
                break;
            }
            uint fileCrc = BinaryPrimitives.ReadUInt32LittleEndian(crcBuf);
            uint calcCrc = crc.Compute(payload);
            if (fileCrc != calcCrc)
            {
                truncateReason = $"CRC mismatch at offset {frameStart}: stored=0x{fileCrc:X8}, computed=0x{calcCrc:X8}";
                truncateTail = true;
                issueMayBeTail = fs.Position == fs.Length;
                break; // uszkodzona ramka → przerwij
            }

            // parse payload
            var span = payload.AsSpan();
            byte op = span[0];

            switch ((Wal.WalOp)op)
            {
                case Wal.WalOp.Begin:
                    {
                        if (span.Length < 1 + 8 + 8)
                        {
                            truncateReason = $"BEGIN frame too short ({span.Length} bytes) at offset {frameStart}";
                            truncateTail = true;
                            issueMayBeTail = false;
                            break;
                        }
                        ulong txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        if (!pending.ContainsKey(txId))
                            pending[txId] = new List<Action>(8);
                        break;
                    }
                case Wal.WalOp.Put:
                    {
                        if (span.Length < 1 + 8 + 2 + 4 + 4)
                        {
                            truncateReason = $"PUT frame too short ({span.Length} bytes) at offset {frameStart}";
                            truncateTail = true;
                            issueMayBeTail = false;
                            break;
                        }
                        ulong txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        ushort tlen = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(9, 2));
                        int klen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(11, 4));
                        int vlen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(15, 4));
                        int off = 19;
                        if (klen < 0 || vlen < 0 || (long)off + tlen + klen + vlen != span.Length)
                        {
                            truncateReason = $"PUT frame payload truncated at offset {frameStart}";
                            truncateTail = true;
                            issueMayBeTail = false;
                            break;
                        }

                        string table = Encoding.UTF8.GetString(span.Slice(off, tlen));
                        off += tlen;
                        var key = span.Slice(off, klen).ToArray(); off += klen;
                        var val = span.Slice(off, vlen).ToArray();

                        // Secondary-index PUTs intentionally carry an empty
                        // value. Document ciphertext is never empty (it has a
                        // version, nonce and tag), so this preserves encrypted
                        // document recovery without trying to decrypt indexes.
                        if (encryption is not null && val.Length > 0)
                            val = encryption.Decrypt(val, table, key);

                        if (!pending.TryGetValue(txId, out var list))
                            list = pending[txId] = new List<Action>(8);

                        list.Add(() =>
                        {
                            var mem = tables.GetOrAdd(table, _ => new MemTable());
                            mem.Upsert(key, val);
                        });
                        break;
                    }
                case Wal.WalOp.Delete:
                    {
                        if (span.Length < 1 + 8 + 2 + 4)
                        {
                            truncateReason = $"DELETE frame too short ({span.Length} bytes) at offset {frameStart}";
                            truncateTail = true;
                            issueMayBeTail = false;
                            break;
                        }
                        ulong txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        ushort tlen = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(9, 2));
                        int klen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(11, 4));
                        int off = 15;
                        if (klen < 0 || (long)off + tlen + klen != span.Length)
                        {
                            truncateReason = $"DELETE frame payload truncated at offset {frameStart}";
                            truncateTail = true;
                            issueMayBeTail = false;
                            break;
                        }

                        string table = Encoding.UTF8.GetString(span.Slice(off, tlen));
                        off += tlen;
                        var key = span.Slice(off, klen).ToArray();

                        if (!pending.TryGetValue(txId, out var list))
                            list = pending[txId] = new List<Action>(8);

                        list.Add(() =>
                        {
                            var mem = tables.GetOrAdd(table, _ => new MemTable());
                            mem.Delete(key);
                        });
                        break;
                    }
                case Wal.WalOp.DropTable:
                    {
                        if (span.Length < 1 + 8 + 2)
                        {
                            truncateReason = $"DROP frame too short ({span.Length} bytes) at offset {frameStart}";
                            truncateTail = true;
                            issueMayBeTail = false;
                            break;
                        }

                        ulong txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        ushort tlen = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(9, 2));
                        int off = 11;
                        if ((long)off + tlen != span.Length)
                        {
                            truncateReason = $"DROP frame payload truncated at offset {frameStart}";
                            truncateTail = true;
                            issueMayBeTail = false;
                            break;
                        }

                        string table = Encoding.UTF8.GetString(span.Slice(off, tlen));

                        if (!pending.TryGetValue(txId, out var list))
                            list = pending[txId] = new List<Action>(8);

                        list.Add(() => DropRecoveredTable(tables, droppedTables, table));
                        break;
                    }
                case Wal.WalOp.Commit:
                    {
                        if (span.Length < 1 + 8 + 4)
                        {
                            truncateReason = $"COMMIT frame too short ({span.Length} bytes) at offset {frameStart}";
                            truncateTail = true;
                            issueMayBeTail = false;
                            break;
                        }
                        ulong txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        int opsCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(9, 4));

                        if (pending.TryGetValue(txId, out var list))
                        {
                            if (opsCount < 0 || list.Count != opsCount)
                            {
                                truncateReason = $"COMMIT operation count mismatch for transaction {txId}: declared={opsCount}, observed={list.Count}";
                                truncateTail = true;
                                issueMayBeTail = false;
                                break;
                            }
                            foreach (var act in list) act();
                            pending.Remove(txId);
                        }
                        lastGoodPosition = fs.Position;
                        break;
                    }
                default:
                    // nieznana ramka → bezpiecznie zatrzymać się
                    truncateReason = $"unknown WAL opcode 0x{op:X2} at offset {frameStart}";
                    truncateTail = true;
                    issueMayBeTail = false;
                    fs.Position = fs.Length;
                    break;
            }
            if (truncateTail)
            {
                break;
            }
        }

        // Transakcje bez COMMIT pozostają w pending i są ignorowane — to OK.

        if (!truncateTail && pending.Count > 0)
        {
            truncateTail = true;
            truncateReason ??= $"dangling {pending.Count} transaction(s) without COMMIT";
        }

        if (!truncateTail && fs.Position < fs.Length)
        {
            truncateTail = true;
            truncateReason = $"trailing {fs.Length - fs.Position} byte(s) after last complete frame";
        }

        if (truncateTail)
        {
            if (!issueMayBeTail)
                throw new InvalidDataException($"WAL corruption is not confined to an incomplete tail: {truncateReason ?? "unknown reason"}. The WAL was left unchanged.");

            long before = fs.Length;
            if (lastGoodPosition < before)
            {
                WalnutLogger.Warning($"Truncating WAL tail: {truncateReason ?? "unknown reason"} (from {before} to {lastGoodPosition} bytes)");
                try
                {
                    fs.Position = lastGoodPosition;
                    fs.SetLength(lastGoodPosition);
                    fs.Flush(true);
                }
                catch (Exception ex)
                {
                    WalnutLogger.Exception(ex);
                    throw new IOException("Cannot durably repair WAL tail.", ex);
                }
            }
            else
            {
                WalnutLogger.Warning($"Detected WAL tail issue but nothing to truncate: {truncateReason ?? "unknown reason"} (length {before} bytes)");
            }
        }
    }

    private static bool TryReadExactly(Stream s, Span<byte> dst)
    {
        int readTotal = 0;
        while (readTotal < dst.Length)
        {
            int r = s.Read(dst.Slice(readTotal));
            if (r == 0) return false;
            readTotal += r;
        }
        return true;
    }

    private static bool TryReadExactly(Stream s, byte[] dst)
        => TryReadExactly(s, dst.AsSpan());

    private static bool ContainsValidFrameAfter(FileStream fs, long searchStart, Crc32 crc)
    {
        long original = fs.Position;
        try
        {
            Span<byte> lenBytes = stackalloc byte[4];
            Span<byte> crcBytes = stackalloc byte[4];
            for (long candidate = searchStart; candidate + 9 <= fs.Length; candidate++)
            {
                fs.Position = candidate;
                if (!TryReadExactly(fs, lenBytes)) return false;
                uint length = BinaryPrimitives.ReadUInt32LittleEndian(lenBytes);
                if (length == 0 || length > int.MaxValue || length > fs.Length - candidate - 8)
                    continue;

                int first = fs.ReadByte();
                if (first != (int)Wal.WalOp.Begin &&
                    first != (int)Wal.WalOp.Put &&
                    first != (int)Wal.WalOp.Delete &&
                    first != (int)Wal.WalOp.DropTable &&
                    first != (int)Wal.WalOp.Commit)
                    continue;

                fs.Position = candidate + 4;
                var payload = new byte[(int)length];
                if (!TryReadExactly(fs, payload) || !TryReadExactly(fs, crcBytes))
                    continue;

                if (BinaryPrimitives.ReadUInt32LittleEndian(crcBytes) == crc.Compute(payload))
                    return true;
            }

            return false;
        }
        finally
        {
            fs.Position = original;
        }
    }

    private static void DropRecoveredTable(ConcurrentDictionary<string, MemTable> tables, ISet<string> droppedTables, string canonicalName)
    {
        tables.TryRemove(canonicalName, out _);

        var idxPrefix = $"__index__{canonicalName}__";
        var toRemove = tables.Keys
            .Where(n => n.StartsWith(idxPrefix, StringComparison.Ordinal))
            .ToArray();

        foreach (var idx in toRemove)
            tables.TryRemove(idx, out _);

        droppedTables.Add(canonicalName);
    }

    // lokalny CRC32 (polinom 0xEDB88320)
    private sealed class Crc32
    {
        private readonly uint[] _table = new uint[256];
        public Crc32()
        {
            const uint poly = 0xEDB88320u;
            for (uint i = 0; i < 256; i++)
            {
                uint c = i;
                for (int k = 0; k < 8; k++) c = ((c & 1) != 0) ? (poly ^ (c >> 1)) : (c >> 1);
                _table[i] = c;
            }
        }
        public uint Compute(ReadOnlySpan<byte> data)
        {
            uint c = 0xFFFF_FFFFu;
            foreach (var b in data) c = _table[(c ^ b) & 0xFF] ^ (c >> 8);
            return c ^ 0xFFFF_FFFFu;
        }
    }
}
