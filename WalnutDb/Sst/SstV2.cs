using System.Buffers.Binary;
using System.Diagnostics;
using WalnutDb.Core;
using WalnutDb.Storage;

namespace WalnutDb.Sst;

internal readonly record struct SegmentRow(byte[] Key, byte[] Value, bool Deleted);

// SSTv2: magic(8), records [kind:u8,keyLen:i32,valueLen:i32,key,value,crc32:u32],
// footer [kind=0,count:i64,crc32(count):u32]. Empty values are NOT deletes.
internal static class SstV2
{
    internal static readonly byte[] Magic = "SSTv2\0\0\0"u8.ToArray();
    internal static readonly Wal.Crc32 Crc = new();

    public static async ValueTask<long> WriteAsync(string path, IEnumerable<SegmentRow> rows,
        long maxBytes, long bytesPerSecond, Action<string>? fault, CancellationToken ct)
    {
        using var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.Read,
            64 * 1024, FileOptions.SequentialScan);
        fault?.Invoke("segment.before-write");
        await fs.WriteAsync(Magic, ct).ConfigureAwait(false);
        long count = 0;
        byte[]? previous = null;
        var watch = Stopwatch.StartNew();
        foreach (var row in rows)
        {
            ct.ThrowIfCancellationRequested();
            if (previous is not null && ByteArrayComparer.Instance.Compare(previous, row.Key) >= 0)
                throw new InvalidDataException("SSTv2 input keys must be strictly ordered.");
            var frame = Encode(row);
            if (fs.Position + frame.LongLength + 13 > maxBytes)
                throw new IOException("Segment output exceeds the configured maintenance byte budget.");
            await fs.WriteAsync(frame, ct).ConfigureAwait(false);
            fault?.Invoke("segment.after-record");
            previous = row.Key;
            count++;
            if (bytesPerSecond > 0)
            {
                var delay = TimeSpan.FromSeconds((double)fs.Position / bytesPerSecond) - watch.Elapsed;
                while (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay < TimeSpan.FromSeconds(1) ? delay : TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
                    delay = TimeSpan.FromSeconds((double)fs.Position / bytesPerSecond) - watch.Elapsed;
                }
            }
        }
        var footer = new byte[13];
        BinaryPrimitives.WriteInt64LittleEndian(footer.AsSpan(1), count);
        BinaryPrimitives.WriteUInt32LittleEndian(footer.AsSpan(9), Crc.Compute(footer.AsSpan(1, 8)));
        await fs.WriteAsync(footer, ct).ConfigureAwait(false);
        fault?.Invoke("segment.before-sync");
        fs.Flush(true);
        fault?.Invoke("segment.after-sync");
        return fs.Length;
    }

    private static byte[] Encode(SegmentRow row)
    {
        int valueLength = row.Deleted ? 0 : row.Value.Length;
        var result = new byte[checked(13 + row.Key.Length + valueLength)];
        result[0] = row.Deleted ? (byte)2 : (byte)1;
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(1), row.Key.Length);
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(5), valueLength);
        row.Key.CopyTo(result, 9);
        if (!row.Deleted) row.Value.CopyTo(result, 9 + row.Key.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(result.Length - 4), Crc.Compute(result.AsSpan(0, result.Length - 4)));
        return result;
    }
}

internal sealed class SegmentReader : IDisposable
{
    private readonly string _path;
    private readonly SegmentFile _file;
    private readonly SstReader? _legacy;
    private readonly List<byte[]> _keys = new();
    private readonly List<long> _offsets = new();

    public SegmentReader(string directory, SegmentFile file)
    {
        _file = file;
        _path = System.IO.Path.Combine(directory, file.FileName);
        if (new FileInfo(_path).Length != file.Bytes) throw new InvalidDataException("Segment length does not match manifest.");
        if (file.Format == 1) { _legacy = new SstReader(_path); return; }
        if (file.Format != 2) throw new InvalidDataException("Unsupported segment format.");
        long count = 0;
        foreach (var _ in ReadV2(8, (key, offset) => { if (count++ % 256 == 0) { _keys.Add(key); _offsets.Add(offset); } })) { }
    }

    public void Dispose() => _legacy?.Dispose();

    public IEnumerable<SegmentRow> Scan(byte[]? from = null, byte[]? to = null)
    {
        if (_legacy is not null)
        {
            foreach (var row in _legacy.ScanRange(from, to)) yield return new(row.Key, row.Val, false);
            yield break;
        }
        long start = 8;
        if (from is { Length: > 0 } && _keys.Count > 0)
        {
            int index = _keys.BinarySearch(from, ByteArrayComparer.Instance);
            if (index < 0) index = ~index - 1;
            if (index >= 0) start = _offsets[index];
        }
        foreach (var row in ReadV2(start))
        {
            if (from is { Length: > 0 } && ByteArrayComparer.Instance.Compare(row.Key, from) < 0) continue;
            if (to is { Length: > 0 } && ByteArrayComparer.Instance.Compare(row.Key, to) >= 0) yield break;
            yield return row;
        }
    }

    private IEnumerable<SegmentRow> ReadV2(long start, Action<byte[], long>? anchor = null)
    {
        using var fs = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, 64 * 1024);
        if (fs.Length != _file.Bytes || fs.Length < 21) throw new InvalidDataException("Truncated SSTv2.");
        var header = new byte[8]; fs.ReadExactly(header);
        if (!header.AsSpan().SequenceEqual(SstV2.Magic)) throw new InvalidDataException("Invalid SSTv2 header.");
        fs.Position = start;
        long count = 0;
        byte[]? previous = null;
        while (true)
        {
            long offset = fs.Position;
            int kind = fs.ReadByte();
            if (kind == 0)
            {
                var footer = new byte[12]; fs.ReadExactly(footer);
                long expected = BinaryPrimitives.ReadInt64LittleEndian(footer);
                if (fs.Position != fs.Length || expected < 0 || (start == 8 && expected != count) ||
                    BinaryPrimitives.ReadUInt32LittleEndian(footer.AsSpan(8)) != SstV2.Crc.Compute(footer.AsSpan(0, 8)))
                    throw new InvalidDataException("Invalid SSTv2 footer.");
                yield break;
            }
            if (kind is not (1 or 2)) throw new InvalidDataException("Invalid SSTv2 record kind.");
            var lengths = new byte[8]; fs.ReadExactly(lengths);
            int keyLength = BinaryPrimitives.ReadInt32LittleEndian(lengths);
            int valueLength = BinaryPrimitives.ReadInt32LittleEndian(lengths.AsSpan(4));
            if (keyLength < 0 || valueLength < 0 || (kind == 2 && valueLength != 0) ||
                (long)keyLength + valueLength > fs.Length - fs.Position - 17 || (long)keyLength + valueLength > int.MaxValue - 13)
                throw new InvalidDataException("Invalid SSTv2 record lengths.");
            var frame = new byte[13 + keyLength + valueLength];
            frame[0] = (byte)kind; lengths.CopyTo(frame, 1);
            fs.ReadExactly(frame.AsSpan(9));
            if (BinaryPrimitives.ReadUInt32LittleEndian(frame.AsSpan(frame.Length - 4)) != SstV2.Crc.Compute(frame.AsSpan(0, frame.Length - 4)))
                throw new InvalidDataException("SSTv2 record checksum mismatch.");
            var key = frame.AsSpan(9, keyLength).ToArray();
            if (previous is not null && ByteArrayComparer.Instance.Compare(previous, key) >= 0)
                throw new InvalidDataException("SSTv2 keys are not strictly ordered.");
            previous = key;
            anchor?.Invoke(key, offset);
            count++;
            yield return new(key, frame.AsSpan(9 + keyLength, valueLength).ToArray(), kind == 2);
        }
    }
}
