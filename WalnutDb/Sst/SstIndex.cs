// src/WalnutDb/Sst/SstIndex.cs
#nullable enable
using System.Buffers.Binary;

namespace WalnutDb.Sst
{
    internal static class SstIndex
    {
        internal const int DefaultStride = 1024; // co ile rekordów łapać „kotwicę”

        internal static async ValueTask WriteAsync(string indexPath, List<(byte[] Key, long Offset)> entries, CancellationToken ct = default)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(indexPath)!);

            using var fs = new FileStream(indexPath, new FileStreamOptions
            {
                Mode = FileMode.Create,
                Access = FileAccess.Write,
                Share = FileShare.Read,
                Options = FileOptions.WriteThrough
            });

            var u32 = new byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(u32, (uint)entries.Count);
            await fs.WriteAsync(u32, ct).ConfigureAwait(false);

            foreach (var (k, off) in entries)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(u32, (uint)k.Length);
                await fs.WriteAsync(u32, ct).ConfigureAwait(false);
                await fs.WriteAsync(k, ct).ConfigureAwait(false);

                var i64 = new byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(i64, off);
                await fs.WriteAsync(i64, ct).ConfigureAwait(false);
            }

            fs.Flush(true);
        }

        internal static (byte[][] Keys, long[] Offsets)? TryLoad(string indexPath)
        {
            if (!File.Exists(indexPath)) return null;

            using var fs = new FileStream(indexPath, new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.ReadWrite,
                Options = FileOptions.SequentialScan
            });

            Span<byte> u32 = stackalloc byte[4];
            if (fs.Read(u32) != 4) return null;
            uint count = BinaryPrimitives.ReadUInt32LittleEndian(u32);
            if (count > int.MaxValue || count > (fs.Length - 4) / 12)
                return null;

            var keys = new byte[(int)count][];
            var offs = new long[(int)count];
            Span<byte> i64 = stackalloc byte[8];

            for (uint i = 0; i < count; i++)
            {
                if (fs.Read(u32) != 4) return null;
                uint klen = BinaryPrimitives.ReadUInt32LittleEndian(u32);
                if (klen > int.MaxValue || klen > fs.Length - fs.Position - 8)
                    return null;

                var k = new byte[(int)klen];
                if (fs.Read(k, 0, (int)klen) != (int)klen) return null;

                if (fs.Read(i64) != 8) return null;
                long off = BinaryPrimitives.ReadInt64LittleEndian(i64);

                keys[(int)i] = k;
                offs[(int)i] = off;
            }

            return (keys, offs);
        }

        internal static int LowerBound(byte[][] keys, ReadOnlySpan<byte> target)
        {
            int lo = 0, hi = keys.Length;
            while (lo < hi)
            {
                int mid = (lo + hi) >> 1;
                int cmp = ByteCompare(keys[mid], target);
                if (cmp < 0) lo = mid + 1; else hi = mid;
            }
            return lo; // pierwsze >= target
        }

        internal static int ByteCompare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
        {
            int n = Math.Min(a.Length, b.Length);
            for (int i = 0; i < n; i++)
            {
                int d = a[i] - b[i];
                if (d != 0) return d;
            }
            return a.Length - b.Length;
        }
    }
}
