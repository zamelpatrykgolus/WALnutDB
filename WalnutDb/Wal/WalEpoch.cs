using System.Buffers.Binary;

namespace WalnutDb.Wal;

internal readonly record struct WalPosition(Guid Epoch, long Offset);

internal static class WalEpoch
{
    private static readonly byte[] Magic = "WALv2\0\0\0"u8.ToArray();
    public const int HeaderLength = 28;
    public static byte[] Header(Guid epoch)
    {
        var bytes = new byte[HeaderLength];
        Magic.CopyTo(bytes, 0);
        epoch.TryWriteBytes(bytes.AsSpan(8));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(24), new Crc32().Compute(bytes.AsSpan(0, 24)));
        return bytes;
    }
    public static WalPosition Read(Stream stream)
    {
        stream.Position = 0;
        Span<byte> header = stackalloc byte[HeaderLength];
        int count = stream.ReadAtLeast(header[..8], 8, throwOnEndOfStream: false);
        if (count < 8 || !header[..8].SequenceEqual(Magic)) return new(Guid.Empty, 0);
        if (stream.ReadAtLeast(header[8..], 20, throwOnEndOfStream: false) != 20)
            throw new InvalidDataException("Incomplete WAL epoch header.");
        if (BinaryPrimitives.ReadUInt32LittleEndian(header[24..]) != new Crc32().Compute(header[..24]))
            throw new InvalidDataException("WAL epoch header checksum mismatch.");
        var epoch = new Guid(header[8..24]);
        if (epoch == Guid.Empty) throw new InvalidDataException("Invalid WAL epoch.");
        return new(epoch, HeaderLength);
    }
}
