using Microsoft.Win32.SafeHandles;

namespace WalnutDb.Sst;

// Independent cursor over a pinned immutable file identity, not a mutable path.
internal sealed class RandomAccessReadStream(SafeFileHandle handle) : Stream
{
    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length => RandomAccess.GetLength(handle);
    public override long Position { get; set; }
    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));
    public override int Read(Span<byte> buffer)
    {
        int read = RandomAccess.Read(handle, buffer, Position);
        Position += read;
        return read;
    }
    public override long Seek(long offset, SeekOrigin origin) => Position = origin switch
    {
        SeekOrigin.Begin => offset, SeekOrigin.Current => Position + offset, SeekOrigin.End => Length + offset,
        _ => throw new ArgumentOutOfRangeException(nameof(origin))
    };
    public override void Flush() { }
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
