// SstReader.cs
#nullable enable
using System.Buffers.Binary;

namespace WalnutDb.Sst
{
    internal sealed class SstReader : ITableReader
    {
        private static readonly byte[] Header = new byte[] { (byte)'S', (byte)'S', (byte)'T', (byte)'v', (byte)'1', 0, 0, 0 };

        public string Path { get; }
        private readonly FileStream _source;
        private readonly object _leaseLock = new();
        private int _activeReaders;
        private bool _retired;
        private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private IDisposable Pin()
        {
            lock (_leaseLock)
            {
                if (_retired) throw new ObjectDisposedException(nameof(SstReader));
                _activeReaders++;
                return new ReaderLease(this);
            }
        }
        private sealed class ReaderLease(SstReader owner) : IDisposable
        {
            public void Dispose() { lock (owner._leaseLock) { if (--owner._activeReaders == 0 && owner._retired) { owner._source.Dispose(); owner._drained.TrySetResult(); } } }
        }
        public Task Retire()
        {
            lock (_leaseLock) { _retired = true; if (_activeReaders == 0) { _source.Dispose(); _drained.TrySetResult(); } return _drained.Task; }
        }

        // —— indeks poboczny (opcjonalny) ——
        private readonly byte[][]? _idxKeys;
        private readonly long[]? _idxOffsets;

        public SstReader(string path)
        {
            Path = path ?? throw new ArgumentNullException(nameof(path));
            _source = new FileStream(Path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete,
                4096, FileOptions.RandomAccess);

            try
            {
            using var fs = OpenRead();
            if (fs.Length < Header.Length + 4)
                throw new InvalidDataException("SST file is shorter than its header and trailer.");
            var hdr = new byte[Header.Length];

            if (fs.Read(hdr, 0, hdr.Length) != hdr.Length || !hdr.AsSpan().SequenceEqual(Header))
                throw new InvalidDataException("Invalid SST header.");

            ValidateLayout(fs);

            // spróbuj wczytać .sxi
            try
            {
                var idx = SstIndex.TryLoad(path + ".sxi");
                if (idx is not null)
                {
                    if (idx.Value.Offsets.Any(offset => offset < Header.Length || offset >= fs.Length - 4))
                        throw new InvalidDataException("SST sidecar contains an offset outside the data file.");
                    _idxKeys = idx.Value.Keys;
                    _idxOffsets = idx.Value.Offsets;
                }
            }
            catch (Exception ex)
            {
                WalnutLogger.Exception(ex);
                _idxKeys = null;
                _idxOffsets = null;
            }
            }
            catch { _source.Dispose(); throw; }
        }

        public bool TryGet(ReadOnlySpan<byte> key, out byte[]? value)
        {
            using var lease = Pin();
            value = null;

            using var fs = OpenRead();
            fs.Position = Header.Length;
            long endPos = fs.Length - 4;

            var len = new byte[8];

            while (fs.Position < endPos)
            {
                if (endPos - fs.Position < 8)
                    throw new InvalidDataException($"Truncated SST record header at offset {fs.Position}.");
                if (fs.Read(len, 0, 8) != 8) 
                    throw new InvalidDataException("Truncated SST record header.");

                uint klen = BinaryPrimitives.ReadUInt32LittleEndian(len.AsSpan(0, 4));
                uint vlen = BinaryPrimitives.ReadUInt32LittleEndian(len.AsSpan(4, 4));

                if (klen > int.MaxValue || vlen > int.MaxValue ||
                    (long)klen + vlen > endPos - fs.Position)
                    throw new InvalidDataException($"Invalid SST record length at offset {fs.Position - 8}.");

                var kbuf = new byte[(int)klen];
                var vbuf = new byte[(int)vlen];

                if (fs.Read(kbuf, 0, kbuf.Length) != kbuf.Length)
                    throw new InvalidDataException("Truncated SST key.");
                if (fs.Read(vbuf, 0, vbuf.Length) != vbuf.Length)
                    throw new InvalidDataException("Truncated SST value.");

                int cmp = ByteCompare(kbuf, key);
                if (cmp == 0)
                {
                    value = vbuf;
                    return true;
                }

                if (cmp > 0)
                {
                    return false;
                } // sortowane
            }
            return false;
        }

        public System.Collections.Generic.IEnumerable<(byte[] Key, byte[] Val)> ScanRange(byte[]? fromInclusive, byte[]? toExclusive)
        {
            using var lease = Pin();
            using var fs = OpenRead();

            // —— jeśli mamy indeks, przeskocz od razu do okolic fromInclusive ——
            if (fromInclusive is { Length: > 0 } && _idxKeys is not null && _idxOffsets is not null && _idxKeys.Length > 0)
            {
                int lb = SstIndex.LowerBound(_idxKeys, fromInclusive);
                long pos = (lb <= 0) ? Header.Length : _idxOffsets[lb - 1];
                fs.Position = Math.Max(pos, Header.Length);
            }
            else
            {
                fs.Position = Header.Length;
            }

            long endPos = fs.Length - 4;
            var len = new byte[8];

            var from = fromInclusive ?? Array.Empty<byte>();
            var to = toExclusive ?? Array.Empty<byte>();
            bool inRange = from.Length == 0;

            while (fs.Position < endPos)
            {
                if (endPos - fs.Position < 8)
                    throw new InvalidDataException($"Truncated SST record header at offset {fs.Position}.");
                if (fs.Read(len, 0, 8) != 8) 
                    throw new InvalidDataException("Truncated SST record header.");

                uint klen = BinaryPrimitives.ReadUInt32LittleEndian(len.AsSpan(0, 4));
                uint vlen = BinaryPrimitives.ReadUInt32LittleEndian(len.AsSpan(4, 4));

                if (klen > int.MaxValue || vlen > int.MaxValue ||
                    (long)klen + vlen > endPos - fs.Position)
                    throw new InvalidDataException($"Invalid SST record length at offset {fs.Position - 8}.");

                var kbuf = new byte[(int)klen];
                var vbuf = new byte[(int)vlen];

                if (fs.Read(kbuf, 0, kbuf.Length) != kbuf.Length)
                    throw new InvalidDataException("Truncated SST key.");

                if (fs.Read(vbuf, 0, vbuf.Length) != vbuf.Length)
                    throw new InvalidDataException("Truncated SST value.");

                if (!inRange)
                    inRange = ByteCompare(kbuf, from) >= 0;

                if (inRange)
                {
                    if (to.Length != 0 && ByteCompare(kbuf, to) >= 0)
                        yield break;

                    yield return (kbuf, vbuf);
                }
            }
        }

        private Stream OpenRead() => new BufferedStream(new RandomAccessReadStream(_source.SafeFileHandle), 64 * 1024);

        private static void ValidateLayout(Stream fs)
        {
            long endPos = fs.Length - 4;
            fs.Position = Header.Length;
            Span<byte> lengths = stackalloc byte[8];
            uint actualCount = 0;

            while (fs.Position < endPos)
            {
                long recordOffset = fs.Position;
                if (endPos - recordOffset < 8 || fs.Read(lengths) != 8)
                    throw new InvalidDataException($"Truncated SST record header at offset {recordOffset}.");

                uint keyLength = BinaryPrimitives.ReadUInt32LittleEndian(lengths[..4]);
                uint valueLength = BinaryPrimitives.ReadUInt32LittleEndian(lengths[4..]);
                long payloadLength = (long)keyLength + valueLength;
                if (keyLength > int.MaxValue || valueLength > int.MaxValue || payloadLength > endPos - fs.Position)
                    throw new InvalidDataException($"Invalid SST record length at offset {recordOffset}.");

                fs.Position += payloadLength;
                actualCount++;
            }

            if (fs.Position != endPos)
                throw new InvalidDataException("SST records do not end at the trailer boundary.");

            Span<byte> trailer = stackalloc byte[4];
            if (fs.Read(trailer) != 4)
                throw new InvalidDataException("Truncated SST trailer.");
            uint declaredCount = BinaryPrimitives.ReadUInt32LittleEndian(trailer);
            if (actualCount != declaredCount)
                throw new InvalidDataException($"SST record count mismatch: declared={declaredCount}, observed={actualCount}.");
        }

        private static int ByteCompare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
        {
            int n = Math.Min(a.Length, b.Length);
            for (int i = 0; i < n; i++)
            {
                int d = a[i] - b[i];
                if (d != 0) return d;
            }
            return a.Length - b.Length;
        }

        public void Dispose() => Retire();
    }
}
