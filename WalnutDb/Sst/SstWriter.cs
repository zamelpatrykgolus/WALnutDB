// SstWriter.cs
#nullable enable
namespace WalnutDb.Sst
{
    internal static class SstWriter
    {
        private static readonly byte[] Header = new byte[] { (byte)'S', (byte)'S', (byte)'T', (byte)'v', (byte)'1', 0, 0, 0 };

        public static async ValueTask WriteAsync(string path, IAsyncEnumerable<(byte[] Key, byte[] Val)> sorted, CancellationToken ct = default, Action<string>? fault = null)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            var idxPath = path + ".sxi";
            // Never allow a sidecar left by an interrupted older attempt to be
            // promoted for newly written SST contents.
            if (File.Exists(idxPath))
                File.Delete(idxPath);
            using var fs = new FileStream(path, new FileStreamOptions
            {
                Mode = FileMode.Create,
                Access = FileAccess.Write,
                Share = FileShare.Read,
                Options = FileOptions.SequentialScan,
                BufferSize = 64 * 1024
            });

            fault?.Invoke("segment.before-write");
            await fs.WriteAsync(Header, 0, Header.Length, ct).ConfigureAwait(false);

            uint count = 0;
            var lenBuf = new byte[8]; // [kLen(4) vLen(4)] LE

            // —— zbieranie indeksu rzadkiego ——
            var idx = new List<(byte[] Key, long Offset)>(1024);

            await foreach (var (k, v) in sorted.WithCancellation(ct))
            {
                // offset na początek rekordu (przed [kLen vLen])
                long offset = fs.Position;

                WriteUInt32LE(lenBuf, 0, (uint)k.Length);
                WriteUInt32LE(lenBuf, 4, (uint)v.Length);

                await fs.WriteAsync(lenBuf, 0, 8, ct).ConfigureAwait(false);
                await fs.WriteAsync(k, 0, k.Length, ct).ConfigureAwait(false);
                await fs.WriteAsync(v, 0, v.Length, ct).ConfigureAwait(false);
                count++;
                fault?.Invoke("segment.after-record");

                // co N-ty rekord – kotwica indeksu
                if ((count % SstIndex.DefaultStride) == 0)
                {
                    // kopiujemy klucz, bo bufor źródłowy może zostać nadpisany
                    var kk = new byte[k.Length];
                    Buffer.BlockCopy(k, 0, kk, 0, k.Length);
                    idx.Add((kk, offset));
                }
            }

            var trailer = new byte[4];
            WriteUInt32LE(trailer, 0, count);
            await fs.WriteAsync(trailer, 0, 4, ct).ConfigureAwait(false);
            fault?.Invoke("segment.before-sync");
            fs.Flush(true);
            fault?.Invoke("segment.after-sync");

            // —— zapisz indeks poboczny (best-effort) ——
            try
            {
                await SstIndex.WriteAsync(idxPath, idx, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                WalnutLogger.Exception(ex);
                try { if (File.Exists(idxPath)) File.Delete(idxPath); } catch { /* optional sidecar */ }
                // indeks jest opcjonalny; w razie błędu po prostu go pomijamy
            }
        }

        private static void WriteUInt32LE(byte[] buf, int offset, uint value)
        {
            buf[offset + 0] = (byte)(value & 0xFF);
            buf[offset + 1] = (byte)((value >> 8) & 0xFF);
            buf[offset + 2] = (byte)((value >> 16) & 0xFF);
            buf[offset + 3] = (byte)((value >> 24) & 0xFF);
        }
    }
}
