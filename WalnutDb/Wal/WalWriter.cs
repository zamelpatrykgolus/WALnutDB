#nullable enable
using System.Buffers.Binary;
using System.Diagnostics;
using System.Threading.Channels;

namespace WalnutDb.Wal;

internal sealed record WalItem(IReadOnlyList<ReadOnlyMemory<byte>> Frames,
                               Durability Durability,
                               TaskCompletionSource<bool> Promise);

/// <summary>
/// Draft implementacja IWalWriter: kolejka Channel → pętla writer-a z group-commit i jednym Flush(true) na batch.
/// Zgodna z .NET 8 / C# 12: brak użycia Span/stackalloc w metodach async.
/// </summary>
public sealed class WalWriter : IWalWriter
{
    private readonly FileStream _fs;
    private readonly Channel<WalItem> _queue;
    private readonly TimeSpan _groupWindow;
    private readonly int _maxBatch;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;
    private readonly Crc32 _crc = new();
    private readonly SemaphoreSlim _ioGate = new(1, 1); // serializacja operacji
    private int _disposeOnce;
    private int _faulted;
    private Exception? _faultException;
    private readonly string _path;
    public string Path => _path;

    public WalWriter(string path, TimeSpan? groupWindow = null, int maxBatch = 256)
    {
        _path = path; // ⬅ zapamiętaj
        _groupWindow = groupWindow ?? TimeSpan.FromMilliseconds(25);
        _maxBatch = Math.Max(1, maxBatch);
        _queue = Channel.CreateUnbounded<WalItem>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        _fs = new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.OpenOrCreate,
            Access = FileAccess.ReadWrite,
            Share = FileShare.ReadWrite, // pozwól recovery otworzyć uchwyt RW (truncate)
            Options = FileOptions.Asynchronous | FileOptions.SequentialScan
        });
        _fs.Seek(0, SeekOrigin.End);
        _loop = Task.Run(WriterLoopAsync);
    }

    public async ValueTask TruncateAsync(CancellationToken ct = default)
    {
        // Queue barrier: all transactions accepted before truncate must reach
        // the stream before we take the physical I/O lock.
        await FlushAsync(ct).ConfigureAwait(false);
        await _ioGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // wyzeruj plik
            _fs.Flush(true);
            _fs.Position = 0;
            _fs.SetLength(0);
            _fs.Flush(true);
        }
        catch (Exception ex)
        {
            WalnutLogger.Exception(ex);
            throw new IOException("WalWriter.TruncateAsync failed", ex);
        }
        finally
        {
            _ioGate.Release();
        }
    }

    public async ValueTask<CommitHandle> AppendTransactionAsync(IReadOnlyList<ReadOnlyMemory<byte>> frames, Durability durability, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposeOnce) != 0, this);
        ThrowIfFaulted();
        if (frames.Count == 0) 
            throw new ArgumentException("empty frames");

        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = new WalItem(frames, durability, tcs);

        if (!_queue.Writer.TryWrite(item))
            await _queue.Writer.WriteAsync(item, ct).ConfigureAwait(false);

        return new CommitHandle(tcs.Task);
    }

    private async Task WriterLoopAsync()
    {
        var reader = _queue.Reader;
        var pending = new List<WalItem>(_maxBatch);
        try
        {
            while (await reader.WaitToReadAsync(_cts.Token).ConfigureAwait(false))
            {
                pending.Clear();
                if (reader.TryRead(out var first)) pending.Add(first);

                var sw = ValueStopwatch.StartNew();
                while (pending.Count < _maxBatch && sw.Elapsed < _groupWindow && reader.TryRead(out var item))
                    pending.Add(item);

                await _ioGate.WaitAsync(_cts.Token).ConfigureAwait(false);
                try
                {
                    // Always append at the current physical end of the file. Recovery may have
                    // truncated the underlying stream, so reset the position to Length before
                    // emitting the next batch to avoid leaving zero-filled gaps that would break
                    // subsequent replays.
                    if (_fs.Position != _fs.Length)
                        _fs.Seek(0, SeekOrigin.End);

                    foreach (var item in pending)
                        foreach (var frame in item.Frames)
                            await WriteFrameAsync(frame, _cts.Token).ConfigureAwait(false);

                    // Safe/Group commits and explicit barriers request durable
                    // media flush. Fast-only batches remain in OS buffers until
                    // a later durable operation or orderly disposal.
                    if (pending.Any(item => item.Durability != Durability.Fast))
                        _fs.Flush(true);
                    else
                        await _fs.FlushAsync(_cts.Token).ConfigureAwait(false);

                    foreach (var item in pending)
                        item.Promise.TrySetResult(true);
                }
                finally
                {
                    _ioGate.Release();
                }
            }
        }
        catch (OperationCanceledException) { /* normal shutdown */ }
        catch (Exception ex)
        {
            Volatile.Write(ref _faulted, 1);
            _faultException = ex;
            _queue.Writer.TryComplete(ex);
            WalnutLogger.Exception(ex);

            foreach (var item in pending)
                item.Promise.TrySetException(ex);

            // Spróbuj opróżnić kolejkę i też zasygnalizować błąd
            try
            {
                while (reader.TryRead(out var item))
                    item.Promise.TrySetException(ex);
            }
            catch (Exception e)
            {
                WalnutLogger.Exception(e);
                /* ignore */
            }
        }
    }

    private static void WriteU32Le(FileStream fs, uint value)
    {
        Span<byte> buf = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(buf, value);
        fs.Write(buf);
    }

    private static uint ComputeCrc(Crc32 crc, ReadOnlyMemory<byte> payload)
        => crc.Compute(payload.Span);

    private async ValueTask WriteFrameAsync(ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        // Uwaga: bez Span/stackalloc w async — wszystko w helperach synchronicznych
        var crc = ComputeCrc(_crc, payload);
        WriteU32Le(_fs, (uint)payload.Length);         // length
        await _fs.WriteAsync(payload, ct).ConfigureAwait(false); // payload
        WriteU32Le(_fs, crc);                          // crc
    }

    public async ValueTask FlushAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposeOnce) != 0, this);
        ThrowIfFaulted();

        // An empty item is a queue barrier. WriterLoop completes it only after
        // every preceding item has been written and Flush(true) has succeeded.
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var barrier = new WalItem(Array.Empty<ReadOnlyMemory<byte>>(), Durability.Safe, tcs);
        if (!_queue.Writer.TryWrite(barrier))
            await _queue.Writer.WriteAsync(barrier, ct).ConfigureAwait(false);

        await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        ThrowIfFaulted();
    }

    private void ThrowIfFaulted()
    {
        if (Volatile.Read(ref _faulted) != 0)
            throw new IOException("The WAL writer is faulted.", _faultException);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeOnce, 1) != 0)
            return;

        // Zakończ przyjmowanie zadań
        try { _queue.Writer.TryComplete(); } catch { /* ignore */ }

        // Poczekaj aż pętla opróżni kolejkę i zakończy się po TryComplete().
        try { await _loop.ConfigureAwait(false); } catch { /* ignore */ }

        // (Opcjonalnie) oznacz wszystkie niedoszłe promise jako faulted,
        // żeby nikt nie zawisł czekając na WhenCommitted
        try
        {
            while (_queue.Reader.TryRead(out var item))
                item.Promise.TrySetException(new ObjectDisposedException(nameof(WalWriter)));
        }
        catch { /* ignore */ }

        // Dokończ IO
        try { _fs.Flush(true); } catch { /* ignore */ }
        try { await _fs.DisposeAsync().ConfigureAwait(false); } catch { /* ignore */ }

        // Na końcu sprzątnij CTS
        try { _cts.Dispose(); } catch { /* ignore */ }
        try { _ioGate.Dispose(); } catch { /* ignore */ }

    }
}

/// <summary>Minimalny, szybki CRC32 (polinom 0xEDB88320).</summary>
internal sealed class Crc32
{
    private readonly uint[] _table = new uint[256];
    public Crc32()
    {
        const uint poly = 0xEDB88320u;
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) != 0 ? poly ^ c >> 1 : c >> 1;
            _table[i] = c;
        }
    }
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public uint Compute(ReadOnlySpan<byte> data)
    {
        uint c = 0xFFFF_FFFFu;
        foreach (var b in data) c = _table[(c ^ b) & 0xFF] ^ c >> 8;
        return c ^ 0xFFFF_FFFFu;
    }
}

/// <summary>Prosty stoper o niskich narzutach.</summary>
internal struct ValueStopwatch
{
    private static readonly double TimestampToTimeSpan = 1.0 / Stopwatch.Frequency;
    private long _start;
    public static ValueStopwatch StartNew() => new ValueStopwatch { _start = Stopwatch.GetTimestamp() };
    public TimeSpan Elapsed => TimeSpan.FromSeconds((Stopwatch.GetTimestamp() - _start) * TimestampToTimeSpan);
}
