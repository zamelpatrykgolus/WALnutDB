#nullable enable

namespace WalnutDb.Core;

/// <summary>
/// Small async reader/writer gate. Normal commits enter as readers, while
/// maintenance operations (checkpoint, backup, repair) enter as writers.
/// Once a writer is waiting, new readers queue behind it, preventing starvation.
/// </summary>
internal sealed class AsyncReadWriteGate : IDisposable
{
    private readonly SemaphoreSlim _turnstile = new(1, 1);
    private readonly SemaphoreSlim _readerMutex = new(1, 1);
    private readonly SemaphoreSlim _roomEmpty = new(1, 1);
    private int _readers;

    public async ValueTask<Lease> EnterReadAsync(CancellationToken ct = default)
    {
        await _turnstile.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await _readerMutex.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (_readers == 0)
                    await _roomEmpty.WaitAsync(ct).ConfigureAwait(false);

                _readers++;
            }
            finally
            {
                _readerMutex.Release();
            }
        }
        finally
        {
            _turnstile.Release();
        }

        return new Lease(this, writer: false);
    }

    public async ValueTask<Lease> EnterWriteAsync(CancellationToken ct = default)
    {
        await _turnstile.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await _roomEmpty.WaitAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            _turnstile.Release();
            throw;
        }

        return new Lease(this, writer: true);
    }

    private void ExitRead()
    {
        _readerMutex.Wait();
        try
        {
            _readers--;
            if (_readers == 0)
                _roomEmpty.Release();
        }
        finally
        {
            _readerMutex.Release();
        }
    }

    private void ExitWrite()
    {
        _roomEmpty.Release();
        _turnstile.Release();
    }

    public void Dispose()
    {
        _turnstile.Dispose();
        _readerMutex.Dispose();
        _roomEmpty.Dispose();
    }

    internal sealed class Lease : IDisposable, IAsyncDisposable
    {
        private AsyncReadWriteGate? _owner;
        private readonly bool _writer;

        internal Lease(AsyncReadWriteGate owner, bool writer)
        {
            _owner = owner;
            _writer = writer;
        }

        public void Dispose()
        {
            var owner = Interlocked.Exchange(ref _owner, null);
            if (owner is null)
                return;

            if (_writer) owner.ExitWrite();
            else owner.ExitRead();
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
