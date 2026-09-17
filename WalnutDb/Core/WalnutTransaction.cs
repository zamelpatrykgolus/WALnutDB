// src/WalnutDb/Core/WalnutTransaction.cs
#nullable enable
using WalnutDb.Wal;

namespace WalnutDb.Core;

internal sealed class WalnutTransaction : WalnutDb.ITransaction
{
    private readonly WalnutDatabase _db;
    private readonly ulong _txId;
    private readonly ulong _seqNo;

    private int _ops;

    // Zbieramy ramki WAL w pamięci (BEGIN/PUT/DELETE/…/COMMIT)
    private readonly List<ReadOnlyMemory<byte>> _frames = new();
    // A tutaj akcje do zastosowania w MemTable po commit
    private readonly List<Action> _applyActions = new();
    // Akcje uruchamiane przy Dispose, gdy transakcja nie została zatwierdzona
    private readonly List<Action> _rollbackActions = new();
    private readonly Dictionary<string, byte[]?> _pendingValues = new(StringComparer.Ordinal);
    private const int Active = 0;
    private const int Committing = 1;
    private const int Committed = 2;
    private const int Disposed = 3;
    private int _state;

    internal WalnutTransaction(WalnutDatabase db, ulong txId, ulong seqNo)
    {
        _db = db; _txId = txId; _seqNo = seqNo;
    }

    public async ValueTask CommitAsync(WalnutDb.Durability durability = WalnutDb.Durability.Safe, CancellationToken ct = default)
    {
        if (Interlocked.CompareExchange(ref _state, Committing, Active) != Active)
            throw new InvalidOperationException("Transaction is no longer active.");

        AsyncReadWriteGate.Lease maintenanceLease;
        try
        {
            maintenanceLease = await _db.MaintenanceGate.EnterReadAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            Interlocked.CompareExchange(ref _state, Active, Committing);
            throw;
        }
        await using var _ = maintenanceLease;
        _db.ThrowIfMaintenanceFailed();
        if (_db.IsDisposing)
        {
            Interlocked.CompareExchange(ref _state, Active, Committing);
            throw new ObjectDisposedException(nameof(WalnutDatabase));
        }

        if (_ops == 0)
        {
            Volatile.Write(ref _state, Committed);
            return;
        }

        var commitFrames = new List<ReadOnlyMemory<byte>>(_frames.Count + 2)
        {
            Wal.WalCodec.BuildBegin(_txId, _seqNo)
        };
        commitFrames.AddRange(_frames);
        commitFrames.Add(Wal.WalCodec.BuildCommit(_txId, _ops));

        CommitHandle handle;
        try { await _db.WriterLock.WaitAsync(ct).ConfigureAwait(false); }
        catch { Interlocked.CompareExchange(ref _state, Active, Committing); throw; }
        try
        {
            // Keep WAL enqueue order identical to MemTable apply order, including
            // concurrent commits of the same key and Fast/Safe mixtures.
            // Wyślij całą transakcję do WAL
            handle = await _db.Wal.AppendTransactionAsync(commitFrames, durability, ct).ConfigureAwait(false);

            // Trwałość: dla Safe/Group poczekaj aż batch zostanie zfsyncowany
            if (durability is WalnutDb.Durability.Safe or WalnutDb.Durability.Group)
                await handle.WhenCommitted.ConfigureAwait(false);
            foreach (var act in _applyActions) act();
            Volatile.Write(ref _state, Committed);
        }
        catch
        {
            Interlocked.CompareExchange(ref _state, Active, Committing);
            throw;
        }

        finally
        {
            _db.WriterLock.Release();
        }
    }
    public void Dispose()
    {
        var previous = Interlocked.Exchange(ref _state, Disposed);
        if (previous == Active)
            foreach (var act in _rollbackActions) act();
    }

    public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }

    // ---- używane przez DefaultTable<T> ----
    private void EnsureActive()
    {
        if (Volatile.Read(ref _state) != Active)
            throw new InvalidOperationException("Transaction is no longer active.");
    }

    internal void AddApply(Action action) { EnsureActive(); _applyActions.Add(action); }
    internal void AddRollback(Action action) { EnsureActive(); _rollbackActions.Add(action); }
    internal void AddPut(string table, byte[] key, byte[] value)
    {
        EnsureActive();
        _frames.Add(Wal.WalCodec.BuildPut(_txId, table, key, value));
        _ops++;
    }
    internal void AddDelete(string table, byte[] key)
    {
        EnsureActive();
        _frames.Add(Wal.WalCodec.BuildDelete(_txId, table, key));
        _ops++;
    }
    internal bool TryGetPendingValue(string table, byte[] key, out byte[]? value)
        => _pendingValues.TryGetValue(PendingKey(table, key), out value);

    internal void SetPendingValue(string table, byte[] key, byte[]? value)
    {
        EnsureActive();
        _pendingValues[PendingKey(table, key)] = value;
    }

    private static string PendingKey(string table, byte[] key)
        => table + "|" + Convert.ToBase64String(key);
    internal ulong TxId => _txId;
    internal ulong SeqNo => _seqNo;
    internal bool BelongsTo(WalnutDatabase database) => ReferenceEquals(_db, database);
}
