#nullable enable
using System.Collections.Generic;
using System.Threading;

namespace WalnutDb.Core;

internal sealed class MemTable
{
    private readonly ReaderWriterLockSlim _rw = new(LockRecursionPolicy.NoRecursion);
    private readonly SortedDictionary<byte[], Entry> _map = new(ByteArrayComparer.Instance);

    public bool IsDirty
    {
        get { _rw.EnterReadLock(); try { return _map.Count != 0; } finally { _rw.ExitReadLock(); } }
    }

    internal readonly struct Entry
    {
        public readonly byte[]? Value;
        public readonly bool Tombstone;
        public Entry(byte[]? value, bool tombstone) { Value = value; Tombstone = tombstone; }
    }

    public bool TryGet(byte[] key, out byte[]? value)
    {
        _rw.EnterReadLock();
        try
        {
            if (_map.TryGetValue(key, out var e) && !e.Tombstone)
            {
                value = e.Value;
                return true;
            }
            value = null;
            return false;
        }
        catch (Exception ex)
        {
            WalnutLogger.Exception(ex);
            throw;
        }
        finally
        {
            _rw.ExitReadLock();
        }
    }

    public void Upsert(byte[] key, byte[] value)
    {
        _rw.EnterWriteLock();
        try
        {
            _map[key] = new Entry(value, tombstone: false);
        }
        catch (Exception ex)
        {
            WalnutLogger.Exception(ex);
            throw;
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    public void Delete(byte[] key)
    {
        _rw.EnterWriteLock();
        try
        {
            _map[key] = new Entry(value: null, tombstone: true);
        }
        catch (Exception ex)
        {
            WalnutLogger.Exception(ex);
            throw;
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    public IEnumerable<(byte[] Key, Entry Value)> SnapshotAll(byte[]? afterKeyExclusive)
    {
        (byte[] Key, Entry Value)[] snap;
        _rw.EnterReadLock();
        try
        {
            snap = new (byte[] Key, Entry Value)[_map.Count];
            int i = 0;
            foreach (var kv in _map) snap[i++] = (kv.Key, kv.Value);
        }
        catch (Exception ex)
        {
            WalnutLogger.Exception(ex);
            throw;
        }
        finally
        {
            _rw.ExitReadLock();
        }

        if (afterKeyExclusive is null || afterKeyExclusive.Length == 0)
        {
            for (int i = 0; i < snap.Length; i++)
                yield return snap[i];

            yield break;
        }
        for (int i = 0; i < snap.Length; i++)
            if (ByteCompare(snap[i].Key, afterKeyExclusive) > 0)
                yield return snap[i];
    }

    public IEnumerable<(byte[] Key, Entry Value)> SnapshotRange(byte[] fromInclusive, byte[] toExclusive, byte[]? afterKeyExclusive)
    {
        (byte[] Key, Entry Value)[] snap;
        _rw.EnterReadLock();
        try
        {
            snap = new (byte[] Key, Entry Value)[_map.Count];
            int i = 0;

            foreach (var kv in _map)
                snap[i++] = (kv.Key, kv.Value);
        }
        catch (Exception ex)
        {
            WalnutLogger.Exception(ex);
            throw;
        }
        finally
        {
            _rw.ExitReadLock();
        }

        for (int i = 0; i < snap.Length; i++)
        {
            var (k, v) = snap[i];

            if (fromInclusive.Length != 0 && ByteCompare(k, fromInclusive) < 0)
                continue;

            if (toExclusive.Length != 0 && ByteCompare(k, toExclusive) >= 0)
                break;

            if (afterKeyExclusive is not null && afterKeyExclusive.Length != 0 && ByteCompare(k, afterKeyExclusive) <= 0)
                continue;

            yield return (k, v);
        }
    }

    public bool HasTombstoneExact(byte[] key)
    {
        _rw.EnterReadLock();
        try
        {
            return _map.TryGetValue(key, out var e) && e.Tombstone;
        }
        catch (Exception ex)
        {
            WalnutLogger.Exception(ex);
            throw;
        }
        finally
        {
            _rw.ExitReadLock();
        }
    }

    private static int ByteCompare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++)
        {
            int d = a[i] - b[i];

            if (d != 0)
                return d;
        }
        return a.Length - b.Length;
    }
}
