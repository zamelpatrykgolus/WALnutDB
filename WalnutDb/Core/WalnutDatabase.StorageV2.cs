using WalnutDb.Sst;
using WalnutDb.Storage;
using WalnutDb.Wal;

namespace WalnutDb.Core;

public sealed partial class WalnutDatabase
{
    private Exception? _maintenanceFailure;
    private readonly List<Task> _retiredReaders = new();
    private readonly Dictionary<SegmentFile, SegmentReader> _segmentCache = new();
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, bool> _rebuildV2Indexes = new();
    private long _checkpointCount, _skippedCheckpoints, _sstBytesWritten, _compactionBytesWritten;

    internal void ThrowIfMaintenanceFailed()
    {
        if (_maintenanceFailure is not null)
            throw new IOException("A maintenance publication failed. Reopen the database to recover before writing again.", _maintenanceFailure);
    }
    private void Fault(string point) => _options.StorageFault?.Invoke(point);

    private void CleanupV2Orphans()
    {
        var retained = _catalog.TableSegments.Values.SelectMany(x => x).Select(f => f.FileName)
            .Concat(_catalog.PendingDeletes).Concat(_catalog.StagedFiles).ToHashSet(StringComparer.Ordinal);
        bool changed = false;
        foreach (var file in Directory.EnumerateFiles(_sstDir, "seg-*"))
        {
            var name = Path.GetFileName(file);
            var id = name.EndsWith(".sst.tmp", StringComparison.Ordinal) ? name[4..^8] :
                name.EndsWith(".sst", StringComparison.Ordinal) ? name[4..^4] : "";
            if (Guid.TryParseExact(id, "N", out _) && !retained.Contains(name))
            { File.Delete(file); changed = true; }
        }
        if (changed) DurableFile.SyncDirectory(_sstDir);
        string? current = _manifest.ReadCurrentAsync().AsTask().GetAwaiter().GetResult();
        changed = false;
        foreach (var file in Directory.EnumerateFiles(_dir, "MANIFEST-v2-*"))
        {
            var name = Path.GetFileName(file);
            if (name == current) continue;
            var id = name.EndsWith(".json.tmp", StringComparison.Ordinal) ? name[12..^9] :
                name.EndsWith(".json", StringComparison.Ordinal) ? name[12..^5] : "";
            if (Guid.TryParseExact(id, "N", out _)) { File.Delete(file); changed = true; }
        }
        if (changed) DurableFile.SyncDirectory(_dir);
    }

    public StorageWriteStatistics GetWriteStatistics() => new(
        Interlocked.Read(ref _checkpointCount), Interlocked.Read(ref _skippedCheckpoints),
        Interlocked.Read(ref _sstBytesWritten), Interlocked.Read(ref _compactionBytesWritten));

    public CompactionStatus? GetCompactionStatus() => _catalog.Compaction;

    public async ValueTask<StorageMigrationPlan> PlanStorageUpgradeAsync(CancellationToken ct = default)
    {
        await using var lease = await MaintenanceGate.EnterWriteAsync(ct).ConfigureAwait(false);
        ThrowIfMaintenanceFailed();
        long bytes = _catalog.StorageVersion == 2
            ? _catalog.TableSegments.Values.SelectMany(x => x).Sum(f => f.Bytes)
            : _tableFiles.Values.Sum(f => new FileInfo(Path.Combine(_sstDir, f)).Length);
        return new(_catalog.StorageVersion, 2, bytes, 0, true);
    }

    /// <summary>Metadata-only upgrade. Enable only after all rollback versions can read storage v2.</summary>
    public async ValueTask UpgradeStorageAsync(CancellationToken ct = default)
    {
        await using var lease = await MaintenanceGate.EnterWriteAsync(ct).ConfigureAwait(false);
        ThrowIfMaintenanceFailed();
        if (_catalog.StorageVersion == 2) return;
        if (!_options.AllowStorageV2Upgrade)
            throw new InvalidOperationException("Explicit AllowStorageV2Upgrade is required after validating software rollback compatibility.");
        if (Wal is not WalWriter) throw new NotSupportedException("Storage v2 requires the built-in epoch-aware WalWriter.");
        var segments = _tableFiles.ToDictionary(x => x.Key,
            x => new List<SegmentFile> { new(x.Value, 1, new FileInfo(Path.Combine(_sstDir, x.Value)).Length, true) }, StringComparer.Ordinal);
        var next = _catalog with
        {
            StorageVersion = 2, ManifestVersion = 2, WalFormat = "WALv1+epochs", SstFormat = "SSTv1+SSTv2",
            TableFiles = new(StringComparer.Ordinal), TableSegments = segments,
            CoveredWalEpoch = Guid.Empty, CoveredWalOffset = 0
        };
        try { await PublishV2Async(next, ct).ConfigureAwait(false); }
        catch (Exception ex) { _maintenanceFailure = ex; throw; }
    }

    private static Dictionary<string, List<SegmentFile>> CopySegments(StorageCatalog catalog) =>
        catalog.TableSegments.ToDictionary(x => x.Key, x => x.Value.ToList(), StringComparer.Ordinal);

    private void InstallV2Catalog(StorageCatalog catalog)
    {
        // Construct and validate every new view before replacing any live view.
        var views = catalog.TableSegments.Where(x => x.Value.Count > 0)
            .ToDictionary(x => x.Key, x => new SegmentSetReader(_sstDir, x.Value, _segmentCache), StringComparer.Ordinal);
        foreach (var name in _sst.Keys.Except(views.Keys).ToArray())
            if (_sst.TryRemove(name, out var old)) RetireReader(old);
        foreach (var view in views)
        {
            _sst.TryGetValue(view.Key, out var old);
            _sst[view.Key] = view.Value;
            if (old is not null) RetireReader(old);
        }
        _catalog = catalog;
        _retiredReaders.RemoveAll(task => task.IsCompletedSuccessfully);
    }

    private void RetireReader(ITableReader reader)
    {
        _retiredReaders.Add(reader.Retire());
    }

    private async ValueTask PublishV2Async(StorageCatalog next, CancellationToken ct)
    {
        await StorageCatalogStore.SaveAsync(_dir, _manifest, next, ct, Fault).ConfigureAwait(false);
        InstallV2Catalog(next);
    }

    private void EnsureMaintenanceSpace(long outputBytes)
    {
        long available = _options.AvailableSpaceOverride?.Invoke() ??
            new DriveInfo(Path.GetPathRoot(Path.GetFullPath(_dir))!).AvailableFreeSpace;
        if (outputBytes < 0 || available < checked(outputBytes + _options.MaintenanceReserveBytes))
            throw new IOException("Insufficient free space for maintenance. Existing segments and WAL were preserved.");
    }

    private IEnumerable<SegmentRow> DeltaRows(string name, MemTable mem)
    {
        bool index = name.StartsWith("__index__", StringComparison.Ordinal);
        foreach (var row in mem.SnapshotAll(null))
        {
            var value = row.Value.Value ?? Array.Empty<byte>();
            if (!row.Value.Tombstone && Encryption is not null && !index)
                value = Encryption.Encrypt(value, name, row.Key);
            yield return new(row.Key, value, row.Value.Tombstone);
        }
    }

    private async ValueTask<SegmentFile> WriteSegmentAsync(string fileName, IEnumerable<SegmentRow> rows,
        long maxOutputBytes, long bytesPerSecond, CancellationToken ct)
    {
        var temp = Path.Combine(_sstDir, fileName + ".tmp");
        long size = await SstV2.WriteAsync(temp, rows, maxOutputBytes, bytesPerSecond, Fault, ct).ConfigureAwait(false);
        // Validate the entire unpublished result before any active manifest can reference it.
        _ = new SegmentReader(_sstDir, new SegmentFile(fileName + ".tmp", 2, size));
        Fault("segment.before-rename");
        DurableFile.Move(temp, Path.Combine(_sstDir, fileName));
        Fault("segment.after-rename");
        return new(fileName, 2, size);
    }

    private async ValueTask CheckpointV2Async(CancellationToken ct)
    {
        var dirty = _tables.Where(x => x.Value.Current.IsDirty || _rebuildV2Indexes.ContainsKey(x.Key)).ToArray();
        if (dirty.Length == 0) { Interlocked.Increment(ref _skippedCheckpoints); return; }
        var writer = Wal as WalWriter ?? throw new NotSupportedException("Storage v2 requires WalWriter.");
        var nextSegments = CopySegments(_catalog);
        var obsoleteIndexes = new List<string>(_catalog.PendingDeletes);
        foreach (var name in _rebuildV2Indexes.Keys)
        {
            if (nextSegments.Remove(name, out var oldFiles))
                foreach (var old in oldFiles) { obsoleteIndexes.Add(old.FileName); if (old.Format == 1) obsoleteIndexes.Add(old.FileName + ".sxi"); }
        }
        foreach (var entry in dirty)
            if (nextSegments.TryGetValue(entry.Key, out var files) && files.Count >= _options.MaxSegmentsPerTable)
                throw new IOException($"Table '{entry.Key}' reached the segment limit. Run controlled compaction before checkpointing.");
        long estimate = dirty.Sum(t => t.Value.Current.SnapshotAll(null).Sum(r => (long)r.Key.Length + (r.Value.Value?.Length ?? 0) + 64));
        EnsureMaintenanceSpace(estimate);
        try
        {
            var boundary = await writer.CaptureAsync(ct).ConfigureAwait(false);
            foreach (var entry in dirty)
            {
                var file = await WriteSegmentAsync($"seg-{Guid.NewGuid():N}.sst", DeltaRows(entry.Key, entry.Value.Current), long.MaxValue, 0, ct).ConfigureAwait(false);
                if (!nextSegments.TryGetValue(entry.Key, out var files)) nextSegments[entry.Key] = files = new();
                if (files.Count == 0) file = file with { IsBase = true };
                files.Add(file);
                Interlocked.Add(ref _sstBytesWritten, file.Bytes);
            }
            await PublishV2Async(_catalog with
            {
                TableSegments = nextSegments, CoveredWalEpoch = boundary.Epoch, CoveredWalOffset = boundary.Offset,
                LastSequence = Volatile.Read(ref _nextSeqNo), PendingDeletes = obsoleteIndexes
            }, ct).ConfigureAwait(false);
            foreach (var entry in dirty) entry.Value.Swap(new MemTable());
            _rebuildV2Indexes.Clear();
            await writer.RotateAsync(Fault, ct).ConfigureAwait(false);
            if (_catalog.PendingDeletes.Count != 0) await FinishV2CleanupAsync(ct).ConfigureAwait(false);
            Interlocked.Increment(ref _checkpointCount);
        }
        catch (Exception ex) { _maintenanceFailure = ex; throw; }
    }

    public async ValueTask<CompactionStatus> CompactAsync(string table, CompactionOptions? options = null, CancellationToken ct = default)
    {
        options ??= new CompactionOptions();
        await using var lease = await MaintenanceGate.EnterWriteAsync(ct).ConfigureAwait(false);
        ThrowIfMaintenanceFailed();
        if (_catalog.StorageVersion != 2) throw new InvalidOperationException("Incremental compaction requires an explicit storage v2 upgrade.");
        if (_catalog.Compaction?.State == CompactionState.PublishedCleanupPending)
            await FinishV2CleanupAsync(ct).ConfigureAwait(false);
        // Compact only immutable inputs. Dirty MemTables remain covered by WAL;
        // this also permits compaction when the checkpoint segment limit is hit.
        table = CanonicalizeName(table);
        if (!_catalog.TableSegments.TryGetValue(table, out var current)) throw new ArgumentException("Table does not exist.", nameof(table));
        var selected = current.Where(f => options.IncludeBaseSegments || (f.Format == 2 && !f.IsBase)).ToList();
        if (selected.Count == 0) throw new InvalidOperationException("No eligible delta segments. Set IncludeBaseSegments to compact the base explicitly.");
        if (options.MaxInputBytes <= 0 || options.MaxOutputBytes <= 0 || options.WriteBytesPerSecond < 0)
            throw new ArgumentOutOfRangeException(nameof(options));
        long inputBytes = selected.Sum(f => f.Bytes);
        if (inputBytes > options.MaxInputBytes) throw new IOException("Compaction input exceeds its configured byte budget.");
        EnsureMaintenanceSpace(Math.Min(options.MaxOutputBytes, checked(inputBytes + selected.Count * 32L)));
        string output = $"seg-{Guid.NewGuid():N}.sst";
        var status = new CompactionStatus(Guid.NewGuid().ToString("N"), table, CompactionState.Preparing);
        try
        {
            await PublishV2Async(_catalog with { Compaction = status, StagedFiles = new() { output, output + ".tmp" } }, ct).ConfigureAwait(false);
            using var input = new SegmentSetReader(_sstDir, selected, _segmentCache);
            bool all = selected.Count == current.Count;
            var rows = input.ScanRows().Where(row => !all || !row.Deleted);
            var result = await WriteSegmentAsync(output, rows, options.MaxOutputBytes, options.WriteBytesPerSecond, ct).ConfigureAwait(false);
            result = result with { IsBase = all };
            Interlocked.Add(ref _compactionBytesWritten, result.Bytes);
            var next = CopySegments(_catalog);
            next[table] = current.Where(f => !selected.Contains(f)).Append(result).ToList();
            var deletes = selected.Select(f => f.FileName).ToList();
            deletes.AddRange(selected.Where(f => f.Format == 1).Select(f => f.FileName + ".sxi"));
            await PublishV2Async(_catalog with
            {
                TableSegments = next, Compaction = status with { State = CompactionState.PublishedCleanupPending },
                PendingDeletes = deletes, StagedFiles = new()
            }, ct).ConfigureAwait(false);
            await FinishV2CleanupAsync(ct).ConfigureAwait(false);
            return _catalog.Compaction!;
        }
        catch (Exception ex) { _maintenanceFailure = ex; throw; }
    }

    private async ValueTask FinishV2CleanupAsync(CancellationToken ct)
    {
        await Task.WhenAll(_retiredReaders).WaitAsync(ct).ConfigureAwait(false);
        foreach (var file in _catalog.PendingDeletes.Concat(_catalog.StagedFiles).Distinct())
        {
            ct.ThrowIfCancellationRequested();
            if (_catalog.TableSegments.Values.SelectMany(x => x).Any(f => f.FileName == file))
                throw new InvalidDataException("Cleanup attempts to delete an active segment.");
            Fault("cleanup.before-delete");
            foreach (var cached in _segmentCache.Keys.Where(f => f.FileName == file).ToArray())
                if (_segmentCache.Remove(cached, out var reader)) reader.Dispose();
            File.Delete(Path.Combine(_sstDir, file));
            Fault("cleanup.after-delete");
        }
        DurableFile.SyncDirectory(_sstDir);
        Fault("cleanup.before-complete");
        var state = _catalog.Compaction?.State == CompactionState.Preparing ? CompactionState.Failed : CompactionState.Completed;
        var status = _catalog.Compaction is null ? null : _catalog.Compaction with
        { State = state, Detail = state == CompactionState.Failed ? "Interrupted before publication; original inputs retained." : null };
        await PublishV2Async(_catalog with { Compaction = status, PendingDeletes = new(), StagedFiles = new() }, ct).ConfigureAwait(false);
        _retiredReaders.RemoveAll(task => task.IsCompletedSuccessfully);
    }

    private async ValueTask DropV2Async(string name, CancellationToken ct)
    {
        // Keep the WAL (including other tables' dirty data) until the next global
        // checkpoint. The durable DROP is replayed after all earlier PUTs.
        var next = CopySegments(_catalog);
        var names = next.Keys.Concat(_tables.Keys).Where(n => n == name || n.StartsWith($"__index__{name}__", StringComparison.Ordinal)).Distinct().ToArray();
        var deleted = new List<string>(_catalog.PendingDeletes);
        foreach (var n in names)
        {
            if (next.Remove(n, out var files))
                foreach (var file in files) { deleted.Add(file.FileName); if (file.Format == 1) deleted.Add(file.FileName + ".sxi"); }
        }
        try
        {
            var writer = (WalWriter)Wal;
            ulong txId = (ulong)Random.Shared.NextInt64(1, long.MaxValue);
            var handle = await writer.AppendTransactionAsync(new ReadOnlyMemory<byte>[]
            {
                WalCodec.BuildBegin(txId, (ulong)Interlocked.Increment(ref _nextSeqNo)),
                WalCodec.BuildDropTable(txId, name), WalCodec.BuildCommit(txId, 1)
            }, Durability.Safe, ct).ConfigureAwait(false);
            await handle.WhenCommitted.ConfigureAwait(false);
            await PublishV2Async(_catalog with { TableSegments = next, PendingDeletes = deleted }, CancellationToken.None).ConfigureAwait(false);
            foreach (var n in names)
            {
                if (_tables.TryRemove(n, out var mem)) mem.Swap(new MemTable());
                _indexUnique.TryRemove(n, out _);
                _metrics.TryRemove(n, out _);
                RemoveUniqueGuardsForIndex(n);
            }
            await FinishV2CleanupAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) { _maintenanceFailure = ex; throw; }
    }
}
