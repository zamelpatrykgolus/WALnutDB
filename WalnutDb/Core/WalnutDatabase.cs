using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

using WalnutDb.Indexing;
using WalnutDb.Sst;
using WalnutDb.Storage;
using WalnutDb.Wal;

namespace WalnutDb.Core;

public sealed class WalnutDatabase : IDatabase
{
    private readonly string _dir;
    private readonly DatabaseOptions _options;
    private readonly IManifestStore _manifest;
    internal readonly IWalWriter Wal;
    private readonly string _sstDir;
    private long _nextSeqNo = 1;
    private readonly ITypeNameResolver _typeNames;
    internal IEncryption? Encryption => _options.Encryption;
    private readonly ConcurrentDictionary<string, SstReader> _sst = new();
    internal readonly SemaphoreSlim WriterLock = new(1, 1);
    internal readonly AsyncReadWriteGate MaintenanceGate = new();
    private readonly FileStream _databaseLock;
    private readonly ConcurrentDictionary<string, MemTableRef> _tables = new();
    internal readonly ConcurrentDictionary<string, TableMetrics> _metrics = new();
    private readonly ConcurrentDictionary<string, byte[]> _uniqueGuards = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _indexUnique = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> _tableFiles = new(StringComparer.Ordinal);
    private StorageCatalog _catalog = new();
    private int _disposeOnce;
    internal bool IsDisposing => Volatile.Read(ref _disposeOnce) != 0;

    internal void RegisterIndex(string indexTableName, bool unique)
    => _indexUnique[CanonicalizeName(indexTableName)] = unique;

    internal bool IsIndexUnique(string indexTableName)
        => _indexUnique.TryGetValue(CanonicalizeName(indexTableName), out var u) && u;

    public WalnutDatabase(string directory, DatabaseOptions options, IManifestStore manifest, IWalWriter wal, ITypeNameResolver? typeResolver = null)
    {
        _dir = directory;
        _options = options;
        _manifest = manifest;
        Wal = wal;
        _typeNames = typeResolver ?? new DefaultTypeNameResolver(options);
        Directory.CreateDirectory(_dir);

        // A WalnutDb directory has a single writer. Without this lock, two
        // FileStreams can overwrite/interleave frames in the shared WAL.
        var lockPath = Path.Combine(_dir, ".walnutdb.lock");
        try
        {
        var incompleteBackupMarker = Path.Combine(_dir, ".walnutdb-backup-incomplete");
        if (File.Exists(incompleteBackupMarker))
            throw new InvalidDataException("The database directory contains an incomplete-backup marker and must not be opened as a valid backup.");
            _databaseLock = new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (IOException ex)
        {
            try { Wal.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { /* preserve lock error */ }
            throw new IOException($"Database '{Path.GetFullPath(_dir)}' is already open by another process or WalnutDatabase instance.", ex);
        }

        try
        {
        // ⬇⬇⬇ NOWE: ustal właściwą ścieżkę WAL
        string walPath = (wal is WalWriter ww && !string.IsNullOrWhiteSpace(ww.Path))
            ? ww.Path
            : Path.Combine(_dir, "wal.log");

        var recovered = new ConcurrentDictionary<string, MemTable>();
        var droppedTables = new HashSet<string>(StringComparer.Ordinal);

        if (File.Exists(walPath))
        {
            // Tail damage is repaired by WalRecovery itself. Any other error
            // (wrong encryption key, malformed committed frame, I/O failure)
            // must stop opening the database; continuing would expose a
            // silently incomplete view.
            WalRecovery.Replay(walPath, recovered, droppedTables, _options.Encryption);
            AdoptRecoveredTables(recovered);
        }

        _sstDir = Path.Combine(_dir, "sst");
        Directory.CreateDirectory(_sstDir);

        if (droppedTables.Count > 0)
            PurgeDroppedArtifacts(droppedTables);

        StorageCatalog? loadedCatalog;
        try
        {
            loadedCatalog = StorageCatalogStore.Load(_dir, _manifest);
        }
        catch (UnsupportedStorageVersionException)
        {
            _databaseLock.Dispose();
            throw;
        }
        catch (InvalidDataException ex)
        {
            // The manifest is only metadata for storage v1. Valid WAL/SST files
            // can reconstruct it without copying the data set.
            WalnutLogger.Warning($"Ignoring damaged v1 manifest and rebuilding its table mapping: {ex.Message}");
            loadedCatalog = null;
        }
        if (loadedCatalog is not null)
        {
            _catalog = loadedCatalog;
            _nextSeqNo = Math.Max(_nextSeqNo, loadedCatalog.LastSequence + 1);
            foreach (var mapping in loadedCatalog.TableFiles)
            {
                var canonical = CanonicalizeName(mapping.Key);
                if (droppedTables.Contains(canonical) ||
                    droppedTables.Any(t => canonical.StartsWith($"__index__{t}__", StringComparison.Ordinal)))
                    continue;

                var fileName = mapping.Value;
                if (!string.Equals(Path.GetFileName(fileName), fileName, StringComparison.Ordinal) ||
                    !fileName.EndsWith(".sst", StringComparison.OrdinalIgnoreCase))
                    throw new InvalidDataException($"Manifest contains invalid SST file name '{fileName}'.");

                var file = Path.Combine(_sstDir, fileName);
                if (!File.Exists(file))
                {
                    WalnutLogger.Warning($"Manifest references missing SST file '{fileName}' for table '{canonical}'. Removing the stale mapping so legacy self-healing can run.");
                    continue;
                }

                LoadSst(canonical, file);
            }
        }

        // Legacy migration is metadata-only. Existing SST files stay in place;
        // their discovered mapping is persisted in the small v1 manifest.
        var knownFiles = new HashSet<string>(_tableFiles.Values, StringComparer.OrdinalIgnoreCase);
        if (loadedCatalog is null)
        {
            foreach (var file in Directory.EnumerateFiles(_sstDir, "*.sst"))
            {
                var fileName = Path.GetFileName(file);
                if (knownFiles.Contains(fileName))
                    continue;

                var baseName = Path.GetFileNameWithoutExtension(file);
                string logicalName;
                if (_options.LegacyTableNameMappings?.TryGetValue(baseName, out var configuredName) == true)
                {
                    logicalName = configuredName;
                }
                else
                {
                    logicalName = recovered.ContainsKey(baseName) ? baseName : DecodeNameFromFile(baseName);
                    if (!string.Equals(logicalName, baseName, StringComparison.Ordinal))
                        WalnutLogger.Warning($"Legacy SST name '{baseName}' is ambiguous and was interpreted as table '{logicalName}'. Configure DatabaseOptions.LegacyTableNameMappings to override this without rewriting the SST.");
                }
                var canonical = CanonicalizeName(logicalName);
                if (_sst.ContainsKey(canonical))
                    throw new InvalidDataException($"Multiple SST files resolve to table '{canonical}'. Manual repair is required.");

                LoadSst(canonical, file);
                knownFiles.Add(fileName);
            }
        }
        else
        {
            foreach (var file in Directory.EnumerateFiles(_sstDir, "*.sst"))
            {
                var fileName = Path.GetFileName(file);
                if (!knownFiles.Contains(fileName))
                    WalnutLogger.Warning($"Ignoring orphaned SST file '{fileName}' because it is not referenced by the valid manifest.");
            }
        }

        PersistCatalogAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();

        var legacyDangling = new List<(string IndexTable, byte[] Key)>();
        var legacySeen = new HashSet<string>(StringComparer.Ordinal);

        try
        {
            foreach (var name in _tables.Keys.Concat(_sst.Keys).Distinct())
            {
                if (!name.StartsWith("__index__", StringComparison.Ordinal)) continue;

                _tables.TryGetValue(name, out var indexMemRef);

                // MEM
                if (indexMemRef is not null)
                {
                    foreach (var it in indexMemRef.Current.SnapshotAll(null))
                    {
                        if (it.Value.Tombstone) continue;
                        var prefix = IndexKeyCodec.ExtractValuePrefix(it.Key);
                        var pk = IndexKeyCodec.ExtractPrimaryKey(it.Key);

                        if (pk.Length == 0 || !PrimaryRowExistsForIndex(name, pk))
                        {
                            if (TryRecordLegacyIndexEntry(name, it.Key, legacyDangling, legacySeen))
                                indexMemRef.Current.Delete(it.Key);
                            continue;
                        }

                        TryReserveUnique(name, prefix, pk);
                    }
                }

                // SST
                if (_sst.TryGetValue(name, out var sst))
                {
                    foreach (var (k, _) in sst.ScanRange(Array.Empty<byte>(), Array.Empty<byte>()))
                    {
                        if (indexMemRef is not null && indexMemRef.Current.HasTombstoneExact(k))
                            continue;

                        var prefix = IndexKeyCodec.ExtractValuePrefix(k);
                        var pk = IndexKeyCodec.ExtractPrimaryKey(k);

                        if (pk.Length == 0 || !PrimaryRowExistsForIndex(name, pk))
                        {
                            TryRecordLegacyIndexEntry(name, k, legacyDangling, legacySeen);
                            continue;
                        }

                        TryReserveUnique(name, prefix, pk);
                    }
                }
            }

            foreach (var n in _sst.Keys.Except(_tables.Keys))
                Console.WriteLine($"[SST only] {n}");

            foreach (var n in _tables.Keys.Except(_sst.Keys))
                Console.WriteLine($"[Mem only] {n}");
        }
        catch (Exception ex)
        {
            WalnutLogger.Exception(ex);
            /* seed jest best-effort */
        }

        if (legacyDangling.Count > 0)
        {
            try
            {
                ApplyLegacyIndexFixups(legacyDangling);
            }
            catch (Exception ex)
            {
                WalnutLogger.Exception(ex);
            }
        }
        }
        catch
        {
            foreach (var reader in _sst.Values)
                try { reader.Dispose(); } catch { }
            _sst.Clear();
            try { _databaseLock.Dispose(); } catch { }
            try { Wal.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
            throw;
        }
    }

    private void AdoptRecoveredTables(ConcurrentDictionary<string, MemTable> recovered)
    {
        foreach (var kv in recovered)
        {
            var canonical = CanonicalizeName(kv.Key);

            if (_tables.TryGetValue(canonical, out var existing))
            {
                // Zmerguj zawartość z odzyskanej memki do istniejącej (upserty/tombstony)
                foreach (var it in kv.Value.SnapshotAll(afterKeyExclusive: null))
                {
                    if (it.Value.Tombstone)
                        existing.Current.Delete(it.Key);
                    else if (it.Value.Value is not null)
                        existing.Current.Upsert(it.Key, it.Value.Value);
                }
            }
            else
            {
                _tables[canonical] = new MemTableRef(kv.Value);
            }
        }
    }

    private void MigrateSstFilenames()
    {
        // --- 1) *.sst (+ ich *.sst.sxi) ---
        var sstFiles = Directory.GetFiles(_sstDir, "*.sst");
        foreach (var sstPath in sstFiles)
        {
            var baseName = Path.GetFileNameWithoutExtension(sstPath);
            var logical = DecodeNameFromFile(baseName);
            var canonical = CanonicalizeName(logical);
            var prefer = EncodeNameToFile(canonical);

            if (!string.Equals(prefer, baseName, StringComparison.Ordinal))
            {
                var newSst = Path.Combine(_sstDir, prefer + ".sst");
                SafeMoveReplacing(sstPath, newSst);

                var oldSxi = sstPath + ".sxi";
                var newSxi = newSst + ".sxi";
                if (File.Exists(oldSxi)) SafeMoveReplacing(oldSxi, newSxi);
            }
        }

        // --- 2) *.sst.tmp  (pozostałości po zapisie; tylko zmiana bazy nazwy) ---
        var tmpFiles = Directory.GetFiles(_sstDir, "*.sst.tmp");
        foreach (var tmpPath in tmpFiles)
        {
            var fn = Path.GetFileName(tmpPath);                    // np. "devices.sst.tmp" lub "ZGF...=.sst.tmp"
            var stem = fn.Substring(0, fn.Length - ".sst.tmp".Length);
            var logical = DecodeNameFromFile(stem);
            var canonical = CanonicalizeName(logical);
            var prefer = EncodeNameToFile(canonical);

            if (!string.Equals(prefer, stem, StringComparison.Ordinal))
            {
                var newTmp = Path.Combine(_sstDir, prefer + ".sst.tmp");
                SafeMoveReplacing(tmpPath, newTmp);
            }
        }

        // --- 3) *.sst.tmp.sxi  (indeks do pliku tymczasowego) ---
        var tmpSxiFiles = Directory.GetFiles(_sstDir, "*.sst.tmp.sxi");
        foreach (var sxiPath in tmpSxiFiles)
        {
            var fn = Path.GetFileName(sxiPath);                    // np. "devices.sst.tmp.sxi" lub "ZGF...=.sst.tmp.sxi"
            var stem = fn.Substring(0, fn.Length - ".sst.tmp.sxi".Length);
            var logical = DecodeNameFromFile(stem);
            var canonical = CanonicalizeName(logical);
            var prefer = EncodeNameToFile(canonical);

            if (!string.Equals(prefer, stem, StringComparison.Ordinal))
            {
                var newSxi = Path.Combine(_sstDir, prefer + ".sst.tmp.sxi");
                SafeMoveReplacing(sxiPath, newSxi);
            }
        }

        static void SafeMoveReplacing(string src, string dst)
        {
            try
            {
                Directory.CreateDirectory(Path.GetDirectoryName(dst)!);
                if (File.Exists(dst))
                {
                    // Preferujemy docelowy – źródłowy sprzątamy (best-effort).
                    try
                    {
                        File.Delete(src);
                    }
                    catch { }
                }
                else
                {
                    File.Move(src, dst);
                }
            }
            catch { /* best-effort */ }
        }
    }

    private static string MakeGuardKey(string indexTableName, byte[] valuePrefix)
        => indexTableName + "|" + Convert.ToBase64String(valuePrefix);

    internal bool TryReserveUnique(string indexTableName, byte[] valuePrefix, byte[] pk)
    {
        var gk = MakeGuardKey(indexTableName, valuePrefix);

        while (true)
        {
            if (_uniqueGuards.TryGetValue(gk, out var existing))
            {
                if (ByteArrayEquals(existing, pk))
                    return true;

                if (!PrimaryRowExistsForIndex(indexTableName, existing) ||
                    !IndexEntryExists(indexTableName, valuePrefix, existing))
                {
                    _uniqueGuards.TryRemove(gk, out _);
                    continue;
                }

                return false;
            }

            if (_uniqueGuards.TryAdd(gk, pk))
            {
                return true;
            }
            // kolizja podczas Add — pętla
        }
    }


    internal void ClearUniqueReservations(string indexTableName)
    {
        var prefix = indexTableName + "|";
        var toRemove = new List<string>();

        foreach (var key in _uniqueGuards.Keys)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
                toRemove.Add(key);
        }

        foreach (var key in toRemove)
            _uniqueGuards.TryRemove(key, out _);
    }

    internal void RemoveIndexArtifacts(string indexTableName)
    {
        if (_sst.TryRemove(indexTableName, out var removed))
        {
            try { removed.Dispose(); } catch { }
        }

        var sstPath = GetSstPath(indexTableName);
        try { if (File.Exists(sstPath)) File.Delete(sstPath); } catch { }

        var sxiPath = sstPath + ".sxi";
        try { if (File.Exists(sxiPath)) File.Delete(sxiPath); } catch { }
        _tableFiles.TryRemove(indexTableName, out _);
    }


    private bool IndexEntryExists(string indexTableName, byte[] valuePrefix, byte[] ownerPk)
    {
        var composite = IndexKeyCodec.ComposeIndexEntryKey(valuePrefix, ownerPk);

        if (_tables.TryGetValue(indexTableName, out var memRef))
        {
            if (memRef.Current.TryGet(composite, out _))
                return true;

            if (memRef.Current.HasTombstoneExact(composite))
                return false;
        }

        foreach (var (key, _) in ScanSstRange(indexTableName, composite, ExclusiveUpperBound(composite)))
        {
            if (ByteArrayEquals(key, composite))
                return true;
        }

        return false;
    }

    private bool PrimaryRowExistsForIndex(string indexTableName, byte[] primaryKey)
    {
        var baseTable = TryExtractBaseTableName(indexTableName);
        if (string.IsNullOrEmpty(baseTable))
            return true;

        if (_tables.TryGetValue(baseTable, out var memRef))
        {
            if (memRef.Current.TryGet(primaryKey, out _))
                return true;

            if (memRef.Current.HasTombstoneExact(primaryKey))
                return false;
        }

        if (_sst.TryGetValue(baseTable, out var sst))
        {
            bool sawSharingViolation = false;
            for (int attempt = 0; attempt < 5; attempt++)
            {
                try
                {
                    if (sst.TryGet(primaryKey, out var _))
                        return true;

                    break;
                }
                catch (FileNotFoundException)
                {
                    if (_sst.TryRemove(baseTable, out var missing))
                        missing.Dispose();

                    break;
                }
                catch (DirectoryNotFoundException)
                {
                    if (_sst.TryRemove(baseTable, out var missing))
                        missing.Dispose();

                    break;
                }
                catch (IOException ex) when (IsSharingViolation(ex))
                {
                    sawSharingViolation = true;
                    Thread.Sleep(5);
                    continue;
                }
                catch (IOException)
                {
                    if (_sst.TryRemove(baseTable, out var missing))
                        missing.Dispose();

                    break;
                }
            }

            if (sawSharingViolation)
                return true; // conservatively assume the owner still exists
        }

        return false;
    }

    private static bool IsSharingViolation(IOException ex)
    {
        const int ERROR_SHARING_VIOLATION = 32;
        const int ERROR_LOCK_VIOLATION = 33;
        int code = ex.HResult & 0xFFFF;
        return code == ERROR_SHARING_VIOLATION || code == ERROR_LOCK_VIOLATION;
    }

    private string? TryExtractBaseTableName(string indexTableName)
    {
        const string prefix = "__index__";
        if (!indexTableName.StartsWith(prefix, StringComparison.Ordinal))
            return null;

        // Prefer the longest known table name. This keeps indexes of tables
        // such as "plant__devices" associated with the correct owner.
        string? best = null;
        foreach (var candidate in _tables.Keys.Concat(_sst.Keys))
        {
            if (candidate.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            var expectedPrefix = prefix + candidate + "__";
            if (indexTableName.StartsWith(expectedPrefix, StringComparison.Ordinal) &&
                (best is null || candidate.Length > best.Length))
                best = candidate;
        }

        if (best is not null)
            return best;

        var rest = indexTableName.Substring(prefix.Length);
        var sep = rest.IndexOf("__", StringComparison.Ordinal);
        if (sep <= 0)
            return null;

        return rest.Substring(0, sep);
    }

    private bool TryRecordLegacyIndexEntry(string indexTableName, byte[] compositeKey, List<(string IndexTable, byte[] Key)> bag, HashSet<string> seen)
    {
        var fingerprint = indexTableName + "|" + Convert.ToBase64String(compositeKey);
        if (!seen.Add(fingerprint))
            return false;

        var copy = new byte[compositeKey.Length];
        Buffer.BlockCopy(compositeKey, 0, copy, 0, compositeKey.Length);
        bag.Add((indexTableName, copy));
        return true;
    }

    internal bool ShouldRebuildIndex(string tableName, string indexTableName)
    {
        if (!TableHasLiveRows(tableName))
            return false;

        if (IndexHasEntries(indexTableName))
            return false;

        WalnutLogger.Warning($"Index '{indexTableName}' lost its on-disk state while base table '{tableName}' still contains data – scheduling automatic rebuild.");
        return true;
    }

    private bool TableHasLiveRows(string tableName)
    {
        if (_tables.TryGetValue(tableName, out var memRef))
        {
            foreach (var entry in memRef.Current.SnapshotAll(null))
            {
                if (!entry.Value.Tombstone && entry.Value.Value is not null)
                    return true;
            }
        }

        foreach (var _ in ScanSstRange(tableName, Array.Empty<byte>(), Array.Empty<byte>()))
            return true;

        return false;
    }

    private bool IndexHasEntries(string indexTableName)
    {
        if (_tables.TryGetValue(indexTableName, out var memRef))
        {
            foreach (var entry in memRef.Current.SnapshotAll(null))
                if (!entry.Value.Tombstone)
                    return true;
        }

        if (_sst.TryGetValue(indexTableName, out var sst))
        {
            try
            {
                using var it = sst.ScanRange(Array.Empty<byte>(), Array.Empty<byte>()).GetEnumerator();
                if (it.MoveNext())
                    return true;
            }
            catch (IOException ex)
            {
                WalnutLogger.Warning($"Unable to read index segment for '{indexTableName}' ({ex.Message}). It will be rebuilt from the primary table.");
                if (_sst.TryRemove(indexTableName, out var removed))
                    removed.Dispose();
                return false;
            }
            catch (Exception ex)
            {
                WalnutLogger.Warning($"Unexpected failure while scanning index '{indexTableName}': {ex.Message}. The index will be rebuilt from base data.");
                if (_sst.TryRemove(indexTableName, out var removed))
                    removed.Dispose();
                return false;
            }

            return false;
        }

        var candidate = GetSstPath(indexTableName);
        if (!File.Exists(candidate))
            WalnutLogger.Warning($"Index segment '{candidate}' is missing; rebuilding index '{indexTableName}'.");
        else
            WalnutLogger.Warning($"Index '{indexTableName}' is not loaded into the SST cache despite existing file '{candidate}'. It will be rebuilt.");

        return false;
    }

    private void ApplyLegacyIndexFixups(List<(string IndexTable, byte[] Key)> entries)
    {
        if (entries.Count == 0)
            return;

        WalnutLogger.Warning($"Detected {entries.Count} dangling unique index entr{(entries.Count == 1 ? "y" : "ies")} without owning rows – cleaning up legacy drop artefacts.");

        var grouped = new Dictionary<string, List<byte[]>>(StringComparer.Ordinal);
        foreach (var entry in entries)
        {
            if (!grouped.TryGetValue(entry.IndexTable, out var list))
            {
                list = new List<byte[]>();
                grouped[entry.IndexTable] = list;
            }
            list.Add(entry.Key);
        }

        foreach (var kvp in grouped)
        {
            var indexName = kvp.Key;
            var keys = kvp.Value;
            int offset = 0;
            while (offset < keys.Count)
            {
                int take = Math.Min(32, keys.Count - offset);
                var txId = (ulong)(Random.Shared.NextInt64() & long.MaxValue);
                var seq = (ulong)Interlocked.Increment(ref _nextSeqNo);
                var frames = new List<ReadOnlyMemory<byte>>(take + 2)
                {
                    WalCodec.BuildBegin(txId, seq)
                };

                for (int i = 0; i < take; i++)
                    frames.Add(WalCodec.BuildDelete(txId, indexName, keys[offset + i]));

                frames.Add(WalCodec.BuildCommit(txId, take));

                var handle = Wal.AppendTransactionAsync(frames, Durability.Safe).GetAwaiter().GetResult();
                handle.WhenCommitted.GetAwaiter().GetResult();

                WriterLock.Wait();
                try
                {
                    var mem = GetOrAddMemRef(indexName);
                    for (int i = 0; i < take; i++)
                    {
                        var composite = keys[offset + i];
                        mem.Current.Delete(composite);

                        var prefix = IndexKeyCodec.ExtractValuePrefix(composite);
                        var pk = IndexKeyCodec.ExtractPrimaryKey(composite);
                        ReleaseUnique(indexName, prefix, pk);
                    }
                }
                finally
                {
                    WriterLock.Release();
                }

                offset += take;
            }
        }

        Wal.FlushAsync().GetAwaiter().GetResult();
    }

    private static byte[] ExclusiveUpperBound(byte[] key)
    {
        var to = new byte[key.Length + 1];
        Buffer.BlockCopy(key, 0, to, 0, key.Length);
        to[^1] = 0x00;
        return to;
    }
    internal void ReleaseUnique(string indexTableName, byte[] valuePrefix, byte[] pk)
    {
        var gk = MakeGuardKey(indexTableName, valuePrefix);
        if (_uniqueGuards.TryGetValue(gk, out var cur) && ByteArrayEquals(cur, pk))
        {
            _uniqueGuards.TryRemove(gk, out _);
            //Diag.U($"RELEASE ok    idx={indexTableName} val={Diag.B64(valuePrefix)} pk={Diag.B64(pk)}");
        }
        else
        {
            //Diag.U($"RELEASE skip  idx={indexTableName} val={Diag.B64(valuePrefix)} (not owner)");
        }
    }

    private static bool ByteArrayEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != b.Length)
            return false;

        for (int i = 0; i < a.Length; i++) if (a[i] != b[i])
                return false;

        return true;
    }

    internal sealed class TableMetrics
    {
        public long LiveBytes;  // sumaryczny rozmiar aktualnych wartości
        public long DeadBytes;  // rozmiar nadpisanych/usuniętych wartości (do defragu)
    }

    internal TableMetrics Metrics(string table) => _metrics.GetOrAdd(table, _ => new TableMetrics());

    public async ValueTask RebuildTableAsync(string name, CancellationToken ct = default)
    {
        // WAL is global, therefore rebuilding one table must never truncate it
        // independently of the other tables. The safe v1 implementation uses
        // the database-wide checkpoint path.
        if (!_tables.ContainsKey(CanonicalizeName(name)) && !_sst.ContainsKey(CanonicalizeName(name)))
            return;

        await CheckpointAsync(ct).ConfigureAwait(false);
    }

    internal MemTableRef GetOrAddMemRef(string name)
    => _tables.GetOrAdd(name, _ => new MemTableRef(new MemTable()));

    internal MemTableRef ReattachTable(string name, MemTableRef preferred)
    {
        var canonical = CanonicalizeName(name);
        var current = _tables.AddOrUpdate(canonical, preferred, static (_, existing) => existing);
        _metrics.GetOrAdd(canonical, _ => new TableMetrics());
        return current;
    }

    private void LoadSst(string canonicalName, string path)
    {
        RepairInterruptedSidecarPromotion(path);
        var reader = new SstReader(path);
        _sst[canonicalName] = reader;
        _tableFiles[canonicalName] = Path.GetFileName(path);
    }

    private static void RepairInterruptedSidecarPromotion(string sstPath)
    {
        var finalIndex = sstPath + ".sxi";
        var temporaryIndex = sstPath + ".tmp.sxi";
        if (File.Exists(finalIndex) || !File.Exists(temporaryIndex))
            return;

        try
        {
            // SstWriter creates <data-temp>.sxi. Once the data temp was
            // promoted to the final SST, this is the matching sidecar.
            File.Move(temporaryIndex, finalIndex);
            WalnutLogger.Warning($"Recovered interrupted SST sidecar promotion for '{Path.GetFileName(sstPath)}'.");
        }
        catch (IOException ex)
        {
            WalnutLogger.Warning($"Could not recover SST sidecar '{temporaryIndex}': {ex.Message}");
        }
    }

    private async ValueTask PersistCatalogAsync(CancellationToken ct)
    {
        _catalog = new StorageCatalog
        {
            CreatedWith = _catalog.CreatedWith,
            LastSequence = Volatile.Read(ref _nextSeqNo),
            TableFiles = _tableFiles.OrderBy(k => k.Key, StringComparer.Ordinal)
                                    .ToDictionary(k => k.Key, v => v.Value, StringComparer.Ordinal)
        };
        await StorageCatalogStore.SaveAsync(_dir, _manifest, _catalog, ct).ConfigureAwait(false);
    }

    internal MemTableRef ReattachIndex(string indexTableName, MemTableRef preferred, bool unique)
    {
        var canonical = CanonicalizeName(indexTableName);
        var current = ReattachTable(canonical, preferred);
        _indexUnique[canonical] = unique;
        return current;
    }

    // (opcjonalnie dla zgodności – jeśli coś jeszcze woła starą wersję)
    internal MemTable GetOrAddMemTable(string name) => GetOrAddMemRef(name).Current;

    internal bool TryGetFromSst(string name, ReadOnlySpan<byte> key, out byte[]? value)
    {
        value = null;

        IOException? transientError = null;
        // Retry only an actual sharing violation. Corruption and permanent I/O
        // failures must never be reinterpreted as a missing row.
        for (int attempt = 0; attempt < 50; attempt++)
        {
            try
            {
                if (_sst.TryGetValue(name, out var sst))
                    return sst.TryGet(key, out value);

                return false;
            }
            catch (IOException ex) when (IsSharingViolation(ex))
            {
                transientError = ex;
            }
            catch (FileNotFoundException ex)
            {
                transientError = ex;
            }
            catch (DirectoryNotFoundException ex)
            {
                transientError = ex;
            }

            Thread.Sleep(1);
        }

        throw new IOException($"Could not read SST for table '{name}' because the file remained locked.", transientError);
    }

    internal bool IsUniqueOwner(string indexTableName, ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> pk)
    {
        var gk = MakeGuardKey(indexTableName, prefix);
        return _uniqueGuards.TryGetValue(gk, out var owner) && ByteArrayEquals(owner, pk);
    }

    private static string MakeGuardKey(string indexTableName, ReadOnlySpan<byte> valuePrefix)
        => indexTableName + "|" + Convert.ToBase64String(valuePrefix);

    internal IEnumerable<(byte[] Key, byte[] Val)> ScanSstRange(string name, byte[] fromInclusive, byte[] toExclusive)
    {
        IOException? transientError = null;
        for (int attempt = 0; attempt < 50; attempt++)
        {
            if (!_sst.TryGetValue(name, out var sst))
                yield break;

            using var iterator = sst.ScanRange(fromInclusive, toExclusive).GetEnumerator();
            bool hasCurrent;
            try
            {
                hasCurrent = iterator.MoveNext(); // opens the file lazily
            }
            catch (IOException ex) when (ex is FileNotFoundException or DirectoryNotFoundException || IsSharingViolation(ex))
            {
                transientError = ex;
                Thread.Sleep(1);
                continue;
            }

            while (hasCurrent)
            {
                yield return iterator.Current;
                // Once opened, the stream retains the old file across atomic
                // replacement. Any later IOException is real and is propagated.
                hasCurrent = iterator.MoveNext();
            }
            yield break;
        }

        throw new IOException($"Could not scan SST for table '{name}' because the file remained unavailable.", transientError);
    }


    private void ReplaceSst(string name, string newPath)
    {
        var reader = new SstReader(newPath);
        SstReader? replaced = null;
        _sst.AddOrUpdate(name, reader, (_, old) =>
        {
            replaced = old;
            return reader;
        });
        if (replaced is not null)
            try { replaced.Dispose(); } catch { }
    }

    private static string CanonicalizeName(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) 
            return "_";

        var sb = new StringBuilder(raw.Length);

        foreach (var ch in raw)
        {
            if (char.IsLetterOrDigit(ch) || ch == '_' || ch == '-' || ch == '.')
                sb.Append(ch);
            else
                sb.Append('_'); // zamień znaki niedozwolone w nazwach plików Windows/Unix
        }
        var s = sb.ToString().Trim().TrimEnd('.', ' ');

        if (s.Length == 0) 
            s = "_";

        if (s.Length > 180) 
            s = s.Substring(0, 180);

        return s;
    }

    // --- NAME ENCODING -----------------------------------------------------------

    private static readonly char[] InvalidFileChars = Path.GetInvalidFileNameChars();
    private static readonly HashSet<string> WindowsReserved = new(StringComparer.OrdinalIgnoreCase)
{
    "CON","PRN","AUX","NUL",
    "COM1","COM2","COM3","COM4","COM5","COM6","COM7","COM8","COM9",
    "LPT1","LPT2","LPT3","LPT4","LPT5","LPT6","LPT7","LPT8","LPT9"
};

    private static bool IsSafeFileName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return false;

        if (name.IndexOfAny(InvalidFileChars) >= 0)
            return false;

        if (name.Length > 120)
            return false; // margines na rozszerzenia

        if (name.TrimEnd().EndsWith(".", StringComparison.Ordinal))
            return false;

        if (WindowsReserved.Contains(name))
            return false;

        return true;
    }

    private static string Base64UrlEncode(string logicalName)
    {
        var bytes = Encoding.UTF8.GetBytes(logicalName);
        return Convert.ToBase64String(bytes).Replace('+', '-').Replace('/', '_').TrimEnd('=');
    }

    private static string EncodeNameToFile(string logicalName)
    {
        // Jawnie, jeśli nazwa jest bezpieczna dla FS; inaczej Base64-url bez paddingu.
        return IsSafeFileName(logicalName) ? logicalName : Base64UrlEncode(logicalName);
    }

    private string GetSstFileName(string logicalName)
    {
        if (_tableFiles.TryGetValue(logicalName, out var existing))
            return existing;

        var candidate = EncodeNameToFile(logicalName) + ".sst";
        foreach (var mapping in _tableFiles)
        {
            if (!string.Equals(mapping.Key, logicalName, StringComparison.Ordinal) &&
                string.Equals(mapping.Value, candidate, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Tables '{logicalName}' and '{mapping.Key}' resolve to the same SST file '{candidate}'. Use distinct explicit table names.");
        }

        return candidate;
    }

    private string GetSstPath(string logicalName)
        => Path.Combine(_sstDir, GetSstFileName(logicalName));

    private static string DecodeNameFromFile(string fileBaseName)
    {
        // Rozpoznaj *kanoniczne* Base64-url: dekoduj bajty, sprawdź round-trip do identycznego ciągu
        // i zaakceptuj TYLKO gdy UTF-8 jest poprawny.
        // The legacy encoder only used Base64 when the logical name itself was
        // unsafe as a file name. A safe decoded value therefore proves that the
        // Base64-looking file name was meant literally (e.g. "YWJj").
        if (TryDecodeBase64UrlRoundtrip(fileBaseName, out var decoded) && !IsSafeFileName(decoded))
            return decoded;

        // Nie jest prawidłowym Base64-url → traktuj jako jawną nazwę.
        return fileBaseName;
    }

    private static bool TryDecodeBase64UrlRoundtrip(string s, out string decoded)
    {
        decoded = "";
        try
        {
            string std = s.Replace('-', '+').Replace('_', '/');
            int mod = std.Length % 4;
            if (mod == 1) return false;       // to nie może być Base64
            if (mod == 2) std += "==";
            else if (mod == 3) std += "=";

            var bytes = Convert.FromBase64String(std);

            // round-trip do *identycznego* base64-url bez '='
            string again = ToBase64UrlNoPad(bytes);
            if (!string.Equals(again, s, StringComparison.Ordinal)) return false;

            // Wymuś poprawny UTF-8 (żadnych znaków zamienników)
            decoded = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true)
                          .GetString(bytes);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static string ToBase64UrlNoPad(byte[] bytes)
    => Convert.ToBase64String(bytes).Replace('+', '-').Replace('/', '_').TrimEnd('=');

    public async ValueTask CheckpointAsync(CancellationToken ct = default)
    {
        // The writer lease waits for commits already in progress and prevents
        // new commits from entering until all SST files are durable and the WAL
        // has been truncated. MemTables stay attached until every SST succeeds,
        // so cancellation/failure cannot make live data disappear in-process.
        await using var maintenanceLease = await MaintenanceGate.EnterWriteAsync(ct).ConfigureAwait(false);
        var snapshot = _tables.ToArray();

        foreach (var entry in snapshot)
        {
            var name = entry.Key;
            var mem = entry.Value.Current;
            ct.ThrowIfCancellationRequested();

            bool isIndex = name.StartsWith("__index__", StringComparison.Ordinal);
            bool isUniqueIndex = isIndex && IsIndexUnique(name);

            var dst = GetSstPath(name);
            var tmp = dst + ".tmp";
            TryDeleteFile(tmp);
            TryDeleteFile(tmp + ".sxi");
            EnsureCheckpointSpace(dst, mem);

            try
            {
                await SstWriter.WriteAsync(tmp, StreamCheckpointRows(name, mem, isIndex, isUniqueIndex, ct), ct).ConfigureAwait(false);
                // Remove the old optional sidecar before replacing data. This makes
                // every crash point safe: old data without an index or new data
                // without an index are both readable by a full scan.
                TryDeleteFile(dst + ".sxi");
                if (File.Exists(dst)) File.Replace(tmp, dst, null); else File.Move(tmp, dst);
                ReplaceSst(name, dst);
                PromoteSstSidecar(tmp + ".sxi", dst + ".sxi");
                if (File.Exists(dst + ".sxi"))
                    ReplaceSst(name, dst); // reload the freshly promoted sparse index
                _tableFiles[name] = Path.GetFileName(dst);
            }
            catch
            {
                TryDeleteFile(tmp);
                TryDeleteFile(tmp + ".sxi");
                throw;
            }
        }

        // Commit the small metadata mapping before the global WAL is removed.
        await PersistCatalogAsync(ct).ConfigureAwait(false);

        // All replacements succeeded. Current MemTables are now fully covered
        // by the new SST set, so they can be cleared atomically for writers.
        await WriterLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            foreach (var entry in snapshot)
                entry.Value.Swap(new MemTable());
        }
        finally
        {
            WriterLock.Release();
        }

        await Wal.FlushAsync(ct).ConfigureAwait(false);
        await Wal.TruncateAsync(ct).ConfigureAwait(false);
    }

    private async IAsyncEnumerable<(byte[] Key, byte[] Val)> StreamCheckpointRows(
        string name,
        MemTable mem,
        bool isIndex,
        bool isUniqueIndex,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        using var memRows = mem.SnapshotAll(afterKeyExclusive: null).GetEnumerator();
        IEnumerator<(byte[] Key, byte[] Val)>? sstRows = null;
        if (_sst.TryGetValue(name, out var previous))
            sstRows = previous.ScanRange(Array.Empty<byte>(), Array.Empty<byte>()).GetEnumerator();

        using (sstRows)
        {
            bool hasMem = memRows.MoveNext();
            bool hasSst = sstRows?.MoveNext() == true;
            byte[]? uniquePrefix = null;
            (byte[] Key, byte[] Val, bool FromMem)? uniqueCandidate = null;
            int yielded = 0;

            while (hasMem || hasSst)
            {
                ct.ThrowIfCancellationRequested();
                (byte[] Key, byte[] Val, bool FromMem)? row = null;

                int comparison = hasMem && hasSst
                    ? ByteArrayComparer.Instance.Compare(memRows.Current.Key, sstRows!.Current.Key)
                    : hasMem ? -1 : 1;

                if (hasMem && comparison <= 0)
                {
                    var current = memRows.Current;
                    hasMem = memRows.MoveNext();
                    if (comparison == 0)
                        hasSst = sstRows!.MoveNext();

                    if (!current.Value.Tombstone && current.Value.Value is not null)
                    {
                        var value = Encryption is null || isIndex
                            ? current.Value.Value
                            : Encryption.Encrypt(current.Value.Value, name, current.Key);
                        row = (current.Key, value, true);
                    }
                }
                else
                {
                    var current = sstRows!.Current;
                    hasSst = sstRows.MoveNext();
                    row = (current.Key, current.Val, false);
                }

                if (row is null)
                    continue;

                if (!isUniqueIndex)
                {
                    yield return (row.Value.Key, row.Value.Val);
                }
                else
                {
                    var prefix = IndexKeyCodec.ExtractValuePrefix(row.Value.Key);
                    if (uniquePrefix is null || !ByteArrayComparer.Instance.Equals(uniquePrefix, prefix))
                    {
                        if (uniqueCandidate is not null)
                            yield return (uniqueCandidate.Value.Key, uniqueCandidate.Value.Val);
                        uniquePrefix = prefix;
                        uniqueCandidate = row;
                    }
                    else if (uniqueCandidate is null || row.Value.FromMem || !uniqueCandidate.Value.FromMem)
                    {
                        // MemTable wins over SST; for multiple legacy duplicates
                        // from the same source retain the last sorted owner.
                        uniqueCandidate = row;
                    }
                }

                if (++yielded % 1024 == 0)
                    await Task.Yield();
            }

            if (isUniqueIndex && uniqueCandidate is not null)
                yield return (uniqueCandidate.Value.Key, uniqueCandidate.Value.Val);
        }
    }

    private void EnsureCheckpointSpace(string currentSstPath, MemTable mem)
    {
        long estimate;
        try
        {
            estimate = File.Exists(currentSstPath) ? new FileInfo(currentSstPath).Length : 12;
            foreach (var row in mem.SnapshotAll(afterKeyExclusive: null))
                if (!row.Value.Tombstone && row.Value.Value is not null)
                    estimate = checked(estimate + row.Key.LongLength + row.Value.Value.LongLength + 64L);

            estimate = checked(estimate + 1024L * 1024L);
            var root = Path.GetPathRoot(Path.GetFullPath(_sstDir));
            if (string.IsNullOrWhiteSpace(root))
                return;

            long free = new DriveInfo(root).AvailableFreeSpace;
            if (free < estimate)
                throw new InsufficientCheckpointSpaceException(estimate, free);
        }
        catch (Exception ex)
        {
            if (ex is InsufficientCheckpointSpaceException)
                throw;
            WalnutLogger.Warning($"Could not determine free space before checkpoint: {ex.Message}");
        }
    }

    private sealed class InsufficientCheckpointSpaceException : IOException
    {
        public InsufficientCheckpointSpaceException(long required, long available)
            : base($"Checkpoint requires approximately {required} free bytes but only {available} bytes are available. WAL was preserved.") { }
    }

    private static void PromoteSstSidecar(string temporaryIndex, string finalIndex)
    {
        if (!File.Exists(temporaryIndex))
        {
            TryDeleteFile(finalIndex); // never keep an index for an older SST
            return;
        }

        if (File.Exists(finalIndex))
            File.Replace(temporaryIndex, finalIndex, destinationBackupFileName: null);
        else
            File.Move(temporaryIndex, finalIndex);
    }

    public ValueTask<DbStats> GetStatsAsync(CancellationToken ct = default)
    {
        // WAL: użyj ścieżki z WalWriter, jeśli dostępna
        string walPath = (Wal is WalWriter ww && !string.IsNullOrWhiteSpace(ww.Path))
            ? ww.Path
            : Path.Combine(_dir, "wal.log");

        long walBytes = 0;
        try
        {
            walBytes = File.Exists(walPath) ? new FileInfo(walPath).Length : 0;
        }
        catch { /* best-effort */ }

        var names = new HashSet<string>(_tables.Keys);
        foreach (var n in _sst.Keys) names.Add(n);

        long totalLive = 0;
        long totalDead = 0;
        var tables = new List<TableStats>();

        foreach (var name in names)
        {
            ct.ThrowIfCancellationRequested();

            long live = 0;
            long dead = 0;
            long memLive = 0;
            int sstCount = 0;
            long sstSizeBytes = 0;
            var coveredByMem = new HashSet<byte[]>(ByteArrayComparer.Instance);

            if (_tables.TryGetValue(name, out var memRef))
            {
                foreach (var kv in memRef.Current.SnapshotAll(afterKeyExclusive: null))
                {
                    coveredByMem.Add(kv.Key);
                    if (!kv.Value.Tombstone && kv.Value.Value is not null)
                    {
                        memLive += kv.Value.Value.LongLength;
                        live += kv.Value.Value.LongLength;
                    }
                }
            }

            if (_sst.TryGetValue(name, out var sst))
            {
                sstCount = 1;
                try
                {
                    sstSizeBytes = new FileInfo(sst.Path).Length;
                }
                catch { /* ignore */ }

                foreach (var kv in sst.ScanRange(Array.Empty<byte>(), Array.Empty<byte>()))
                {
                    if (coveredByMem.Contains(kv.Key)) dead += kv.Val.LongLength;
                    else live += kv.Val.LongLength;
                }
            }

            totalLive += live;
            totalDead += dead;

            double frag = (live + dead) > 0 ? (double)dead / (live + dead) * 100.0 : 0.0;
            tables.Add(new TableStats(name,
                TotalBytes: sstSizeBytes + memLive,
                LiveBytes: live,
                DeadBytes: dead,
                SstCount: sstCount,
                FragmentationPercent: frag));
        }

        long total = walBytes;
        foreach (var t in tables) total += t.TotalBytes;

        double totalFrag = (totalLive + totalDead) > 0 ? (double)totalDead / (totalLive + totalDead) * 100.0 : 0.0;
        var stats = new DbStats(total, walBytes, totalLive, totalDead, totalFrag, tables);
        return ValueTask.FromResult(stats);
    }

    public async ValueTask<BackupResult> CreateBackupAsync(string targetDir, CancellationToken ct = default)
    {
        var sourceFull = Path.GetFullPath(_dir).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        var targetFull = Path.GetFullPath(targetDir).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        if (targetFull.StartsWith(sourceFull, StringComparison.OrdinalIgnoreCase) ||
            sourceFull.StartsWith(targetFull, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("Backup target must be outside the database directory.", nameof(targetDir));

        await using var maintenanceLease = await MaintenanceGate.EnterWriteAsync(ct).ConfigureAwait(false);
        Directory.CreateDirectory(targetDir);
        using var targetLock = new FileStream(Path.Combine(targetDir, ".walnutdb.lock"),
            FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        var targetSstDir = Path.Combine(targetDir, "sst");
        Directory.CreateDirectory(targetSstDir);
        var incompleteMarker = Path.Combine(targetDir, ".walnutdb-backup-incomplete");
        WriteDurableMarker(incompleteMarker, "Backup is incomplete.\n");

        // 1) Trwały flush WAL do bieżącego pliku
        await Wal.FlushAsync(ct).ConfigureAwait(false);

        long copied = 0;

        // 2) Skopiuj wszystkie *.sst (snapshot listy; kopiuj z Share Read/Write)
        var sourceSstArtifacts = Directory.EnumerateFiles(_sstDir)
            .Where(path => path.EndsWith(".sst", StringComparison.OrdinalIgnoreCase) ||
                           path.EndsWith(".sst.sxi", StringComparison.OrdinalIgnoreCase))
            .ToArray();
        var sourceSstNames = new HashSet<string>(sourceSstArtifacts.Select(path => Path.GetFileName(path)!), StringComparer.OrdinalIgnoreCase);
        foreach (var file in sourceSstArtifacts)
        {
            ct.ThrowIfCancellationRequested();
            var dst = Path.Combine(targetSstDir, Path.GetFileName(file));
            using (var src = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var dstFs = new FileStream(dst, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await src.CopyToAsync(dstFs, ct).ConfigureAwait(false);
                dstFs.Flush(true);
                copied += dstFs.Length;
            }
        }

        // 3) Skopiuj WAL (użyj ścieżki z WalWriter, jeśli dostępna)
        string walPath = (Wal is WalWriter ww && !string.IsNullOrWhiteSpace(ww.Path))
            ? ww.Path
            : Path.Combine(_dir, "wal.log");

        if (File.Exists(walPath))
        {
            var dst = Path.Combine(targetDir, "wal.log");
            using (var src = new FileStream(walPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var dstFs = new FileStream(dst, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await src.CopyToAsync(dstFs, ct).ConfigureAwait(false);
                dstFs.Flush(true);
                copied += dstFs.Length;
            }
        }
        else
        {
            TryDeleteFile(Path.Combine(targetDir, "wal.log"));
        }

        // Manifest and CURRENT are part of the consistency boundary.
        var sourceManifests = Directory.EnumerateFiles(_dir, "MANIFEST-*").ToArray();
        var sourceManifestNames = new HashSet<string>(sourceManifests.Select(path => Path.GetFileName(path)!), StringComparer.OrdinalIgnoreCase);
        foreach (var mf in sourceManifests)
        {
            var dst = Path.Combine(targetDir, Path.GetFileName(mf));
            CopyFileDurably(mf, dst);
            try
            {
                copied += new FileInfo(mf).Length;
            }
            catch { /* best-effort */ }
        }

        var currentPath = Path.Combine(_dir, "CURRENT");
        if (File.Exists(currentPath))
        {
            var dst = Path.Combine(targetDir, "CURRENT");
            CopyFileDurably(currentPath, dst);
            copied += new FileInfo(currentPath).Length;
        }

        // Remove database artifacts left by an older backup in the same target.
        foreach (var stale in Directory.EnumerateFiles(targetSstDir))
        {
            var fileName = Path.GetFileName(stale);
            if ((fileName.EndsWith(".sst", StringComparison.OrdinalIgnoreCase) ||
                 fileName.EndsWith(".sst.sxi", StringComparison.OrdinalIgnoreCase)) &&
                !sourceSstNames.Contains(fileName))
                File.Delete(stale);
        }
        foreach (var stale in Directory.EnumerateFiles(targetDir, "MANIFEST-*"))
            if (!sourceManifestNames.Contains(Path.GetFileName(stale)))
                File.Delete(stale);

        File.Delete(incompleteMarker);

        return new BackupResult(targetDir, copied);
    }

    private static void WriteDurableMarker(string path, string contents)
    {
        var bytes = Encoding.UTF8.GetBytes(contents);
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read,
            bufferSize: 4096, options: FileOptions.WriteThrough);
        fs.Write(bytes);
        fs.Flush(true);
    }

    private static void CopyFileDurably(string source, string destination)
    {
        using var input = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var output = new FileStream(destination, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 81920, options: FileOptions.WriteThrough);
        input.CopyTo(output);
        output.Flush(true);
    }

    public async ValueTask DefragmentAsync(DefragMode mode, CancellationToken ct = default)
    {
        if (mode is not DefragMode.Compact and not DefragMode.RebuildSwap)
            throw new ArgumentOutOfRangeException(nameof(mode));

        // With one SST per table, a checkpoint already produces the compact,
        // tombstone-free representation. Reusing the single safe implementation
        // also avoids ciphertext double-encryption and concurrent-write loss.
        await CheckpointAsync(ct).ConfigureAwait(false);
    }

    public ValueTask<StorageVersionInfo> GetStorageVersionAsync(CancellationToken ct = default)
        => ValueTask.FromResult(new StorageVersionInfo(
            _catalog.StorageVersion,
            _catalog.CreatedWith,
            new[] { _catalog.WalFormat, _catalog.SstFormat, $"manifest-v{_catalog.ManifestVersion}" }));

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeOnce, 1) != 0)
            return;

        Exception? checkpointError = null;
        if (_options.CheckpointOnDispose)
        {
            try
            {
                await CheckpointAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Keep disposing so the WAL queue is drained. The WAL remains
                // the recovery source when checkpointing failed.
                checkpointError = ex;
            }
        }

        foreach (var s in _sst.Values)
            try
            {
                s.Dispose();
            }
            catch { }
        _sst.Clear();
        await Wal.DisposeAsync().ConfigureAwait(false);

        if (_options.Encryption is IDisposable disp)
            disp.Dispose();

        _databaseLock.Dispose();
        MaintenanceGate.Dispose();
        WriterLock.Dispose();

        if (checkpointError is not null)
            throw new IOException("Checkpoint-on-dispose failed; WAL was preserved for recovery.", checkpointError);
    }

    public ValueTask FlushAsync(CancellationToken ct = default)
    => Wal.FlushAsync(ct);

    public async ValueTask<ITable<T>> OpenTableAsync<T>(string name, TableOptions<T> options, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(IsDisposing, this);
        var canonical = CanonicalizeName(name);
        var memRef = GetOrAddMemRef(canonical);
        await Task.Yield();
        return new DefaultTable<T>(this, canonical, options, memRef);
    }

    public ValueTask<ITable<T>> OpenTableAsync<T>(TableOptions<T> options, CancellationToken ct = default)
        => OpenTableAsync<T>(_typeNames.Resolve(typeof(T)), options, ct);

    public async ValueTask DropTableAsync(string name, CancellationToken ct = default)
    {
        await using var maintenanceLease = await MaintenanceGate.EnterReadAsync(ct).ConfigureAwait(false);
        name = CanonicalizeName(name);

        var txId = (ulong)(Random.Shared.NextInt64() & long.MaxValue);
        var seq = (ulong)Interlocked.Increment(ref _nextSeqNo);

        var frames = new List<ReadOnlyMemory<byte>>(capacity: 3)
        {
            WalCodec.BuildBegin(txId, seq),
            WalCodec.BuildDropTable(txId, name),
            WalCodec.BuildCommit(txId, 1)
        };

        var handle = await Wal.AppendTransactionAsync(frames, Durability.Safe, ct).ConfigureAwait(false);
        await handle.WhenCommitted.ConfigureAwait(false);

        await WriterLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            DropTableInMemory(name);
        }
        finally
        {
            WriterLock.Release();
        }

        await PersistCatalogAsync(ct).ConfigureAwait(false);
        await Wal.FlushAsync(ct).ConfigureAwait(false);
    }

    private void DropTableInMemory(string name)
    {
        if (_tables.TryRemove(name, out var memRef))
        {
            memRef.Swap(new MemTable());
        }

        _metrics.TryRemove(name, out _);
        _indexUnique.TryRemove(name, out _);

        if (_sst.TryRemove(name, out var sst))
        {
            try
            {
                sst.Dispose();
            }
            catch { /* ignore */ }
        }

        DeleteTableFiles(name);

        var idxPrefix = $"__index__{name}__";
        var indexNames = _tables.Keys
            .Concat(_sst.Keys)
            .Where(n => n.StartsWith(idxPrefix, StringComparison.Ordinal))
            .Distinct()
            .ToArray();

        foreach (var idxName in indexNames)
        {
            if (_tables.TryRemove(idxName, out var idxRef))
            {
                idxRef.Swap(new MemTable());
            }

            _metrics.TryRemove(idxName, out _);

            if (_sst.TryRemove(idxName, out var idxSst))
            {
                try
                {
                    idxSst.Dispose();
                }
                catch { /* ignore */ }
            }

            DeleteTableFiles(idxName);
            RemoveUniqueGuardsForIndex(idxName);
            _indexUnique.TryRemove(idxName, out _);
        }
    }

    private void DeleteTableFiles(string canonicalName)
    {
        var sst = GetSstPath(canonicalName);
        TryDeleteFile(sst);
        TryDeleteFile(sst + ".sxi");
        TryDeleteFile(sst + ".tmp");
        TryDeleteFile(sst + ".tmp.sxi");
        _tableFiles.TryRemove(canonicalName, out _);
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch { /* best-effort */ }
    }

    private void PurgeDroppedArtifacts(ISet<string> droppedTables)
    {
        if (droppedTables.Count == 0)
            return;

        var indexPrefixes = droppedTables.Select(t => $"__index__{t}__").ToArray();

        bool IsDropped(string canonical)
        {
            if (droppedTables.Contains(canonical))
                return true;

            foreach (var prefix in indexPrefixes)
                if (canonical.StartsWith(prefix, StringComparison.Ordinal))
                    return true;

            return false;
        }

        foreach (var file in Directory.EnumerateFiles(_sstDir, "*.sst"))
        {
            var baseName = Path.GetFileNameWithoutExtension(file);
            var logical = _options.LegacyTableNameMappings?.TryGetValue(baseName, out var configured) == true
                ? configured
                : DecodeNameFromFile(baseName);
            var canonical = CanonicalizeName(logical);
            if (IsDropped(canonical))
            {
                TryDeleteFile(file);
                TryDeleteFile(file + ".sxi");
            }
        }

        foreach (var file in Directory.EnumerateFiles(_sstDir, "*.sst.tmp"))
        {
            var fn = Path.GetFileName(file);
            var stem = fn.Substring(0, fn.Length - ".sst.tmp".Length);
            var logical = _options.LegacyTableNameMappings?.TryGetValue(stem, out var configured) == true
                ? configured
                : DecodeNameFromFile(stem);
            var canonical = CanonicalizeName(logical);
            if (IsDropped(canonical))
            {
                TryDeleteFile(file);
                TryDeleteFile(file + ".sxi");
            }
        }

        foreach (var file in Directory.EnumerateFiles(_sstDir, "*.sst.tmp.sxi"))
        {
            var fn = Path.GetFileName(file);
            var stem = fn.Substring(0, fn.Length - ".sst.tmp.sxi".Length);
            var logical = DecodeNameFromFile(stem);
            var canonical = CanonicalizeName(logical);
            if (IsDropped(canonical))
                TryDeleteFile(file);
        }
    }

    private void RemoveUniqueGuardsForIndex(string indexTableName)
    {
        // klucz guardu: "<indexTableName>|<b64(prefix)>"
        foreach (var k in _uniqueGuards.Keys)
            if (k.StartsWith(indexTableName + "|", StringComparison.Ordinal))
                _uniqueGuards.TryRemove(k, out _);
    }

    public ValueTask DeleteTableAsync(string name, CancellationToken ct = default)
    => DropTableAsync(name, ct);

    public IEnumerable<string> EnumerateTableNames(bool includeIndexes = false)
    {
        var names = _tables.Keys.Concat(_sst.Keys).Distinct();
        return includeIndexes ? names : names.Where(n => !n.StartsWith("__index__", StringComparison.Ordinal));
    }

    public async ValueTask<ITimeSeriesTable<T>> OpenTimeSeriesAsync<T>(string name, TimeSeriesOptions<T> options, CancellationToken ct = default)
    {
        var canonical = CanonicalizeName(name);
        var mapper = new TimeSeriesMapper<T>(options);
        var tbl = await OpenTableAsync<T>(canonical, new TableOptions<T>
        {
            GetId = (T item) => (object)mapper.BuildKey(item),
            Serialize = options.Serialize,
            Deserialize = options.Deserialize,
            StoreGuidStringsAsBinary = options.StoreGuidStringsAsBinary
        }, ct).ConfigureAwait(false);
        return new TimeSeriesTable<T>(tbl, mapper);
    }

    public ValueTask<ITimeSeriesTable<T>> OpenTimeSeriesAsync<T>(TimeSeriesOptions<T> options, CancellationToken ct = default)
        => OpenTimeSeriesAsync<T>(_typeNames.Resolve(typeof(T)), options, ct);

    public ValueTask DeleteTimeSeriesAsync(string name, CancellationToken ct = default)
        => DeleteTableAsync(name, ct);

    public async ValueTask<ITransaction> BeginTransactionAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(IsDisposing, this);
        var txId = (ulong)(Random.Shared.NextInt64() & long.MaxValue);
        var seq = (ulong)Interlocked.Increment(ref _nextSeqNo);
        await Task.Yield();
        return new WalnutTransaction(this, txId, seq);
    }

    public async ValueTask RunInTransactionAsync(Func<ITransaction, ValueTask> work, CancellationToken ct = default)
    {
        await using var tx = await BeginTransactionAsync(ct).ConfigureAwait(false);
        await work(tx).ConfigureAwait(false);
        await tx.CommitAsync(Durability.Safe, ct).ConfigureAwait(false);
    }

    public async ValueTask<PreflightReport> PreflightAsync(string directory, long reserveBytes = 4 * 1024 * 1024, CancellationToken ct = default)
    {
        Directory.CreateDirectory(directory);
        var testFile = Path.Combine(directory, ".walnutdb.preflight");
        try
        {
            await File.WriteAllBytesAsync(testFile, new byte[1024], ct).ConfigureAwait(false);
            var bytes = await File.ReadAllBytesAsync(testFile, ct).ConfigureAwait(false);
            using var fs = new FileStream(testFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            bool canExclusive = fs.CanWrite;

            var di = new DriveInfo(Path.GetPathRoot(directory)!);
            long free = di.AvailableFreeSpace;
            string fsName = di.DriveFormat;
            string os = RuntimeInformation.OSDescription;

            return new PreflightReport(true, true, true, canExclusive, free, fsName, os, free < reserveBytes ? "Low free space" : "");
        }
        finally
        {
            try
            {
                File.Delete(testFile);
            }
            catch { /* ignore */ }
        }
    }

    // ---- pomocnicze typy/adaptery ----

    private sealed class DefaultTypeNameResolver : ITypeNameResolver
    {
        private readonly DatabaseOptions _opt;
        public DefaultTypeNameResolver(DatabaseOptions opt) { _opt = opt; }

        public string Resolve(Type t)
        {
            // Bazowa nazwa zgodnie z konfiguracją (jak dotąd)
            string baseName = _opt.TypeNaming switch
            {
                TypeNamingStrategy.TypeFullName => t.FullName ?? t.Name,
                TypeNamingStrategy.TypeNameOnly => t.Name,
                TypeNamingStrategy.AssemblyQualifiedNoVer => $"{t.Namespace}.{t.Name}, {t.Assembly.GetName().Name}",
                _ => t.FullName ?? t.Name
            };

            // 1) Znormalizuj nazwy generyków/tupli: obetnij listę argumentów ([...])
            int bracket = baseName.IndexOf('[');
            if (bracket >= 0)
                baseName = baseName.Substring(0, bracket);

            // 2) Dodaj arność dla generyków (np. ValueTuple`2)
            if (t.IsGenericType && !baseName.Contains('`'))
                baseName += $"`{t.GetGenericArguments().Length}";

            // 3) Uczyń nazwę krótką i unikalną: dodaj krótki hash typu (AQN daje stabilny podpis)
            string sig = t.AssemblyQualifiedName ?? (t.FullName ?? t.Name);
            string hash8 = ToShortHash(sig); // 8 hex (32 bity)

            string shortName = $"{baseName}-{hash8}";

            // 4) Ostatecznie, jeśli i tak jest długa, przytnij do sensownego limitu (np. 64 znaki)
            if (shortName.Length > 64)
                shortName = shortName.Substring(0, 64);

            return shortName;
        }

        private static string ToShortHash(string s)
        {
            using var sha1 = System.Security.Cryptography.SHA1.Create();
            var bytes = System.Text.Encoding.UTF8.GetBytes(s);
            var h = sha1.ComputeHash(bytes);
            // 4 bajty → 8 znaków hex; wystarczająco mało i stabilnie
            return Convert.ToHexString(h, 0, 4).ToLowerInvariant();
        }
    }

}
