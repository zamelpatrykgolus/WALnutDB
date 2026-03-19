using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

using WalnutDb.Indexing;
using WalnutDb.Sst;
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
    private readonly ConcurrentDictionary<string, MemTableRef> _tables = new();
    internal readonly ConcurrentDictionary<string, TableMetrics> _metrics = new();
    private readonly ConcurrentDictionary<string, byte[]> _uniqueGuards = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _indexUnique = new(StringComparer.Ordinal);

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

        // ⬇⬇⬇ NOWE: ustal właściwą ścieżkę WAL
        string walPath = (wal is WalWriter ww && !string.IsNullOrWhiteSpace(ww.Path))
            ? ww.Path
            : Path.Combine(_dir, "wal.log");

        var recovered = new ConcurrentDictionary<string, MemTable>();
        var droppedTables = new HashSet<string>(StringComparer.Ordinal);

        if (File.Exists(walPath))
        {
            try
            {
                WalRecovery.Replay(walPath, recovered, droppedTables, _options.Encryption);
                AdoptRecoveredTables(recovered);
            }
            catch (Exception ex)
            {
                WalnutLogger.Exception(ex);
            }
        }

        _sstDir = Path.Combine(_dir, "sst");
        Directory.CreateDirectory(_sstDir);

        if (droppedTables.Count > 0)
            PurgeDroppedArtifacts(droppedTables);

        MigrateSstFilenames();

        foreach (var file in Directory.EnumerateFiles(_sstDir, "*.sst"))
        {
            var baseName = Path.GetFileNameWithoutExtension(file);
            var logicalName = DecodeNameFromFile(baseName);
            var canonical = CanonicalizeName(logicalName);
            try
            {
                _sst[canonical] = new SstReader(file);
            }
            catch (Exception ex)
            {
                WalnutLogger.Exception(ex);
                /* ignore */
            }
        }

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

        var safe = EncodeNameToFile(indexTableName);
        var sstPath = Path.Combine(_sstDir, safe + ".sst");
        try { if (File.Exists(sstPath)) File.Delete(sstPath); } catch { }

        var sxiPath = sstPath + ".sxi";
        try { if (File.Exists(sxiPath)) File.Delete(sxiPath); } catch { }
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

    private static string? TryExtractBaseTableName(string indexTableName)
    {
        const string prefix = "__index__";
        if (!indexTableName.StartsWith(prefix, StringComparison.Ordinal))
            return null;

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

        var safe = EncodeNameToFile(indexTableName);
        var candidate = Path.Combine(_sstDir, safe + ".sst");
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
        if (!_tables.TryGetValue(name, out var refMem)) return;

        // 1) Zamroź starą memkę dla tej tabeli (swap na świeżą)
        var oldMem = refMem.Swap(new MemTable());

        // 2) Zbierz wpisy z oldMem oraz zbuduj zbiór kluczy „pokrytych”
        var list = new List<(byte[] Key, byte[] Val)>();
        var covered = new HashSet<string>(); // użyjemy podpisu Base64 dla porównywania kluczy

        foreach (var it in oldMem.SnapshotAll(null))
        {
            var sig = Convert.ToBase64String(it.Key);
            covered.Add(sig); // klucz istnieje w snapshot'cie (żywy lub tombstone)

            if (!it.Value.Tombstone && it.Value.Value is not null)
            {
                var v = it.Value.Value;
                var vOut = Encryption is null ? v : Encryption.Encrypt(v, name, it.Key);
                list.Add((it.Key, vOut));
            }

        }

        // 3) Dociągnij z SST te klucze, których nie nadpisała/nie usunęła oldMem
        foreach (var (k, v) in ScanSstRange(name, Array.Empty<byte>(), Array.Empty<byte>()))
        {
            var sig = Convert.ToBase64String(k);
            if (!covered.Contains(sig))
            {
                var vOut = Encryption is null ? v : Encryption.Encrypt(v, name, k);
                list.Add((k, vOut));
            }
        }

        // 4) Posortuj i zapisz nowy SST
        list.Sort(static (a, b) =>
        {
            int min = Math.Min(a.Key.Length, b.Key.Length);
            for (int i = 0; i < min; i++) { int d = a.Key[i] - b.Key[i]; if (d != 0) return d; }
            return a.Key.Length - b.Key.Length;
        });

        async IAsyncEnumerable<(byte[] Key, byte[] Val)> Source()
        {
            foreach (var t in list) { yield return t; await Task.Yield(); }
        }

        var safe = EncodeNameToFile(name);
        var tmp = Path.Combine(_sstDir, $"{safe}.sst.tmp");
        var dst = Path.Combine(_sstDir, $"{safe}.sst");

        await SstWriter.WriteAsync(tmp, Source(), ct).ConfigureAwait(false);
        if (File.Exists(dst)) File.Replace(tmp, dst, null); else File.Move(tmp, dst);
        ReplaceSst(name, dst);

        // 5) Trwałość
        await Wal.FlushAsync(ct).ConfigureAwait(false);
        await Wal.TruncateAsync(ct).ConfigureAwait(false);
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

        // Krótki retry w razie wyścigu z ReplaceSst (ObjectDisposed/IO)
        for (int attempt = 0; attempt < 3; attempt++)
        {
            try
            {
                if (_sst.TryGetValue(name, out var sst))
                    return sst.TryGet(key, out value);

                return false;
            }
            catch (IOException)
            {
                // chwilowo podmieniany plik – spróbuj ponownie
            }
            catch (ObjectDisposedException)
            {
                // stary reader został właśnie wymieniony – spróbuj ponownie
            }

            // króciutka pauza zanim sprawdzimy jeszcze raz
            System.Threading.Thread.SpinWait(64);
        }

        return false;
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
        // bardzo krótki, „bezpieczny” reader – 3 podejścia i koniec
        for (int attempt = 0; attempt < 3; attempt++)
        {
            Sst.SstReader? sst;
            if (!_sst.TryGetValue(name, out sst))
                yield break;

            IEnumerator<(byte[] Key, byte[] Val)>? it = null;
            try
            {
                it = sst.ScanRange(fromInclusive, toExclusive).GetEnumerator();
            }
            catch (IOException)
            {
                System.Threading.Thread.SpinWait(64);
                continue;
            }

            using (it)
            {
                while (true)
                {
                    bool ok;
                    try { ok = it.MoveNext(); }
                    catch (IOException) { ok = false; }
                    if (!ok) yield break;

                    yield return it.Current;
                }
            }

            yield break; // sukces – nie próbujemy ponownie
        }

        // po 3 próbach – oddaj pustą sekwencję
        yield break;
    }


    private void ReplaceSst(string name, string newPath)
    {
        var reader = new SstReader(newPath);
        if (_sst.TryRemove(name, out var old))
        {
            try { old.Dispose(); } catch { }
        }
        _sst[name] = reader;
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

    private static string DecodeNameFromFile(string fileBaseName)
    {
        // Rozpoznaj *kanoniczne* Base64-url: dekoduj bajty, sprawdź round-trip do identycznego ciągu
        // i zaakceptuj TYLKO gdy UTF-8 jest poprawny.
        if (TryDecodeBase64UrlRoundtrip(fileBaseName, out var decoded))
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
        var snapshot = new List<(string Name, MemTable Old)>(_tables.Count);

        await WriterLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            foreach (var kv in _tables.ToArray())
            {
                ct.ThrowIfCancellationRequested();
                var (name, @ref) = (kv.Key, kv.Value);
                var old = @ref.Swap(new MemTable());
                snapshot.Add((name, old));
            }
        }
        finally { WriterLock.Release(); }

        var enc = Encryption;

        foreach (var (name, oldMem) in snapshot)
        {
            ct.ThrowIfCancellationRequested();

            bool isIndex = name.StartsWith("__index__", StringComparison.Ordinal);
            bool isUniqueIndex = isIndex && IsIndexUnique(name);

            var coveredExact = new HashSet<string>(StringComparer.Ordinal);

            List<(byte[] Key, byte[] Val)> outList =
                (!isIndex || !isUniqueIndex) ? new() : null!;

            Dictionary<string, (byte[] Key, byte[] Val)> outByPrefix =
                (isIndex && isUniqueIndex) ? new(StringComparer.Ordinal) : null!;

            // 2a) Z RAM (snapshot)
            foreach (var it in oldMem.SnapshotAll(afterKeyExclusive: null))
            {
                coveredExact.Add(Convert.ToBase64String(it.Key));

                if (it.Value.Tombstone || it.Value.Value is null) continue;

                if (!isIndex || !isUniqueIndex)
                {
                    var vOut = enc is null ? it.Value.Value : enc.Encrypt(it.Value.Value, name, it.Key);
                    outList.Add((it.Key, vOut));
                }
                else
                {
                    var pSig = Convert.ToBase64String(IndexKeyCodec.ExtractValuePrefix(it.Key));
                    var vOut = enc is null ? it.Value.Value : enc.Encrypt(it.Value.Value, name, it.Key);
                    outByPrefix[pSig] = (it.Key, vOut); // RAM wygrywa w danym prefiksie
                }
            }

            // 2b) Z poprzedniego SST
            if (_sst.TryGetValue(name, out var prev))
            {
                foreach (var (k, v) in prev.ScanRange(Array.Empty<byte>(), Array.Empty<byte>()))
                {
                    if (coveredExact.Contains(Convert.ToBase64String(k))) continue;

                    if (!isIndex || !isUniqueIndex)
                    {
                        outList.Add((k, v));
                    }
                    else
                    {
                        var pSig = Convert.ToBase64String(IndexKeyCodec.ExtractValuePrefix(k));
                        if (!outByPrefix.ContainsKey(pSig))
                            outByPrefix[pSig] = (k, v);
                    }
                }
            }

            IEnumerable<(byte[] Key, byte[] Val)> materialized =
            (isIndex && isUniqueIndex)
                ? (IEnumerable<(byte[] Key, byte[] Val)>)outByPrefix.Values
                : outList;

            var list = new List<(byte[] Key, byte[] Val)>(materialized);
            list.Sort(static (a, b) =>
            {
                int min = Math.Min(a.Key.Length, b.Key.Length);
                for (int i = 0; i < min; i++) { int d = a.Key[i] - b.Key[i]; if (d != 0) return d; }
                return a.Key.Length - b.Key.Length;
            });

            async IAsyncEnumerable<(byte[] Key, byte[] Val)> Source()
            {
                foreach (var t in list) { yield return t; await Task.Yield(); }
            }

            var safe = EncodeNameToFile(name);
            var tmp = Path.Combine(_sstDir, $"{safe}.sst.tmp");
            var dst = Path.Combine(_sstDir, $"{safe}.sst");

            await SstWriter.WriteAsync(tmp, Source(), ct).ConfigureAwait(false);
            if (File.Exists(dst)) File.Replace(tmp, dst, null); else File.Move(tmp, dst);
            ReplaceSst(name, dst);
        }

        await Wal.FlushAsync(ct).ConfigureAwait(false);
        await Wal.TruncateAsync(ct).ConfigureAwait(false);
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
            int sstCount = 0;
            long sstSizeBytes = 0;

            if (_tables.TryGetValue(name, out var memRef))
            {
                foreach (var kv in memRef.Current.SnapshotAll(afterKeyExclusive: null))
                {
                    if (kv.Value.Tombstone) dead++;
                    else if (kv.Value.Value is not null) live += kv.Value.Value.LongLength;
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
                    live += kv.Val.LongLength;
            }

            totalLive += live;
            totalDead += dead;

            double frag = (live + dead) > 0 ? (double)dead / (live + dead) * 100.0 : 0.0;
            tables.Add(new TableStats(name,
                TotalBytes: sstSizeBytes + live,
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
        Directory.CreateDirectory(targetDir);
        var targetSstDir = Path.Combine(targetDir, "sst");
        Directory.CreateDirectory(targetSstDir);

        // 1) Trwały flush WAL do bieżącego pliku
        await Wal.FlushAsync(ct).ConfigureAwait(false);

        long copied = 0;

        // 2) Skopiuj wszystkie *.sst (snapshot listy; kopiuj z Share Read/Write)
        foreach (var file in Directory.EnumerateFiles(_sstDir, "*.sst"))
        {
            ct.ThrowIfCancellationRequested();
            var dst = Path.Combine(targetSstDir, Path.GetFileName(file));
            using (var src = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var dstFs = new FileStream(dst, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await src.CopyToAsync(dstFs, ct).ConfigureAwait(false);
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
                copied += dstFs.Length;
            }
        }

        // (opcjonalnie) manifesty
        foreach (var mf in Directory.EnumerateFiles(_dir, "*.manifest"))
        {
            var dst = Path.Combine(targetDir, Path.GetFileName(mf));
            File.Copy(mf, dst, overwrite: true);
            try
            {
                copied += new FileInfo(mf).Length;
            }
            catch { /* best-effort */ }
        }

        return new BackupResult(targetDir, copied);
    }

    public async ValueTask DefragmentAsync(DefragMode mode, CancellationToken ct = default)
    {
        // MVP: „defragmentacja” == pełny rebuild SST z aktualnego widoku + opróżnienie Mem (swap),
        //       a potem truncate WAL. Wymaga chwilowego single-writera na czas swapu memek.
        //       Działa identycznie dla Compact i RebuildSwap w tym modelu 1-SST-na-tabelę.

        // 1) Zbuduj świeże SST z live view (Mem ∪ SST)
        foreach (var kv in _tables.ToArray())
        {
            ct.ThrowIfCancellationRequested();
            var name = kv.Key;

            // poskładamy pełny widok jako async-enum (merge zrobi DefaultTable.ScanByKeyAsync,
            // ale tu zrobimy lokalnie: Mem.Current + SST -> lista posortowana)
            var mem = kv.Value.Current;

            // zbierz live wpisy z Mem
            var live = new List<(byte[] Key, byte[] Val)>();
            foreach (var it in mem.SnapshotAll(afterKeyExclusive: null))
                if (!it.Value.Tombstone && it.Value.Value is not null)
                {
                    var v = it.Value.Value;
                    var vOut = Encryption is null ? v : Encryption.Encrypt(v, name, it.Key);
                    live.Add((it.Key, vOut));
                }

            // dołóż wszystko z aktualnego SST (jeśli klucz nie jest nadpisany przez Mem)
            if (_sst.TryGetValue(name, out var sst))
            {
                foreach (var it in sst.ScanRange(Array.Empty<byte>(), Array.Empty<byte>()))
                {
                    // jeśli Mem nie ma override, weź z SST
                    // (proste sprawdzenie – w małej bazie OK; dla większej można sortować/mergować)
                    bool overridden = mem.TryGet(it.Key, out var raw) && raw is not null;
                    if (!overridden)
                    {
                        var vOut = Encryption is null ? it.Val : Encryption.Encrypt(it.Val, name, it.Key);
                        live.Add((it.Key, vOut));
                    }
                }
            }

            // posortuj po kluczu
            live.Sort(static (a, b) =>
            {
                int min = Math.Min(a.Key.Length, b.Key.Length);
                for (int i = 0; i < min; i++)
                {
                    int d = a.Key[i] - b.Key[i];
                    if (d != 0) return d;
                }
                return a.Key.Length - b.Key.Length;
            });

            // asynchroniczne źródło
            async IAsyncEnumerable<(byte[] Key, byte[] Val)> AllAsync()
            {
                foreach (var t in live) { yield return t; await Task.Yield(); }
            }

            var safe = EncodeNameToFile(name);
            var tmp = Path.Combine(_sstDir, $"{safe}.sst.tmp");
            var dst = Path.Combine(_sstDir, $"{safe}.sst");

            await SstWriter.WriteAsync(tmp, AllAsync(), ct).ConfigureAwait(false);

            if (File.Exists(dst))
                File.Replace(tmp, dst, destinationBackupFileName: null);
            else
                File.Move(tmp, dst);

            ReplaceSst(name, dst);
        }

        // 2) Wymiana MemTableRef na świeże (czyści tombstony i „pofragmentowanie” pamięci)
        await WriterLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            foreach (var k in _tables.Keys)
            {
                // swap na pustą memę
                var old = _tables[k].Swap(new MemTable());
                // stara instancja do GC
            }
        }
        finally
        {
            WriterLock.Release();
        }

        // 3) Opróżnij WAL
        await Wal.FlushAsync(ct).ConfigureAwait(false);
        if (Wal is WalWriter ww)
            await ww.TruncateAsync(ct).ConfigureAwait(false);
    }

    public ValueTask<StorageVersionInfo> GetStorageVersionAsync(CancellationToken ct = default)
        => ValueTask.FromResult(new StorageVersionInfo(1, "WalnutDb-0.1", Array.Empty<string>()));

    public async ValueTask DisposeAsync()
    {
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
    }

    public ValueTask FlushAsync(CancellationToken ct = default)
    => Wal.FlushAsync(ct);

    public async ValueTask<ITable<T>> OpenTableAsync<T>(string name, TableOptions<T> options, CancellationToken ct = default)
    {
        var canonical = CanonicalizeName(name);
        var memRef = GetOrAddMemRef(canonical);
        await Task.Yield();
        return new DefaultTable<T>(this, canonical, options, memRef);
    }

    public ValueTask<ITable<T>> OpenTableAsync<T>(TableOptions<T> options, CancellationToken ct = default)
        => OpenTableAsync<T>(_typeNames.Resolve(typeof(T)), options, ct);

    public async ValueTask DropTableAsync(string name, CancellationToken ct = default)
    {
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

        await WriterLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            DropTableInMemory(name);
        }
        finally
        {
            WriterLock.Release();
        }

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
        var safe = EncodeNameToFile(canonicalName);
        TryDeleteFile(Path.Combine(_sstDir, $"{safe}.sst"));
        TryDeleteFile(Path.Combine(_sstDir, $"{safe}.sst.sxi"));
        TryDeleteFile(Path.Combine(_sstDir, $"{safe}.sst.tmp"));
        TryDeleteFile(Path.Combine(_sstDir, $"{safe}.sst.tmp.sxi"));
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
            var logical = DecodeNameFromFile(baseName);
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
            var logical = DecodeNameFromFile(stem);
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
