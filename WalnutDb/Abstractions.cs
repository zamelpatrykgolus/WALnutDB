#nullable enable
using System.Runtime.CompilerServices;

using WalnutDb.Indexing;

namespace WalnutDb;

/// <summary>
/// Poziom trwałości przy Commit: Safe = fsync per commit; Group = grupowanie commitów; Fast = rzadkie fsync.
/// </summary>
public enum Durability { Safe, Group, Fast }

/// <summary>
/// Tryb defragmentacji. W formacie SSTv1 oba tryby korzystają z bezpiecznej
/// przebudowy pojedynczego segmentu na tabelę; wartości pozostają rozdzielone,
/// aby zachować zgodność API z przyszłymi formatami wielosegmentowymi.
/// </summary>
public enum DefragMode { Compact, RebuildSwap }

/// <summary>
/// Strategia nazywania tabel po typie (dla overloadów bez jawnej nazwy).
/// </summary>
public enum TypeNamingStrategy { TypeFullName, TypeNameOnly, AssemblyQualifiedNoVer, Custom }

/// <summary>
/// Opcje poziomu bazy danych.
/// </summary>
public sealed class DatabaseOptions
{
    public IEncryption? Encryption { get; init; }
    /// <summary>Strategia wyprowadzania nazw tabel na podstawie typu T.</summary>
    public TypeNamingStrategy TypeNaming { get; init; } = TypeNamingStrategy.TypeFullName;

    /// <summary>Własny namer, jeśli TypeNaming == Custom.</summary>
    public Func<Type, string>? CustomTypeNamer { get; init; }

    /// <summary>Czy nazwy mają być stabilne między platformami (bez wpływu kultury/regionalnych ustawień).</summary>
    public bool CrossPlatformStableNames { get; init; } = true;

    /// <summary>Przy Dispose() wykonać Checkpoint (flush memtable→SST + rotacja WAL)?</summary>
    public bool CheckpointOnDispose { get; init; } = true;

    /// <summary>
    /// Opcjonalne rozstrzygnięcie niejednoznacznych nazw w bazach sprzed manifestu.
    /// Kluczem jest nazwa pliku SST bez rozszerzenia, wartością logiczna nazwa tabeli.
    /// Migracja pozostaje metadana-only i nie zmienia wskazanego SST.
    /// </summary>
    public IReadOnlyDictionary<string, string>? LegacyTableNameMappings { get; init; }
}

/// <summary>
/// Podstawowe metadane wersjonowania formatu plików.
/// </summary>
public sealed record StorageVersionInfo(int StorageVersion, string CreatedWith, IReadOnlyList<string> FeatureFlags);

/// <summary>
/// Wynik backupu.
/// </summary>
public sealed record BackupResult(string TargetDirectory, long BytesCopied);

/// <summary>
/// Statystyki pojedynczej tabeli.
/// </summary>
public sealed record TableStats(string Name, long TotalBytes, long LiveBytes, long DeadBytes, int SstCount, double FragmentationPercent);

/// <summary>
/// Statystyki całej bazy.
/// </summary>
public sealed record DbStats(long TotalBytes, long WalBytes, long LiveBytes, long DeadBytes,
                             double FragmentationPercent, IReadOnlyList<TableStats> Tables);

/// <summary>
/// Raport testu wstępnego (uprawnienia/miejsce/locki) dla katalogu bazy.
/// </summary>
public sealed record PreflightReport(
    bool CanCreateDirectory,
    bool CanCreateFiles,
    bool CanReadWrite,
    bool CanExclusiveLock,
    long FreeBytes,
    string FileSystem,
    string OsDescription,
    string Notes);

/// <summary>
/// Interfejs bazy. Implementacja musi być bezpieczna na zaniki zasilania (WAL + manifest snapshoty).
/// </summary>
public interface IDatabase : IAsyncDisposable
{
    // ——— Admin / meta ———
    ValueTask<DbStats> GetStatsAsync(CancellationToken ct = default);
    ValueTask<BackupResult> CreateBackupAsync(string targetDir, CancellationToken ct = default);
    ValueTask DefragmentAsync(DefragMode mode, CancellationToken ct = default);
    ValueTask<StorageVersionInfo> GetStorageVersionAsync(CancellationToken ct = default);
    ValueTask<PreflightReport> PreflightAsync(string directory, long reserveBytes = 4 * 1024 * 1024, CancellationToken ct = default);

    // ——— Trwałość ręczna ———
    /// <summary>Natychmiastowy fsync WAL (bez flushu memtable). Przydatne przed restartem.</summary>
    ValueTask FlushAsync(CancellationToken ct = default);
    /// <summary>Flush memtable do SST + rotacja WAL; skraca recovery.</summary>
    ValueTask CheckpointAsync(CancellationToken ct = default);

    // ——— Tabele (Key-Value / dokumentowe) ———
    ValueTask<ITable<T>> OpenTableAsync<T>(string name, TableOptions<T> options, CancellationToken ct = default);
    ValueTask<ITable<T>> OpenTableAsync<T>(TableOptions<T> options, CancellationToken ct = default);
    ValueTask DeleteTableAsync(string name, CancellationToken ct = default);

    // ——— Time Series ———
    ValueTask<ITimeSeriesTable<T>> OpenTimeSeriesAsync<T>(string name, TimeSeriesOptions<T> options, CancellationToken ct = default);
    ValueTask<ITimeSeriesTable<T>> OpenTimeSeriesAsync<T>(TimeSeriesOptions<T> options, CancellationToken ct = default);
    ValueTask DeleteTimeSeriesAsync(string name, CancellationToken ct = default);

    // ——— Transakcje ———
    ValueTask<ITransaction> BeginTransactionAsync(CancellationToken ct = default);
    ValueTask RunInTransactionAsync(Func<ITransaction, ValueTask> work, CancellationToken ct = default);
}

/// <summary>
/// Transakcja. Brak CommitAsync() == brak efektów (rollback implicit).
/// </summary>
public interface ITransaction : IAsyncDisposable
{
    ValueTask CommitAsync(Durability durability = Durability.Safe, CancellationToken ct = default);
}

/// <summary>
/// Tabela generyczna operująca na obiektach T (serializowanych do bajtów według TableOptions).
/// </summary>
public interface ITable<T>
{
    // ——— Auto-transakcje ———
    ValueTask<bool> UpsertAsync(T item, CancellationToken ct = default);
    ValueTask<bool> DeleteAsync(object id, CancellationToken ct = default);
    /// <summary>Usuwa obiekt na podstawie jego ID wyciągniętego z instancji (np. z [DatabaseObjectId]).</summary>
    ValueTask<bool> DeleteAsync(T item, CancellationToken ct = default);
    /// <summary>Ergonomia: usuń po Guid.</summary>
    ValueTask<bool> DeleteAsync(Guid id, CancellationToken ct = default);
    /// <summary>Ergonomia: usuń po string ID.</summary>
    ValueTask<bool> DeleteAsync(string id, CancellationToken ct = default);

    // ——— Ręczne transakcje ———
    ValueTask<bool> UpsertAsync(T item, ITransaction tx, CancellationToken ct = default);
    ValueTask<bool> DeleteAsync(object id, ITransaction tx, CancellationToken ct = default);
    /// <summary>Usuwa obiekt w ramach transakcji, ID pobierane z instancji.</summary>
    ValueTask<bool> DeleteAsync(T item, ITransaction tx, CancellationToken ct = default);
    /// <summary>Ergonomia: usuń po Guid w ramach transakcji.</summary>
    ValueTask<bool> DeleteAsync(Guid id, ITransaction tx, CancellationToken ct = default);
    /// <summary>Ergonomia: usuń po string ID w ramach transakcji.</summary>
    ValueTask<bool> DeleteAsync(string id, ITransaction tx, CancellationToken ct = default);

    // ——— Odczyty ———
    ValueTask<T?> GetAsync(object id, CancellationToken ct = default);
    /// <summary>Ergonomia: pobierz po Guid.</summary>
    ValueTask<T?> GetAsync(Guid id, CancellationToken ct = default);
    /// <summary>Ergonomia: pobierz po string ID.</summary>
    ValueTask<T?> GetAsync(string id, CancellationToken ct = default);

    /// <summary>Zwraca pierwszy element spełniający predykat (filtrowanie po stronie klienta; opcjonalny pushdown przez IndexHint).</summary>
    ValueTask<T?> GetFirstAsync(Func<T, bool> predicate, CancellationToken ct = default);
    ValueTask<T?> GetFirstAsync(Func<T, bool> predicate, IndexHint hint, CancellationToken ct = default);

    /// <summary>Strumień wszystkich elementów z paginacją tokenem.</summary>
    IAsyncEnumerable<T> GetAllAsync(int pageSize = 1024, ReadOnlyMemory<byte> token = default, CancellationToken ct = default);

    /// <summary>Skan zakresowy po kluczu głównym (prefiks/od-do).</summary>
    IAsyncEnumerable<T> ScanByKeyAsync(ReadOnlyMemory<byte> fromInclusive,
                                       ReadOnlyMemory<byte> toExclusive,
                                       int pageSize = 1024,
                                       ReadOnlyMemory<byte> token = default,
                                       CancellationToken ct = default);

    /// <summary>Skan po indeksie (prefiks pushdown); resztę można dociąć LINQ po stronie klienta.</summary>
    IAsyncEnumerable<T> ScanByIndexAsync(string indexName,
                                         ReadOnlyMemory<byte> start,
                                         ReadOnlyMemory<byte> end,
                                         int pageSize = 1024,
                                         ReadOnlyMemory<byte> token = default,
                                         CancellationToken ct = default);

    /// <summary>Zapytanie z predykatem; opcjonalnie IndexHint do zredukowania zakresu scan.</summary>
    IAsyncEnumerable<T> QueryAsync(Func<T, bool> predicate,
                                   int pageSize = 1024,
                                   ReadOnlyMemory<byte> token = default,
                                   CancellationToken ct = default);
    IAsyncEnumerable<T> QueryAsync(Func<T, bool> predicate,
                                   IndexHint hint,
                                   int pageSize = 1024,
                                   ReadOnlyMemory<byte> token = default,
                                   CancellationToken ct = default);
}

/// <summary>
/// Tabela Time Series – klucz złożony z SeriesId i UTC timestampu kodowanego tak, by porządek bajtowy == czasowy.
/// </summary>
public interface ITimeSeriesTable<T>
{
    ValueTask AppendAsync(T sample, CancellationToken ct = default);

    ValueTask AppendAsync(T sample, ITransaction tx, CancellationToken ct = default);

    IAsyncEnumerable<T> QueryAsync(object seriesId, DateTime fromUtc, DateTime toUtc,
                                   int pageSize = 2048,
                                   ReadOnlyMemory<byte> token = default,
                                   CancellationToken ct = default);

    IAsyncEnumerable<T> QueryTailAsync(object seriesId, int take, CancellationToken cancellationToken = default);
}
