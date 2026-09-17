# WalnutDb — Dokumentacja techniczna

Witaj w dokumentacji **WalnutDb** — lekkiej bazy typu *LSM* (memtable + SST), z transakcjami, WAL, indeksami (także unikalnymi) i opcjonalnym szyfrowaniem payloadów.

## Spis treści

- [Architektura](architecture.md)
- [Warstwa przechowywania](storage.md)
- [Wersje formatu i migracje](storage-versions.md)
- [Storage v2: migracja, delty i compaction](storage-v2.md)
- [Transakcje i WAL](transactions.md)
- [Indeksy](indexing.md)
- [Unikalność i współbieżność](uniqueness.md)
- [Skanowanie, zapytania i stronicowanie](scanning.md)
- [Szyfrowanie](encryption.md)
- [API i przykłady](api.md)
- [Testy i praktyki](testing.md)
- [Rozwiązywanie problemów (FAQ)](troubleshooting.md)
- [Szczegóły wewnętrzne / kodowanie kluczy](internals/encoding.md)
- [Plan rozwoju](roadmap.md)

## Szybki start

```csharp
using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
await using var db = new WalnutDatabase(
    dir,
    new DatabaseOptions(),
    new FileSystemManifestStore(dir),
    wal);

// Definicja tabeli
var users = await db.OpenTableAsync(new TableOptions<User> {
    GetId = u => u.Id
});

// Wstawienie / aktualizacja
await users.UpsertAsync(new User { Id = "U001", Email = "alice@example.com" });

// Odczyt
var u = await users.GetAsync("U001");

// Indeks (atrybut na modelu)
[DbIndex("Email", Unique = true)]
public string? Email { get; set; }

// Skan po indeksie
var hint = new IndexHint("Email", start: default, end: default, Asc: true, Skip: 0, Take: 10);
await foreach (var it in users.ScanByIndexAsync(hint))
    Console.WriteLine(it.Id);
```

## Założenia projektowe

- Prosty, przewidywalny model LSM: **MemTable** (RAM) + segmenty **SST** (dysk).
- Trwałość poprzez **WAL** oraz **checkpoint** do SST.
- **Transakcje** z buforem operacji (`AddPut/Delete/AddApply`) i atomowym `CommitAsync`.
- **Indeksy** jako zwykłe tabele (prefiks `__index__<table>__<name>`), z kluczem kompozytowym `(valuePrefix|pk)`.
- **Unikalność** wymuszana **w czasie zapisu** (rezerwacje + „sweep” duplikatów), **nie** w odczycie.
- **Tombstony**: usunięcia są logiczne, SST nigdy nie nadpisuje MEM.
- Opcjonalne **szyfrowanie** payloadów (WAL/SST). Klucze indeksów pozostają jawne (do sortowania).
