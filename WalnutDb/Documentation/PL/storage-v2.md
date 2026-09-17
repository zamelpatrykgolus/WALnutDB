# Storage v2 — przyrostowe checkpointy i bezpieczna migracja

Biblioteka 2.0.0 domyślnie zapisuje v1. Otwarcie starej bazy nie migruje jej automatycznie. Czytnik obsługuje SSTv1, SSTv2 oraz ich mieszankę.

## Optymalizacje wspólne

Checkpoint pomija czyste tabele i indeksy. Pusty checkpoint nie zapisuje SST ani manifestu. Otwarcie poprawnej bazy nie przepisuje metadanych. Puste transakcje nie generują ramek WAL.

SST zapisuje się buforowo i utrwala przez `Flush(true)` przed publikacją. Safe/Group nadal kończą commit dopiero po trwałym WAL. Fast nie jest włączany automatycznie. Commit jest serializowany na poziomie bazy, aby kolejność stosowania zmian w pamięci odpowiadała kolejności WAL. Okno grupowania `WalWriter` zbiera bezpośrednio kolejkowane transakcje Group; nie dodaje oczekiwania do Safe.

Aktywne skany przypinają konkretny plik. Podmiana v1 nie może połączyć starego indeksu z nowym plikiem; compaction v2 czeka na zwolnienie wejściowych segmentów.

`DatabaseOptions.UniqueReservationTimeout` (domyślnie 2 s) ogranicza oczekiwanie
na zwolnienie wartości unique przez współbieżny commit. Test obu formatów obejmuje
celowo opóźniony trwały commit, dłuższy niż poprzedni sztywny limit 300 ms.

## Jawna migracja

```csharp
var db = new WalnutDatabase(directory,
    new DatabaseOptions
    {
        AllowStorageV2Upgrade = true, // po sprawdzeniu wersji software do rollbacku
        CheckpointOnDispose = false,
        MaxSegmentsPerTable = 64,
        MaintenanceReserveBytes = 1024 * 1024
    },
    new FileSystemManifestStore(directory),
    new WalWriter(Path.Combine(directory, "wal.log")));

var plan = await db.PlanStorageUpgradeAsync(); // DataBytesToRewrite == 0
await db.UpgradeStorageAsync();
```

Migracja publikuje tylko nowy manifest wskazujący niezmienione SSTv1. WAL i MemTable zachowują niecheckpointowane zmiany. Kolejne checkpointy tworzą małe pliki `seg-<guid>.sst`. Duża baza może pozostać w v1 bezterminowo.

Opcja `AllowStorageV2Upgrade` kontroluje wyłącznie przejście v1→v2. Już zmigrowaną bazę można otworzyć z wartością false. V2 wymaga wbudowanego `WalWriter`; własne implementacje `IWalWriter` pozostają obsługiwane w v1.

Nie włączać migracji, jeżeli rollback może przywrócić bibliotekę 1.0.x. Nie odczyta ona delt, a niektóre stare wydania wykonują recovery WAL przed sprawdzeniem manifestu. Stary SSTv1 nie zawiera późniejszych zmian. Najpierw wdrożyć czytnik 2.0.0 do wszystkich wersji aplikacji dopuszczonych do rollbacku. Automatyczny downgrade nie jest dostępny.

## Format i trwałość

SSTv2 zawiera nagłówek, uporządkowane rekordy PUT/DELETE z długościami i CRC32 oraz footer z liczbą rekordów i CRC. Pusta wartość nie oznacza DELETE. Indeks rzadki jest odtwarzany w pamięci po walidacji, bez niezależnego SXI dla v2.

Manifest v2 zawiera listy segmentów, kolejność, format, rozmiar, rolę bazy/delty i SHA-256 payloadu. Wersja jest jawna również w zewnętrznej otoczce JSON. Wadliwy albo nieznany manifest v2 blokuje otwarcie; nie zgadujemy kolejności segmentów na podstawie nazw.

Ramki WAL pozostają zgodne z dotychczasowym kodowaniem i CRC. Po pierwszym checkpoincie v2 plik otrzymuje 28-bajtowy nagłówek generacji (`WALv2`, GUID, CRC). Manifest zapamiętuje generację oraz offset objętego checkpointem COMMIT. Numer przydzielony przy BEGIN nie jest granicą recovery.

Checkpoint pod blokadą maintenance:

1. Czeka na rozpoczęte commity, utrwala kolejkę WAL i zapamiętuje jej granicę.
2. Zapisuje i waliduje tylko brudne rekordy, utrwala segmenty oraz ich nazwy.
3. Trwale publikuje manifest/CURRENT obejmujący cały spójny zestaw tabel i indeksów.
4. Dopiero wtedy czyści objęte MemTable i atomowo zastępuje WAL nową generacją.

Awaria przed publikacją pozostawia stare segmenty i WAL. Awaria po publikacji, przed rotacją WAL, jest rozpoznawana na podstawie zapisanej granicy. Błąd publikacji v2 blokuje dalszą pracę na instancji: zamknąć ją i ponownie otworzyć, aby wykonać recovery.

Linux utrwala pliki i katalogi przez fsync. Windows używa atomowej podmiany i trwałego flush; rename nowych plików korzysta z WRITE_THROUGH. Testy programowe nie zastępują testu fizycznego zaniku zasilania na docelowym filesystemie, kontrolerze i eMMC.

## Compaction i jej zakończenie

```csharp
var result = await db.CompactAsync("devices", new CompactionOptions
{
    IncludeBaseSegments = false,
    MaxInputBytes = 16 * 1024 * 1024,
    MaxOutputBytes = 16 * 1024 * 1024,
    WriteBytesPerSecond = 1024 * 1024
}, cancellationToken);
// Poprawny powrót następuje wyłącznie po Completed.
```

Domyślnie łączone są delty, z zachowaniem bazy i potrzebnych tombstonów. `IncludeBaseSegments=true` obejmuje również bazę i umożliwia strumieniową konwersję v1→v2. Wymaga miejsca na wynik; jeśli go brakuje, pozostawić stary SST jako bazę. Nie nadpisujemy jedynej kopii danych.

Compaction nie opróżnia dirty MemTable i nie usuwa ich WAL. Można ją wykonać po osiągnięciu limitu segmentów, następnie ponowić checkpoint. Limit daje kontrolowany błąd zamiast nieograniczonego wzrostu kosztu odczytów. Aplikacja planuje maintenance; nie dodano automatycznego timera. `DefragMode.Compact` w v2 scala delty, a `RebuildSwap` obejmuje także bazę.

Trwałe stany:

- `Preparing`: stare wejścia są aktywne, wynik jeszcze nie jest opublikowany.
- `PublishedCleanupPending`: nowy widok jest aktywny; operacja nadal jest niedokończona.
- `Completed`: wynik zweryfikowano, utrwalono i opublikowano, czytelnicy zwolnili wejścia, wymagane pliki usunięto, a końcowy status utrwalono.
- `Failed`: recovery posprzątało operację przerwaną przed publikacją, zachowując wejścia.

Timeout, anulowanie lub błąd przed końcem rzuca wyjątek. `GetCompactionStatus()` pokazuje ostatni znany trwały etap. Po publikacji nie wycofujemy bazy do starych wejść. Restart ponawia sprzątanie idempotentnie. Wartość enum `Cancelled` jest zarezerwowana; obecnie recovery operacji przerwanej przed publikacją zapisuje Failed.

Długi skan może wstrzymać sprzątanie; należy zwalniać enumeratory także przy wcześniejszym zakończeniu odczytu. Backup przypina spójny zestaw przez blokadę maintenance i kopiuje aktywne segmenty wskazane przez manifest.

## Diagnostyka i testy

`GetWriteStatistics()` udostępnia liczniki checkpointów, pustych checkpointów, bajtów SST i compaction. Liczniki są w RAM. Nie opisują wewnętrznych zapisów NAND ani pełnego narzutu filesystemu. `GetStatsAsync()` uwzględnia liczbę i rozmiar aktywnych segmentów.

```powershell
dotnet test WalnutDb.sln -c Release
dotnet run --project WalnutDb.Bench -c Release -- --storage-audit artifacts/emmc-audit/after-v1-v2.json
```

Benchmark 100 MiB z 2026-09-17: zmiana 100 KiB daje 105 021 452 B SST w v1 i 102 631 B SST w v2. Pusty checkpoint daje 0 B SST w obu formatach. Scalenie trzech delt zapisało 1 026 121 B i nie zmieniło bazowego SST około 100 MiB. To pomiar zapisów hosta, nie komórek NAND.

Zawsze uruchamiać projekty V1, V2, Migration, Safety i testy ogólne. V2 zawiera także scenariusze parametryzowane oboma formatami oraz abrupt process termination dla v1 i v2. Testy obejmują migrację bez rewrite, WAL epochs, transakcje wielu tabel, delete, unique/nullable, szyfrowanie, time-series, backup, ENOSPC/budżety, korupcję, przyszłe wersje i status compaction po restarcie.

Przed aktywacją na flocie obowiązuje test odcięcia zasilania na reprezentatywnym i.MX6ULL/eMMC, pomiar rzeczywistej częstotliwości maintenance i sprawdzenie wersji aplikacji do rollbacku. Domyślny v1 umożliwia wdrożenie czytnika przed aktywacją nowego zapisu.
