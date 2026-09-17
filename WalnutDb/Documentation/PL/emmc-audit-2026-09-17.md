# Audyt zapisów na eMMC i plan bezpiecznej optymalizacji

Data: 2026-09-17. Badany kod: commit `eb29c9e`, paczka w projekcie `1.0.19`.
Zakres tej rundy: analiza, pomiar stanu obecnego i plan; bez zmian implementacji silnika i bez publikacji paczki.

## Wniosek

Problem został potwierdzony pomiarem. Checkpoint tabeli o około 100 MiB przepisuje cały SST zarówno po zmianie 10 KiB, jak i bez jakichkolwiek zmian. Otwarcie tabeli i domyślne zamknięcie bazy również powoduje pełny rewrite. Strumieniowe scalanie w 1.0.19 ogranicza pamięć szczytową, ale nie redukuje ilości danych zapisywanych do SST.

Potrzebne są dwa osobne kroki: szybkie ograniczenie zbędnych operacji w formacie v1 oraz późniejsze wprowadzenie checkpointów przyrostowych z kontrolowanym wdrożeniem czytnika wielosegmentowego. Użytkownik dopuścił nowy format pod warunkiem bezpiecznej migracji. Docelowo rekomendowany jest mieszany odczyt istniejących SSTv1 i nowych segmentów SSTv2, bez obowiązkowej konwersji starej bazy podczas otwierania.

Bezwzględne warunki wskazane przez użytkownika: zachować WAL i dotychczasowe założenia transakcyjne; awaria zasilania i niepełny zapis nie mogą zniszczyć ostatniego kompletnego stanu ani zgubić transakcji potwierdzonych jako trwałe. Compaction może raportować sukces wyłącznie po zakończeniu całego wymaganego procesu, włącznie z utrwaleniem końcowego stanu i wymaganym sprzątaniem. Publikacja nowego manifestu sama w sobie nie wystarcza do oznaczenia operacji jako zakończonej.

## Pomiar bazowy

Jedna lokalna próba na Windows 10.0.26200, .NET 8.0.31. Jedna tabela, 10 240 rekordów po 10 240 bajtów wartości, klucze 8-bajtowe, bez szyfrowania i indeksów wtórnych. Każda grupa zmian zatwierdzana jedną transakcją `Safe`. Początkowy SST: **105 021 452 B = 100,156 MiB**. Pomiary poniżej nie zawierają pierwszego wypełnienia bazy.

| Zmienione wartości | WAL zapisany przed checkpointem | SST zapisany w checkpoincie | Amplifikacja zapisów plikowych* | Czas checkpointu |
|---|---:|---:|---:|---:|
| 10 KiB | 10 326 B | 105 021 452 B | 10 257,05× | 4,065 s |
| 100 KiB | 102 846 B | 105 021 452 B | 1 026,61× | 3,939 s |
| 1000 KiB (około 1 MiB) | 1 028 046 B | 105 021 452 B | 103,56× | 5,328 s |
| 0 B — pusty checkpoint | 0 B | 105 021 452 B | nieokreślona: dzielenie przez zero | 3,703 s |

\* `(WAL + SST + SXI + manifest + CURRENT) / bajty zmienionych wartości`. Każdy checkpoint zapisał dodatkowo 204 B SXI oraz 234 B manifestu i CURRENT. To amplifikacja na poziomie plików aplikacji, **nie pomiar fizycznych zapisów NAND**.

Dla każdego checkpointu:

- Scalanie przebiega przez 105 021 440 B rekordów poprzedniego SST. Jest to wielkość wynikająca z pełnego przejścia po rekordach, a nie sprzętowy licznik odczytów.
- Windows `GetProcessIoCounters` odnotował 105 021 890 B zapisów i 188 907 732 B odczytów procesu w czasie checkpointu. Odczyty obejmują również walidację po podmianie pliku. Te liczniki także nie opisują ruchu wewnątrz NAND.
- Liczba operacji zapisu procesu: **20 484**. Kod SST korzysta z małych zapisów i `WriteThrough`; to dodatkowy obszar do optymalizacji kosztu I/O.
- Przyrost sumy alokacji .NET: około 106,8–107,0 MB. Próbkowane maksimum zajętej pamięci zarządzanej wyniosło około 9–11 MB, a working set procesu około 62–93 MB. Pomiar jest procesowy, co 10 ms; nie jest dokładnym maksimum RAM na i.MX6ULL. Niska pamięć szczytowa nie oznacza małej sumy alokacji podczas pełnego skanu.

Osobna próba `OpenTableAsync` + `DisposeAsync` z domyślnym `CheckpointOnDispose=true`, bez zmian użytkownika: SST i manifest zostały ponownie zapisane, licznik zapisów procesu wzrósł o **105 022 142 B**, czas wyniósł 3,871 s.

Kod benchmarku: `artifacts/emmc-audit/Program.cs`; wyniki: `artifacts/emmc-audit/baseline.json`. Polecenie odtworzenia:

```powershell
dotnet run --project artifacts/emmc-audit/Audit.csproj --configuration Release -p:GeneratePackageOnBuild=false -- artifacts/emmc-audit/baseline.json
```

Benchmark tworzy wyłącznie własną bazę testową w katalogu tymczasowym wskazanym w JSON. Pliki benchmarku są lokalnymi artefaktami ignorowanymi przez Git. Jeden przebieg wystarcza do potwierdzenia pełnego rewrite, ale nie do porównania wydajności urządzeń ani prognozy zużycia eMMC.

## Potwierdzone źródła zapisów

1. **Pełny rewrite w checkpoincie.** `Core/WalnutDatabase.cs:1050`, `StreamCheckpointRows` od linii 1116 i `Sst/SstWriter.cs:9`: stary SST jest scalany ze zmianami i w całości zapisywany ponownie. W aktualnym kodzie MemTable jest czyszczony dopiero po zapisach i publikacji metadanych; kolejność z początkowego opisu audytu nie odpowiada już tej wersji.
2. **Brak pomijania niezmienionych tabel.** Checkpoint iteruje po `_tables.ToArray()`, także po pustych MemTable przypiętych podczas otwierania tabel. Zmiana jednej tabeli może spowodować przepisanie innych otwartych tabel i ich indeksów.
3. **Checkpoint przy zamykaniu.** `Abstractions.cs:41` ustawia `CheckpointOnDispose=true`, a `Core/WalnutDatabase.cs:1472` wykonuje checkpoint przy dispose. Długotrwała jedna instancja ma inny profil zapisów niż częste otwieranie i zamykanie tej samej bazy.
4. **Rebuild i defragmentacja mają zakres całej bazy.** `RebuildTableAsync` (`Core/WalnutDatabase.cs:754`) i oba tryby `DefragmentAsync` (linia 1449) kierują do wspólnego checkpointu. Wynika to z globalnego WAL; ograniczenie zakresu wymaga zachowania informacji, które operacje WAL zostały już utrwalone.
5. **Manifest i CURRENT są zapisywane przy każdym otwarciu.** `Core/WalnutDatabase.cs:181` bezwarunkowo wywołuje `PersistCatalogAsync`. Ładowanie zwiększa również `_nextSeqNo`, a nowa wartość trafia do manifestu. To mało bajtów, ale dodatkowe utrwalenia i zmiany metadanych również dla odczytów.
6. **Powtarzane bariery WAL.** Na zwykłej ścieżce checkpoint wywołuje `Wal.FlushAsync`, potem `TruncateAsync`, które ponownie wywołuje `FlushAsync`, `Flush(true)` przed `SetLength(0)` i `Flush(true)` po skróceniu. To cztery jawne utrwalenia WAL, niezależnie od utrwaleń SST, SXI i manifestu. Są to ustalenia z kodu, a nie pomiar liczby komend wysłanych do eMMC. Redukcja wymaga udowodnienia zachowania granicy trwałości, a nie prostego usunięcia fsync.
7. **Dużo drobnych zapisów `WriteThrough`.** SST i SXI zapisują osobno nagłówki, klucze i wartości (`Sst/SstWriter.cs`, `Sst/SstIndex.cs`). Można rozważyć buforowanie kompletnego nowego pliku i końcowe trwałe utrwalenie przed publikacją, po testach zaniku zasilania na docelowym systemie.
8. **Okno group commit nie zbiera opóźnionych zgłoszeń.** Pętla `Wal/WalWriter.cs:102` bierze tylko elementy już dostępne w kolejce; kończy grupowanie natychmiast, gdy `TryRead` zwróci false. Parametr `groupWindow` nie oznacza oczekiwania przez to okno. Warto poprawić grupowanie z określoną maksymalną latencją, zachowując zakończenie `Safe/Group` dopiero po trwałym zapisie.

## Częstotliwość na urządzeniach

Przeszukanie silnika, narzędzi, benchmarków i dokumentacji wykazało powyższe wywołania oraz przykłady/testy. W tym repozytorium nie znalazłem produkcyjnego timera checkpointów. Nie ma tu konfiguracji i logów aplikacji działającej na urządzeniach, więc nie można rzetelnie podać jej rzeczywistego GB/dzień ani czasu do zużycia pamięci.

Przykład warunkowy: jedna tabela 100 MiB przepisywana co 5 minut to około **28,1 GiB/dzień i 10 TiB/rok** samego SST; dochodzą WAL, indeksy, filesystem i wewnętrzna amplifikacja kontrolera. To scenariusz, a nie ustalony profil floty.

Do zebrania na reprezentatywnych urządzeniach: przyczyna i czas każdego checkpointu, ilość brudnych danych, rozmiary SST, liczba otwarć/zamknięć bazy, checkpointy/dobę, licznik bajtów i operacji urządzenia blokowego, dostępne wskaźniki lifetime/PRE_EOL eMMC. Raportować zbiorczo i rzadko, aby sama diagnostyka nie generowała dużej ilości zapisów.

## Plan wdrożenia

### 1. Ograniczenie zbędnych zapisów w v1

- Dodać spójny pod blokadą maintenance licznik brudnych rekordów/bajtów i tombstonów dla każdej tabeli oraz indeksu.
- Pomijać czyste tabele i puste checkpointy. Uwzględnić odzyskany WAL, naprawy indeksów, zmiany metadanych i `DropTable`: brak brudnych rekordów nie zawsze oznacza brak pracy do utrwalenia.
- Nie przepisywać niezmienionego manifestu ani CURRENT. Zachować zapis potrzebny do migracji legacy i rzeczywistej naprawy.
- Ograniczyć puste transakcje i identyczne upserty tylko po ustaleniu semantyki serializerów, indeksów oraz wyniku operacji.
- Uporządkować bariery WAL i grupowanie; zachować gwarancję `Safe/Group`. Nie zmieniać globalnie durability na `Fast`.
- Rozważyć zmianę polityki zamykania w aplikacji: trwały flush WAL zamiast pełnego checkpointu przy każdym krótkim użyciu. Nie zmieniać po cichu publicznego domyślnego zachowania. Pilnować przy tym rozmiaru WAL, pamięci i czasu recovery.
- Dodać regresje: pusty checkpoint i read-only reopen nie zapisują SST, zmiana A nie przepisuje czystego B, błąd nie czyści dirty-state, awaria checkpointu zachowuje recovery.

Warunek ukończenia: pomiary liczby zapisów potwierdzają brak rewrite dla tych scenariuszy, stare fixture WALv1/SSTv1 nadal działają. Zmiana 10 KiB w brudnej tabeli wciąż będzie przepisywać całą tę tabelę; etap 1 usuwa operacje zbędne, ale nie rozwiązuje głównego ograniczenia modelu jednego SST.

### 2. Kontrakt wielosegmentowy i rollout czytnika

- Zachować istniejący SST jako niezmienioną bazę; nie kopiować go przy pierwszym otwarciu.
- Manifest opisuje uporządkowaną listę segmentów, generacje, rolę segmentu, wersję odczytu i granicę odtworzenia WAL. Identyfikator tabeli używany w szyfrowaniu pozostaje stabilny; nazwa pliku/generacja nie może zmieniać AAD.
- Zaprojektować minimalny SSTv2 dla nowych segmentów: wersjonowany nagłówek, jawny typ rekordu PUT/DELETE, jednoznaczne długości, sumy kontrolne bloków/rekordów oraz walidowany indeks przypisany do konkretnego segmentu. Obecny SSTv1 zawiera tylko key/value i nie ma flagi DELETE; pusta wartość jest legalna dla indeksów. Nie używać pustej wartości jako tombstone. Rozmiar bloków dobrać pomiarem; w pierwszej wersji nie dokładać równocześnie kompresji ani zmiany WAL, jeżeli nie są konieczne.
- Manifest v2 przypisuje każdemu segmentowi format `SSTv1` lub `SSTv2`, rolę, identyfikator/generację i porządek. Czytnik v1 pozostaje obsługiwany także po migracji metadanych; nowa biblioteka musi działać na bazie mieszanej.
- Czytać w kolejności MemTable → najnowsza generacja → starsze → baza. GET musi rozróżniać found/deleted/absent; skany muszą scalać generacje i tłumić stare wersje. Testować również indeksy, unique, range/tail, usunięcie i ponowne utworzenie tabeli.
- Oznaczyć nowy manifest/układ jako wymagający nowego czytnika; nie doklejać pól, które stary reader zignoruje i pokaże niepełne dane.
- Wydać najpierw czytnik przy wyłączonym zapisie nowego układu. Włączyć zapis delt dopiero, gdy wszystkie wersje software dopuszczone do rollbacku obsługują ten układ. Do tego czasu pozostaje tryb v1.
- Kontrolę obsługiwanej wersji przenieść przed modyfikujące recovery. Obecnie replay WAL jest uruchamiany przed odczytem wersji manifestu (`Core/WalnutDatabase.cs:86` i `:99`); samo odrzucenie nowej wersji na końcu nie gwarantuje otwarcia bez skutków ubocznych przez stary software.

Warunek ukończenia: stara baza otwiera się bez zmiany bajtów SST; reader poprawnie odczytuje mieszany zestaw baza+delta; rollback jest przetestowany dla konkretnych wersji aplikacji.

Bezpieczna migracja ma być jawną, kontrolowaną operacją z trybem sprawdzenia przed wykonaniem. Raport wstępny powinien wskazać wersje plików, stan integralności, potrzebne miejsce, dozwolone wersje aplikacji po migracji i przewidywany koszt zapisów. Samo otwarcie przez nową bibliotekę nie musi od razu przełączać bazy na zapis v2.

Podstawowy tryb migracji: opublikować mały manifest v2 wskazujący niezmienione SSTv1 jako bazę, a dopiero kolejne checkpointy zapisywać jako v2. Baza ma działać w tym stanie również bez późniejszej pełnej konwersji. W razie przerwania przed publikacją nadal obowiązuje poprzedni stan; po publikacji wszystkie jego pliki muszą już być trwałe, a WAL umożliwiać dokończenie recovery.

Opcjonalny tryb konwersji starych plików: przetwarzać strumieniowo wybrany segment/tabelę, ze skonfigurowanym budżetem I/O i miejsca tymczasowego; publikować wynik przed usunięciem wejścia. Przerwana operacja musi być bezpiecznie ponawialna. Jeśli jeden stary SST jest większy niż dostępne miejsce na bezpieczną konwersję, pozostawić go jako bazę v1. Samo podzielenie wyniku na mniejsze pliki nie zwalnia starego źródła i nie rozwiązuje tego ograniczenia.

Po zapisaniu nowych danych w v2 powrót do programu obsługującego wyłącznie v1 nie jest bezpieczny: stary bazowy SST nie zawiera nowych zmian. Dostępne ścieżki to rollout czytnika v2 do wszystkich wersji dopuszczonych do rollbacku albo osobny, jawny eksport do v1, który wymaga dodatkowych zapisów i miejsca. Nie obiecywać bezkosztowego downgrade'u. Uszkodzone stare rekordy wymagają osobnego raportu naprawy/odzysku; migracja nie może po cichu ich pomijać.

### 3. Checkpoint delta i jednoznaczna publikacja

- Na początek zachować obecną blokadę maintenance: zaczekać na rozpoczęte commity i nie wpuszczać nowych do zakończenia publikacji. Zapisywać tylko zmiany z MemTable do małych segmentów.
- Dla globalnego WAL publikować jeden spójny zestaw segmentów wszystkich dirty tables i indeksów, a nie niezależne checkpointy usuwające wspólny WAL.
- Sekwencja: pliki tymczasowe → trwałe utrwalenie → nazwy docelowe → nowa generacja manifestu → trwałe opublikowanie CURRENT → dopiero wtedy sprzątanie objętego checkpointem WAL.
- Na Linux uwzględnić utrwalenie katalogów po rename i przed utratą starej ścieżki recovery. Samo `Flush(true)` pliku i atomowy rename nie zapewniają trwałości wpisu katalogowego po power loss. Zweryfikować również semantykę platformy Windows.
- Granicę recovery opisać np. parą generacja WAL + offset końca COMMIT. Obecne `LastSequence` nie jest gotową granicą checkpointu: numer jest przydzielany przy BEGIN, a transakcje mogą commitować w innej kolejności.
- Czyścić MemTable/dirty-state dopiero po publikacji. Przy awarii zostawić stary widok i WAL; pliki osierocone nie mogą być samoczynnie adoptowane jako committed dane.
- Zdefiniować odzyskanie po uszkodzeniu manifestu. W układzie multi-SST nie wolno zgadywać kolejności segmentów na podstawie plików. Powrót do poprzedniego manifestu jest poprawny tylko, gdy nadal istnieją wszystkie jego segmenty i potrzebny WAL.

Warunek ukończenia: przy 100 MiB bazy i zmianie 100 KiB checkpoint zapisuje dane proporcjonalne do delty, indeksów i metadanych; stary SST pozostaje identyczny bajtowo.

### 4. Testy awarii przed uruchomieniem compaction

- Testy przerwania na zapisie/utrwaleniu/rename segmentów, zapisie/utrwaleniu/publikacji manifestu i sprzątaniu WAL. Użyć zarówno kontrolowanych błędów I/O, jak i przerwania oddzielnego procesu.
- ENOSPC w segmencie, indeksie, manifeście i później w compaction. Stary stan pozostaje dostępny; błędy i anulowanie nie potwierdzają nieukończonego checkpointu.
- Po restarcie sprawdzać pełne committed transakcje obejmujące wiele tabel, brak cofniętych delete, brak częściowych indeksów, unikalność, szyfrowanie, time-series i ponowne wykorzystanie nazw tabel.
- Testy generacji: powtórny replay, opóźniony commit starszego BEGIN, awaria po publikacji i przed skróceniem WAL, nieznana wersja przed jakąkolwiek zmianą plików.
- Testy backupu i długich skanów: backup obejmuje segmenty przypiętego manifestu, a czytelnicy utrzymują stare segmenty przy życiu do końca odczytu.
- Test zaniku zasilania na docelowym Linux/filesystem/eMMC. Zabicie procesu nie symuluje utraty danych z cache kontrolera. Dotychczasowe testy nie dowodzą bezpieczeństwa we wszystkich tych granicach.

### 5. Kontrolowana compaction

- Oddzielić compaction od checkpointu; scalać wybrany zestaw segmentów, z ograniczeniem RAM i budżetem I/O.
- Nie przepisywać dużej bazy po każdych kilku małych deltach. Dobierać segmenty według rozmiaru i zakresu kluczy; sam limit liczby plików może odtworzyć obecny problem.
- Tombstone można usunąć dopiero, gdy compaction obejmuje wszystkie starsze wersje tego klucza, które mógłby zasłonić.
- Nowy zestaw opublikować przed kasowaniem wejść; stare pliki usuwać dopiero po zwolnieniu czytelników i backupów. Planować osobno odzyskanie po awarii podczas usuwania.
- Respektować budżet wolnego miejsca, liczbę segmentów i limity odczytów. Multi-SST redukuje miejsce potrzebne na zwykły checkpoint, ale compaction nadal potrzebuje przestrzeni na wynik. Przy zbyt małej przestrzeni potrzebna jest kontrolowana odmowa/backpressure, nie nadpisywanie jedynej kopii.

Kontrakt zakończenia compaction:

1. Przypiąć spójny zestaw wejściowy. Przygotować komplet wynikowych segmentów i indeksów, sprawdzić ich strukturę, sumy kontrolne, kolejność kluczy i granice rekordów.
2. Trwale utrwalić pliki, nadać nazwy docelowe i utrwalić zmiany katalogów wymagane przez platformę.
3. Trwale opublikować nowy manifest zawierający identyfikator compaction, aktywne wyniki oraz listę starych segmentów przeznaczonych do posprzątania. Odtąd baza może bezpiecznie korzystać z nowego zestawu, ale operacja jest jeszcze niedokończona.
4. Zaczekać na zwolnienie przypiętych wejść przez czytelników/backupy, usunąć wszystkie pliki objęte wymaganym sprzątaniem, utrwalić usunięcia.
5. Trwale opublikować końcowy stan operacji, bez zaległego sprzątania. Dopiero wtedy zwrócić `Completed`/sukces. Błąd I/O, anulowanie lub timeout przed tym punktem nie mogą być przedstawione jako sukces.

Potrzebne są odrębne stany `Preparing`, `PublishedCleanupPending`, `Completed`, `Failed`/`Cancelled` z informacją, czy publikacja nastąpiła. To propozycja kontraktu, nie istniejące API. Gdy przerwanie następuje po publikacji, nowy stan danych pozostaje aktywny; nie wycofywać go pochopnie do nieaktualnych wejść. Po restarcie wznowić idempotentne sprzątanie według trwałego identyfikatora operacji. Usuwanie plików tylko na podstawie zgadywania nazw jest niedopuszczalne.

Testy muszą dodatkowo sprawdzać **wynik i trwały status operacji**, nie tylko czytelność danych: wymuszone błędy przy usuwaniu wejść lub finalnym utrwalaniu nie mogą dać `Completed`. Awaria po końcowym utrwaleniu, ale przed dostarczeniem odpowiedzi wywołującemu, powinna pozwalać odczytać rzeczywisty zakończony status po restarcie. Jawna kopia archiwalna zachowywana zgodnie z polityką backupu nie jest zaległym sprzątaniem; musi być odróżniona od wejść, które compaction zobowiązała się usunąć.

### 6. Walidacja na urządzeniu i stopniowe włączenie

- Powtórzyć benchmarki przed/po na i.MX6ULL z rzeczywistymi danymi: 10 KiB, 100 KiB, około 1 MiB, puste checkpointy, wiele tabel i indeksów, update/delete/reinsert.
- Mierzyć całe cykle checkpoint+compaction, WAL oraz metadane. Sam tani checkpoint może ukrywać koszt późniejszej compaction.
- Dodać progi akceptacji dla host bytes/dzień, p95 czasu commit/odczytu, pamięci, miejsca tymczasowego, rozmiaru WAL i czasu recovery. Oprzeć progi na wynikach i wymaganiach urządzenia.
- Włączyć nowy zapis flagą dopiero po zgodności rollbacku i testach awaryjnych, początkowo na małej grupie urządzeń. Opublikować kolejną wersję paczki po tych bramkach; ta runda nie tworzy nowego release.

## Rekomendowana kolejność

Najpierw etap 1 i telemetria częstotliwości na urządzeniach. Równolegle na poziomie projektu przygotować kontrakt manifestu, tombstonów, recovery i rollbacku; następnie czytnik, checkpoint delta, testy awarii i dopiero compaction. Ochrona trwałości `Safe/Group` pozostaje warunkiem wszystkich etapów. Brak produkcyjnej częstotliwości checkpointów nie blokuje usunięcia potwierdzonych zbędnych zapisów, ale blokuje uczciwą prognozę żywotności eMMC.
