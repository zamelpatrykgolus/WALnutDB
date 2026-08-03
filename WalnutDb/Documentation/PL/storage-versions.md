# Wersje formatu i migracje

## Aktualny format

WalnutDb zachowuje binarne formaty `WALv1` i `SSTv1`. Poprawki bezpieczeństwa nie wymagają przepisywania istniejących tabel.

Przy pierwszym otwarciu bazy bez pliku `CURRENT` tworzony jest mały manifest `MANIFEST-000001.json`. Zawiera wersję formatu oraz mapowanie logicznych nazw tabel na istniejące pliki SST. Jest to migracja wyłącznie metadanych: pliki WAL i SST nie są kopiowane, przemianowywane ani ponownie szyfrowane.

Usunięcie manifestu nie usuwa danych. Dla storage v1 WalnutDb może odbudować mapowanie z istniejących plików. Dla historycznie niejednoznacznych nazw można podać `DatabaseOptions.LegacyTableNameMappings`; zapisany zostanie tylko nowy manifest, bez zmiany SST.

## Naprawa przy otwieraniu

Recovery automatycznie:

- przycina wyłącznie niepełny lub uszkodzony ogon WAL do ostatniego kompletnego COMMIT;
- zatrzymuje otwieranie, jeśli korupcja występuje wewnątrz WAL albo struktura SST jest niespójna;
- odrzuca transakcje z niezgodną liczbą operacji;
- odbudowuje uszkodzony manifest v1 bez przepisywania danych;
- ignoruje osierocone SST, jeżeli istnieje poprawny manifest, dzięki czemu nieudane fizyczne usunięcie nie może wskrzesić tabeli;
- kończy przerwaną promocję indeksu pobocznego `.sxi`, jeśli odpowiadający SST został już podmieniony;
- odbudowuje brakujące indeksy wtórne po otwarciu tabeli.

Błąd odszyfrowania, uszkodzenie zatwierdzonej części WAL albo wersja formatu nowsza niż obsługiwana zatrzymują otwieranie bazy. Baza nie jest wtedy udostępniana jako częściowo pusta.

## Koszt miejsca

Checkpoint strumieniowo scala MemTable z istniejącym SST, tworzy `*.sst.tmp` dla jednej tabeli, a następnie atomowo zastępuje jej SST. Nie materializuje całej tabeli w RAM. Przed rozpoczęciem sprawdza przybliżone wymagane wolne miejsce; przy jego braku usuwa plik tymczasowy i zachowuje WAL. Szczytowe dodatkowe użycie miejsca odpowiada największej aktualnie przebudowywanej tabeli, a nie całej bazie. Migracja manifestu wymaga jedynie kilku kilobajtów.

Backup ma znacznik `.walnutdb-backup-incomplete`. Niekompletna kopia nie zostanie otwarta jako poprawna baza; ponowienie backupu może ją dokończyć i usunąć stare artefakty poprzedniej kopii.

W aktualnym bezpiecznym wariancie commity czekają podczas checkpointu. Pozwala to zachować WALv1 bez generacyjnej migracji. Przyszły nieblokujący checkpoint powinien używać generacji WAL i nadal zachowywać czytnik WALv1.

## Zasady dla przyszłego v2

Nowa wersja binarna musi mieć osobny reader i osobne testy golden-file. Migracja powinna działać tabela po tabeli, przez plik tymczasowy i atomową zmianę manifestu. Stary plik wolno usunąć dopiero po utrwaleniu nowego pliku oraz manifestu; migracja nigdy nie może wymagać jednoczesnej kopii całej bazy.
