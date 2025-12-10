NormalizatorTests
=================

Opis
----
- Narzędzie konsolowe do weryfikacji odpowiedzi API normalizacji adresów oraz (nowo) do pobierania wyników z bazy PostgreSQL.
- Działa na pliku XLSX z danymi wejściowymi; wynik API zapisuje do `ResultFilePath`, wynik DB do `DbResultFilePath`.
- Kolorowanie (API): `RESULT` przy EXPECTED – zielony gdy zgodne, czerwony gdy różne; kolumna `IsCorrect` z 1/0.
- Wyniki DB: w drugim pliku tworzone są kolumny `DB_RESULT_1_*`, `DB_RESULT_2_*` itd. (kolejne rekordy z bazy w kolejnych blokach kolumn).

Wymagania
---------
- .NET 8 SDK.
- Dostęp do pliku wejściowego XLSX z kolumnami `REQUEST_*` i `EXPECTED_*`.
- Dostęp do endpointu API i do bazy PostgreSQL.

Konfiguracja (`appsettings.json`)
---------------------------------
- `OriginalFilePath` – pełna ścieżka do pliku XLSX z danymi testowymi.
- `ResultFilePath` – pełna ścieżka pliku wynikowego dla API.
- `ApiUrl` – endpoint np. `https://localhost:7266/addresses/normalize`.
- `ProbabilityThreshold` – minimalne `CombinedProbability`, od którego wpisujemy dane z API (domyślnie 0.8).
- `MaxParallelRequests` – maksymalna liczba równoległych zapytań.
- `DbResultFilePath` – pełna ścieżka pliku wynikowego z bazy (drugi wynik).
- `DbConnectionString` – connection string do Postgresa.
- `DbQueryFilePath` – ścieżka do pliku z zapytaniem SQL (ma priorytet nad `DbQuery`).
- `DbQuery` – opcjonalna treść zapytania SQL (gdy nie używasz pliku). Parametry: `@StreetName`, `@StreetPrefix`, `@BuildingNumber`, `@City`, `@PostalCode`. Aliasuj kolumny do: `StreetPrefix`, `StreetName`, `BuildingNumber`, `City`, `PostalCode`, `Commune`, `District`, `Province`.

Przykładowa sekcja zapytania
----------------------------
```
SELECT
    street_prefix  AS "StreetPrefix",
    street_name    AS "StreetName",
    building_no    AS "BuildingNumber",
    city           AS "City",
    postal_code    AS "PostalCode",
    commune        AS "Commune",
    district       AS "District",
    province       AS "Province"
FROM addresses
WHERE
    street_name ILIKE @StreetName
    AND COALESCE(street_prefix, '') ILIKE @StreetPrefix
    AND COALESCE(building_no, '') = @BuildingNumber
    AND COALESCE(city, '') ILIKE @City
    AND COALESCE(postal_code, '') = @PostalCode;
```

Struktura pliku XLSX
--------------------
- Wiersz 1: nagłówki. Dla każdej kolumny `EXPECTED_*` narzędzie doda obok kolumnę `RESULT_*` (plik API).
- Wiersze 2+: dane. Pola `REQUEST_*` wysyłane są do API i wykorzystywane jako parametry zapytania SQL. Literalne słowo `null` traktowane jest jako pusty ciąg.
- Plik API: dodawana kolumna `IsCorrect` (1/0) i filtrowanie.
- Plik DB: dodawane bloki kolumn `DB_RESULT_N_*` dla kolejnych rekordów zwróconych przez bazę.

Uruchomienie
------------
- Ustaw wartości w `appsettings.json` (co najmniej sekcję API; sekcję DB tylko jeśli chcesz generować drugi plik).
- W katalogu `NormalizatorTests` uruchom:
  - `dotnet run`
- Po zakończeniu powstaną: plik wynikowy API (`ResultFilePath`) i – jeśli skonfigurowano DB – plik wynikowy DB (`DbResultFilePath`).

Jak to działa
-------------
- `Program.cs` wczytuje konfigurację i uruchamia dwa przebiegi: API (`RunApiTest`) oraz opcjonalnie DB (`RunDbTest`).
- `RunApiTest`:
  - dodaje kolumny RESULT/IsCorrect,
  - wysyła zapytania HTTP w partiach (semafor `MaxParallelRequests`),
  - wpisuje wyniki powyżej progu `ProbabilityThreshold`,
  - koloruje komórki i uzupełnia `IsCorrect`.
- `RunDbTest`:
  - wykorzystuje te same dane wejściowe `REQUEST_*`,
  - odpytuje Postgresa parametryzowanym SQL (`DbQuery`),
  - zbiera wszystkie rekordy i wpisuje je w kolejnych blokach kolumn `DB_RESULT_N_*`.

Diagnostyka i bezpieczeństwo
----------------------------
- Błędy pojedynczych wierszy (API/DB) nie zatrzymują procesu – wiersz dostaje pustą odpowiedź.
- Zapytania do DB są parametryzowane (Npgsql) – unikamy SQL injection.
- Upewnij się, że baza i API akceptują podaną liczbę równoległych zapytań.

