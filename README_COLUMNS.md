Struktura kolumn (API i DB)
===========================

Plik wejściowy (wspólny)
------------------------
- Wiersz 1: nagłówki.
- `REQUEST_*` – dane wejściowe wysyłane do API i używane jako parametry zapytania SQL:
  - `REQUEST_streetN`
  - `REQUEST_streetP`
  - `REQUEST_building`
  - `REQUEST_city`
  - `REQUEST_postal`
- `EXPECTED_*` – wartości referencyjne do porównania z wynikiem API:
  - `EXPECTED_streetN`
  - `EXPECTED_streetP`
  - `EXPECTED_building`
  - `EXPECTED_city`
  - `EXPECTED_postal`
  - `EXPECTED_commune`
  - `EXPECTED_district`
  - `EXPECTED_province`
- Wiersze 2+: rekordy testowe; literalny tekst `null` traktowany jest jako pusty ciąg.

Plik wynikowy API (`ResultFilePath`)
------------------------------------
- Dla każdej kolumny `EXPECTED_*` tworzona jest sąsiednia kolumna `RESULT_*`, np.:
  - `RESULT_streetN`
  - `RESULT_streetP`
  - `RESULT_building`
  - `RESULT_city`
  - `RESULT_postal`
  - `RESULT_commune`
  - `RESULT_district`
  - `RESULT_province`
- Kolory w komórkach `RESULT_*`: zielony = zgodność z `EXPECTED_*` (case-insensitive), czerwony = różnica.
- `IsCorrect` – kolumna dodawana na końcu arkusza; wartość 1 gdy wszystkie sprawdzone pary wiersza są zgodne, inaczej 0. Ma włączony filtr.

Plik wynikowy DB (`DbResultFilePath`)
-------------------------------------
- Wyniki z bazy są zapisywane w blokach kolumn dla każdego zwróconego rekordu:
  - `DB_RESULT_1_streetP`, `DB_RESULT_1_streetN`, `DB_RESULT_1_building`, `DB_RESULT_1_city`, `DB_RESULT_1_postal`, `DB_RESULT_1_commune`, `DB_RESULT_1_district`, `DB_RESULT_1_province`
  - `DB_RESULT_2_*` – kolejny rekord, itd.
- Liczba bloków zależy od maksymalnej liczby wyników zwróconych dla któregokolwiek wiersza.
- Brak wyników dla wiersza oznacza puste komórki w jego blokach.

Mapowanie aliasów z zapytania SQL
---------------------------------
- Zapytanie (z pliku lub `DbQuery`) powinno zwracać kolumny o aliasach:
  - `StreetPrefix`, `StreetName`, `BuildingNumber`, `City`, `PostalCode`, `Commune`, `District`, `Province`
- Te aliasy są mapowane w kodzie na powyższe kolumny `DB_RESULT_N_*`.


