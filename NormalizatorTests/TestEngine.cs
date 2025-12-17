using ClosedXML.Excel;
using System.Collections.Concurrent;
using System.Data;
using System.IO;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using Npgsql;

namespace NormalizatorTests
{
    internal class TestEngine
    {
        // Współdzielony HttpClient dla wszystkich zapytań API (thread-safe)
        private static readonly Lazy<HttpClient> _sharedHttpClient = new Lazy<HttpClient>(() =>
        {
            var handler = new HttpClientHandler
            {
                MaxConnectionsPerServer = 100, // Zwiększamy limit połączeń równoległych na serwer
                UseCookies = false, // Wyłączamy cookies jeśli nie są potrzebne dla lepszej wydajności
                AutomaticDecompression = System.Net.DecompressionMethods.All, // Automatyczna dekompresja gzip/deflate/brotli
                // Wyłączamy weryfikację certyfikatu SSL dla localhost - znacznie przyspiesza połączenia lokalne
                ServerCertificateCustomValidationCallback = (message, cert, chain, errors) =>
                {
                    // Dla localhost akceptujemy wszystkie certyfikaty (tylko dla development)
                    return message.RequestUri?.Host == "localhost" || message.RequestUri?.Host == "127.0.0.1";
                }
            };

            var client = new HttpClient(handler)
            {
                Timeout = TimeSpan.FromMinutes(5) // Timeout 5 minut dla długich zapytań
            };

            // Wymuszamy kompresję w nagłówkach requestów
            client.DefaultRequestHeaders.Add("Accept-Encoding", "gzip, deflate, br");
            
            return client;
        });

        private static HttpClient SharedHttpClient => _sharedHttpClient.Value;

        // Współdzielone ustawienia JSON dla lepszej wydajności serializacji/deserializacji
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true, // Case-insensitive dla większej elastyczności
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.Never,
            WriteIndented = false, // Bez formatowania - mniejsze JSON i szybsza serializacja
            AllowTrailingCommas = true,
            ReadCommentHandling = JsonCommentHandling.Skip
        };

        // Główny przebieg testów dla API: odczyt danych z Excela, wywołanie API,
        // zapis wyników i wizualne oznaczenie poprawności.
        public static async Task RunApiTest(string originalFilePath, string resultFilePath, string apiUrl, int maxParallelRequests)
        {
            Console.WriteLine($"{LogTs()} [API] Otwieranie pliku XLSX: {originalFilePath}");
            using var workbook = new XLWorkbook(originalFilePath);
            workbook.CalculateMode = XLCalculateMode.Manual; // Wyłączamy auto-kalkulację dla lepszej wydajności
            var sheet = workbook.Worksheet(1);
            Console.WriteLine($"{LogTs()} [API] Plik XLSX otwarty pomyślnie");

            var rowsNo = sheet.Rows().Count();
            Console.WriteLine($"{LogTs()} [API] Start: Rows={rowsNo - 1},  Parallel requests={maxParallelRequests}");

            // Tworzymy dodatkowe kolumny na wyniki, aby nie nadpisywać oczekiwanych wartości.
            Console.WriteLine($"{LogTs()} [API] Dodawanie kolumn wynikowych...");
            AddResultColumns(sheet);
            Console.WriteLine($"{LogTs()} [API] Kolumny wynikowe dodane");

            Console.WriteLine($"{LogTs()} [API] Analizowanie struktury kolumn...");
            var indexes = GetColumnIndexes(sheet);
            Console.WriteLine($"{LogTs()} [API] Struktura kolumn przeanalizowana");

            // Optymalizacja: wczytujemy wszystkie dane z Excela do pamięci na początku
            Console.WriteLine($"{LogTs()} [API] Wczytywanie danych z Excela do pamięci...");
            var rowDataCache = new Dictionary<int, RowData>();
            for (int i = 2; i <= rowsNo; i++)
            {
                rowDataCache[i] = new RowData
                {
                    StreetName = NormalizeValue(sheet.Cell(i, indexes.RequestStreetIndex).Value.ToString()),
                    Prefix = NormalizeValue(sheet.Cell(i, indexes.RequestPrefixIndex).Value.ToString()),
                    BuildingNo = NormalizeValue(sheet.Cell(i, indexes.RequestBuildingNoIndex).Value.ToString()),
                    City = NormalizeValue(sheet.Cell(i, indexes.RequestCityIndex).Value.ToString()),
                    PostalCode = NormalizeValue(sheet.Cell(i, indexes.RequestPostalCodeIndex).Value.ToString())
                };
            }
            Console.WriteLine($"{LogTs()} [API] Wczytano {rowDataCache.Count} wierszy danych do pamięci");

            // Kontrolujemy równoległość zapytań, aby nie przeciążyć usługi.
            var semaphore = new SemaphoreSlim(maxParallelRequests);
            var tasks = new List<Task>();
            var results = new ConcurrentDictionary<int, NormalizationApiResponseDto>();
            var totalRows = rowsNo - 1;
            var processed = 0;
            
            Console.WriteLine($"{LogTs()} [API] Tworzenie zadań dla {totalRows} wierszy...");
            for (int i = 2; i <= rowsNo; i++)
            {
                tasks.Add(ProcessRow(
                    rowDataCache[i],
                    i,
                    indexes,
                    apiUrl,
                    semaphore,
                    results,
                    () =>
                    {
                        var done = Interlocked.Increment(ref processed);
                        if (done % 50 == 0 || done == totalRows || done <= 5)
                        {
                            Console.WriteLine($"{LogTs()} [API] Postęp: {done}/{totalRows}");
                        }
                    }));
            }
            Console.WriteLine($"{LogTs()} [API] Utworzono {tasks.Count} zadań, rozpoczynam przetwarzanie...");

            await Task.WhenAll(tasks);
            Console.WriteLine($"{LogTs()} [API] Wszystkie zapytania API zakończone, przetworzono {results.Count} wyników");

            // Po zebraniu odpowiedzi wpisujemy wyniki i kolorujemy komórki
            // (zielony = zgodność, czerwony = różnica).
            Console.WriteLine($"{LogTs()} [API] Zapis wyników do arkusza i kolorowanie komórek...");
            var writtenCount = 0;
            for (int i = 2; i <= rowsNo; i++)
            {
                var result = results[i];
                if (result.Address != null)
                {
                    WriteRowResult(sheet, i, indexes, result.Address, result.NormalizationMetadata?.CombinedProbability ?? 0);
                    writtenCount++;
                }

                SetBackroundColor(sheet, i);
            }
            Console.WriteLine($"{LogTs()} [API] Zapisano {writtenCount} wyników spełniających próg prawdopodobieństwa");

            Console.WriteLine($"{LogTs()} [API] Zapis pliku wynikowego: {resultFilePath}");
            workbook.SaveAs(resultFilePath);
            Console.WriteLine($"{LogTs()} [API] Plik zapisany pomyślnie");
            Console.WriteLine($"{LogTs()} [API] Zamykanie pliku XLSX...");
        }

        // Główny przebieg testów dla DB: odczyt danych z Excela, zapytania SQL i zapis wielowynikowy.
        public static async Task RunDbTest(string originalFilePath, string dbResultFilePath, string connectionString, string query, int maxParallelRequests, IDictionary<string, string> dbMapping)
        {
            Console.WriteLine($"{LogTs()} [DB] Otwieranie pliku XLSX: {originalFilePath}");
            using var workbook = new XLWorkbook(originalFilePath);
            workbook.CalculateMode = XLCalculateMode.Manual; // Wyłączamy auto-kalkulację dla lepszej wydajności
            var sheet = workbook.Worksheet(1);
            Console.WriteLine($"{LogTs()} [DB] Plik XLSX otwarty pomyślnie");

            Console.WriteLine($"{LogTs()} [DB] Analizowanie struktury kolumn...");
            var indexes = GetColumnIndexes(sheet);
            var rowsNo = sheet.Rows().Count();
            Console.WriteLine($"{LogTs()} [DB] Struktura kolumn przeanalizowana");

            Console.WriteLine($"{LogTs()} [DB] Start: wiersze={rowsNo - 1}, równoległość={maxParallelRequests}");

            var semaphore = new SemaphoreSlim(maxParallelRequests);
            var tasks = new List<Task>();
            var results = new ConcurrentDictionary<int, List<ApiResponseAddressDto>>();
            var errorCount = new ConcurrentDictionary<string, int>(); // Śledzenie typów błędów
            var totalRows = rowsNo - 1;
            var processed = 0;

            Console.WriteLine($"{LogTs()} [DB] Tworzenie zadań dla {totalRows} wierszy...");
            for (int i = 2; i <= rowsNo; i++)
            {
                tasks.Add(ProcessDbRow(
                    sheet,
                    i,
                    indexes,
                    connectionString,
                    query,
                    semaphore,
                    results,
                    dbMapping,
                    errorCount,
                    () =>
                    {
                        var done = Interlocked.Increment(ref processed);
                        if (done % 50 == 0 || done == totalRows || done <= 5)
                        {
                            Console.WriteLine($"{LogTs()} [DB] Postęp: {done}/{totalRows}");
                        }
                    }));
            }
            Console.WriteLine($"{LogTs()} [DB] Utworzono {tasks.Count} zadań, rozpoczynam przetwarzanie...");

            await Task.WhenAll(tasks);
            Console.WriteLine($"{LogTs()} [DB] Wszystkie zapytania SQL zakończone, przetworzono {results.Count} wierszy");
            
            // Podsumowanie błędów
            if (errorCount.Count > 0)
            {
                Console.WriteLine($"{LogTs()} [DB] ⚠️  Wykryto błędy podczas przetwarzania:");
                foreach (var error in errorCount.OrderByDescending(e => e.Value))
                {
                    Console.WriteLine($"{LogTs()} [DB]   - {error.Key}: {error.Value} razy");
                }
            }
            else
            {
                Console.WriteLine($"{LogTs()} [DB] ✓ Wszystkie wiersze przetworzone bez błędów");
            }

            Console.WriteLine($"{LogTs()} [DB] Analizowanie wyników...");
            
            // Sprawdzamy szczegóły wyników przed zapisem
            var rowsWithResults = results.Where(kvp => kvp.Value != null && kvp.Value.Count > 0).ToList();
            Console.WriteLine($"{LogTs()} [DB] Wiersze z wynikami: {rowsWithResults.Count} z {results.Count}");
            
            if (rowsWithResults.Any())
            {
                var sampleRow = rowsWithResults.First();
                Console.WriteLine($"{LogTs()} [DB] Przykład: wiersz {sampleRow.Key} ma {sampleRow.Value.Count} rekordów");
                if (sampleRow.Value.Any())
                {
                    var firstRecord = sampleRow.Value[0];
                    Console.WriteLine($"{LogTs()} [DB]   Pierwszy rekord: City={firstRecord.City}, StreetName={firstRecord.StreetName}, PostalCode={firstRecord.PostalCode}");
                }
            }
            
            var maxResults = results.Values.DefaultIfEmpty(new List<ApiResponseAddressDto>()).Max(r => r.Count);
            Console.WriteLine($"{LogTs()} [DB] Maksymalna liczba rekordów na wiersz: {maxResults}");
            
            if (maxResults > 0)
            {
                // Używamy LastColumnUsed() zamiast Columns().Count() dla dokładniejszego określenia ostatniej kolumny
                int lastUsedColumn;
                var lastCol = sheet.LastColumnUsed();
                if (lastCol != null)
                {
                    lastUsedColumn = lastCol.ColumnNumber();
                }
                else
                {
                    // Jeśli LastColumnUsed() zwraca null, szukamy ostatniej kolumny z danymi w pierwszym wierszu
                    lastUsedColumn = 0;
                    for (int col = 1; col <= sheet.Columns().Count(); col++)
                    {
                        var cell = sheet.Cell(1, col);
                        var cellValue = cell.GetString() ?? string.Empty;
                        if (!string.IsNullOrWhiteSpace(cellValue))
                        {
                            lastUsedColumn = col;
                        }
                    }
                    if (lastUsedColumn == 0)
                    {
                        lastUsedColumn = sheet.Columns().Count();
                    }
                }
                var startColumn = lastUsedColumn + 1;
                Console.WriteLine($"{LogTs()} [DB] Ostatnia użyta kolumna: {lastUsedColumn}, start kolumna dla wyników DB: {startColumn}");
                Console.WriteLine($"{LogTs()} [DB] Tworzenie nagłówków dla {maxResults} wyników na wiersz (start kolumna: {startColumn})...");
                EnsureDbResultHeaders(sheet, startColumn, maxResults);
                Console.WriteLine($"{LogTs()} [DB] Nagłówki utworzone");

                Console.WriteLine($"{LogTs()} [DB] Zapis wyników do arkusza...");
                var totalRecordsWritten = 0;
                var rowsWithDataWritten = 0;
                for (int i = 2; i <= rowsNo; i++)
                {
                    results.TryGetValue(i, out var rowResults);
                    var rowResultsList = rowResults ?? new List<ApiResponseAddressDto>();
                    if (rowResultsList.Count > 0)
                    {
                        WriteDbRowResults(sheet, i, startColumn, rowResultsList);
                        totalRecordsWritten += rowResultsList.Count;
                        rowsWithDataWritten++;
                        
                        // Logujemy pierwsze kilka wierszy z danymi dla weryfikacji
                        if (rowsWithDataWritten <= 3)
                        {
                            Console.WriteLine($"{LogTs()} [DB]   Zapisano wiersz {i}: {rowResultsList.Count} rekordów, start kolumna: {startColumn}");
                            var firstRec = rowResultsList[0];
                            Console.WriteLine($"{LogTs()} [DB]     Przykład danych: City='{firstRec.City}', Street='{firstRec.StreetName}'");
                        }
                    }
                }
                Console.WriteLine($"{LogTs()} [DB] Zapisano łącznie {totalRecordsWritten} rekordów z bazy danych w {rowsWithDataWritten} wierszach");
                
                // Weryfikacja zapisu - sprawdzamy kilka przykładowych komórek
                if (rowsWithDataWritten > 0)
                {
                    var firstRowWithData = results.FirstOrDefault(kvp => kvp.Value != null && kvp.Value.Count > 0);
                    if (firstRowWithData.Key > 0)
                    {
                        var verifyCol = startColumn;
                        var verifyValue = sheet.Cell(firstRowWithData.Key, verifyCol).GetString() ?? string.Empty;
                        Console.WriteLine($"{LogTs()} [DB] Weryfikacja: wiersz {firstRowWithData.Key}, kolumna {verifyCol} = '{verifyValue}'");
                        
                        // Sprawdzamy też nagłówek
                        var headerValue = sheet.Cell(1, startColumn).GetString() ?? string.Empty;
                        Console.WriteLine($"{LogTs()} [DB] Weryfikacja nagłówka: kolumna {startColumn} = '{headerValue}'");
                    }
                }
            }
            else
            {
                Console.WriteLine($"{LogTs()} [DB] ⚠️  Brak wyników do zapisania - sprawdź czy zapytania SQL zwracają dane");
            }

            Console.WriteLine($"{LogTs()} [DB] Zapis pliku wynikowego: {dbResultFilePath}");
            workbook.SaveAs(dbResultFilePath);
            Console.WriteLine($"{LogTs()} [DB] Plik zapisany pomyślnie");
            
            // Ostatnia weryfikacja - sprawdzamy czy plik został zapisany i ma odpowiedni rozmiar
            if (File.Exists(dbResultFilePath))
            {
                var fileInfo = new FileInfo(dbResultFilePath);
                Console.WriteLine($"{LogTs()} [DB] Plik zapisany: {fileInfo.Length} bajtów");
            }
            else
            {
                Console.WriteLine($"{LogTs()} [DB] ⚠️  OSTRZEŻENIE: Plik wynikowy nie istnieje po zapisie!");
            }
            Console.WriteLine($"{LogTs()} [DB] Zamykanie pliku XLSX...");
            Console.WriteLine($"{LogTs()} [DB] Zakończono: zapisano do {dbResultFilePath}, maks liczba rekordów na wiersz={maxResults}");
        }

        private static readonly string[] DbResultFieldOrder = new[]
        {
            "StreetPrefix",
            "StreetName",
            "BuildingNumber",
            "City",
            "PostalCode",
            "Commune",
            "District",
            "Province"
        };

        // Klasa pomocnicza do przechowywania danych wiersza w pamięci
        private class RowData
        {
            public string StreetName { get; set; } = string.Empty;
            public string Prefix { get; set; } = string.Empty;
            public string BuildingNo { get; set; } = string.Empty;
            public string City { get; set; } = string.Empty;
            public string PostalCode { get; set; } = string.Empty;
        }

        // Przetwarza pojedynczy wiersz: pobiera wynik z API i zapisuje go w pamięci współdzielonej.
        private static async Task ProcessRow(
            RowData rowData, 
            int i, 
            ColumnIndexes indexes, 
            string apiUrl, 
            SemaphoreSlim semaphore,
            ConcurrentDictionary<int, NormalizationApiResponseDto> results,
            Action progressCallback)
        {
            await semaphore.WaitAsync();

            try
            {
                var result = await GetResultForRow(rowData, apiUrl);
                results.TryAdd(i, result);
            }
            catch
            {
                // W przypadku błędu zapisujemy pustą odpowiedź, aby nie zatrzymać całego procesu.
                results.TryAdd(i, new NormalizationApiResponseDto());
            }
            finally
            {
                semaphore.Release();
                progressCallback?.Invoke();
            }
        }

        // Przetwarza pojedynczy wiersz dla DB: wykonuje zapytanie SQL i gromadzi listę wyników.
        private static async Task ProcessDbRow(
            IXLWorksheet sheet,
            int i,
            ColumnIndexes indexes,
            string connectionString,
            string query,
            SemaphoreSlim semaphore,
            ConcurrentDictionary<int, List<ApiResponseAddressDto>> results,
            IDictionary<string, string> dbMapping,
            ConcurrentDictionary<string, int> errorCount,
            Action progressCallback)
        {
            await semaphore.WaitAsync();

            try
            {
                var rowResults = await GetDbResultsForRow(sheet, i, indexes, connectionString, query, dbMapping);
                results.TryAdd(i, rowResults);
            }
            catch (Exception ex)
            {
                // Logujemy błąd dla diagnostyki, ale kontynuujemy przetwarzanie
                var errorKey = $"{ex.GetType().Name}: {ex.Message}";
                errorCount.AddOrUpdate(errorKey, 1, (key, count) => count + 1);
                
                Console.WriteLine($"{LogTs()} [DB] ⚠️  BŁĄD w wierszu {i}: {ex.GetType().Name}");
                Console.WriteLine($"{LogTs()} [DB]    Komunikat: {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"{LogTs()} [DB]    Wewnętrzny błąd: {ex.InnerException.GetType().Name} - {ex.InnerException.Message}");
                }
                if (ex is IndexOutOfRangeException)
                {
                    Console.WriteLine($"{LogTs()} [DB]    Problem: Próba odczytu nieistniejącej kolumny z wyniku zapytania SQL");
                }
                results.TryAdd(i, new List<ApiResponseAddressDto>());
            }
            finally
            {
                semaphore.Release();
                progressCallback?.Invoke();
            }
        }

        // Nadaje tło komórkom na podstawie porównania EXPECTED vs RESULT oraz uzupełnia kolumnę IsCorrect.
        private static void SetBackroundColor(IXLWorksheet sheet, int rowNo)
        {
            var columnsNo = sheet.Columns().Count();

            for (int i = columnsNo; i > 0; i--)
            {
                var header = sheet.Cell(1, i).Value.ToString();
                if (header.Contains("EXPECTED"))
                {
                    var expectedValue = sheet.Cell(rowNo, i).Value.ToString();
                    if (expectedValue == "null")
                        expectedValue = string.Empty;

                    var resultValue = sheet.Cell(rowNo, i + 1).Value.ToString();
                    if (resultValue == "null")
                        resultValue = string.Empty;

                    var color = XLColor.Red;

                    if (string.Equals(expectedValue, resultValue, StringComparison.CurrentCultureIgnoreCase))
                    {
                        color = XLColor.Green;
                    }

                    sheet.Cell(rowNo, i + 1).Style.Fill.BackgroundColor = color;

                    var isCorrectCurrentValue = sheet.Cell(rowNo, columnsNo).Value.ToString();
                    if (isCorrectCurrentValue == "1" || isCorrectCurrentValue == string.Empty)
                    {
                        sheet.Cell(rowNo, columnsNo - 1).Value = color == XLColor.Red ? 0 : 1;
                    }                   
                }
            }
        }

        // Tworzy nagłówki DB_RESULT_X_* dynamicznie w zależności od maksymalnej liczby zwróconych rekordów.
        private static void EnsureDbResultHeaders(IXLWorksheet sheet, int startColumn, int maxResults)
        {
            var fieldsPerResult = DbResultFieldOrder.Length;

            for (int resultIndex = 1; resultIndex <= maxResults; resultIndex++)
            {
                var baseCol = startColumn + (resultIndex - 1) * fieldsPerResult;
                sheet.Cell(1, baseCol + 0).Value = $"DB_RESULT_{resultIndex}_streetP";
                sheet.Cell(1, baseCol + 1).Value = $"DB_RESULT_{resultIndex}_streetN";
                sheet.Cell(1, baseCol + 2).Value = $"DB_RESULT_{resultIndex}_building";
                sheet.Cell(1, baseCol + 3).Value = $"DB_RESULT_{resultIndex}_city";
                sheet.Cell(1, baseCol + 4).Value = $"DB_RESULT_{resultIndex}_postal";
                sheet.Cell(1, baseCol + 5).Value = $"DB_RESULT_{resultIndex}_commune";
                sheet.Cell(1, baseCol + 6).Value = $"DB_RESULT_{resultIndex}_district";
                sheet.Cell(1, baseCol + 7).Value = $"DB_RESULT_{resultIndex}_province";
            }
        }

        // Wpisuje zestaw wyników DB w kolejnych blokach kolumn dla danego wiersza.
        private static void WriteDbRowResults(IXLWorksheet sheet, int rowNo, int startColumn, List<ApiResponseAddressDto> results)
        {
            if (results == null || results.Count == 0)
            {
                return;
            }

            var fieldsPerResult = DbResultFieldOrder.Length;

            for (int idx = 0; idx < results.Count; idx++)
            {
                var baseCol = startColumn + idx * fieldsPerResult;
                var r = results[idx];
                
                // Zapisujemy wartości - null zostanie zapisany jako pusta komórka
                sheet.Cell(rowNo, baseCol + 0).Value = r.StreetPrefix ?? string.Empty;
                sheet.Cell(rowNo, baseCol + 1).Value = r.StreetName ?? string.Empty;
                sheet.Cell(rowNo, baseCol + 2).Value = r.BuildingNumber ?? string.Empty;
                sheet.Cell(rowNo, baseCol + 3).Value = r.City ?? string.Empty;
                sheet.Cell(rowNo, baseCol + 4).Value = r.PostalCode ?? string.Empty;
                sheet.Cell(rowNo, baseCol + 5).Value = r.Commune ?? string.Empty;
                sheet.Cell(rowNo, baseCol + 6).Value = r.District ?? string.Empty;
                sheet.Cell(rowNo, baseCol + 7).Value = r.Province ?? string.Empty;
            }
        }

        // Wpisuje wartości zwrócone przez API do kolumn wynikowych (RESULT*).
                private static void WriteRowResult(IXLWorksheet sheet, int rowNo, ColumnIndexes idx, ApiResponseAddressDto result, decimal combinedProbability)
        {
            sheet.Cell(rowNo, idx.ResultBuildingNoIndex).Value = result.BuildingNumber;
            sheet.Cell(rowNo, idx.ResultCityIndex).Value = result.City;
            sheet.Cell(rowNo, idx.ResultCommuneIndex).Value = result.Commune;
            sheet.Cell(rowNo, idx.ResultDistrictIndex).Value = result.District;
            sheet.Cell(rowNo, idx.ResultPostalCodeIndex).Value = result.PostalCode;
            sheet.Cell(rowNo, idx.ResultProvinceIndex).Value = result.Province;
            sheet.Cell(rowNo, idx.ResultStreetIndex).Value = result.StreetName;
            sheet.Cell(rowNo, idx.ResultPrefixIndex).Value = result.StreetPrefix;
            sheet.Cell(rowNo, idx.ProbabilityIndex).Value = combinedProbability;
        }

        // Pomocnicza metoda do normalizacji wartości z Excela
        private static string NormalizeValue(string? value)
        {
            return value == "null" || value == null ? string.Empty : value;
        }

        // Pobiera wartości z wiersza, sanitizuje "null" na puste ciągi i wysyła zapytanie POST do API.
        private static async Task<NormalizationApiResponseDto> GetResultForRow(RowData rowData, string endpoint)
        {
            try
            {
                var body = new
                {
                    StreetName = rowData.StreetName,
                    StreetPrefix = rowData.Prefix,
                    BuildingNumber = rowData.BuildingNo,
                    City = rowData.City,
                    PostalCode = rowData.PostalCode
                };

                // Używamy współdzielonego HttpClient z optymalizowanymi ustawieniami JSON
                var requestContent = JsonContent.Create(body, options: JsonOptions);
                var response = await SharedHttpClient.PostAsync(endpoint, requestContent);
                
                if (!response.IsSuccessStatusCode)
                {
                    // Zwracamy pustą odpowiedź, aby przetwarzanie innych wierszy mogło trwać dalej.
                    return new NormalizationApiResponseDto();
                }

                // Używamy zoptymalizowanej deserializacji JSON
                var jsonString = await response.Content.ReadAsStringAsync();
                var responseObject = JsonSerializer.Deserialize<NormalizationApiResponseDto>(jsonString, JsonOptions);
                
                if (responseObject is null)
                {
                    return new NormalizationApiResponseDto();
                }

                return responseObject;
            }
            catch
            {
                // Każdy błąd sieci/deserializacji mapujemy na pustą odpowiedź.
                return new NormalizationApiResponseDto();
            }
        }

        // Wykonuje zapytanie do Postgresa i zwraca listę pasujących adresów dla wiersza.
        private static async Task<List<ApiResponseAddressDto>> GetDbResultsForRow(IXLWorksheet sheet, int rowNo, ColumnIndexes idx, string connectionString, string query, IDictionary<string, string> dbMapping)
        {
            var streetName = GetRequestValue(sheet, rowNo, idx.RequestStreetIndex, dbMapping, "StreetName");
            var prefix = GetRequestValue(sheet, rowNo, idx.RequestPrefixIndex, dbMapping, "StreetPrefix");
            var buildingNo = GetRequestValue(sheet, rowNo, idx.RequestBuildingNoIndex, dbMapping, "BuildingNumber");
            var city = GetRequestValue(sheet, rowNo, idx.RequestCityIndex, dbMapping, "City");
            var postalCode = GetRequestValue(sheet, rowNo, idx.RequestPostalCodeIndex, dbMapping, "PostalCode");

            if (streetName == "null")
                streetName = string.Empty;
            if (prefix == "null")
                prefix = string.Empty;
            if (buildingNo == "null")
                buildingNo = string.Empty;
            if (city == "null")
                city = string.Empty;
            if (postalCode == "null")
                postalCode = string.Empty;

            var results = new List<ApiResponseAddressDto>();

            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            await using var command = new NpgsqlCommand(query, connection);
            command.Parameters.AddWithValue("StreetName", streetName ?? string.Empty);
            command.Parameters.AddWithValue("StreetPrefix", prefix ?? string.Empty);
            command.Parameters.AddWithValue("BuildingNumber", buildingNo ?? string.Empty);
            command.Parameters.AddWithValue("City", city ?? string.Empty);
            command.Parameters.AddWithValue("PostalCode", postalCode ?? string.Empty);

            await using var reader = await command.ExecuteReaderAsync();
            
            // Sprawdzamy dostępne kolumny w wyniku zapytania (przed pierwszym ReadAsync)
            var availableColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var schemaTable = reader.GetSchemaTable();
            if (schemaTable != null)
            {
                foreach (DataRow row in schemaTable.Rows)
                {
                    var columnName = row["ColumnName"]?.ToString();
                    if (!string.IsNullOrEmpty(columnName))
                    {
                        availableColumns.Add(columnName);
                    }
                }
            }
            
            // Oczekiwane kolumny
            var expectedColumns = new[] { "StreetPrefix", "StreetName", "BuildingNumber", "City", "PostalCode", "Commune", "District", "Province" };
            var missingColumns = expectedColumns.Where(col => !availableColumns.Contains(col)).ToList();
            
            // Logujemy brakujące kolumny tylko przy pierwszym wierszu (aby uniknąć spamowania)
            if (rowNo == 2 && missingColumns.Any())
            {
                Console.WriteLine($"{LogTs()} [DB] ⚠️  OSTRZEŻENIE: Zapytanie SQL nie zwraca wszystkich oczekiwanych kolumn:");
                Console.WriteLine($"{LogTs()} [DB]    Dostępne kolumny: {string.Join(", ", availableColumns.OrderBy(c => c))}");
                Console.WriteLine($"{LogTs()} [DB]    Brakujące kolumny: {string.Join(", ", missingColumns)}");
                Console.WriteLine($"{LogTs()} [DB]    Brakujące wartości będą ustawione na NULL");
            }
            
            while (await reader.ReadAsync())
            {
                results.Add(new ApiResponseAddressDto
                {
                    StreetPrefix = GetDbStringSafe(reader, "StreetPrefix", availableColumns, rowNo, missingColumns.Contains("StreetPrefix")),
                    StreetName = GetDbStringSafe(reader, "StreetName", availableColumns, rowNo, missingColumns.Contains("StreetName")),
                    BuildingNumber = GetDbStringSafe(reader, "BuildingNumber", availableColumns, rowNo, missingColumns.Contains("BuildingNumber")),
                    City = GetDbStringSafe(reader, "City", availableColumns, rowNo, missingColumns.Contains("City")),
                    PostalCode = GetDbStringSafe(reader, "PostalCode", availableColumns, rowNo, missingColumns.Contains("PostalCode")),
                    Commune = GetDbStringSafe(reader, "Commune", availableColumns, rowNo, missingColumns.Contains("Commune")),
                    District = GetDbStringSafe(reader, "District", availableColumns, rowNo, missingColumns.Contains("District")),
                    Province = GetDbStringSafe(reader, "Province", availableColumns, rowNo, missingColumns.Contains("Province"))
                });
            }

            return results;
        }

        private static string GetRequestValue(IXLWorksheet sheet, int rowNo, int fallbackIndex, IDictionary<string, string> mapping, string paramName)
        {
            // Jeżeli zdefiniowano mapowanie dla parametru, próbujemy znaleźć kolumnę po nagłówku.
            if (mapping.TryGetValue(paramName, out var headerName) && !string.IsNullOrWhiteSpace(headerName))
            {
                var colIndex = GetColumnIndexByHeader(sheet, headerName);
                if (colIndex.HasValue)
                {
                    return NormalizeRequestValue(sheet.Cell(rowNo, colIndex.Value).Value.ToString());
                }
            }

            // Fallback do indeksu wyznaczonego standardowo (REQUEST_*).
            return NormalizeRequestValue(sheet.Cell(rowNo, fallbackIndex).Value.ToString());
        }

        private static int? GetColumnIndexByHeader(IXLWorksheet sheet, string headerName)
        {
            var columnsNo = sheet.Columns().Count();
            for (int i = columnsNo; i > 0; i--)
            {
                var header = sheet.Cell(1, i).Value.ToString();
                if (string.Equals(header, headerName, StringComparison.OrdinalIgnoreCase))
                    return i;
            }
            return null;
        }

        private static string NormalizeRequestValue(string value)
        {
            return value == "null" ? string.Empty : value;
        }

        private static string LogTs() => DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

        private static string? GetDbString(IDataRecord record, string columnName)
        {
            try
            {
                // Sprawdzamy czy kolumna istnieje przed próbą odczytania
                var ordinal = record.GetOrdinal(columnName);
                if (ordinal < 0 || ordinal >= record.FieldCount)
                {
                    return null;
                }
                return record.IsDBNull(ordinal) ? null : record.GetString(ordinal);
            }
            catch (IndexOutOfRangeException)
            {
                return null;
            }
            catch (ArgumentException)
            {
                // GetOrdinal rzuca ArgumentException gdy kolumna nie istnieje
                return null;
            }
        }

        private static string? GetDbStringSafe(IDataRecord record, string columnName, HashSet<string> availableColumns, int rowNo, bool isMissingColumn)
        {
            // Najpierw sprawdzamy czy kolumna istnieje w dostępnych kolumnach
            if (!availableColumns.Contains(columnName))
            {
                // Logujemy tylko raz dla pierwszego wiersza z danymi (rowNo == 2 to pierwszy wiersz danych)
                // Informacja o brakujących kolumnach jest już zalogowana wcześniej
                return null;
            }

            try
            {
                var ordinal = record.GetOrdinal(columnName);
                if (ordinal < 0 || ordinal >= record.FieldCount)
                {
                    if (rowNo == 2) // Logujemy tylko raz
                    {
                        Console.WriteLine($"{LogTs()} [DB] ⚠️  Kolumna '{columnName}' ma nieprawidłowy indeks: {ordinal} (FieldCount: {record.FieldCount})");
                    }
                    return null;
                }
                return record.IsDBNull(ordinal) ? null : record.GetString(ordinal);
            }
            catch (IndexOutOfRangeException ex)
            {
                if (rowNo == 2) // Logujemy tylko raz
                {
                    Console.WriteLine($"{LogTs()} [DB] ⚠️  IndexOutOfRangeException dla kolumny '{columnName}' w wierszu {rowNo}: {ex.Message}");
                }
                return null;
            }
            catch (ArgumentException ex)
            {
                // GetOrdinal rzuca ArgumentException gdy kolumna nie istnieje
                if (rowNo == 2) // Logujemy tylko raz
                {
                    Console.WriteLine($"{LogTs()} [DB] ⚠️  ArgumentException dla kolumny '{columnName}' w wierszu {rowNo}: {ex.Message}");
                }
                return null;
            }
        }

        // Dodaje kolumny RESULT obok EXPECTED oraz kolumnę IsCorrect na końcu arkusza.
        private static void AddResultColumns(IXLWorksheet sheet)
        {
            var columnsNo = sheet.Columns().Count();

            for (int i = columnsNo; i > 0; i--)
            {
                var header = sheet.Cell(1, i).Value.ToString();
                if (header.Contains("EXPECTED"))
                {
                    sheet.Column(i).InsertColumnsAfter(1);
                    sheet.Cell(1, i + 1).Value = header.Replace("EXPECTED", "RESULT");
                }
            }

            columnsNo = sheet.Columns().Count();
            sheet.Column(columnsNo).InsertColumnsAfter(2);
            sheet.Cell(1, columnsNo + 1).Value = "IsCorrect";
            sheet.Column(columnsNo + 1).SetAutoFilter();
            sheet.Column(columnsNo + 1).Width = 15;

            sheet.Cell(1, columnsNo + 2).Value = "Probability";
            sheet.Column(columnsNo + 2).Width = 10;
        }

        // Odczytuje numery kolumn REQUEST/RESULT na podstawie nagłówków, aby logika była odporna na kolejność kolumn.
        private static ColumnIndexes GetColumnIndexes(IXLWorksheet sheet)
        {
            var result = new ColumnIndexes();

            var columnsNo = sheet.Columns().Count();
            
            result.ProbabilityIndex = columnsNo;

            for (int i = columnsNo; i > 0; i--)
            {
                var header = sheet.Cell(1, i).Value.ToString();

                if (header.Contains("REQUEST"))
                {
                    if (header.Contains("streetN"))
                        result.RequestStreetIndex = i;
                    if (header.Contains("streetP"))
                        result.RequestPrefixIndex = i;
                    if (header.Contains("building"))
                        result.RequestBuildingNoIndex = i;
                    if (header.Contains("city"))
                        result.RequestCityIndex = i;
                    if (header.Contains("postal"))
                        result.RequestPostalCodeIndex = i;
                }

                if (header.Contains("RESULT"))
                {
                    if (header.Contains("streetN"))
                        result.ResultStreetIndex = i;
                    if (header.Contains("streetP"))
                        result.ResultPrefixIndex = i;
                    if (header.Contains("building"))
                        result.ResultBuildingNoIndex = i;
                    if (header.Contains("city"))
                        result.ResultCityIndex = i;
                    if (header.Contains("postal"))
                        result.ResultPostalCodeIndex = i;
                    if (header.Contains("commune"))
                        result.ResultCommuneIndex = i;
                    if (header.Contains("district"))
                        result.ResultDistrictIndex = i;
                    if (header.Contains("province"))
                        result.ResultProvinceIndex = i;
                }
            }

            return result;
        }

        private class ColumnIndexes
        {
            public int RequestStreetIndex { get; set; }
            public int RequestPrefixIndex { get; set; }
            public int RequestBuildingNoIndex { get; set; }
            public int RequestCityIndex { get; set; }
            public int RequestPostalCodeIndex { get; set; }
      
            public int ResultStreetIndex { get; set; }
            public int ResultPrefixIndex { get; set; }
            public int ResultBuildingNoIndex { get; set; }
            public int ResultCityIndex { get; set; }
            public int ResultPostalCodeIndex { get; set; }
            public int ResultProvinceIndex { get; set; }
            public int ResultDistrictIndex { get; set; }
            public int ResultCommuneIndex{ get; set; }
            public int ProbabilityIndex { get; set; }
        }

        private class NormalizationApiResponseDto
        {
            public ApiResponseMetadataDto? NormalizationMetadata { get; set; }

            public ApiResponseAddressDto? Address { get; set; }
        }

        private class ApiResponseMetadataDto
        {
            public decimal StreetProbability { get; set; }

            public decimal PostalCodeProbability { get; set; }

            public decimal CityProbability { get; set; }

            public decimal CombinedProbability { get; set; }

            public int? NormalizationId { get; set; }
        }

        private class ApiResponseAddressDto
        {
            public bool IsMultiFamily { get; set; } = false;

            public decimal? Longitude { get; set; }

            public decimal? Latitude { get; set; }

            public string? PostOfficeLocation { get; set; }

            public string? Commune { get; set; }

            public string? Province { get; set; }

            public string? District { get; set; }

            public string? StreetPrefix { get; set; }

            public string? StreetName { get; set; }

            public string? BuildingNumber { get; set; }

            public string? City { get; set; }

            public string? PostalCode { get; set; }
        }
    }
}
