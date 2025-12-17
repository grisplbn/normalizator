using ClosedXML.Excel;
using System.Collections.Concurrent;
using System.Data;
using System.IO;
using System.Linq;
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
            // Używamy SocketsHttpHandler dla lepszej wydajności i wsparcia HTTP/2
            var handler = new System.Net.Http.SocketsHttpHandler
            {
                MaxConnectionsPerServer = 100, // Zwiększamy limit połączeń równoległych na serwer
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2), // Dłużej trzymamy połączenia w puli
                PooledConnectionLifetime = TimeSpan.FromMinutes(10), // Maksymalny czas życia połączenia
                EnableMultipleHttp2Connections = true // Włączamy wiele połączeń HTTP/2 dla lepszej równoległości
            };

            // Wyłączamy weryfikację certyfikatu SSL dla localhost - znacznie przyspiesza połączenia lokalne
            // Uwaga: w SocketsHttpHandler używamy ConnectCallback zamiast RemoteCertificateValidationCallback
            // Dla uproszczenia akceptujemy wszystkie certyfikaty (tylko dla development/localhost)
            handler.SslOptions.RemoteCertificateValidationCallback = (sender, cert, chain, errors) =>
            {
                // Dla localhost akceptujemy wszystkie certyfikaty (tylko dla development)
                // W kontekście SocketsHttpHandler nie mamy bezpośredniego dostępu do hosta w callbacku,
                // więc akceptujemy wszystkie certyfikaty - używaj tylko dla localhost!
                return true;
            };

            var client = new HttpClient(handler)
            {
                Timeout = TimeSpan.FromMinutes(5), // Timeout 5 minut dla długich zapytań
                DefaultRequestVersion = new Version(2, 0) // Wymuszamy HTTP/2 jeśli dostępne
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

        // Benchmark API - testuje różne poziomy równoległości aby znaleźć optymalną wartość
        public static async Task<int> RunBenchmark(string apiUrl, int testRequests = 50)
        {
            Console.WriteLine($"{LogTs()} [BENCHMARK] Rozpoczynam benchmark API...");
            Console.WriteLine($"{LogTs()} [BENCHMARK] Testuję {testRequests} requestów dla różnych poziomów równoległości");
            
            // Testowe dane do wysłania
            var testData = new RowData
            {
                StreetName = "Test",
                Prefix = "",
                BuildingNo = "1",
                City = "Warszawa",
                PostalCode = "00-001"
            };

            var levels = new[] { 5, 10, 15, 20, 30, 50, 75, 100 };
            var results = new Dictionary<int, BenchmarkResult>();

            foreach (var level in levels)
            {
                Console.WriteLine($"{LogTs()} [BENCHMARK] Testuję równoległość = {level}...");
                var result = await BenchmarkLevel(apiUrl, testData, level, testRequests);
                results[level] = result;
                Console.WriteLine($"{LogTs()} [BENCHMARK]   Wynik: {result.RequestsPerSecond:F2} req/s, średni czas: {result.AverageLatencyMs:F0}ms");
            }

            // Znajdź optymalny poziom (najwyższa przepustowość z rozsądnym opóźnieniem)
            var optimal = FindOptimalParallelism(results);
            
            Console.WriteLine($"{LogTs()} [BENCHMARK] ========== WYNIKI BENCHMARKA ==========");
            Console.WriteLine($"{LogTs()} [BENCHMARK] Najlepsza przepustowość: {optimal.BestThroughput.RequestsPerSecond:F2} req/s przy {optimal.BestThroughput.Parallelism} równoległych requestach");
            Console.WriteLine($"{LogTs()} [BENCHMARK] Najlepsze opóźnienie: {optimal.BestLatency.AverageLatencyMs:F0}ms przy {optimal.BestLatency.Parallelism} równoległych requestach");
            Console.WriteLine($"{LogTs()} [BENCHMARK] REKOMENDOWANA wartość MaxParallelRequests: {optimal.Recommended}");
            Console.WriteLine($"{LogTs()} [BENCHMARK] Oczekiwana przepustowość: {results[optimal.Recommended].RequestsPerSecond:F2} req/s");
            Console.WriteLine($"{LogTs()} [BENCHMARK] Oczekiwane średnie opóźnienie: {results[optimal.Recommended].AverageLatencyMs:F0}ms");
            Console.WriteLine($"{LogTs()} [BENCHMARK] ========================================");

            return optimal.Recommended;
        }

        private static async Task<BenchmarkResult> BenchmarkLevel(string apiUrl, RowData testData, int parallelism, int totalRequests)
        {
            var semaphore = new SemaphoreSlim(parallelism);
            var tasks = new List<Task<long>>();
            var startTime = DateTime.UtcNow;

            for (int i = 0; i < totalRequests; i++)
            {
                tasks.Add(BenchmarkSingleRequest(apiUrl, testData, semaphore));
            }

            var latencies = await Task.WhenAll(tasks);
            var endTime = DateTime.UtcNow;
            var duration = (endTime - startTime).TotalSeconds;
            var requestsPerSecond = totalRequests / duration;
            var averageLatency = latencies.Average();

            return new BenchmarkResult
            {
                Parallelism = parallelism,
                RequestsPerSecond = requestsPerSecond,
                AverageLatencyMs = averageLatency
            };
        }

        private static async Task<long> BenchmarkSingleRequest(string apiUrl, RowData testData, SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            try
            {
                var startTime = DateTime.UtcNow;
                await GetResultForRow(testData, apiUrl);
                var elapsed = (DateTime.UtcNow - startTime).TotalMilliseconds;
                return (long)elapsed;
            }
            finally
            {
                semaphore.Release();
            }
        }

        private static (BenchmarkResult BestThroughput, BenchmarkResult BestLatency, int Recommended) FindOptimalParallelism(Dictionary<int, BenchmarkResult> results)
        {
            var bestThroughput = results.Values.OrderByDescending(r => r.RequestsPerSecond).First();
            var bestLatency = results.Values.OrderBy(r => r.AverageLatencyMs).First();

            // Rekomendacja: wybierz poziom który ma >90% najlepszej przepustowości i <150% najlepszego opóźnienia
            var thresholdThroughput = bestThroughput.RequestsPerSecond * 0.90;
            var thresholdLatency = bestLatency.AverageLatencyMs * 1.50;

            var candidates = results.Values
                .Where(r => r.RequestsPerSecond >= thresholdThroughput && r.AverageLatencyMs <= thresholdLatency)
                .OrderByDescending(r => r.RequestsPerSecond)
                .ThenBy(r => r.Parallelism);

            var recommended = candidates.FirstOrDefault() ?? bestThroughput;

            return (bestThroughput, bestLatency, recommended.Parallelism);
        }

        private class BenchmarkResult
        {
            public int Parallelism { get; set; }
            public double RequestsPerSecond { get; set; }
            public double AverageLatencyMs { get; set; }
        }

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
            var totalRows = rowsNo - 1;
            Console.WriteLine($"{LogTs()} [API] Start: Rows={totalRows},  Parallel requests={maxParallelRequests}");

            // Tworzymy dodatkowe kolumny na wyniki, aby nie nadpisywać oczekiwanych wartości.
            Console.WriteLine($"{LogTs()} [API] Dodawanie kolumn wynikowych...");
            AddResultColumns(sheet);
            Console.WriteLine($"{LogTs()} [API] Kolumny wynikowe dodane");

            Console.WriteLine($"{LogTs()} [API] Analizowanie struktury kolumn...");
            var indexes = GetColumnIndexes(sheet);
            Console.WriteLine($"{LogTs()} [API] Struktura kolumn przeanalizowana");

            // Optymalizacja: cache'ujemy kolumny EXPECTED dla szybkiego dostępu podczas kolorowania
            var expectedColumns = GetExpectedColumns(sheet);
            Console.WriteLine($"{LogTs()} [API] Znaleziono {expectedColumns.Count} kolumn EXPECTED do porównania");

            // Optymalizacja: wczytujemy wszystkie dane z Excela do pamięci na początku
            Console.WriteLine($"{LogTs()} [API] Wczytywanie danych z Excela do pamięci...");
            // Ustawiamy początkową pojemność Dictionary aby uniknąć realokacji podczas dodawania elementów
            var rowDataCache = new Dictionary<int, RowData>(totalRows);
            for (int i = 2; i <= rowsNo; i++)
            {
                // Używamy GetValue<T>() zamiast Value.ToString() dla lepszej wydajności
                rowDataCache[i] = new RowData
                {
                    StreetName = NormalizeValue(sheet.Cell(i, indexes.RequestStreetIndex).GetValue<string>()),
                    Prefix = NormalizeValue(sheet.Cell(i, indexes.RequestPrefixIndex).GetValue<string>()),
                    BuildingNo = NormalizeValue(sheet.Cell(i, indexes.RequestBuildingNoIndex).GetValue<string>()),
                    City = NormalizeValue(sheet.Cell(i, indexes.RequestCityIndex).GetValue<string>()),
                    PostalCode = NormalizeValue(sheet.Cell(i, indexes.RequestPostalCodeIndex).GetValue<string>())
                };
            }
            Console.WriteLine($"{LogTs()} [API] Wczytano {rowDataCache.Count} wierszy danych do pamięci");

            // Kontrolujemy równoległość zapytań, aby nie przeciążyć usługi.
            var semaphore = new SemaphoreSlim(maxParallelRequests);
            // Ustawiamy początkową pojemność listy zadań
            var tasks = new List<Task>(totalRows);
            // Ustawiamy concurrency level i initial capacity dla ConcurrentDictionary
            var results = new ConcurrentDictionary<int, NormalizationApiResponseDto>(System.Environment.ProcessorCount, totalRows);
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
            // Cache'ujemy columnsNo aby uniknąć wielokrotnego wywoływania
            var columnsNo = sheet.Columns().Count();
            var writtenCount = 0;
            for (int i = 2; i <= rowsNo; i++)
            {
                // Używamy TryGetValue zamiast bezpośredniego dostępu - bezpieczniejsze i szybsze
                if (results.TryGetValue(i, out var result) && result.Address != null)
                {
                    WriteRowResult(sheet, i, indexes, result.Address, result.NormalizationMetadata?.CombinedProbability ?? 0);
                    writtenCount++;
                }

                SetBackroundColor(sheet, i, expectedColumns, indexes, columnsNo);
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
            // Wyłączamy auto-formatowanie i inne zbędne funkcje dla lepszej wydajności
            sheet.Style.Alignment.WrapText = false;
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

        // Pomocnicza klasa do cache'owania kolumn EXPECTED
        private class ExpectedColumn
        {
            public int ExpectedIndex { get; set; }
            public int ResultIndex { get; set; }
        }

        // Zwraca listę kolumn EXPECTED wraz z odpowiadającymi im kolumnami RESULT
        private static List<ExpectedColumn> GetExpectedColumns(IXLWorksheet sheet)
        {
            var expectedColumns = new List<ExpectedColumn>();
            var columnsNo = sheet.Columns().Count();

            for (int i = columnsNo; i > 0; i--)
            {
                var header = sheet.Cell(1, i).GetValue<string>();
                if (header != null && header.Contains("EXPECTED", StringComparison.OrdinalIgnoreCase))
                {
                    expectedColumns.Add(new ExpectedColumn { ExpectedIndex = i, ResultIndex = i + 1 });
                }
            }

            return expectedColumns;
        }

        // Nadaje tło komórkom na podstawie porównania EXPECTED vs RESULT oraz uzupełnia kolumnę IsCorrect.
        private static void SetBackroundColor(IXLWorksheet sheet, int rowNo, List<ExpectedColumn> expectedColumns, ColumnIndexes indexes, int columnsNo)
        {
            // Cache'ujemy wartość IsCorrect column - odczytujemy tylko raz
            var isCorrectCell = sheet.Cell(rowNo, columnsNo);
            var isCorrectCurrentValue = isCorrectCell.GetValue<string>();
            var needsUpdate = isCorrectCurrentValue == "1" || string.IsNullOrEmpty(isCorrectCurrentValue);

            // Iterujemy tylko przez znalezione kolumny EXPECTED zamiast przez wszystkie kolumny
            foreach (var col in expectedColumns)
            {
                // Używamy GetValue<T>() zamiast Value.ToString() - szybsze i bez alokacji stringa
                // Używamy inline normalizacji zamiast wywołania metody dla lepszej wydajności
                var expectedValue = NormalizeCellValue(sheet.Cell(rowNo, col.ExpectedIndex).GetValue<string>());
                var resultValue = NormalizeCellValue(sheet.Cell(rowNo, col.ResultIndex).GetValue<string>());

                // OrdinalIgnoreCase jest szybsze niż CurrentCultureIgnoreCase
                var isMatch = string.Equals(expectedValue, resultValue, StringComparison.OrdinalIgnoreCase);
                var color = isMatch ? XLColor.Green : XLColor.Red;

                sheet.Cell(rowNo, col.ResultIndex).Style.Fill.BackgroundColor = color;

                // Aktualizujemy IsCorrect tylko jeśli potrzebne
                if (needsUpdate)
                {
                    sheet.Cell(rowNo, columnsNo - 1).Value = isMatch ? 1 : 0;
                    needsUpdate = false; // Aktualizujemy tylko raz
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
        // Używamy porównania OrdinalIgnoreCase - najszybsze dostępne w .NET
        private static string NormalizeValue(string? value)
        {
            if (value == null)
                return string.Empty;
            
            // OrdinalIgnoreCase jest zoptymalizowane w .NET i szybsze niż inne metody
            return string.Equals(value, "null", StringComparison.OrdinalIgnoreCase) ? string.Empty : value;
        }

        // Pomocnicza metoda do normalizacji wartości z porównaniem (inline dla lepszej wydajności)
        // Optymalizacja: sprawdzamy długość przed porównaniem - szybki early exit
        private static string NormalizeCellValue(string? value)
        {
            if (value == null || value.Length == 0)
                return string.Empty;
            
            // Szybkie porównanie - sprawdzamy długość przed porównaniem stringów
            // Dla wartości różnej długości niż 4, natychmiast zwracamy oryginał
            return (value.Length == 4 && string.Equals(value, "null", StringComparison.OrdinalIgnoreCase)) 
                ? string.Empty 
                : value;
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
                // Używamy SendAsync z HttpRequestMessage aby móc użyć HttpCompletionOption.ResponseHeadersRead
                using var request = new HttpRequestMessage(HttpMethod.Post, endpoint)
                {
                    Content = requestContent
                };
                var response = await SharedHttpClient.SendAsync(request, System.Net.Http.HttpCompletionOption.ResponseHeadersRead, CancellationToken.None);
                
                if (!response.IsSuccessStatusCode)
                {
                    // Zwracamy pustą odpowiedź, aby przetwarzanie innych wierszy mogło trwać dalej.
                    return new NormalizationApiResponseDto();
                }

                // Używamy zoptymalizowanej deserializacji JSON - ReadFromJsonAsync jest szybsze niż ReadAsStringAsync + Deserialize
                using var responseStream = await response.Content.ReadAsStreamAsync();
                var responseObject = await JsonSerializer.DeserializeAsync<NormalizationApiResponseDto>(responseStream, JsonOptions);
                
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
                    return NormalizeRequestValue(sheet.Cell(rowNo, colIndex.Value).GetValue<string>());
                }
            }

            // Fallback do indeksu wyznaczonego standardowo (REQUEST_*).
            return NormalizeRequestValue(sheet.Cell(rowNo, fallbackIndex).GetValue<string>());
        }

        private static int? GetColumnIndexByHeader(IXLWorksheet sheet, string headerName)
        {
            var columnsNo = sheet.Columns().Count();
            for (int i = columnsNo; i > 0; i--)
            {
                var header = sheet.Cell(1, i).GetValue<string>();
                if (header != null && string.Equals(header, headerName, StringComparison.OrdinalIgnoreCase))
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
                var header = sheet.Cell(1, i).GetValue<string>();
                if (header != null && header.Contains("EXPECTED", StringComparison.OrdinalIgnoreCase))
                {
                    sheet.Column(i).InsertColumnsAfter(1);
                    sheet.Cell(1, i + 1).Value = header.Replace("EXPECTED", "RESULT", StringComparison.OrdinalIgnoreCase);
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
                var header = sheet.Cell(1, i).GetValue<string>();
                if (header == null) continue;

                var headerUpper = header.ToUpperInvariant(); // Konwertujemy raz, używamy wielokrotnie

                if (headerUpper.Contains("REQUEST", StringComparison.Ordinal))
                {
                    if (headerUpper.Contains("STREETN", StringComparison.Ordinal))
                        result.RequestStreetIndex = i;
                    if (headerUpper.Contains("STREETP", StringComparison.Ordinal))
                        result.RequestPrefixIndex = i;
                    if (headerUpper.Contains("BUILDING", StringComparison.Ordinal))
                        result.RequestBuildingNoIndex = i;
                    if (headerUpper.Contains("CITY", StringComparison.Ordinal))
                        result.RequestCityIndex = i;
                    if (headerUpper.Contains("POSTAL", StringComparison.Ordinal))
                        result.RequestPostalCodeIndex = i;
                }

                if (headerUpper.Contains("RESULT", StringComparison.Ordinal))
                {
                    if (headerUpper.Contains("STREETN", StringComparison.Ordinal))
                        result.ResultStreetIndex = i;
                    if (headerUpper.Contains("STREETP", StringComparison.Ordinal))
                        result.ResultPrefixIndex = i;
                    if (headerUpper.Contains("BUILDING", StringComparison.Ordinal))
                        result.ResultBuildingNoIndex = i;
                    if (headerUpper.Contains("CITY", StringComparison.Ordinal))
                        result.ResultCityIndex = i;
                    if (headerUpper.Contains("POSTAL", StringComparison.Ordinal))
                        result.ResultPostalCodeIndex = i;
                    if (headerUpper.Contains("COMMUNE", StringComparison.Ordinal))
                        result.ResultCommuneIndex = i;
                    if (headerUpper.Contains("DISTRICT", StringComparison.Ordinal))
                        result.ResultDistrictIndex = i;
                    if (headerUpper.Contains("PROVINCE", StringComparison.Ordinal))
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
