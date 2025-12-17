using System.Net.Http.Json;
using System.Text.Json;

namespace NormalizatorTests
{
    internal class BenchmarkEngine
    {
        // Współdzielony HttpClient dla benchmarków (thread-safe)
        private static readonly Lazy<HttpClient> _sharedHttpClient = new Lazy<HttpClient>(() =>
        {
            // Używamy SocketsHttpHandler dla lepszej wydajności i wsparcia HTTP/2
            var handler = new System.Net.Http.SocketsHttpHandler
            {
                MaxConnectionsPerServer = 100,
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
                PooledConnectionLifetime = TimeSpan.FromMinutes(10),
                EnableMultipleHttp2Connections = true
            };

            // Wyłączamy weryfikację certyfikatu SSL dla localhost
            handler.SslOptions.RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true;

            var client = new HttpClient(handler)
            {
                Timeout = TimeSpan.FromMinutes(5),
                DefaultRequestVersion = new Version(2, 0)
            };

            client.DefaultRequestHeaders.Add("Accept-Encoding", "gzip, deflate, br");
            
            return client;
        });

        private static HttpClient SharedHttpClient => _sharedHttpClient.Value;

        // Współdzielone ustawienia JSON dla lepszej wydajności serializacji/deserializacji
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.Never,
            WriteIndented = false,
            AllowTrailingCommas = true,
            ReadCommentHandling = JsonCommentHandling.Skip
        };

        // Benchmark API - testuje różne poziomy równoległości aby znaleźć optymalną wartość
        public static async Task<int> RunBenchmark(string apiUrl, int testRequests = 200)
        {
            Console.WriteLine($"{LogTs()} [BENCHMARK] Rozpoczynam benchmark API...");
            Console.WriteLine($"{LogTs()} [BENCHMARK] Testuję różne poziomy równoległości:");
            Console.WriteLine($"{LogTs()} [BENCHMARK]   - Poziomy 1-5: {testRequests / 2} próbek (50%)");
            Console.WriteLine($"{LogTs()} [BENCHMARK]   - Poziomy 6+: {testRequests} próbek (100%)");
            Console.WriteLine($"{LogTs()} [BENCHMARK] To może zająć kilka minut, proszę czekać...");
            
            // Testowe dane do wysłania
            var testData = new RowData
            {
                StreetName = "Test",
                Prefix = "",
                BuildingNo = "1",
                City = "Warszawa",
                PostalCode = "00-001"
            };

            // Testujemy od 1 do 10, a potem wyższe poziomy
            var levels = new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 50, 75, 100 };
            var results = new Dictionary<int, BenchmarkResult>();

            foreach (var level in levels)
            {
                // Określamy liczbę próbek w zależności od poziomu równoległości
                int actualTestRequests;
                if (level <= 5)
                {
                    // Dla poziomów 1-5 używamy 50% próbek (100 próbek jeśli testRequests = 200)
                    actualTestRequests = testRequests / 2;
                }
                else
                {
                    // Dla poziomów 6+ używamy 100% próbek (200 próbek jeśli testRequests = 200)
                    actualTestRequests = testRequests;
                }
                
                Console.WriteLine($"{LogTs()} [BENCHMARK] Testuję równoległość = {level} ({actualTestRequests} próbek)...");
                var result = await BenchmarkLevel(apiUrl, testData, level, actualTestRequests);
                results[level] = result;
                
                var statusIndicator = result.ErrorRate > 10 ? "⚠️" : result.ErrorRate > 0 ? "⚡" : "✓";
                var totalTested = result.SuccessCount + result.ErrorCount;
                var timeFor100K = FormatTimeFor100KStatic(result.RequestsPerSecond);
                Console.WriteLine($"{LogTs()} [BENCHMARK]   {statusIndicator} Wynik: {result.RequestsPerSecond:F2} req/s, średni czas: {result.AverageLatencyMs:F0}ms, sukces: {result.SuccessCount}/{totalTested} ({100.0 - result.ErrorRate:F1}%), 100k req: ~{timeFor100K}");
                
                if (result.ErrorCount > 0)
                {
                    var errorDetails = new List<string>();
                    if (result.TimeoutCount > 0) errorDetails.Add($"{result.TimeoutCount} timeoutów");
                    if (result.HttpErrorCount > 0) errorDetails.Add($"{result.HttpErrorCount} błędów HTTP");
                    if (result.OtherErrorCount > 0) errorDetails.Add($"{result.OtherErrorCount} innych błędów");
                    
                    Console.WriteLine($"{LogTs()} [BENCHMARK]     Błędy: {string.Join(", ", errorDetails)}");
                }
            }

            // Znajdź optymalny poziom (najwyższa przepustowość z rozsądnym opóźnieniem)
            var optimal = FindOptimalParallelism(results);
            
            // Zapisz wyniki pierwszej iteracji
            var firstIterationResults = new Dictionary<int, BenchmarkResult>(results);
            
            // Druga iteracja - szczegółowe testowanie 5 najbardziej efektywnych poziomów (najszybsze dla 100k requestów)
            Console.WriteLine($"{LogTs()} [BENCHMARK] ");
            Console.WriteLine($"{LogTs()} [BENCHMARK] ========== DRUGA ITERACJA - SZCZEGÓŁOWA ANALIZA TOP 5 ==========");
            
            var secondIterationResults = await RunDetailedBenchmarkTop5(apiUrl, testData, firstIterationResults);
            
            // Łączymy wyniki z obu iteracji
            foreach (var kvp in secondIterationResults)
            {
                results[kvp.Key] = kvp.Value;
            }
            
            // Ponowna analiza z dokładniejszymi danymi
            optimal = FindOptimalParallelism(results);
            
            var recommendedResult = results[optimal.Recommended];

            // Wyświetl szczegółową tabelę porównawczą wszystkich poziomów (z obu iteracji)
            Console.WriteLine($"{LogTs()} [BENCHMARK] ========== SZCZEGÓŁOWA TABELA PORÓWNAWCZA (I + II iteracja) ==========");
            Console.WriteLine($"{LogTs()} [BENCHMARK] {"Równoległość",-15} {"Req/s",-12} {"Średni",-10} {"Min",-10} {"Max",-10} {"Błędy",-10} {"Efektywność",-12} {"100k req",-15} {"Status"}");
            Console.WriteLine($"{LogTs()} [BENCHMARK] {"",-15} {"",-12} {"czas (ms)",-10} {"(ms)",-10} {"(ms)",-10} {"%",-10} {"(req/s/ms)",-12} {"(czas)",-15} {""}");
            Console.WriteLine($"{LogTs()} [BENCHMARK] {new string('-', 120)}");
            
            // Wyświetlamy wszystkie wyniki posortowane po równoległości
            var allResults = results.Values.OrderBy(r => r.Parallelism).ToList();
            foreach (var result in allResults)
            {
                // Obliczamy efektywność: req/s per ms (im wyższe, tym lepiej)
                var efficiency = result.AverageLatencyMs > 0 ? result.RequestsPerSecond / result.AverageLatencyMs * 1000 : 0;
                
                var statusIcon = result.ErrorRate > 10 ? "⚠️" : result.ErrorRate > 5 ? "⚡" : result.ErrorRate > 0 ? "✓" : "✓";
                var statusText = result.ErrorRate > 10 ? "RYZYKO" : result.ErrorRate > 5 ? "OK" : result.ErrorRate > 0 ? "OK" : "IDEAL";
                
                // Oznaczamy wyniki z drugiej iteracji
                var iterationMark = firstIterationResults.ContainsKey(result.Parallelism) ? "   " : "[II]";
                var parallelismDisplay = firstIterationResults.ContainsKey(result.Parallelism) 
                    ? result.Parallelism.ToString() 
                    : $"{result.Parallelism}";
                
                var timeFor100K = FormatTimeFor100KStatic(result.RequestsPerSecond);
                
                Console.WriteLine($"{LogTs()} [BENCHMARK] {iterationMark} {parallelismDisplay,-12} {result.RequestsPerSecond,-12:F2} {result.AverageLatencyMs,-10:F0} {result.MinLatencyMs,-10:F0} {result.MaxLatencyMs,-10:F0} {result.ErrorRate,-10:F1}% {efficiency,-12:F2} {timeFor100K,-15} {statusIcon} {statusText}");
            }
            
            Console.WriteLine($"{LogTs()} [BENCHMARK] {new string('-', 120)}");
            Console.WriteLine($"{LogTs()} [BENCHMARK] Efektywność = (Req/s / Średni czas) × 1000 - im wyższa, tym lepiej");
            Console.WriteLine($"{LogTs()} [BENCHMARK] 100k req = szacowany czas wykonania 100 000 requestów przy danej przepustowości");
            Console.WriteLine($"{LogTs()} [BENCHMARK] ");
            
            // Funkcja pomocnicza do obliczania efektywności
            static double CalculateEfficiency(BenchmarkResult r) => 
                r.AverageLatencyMs > 0 ? (r.RequestsPerSecond / r.AverageLatencyMs) * 1000 : 0;

            // Analiza trendów - porównanie wzrostu przepustowości vs wzrostu opóźnień z naciskiem na efektywność
            Console.WriteLine($"{LogTs()} [BENCHMARK] ========== ANALIZA TRENDÓW (Przepustowość vs Opóźnienia - FOKUS NA EFEKTYWNOŚCI) ==========");
            var sortedResults = results.Values.Where(r => r.SuccessCount > 0).OrderBy(r => r.Parallelism).ToList();
            
            if (sortedResults.Count > 1)
            {
                Console.WriteLine($"{LogTs()} [BENCHMARK] {"Z",-5} {"Na",-5} {"Efektywność",-13} {"Δ Efektywność",-15} {"Δ Req/s",-12} {"Δ Opóźnienie",-15} {"Korzyść",-12} {"Komentarz"}");
                Console.WriteLine($"{LogTs()} [BENCHMARK] {new string('-', 100)}");
                
                for (int i = 1; i < sortedResults.Count; i++)
                {
                    var prev = sortedResults[i - 1];
                    var curr = sortedResults[i];
                    
                    var prevEfficiency = CalculateEfficiency(prev);
                    var currEfficiency = CalculateEfficiency(curr);
                    var efficiencyDelta = currEfficiency - prevEfficiency;
                    var efficiencyDeltaPercent = prevEfficiency > 0 ? (efficiencyDelta / prevEfficiency) * 100 : 0;
                    
                    var throughputDelta = curr.RequestsPerSecond - prev.RequestsPerSecond;
                    var latencyDelta = curr.AverageLatencyMs - prev.AverageLatencyMs;
                    var latencyDeltaPercent = prev.AverageLatencyMs > 0 ? (latencyDelta / prev.AverageLatencyMs) * 100 : 0;
                    var throughputDeltaPercent = prev.RequestsPerSecond > 0 ? (throughputDelta / prev.RequestsPerSecond) * 100 : 0;
                    
                    // Korzyść = procentowy wzrost przepustowości vs procentowy wzrost opóźnień
                    var benefit = latencyDeltaPercent > 0 ? throughputDeltaPercent / latencyDeltaPercent : double.MaxValue;
                    
                    string comment;
                    if (efficiencyDelta < 0)
                        comment = "🔴 EFEKTYWNOŚĆ SPADA";
                    else if (efficiencyDeltaPercent > 5)
                        comment = "🟢 EFEKTYWNOŚĆ znacząco rośnie";
                    else if (efficiencyDeltaPercent > 0)
                        comment = "🟡 EFEKTYWNOŚĆ rośnie";
                    else if (throughputDelta < 0)
                        comment = "⚠️ Przepustowość SPADA";
                    else if (latencyDeltaPercent > 50 && throughputDeltaPercent < 10)
                        comment = "⚠️ Opóźnienia ROSNĄ dużo szybciej niż przepustowość";
                    else if (latencyDeltaPercent > throughputDeltaPercent * 2)
                        comment = "⚠️ Opóźnienia rosną 2x szybciej";
                    else if (benefit > 2)
                        comment = "✓ Dobry kompromis";
                    else if (benefit > 1)
                        comment = "⚡ Akceptowalny";
                    else
                        comment = "⚠️ Słaby kompromis";
                    
                    Console.WriteLine($"{LogTs()} [BENCHMARK] {prev.Parallelism,-5} {curr.Parallelism,-5} {currEfficiency,-13:F2} {efficiencyDelta:+0.00;-0.00;0.00} ({efficiencyDeltaPercent:+0.0;-0.0;0.0}%) {throughputDelta:+0.00;-0.00;0.00} ({throughputDeltaPercent:+0.0;-0.0;0.0}%) {latencyDelta:+0.0;-0.0;0.0}ms ({latencyDeltaPercent:+0.0;-0.0;0.0}%) {benefit:F2}x {comment}");
                }
                
                Console.WriteLine($"{LogTs()} [BENCHMARK] {new string('-', 100)}");
                Console.WriteLine($"{LogTs()} [BENCHMARK] EFEKTYWNOŚĆ = (Req/s / Średni czas) × 1000 - im wyższa, tym lepiej ⭐");
                Console.WriteLine($"{LogTs()} [BENCHMARK] Korzyść = (Wzrost przepustowości %) / (Wzrost opóźnień %) - im wyższa, tym lepiej");
                Console.WriteLine($"{LogTs()} [BENCHMARK] ");
                
                // Znajdź punkt gdzie korzyść jest najlepsza i gdzie zaczyna spadać
                var benefitData = sortedResults.Skip(1).Select((curr, idx) =>
                {
                    var prev = sortedResults[idx];
                    var throughputDeltaPercent = prev.RequestsPerSecond > 0 ? ((curr.RequestsPerSecond - prev.RequestsPerSecond) / prev.RequestsPerSecond) * 100 : 0;
                    var latencyDeltaPercent = prev.AverageLatencyMs > 0 ? ((curr.AverageLatencyMs - prev.AverageLatencyMs) / prev.AverageLatencyMs) * 100 : 0;
                    var benefit = latencyDeltaPercent > 0 ? throughputDeltaPercent / latencyDeltaPercent : double.MaxValue;
                    return (FromLevel: prev.Parallelism, ToLevel: curr.Parallelism, Benefit: benefit);
                }).Where(b => b.Benefit < double.MaxValue).ToList();
                
                var bestBenefit = benefitData.OrderByDescending(b => b.Benefit).FirstOrDefault();
                var worstBenefitAfter = benefitData.Where(b => b.Benefit < 1.0 && b.FromLevel > (bestBenefit.FromLevel > 0 ? bestBenefit.FromLevel : 10)).FirstOrDefault();
                
                if (bestBenefit.FromLevel > 0)
                {
                    Console.WriteLine($"{LogTs()} [BENCHMARK] 💡 Najlepszy kompromis: {bestBenefit.FromLevel} → {bestBenefit.ToLevel} równoległych requestów (korzyść: {bestBenefit.Benefit:F2}x)");
                }
                
                if (worstBenefitAfter.FromLevel > 0)
                {
                    Console.WriteLine($"{LogTs()} [BENCHMARK] ⚠️  Od {worstBenefitAfter.FromLevel} → {worstBenefitAfter.ToLevel} korzyść spada poniżej 1.0x - opóźnienia rosną szybciej niż przepustowość");
                    Console.WriteLine($"{LogTs()} [BENCHMARK]    Rozważ zatrzymanie się na poziomie {worstBenefitAfter.FromLevel} lub niższym");
                }
            }
            
            Console.WriteLine($"{LogTs()} [BENCHMARK] ");

            // Znajdź najlepszą efektywność
            var bestEfficiencyResult = results.Values
                .Where(r => r.SuccessCount > 0)
                .OrderByDescending(r => CalculateEfficiency(r))
                .FirstOrDefault();

            // Rekomendacje
            Console.WriteLine($"{LogTs()} [BENCHMARK] ========== REKOMENDACJE ==========");
            Console.WriteLine($"{LogTs()} [BENCHMARK] ⭐ Najlepsza EFEKTYWNOŚĆ: {CalculateEfficiency(bestEfficiencyResult ?? recommendedResult):F2} przy {bestEfficiencyResult?.Parallelism ?? optimal.Recommended} równoległych requestach");
            if (bestEfficiencyResult != null)
            {
                Console.WriteLine($"{LogTs()} [BENCHMARK]   - Przepustowość: {bestEfficiencyResult.RequestsPerSecond:F2} req/s");
                Console.WriteLine($"{LogTs()} [BENCHMARK]   - Średni czas: {bestEfficiencyResult.AverageLatencyMs:F0}ms");
                Console.WriteLine($"{LogTs()} [BENCHMARK]   - Błędy: {bestEfficiencyResult.ErrorRate:F1}%");
            }
            
            Console.WriteLine($"{LogTs()} [BENCHMARK] ");
            Console.WriteLine($"{LogTs()} [BENCHMARK] Najlepsza przepustowość: {optimal.BestThroughput.RequestsPerSecond:F2} req/s przy {optimal.BestThroughput.Parallelism} równoległych requestach");
            Console.WriteLine($"{LogTs()} [BENCHMARK]   - Efektywność: {CalculateEfficiency(optimal.BestThroughput):F2}");
            Console.WriteLine($"{LogTs()} [BENCHMARK]   - Średni czas: {optimal.BestThroughput.AverageLatencyMs:F0}ms");
            Console.WriteLine($"{LogTs()} [BENCHMARK]   - Błędy: {optimal.BestThroughput.ErrorRate:F1}%");
            
            Console.WriteLine($"{LogTs()} [BENCHMARK] Najlepsze opóźnienie: {optimal.BestLatency.AverageLatencyMs:F0}ms przy {optimal.BestLatency.Parallelism} równoległych requestach");
            Console.WriteLine($"{LogTs()} [BENCHMARK]   - Efektywność: {CalculateEfficiency(optimal.BestLatency):F2}");
            Console.WriteLine($"{LogTs()} [BENCHMARK]   - Przepustowość: {optimal.BestLatency.RequestsPerSecond:F2} req/s");
            Console.WriteLine($"{LogTs()} [BENCHMARK]   - Błędy: {optimal.BestLatency.ErrorRate:F1}%");
            
            Console.WriteLine($"{LogTs()} [BENCHMARK] ");
            Console.WriteLine($"{LogTs()} [BENCHMARK] ⭐⭐ REKOMENDOWANA wartość MaxParallelRequests: {optimal.Recommended} (wybrana na podstawie EFEKTYWNOŚCI)");
            Console.WriteLine($"{LogTs()} [BENCHMARK] Oczekiwana przepustowość: {recommendedResult.RequestsPerSecond:F2} req/s");
            Console.WriteLine($"{LogTs()} [BENCHMARK] Oczekiwane średnie opóźnienie: {recommendedResult.AverageLatencyMs:F0}ms (min: {recommendedResult.MinLatencyMs:F0}ms, max: {recommendedResult.MaxLatencyMs:F0}ms)");
            Console.WriteLine($"{LogTs()} [BENCHMARK] ⭐ EFEKTYWNOŚĆ: {CalculateEfficiency(recommendedResult):F2}");
            Console.WriteLine($"{LogTs()} [BENCHMARK] ⏱️  Szacowany czas dla 100 000 requestów: ~{FormatTimeFor100KStatic(recommendedResult.RequestsPerSecond)}");
            
            var totalTestedForRecommended = recommendedResult.SuccessCount + recommendedResult.ErrorCount;
            Console.WriteLine($"{LogTs()} [BENCHMARK] Oczekiwana liczba błędów: {recommendedResult.ErrorRate:F1}% ({recommendedResult.ErrorCount} błędów na {totalTestedForRecommended} requestów)");
            
            if (recommendedResult.ErrorCount > 0)
            {
                Console.WriteLine($"{LogTs()} [BENCHMARK]   Szczegóły błędów: {recommendedResult.TimeoutCount} timeoutów, {recommendedResult.HttpErrorCount} błędów HTTP, {recommendedResult.OtherErrorCount} innych");
            }
            
            if (recommendedResult.ErrorRate > 5.0)
            {
                Console.WriteLine($"{LogTs()} [BENCHMARK] ⚠️  OSTRZEŻENIE: Rekomendowany poziom ma >5% błędów. Rozważ użycie niższej wartości MaxParallelRequests.");
            }
            
            Console.WriteLine($"{LogTs()} [BENCHMARK] ========================================");

            return optimal.Recommended;
        }

        // Funkcja pomocnicza do formatowania czasu dla 100k requestów
        private static string FormatTimeFor100KStatic(double requestsPerSecond)
        {
            if (requestsPerSecond <= 0) return "N/A";
            var totalSeconds = 100000.0 / requestsPerSecond;
            var hours = (int)(totalSeconds / 3600);
            var minutes = (int)((totalSeconds % 3600) / 60);
            var seconds = (int)(totalSeconds % 60);
            
            if (hours > 0)
                return $"{hours}h {minutes}m {seconds}s";
            else if (minutes > 0)
                return $"{minutes}m {seconds}s";
            else
                return $"{seconds}s";
        }

        // Druga iteracja benchmarka - szczegółowe testowanie 5 najbardziej efektywnych poziomów (najszybsze dla 100k requestów)
        private static async Task<Dictionary<int, BenchmarkResult>> RunDetailedBenchmarkTop5(
            string apiUrl, 
            RowData testData, 
            Dictionary<int, BenchmarkResult> firstIterationResults)
        {
            var detailedResults = new Dictionary<int, BenchmarkResult>();
            
            // Funkcja pomocnicza do obliczania czasu dla 100k requestów (w sekundach)
            static double GetTimeFor100K(BenchmarkResult r)
            {
                if (r.RequestsPerSecond <= 0) return double.MaxValue;
                return 100000.0 / r.RequestsPerSecond;
            }

            // Wybieramy 5 najbardziej efektywnych poziomów pod względem czasu dla 100k requestów (najszybsze)
            var top5Levels = firstIterationResults.Values
                .Where(r => r.SuccessCount > 0 && r.ErrorRate <= 10.0)
                .OrderBy(r => GetTimeFor100K(r))
                .Take(5)
                .Select(r => r.Parallelism)
                .ToList();

            if (!top5Levels.Any())
            {
                Console.WriteLine($"{LogTs()} [BENCHMARK] ⚠️  Nie znaleziono odpowiednich poziomów do szczegółowej analizy.");
                return detailedResults;
            }

            Console.WriteLine($"{LogTs()} [BENCHMARK] Wybrano 5 najbardziej efektywnych poziomów (najszybsze dla 100k requestów): {string.Join(", ", top5Levels)}");
            Console.WriteLine($"{LogTs()} [BENCHMARK] Dla każdego poziomu wykonam szczegółowy test z 1000 requestami");
            Console.WriteLine($"{LogTs()} [BENCHMARK] ");

            const int detailedTestRequests = 1000;

            foreach (var level in top5Levels)
            {
                Console.WriteLine($"{LogTs()} [BENCHMARK] [II] Testuję równoległość = {level} ({detailedTestRequests} próbek)...");
                var result = await BenchmarkLevel(apiUrl, testData, level, detailedTestRequests);
                detailedResults[level] = result;
                
                var statusIndicator = result.ErrorRate > 10 ? "⚠️" : result.ErrorRate > 0 ? "⚡" : "✓";
                var totalTested = result.SuccessCount + result.ErrorCount;
                
                // Oblicz czas dla 100k requestów
                var timeFor100K = FormatTimeFor100KStatic(result.RequestsPerSecond);
                Console.WriteLine($"{LogTs()} [BENCHMARK] [II]   {statusIndicator} Wynik: {result.RequestsPerSecond:F2} req/s, średni czas: {result.AverageLatencyMs:F0}ms, sukces: {result.SuccessCount}/{totalTested} ({100.0 - result.ErrorRate:F1}%), 100k req: ~{timeFor100K}");
                
                if (result.ErrorCount > 0)
                {
                    var errorDetails = new List<string>();
                    if (result.TimeoutCount > 0) errorDetails.Add($"{result.TimeoutCount} timeoutów");
                    if (result.HttpErrorCount > 0) errorDetails.Add($"{result.HttpErrorCount} błędów HTTP");
                    if (result.OtherErrorCount > 0) errorDetails.Add($"{result.OtherErrorCount} innych błędów");
                    
                    Console.WriteLine($"{LogTs()} [BENCHMARK] [II]     Błędy: {string.Join(", ", errorDetails)}");
                }
            }
            
            Console.WriteLine($"{LogTs()} [BENCHMARK] Druga iteracja zakończona - przetestowano {detailedResults.Count} poziomów z 1000 requestami każdy");
            Console.WriteLine($"{LogTs()} [BENCHMARK] ");
            
            return detailedResults;
        }

        private static async Task<BenchmarkResult> BenchmarkLevel(string apiUrl, RowData testData, int parallelism, int totalRequests)
        {
            var semaphore = new SemaphoreSlim(parallelism);
            var tasks = new List<Task<BenchmarkRequestResult>>();
            var startTime = DateTime.UtcNow;

            for (int i = 0; i < totalRequests; i++)
            {
                tasks.Add(BenchmarkSingleRequest(apiUrl, testData, semaphore));
            }

            var results = await Task.WhenAll(tasks);
            var endTime = DateTime.UtcNow;
            var duration = (endTime - startTime).TotalSeconds;

            // Filtrujemy tylko poprawne odpowiedzi do obliczeń przepustowości
            var successfulResults = results.Where(r => r.IsSuccess).ToList();
            var successCount = successfulResults.Count;
            var errorCount = results.Length - successCount;
            var errorRate = (double)errorCount / results.Length * 100.0;

            // Obliczamy przepustowość tylko dla poprawnych requestów
            var requestsPerSecond = successCount / duration;
            var averageLatency = successfulResults.Any() ? successfulResults.Select(r => r.LatencyMs).Average() : 0;
            var minLatency = successfulResults.Any() ? successfulResults.Min(r => r.LatencyMs) : 0;
            var maxLatency = successfulResults.Any() ? successfulResults.Max(r => r.LatencyMs) : 0;

            // Śledzenie typów błędów
            var timeoutCount = results.Count(r => r.IsTimeout);
            var httpErrorCount = results.Count(r => r.IsHttpError);
            var otherErrorCount = results.Count(r => !r.IsSuccess && !r.IsTimeout && !r.IsHttpError);

            return new BenchmarkResult
            {
                Parallelism = parallelism,
                RequestsPerSecond = requestsPerSecond,
                AverageLatencyMs = averageLatency,
                MinLatencyMs = minLatency,
                MaxLatencyMs = maxLatency,
                SuccessCount = successCount,
                ErrorCount = errorCount,
                ErrorRate = errorRate,
                TimeoutCount = timeoutCount,
                HttpErrorCount = httpErrorCount,
                OtherErrorCount = otherErrorCount
            };
        }

        private static async Task<BenchmarkRequestResult> BenchmarkSingleRequest(string apiUrl, RowData testData, SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            try
            {
                var startTime = DateTime.UtcNow;
                
                try
                {
                    var result = await GetResultForRow(testData, apiUrl);
                    var elapsed = (DateTime.UtcNow - startTime).TotalMilliseconds;
                    
                    // Sprawdzamy czy odpowiedź jest poprawna (nie jest pusta i ma dane)
                    var isSuccess = result != null && result.Address != null;
                    
                    return new BenchmarkRequestResult
                    {
                        IsSuccess = isSuccess,
                        IsTimeout = false,
                        IsHttpError = !isSuccess,
                        LatencyMs = (long)elapsed
                    };
                }
                catch (TaskCanceledException) when (DateTime.UtcNow - startTime > TimeSpan.FromMinutes(4))
                {
                    // Timeout (HttpClient ma timeout 5 minut, więc jeśli upłynęło >4 min, to prawdopodobnie timeout)
                    var elapsed = (DateTime.UtcNow - startTime).TotalMilliseconds;
                    return new BenchmarkRequestResult
                    {
                        IsSuccess = false,
                        IsTimeout = true,
                        IsHttpError = false,
                        LatencyMs = (long)elapsed
                    };
                }
                catch (System.Net.Http.HttpRequestException)
                {
                    // Błąd HTTP (połączenie, timeout sieciowy, itp.)
                    var elapsed = (DateTime.UtcNow - startTime).TotalMilliseconds;
                    return new BenchmarkRequestResult
                    {
                        IsSuccess = false,
                        IsTimeout = false,
                        IsHttpError = true,
                        LatencyMs = (long)elapsed
                    };
                }
                catch
                {
                    // Inny błąd
                    var elapsed = (DateTime.UtcNow - startTime).TotalMilliseconds;
                    return new BenchmarkRequestResult
                    {
                        IsSuccess = false,
                        IsTimeout = false,
                        IsHttpError = false,
                        LatencyMs = (long)elapsed
                    };
                }
            }
            finally
            {
                semaphore.Release();
            }
        }

        // Wykonuje zapytanie API dla benchmarku (uproszczona wersja GetResultForRow z TestEngine)
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

                var requestContent = JsonContent.Create(body, options: JsonOptions);
                using var request = new HttpRequestMessage(HttpMethod.Post, endpoint)
                {
                    Content = requestContent
                };
                var response = await SharedHttpClient.SendAsync(request, System.Net.Http.HttpCompletionOption.ResponseHeadersRead, CancellationToken.None);
                
                if (!response.IsSuccessStatusCode)
                {
                    return new NormalizationApiResponseDto();
                }

                using var responseStream = await response.Content.ReadAsStreamAsync();
                var responseObject = await JsonSerializer.DeserializeAsync<NormalizationApiResponseDto>(responseStream, JsonOptions);
                
                return responseObject ?? new NormalizationApiResponseDto();
            }
            catch
            {
                return new NormalizationApiResponseDto();
            }
        }

        private static (BenchmarkResult BestThroughput, BenchmarkResult BestLatency, int Recommended) FindOptimalParallelism(Dictionary<int, BenchmarkResult> results)
        {
            // Funkcja pomocnicza do obliczania efektywności
            static double CalculateEfficiency(BenchmarkResult r) => 
                r.AverageLatencyMs > 0 ? (r.RequestsPerSecond / r.AverageLatencyMs) * 1000 : 0;

            // Filtrujemy wyniki z za dużą liczbą błędów (>5% błędów) - nie są bezpieczne
            var reliableResults = results.Values.Where(r => r.ErrorRate <= 5.0 && r.SuccessCount > 0).ToList();
            
            if (!reliableResults.Any())
            {
                // Jeśli wszystkie mają błędy, używamy tego z najmniejszą liczbą błędów
                reliableResults = results.Values
                    .Where(r => r.SuccessCount > 0)
                    .OrderBy(r => r.ErrorRate)
                    .Take(3)
                    .ToList();
                Console.WriteLine($"{LogTs()} [BENCHMARK] ⚠️  OSTRZEŻENIE: Wszystkie poziomy równoległości mają błędy! Używam poziomów z najmniejszą liczbą błędów.");
            }

            var bestThroughput = reliableResults.OrderByDescending(r => r.RequestsPerSecond).First();
            var bestLatency = reliableResults.OrderBy(r => r.AverageLatencyMs).First();
            var bestEfficiency = reliableResults.OrderByDescending(r => CalculateEfficiency(r)).First();

            // Rekomendacja: PRIORYTETEM JEST EFEKTYWNOŚĆ
            // Wybieramy poziom z najwyższą efektywnością, ale z dodatkowymi filtrami:
            // - Error rate <= 5% (bezpieczny poziom)
            // - Efektywność >= 95% najlepszej efektywności (aby nie wybrać znacznie gorszych)
            var thresholdEfficiency = CalculateEfficiency(bestEfficiency) * 0.95;

            var candidates = reliableResults
                .Where(r => CalculateEfficiency(r) >= thresholdEfficiency && r.ErrorRate <= 5.0)
                .OrderByDescending(r => CalculateEfficiency(r))
                .ThenByDescending(r => r.RequestsPerSecond)
                .ThenBy(r => r.ErrorRate)
                .ThenBy(r => r.Parallelism);

            var recommended = candidates.FirstOrDefault() ?? bestEfficiency;

            return (bestThroughput, bestLatency, recommended.Parallelism);
        }

        private static string LogTs() => DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

        // Klasy pomocnicze dla benchmarka
        private class BenchmarkRequestResult
        {
            public bool IsSuccess { get; set; }
            public bool IsTimeout { get; set; }
            public bool IsHttpError { get; set; }
            public long LatencyMs { get; set; }
        }

        private class BenchmarkResult
        {
            public int Parallelism { get; set; }
            public double RequestsPerSecond { get; set; }
            public double AverageLatencyMs { get; set; }
            public double MinLatencyMs { get; set; }
            public double MaxLatencyMs { get; set; }
            public int SuccessCount { get; set; }
            public int ErrorCount { get; set; }
            public double ErrorRate { get; set; }
            public int TimeoutCount { get; set; }
            public int HttpErrorCount { get; set; }
            public int OtherErrorCount { get; set; }
        }

        // Klasy DTO dla odpowiedzi API (używane przez benchmark)
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

