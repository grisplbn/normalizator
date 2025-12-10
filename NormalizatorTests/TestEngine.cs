using ClosedXML.Excel;
using System.Collections.Concurrent;
using System.Data;
using System.Net.Http.Json;
using System.Threading;
using Npgsql;

namespace NormalizatorTests
{
    internal class TestEngine
    {
        // Główny przebieg testów dla API: odczyt danych z Excela, wywołanie API,
        // zapis wyników i wizualne oznaczenie poprawności.
        public static async Task RunApiTest(string originalFilePath, string resultFilePath, string apiUrl, decimal probabilityThreshold, int maxParallelRequests)
        {
            using var workbook = new XLWorkbook(originalFilePath);
            var sheet = workbook.Worksheet(1);

            var rowsNo = sheet.Rows().Count();
            Console.WriteLine($"{LogTs()} [API] Start: wiersze={rowsNo - 1}, próg={probabilityThreshold}, równoległość={maxParallelRequests}");

            // Tworzymy dodatkowe kolumny na wyniki, aby nie nadpisywać oczekiwanych wartości.
            AddResultColumns(sheet);

            var indexes = GetColumnIndexes(sheet);

            // Kontrolujemy równoległość zapytań, aby nie przeciążyć usługi.
            var semaphore = new SemaphoreSlim(maxParallelRequests);
            var tasks = new List<Task>();
            var results = new ConcurrentDictionary<int, NormalizationApiResponseDto>();
            var totalRows = rowsNo - 1;
            var processed = 0;
            for (int i = 2; i <= rowsNo; i++)
            {
                tasks.Add(ProcessRow(
                    sheet,
                    i,
                    indexes,
                    apiUrl,
                    probabilityThreshold,
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

            await Task.WhenAll(tasks);

            // Po zebraniu odpowiedzi wpisujemy wyniki i kolorujemy komórki
            // (zielony = zgodność, czerwony = różnica).
            for (int i = 2; i <= rowsNo; i++)
            {
                var result = results[i];
                if (result.NormalizationMetadata?.CombinedProbability >= probabilityThreshold && result.Address != null)
                {
                    WriteRowResult(sheet, i, indexes, result.Address);
                }

                SetBackroundColor(sheet, i);
            }

            workbook.SaveAs(resultFilePath);
            Console.WriteLine($"{LogTs()} [API] Zakończono: zapisano do {resultFilePath}");
        }

        // Główny przebieg testów dla DB: odczyt danych z Excela, zapytania SQL i zapis wielowynikowy.
        public static async Task RunDbTest(string originalFilePath, string dbResultFilePath, string connectionString, string query, int maxParallelRequests, IDictionary<string, string> dbMapping)
        {
            using var workbook = new XLWorkbook(originalFilePath);
            var sheet = workbook.Worksheet(1);

            var indexes = GetColumnIndexes(sheet);
            var rowsNo = sheet.Rows().Count();

            Console.WriteLine($"{LogTs()} [DB] Start: wiersze={rowsNo - 1}, równoległość={maxParallelRequests}");

            var semaphore = new SemaphoreSlim(maxParallelRequests);
            var tasks = new List<Task>();
            var results = new ConcurrentDictionary<int, List<ApiResponseAddressDto>>();
            var totalRows = rowsNo - 1;
            var processed = 0;

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
                    () =>
                    {
                        var done = Interlocked.Increment(ref processed);
                        if (done % 50 == 0 || done == totalRows || done <= 5)
                        {
                            Console.WriteLine($"{LogTs()} [DB] Postęp: {done}/{totalRows}");
                        }
                    }));
            }

            await Task.WhenAll(tasks);

            var maxResults = results.Values.DefaultIfEmpty(new List<ApiResponseAddressDto>()).Max(r => r.Count);
            if (maxResults > 0)
            {
                var startColumn = sheet.Columns().Count() + 1;
                EnsureDbResultHeaders(sheet, startColumn, maxResults);

                for (int i = 2; i <= rowsNo; i++)
                {
                    results.TryGetValue(i, out var rowResults);
                    WriteDbRowResults(sheet, i, startColumn, rowResults ?? new List<ApiResponseAddressDto>());
                }
            }

            workbook.SaveAs(dbResultFilePath);
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

        // Przetwarza pojedynczy wiersz: pobiera wynik z API i zapisuje go w pamięci współdzielonej.
        private static async Task ProcessRow(
            IXLWorksheet sheet, 
            int i, 
            ColumnIndexes indexes, 
            string apiUrl, 
            decimal probabilityThreshold,
            SemaphoreSlim semaphore,
            ConcurrentDictionary<int, NormalizationApiResponseDto> results,
            Action progressCallback)
        {
            await semaphore.WaitAsync();

            try
            {
                var result = await GetResultForRow(sheet, i, indexes, apiUrl);
                results.TryAdd(i, result);
            }
            catch(Exception ex)
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
            Action progressCallback)
        {
            await semaphore.WaitAsync();

            try
            {
                var rowResults = await GetDbResultsForRow(sheet, i, indexes, connectionString, query, dbMapping);
                results.TryAdd(i, rowResults);
            }
            catch
            {
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
                        sheet.Cell(rowNo, columnsNo).Value = color == XLColor.Red ? 0 : 1;
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
            var fieldsPerResult = DbResultFieldOrder.Length;

            for (int idx = 0; idx < results.Count; idx++)
            {
                var baseCol = startColumn + idx * fieldsPerResult;
                var r = results[idx];
                sheet.Cell(rowNo, baseCol + 0).Value = r.StreetPrefix;
                sheet.Cell(rowNo, baseCol + 1).Value = r.StreetName;
                sheet.Cell(rowNo, baseCol + 2).Value = r.BuildingNumber;
                sheet.Cell(rowNo, baseCol + 3).Value = r.City;
                sheet.Cell(rowNo, baseCol + 4).Value = r.PostalCode;
                sheet.Cell(rowNo, baseCol + 5).Value = r.Commune;
                sheet.Cell(rowNo, baseCol + 6).Value = r.District;
                sheet.Cell(rowNo, baseCol + 7).Value = r.Province;
            }
        }

        // Wpisuje wartości zwrócone przez API do kolumn wynikowych (RESULT*).
        private static void WriteRowResult(IXLWorksheet sheet, int rowNo, ColumnIndexes idx, ApiResponseAddressDto result)
        {
            sheet.Cell(rowNo, idx.ResultBuildingNoIndex).Value = result.BuildingNumber;
            sheet.Cell(rowNo, idx.ResultCityIndex).Value = result.City;
            sheet.Cell(rowNo, idx.ResultCommuneIndex).Value = result.Commune;
            sheet.Cell(rowNo, idx.ResultDistrictIndex).Value = result.District;
            sheet.Cell(rowNo, idx.ResultPostalCodeIndex).Value = result.PostalCode;
            sheet.Cell(rowNo, idx.ResultProvinceIndex).Value = result.Province;
            sheet.Cell(rowNo, idx.ResultStreetIndex).Value = result.StreetName;
            sheet.Cell(rowNo, idx.ResultPrefixIndex).Value = result.StreetPrefix;
        }

        // Pobiera wartości z wiersza, sanitizuje "null" na puste ciągi i wysyła zapytanie POST do API.
        private static async Task<NormalizationApiResponseDto> GetResultForRow(IXLWorksheet sheet, int rowNo, ColumnIndexes idx, string endpoint)
        {
            try
            {
                var streetName = sheet.Cell(rowNo, idx.RequestStreetIndex).Value.ToString();
                var prefix = sheet.Cell(rowNo, idx.RequestPrefixIndex).Value.ToString();
                var buildingNo = sheet.Cell(rowNo, idx.RequestBuildingNoIndex).Value.ToString();
                var city = sheet.Cell(rowNo, idx.RequestCityIndex).Value.ToString();
                var postalCode = sheet.Cell(rowNo, idx.RequestPostalCodeIndex).Value.ToString();

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

                var body = new
                {
                    StreetName = streetName,
                    StreetPrefix = prefix,
                    BuildingNumber = buildingNo,
                    City = city,
                    PostalCode = postalCode
                };

                using (var client = new HttpClient())
                {
                    var response = await client.PostAsJsonAsync(endpoint, body);
                    if (!response.IsSuccessStatusCode)
                    {
                        // Zwracamy pustą odpowiedź, aby przetwarzanie innych wierszy mogło trwać dalej.
                        return new NormalizationApiResponseDto();
                    }

                    var responseObject = await response.Content.ReadFromJsonAsync<NormalizationApiResponseDto>();
                    if (responseObject is null)
                    {
                        return new NormalizationApiResponseDto();
                    }

                    return responseObject;
                }
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
            while (await reader.ReadAsync())
            {
                results.Add(new ApiResponseAddressDto
                {
                    StreetPrefix = GetDbString(reader, "StreetPrefix"),
                    StreetName = GetDbString(reader, "StreetName"),
                    BuildingNumber = GetDbString(reader, "BuildingNumber"),
                    City = GetDbString(reader, "City"),
                    PostalCode = GetDbString(reader, "PostalCode"),
                    Commune = GetDbString(reader, "Commune"),
                    District = GetDbString(reader, "District"),
                    Province = GetDbString(reader, "Province")
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
                var ordinal = record.GetOrdinal(columnName);
                return record.IsDBNull(ordinal) ? null : record.GetString(ordinal);
            }
            catch (IndexOutOfRangeException)
            {
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
            sheet.Column(columnsNo).InsertColumnsAfter(1);
            sheet.Cell(1, columnsNo + 1).Value = "IsCorrect";
            sheet.Column(columnsNo + 1).SetAutoFilter();
            sheet.Column(columnsNo + 1).Width = 15;
        }

        // Odczytuje numery kolumn REQUEST/RESULT na podstawie nagłówków, aby logika była odporna na kolejność kolumn.
        private static ColumnIndexes GetColumnIndexes(IXLWorksheet sheet)
        {
            var result = new ColumnIndexes();

            var columnsNo = sheet.Columns().Count();

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
