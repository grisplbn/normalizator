using Microsoft.Extensions.Configuration;
using NormalizatorTests;

var config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

var originalFilePath = config["OriginalFilePath"];
var resultFilePath = config["ResultFilePath"];
var apiUrl = config["ApiUrl"];
var probabilityThreshold = config.GetValue<decimal>("ProbabilityThreshold", new decimal(0.8));
var maxParallelRequests = config.GetValue<int>("MaxParallelRequests", 10);
var enableApi = config.GetValue<bool>("EnableApi", true);
var enableDb = config.GetValue<bool>("EnableDb", true);

// Konfiguracja testu bazy danych (opcjonalna)
var dbResultFilePath = config["DbResultFilePath"];
var dbConnectionString = config["DbConnectionString"];
var dbQuery = config["DbQuery"];
var dbQueryFilePath = config["DbQueryFilePath"];
var dbMappingFilePath = config["DbMappingFilePath"];

// Jeżeli podano plik z zapytaniem SQL, wczytujemy jego treść (ma pierwszeństwo nad DbQuery).
if (!string.IsNullOrWhiteSpace(dbQueryFilePath) && File.Exists(dbQueryFilePath))
{
    dbQuery = await File.ReadAllTextAsync(dbQueryFilePath);
}

// Jeżeli podano plik z mapowaniem kolumn dla DB, wczytujemy go jako słownik Parametr->Nagłówek.
var dbMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
if (!string.IsNullOrWhiteSpace(dbMappingFilePath) && File.Exists(dbMappingFilePath))
{
    var json = await File.ReadAllTextAsync(dbMappingFilePath);
    var parsed = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(json);
    if (parsed != null)
    {
        foreach (var kv in parsed)
        {
            dbMapping[kv.Key] = kv.Value;
        }
    }
}

// Główne uruchomienie logiki testów w trybie asynchronicznym (API)
if (enableApi)
{
    await TestEngine.RunApiTest(originalFilePath!, resultFilePath!, apiUrl!, probabilityThreshold, maxParallelRequests);
}

// Jeśli podano konfigurację DB, wykonujemy dodatkowy scenariusz z bazą
if (enableDb &&
    !string.IsNullOrWhiteSpace(dbResultFilePath) &&
    !string.IsNullOrWhiteSpace(dbConnectionString) &&
    !string.IsNullOrWhiteSpace(dbQuery))
{
    await TestEngine.RunDbTest(originalFilePath!, dbResultFilePath!, dbConnectionString!, dbQuery!, maxParallelRequests, dbMapping);
}