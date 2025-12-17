using Microsoft.Extensions.Configuration;
using NormalizatorTests;

var config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

var originalFilePath = config["OriginalFilePath"];
var resultFilePath = config["ResultFilePath"];
var apiUrl = config["ApiUrl"];
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
    var apiResultPath = AppendTimestampSuffix(resultFilePath);
    await TestEngine.RunApiTest(originalFilePath!, apiResultPath, apiUrl!, maxParallelRequests);
}

// Jeśli podano konfigurację DB, wykonujemy dodatkowy scenariusz z bazą
if (enableDb &&
    !string.IsNullOrWhiteSpace(dbResultFilePath) &&
    !string.IsNullOrWhiteSpace(dbConnectionString) &&
    !string.IsNullOrWhiteSpace(dbQuery))
{
    var dbResultPath = AppendTimestampSuffix(dbResultFilePath);
    await TestEngine.RunDbTest(originalFilePath!, dbResultPath, dbConnectionString!, dbQuery!, maxParallelRequests, dbMapping);
}

static string AppendTimestampSuffix(string path)
{
    var timestamp = DateTime.Now.ToString("yyMMddHHmm");
    var directory = Path.GetDirectoryName(path);
    var filenameWithoutExt = Path.GetFileNameWithoutExtension(path);
    var extension = Path.GetExtension(path);
    var withSuffix = $"{filenameWithoutExt}_{timestamp}{extension}";
    return string.IsNullOrWhiteSpace(directory) ? withSuffix : Path.Combine(directory, withSuffix);
}