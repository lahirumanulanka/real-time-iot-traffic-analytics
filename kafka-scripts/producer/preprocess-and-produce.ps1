Param(
    [string]$CsvPath = "${PWD}\data\sample\traffic_sample.csv",
    [string]$OutJsonPath = "${PWD}\data\sample\traffic_sample.ndjson",
    [string]$Bootstrap = "localhost:9092",
    [string]$Topic = "sample.traffic.raw",
    [int]$MaxRows = 0
)

$ErrorActionPreference = 'Stop'

$preprocess = Join-Path $PSScriptRoot "..\..\utils\preprocess-csv.ps1"
if (-not (Test-Path $preprocess)) {
    Write-Error "Preprocessor not found at $preprocess"
}

Write-Host "Preprocessing $CsvPath -> $OutJsonPath (MaxRows=$MaxRows)"
powershell -NoProfile -ExecutionPolicy Bypass -File $preprocess -CsvPath $CsvPath -OutJsonPath $OutJsonPath -MaxRows $MaxRows

Write-Host "Copying NDJSON to container and producing to Kafka..."
docker cp "$OutJsonPath" kafka:/tmp/records.ndjson | Out-Null
docker exec kafka bash -lc "cat /tmp/records.ndjson | kafka-console-producer --bootstrap-server $Bootstrap --topic $Topic 1>/dev/null"

Write-Host "Done. Published records from $OutJsonPath to topic $Topic."
