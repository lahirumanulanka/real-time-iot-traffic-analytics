Param(
    [string]$Bootstrap = "localhost:9092",
    [string]$Topic = "iot.traffic.raw",
    [string]$CsvPath = "${PWD}\data\dataset\traffic_counts.csv",
    [int]$MaxRows = 50
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path $CsvPath)) {
    Write-Error "CSV not found at $CsvPath"
}

Write-Host "Reading up to $MaxRows rows from $CsvPath and sending to topic '$Topic'..."

$rows = Import-Csv -Path $CsvPath | Select-Object -First $MaxRows

$tempFile = Join-Path $env:TEMP ("kafka_csv_" + [Guid]::NewGuid().ToString() + ".txt")
try {
    foreach ($row in $rows) {
        # Convert each CSV row to JSON compactly
        ($row | ConvertTo-Json -Compress) | Out-File -FilePath $tempFile -Encoding utf8 -Append
    }

    docker cp "$tempFile" kafka:/tmp/msgs_csv.txt | Out-Null
    docker exec kafka bash -lc "cat /tmp/msgs_csv.txt | kafka-console-producer --bootstrap-server $Bootstrap --topic $Topic 1>/dev/null" | Out-Null
    Write-Host "Done. Sent $($rows.Count) CSV-derived messages to $Topic."
}
finally {
    if (Test-Path $tempFile) { Remove-Item $tempFile -Force }
}
