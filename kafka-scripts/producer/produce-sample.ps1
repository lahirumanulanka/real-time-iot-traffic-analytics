Param(
    [string]$Bootstrap = "localhost:9092",
    [string]$Topic = "iot.traffic.raw",
    [int]$Count = 20,
    [int]$Sensors = 5,
    [int]$RateMs = 200
)

$ErrorActionPreference = 'Stop'

Write-Host "Generating $Count sample messages for topic '$Topic'..."

$tempFile = Join-Path $env:TEMP ("kafka_msgs_" + [Guid]::NewGuid().ToString() + ".txt")
try {
    1..$Count | ForEach-Object {
        $now = [DateTimeOffset]::UtcNow.ToString("o")
        $sensorId = "traffic-sensor-" + ((Get-Random -Minimum 1 -Maximum ($Sensors+1)))
        $vehicleCount = (Get-Random -Minimum 0 -Maximum 50)
        $avgSpeed = [Math]::Round((Get-Random -Minimum 20.0 -Maximum 100.0), 1)
        $occupancy = [Math]::Round((Get-Random -Minimum 0.0 -Maximum 100.0), 1)

        $obj = [ordered]@{
            timestamp = $now
            sensor_id = $sensorId
            vehicle_count = $vehicleCount
            avg_speed_kph = $avgSpeed
            occupancy_pct = $occupancy
        }
        ($obj | ConvertTo-Json -Compress) | Out-File -FilePath $tempFile -Encoding utf8 -Append

        if ($RateMs -gt 0) { Start-Sleep -Milliseconds $RateMs }
    }

    Write-Host "Copying messages to container..."
    docker cp "$tempFile" kafka:/tmp/msgs.txt | Out-Null

    Write-Host "Producing to Kafka topic '$Topic' on $Bootstrap ..."
    docker exec kafka bash -lc "cat /tmp/msgs.txt | kafka-console-producer --bootstrap-server $Bootstrap --topic $Topic 1>/dev/null" | Out-Null

    Write-Host "Done. Sent $Count messages to $Topic."
}
finally {
    if (Test-Path $tempFile) { Remove-Item $tempFile -Force }
}
