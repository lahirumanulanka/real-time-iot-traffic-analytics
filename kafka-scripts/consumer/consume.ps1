Param(
    [string]$Bootstrap = "localhost:9092",
    [string]$Topic = "iot.traffic.raw",
    [int]$TimeoutMs = 5000,
    [int]$MaxMessages = 10,
    [switch]$FromBeginning
)

$ErrorActionPreference = 'Stop'

$fromArg = ""
if ($FromBeginning.IsPresent) {
    $fromArg = "--from-beginning"
}

Write-Host "Consuming up to $MaxMessages messages from '$Topic'..."
docker exec kafka bash -lc "kafka-console-consumer --bootstrap-server $Bootstrap --topic $Topic $fromArg --timeout-ms $TimeoutMs --max-messages $MaxMessages" | Out-Host
Write-Host "Done."
