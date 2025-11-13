Param(
    [string]$Bootstrap = "localhost:9092",
    [string]$RawTopic = "iot.traffic.raw",
    [string]$ProcessedTopic = "iot.traffic.processed",
    [int]$Partitions = 3,
    [int]$ReplicationFactor = 1
)

Write-Host "Creating Kafka topics on $Bootstrap ..."

$createRaw = @(
    'docker exec kafka bash -lc '
    + '"kafka-topics --bootstrap-server ' + $Bootstrap + ' '
    + '--create --if-not-exists '
    + '--topic ' + $RawTopic + ' '
    + '--partitions ' + $Partitions + ' '
    + '--replication-factor ' + $ReplicationFactor + '"'
)

$createProcessed = @(
    'docker exec kafka bash -lc '
    + '"kafka-topics --bootstrap-server ' + $Bootstrap + ' '
    + '--create --if-not-exists '
    + '--topic ' + $ProcessedTopic + ' '
    + '--partitions ' + $Partitions + ' '
    + '--replication-factor ' + $ReplicationFactor + '"'
)

Write-Host "- Creating raw topic: $RawTopic"
Invoke-Expression ($createRaw -join '') | Out-Host

Write-Host "- Creating processed topic: $ProcessedTopic"
Invoke-Expression ($createProcessed -join '') | Out-Host

Write-Host "Listing topics:"
docker exec kafka bash -lc "kafka-topics --bootstrap-server $Bootstrap --list" | Out-Host

Write-Host "Done."
