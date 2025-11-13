Param(
    [Parameter(Mandatory=$true)][string]$CsvPath,
    [Parameter(Mandatory=$true)][string]$OutJsonPath,
    [int]$MaxRows = 0
)

$ErrorActionPreference = 'Stop'

function Convert-ToNullIfEmpty($v) {
    if ($null -eq $v) { return $null }
    $s = [string]$v
    if ([string]::IsNullOrWhiteSpace($s)) { return $null }
    return $s
}

function Convert-ToIntOrNull($s) {
    $s = Convert-ToNullIfEmpty $s
    if ($null -eq $s) { return $null }
    $clean = $s -replace ",", ""
    $out = 0
    if ([int]::TryParse($clean, [ref]$out)) { return $out } else { return $null }
}

function Convert-ToDoubleOrNull($s) {
    $s = Convert-ToNullIfEmpty $s
    if ($null -eq $s) { return $null }
    $clean = $s -replace ",", ""
    $out = 0.0
    if ([double]::TryParse($clean, [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$out)) { return $out } else { return $null }
}

function Convert-ToIso8601OrNull($s) {
    $s = Convert-ToNullIfEmpty $s
    if ($null -eq $s) { return $null }
    try {
        $dt = [DateTimeOffset]::Parse($s, [System.Globalization.CultureInfo]::InvariantCulture)
        return $dt.UtcDateTime.ToString("o")
    } catch {
        try {
            $dt2 = [DateTime]::Parse($s, [System.Globalization.CultureInfo]::InvariantCulture)
            return ([DateTimeOffset]$dt2).UtcDateTime.ToString("o")
        } catch {
            return $null
        }
    }
}

function Parse-PointLatLon($locationField) {
    $locationField = Convert-ToNullIfEmpty $locationField
    if ($null -eq $locationField) { return @{ lat=$null; lon=$null } }
    # Expected format: POINT (-97.842659 30.428783)
    $m = [regex]::Match($locationField, 'POINT\s*\(([-0-9\.]+)\s+([-0-9\.]+)\)')
    if ($m.Success) {
        $lon = Convert-ToDoubleOrNull $m.Groups[1].Value
        $lat = Convert-ToDoubleOrNull $m.Groups[2].Value
        return @{ lat=$lat; lon=$lon }
    }
    return @{ lat=$null; lon=$null }
}

if (-not (Test-Path $CsvPath)) {
    Write-Error "CSV file not found: $CsvPath"
}

$rows = Import-Csv -Path $CsvPath
if ($MaxRows -gt 0) { $rows = $rows | Select-Object -First $MaxRows }

# Detect schema by headers and map accordingly
$headers = @()
if ($rows.Count -gt 0) { $headers = $rows[0].PSObject.Properties.Name }

$outDir = Split-Path -Parent $OutJsonPath
if (-not (Test-Path $outDir)) { New-Item -ItemType Directory -Path $outDir | Out-Null }
if (Test-Path $OutJsonPath) { Remove-Item $OutJsonPath -Force }

Write-Host "Processing $($rows.Count) rows from $CsvPath -> $OutJsonPath"

foreach ($r in $rows) {
    # Branch A: sample dataset (timestamp,sensor_id,vehicle_count,avg_speed_kph,occupancy_pct)
    if ($headers -contains 'sensor_id' -and $headers -contains 'vehicle_count') {
        $obj = [ordered]@{
            timestamp    = Convert-ToIso8601OrNull $r.timestamp
            sensorId     = Convert-ToNullIfEmpty $r.sensor_id
            vehicleCount = Convert-ToIntOrNull $r.vehicle_count
            avgSpeedKph  = Convert-ToDoubleOrNull $r.avg_speed_kph
            occupancyPct = Convert-ToDoubleOrNull $r.occupancy_pct
            source       = 'sample'
        }
        ($obj | ConvertTo-Json -Compress) | Out-File -FilePath $OutJsonPath -Encoding utf8 -Append
        continue
    }

    # Branch B: big traffic_counts.csv dataset
    $pt = Parse-PointLatLon $r.LOCATION
    $lat = if ($r.location_latitude) { Convert-ToDoubleOrNull $r.location_latitude } else { $pt.lat }
    $lon = if ($r.location_longitude) { Convert-ToDoubleOrNull $r.location_longitude } else { $pt.lon }

    $obj2 = [ordered]@{
        detectorId       = Convert-ToIntOrNull $r.detector_id
        type             = Convert-ToNullIfEmpty $r.detector_type
        status           = Convert-ToNullIfEmpty $r.detector_status
        direction        = Convert-ToNullIfEmpty $r.detector_direction
        movement         = Convert-ToNullIfEmpty $r.detector_movement
        locationName     = Convert-ToNullIfEmpty $r.location_name
        atdLocationId    = Convert-ToNullIfEmpty $r.atd_location_id
        signalId         = Convert-ToIntOrNull $r.signal_id
        createdTs        = Convert-ToIso8601OrNull $r.created_date
        modifiedTs       = Convert-ToIso8601OrNull $r.modified_date
        latitude         = $lat
        longitude        = $lon
        source           = 'traffic_counts'
    }
    ($obj2 | ConvertTo-Json -Compress) | Out-File -FilePath $OutJsonPath -Encoding utf8 -Append
}

Write-Host "Wrote NDJSON: $OutJsonPath"
