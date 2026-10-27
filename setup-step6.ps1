[CmdletBinding()]
param([switch]$ParallelDownload)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $PSScriptRoot 'scripts\streaming-common.ps1')
    $runtime = Get-DotaRuntime
    $kafka = Get-Content (Join-Path $PSScriptRoot 'configs\kafka-wsl.json') -Raw | ConvertFrom-Json
    $cache = Join-Path $PSScriptRoot 'artifacts\cache'
    $null = New-Item -ItemType Directory -Path $cache -Force
    $filename = "kafka_2.13-$($kafka.version).tgz"
    $url = "$($kafka.download_base)/$($kafka.version)/$filename"
    $archive = Join-Path $cache $filename
    $checksum = "$archive.sha512"
    Write-Host 'Downloading/verifying official Kafka archive through Windows...'
    Invoke-WebRequest -Uri "$url.sha512" -OutFile $checksum -TimeoutSec 60
    if (-not (Test-Path -LiteralPath $archive)) {
        if ($ParallelDownload) {
            # Optional bounded range download for a slow Apache connection. Full SHA-512 is still mandatory.
            $head = Invoke-WebRequest -Method Head -Uri $url -TimeoutSec 30
            $length = [long]($head.Headers['Content-Length'] | Select-Object -First 1)
            $chunkSize = 4194304
            $pieces = [int][Math]::Ceiling($length / $chunkSize)
            0..($pieces-1) | ForEach-Object -Parallel {
                $index = $_
                $first = [long]$index * $using:chunkSize
                $last = [Math]::Min($first + $using:chunkSize - 1, $using:length - 1)
                $piece = "$using:archive.range-$index"
                if (-not (Test-Path -LiteralPath $piece) -or (Get-Item -LiteralPath $piece).Length -ne ($last-$first+1)) {
                    $status = & curl.exe --http1.1 --silent --show-error --fail --location --connect-timeout 15 --max-time 360 --retry 2 --range "$first-$last" --max-filesize ($last-$first+1) --output $piece --write-out '%{http_code}' $using:url
                    if ($LASTEXITCODE -ne 0 -or $status -ne '206') { throw "Range $index failed: HTTP $status" }
                }
            } -ThrottleLimit 32
            $joined = [IO.File]::Create("$archive.part")
            try {
                for ($i=0; $i -lt $pieces; $i++) {
                    $piecePath = "$archive.range-$i"
                    $expectedSize = [Math]::Min($chunkSize, $length - [long]$i * $chunkSize)
                    if ((Get-Item -LiteralPath $piecePath).Length -ne $expectedSize) { throw "Incomplete range $i" }
                    $pieceStream = [IO.File]::OpenRead($piecePath)
                    try { $pieceStream.CopyTo($joined) } finally { $pieceStream.Dispose() }
                }
            } finally { $joined.Dispose() }
        }
        else {
            & curl.exe --http1.1 --fail --location --connect-timeout 15 --max-time 600 --output "$archive.part" $url
            if ($LASTEXITCODE -ne 0) { throw 'Kafka download failed; partial file preserved.' }
        }
        Move-Item -LiteralPath "$archive.part" -Destination $archive
    }
    $linuxArchive = ConvertTo-DotaLinuxPath -WindowsPath $archive -Runtime $runtime
    $linuxChecksum = ConvertTo-DotaLinuxPath -WindowsPath $checksum -Runtime $runtime
    Invoke-DotaStreamingPython -Job 'streaming/kafka_lab.py' -JobArgs @('setup','--archive-path',$linuxArchive,'--checksum-path',$linuxChecksum)
    $managerPath = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $PSScriptRoot 'streaming\kafka_lab.py') -Runtime $runtime
    $linuxRepo = $managerPath.Substring(0, $managerPath.LastIndexOf('/streaming/'))
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec test -x "$linuxRepo/.venv-streaming/bin/python"
    if ($LASTEXITCODE -ne 0) {
        & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $runtime.spark_python -m venv "$linuxRepo/.venv-streaming"
        if ($LASTEXITCODE -ne 0) { throw 'Cannot create streaming environment.' }
    }
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec "$linuxRepo/.venv-streaming/bin/python" -m pip install -r "$linuxRepo/requirements-streaming.txt"
    if ($LASTEXITCODE -ne 0) { throw 'Cannot install producer dependency.' }
    Invoke-DotaStreamingSpark -Job 'streaming/check_connector.py' -UseKafka
    Write-Host 'Step 6 dependencies ready. Kafka storage is preserved on future starts.' -ForegroundColor Green
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
