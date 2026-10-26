# Starts only the Dota HDFS cluster (1 NameNode and 3 DataNodes).
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    $null = Get-Command wsl.exe -ErrorAction Stop
    . (Join-Path $PSScriptRoot 'scripts\wsl-common.ps1')
    $runtime = Get-DotaRuntime
    $managerPath = Join-Path $PSScriptRoot 'scripts\hdfs_lab.py'
    if (-not (Test-Path -LiteralPath $managerPath -PathType Leaf)) {
        throw "HDFS manager not found: $managerPath"
    }

    $linuxManagerPath = ConvertTo-DotaLinuxPath -WindowsPath $managerPath -Runtime $runtime

    Write-Host 'Starting Dota HDFS: 1 NameNode + 3 DataNodes...'
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec python3 $linuxManagerPath start
    if ($LASTEXITCODE -ne 0) {
        throw 'HDFS startup failed. Check /home/danh/dota-hdfs/logs in Ubuntu.'
    }

    Write-Host 'HDFS is ready. NameNode UI: http://localhost:9870' -ForegroundColor Green
}
catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}

exit 0
