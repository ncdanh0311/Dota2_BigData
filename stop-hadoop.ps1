# Stops only the Dota HDFS cluster. Stored data and metadata are retained.
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

    Write-Host 'Stopping Dota HDFS: DataNodes first, then NameNode...'
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec python3 $linuxManagerPath stop
    if ($LASTEXITCODE -ne 0) {
        throw 'HDFS shutdown failed. Check /home/danh/dota-hdfs/logs in Ubuntu.'
    }

    Write-Host 'HDFS stopped. Dataset and metadata are retained.' -ForegroundColor Green
}
catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}

exit 0
