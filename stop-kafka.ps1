[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $PSScriptRoot 'scripts\streaming-common.ps1')
    Invoke-DotaStreamingPython -Job 'streaming/kafka_lab.py' -JobArgs @('stop')
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
