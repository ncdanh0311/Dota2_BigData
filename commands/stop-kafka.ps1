[CmdletBinding()]
param()
$repoRoot = Split-Path $PSScriptRoot -Parent

$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $repoRoot 'scripts\kafka-job-bridge.ps1')
    Invoke-DotaStreamingPython -Job 'streaming/kafka_broker_manager.py' -JobArgs @('stop')
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
