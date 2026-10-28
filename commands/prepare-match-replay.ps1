[CmdletBinding()]
param([Parameter(Mandatory=$true)][string]$RunId, [ValidateRange(1,2147483647)][int]$Limit = 1000)
$repoRoot = Split-Path $PSScriptRoot -Parent

$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $repoRoot 'scripts\kafka-job-bridge.ps1')
    Invoke-DotaStreamingSpark -Job 'streaming/prepare_replay_catalog.py' -JobArgs @('--run-id',$RunId,'--limit',"$Limit")
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
