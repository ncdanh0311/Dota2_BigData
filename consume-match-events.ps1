[CmdletBinding()]
param([Parameter(Mandatory=$true)][string]$RunId, [int]$TriggerSeconds=10,
      [int]$StopAfterSeconds=0, [switch]$AvailableNow)
$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $PSScriptRoot 'scripts\kafka-job-bridge.ps1')
    $jobArgs = @('--run-id',$RunId,'--trigger-seconds',"$TriggerSeconds",'--stop-after-seconds',"$StopAfterSeconds")
    if ($AvailableNow) { $jobArgs += '--available-now' }
    Invoke-DotaStreamingSpark -Job 'streaming/consume_match_events.py' -JobArgs $jobArgs -UseKafka
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
