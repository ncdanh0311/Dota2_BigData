[CmdletBinding()]
param([Parameter(Mandatory=$true)][string]$RunId,[double]$Rate=5,[int]$Repeat=1)
$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $PSScriptRoot 'scripts\streaming-common.ps1')
    Invoke-DotaStreamingPython -Job 'streaming/producer.py' -JobArgs @('--run-id',$RunId,'--rate',$Rate.ToString([Globalization.CultureInfo]::InvariantCulture),'--repeat',"$Repeat") -Producer
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
