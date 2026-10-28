[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$')][string]$RunId,
    [Parameter(Mandatory=$true)][string]$Radiant,
    [Parameter(Mandatory=$true)][string]$Dire
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $PSScriptRoot 'scripts\wsl-runtime-bridge.ps1')
    $runtime = Get-DotaRuntime
    $scriptPath = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $PSScriptRoot 'ml\predict_draft.py') -Runtime $runtime
    $bundlePath = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $PSScriptRoot "artifacts\models\$RunId\model_bundle.json") -Runtime $runtime
    # Deliberately call Python directly: no Spark gateway, JVM or HDFS needed.
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec `
        $runtime.spark_python $scriptPath --bundle $bundlePath --radiant $Radiant --dire $Dire
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
