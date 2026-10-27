[CmdletBinding()]
param()
$ErrorActionPreference='Stop'
try {
    . (Join-Path $PSScriptRoot 'scripts\wsl-common.ps1')
    $runtime=Get-DotaRuntime
    $job=ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $PSScriptRoot 'orchestration\airflow_lab.py') -Runtime $runtime
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $runtime.spark_python $job start
    if ($LASTEXITCODE -ne 0) { throw 'Airflow start failed; see private launcher.log in its runtime root.' }
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
