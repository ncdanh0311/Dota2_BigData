[CmdletBinding()]
param()
$ErrorActionPreference='Stop'
try {
    . (Join-Path $PSScriptRoot 'scripts\wsl-runtime-bridge.ps1')
    $runtime=Get-DotaRuntime
    $job=ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $PSScriptRoot 'orchestration\airflow_runtime_manager.py') -Runtime $runtime
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $runtime.spark_python $job stop
    if ($LASTEXITCODE -ne 0) { throw 'Airflow stop did not complete; check active DAGs.' }
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
