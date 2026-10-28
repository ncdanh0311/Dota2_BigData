[CmdletBinding()]
param()
$repoRoot = Split-Path $PSScriptRoot -Parent

$ErrorActionPreference='Stop'
try {
    . (Join-Path $repoRoot 'scripts\wsl-runtime-bridge.ps1')
    $runtime=Get-DotaRuntime
    $config=Get-Content (Join-Path $repoRoot 'configs\airflow-wsl.json') -Raw | ConvertFrom-Json
    $cache=Join-Path $repoRoot 'artifacts\cache'
    $null=New-Item -ItemType Directory -Path $cache -Force
    $constraints=Join-Path $cache 'airflow-3.3.2-constraints-3.14.txt'
    Invoke-WebRequest -Uri $config.constraints_url -OutFile $constraints -TimeoutSec 60
    $job=ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $repoRoot 'orchestration\airflow_runtime_manager.py') -Runtime $runtime
    $linuxConstraints=ConvertTo-DotaLinuxPath -WindowsPath $constraints -Runtime $runtime
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $runtime.spark_python $job setup --constraints $linuxConstraints
    if ($LASTEXITCODE -ne 0) { throw 'Airflow setup failed; existing environments/data are retained.' }
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
