[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$')][string]$SnapshotId,
    [ValidateRange(1,65535)][int]$Port = 8000,
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$')][string]$ReplayRunId = ''
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $PSScriptRoot 'scripts\wsl-runtime-bridge.ps1')
    $runtime = Get-DotaRuntime
    $servePath = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $PSScriptRoot 'web\serve.py') -Runtime $runtime
    $linuxRepo = $servePath.Substring(0, $servePath.LastIndexOf('/web/'))
    $webPython = "$linuxRepo/.venv-web/bin/python"
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec test -x $webPython
    if ($LASTEXITCODE -ne 0) {
        Write-Host 'Creating isolated web environment and installing pinned runtime dependencies...'
        & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $runtime.spark_python -m venv "$linuxRepo/.venv-web"
        if ($LASTEXITCODE -ne 0) { throw 'Cannot create .venv-web; ensure python3-venv is installed.' }
        & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $webPython -m pip install -r "$linuxRepo/requirements-web.txt"
        if ($LASTEXITCODE -ne 0) { throw 'Cannot install web runtime dependencies.' }
    }
    Write-Host "DotaLens: http://localhost:$Port  (Ctrl+C to stop)" -ForegroundColor Green
    $webArgs = @('--snapshot-id',$SnapshotId,'--port',"$Port")
    if ($ReplayRunId) { $webArgs += @('--replay-run-id',$ReplayRunId) }
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --cd $linuxRepo --exec $webPython -m web.serve @webArgs
    if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne 130) { throw 'Website stopped with an error; check snapshot and dependencies.' }
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
