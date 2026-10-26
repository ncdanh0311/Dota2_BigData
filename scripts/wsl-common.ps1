# Shared WSL bridge. This file defines helpers and does not start any service.
function Get-DotaRuntime {
    $configPath = Join-Path (Split-Path $PSScriptRoot -Parent) 'configs\hdfs-wsl.json'
    if (-not (Test-Path -LiteralPath $configPath -PathType Leaf)) {
        throw "Runtime configuration not found: $configPath"
    }
    Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
}

function ConvertTo-DotaLinuxPath {
    param([string]$WindowsPath, [object]$Runtime)
    if (-not (Test-Path -LiteralPath $WindowsPath -PathType Leaf)) {
        throw "Script not found: $WindowsPath"
    }
    $converted = & wsl.exe --distribution $Runtime.distribution --user $Runtime.user --exec wslpath -u $WindowsPath
    if ($LASTEXITCODE -ne 0) {
        throw 'Cannot access configured Ubuntu WSL or convert the project path.'
    }
    ($converted -join "`n").Trim()
}
