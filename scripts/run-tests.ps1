[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $PSScriptRoot 'wsl-common.ps1')
    $runtime = Get-DotaRuntime
    $repoRoot = Split-Path $PSScriptRoot -Parent
    foreach ($testName in @('test_prepare_match_drafts.py', 'test_draft_stats.py')) {
      $testPath = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $repoRoot "tests\$testName") -Runtime $runtime
      & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec env `
        "JAVA_HOME=$($runtime.java_home)" `
        SPARK_LOCAL_IP=127.0.0.1 "PYSPARK_PYTHON=$($runtime.spark_python)" `
        $runtime.spark_submit --master $runtime.spark_master --driver-memory $runtime.spark_driver_memory `
        --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 `
        $testPath
      if ($LASTEXITCODE -ne 0) { throw "Spark rule tests failed: $testName" }
    }
    Write-Host 'Spark rule tests passed.' -ForegroundColor Green
}
catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
exit 0
