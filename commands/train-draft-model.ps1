[CmdletBinding()]
param(
    [string]$InputPath = 'hdfs://localhost:9000/dota/processed/match_drafts_v1/',
    [string]$OutputRoot = 'hdfs://localhost:9000/dota/models/draft_lr_v1',
    [string]$RunId = '',
    [int]$Seed = 42
)
$repoRoot = Split-Path $PSScriptRoot -Parent

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $repoRoot 'scripts\wsl-runtime-bridge.ps1')
    $runtime = Get-DotaRuntime
    $job = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $repoRoot 'ml\train_draft_model.py') -Runtime $runtime
    $jobArgs = @('--input', $InputPath, '--output-root', $OutputRoot, '--seed', "$Seed")
    if ($RunId) { $jobArgs += @('--run-id', $RunId) }
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec env `
        "JAVA_HOME=$($runtime.java_home)" "HADOOP_CONF_DIR=$($runtime.cluster_root)/conf/namenode" `
        SPARK_LOCAL_IP=127.0.0.1 "PYSPARK_PYTHON=$($runtime.spark_python)" `
        $runtime.spark_submit --master $runtime.spark_master --driver-memory $runtime.spark_driver_memory `
        --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 `
        --conf spark.sql.shuffle.partitions=8 `
        --conf spark.hadoop.ipc.client.connect.max.retries=1 `
        --conf spark.hadoop.ipc.client.connect.max.retries.on.timeouts=1 `
        $job @jobArgs
    if ($LASTEXITCODE -ne 0) { throw 'Step 4 failed; read the error above and check HDFS/input/run ID.' }
    Write-Host 'Step 4 completed. See evidence\step4.' -ForegroundColor Green
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
