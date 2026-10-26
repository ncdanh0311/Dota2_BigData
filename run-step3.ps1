# Build versioned Spark SQL statistics from the verified Silver drafts.
[CmdletBinding()]
param(
    [string]$InputPath = 'hdfs://localhost:9000/dota/processed/match_drafts_v1/',
    [string]$OutputRoot = 'hdfs://localhost:9000/dota/analytics/draft_stats_v1',
    [string]$RunId = '',
    [ValidateRange(1, 2147483647)][int]$MinHeroMatches = 100,
    [ValidateRange(1, 2147483647)][int]$MinPairMatches = 100
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $PSScriptRoot 'scripts\wsl-common.ps1')
    $runtime = Get-DotaRuntime
    $jobPath = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $PSScriptRoot 'analytics\draft_stats.py') -Runtime $runtime
    $pipelineArgs = @('--input', $InputPath, '--output-root', $OutputRoot,
                      '--min-hero-matches', "$MinHeroMatches", '--min-pair-matches', "$MinPairMatches")
    if ($RunId) { $pipelineArgs += @('--run-id', $RunId) }
    Write-Host 'Step 3: Spark SQL hero, side and pair statistics...'
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec env `
        "JAVA_HOME=$($runtime.java_home)" "HADOOP_CONF_DIR=$($runtime.cluster_root)/conf/namenode" `
        SPARK_LOCAL_IP=127.0.0.1 "PYSPARK_PYTHON=$($runtime.spark_python)" `
        $runtime.spark_submit --master $runtime.spark_master --driver-memory $runtime.spark_driver_memory `
        --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 `
        --conf spark.sql.shuffle.partitions=8 `
        --conf spark.hadoop.ipc.client.connect.max.retries=1 `
        --conf spark.hadoop.ipc.client.connect.max.retries.on.timeouts=1 `
        $jobPath @pipelineArgs
    if ($LASTEXITCODE -ne 0) {
        throw 'Step 3 failed. Read the error above; no completed run is published on failure.'
    }
    Write-Host 'Step 3 completed; Gold read-back verified. See evidence\step3.' -ForegroundColor Green
}
catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
exit 0
