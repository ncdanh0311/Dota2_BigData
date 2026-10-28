# Prepare pre-match Dota drafts from HDFS using the existing WSL PySpark.
[CmdletBinding()]
param(
    [string]$InputPath = 'hdfs://localhost:9000/dota/raw/zenodo_v3/DB_03_Live_Features_ANALYSIS_v3.csv',
    [string]$OutputPath = 'hdfs://localhost:9000/dota/processed/match_drafts_v1'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
try {
    $null = Get-Command wsl.exe -ErrorAction Stop
    . (Join-Path $PSScriptRoot 'scripts\wsl-runtime-bridge.ps1')
    $runtime = Get-DotaRuntime
    $pipelinePath = Join-Path $PSScriptRoot 'pipeline\prepare_match_drafts.py'
    if (-not (Test-Path -LiteralPath $pipelinePath -PathType Leaf)) {
        throw "Pipeline not found: $pipelinePath"
    }
    $linuxPipelinePath = ConvertTo-DotaLinuxPath -WindowsPath $pipelinePath -Runtime $runtime
    Write-Host 'Step 2: validate snapshots and write one draft per match to HDFS.'
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec env `
        "JAVA_HOME=$($runtime.java_home)" `
        "HADOOP_CONF_DIR=$($runtime.cluster_root)/conf/namenode" `
        SPARK_LOCAL_IP=127.0.0.1 "PYSPARK_PYTHON=$($runtime.spark_python)" `
        $runtime.spark_submit `
        --master $runtime.spark_master --driver-memory $runtime.spark_driver_memory `
        --conf spark.driver.host=127.0.0.1 `
        --conf spark.driver.bindAddress=127.0.0.1 `
        --conf spark.sql.shuffle.partitions=8 `
        --conf spark.hadoop.ipc.client.connect.max.retries=1 `
        --conf spark.hadoop.ipc.client.connect.max.retries.on.timeouts=1 `
        $linuxPipelinePath --input $InputPath --output $OutputPath
    if ($LASTEXITCODE -ne 0) {
        throw 'Step 2 failed. Read the error above; check HDFS with start-hadoop.ps1. Existing output is never overwritten.'
    }
    Write-Host 'Step 2 completed and Parquet read-back verified. See evidence\step2.' -ForegroundColor Green
}
catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
exit 0
