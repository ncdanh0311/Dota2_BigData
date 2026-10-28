[CmdletBinding()]
param(
    [string]$GoldRunId = 'step3-20261026-v2',
    [string]$ModelRunId = 'step4-20261027-v1',
    [string]$SnapshotId = '',
    [string]$HeroAssetsSnapshotId = ''
)
$repoRoot = Split-Path $PSScriptRoot -Parent

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $repoRoot 'scripts\wsl-runtime-bridge.ps1')
    $runtime = Get-DotaRuntime
    $job = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $repoRoot 'web\export_snapshot.py') -Runtime $runtime
    $jobArgs = @('--gold-run-id', $GoldRunId, '--model-run-id', $ModelRunId)
    if ($SnapshotId) { $jobArgs += @('--snapshot-id', $SnapshotId) }
    if ($HeroAssetsSnapshotId) {
        if ($HeroAssetsSnapshotId -notmatch '^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$') { throw 'Invalid hero asset snapshot ID.' }
        $linuxRepo=$job.Substring(0,$job.LastIndexOf('/web/'))
        $jobArgs += @('--hero-assets-from',"$linuxRepo/artifacts/web/$HeroAssetsSnapshotId")
    }
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec env `
        "JAVA_HOME=$($runtime.java_home)" "HADOOP_CONF_DIR=$($runtime.cluster_root)/conf/namenode" `
        SPARK_LOCAL_IP=127.0.0.1 "PYSPARK_PYTHON=$($runtime.spark_python)" `
        $runtime.spark_submit --master $runtime.spark_master --driver-memory $runtime.spark_driver_memory `
        --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 `
        --conf spark.sql.shuffle.partitions=8 `
        --conf spark.hadoop.ipc.client.connect.max.retries=1 `
        --conf spark.hadoop.ipc.client.connect.max.retries.on.timeouts=1 $job @jobArgs
    if ($LASTEXITCODE -ne 0) { throw 'Snapshot preparation failed; see source/completion/asset errors above.' }
    Write-Host 'Web snapshot verified. See evidence\step5.' -ForegroundColor Green
}
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
exit 0
