# Shared process bridge for the isolated Kafka replay lab.
. (Join-Path $PSScriptRoot 'wsl-runtime-bridge.ps1')
function Invoke-DotaStreamingPython {
    param([string]$Job, [string[]]$JobArgs = @(), [switch]$Producer)
    $runtime = Get-DotaRuntime
    $repo = Split-Path $PSScriptRoot -Parent
    $linuxJob = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $repo $Job) -Runtime $runtime
    $python = $runtime.spark_python
    if ($Producer) {
        $linuxProducer = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $repo 'streaming\publish_match_events.py') -Runtime $runtime
        $linuxRepo = $linuxProducer.Substring(0, $linuxProducer.LastIndexOf('/streaming/'))
        $python = "$linuxRepo/.venv-streaming/bin/python"
    }
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $python $linuxJob @JobArgs
    if ($LASTEXITCODE -ne 0) { throw "Streaming command failed: $Job" }
}
function Invoke-DotaStreamingSpark {
    param([string]$Job, [string[]]$JobArgs = @(), [switch]$UseKafka)
    $runtime = Get-DotaRuntime
    $repo = Split-Path $PSScriptRoot -Parent
    $linuxJob = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $repo $Job) -Runtime $runtime
    $packageArgs = @()
    if ($UseKafka) {
        $kafka = Get-Content (Join-Path $repo 'configs\kafka-wsl.json') -Raw | ConvertFrom-Json
        $ivy = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $repo 'configs\ivy-kafka.xml') -Runtime $runtime
        # These dependencies already ship with the pinned PySpark runtime.
        $provided = 'org.apache.hadoop:hadoop-client-runtime,org.apache.hadoop:hadoop-client-api,org.scala-lang:scala-library,org.scala-lang.modules:scala-parallel-collections_2.13,org.slf4j:slf4j-api,org.xerial.snappy:snappy-java,org.lz4:lz4-java,at.yawk.lz4:lz4-java,com.github.luben:zstd-jni'
        $packageArgs = @('--conf',"spark.jars.ivySettings=$ivy",'--exclude-packages',$provided,'--packages', $kafka.connector)
    }
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec env `
        "JAVA_HOME=$($runtime.java_home)" "HADOOP_CONF_DIR=$($runtime.cluster_root)/conf/namenode" `
        SPARK_LOCAL_IP=127.0.0.1 "PYSPARK_PYTHON=$($runtime.spark_python)" `
        $runtime.spark_submit --master $runtime.spark_master --driver-memory $runtime.spark_driver_memory `
        --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 `
        --conf spark.sql.shuffle.partitions=8 --conf spark.sql.session.timeZone=UTC `
        @packageArgs $linuxJob @JobArgs
    if ($LASTEXITCODE -ne 0) { throw "Streaming Spark job failed: $Job" }
}
