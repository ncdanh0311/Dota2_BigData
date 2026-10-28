[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
try {
    . (Join-Path $PSScriptRoot 'wsl-runtime-bridge.ps1')
    $runtime = Get-DotaRuntime
    $repoRoot = Split-Path $PSScriptRoot -Parent
    $summaryTest=ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $repoRoot 'tests\test_ci_summary.py') -Runtime $runtime
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $runtime.spark_python $summaryTest
    if ($LASTEXITCODE -ne 0) { throw 'CI summary checks failed.' }
    foreach ($testName in @('test_prepare_match_drafts.py', 'test_draft_stats.py',
                            'test_ml_features.py', 'test_ml_training.py', 'test_ml_inference.py', 'test_web_export.py', 'test_streaming_checkpoint_recovery.py')) {
      $testPath = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $repoRoot "tests\$testName") -Runtime $runtime
      & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec env `
        "JAVA_HOME=$($runtime.java_home)" `
        SPARK_LOCAL_IP=127.0.0.1 "PYSPARK_PYTHON=$($runtime.spark_python)" `
        $runtime.spark_submit --master $runtime.spark_master --driver-memory $runtime.spark_driver_memory `
        --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 `
        $testPath
      if ($LASTEXITCODE -ne 0) { throw "Spark rule tests failed: $testName" }
    }
    $apiTest = ConvertTo-DotaLinuxPath -WindowsPath (Join-Path $repoRoot 'tests\test_web_api.py') -Runtime $runtime
    $linuxRepo = $apiTest.Substring(0, $apiTest.LastIndexOf('/tests/'))
    $webPython = "$linuxRepo/.venv-web/bin/python"
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec test -x $webPython
    if ($LASTEXITCODE -ne 0) {
      & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $runtime.spark_python -m venv "$linuxRepo/.venv-web"
      if ($LASTEXITCODE -ne 0) { throw 'Cannot create web test environment.' }
    }
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $webPython -m pip install -r "$linuxRepo/requirements-web-test.txt"
    if ($LASTEXITCODE -ne 0) { throw 'Cannot install pinned web test dependencies.' }
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $webPython $apiTest
    if ($LASTEXITCODE -ne 0) { throw 'Web API tests failed.' }
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $webPython "$linuxRepo/tests/test_streaming_api.py"
    if ($LASTEXITCODE -ne 0) { throw 'Streaming API tests failed.' }
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec $runtime.spark_python "$linuxRepo/tests/test_workflow_job_runner.py"
    if ($LASTEXITCODE -ne 0) { throw 'Orchestration runner tests failed.' }
    $airflowConfig=Get-Content (Join-Path $repoRoot 'configs\airflow-wsl.json') -Raw | ConvertFrom-Json
    $airflowPython="$($airflowConfig.root)/venv/bin/python"
    & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec test -x $airflowPython
    if ($LASTEXITCODE -eq 0) {
      & wsl.exe --distribution $runtime.distribution --user $runtime.user --exec env `
        "AIRFLOW_HOME=$($airflowConfig.root)/runtime" AIRFLOW__CORE__LOAD_EXAMPLES=False `
        $airflowPython "$linuxRepo/tests/test_airflow_dags.py"
      if ($LASTEXITCODE -ne 0) { throw 'Airflow SDK DAG tests failed.' }
    }
    else { Write-Host 'Airflow DAG parse checks skipped: run install-airflow.ps1 first. CI runs these in a separate environment.' }
    if (Get-Command node -ErrorAction SilentlyContinue) {
      & node (Join-Path $repoRoot 'tests\test_web_logic.mjs')
      if ($LASTEXITCODE -ne 0) { throw 'Web display rule tests failed.' }
    }
    else { Write-Host 'Optional JavaScript unit check skipped (Node is only required for this check, not the website).' }
    Write-Host 'Spark and web rule tests passed.' -ForegroundColor Green
}
catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
exit 0
