param(
    [ValidateSet('A','B')][string]$Application = 'B',
    [ValidateSet('quick','explore')][string]$Mode = 'quick',
    [string]$RunTag = (Get-Date -Format 'yyyyMMdd-HHmmss'),
    [int]$Repetitions = 0,
    [string]$CostModelRun = '',
    [switch]$IncludeBaselines,
    [ValidateSet('D1','D2','D3','D4','NATURAL','MODERATE','SKEW')][string[]]$Scenarios = @('D1','D2','D3','D4')
)
$ErrorActionPreference = 'Stop'
$lab = $PSScriptRoot
$repo = (Resolve-Path (Join-Path $lab '../../..')).Path
python (Join-Path $lab 'apps/verify_sources.py') --repo $repo --audit (Join-Path $lab 'source_audit.json')
if ($LASTEXITCODE -ne 0) { throw 'Audited source content changed' }
$compose = Join-Path $lab 'compose.yml'
$env:RUN_ID = "$Application-$Mode-$RunTag"
$results = Join-Path $lab 'results'
if (Test-Path (Join-Path $results $env:RUN_ID)) { throw 'Use a new run tag' }
function Invoke-Docker {
    param([string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) { throw "docker failed: $($Arguments -join ' ')" }
}
try {
    Invoke-Docker @('compose','-f',$compose,'up','-d','master','worker-1','worker-2')
    $command = @('/opt/spark/bin/spark-submit','--master','spark://master:7077',
        '--driver-memory','1536m','--executor-memory','2g','--conf','spark.cores.max=4',
        '--conf','spark.executor.cores=2','--conf','spark.driver.host=driver','--conf','spark.driver.bindAddress=0.0.0.0',
        '--conf','spark.executor.processTreeMetrics.enabled=true','--conf','spark.executor.metrics.pollingInterval=250',
        '--conf','spark.eventLog.logStageExecutorMetrics=true','--conf','spark.metrics.conf=/experiment/conf/metrics.properties',
        '/experiment/apps/benchmark.py','--source','/source','--app',$Application,'--mode',$Mode,'--output',"/results/$env:RUN_ID")
    if ($Repetitions) { $command += @('--reps',"$Repetitions") }
    if ($CostModelRun) { $command += @('--cost-model',"/results/$CostModelRun/cost_model.json") }
    if ($IncludeBaselines) { $command += '--include-baselines' }
    $command += @('--scenarios') + $Scenarios
    Invoke-Docker -Arguments (@('compose','-f',$compose,'run','-d','--name','witw-spark-partitioning-run',
        '--use-aliases','--no-deps','driver')+$command)
    & docker inspect witw-spark-partitioning-run witw-spark-partitioning-worker-1-1 witw-spark-partitioning-worker-2-1 witw-spark-partitioning-master-1 |
        Set-Content -Encoding utf8 (Join-Path $results "$env:RUN_ID.containers.json")
    do {
        Start-Sleep -Seconds 5
        $state = & docker inspect --format '{{.State.Status}}' witw-spark-partitioning-run
        if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect experiment driver' }
    } while ($state -in @('running','created'))
    $exitCode = & docker inspect --format '{{.State.ExitCode}}' witw-spark-partitioning-run
    & docker inspect witw-spark-partitioning-run witw-spark-partitioning-worker-1-1 witw-spark-partitioning-worker-2-1 |
        Set-Content -Encoding utf8 (Join-Path $results "$env:RUN_ID.final-state.json")
    Invoke-Docker @('compose','-f',$compose,'stop','worker-1','worker-2')
    Invoke-Docker @('cp',"witw-spark-partitioning-run:/results/$env:RUN_ID",$results)
    & docker logs witw-spark-partitioning-run 2>&1 | Out-File -Encoding utf8 (Join-Path $results "$env:RUN_ID.console.log")
    Invoke-Docker @('rm','witw-spark-partitioning-run')
    if ($exitCode -ne '0') { throw "Experiment failed: $env:RUN_ID (exit $exitCode)" }
    python (Join-Path $lab 'apps/validate.py') (Join-Path $results $env:RUN_ID)
    if ($LASTEXITCODE -ne 0) { throw 'Validation failed' }
} finally {
    & docker compose -f $compose down
}
