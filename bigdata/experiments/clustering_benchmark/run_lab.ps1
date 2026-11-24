param(
    [ValidateSet('Quick','Full','All')][string]$Mode = 'All',
    [string]$RunTag = (Get-Date -Format 'yyyyMMdd-HHmmss'),
    [int[]]$Cores = @(1,2,4),
    [switch]$SkipBuild
)
$ErrorActionPreference = 'Stop'
$lab = $PSScriptRoot
$compose = Join-Path $lab 'compose.yml'
$results = Join-Path $lab 'results'
New-Item -ItemType Directory -Force -Path $results | Out-Null
$repository = (Resolve-Path (Join-Path $lab '../../..')).Path
$audit = Get-Content -LiteralPath (Join-Path $lab 'source_audit.json') -Raw | ConvertFrom-Json
foreach ($property in $audit.production_sha256.PSObject.Properties) {
    $currentHash = (Get-FileHash -LiteralPath (Join-Path $repository $property.Name) -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($currentHash -ne $property.Value) { throw "Source changed since audit: $($property.Name). Create a new audited baseline before comparing." }
}
function Invoke-Docker {
    param([string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) { throw "docker failed: $($Arguments -join ' ')" }
}
if (-not $SkipBuild) {
    Invoke-Docker @('build','-f',(Join-Path $lab 'Dockerfile'),'-t','witw-clustering-lab:local',$lab)
}
function Run-Case {
    param([int]$CoreCount,[bool]$Quick)
    $name = if ($Quick) { "quick-4-$RunTag" } else { "full-$CoreCount-$RunTag" }
    $env:LAB_CORES = "$CoreCount"
    $env:LAB_RUN = $name
    if (Test-Path -LiteralPath (Join-Path $results $name)) { throw "Result already exists: $name" }
    Invoke-Docker @('compose','-f',$compose,'up','-d','master','worker')
    $command = @('/opt/spark/bin/spark-submit','--master','spark://master:7077',
        '--driver-memory','1536m','--executor-memory','3g',
        '--conf',"spark.cores.max=$CoreCount",'--conf',"spark.executor.cores=$CoreCount",
        '--conf','spark.driver.host=driver','--conf','spark.driver.bindAddress=0.0.0.0',
        '--conf','spark.executor.processTreeMetrics.enabled=true',
        '--conf','spark.executor.metrics.pollingInterval=250',
        '--conf','spark.eventLog.logStageExecutorMetrics=true',
        '/bench/benchmark.py','--source','/source','--cores',"$CoreCount",'--output',"/results/$name")
    if ($Quick) { $command += '--quick' } else { $command += @('--reps','10','--seeds','3') }
    Invoke-Docker -Arguments (@('compose','-f',$compose,'run','-d','--name','witw-clustering-lab-run',
                    '--use-aliases','--no-deps','driver')+$command)
    $inspect = & docker inspect witw-clustering-lab-run witw-clustering-lab-worker-1 witw-clustering-lab-master-1
    $inspect | Set-Content -Encoding utf8 (Join-Path $results "$name.containers.json")
    do {
        Start-Sleep -Seconds 10
        $state = & docker inspect --format '{{.State.Status}}' witw-clustering-lab-run
        if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect lab driver' }
    } while ($state -in @('running','created'))
    $exitCode = & docker inspect --format '{{.State.ExitCode}}' witw-clustering-lab-run
    Invoke-Docker @('compose','-f',$compose,'stop','worker')
    Invoke-Docker @('cp',"witw-clustering-lab-run:/results/$name",$results)
    & docker logs witw-clustering-lab-run 2>&1 | Out-File -Encoding utf8 (Join-Path $results "$name.console.log")
    Invoke-Docker @('rm','witw-clustering-lab-run')
    if ($exitCode -ne '0') { throw "Experiment failed: $name (exit $exitCode)" }
    python (Join-Path $lab 'validate_results.py') (Join-Path $results $name)
    if ($LASTEXITCODE -ne 0) { throw "Validation failed: $name" }
    Write-Output "Completed and validated: $name"
}
try {
    if ($Mode -in @('Quick','All')) { Run-Case 4 $true }
    if ($Mode -in @('Full','All')) {
        if ($Mode -eq 'Full') {
            $passed = Get-ChildItem -LiteralPath $results -Filter SUCCESS.json -Recurse | Where-Object {
                $data = Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json
                $environment = Get-Content -LiteralPath (Join-Path $_.DirectoryName 'environment.json') -Raw | ConvertFrom-Json
                $currentHash = (Get-FileHash -LiteralPath (Join-Path $lab 'benchmark.py') -Algorithm SHA256).Hash.ToLowerInvariant()
                $data.quick -and $data.equal -and $data.complete -and
                    (Test-Path (Join-Path $_.DirectoryName 'VALIDATED.json')) -and
                    -not (Test-Path (Join-Path $_.DirectoryName 'EXCLUDED.md')) -and
                    $environment.input_backend -eq 'untimed_parquet_then_cached_dataframe' -and
                    $environment.benchmark_sha256 -eq $currentHash
            }
            if (-not $passed) { throw 'Run and validate Quick Test before Full mode' }
        }
        foreach ($core in $Cores) { Run-Case $core $false }
    }
} finally {
    # Only this lab project; preserve named volume, output and all production containers.
    & docker compose -f $compose down
}
