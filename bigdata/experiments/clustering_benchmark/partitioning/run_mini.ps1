param([string]$RunTag = (Get-Date -Format 'yyyyMMdd-HHmmss'))
$ErrorActionPreference = 'Stop'
$lab = Split-Path $PSScriptRoot -Parent
$compose = Join-Path $lab 'compose.yml'
$results = Join-Path $lab 'results'
$costModel = $null
function Invoke-Docker {
    param([string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) { throw "docker failed: $($Arguments -join ' ')" }
}
try {
    foreach ($core in @(1,2,4)) {
        $name = "partition-mini-$core-$RunTag"
        if (Test-Path (Join-Path $results $name)) { throw "Output already exists: $name" }
        $env:LAB_CORES = "$core"
        $env:LAB_RUN = $name
        Invoke-Docker @('compose','-f',$compose,'up','-d','master','worker')
        $command = @('/opt/spark/bin/spark-submit','--master','spark://master:7077',
            '--driver-memory','1536m','--executor-memory','3g','--conf',"spark.cores.max=$core",
            '--conf',"spark.executor.cores=$core",'--conf','spark.driver.host=driver',
            '--conf','spark.driver.bindAddress=0.0.0.0','--conf','spark.executor.processTreeMetrics.enabled=true',
            '--conf','spark.executor.metrics.pollingInterval=250','--conf','spark.eventLog.logStageExecutorMetrics=true',
            '/bench/partitioning/mini_benchmark.py','--source','/source','--cores',"$core",'--output',"/results/$name")
        if ($costModel) { $command += @('--cost-model',$costModel) }
        Invoke-Docker -Arguments (@('compose','-f',$compose,'run','-d','--name','witw-clustering-lab-mini',
            '--use-aliases','--no-deps','driver')+$command)
        & docker inspect witw-clustering-lab-mini witw-clustering-lab-worker-1 witw-clustering-lab-master-1 |
            Set-Content -Encoding utf8 (Join-Path $results "$name.containers.json")
        do {
            Start-Sleep -Seconds 5
            $state = & docker inspect --format '{{.State.Status}}' witw-clustering-lab-mini
        } while ($state -in @('created','running'))
        $exitCode = & docker inspect --format '{{.State.ExitCode}}' witw-clustering-lab-mini
        Invoke-Docker @('compose','-f',$compose,'stop','worker')
        Invoke-Docker @('cp',"witw-clustering-lab-mini:/results/$name",$results)
        & docker logs witw-clustering-lab-mini 2>&1 | Out-File -Encoding utf8 (Join-Path $results "$name.console.log")
        Invoke-Docker @('rm','witw-clustering-lab-mini')
        if ($exitCode -ne '0') { throw "Mini test failed: $name" }
        python (Join-Path $PSScriptRoot 'validate_mini.py') (Join-Path $results $name)
        if ($LASTEXITCODE -ne 0) { throw "Mini validation failed: $name" }
        if (-not $costModel) { $costModel = "/results/$name/cost_model.json" }
        Write-Output "Mini test validated: $name"
    }
} finally {
    & docker compose -f $compose down
}
