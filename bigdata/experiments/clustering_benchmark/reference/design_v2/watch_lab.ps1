# Read progress through Docker's daemon; do not run extra processes inside the timed Driver.
param([int]$IntervalSeconds = 50, [string]$RunTag = 'main-v2')
$destination = Join-Path $PSScriptRoot 'results/progress-snapshot.csv'
while ($true) {
    $raw = & docker inspect witw-clustering-lab-run 2>$null
    if ($LASTEXITCODE -eq 0) {
        $container = ($raw | ConvertFrom-Json)[0]
        $arguments = [string[]]$container.Config.Cmd
        $index = [Array]::IndexOf($arguments, '--output')
        if ($index -ge 0) {
            $directory = $arguments[$index + 1]
            & docker cp "witw-clustering-lab-run:$directory/runs.csv" $destination 2>$null
            if ($LASTEXITCODE -eq 0) {
                $rows = @(Import-Csv -LiteralPath $destination | Where-Object { $_.equal -in @('True','False') })
                $measured = @($rows | Where-Object { $_.phase -in @('calibration','holdout') })
                $last = $rows | Select-Object -Last 1
                [pscustomobject]@{
                    run = Split-Path $directory -Leaf
                    measured = $measured.Count
                    target = 1260
                    equal = @($rows | Where-Object { $_.equal -eq 'False' }).Count -eq 0
                    phase = $last.phase
                    scenario = $last.scenario
                    seed = $last.seed
                    rep = $last.rep
                } | ConvertTo-Json -Compress | Write-Output
            }
        }
    } else {
        $passed = @(Get-ChildItem (Join-Path $PSScriptRoot 'results') -Filter VALIDATED.json -Recurse | Where-Object {
            $_.Directory.Name -like "full-*-$RunTag"
        })
        if ($passed.Count -eq 3) { Write-Output 'ALL_FULL_SESSIONS_VALIDATED'; break }
        Write-Output 'Waiting for next lab session or validation.'
    }
    Start-Sleep -Seconds $IntervalSeconds
}
