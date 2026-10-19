param(
    [string]$TargetDirectory,
    [string]$OutputDirectory,
    [ValidateRange(1, 10)][int]$Repeat = 1,
    [ValidateRange(0, 100000)][double]$MiBPerSecond = 60,
    [switch]$NoRestart,
    [switch]$Release,
    [ValidateRange(1, 10)][int]$TimingScale = 1
)
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$repoPath = Split-Path -Parent $PSScriptRoot
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $repoPath ('target/recovery-repro/' + (Get-Date -Format 'yyyyMMdd-HHmmss-fff'))
}
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
if ((Test-Path -LiteralPath $OutputDirectory) -and (Get-ChildItem -LiteralPath $OutputDirectory | Select-Object -First 1)) {
    throw "Choose a new output directory so existing evidence is preserved: $OutputDirectory"
}
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$manifest = [ordered]@{
    git_commit = (& git -C $repoPath rev-parse HEAD)
    git_dirty = [bool](& git -C $repoPath status --porcelain)
    host_os = [Runtime.InteropServices.RuntimeInformation]::OSDescription
    logical_cpus = [Environment]::ProcessorCount
    profile = if ($Release) { 'release' } else { 'debug' }
    restart_injected = -not $NoRestart
    offered_mainnet_mib_s = $MiBPerSecond
    timing_scale = $TimingScale
}
$manifest | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $OutputDirectory 'manifest.json')
$oldReproDir = $env:FLEET_ROUTER_REPRO_DIR
$oldNoRestart = $env:FLEET_ROUTER_REPRO_NO_RESTART
$oldRate = $env:FLEET_ROUTER_REPRO_MIB_S
$oldTimingScale = $env:FLEET_ROUTER_REPRO_TIMING_SCALE
$env:FLEET_ROUTER_REPRO_NO_RESTART = if ($NoRestart) { '1' } else { $null }
$env:FLEET_ROUTER_REPRO_MIB_S = $MiBPerSecond.ToString([Globalization.CultureInfo]::InvariantCulture)
$env:FLEET_ROUTER_REPRO_TIMING_SCALE = "$TimingScale"
$allSummaries = @()
try {
    for ($run = 1; $run -le $Repeat; $run++) {
        $runPath = Join-Path $OutputDirectory "run-$run"
        New-Item -ItemType Directory -Force -Path $runPath | Out-Null
        $env:FLEET_ROUTER_REPRO_DIR = $runPath
        $stopPath = Join-Path $runPath 'sampling.stop'
        $metadataPath = Join-Path $runPath 'metadata.json'
        $cpuPath = Join-Path $runPath 'process-cpu.jsonl'
        $sampler = Start-Job -ArgumentList $metadataPath,$cpuPath,$stopPath -ScriptBlock {
            param($metadataPath,$cpuPath,$stopPath)
            while (-not (Test-Path -LiteralPath $stopPath)) {
                if (Test-Path -LiteralPath $metadataPath) {
                    try {
                        $metadata = Get-Content -Raw -LiteralPath $metadataPath | ConvertFrom-Json
                        $process = Get-Process -Id $metadata.router_pid -ErrorAction Stop
                        # Sample only the fixture's executable, never a recycled PID.
                        $createdMs = ([DateTimeOffset]$process.StartTime).ToUnixTimeMilliseconds()
                        if ($process.ProcessName -eq 'fleet-router' -and $createdMs -le $metadata.started_ms -and $createdMs -ge ($metadata.started_ms - 30000)) {
                            [ordered]@{
                                t_ms = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
                                cpu_seconds = $process.CPU
                                working_set_bytes = $process.WorkingSet64
                                threads = $process.Threads.Count
                                handles = $process.HandleCount
                            } | ConvertTo-Json -Compress | Add-Content -LiteralPath $cpuPath
                        }
                    } catch { }
                }
                Start-Sleep -Milliseconds 200
            }
        }
        try {
            $cargoArguments = @('test','--locked','--test','recovery_repro')
            if ($Release) { $cargoArguments += '--release' }
            if ($TargetDirectory) { $cargoArguments += @('--target-dir',$TargetDirectory) }
            $cargoArguments += @('--','--ignored','--nocapture')
            Push-Location $repoPath
            try {
                & cargo @cargoArguments 2>&1 | Tee-Object -FilePath (Join-Path $runPath 'test-output.log')
                $testExit = $LASTEXITCODE
            } finally { Pop-Location }
        } finally {
            Set-Content -LiteralPath $stopPath -Value 'done'
            Wait-Job $sampler -Timeout 5 | Out-Null
            if ($sampler.State -eq 'Running') { Stop-Job $sampler }
            Receive-Job $sampler | Out-Null
            Remove-Job $sampler
        }
        $summaryPath = Join-Path $runPath 'summary.json'
        if (-not (Test-Path -LiteralPath $summaryPath)) {
            throw "Run $run did not complete the scenario (exit $testExit). Inspect $runPath"
        }
        $summary = Get-Content -Raw -LiteralPath $summaryPath | ConvertFrom-Json
        $reproduced = $summary.restart_injected -and
            $summary.returned_false_lag_samples[0] -gt 0 -and
            $summary.returned_false_lag_samples[1] -gt 0
        $allSummaries += [ordered]@{run=$run; test_exit=$testExit; alternating_lag_reproduced=$reproduced; summary=$summary}
        Write-Output "Run $run`: alternating lag reproduced=$reproduced; evidence=$runPath"
    }
    $allSummaries | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'runs.json')
    Write-Output "Evidence directory: $OutputDirectory"
} finally {
    $env:FLEET_ROUTER_REPRO_DIR = $oldReproDir
    $env:FLEET_ROUTER_REPRO_NO_RESTART = $oldNoRestart
    $env:FLEET_ROUTER_REPRO_MIB_S = $oldRate
    $env:FLEET_ROUTER_REPRO_TIMING_SCALE = $oldTimingScale
}
