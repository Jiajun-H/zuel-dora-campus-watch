$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$stopPath = Join-Path $projectRoot 'data/reward_watch.stop.json'
$view = ((& (Join-Path $PSScriptRoot 'GetRewardWatch.ps1') | Out-String) | ConvertFrom-Json)
if (-not $view.process_alive) {
    @{ status = 'not_running'; message = 'No active watcher; existing failure evidence is unchanged.' } | ConvertTo-Json
    return
}
@{ pid = [int]$view.monitor.pid; started_at = [double]$view.monitor.started_at;
   requested_at = ([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0) } | ConvertTo-Json -Compress |
    Set-Content -LiteralPath $stopPath -Encoding utf8
@{ status = 'stop_requested'; message = 'Current bounded request may finish; use GetRewardWatch.ps1 to verify stopped.' } | ConvertTo-Json
