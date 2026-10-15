$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$statePath = Join-Path $projectRoot 'data/reward_watch.status.json'

function Read-SharedText([string]$path) {
    $share = [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete
    $stream = [System.IO.File]::Open($path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, $share)
    $reader = New-Object System.IO.StreamReader($stream, [System.Text.Encoding]::UTF8, $true)
    try { return $reader.ReadToEnd() } finally { $reader.Dispose() }
}

function Read-SafeEvents([string]$path) {
    $result = @()
    if (-not (Test-Path -LiteralPath $path)) { return $result }
    try {
        if ((Get-Item -LiteralPath $path).Length -gt 524288) { return $result }
        $lines = (Read-SharedText $path) -split "`n"
        foreach ($line in $lines) {
            if (-not $line.Trim()) { continue }
            if ([System.Text.Encoding]::UTF8.GetByteCount($line) -gt 2048) { continue }
            try { $event = $line | ConvertFrom-Json } catch { continue }
            if ($event.schema -ne 'campus-watch-runtime-v1' -or
                $event.event -notin 'process_started','process_exit','unhandled_exception','state_write_failed','state_publish_failed','watch_runtime_failed','ledger_cleanup_failed') { continue }
            if (($event.pid -isnot [int] -and $event.pid -isnot [long]) -or
                $event.pid -lt 1 -or $event.pid -gt 2147483647 -or $event.at -is [bool]) { continue }
            $at = [double]$event.at
            if ([double]::IsNaN($at) -or [double]::IsInfinity($at) -or $at -le 0) { continue }
            $safe = @{ event=[string]$event.event; pid=[int]$event.pid; at=$at }
            if ($event.started_at -and $event.started_at -isnot [bool]) {
                $start = [double]$event.started_at
                if (-not [double]::IsNaN($start) -and -not [double]::IsInfinity($start) -and $start -gt 0) { $safe.started_at=$start }
                else { continue }
            }
            foreach ($name in 'exit_code','errno','winerror','attempts') {
                if (($event.$name -is [int] -or $event.$name -is [long]) -and $event.$name -ge -2147483648 -and $event.$name -le 4294967295) { $safe[$name]=[long]$event.$name }
            }
            if ($event.error_type -in 'StateWriteError','PermissionError','OSError','RuntimeError','ValueError','TypeError','JSONDecodeError','FileNotFoundError','ModuleNotFoundError','ImportError','KeyboardInterrupt','SystemExit','StoreBusy','OperationalError','UnexpectedError','TimeoutError','DatabaseError','InterruptedError','AssertionError','UnicodeError') { $safe.error_type=[string]$event.error_type }
            if ($event.stage -in 'startup','import','cli','run','state_write','state_publish','temp_write','temporary_write','replace','ledger_cleanup','exit') { $safe.stage=[string]$event.stage }
            $result += [pscustomobject]$safe
        }
    } catch { return @() }
    return @($result | Select-Object -Last 100)
}

$guardian = $null
$guardianAlive = $false
$guardianFault = $null
$guardianExit = $null
$guardianPath = Join-Path $projectRoot 'data/reward_watch.supervisor.json'
if (Test-Path -LiteralPath $guardianPath) {
    try {
        $candidate = (Read-SharedText $guardianPath) | ConvertFrom-Json
        $validGuardPid = ($candidate.supervisor_pid -is [int] -or $candidate.supervisor_pid -is [long]) -and
            $candidate.supervisor_pid -ge 1 -and $candidate.supervisor_pid -le 2147483647
        $validGuardStart = ($candidate.started_at -is [double] -or $candidate.started_at -is [decimal] -or
            $candidate.started_at -is [int] -or $candidate.started_at -is [long]) -and
            $candidate.started_at -gt 0 -and $candidate.started_at -lt 253402300799
        if ($candidate.schema -eq 'campus-watch-supervisor-v1' -and $validGuardPid -and $validGuardStart -and
            $candidate.state -in 'starting','watching','notifying','stopped','failed') {
            $guardian = @{ pid=[int]$candidate.supervisor_pid; started_at=[double]$candidate.started_at; state=[string]$candidate.state }
            if (($candidate.updated_at -is [double] -or $candidate.updated_at -is [decimal] -or
                $candidate.updated_at -is [int] -or $candidate.updated_at -is [long]) -and
                $candidate.updated_at -gt 0 -and $candidate.updated_at -lt 253402300799) { $guardian.updated_at=[double]$candidate.updated_at }
            if ($candidate.alarm_state -in 'none','pending','sending','sent','exhausted','configuration_missing','evidence_failed') { $guardian.alarm_state=[string]$candidate.alarm_state }
            if (($candidate.alarm_attempts -is [int] -or $candidate.alarm_attempts -is [long]) -and $candidate.alarm_attempts -ge 0 -and $candidate.alarm_attempts -le 3) { $guardian.alarm_attempts=[int]$candidate.alarm_attempts }
            if (($candidate.alarm_requests -is [int] -or $candidate.alarm_requests -is [long]) -and $candidate.alarm_requests -ge 0) { $guardian.alarm_requests=[long]$candidate.alarm_requests }
            if ($candidate.reason_code -in 'child_starting','child_running','manual_stop','duplicate_instance','monitor_paused','monitor_failed','unexpected_exit','unexpected_clean_exit','child_launch_failed','outbox_failed','supervisor_failed') { $guardian.reason_code=[string]$candidate.reason_code }
            $guardProcess = Get-Process -Id $guardian.pid -ErrorAction SilentlyContinue
            if ($guardProcess -and $guardProcess.ProcessName -like 'python*') {
                $guardEpoch = ([DateTimeOffset]$guardProcess.StartTime.ToUniversalTime()).ToUnixTimeSeconds()
                $guardianAlive = [Math]::Abs($guardEpoch - $guardian.started_at) -lt 10
            }
        }
    } catch { $guardian=$null; $guardianAlive=$false }
}
$guardEvents = @(Read-SafeEvents (Join-Path $projectRoot 'data/reward_watch.supervisor.runtime.jsonl'))
$recentGuardStart = $guardEvents | Where-Object event -eq 'process_started' | Select-Object -Last 1
if ($recentGuardStart -and -not $guardianAlive -and
    (-not $guardian -or $recentGuardStart.at -gt $guardian.started_at + 1)) {
    $guardian = @{pid=[int]$recentGuardStart.pid; started_at=[double]$recentGuardStart.at;
        updated_at=[double]$recentGuardStart.at; state='starting'; alarm_state='none'; alarm_attempts=0; alarm_requests=0}
    $guardProcess = Get-Process -Id $guardian.pid -ErrorAction SilentlyContinue
    if ($guardProcess -and $guardProcess.ProcessName -like 'python*') {
        $guardEpoch = ([DateTimeOffset]$guardProcess.StartTime.ToUniversalTime()).ToUnixTimeSeconds()
        $guardianAlive = [Math]::Abs($guardEpoch - $guardian.started_at) -lt 10
    }
}
if ($guardian) {
    foreach ($guardEvent in $guardEvents) {
        if ($guardEvent.pid -eq $guardian.pid -and $guardEvent.at -ge $guardian.started_at - 1 -and
            (-not $guardEvent.started_at -or [Math]::Abs($guardEvent.started_at - $guardian.started_at) -lt 10)) {
            if ($guardEvent.event -in 'unhandled_exception','ledger_cleanup_failed') { $guardianFault=$guardEvent }
            if ($guardEvent.event -eq 'process_exit') { $guardianExit=$guardEvent }
        }
    }
}
$watchState = $null
$stateReadable = $false
if (Test-Path -LiteralPath $statePath) {
    try {
        $watchState = (Read-SharedText $statePath) | ConvertFrom-Json
        $validPid = ($watchState.pid -is [int] -or $watchState.pid -is [long]) -and
            $watchState.pid -ge 1 -and $watchState.pid -le 2147483647
        $validStart = ($watchState.started_at -is [double] -or $watchState.started_at -is [decimal] -or $watchState.started_at -is [int] -or $watchState.started_at -is [long]) -and
            -not [double]::IsNaN([double]$watchState.started_at) -and -not [double]::IsInfinity([double]$watchState.started_at) -and
            $watchState.started_at -gt 0 -and $watchState.started_at -lt 253402300799
        $stateReadable = [bool]($validPid -and $validStart -and
            $watchState.state -in 'starting','scanning','waiting','paused','stopped')
        if (-not $stateReadable) { $watchState = $null }
    } catch { $watchState = $null }
}
$failureEvents = @(Read-SafeEvents (Join-Path $projectRoot 'data/reward_watch.status.failures.jsonl'))
$runtimeEvents = @(Read-SafeEvents (Join-Path $projectRoot 'data/reward_watch.runtime.jsonl'))
$recentStart = $runtimeEvents | Where-Object event -eq 'process_started' | Select-Object -Last 1
$originalAlive = $false
if ($watchState) {
    $originalProcess = Get-Process -Id ([int]$watchState.pid) -ErrorAction SilentlyContinue
    if ($originalProcess -and $originalProcess.ProcessName -like 'python*') {
        $originalEpoch = ([DateTimeOffset]$originalProcess.StartTime.ToUniversalTime()).ToUnixTimeSeconds()
        $originalAlive = [Math]::Abs($originalEpoch - [double]$watchState.started_at) -lt 10
    }
}
$evidenceStart = $recentStart -and -not $originalAlive -and
    (-not $stateReadable -or $recentStart.at -gt [double]$watchState.started_at + 1)
if ($evidenceStart) {
    $watchState = [pscustomobject]@{ pid=$recentStart.pid; started_at=$recentStart.at; state='starting'; reason='awaiting_state_publication' }
    $stateReadable = $false
}
if (-not $watchState) {
    @{ running=$false; process_alive=$false; windowless=$false; state='not_started'; state_readable=$false;
       guardian_alive=$guardianAlive; guardian=$guardian; guardian_failure=$guardianFault; guardian_exit=$guardianExit } | ConvertTo-Json -Depth 8
    return
}
$watchProcess = Get-Process -Id ([int]$watchState.pid) -ErrorAction SilentlyContinue
$processAlive = $false
if ($watchProcess -and $watchProcess.ProcessName -like 'python*') {
    $processEpoch = ([DateTimeOffset]$watchProcess.StartTime.ToUniversalTime()).ToUnixTimeSeconds()
    $processAlive = [Math]::Abs($processEpoch - [double]$watchState.started_at) -lt 10
}
$matchingEvents = @(@($failureEvents) + @($runtimeEvents) | Where-Object {
    $_.pid -eq [int]$watchState.pid -and $_.at -ge [double]$watchState.started_at - 1 -and
    (-not $_.started_at -or [Math]::Abs($_.started_at - [double]$watchState.started_at) -lt 10)
} | Sort-Object at)
$lastFault = $matchingEvents | Where-Object event -in 'unhandled_exception','state_write_failed','state_publish_failed','watch_runtime_failed','ledger_cleanup_failed' | Select-Object -Last 1
$lastExit = $matchingEvents | Where-Object event -eq 'process_exit' | Select-Object -Last 1
$running = $processAlive -and $watchState.state -in 'starting','scanning','waiting'
$runtimeState = $null
if (-not $processAlive) {
    if ($lastFault -or ($lastExit -and $lastExit.exit_code -ne 0)) { $runtimeState='failed_exit' }
    elseif ($watchState.state -in 'starting','scanning','waiting') { $runtimeState=if($lastExit){'exited'}else{'unexpected_exit'} }
} elseif ($lastFault -and $lastFault.event -in 'state_write_failed','state_publish_failed') {
    $runtimeState='state_publication_failed'
    $running=$false
} elseif (-not $stateReadable) {
    $runtimeState='status_unavailable'
    $running=$false
}
@{ running=$running; process_alive=$processAlive; state_readable=[bool]$stateReadable;
   windowless=($processAlive -and $watchProcess.ProcessName -eq 'pythonw'); monitor=$watchState;
   runtime_state=$runtimeState; last_runtime_failure=$lastFault; last_process_exit=$lastExit;
   guardian_alive=$guardianAlive; guardian=$guardian; guardian_failure=$guardianFault; guardian_exit=$guardianExit } | ConvertTo-Json -Depth 14
