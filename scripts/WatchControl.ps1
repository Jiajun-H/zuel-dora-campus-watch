param([ValidateSet('start','status','stop')][string]$Action = 'status')
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)

function Read-WatchState {
    return ((& (Join-Path $PSScriptRoot 'GetRewardWatch.ps1') | Out-String) | ConvertFrom-Json)
}

function Format-WatchTime($value) {
    if (-not $value) { return '暂无' }
    return [DateTimeOffset]::FromUnixTimeSeconds([long][Math]::Floor([double]$value)).ToOffset([TimeSpan]::FromHours(8)).ToString('MM-dd HH:mm:ss')
}

function Show-WatchState($view) {
    if ($view.running) {
        $displayState = if ($view.monitor) { $view.monitor.state } else { $view.state }
        switch ($displayState) {
            'waiting' { Write-Host '正在监控：等待下一次检查。' -ForegroundColor Green }
            'scanning' { Write-Host '正在监控：正在读取并检查帖子。' -ForegroundColor Green }
            'starting' { Write-Host '正在启动：尚未确认完成首次检查。' -ForegroundColor Yellow }
            default { Write-Host '进程存活：请核对最新状态。' -ForegroundColor Yellow }
        }
        Write-Host ('检查间隔：{0} 秒；已完成 {1} 轮。' -f $view.monitor.interval_seconds, $view.monitor.rounds)
        if ($view.monitor.last_summary.anchor_quorum_used) {
            Write-Host ('本轮少量旧帖未出现，稳定普通重叠 {0}/{1} 条（至少需 {2} 条），已继续窗口检查。' -f
                $view.monitor.last_summary.ordinary_anchor_count, $view.monitor.last_summary.previous_anchor_count,
                $view.monitor.last_summary.required_anchor_count)
        }
        if ($view.monitor.last_summary.coverage_gap) {
            Write-Host '已完成长中断后的恢复检查：本轮未知积压不补推，下一轮继续检查新观察到的帖子。' -ForegroundColor Yellow
        }
        if ($view.PSObject.Properties.Name -contains 'windowless' -and -not $view.windowless) {
            Write-Host '当前实例使用旧终端启动方式。请先“停止监控”，再“启动监控”，切换到已修复的无窗口后台方式。' -ForegroundColor Yellow
        }
    } else {
        $displayState = if ($view.runtime_state) { $view.runtime_state } elseif ($view.monitor) { $view.monitor.state } else { $view.state }
        switch ($displayState) {
            'paused' {
                Write-Host '已暂停：当前没有自动检查，请先处理暂停原因。' -ForegroundColor Yellow
                $labels = @{
                    'round_failed_requires_review' = '接口、分页、分类/正文或推送失败；请查看最近错误，不要反复重启。'
                    'attempted_pending_requires_review' = '存在发送结果需复核的待发通知；先核对手机和记录，避免重复。'
                    'observation_gap_requires_review' = '观察中断过久；恢复检查已保守处理，请复核后再启动。'
                    'insufficient_daily_round_budget' = '当天请求预算不足；不能通过重启清零。'
                    'fresh_parameter_missing' = '缺少本次悬赏参数。'
                    'visibility_snapshot_requires_rebaseline' = '需要人工核查并重新建立静默基线。'
                    'startup_or_runtime_failed' = '启动或运行失败；可把不含正文的错误类型反馈给 Codex。'
                }
                $reason = [string]$view.monitor.reason
                if ($labels.ContainsKey($reason)) { Write-Host $labels[$reason] }
                Write-Host ('原因代码：{0}' -f $reason)
                if ($view.monitor.error_type) { Write-Host ('错误类型：{0}' -f $view.monitor.error_type) }
                if ($view.monitor.last_summary.error_type) { Write-Host ('最近错误：{0}' -f $view.monitor.last_summary.error_type) }
                if ($view.monitor.last_summary.warnings -contains 'page_limit_visibility_coverage_not_completed') {
                    Write-Host ('分页覆盖未完成：已读 {0} 页，仍有 {1} 条上一轮已见帖子没找到；已达到每轮页数上限，所以自动暂停。' -f
                        $view.monitor.last_summary.pages, $view.monitor.last_summary.missing_anchor_count)
                    if ($view.monitor.last_summary.previous_anchor_count -gt 0) {
                        Write-Host ('普通重叠 {0}/{1} 条（至少需 {2} 条），还须顺序及前后区域一致；未满足安全衔接条件。' -f
                            $view.monitor.last_summary.ordinary_anchor_count, $view.monitor.last_summary.previous_anchor_count,
                            $view.monitor.last_summary.required_anchor_count)
                    }
                    Write-Host '需要核查重叠不足、顺序变化或列表不一致；直接重复启动可能再次暂停。'
                }
                if ($view.monitor.last_summary.partial_candidate_count -gt 0) {
                    Write-Host ('本轮 {0} 条未知候选仅记录诊断，尚未取得推送资格；历史与去重记录保留。' -f
                        $view.monitor.last_summary.partial_candidate_count)
                }
            }
            'stopped' { Write-Host '已停止：当前没有自动检查。' -ForegroundColor Cyan }
            'not_started' { Write-Host '尚未启动。' -ForegroundColor Cyan }
            'failed_exit' { Write-Host '监控已异常退出：当前没有自动检查，脱敏故障记录可供核查。' -ForegroundColor Yellow }
            'unexpected_exit' { Write-Host '监控进程已消失：旧状态仍显示运行，但没有退出原因证据。不能把旧waiting当作仍在监控。' -ForegroundColor Yellow }
            'exited' { Write-Host '监控进程已退出：当前没有自动检查。' -ForegroundColor Cyan }
            'state_publication_failed' { Write-Host '后台状态发布失败：进程可能正在退出，不能确认正常监控。请查看故障记录，不要重复启动。' -ForegroundColor Yellow }
            'status_unavailable' { Write-Host '后台进程存活，但状态尚未发布或暂时无法读取；不能确认正常监控，请稍后核查。' -ForegroundColor Yellow }
            default { Write-Host '当前没有运行中的监控进程；旧状态文件不能证明它还在运行。' -ForegroundColor Yellow }
        }
    }
    if ($view.last_runtime_failure.event -in 'unhandled_exception','state_write_failed','state_publish_failed','watch_runtime_failed','ledger_cleanup_failed') {
        Write-Host ('最近后台故障：{0}；错误类型：{1}；阶段：{2}；Windows代码：{3}；尝试：{4}。' -f
            $view.last_runtime_failure.event, $view.last_runtime_failure.error_type, $view.last_runtime_failure.stage,
            $view.last_runtime_failure.winerror, $view.last_runtime_failure.attempts)
    }
    if ($view.last_process_exit.event -eq 'process_exit' -and
        ($view.last_process_exit.exit_code -is [int] -or $view.last_process_exit.exit_code -is [long])) {
        Write-Host ('后台退出代码：{0}。' -f $view.last_process_exit.exit_code)
    }
    if ($view.guardian_alive) { Write-Host '异常退出提醒守护：正在运行。' }
    elseif ($view.running -and $view.PSObject.Properties.Name -contains 'guardian_alive') {
        Write-Host '异常退出提醒守护未运行；当前无法保证进程退出后的自动提醒。' -ForegroundColor Yellow
    }
    elseif ($view.guardian -and $view.guardian.state -in 'starting','watching','notifying') {
        Write-Host '异常退出提醒守护也已消失，未完成告警；不能保证退出提醒送达。' -ForegroundColor Yellow
        if ($view.guardian.updated_at) { Write-Host ('守护最后更新时间：{0}' -f (Format-WatchTime $view.guardian.updated_at)) }
    }
    if ($view.guardian -and $view.guardian.alarm_state -ne 'none') {
        Write-Host ('异常退出告警：{0}；本次尝试 {1} 次；累计告警请求 {2} 次。' -f
            $view.guardian.alarm_state, $view.guardian.alarm_attempts, $view.guardian.alarm_requests)
    }
    if ($view.guardian_failure.event -in 'unhandled_exception','ledger_cleanup_failed') {
        Write-Host ('守护故障类型：{0}；阶段：{1}。' -f $view.guardian_failure.error_type, $view.guardian_failure.stage)
    }
    if ($view.guardian_exit.event -eq 'process_exit' -and
        ($view.guardian_exit.exit_code -is [int] -or $view.guardian_exit.exit_code -is [long])) {
        Write-Host ('守护退出代码：{0}。' -f $view.guardian_exit.exit_code)
    }
    if ($view.monitor) {
        Write-Host ('状态更新时间：{0}' -f (Format-WatchTime $view.monitor.updated_at))
        if ($view.running -and $view.monitor.next_check_at -and $view.monitor.state -eq 'waiting') {
            Write-Host ('下一次计划检查：{0}' -f (Format-WatchTime $view.monitor.next_check_at))
        }
        Write-Host ('今天列表请求 {0} 次、参数请求 {1} 次、Bark 请求 {2} 次。' -f
            $view.monitor.requests_today.list_get, $view.monitor.requests_today.fresh_get, $view.monitor.requests_today.bark_post)
        if ($view.monitor.last_summary) {
            Write-Host ('最近一轮：新增 {0} 条，Bark 接受 {1} 条，待发 {2} 条。' -f
                $view.monitor.last_summary.new_count, $view.monitor.last_summary.delivered_count, $view.monitor.last_summary.pending_count)
        }
    }
    Write-Host '提示：Bark 接受数不等于手机已收件；新通知请在手机核对。'
}

try {
    Write-Host ''
    Write-Host '中南大悬赏监控 · 操作入口' -ForegroundColor Cyan
    Write-Host '配置：每 60 秒，最近 2 页，最多 3 页，原帖预览。'
    Write-Host ''
    $view = Read-WatchState
    switch ($Action) {
        'status' { Show-WatchState $view }
        'start' {
            if ($view.running) {
                Write-Host '监控已经在运行，无需重复启动，也无需再次输入 Key。' -ForegroundColor Green
                Show-WatchState $view
                return
            }
            Write-Host '正在准备启动；如需输入 Bark，请粘贴你自己的地址，输入不会显示。'
            $launchRequestedAt = ([DateTime]::UtcNow - [DateTime]'1970-01-01T00:00:00Z').TotalSeconds
            $launch = ((& (Join-Path $PSScriptRoot 'StartRewardWatch.ps1') | Out-String) | ConvertFrom-Json)
            if ($launch.status -eq 'already_running') { Show-WatchState (Read-WatchState); return }
            if ($launch.status -ne 'launching') {
                Write-Host '没有启动成功：请检查 Bark 输入与本机运行环境。' -ForegroundColor Yellow
                if ($launch.error_type) { Write-Host ('错误类型：{0}' -f $launch.error_type) }
                return
            }
            Write-Host '进程已创建，正在确认首次检查；请稍候……'
            if ($launch.launch_requested_at) { $launchRequestedAt = [double]$launch.launch_requested_at }
            $deadline = [DateTime]::UtcNow.AddSeconds(35)
            do {
                Start-Sleep -Seconds 1
                $view = Read-WatchState
                $freshState = $view.monitor -and ([double]$view.monitor.started_at -ge $launchRequestedAt)
                if ($freshState -and $view.monitor.state -in 'waiting','paused','stopped') { break }
            } while ([DateTime]::UtcNow -lt $deadline)
            if ($freshState) { Show-WatchState $view } else {
                Write-Host '暂未确认启动完成。请稍后双击“查看监控状态”，不要连续点击启动。' -ForegroundColor Yellow
            }
            Write-Host '首次使用仅建立历史基线，不推送旧帖；已有基线的再次启动沿用去重记录。'
            if ($freshState -and $view.running -and $view.windowless -ne $false) {
                Write-Host '关闭本窗口后，监控仍在后台运行；停止请双击“停止监控”。'
            }
        }
        'stop' {
            if (-not $view.process_alive -and -not $view.running -and -not $view.guardian_alive) {
                Write-Host '当前没有自动监控在运行，无需再停止。'
                Show-WatchState $view
                return
            }
            $null = & (Join-Path $PSScriptRoot 'StopRewardWatch.ps1')
            Write-Host '停止请求已发送，正在等待当前有界请求结束……'
            $deadline = [DateTime]::UtcNow.AddSeconds(20)
            do {
                Start-Sleep -Seconds 1
                $view = Read-WatchState
                if (-not $view.process_alive -and -not $view.running -and -not $view.guardian_alive) { break }
            } while ([DateTime]::UtcNow -lt $deadline)
            if (-not $view.process_alive -and -not $view.running -and -not $view.guardian_alive) {
                Write-Host '已确认没有自动检查在运行。' -ForegroundColor Green
                Show-WatchState $view
            } else {
                Write-Host '停止尚未确认；请稍后查看状态。没有强制结束进程。' -ForegroundColor Yellow
            }
        }
    }
} catch {
    Write-Host '操作未完成。请保留这个窗口，把操作名称和错误类型反馈给 Codex。' -ForegroundColor Red
    Write-Host ('错误类型：{0}' -f $_.Exception.GetType().Name)
    Write-Host '请勿粘贴设备 Key、帖子正文或联系方式。'
    exit 1
}
