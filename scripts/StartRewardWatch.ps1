param([switch]$ChangeBarkKey)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $projectRoot
try {
    $currentState = (& (Join-Path $PSScriptRoot 'GetRewardWatch.ps1') | Out-String) | ConvertFrom-Json
    if ($currentState.running) {
        @{ status = 'already_running'; pid = $currentState.monitor.pid } | ConvertTo-Json -Compress
        return
    }
    if ($currentState.process_alive) {
        @{ status = 'not_launched'; error_type = 'ExistingProcessNeedsReview' } | ConvertTo-Json -Compress
        return
    }
    if ($currentState.guardian_alive) {
        @{ status = 'not_launched'; error_type = 'SupervisorStillFinishing' } | ConvertTo-Json -Compress
        return
    }
    $uvCommand = Get-Command uv -CommandType Application -ErrorAction SilentlyContinue
    $uvPath = if ($uvCommand) { $uvCommand.Source } else { Join-Path $env:USERPROFILE '.local\bin\uv.exe' }
    $pythonPath = Join-Path $projectRoot '.venv\Scripts\python.exe'
    if (-not (Test-Path -LiteralPath $uvPath) -and -not (Test-Path -LiteralPath $pythonPath)) {
        throw 'Local Python environment is unavailable.'
    }
    $barkValue = $env:BARK_KEY
    $savedKeyAvailable = $false
    if (-not $barkValue -and -not $ChangeBarkKey) {
        if (Test-Path -LiteralPath $uvPath) {
            $keyStatusText = (& $uvPath run --locked python -X utf8 .\scripts\launch_watch.py --key-status | Out-String)
        } else {
            $keyStatusText = (& $pythonPath -X utf8 .\scripts\launch_watch.py --key-status | Out-String)
        }
        if ($LASTEXITCODE -ne 0) {
            @{ status = 'not_launched'; error_type = 'SavedKeyUnavailable' } | ConvertTo-Json -Compress
            return
        }
        $savedKeyAvailable = [bool](($keyStatusText | ConvertFrom-Json).key_available)
    }
    if ($ChangeBarkKey -or (-not $barkValue -and -not $savedKeyAvailable)) {
        $secureValue = Read-Host '请输入你自己的 Bark 推送地址或设备 Key（输入隐藏，粘贴后按回车）' -AsSecureString
        $secretPointer = [IntPtr]::Zero
        try {
            $secretPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureValue)
            $barkValue = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($secretPointer)
        } finally {
            if ($secretPointer -ne [IntPtr]::Zero) { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($secretPointer) }
            $secureValue.Dispose()
        }
    }
    # Only pipe to launcher memory; never store the seed or key in a file.
    $OutputEncoding = New-Object System.Text.UTF8Encoding($false)
    if (Test-Path -LiteralPath $uvPath) {
        $launcherOutput = (@{ bark = $barkValue; fresh = $env:DORA_FRESH_KEY_14 } | ConvertTo-Json -Compress |
            & $uvPath run --locked python -X utf8 .\scripts\launch_watch.py | Out-String)
    } else {
        $launcherOutput = (@{ bark = $barkValue; fresh = $env:DORA_FRESH_KEY_14 } | ConvertTo-Json -Compress |
            & $pythonPath -X utf8 .\scripts\launch_watch.py | Out-String)
    }
    if ($LASTEXITCODE -ne 0) {
        @{ status = 'not_launched'; error_type = 'LauncherFailed' } | ConvertTo-Json -Compress
        return
    }
    $launcherOutput | ConvertFrom-Json | ConvertTo-Json -Compress
} finally {
    $barkValue = $null
    Pop-Location
}
