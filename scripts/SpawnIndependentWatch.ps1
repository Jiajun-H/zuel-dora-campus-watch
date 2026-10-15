$ErrorActionPreference = 'Stop'
[Console]::InputEncoding = New-Object System.Text.UTF8Encoding($false)
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
try {
    $inputValue = ([Console]::In.ReadToEnd() | ConvertFrom-Json)
    if ($inputValue.command -isnot [string] -or $inputValue.cwd -isnot [string] -or
        $inputValue.command.Length -gt 16384 -or $inputValue.command.Contains("`n") -or $inputValue.command.Contains("`r")) {
        throw 'InvalidPublicLaunchCommand'
    }
    $startup = New-CimInstance -CimClass (Get-CimClass -ClassName Win32_ProcessStartup) -ClientOnly -Property @{ShowWindow=[uint16]0}
    $result = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
        CommandLine=[string]$inputValue.command; CurrentDirectory=[string]$inputValue.cwd; ProcessStartupInformation=$startup
    }
    @{return_code=[int]$result.ReturnValue; pid=[int]$result.ProcessId} | ConvertTo-Json -Compress
} catch {
    @{return_code=1; error_type='IndependentLaunchError'} | ConvertTo-Json -Compress
    exit 1
}
