<#
.SYNOPSIS
    Pipvane v3: Interactive Desktop Startup Registration & RDP Optimization (Sprint 13).

.DESCRIPTION
    1. Registers the Pipvane Watchdog in HKCU\Software\Microsoft\Windows\CurrentVersion\Run
       so it launches automatically upon Windows Auto-Logon (satisfying FR-1.1 & FR-1.6).
    2. Deploys the RDP Console Disconnect helper (Disconnect-RDPKeepAlive.bat) onto the Desktop
       to prevent Windows VPS from locking the GUI session when disconnecting from Remote Desktop.
    3. Configures Terminal Services policies to ensure headless session rendering continuity.

.PARAMETER Remove
    Switch to deregister Pipvane from HKCU\Run.
#>

[CmdletBinding()]
param (
    [switch]$Remove
)

$runKeyPath = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
$entryName = "PipvaneWatchdog"
$watchdogScript = Join-Path $PSScriptRoot "PipvaneWatchdog.ps1"

if ($Remove) {
    Write-Host "[*] Deregistering Pipvane from HKCU\Run..." -ForegroundColor Yellow
    Remove-ItemProperty -Path $runKeyPath -Name $entryName -ErrorAction SilentlyContinue
    Write-Host "[+] Pipvane startup entry removed." -ForegroundColor Green
    exit 0
}

# -----------------------------------------------------------------------------
# 1. Register Watchdog in HKCU\Run
# -----------------------------------------------------------------------------
Write-Host "[*] Registering Pipvane Watchdog into HKCU\Software\Microsoft\Windows\CurrentVersion\Run..." -ForegroundColor Cyan

# Hidden window, bypass execution policy, non-admin user-space launch
$launchCommand = "powershell.exe -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$watchdogScript`""
Set-ItemProperty -Path $runKeyPath -Name $entryName -Value $launchCommand -Type String -Force

Write-Host "[+] Successfully registered: [$entryName] -> [$launchCommand]" -ForegroundColor Green

# -----------------------------------------------------------------------------
# 2. Deploy RDP Console Transfer Helper (Disconnect-RDPKeepAlive.bat)
# -----------------------------------------------------------------------------
Write-Host "[*] Generating RDP Keep-Alive console disconnect helper..." -ForegroundColor Cyan

$desktopDir = [Environment]::GetFolderPath("Desktop")
$disconnectBat = Join-Path $desktopDir "Disconnect-RDPKeepAlive.bat"

$batContent = @"
@echo off
:: ============================================================================
:: Pipvane v3: RDP Keep-Alive Disconnect Helper
:: Shifts the current RDP remote session back into the local console session.
:: Prevents Windows VPS from locking the screen or pausing GUI message loops.
:: ============================================================================
echo [Pipvane] Transferring RDP session to console to keep MetaTrader GUI active...
for /f "skip=1 tokens=3" %%s in ('query user %USERNAME%') do (
    %windir%\System32\tscon.exe %%s /dest:console
)
if %errorlevel% neq 0 (
    echo [Fallback] Direct tscon session transfer...
    %windir%\System32\tscon.exe %SESSIONNAME% /dest:console
)
"@

Set-Content -Path $disconnectBat -Value $batContent -Encoding ASCII
Write-Host "[+] Created RDP console disconnect shortcut on Desktop: [$disconnectBat]" -ForegroundColor Green

# -----------------------------------------------------------------------------
# 3. Configure Terminal Services Session Timeouts (Policies)
# -----------------------------------------------------------------------------
Write-Host "[*] Optimizing Terminal Server session timeout policies..." -ForegroundColor Cyan
$tsPolicyPath = "HKLM:\SOFTWARE\Policies\Microsoft\Windows NT\Terminal Services"
if (Test-Path "HKLM:\SOFTWARE\Policies\Microsoft\Windows NT") {
    if (-not (Test-Path $tsPolicyPath)) {
        New-Item -Path $tsPolicyPath -Force | Out-Null
    }
    # 0 = Never disconnect or log off disconnected sessions
    Set-ItemProperty -Path $tsPolicyPath -Name "MaxDisconnectionTime" -Value 0 -Type DWord -Force -ErrorAction SilentlyContinue
    Set-ItemProperty -Path $tsPolicyPath -Name "MaxIdleTime" -Value 0 -Type DWord -Force -ErrorAction SilentlyContinue
    Write-Host "[+] Terminal Services session timeouts set to INFINITE." -ForegroundColor Green
}

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Green
Write-Host " [SUCCESS] Interactive Startup and RDP Keep-Alive Configured." -ForegroundColor Green
Write-Host " Pipvane will launch on auto-logon; RDP disconnect will not freeze GUI." -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green
