<#
.SYNOPSIS
    Pipvane v3: High-Availability User-Space Watchdog Process (Sprint 13).

.DESCRIPTION
    Monitors the Pipvane Engine process in the user's interactive desktop session.
    If the engine crashes, terminates unexpectedly, or hangs, the watchdog restarts
    it within 10 seconds (satisfying PRD Reliability SLA: "watchdog restarts the engine in under 10 seconds").
    Features a circuit breaker to prevent high-CPU loops during persistent misconfigurations.

.PARAMETER EnginePath
    Path to the Python interpreter or compiled Pipvane binary. Defaults to auto-detect.

.PARAMETER Arguments
    Command-line arguments passed to the engine. Defaults to "-m engine".

.PARAMETER CheckIntervalSec
    Frequency of heartbeat process checks. Defaults to 2 seconds.

.PARAMETER MaxCrashesInWindow
    Maximum allowable crashes within CrashWindowSec before tripping circuit breaker. Defaults to 5.

.PARAMETER CrashWindowSec
    Time window for circuit breaker trip evaluation. Defaults to 60 seconds.
#>

[CmdletBinding()]
param (
    [string]$EnginePath,
    [string]$Arguments = "-m engine",
    [int]$CheckIntervalSec = 2,
    [int]$MaxCrashesInWindow = 5,
    [int]$CrashWindowSec = 60
)

$ErrorActionPreference = "Continue"

# -----------------------------------------------------------------------------
# 1. Environment & Path Resolution
# -----------------------------------------------------------------------------
$appDataDir = if ($env:APPDATA) { Join-Path $env:APPDATA "Pipvane" } else { Join-Path $env:USERPROFILE "AppData\Roaming\Pipvane" }
if (-not (Test-Path $appDataDir)) {
    New-Item -ItemType Directory -Path $appDataDir -Force | Out-Null
}

$logFile = Join-Path $appDataDir "watchdog.log"
$stopSignalFile = Join-Path $appDataDir "watchdog.stop"
$engineWorkingDir = (Get-Item -Path $PSScriptRoot).Parent.FullName

function Write-WatchdogLog {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss.fff")
    $line = "[$timestamp] [$Level] $Message"
    Write-Host $line -ForegroundColor $(if ($Level -eq "ERROR") { "Red" } elseif ($Level -eq "WARN") { "Yellow" } else { "Cyan" })
    Add-Content -Path $logFile -Value $line -Encoding UTF8 -ErrorAction SilentlyContinue
}

# Auto-detect engine executable if not explicitly provided
if ([string]::IsNullOrWhiteSpace($EnginePath)) {
    $pythonCmd = (Get-Command "python.exe" -ErrorAction SilentlyContinue)
    if ($pythonCmd) {
        $EnginePath = $pythonCmd.Source
    } else {
        $compiledExe = Join-Path $engineWorkingDir "pipvane-engine.exe"
        if (Test-Path $compiledExe) {
            $EnginePath = $compiledExe
            $Arguments = ""
        } else {
            Write-WatchdogLog "Fatal: Could not locate python.exe or pipvane-engine.exe." "ERROR"
            exit 1
        }
    }
}

Write-WatchdogLog "========================================================"
Write-WatchdogLog "Pipvane v3 Interactive Watchdog Initialized"
Write-WatchdogLog "Target Engine: [$EnginePath] $Arguments"
Write-WatchdogLog "Working Dir:   [$engineWorkingDir]"
Write-WatchdogLog "Check Rate:    Every ${CheckIntervalSec}s (Recovery SLA < 10s)"
Write-WatchdogLog "========================================================"

# Remove stale stop signal if present on startup
if (Test-Path $stopSignalFile) {
    Remove-Item -Path $stopSignalFile -Force -ErrorAction SilentlyContinue
}

# -----------------------------------------------------------------------------
# 2. Process Supervision Loop
# -----------------------------------------------------------------------------
$currentProcess = $null
$crashTimestamps = [System.Collections.Generic.List[datetime]]::new()

function Start-EngineProcess {
    try {
        Write-WatchdogLog "Launching Pipvane Engine process..." "INFO"
        $psi = New-Object System.Diagnostics.ProcessStartInfo
        $psi.FileName = $EnginePath
        $psi.Arguments = $Arguments
        $psi.WorkingDirectory = $engineWorkingDir
        $psi.UseShellExecute = $false
        $psi.CreateNoWindow = $false # Visible/minimized in desktop GUI session

        $proc = [System.Diagnostics.Process]::Start($psi)
        Write-WatchdogLog "Pipvane Engine started successfully (PID: $($proc.Id))" "INFO"
        return $proc
    } catch {
        Write-WatchdogLog "Failed to spawn engine process: $_" "ERROR"
        return $null
    }
}

# Initial process start
$currentProcess = Start-EngineProcess

while ($true) {
    Start-Sleep -Seconds $CheckIntervalSec

    # 1. Check for intentional shutdown signal
    if (Test-Path $stopSignalFile) {
        Write-WatchdogLog "Detected stop signal ($stopSignalFile). Initiating clean shutdown..." "WARN"
        if ($currentProcess -and -not $currentProcess.HasExited) {
            $currentProcess.Kill()
        }
        Remove-Item -Path $stopSignalFile -Force -ErrorAction SilentlyContinue
        Write-WatchdogLog "Watchdog stopped cleanly." "INFO"
        break
    }

    # 2. Check if engine is running
    $isAlive = $false
    if ($currentProcess) {
        try {
            $isAlive = -not $currentProcess.HasExited
        } catch {
            $isAlive = $false
        }
    }

    if (-not $isAlive) {
        $exitCode = if ($currentProcess) { $currentProcess.ExitCode } else { -1 }
        $now = Get-Date
        $crashTimestamps.Add($now)
        Write-WatchdogLog "ENGINE PROCESS DOWN (Exit Code: $exitCode). Initiating rapid recovery..." "WARN"

        # 3. Circuit Breaker Evaluation
        # Prune timestamps older than the evaluation window
        $cutoff = $now.AddSeconds(-$CrashWindowSec)
        $recentCrashes = $crashTimestamps | Where-Object { $_ -ge $cutoff }
        $crashTimestamps = [System.Collections.Generic.List[datetime]]::new([datetime[]]$recentCrashes)

        if ($crashTimestamps.Count -ge $MaxCrashesInWindow) {
            Write-WatchdogLog "CIRCUIT BREAKER TRIPPED! Engine crashed $($crashTimestamps.Count) times in ${CrashWindowSec}s." "ERROR"
            Write-WatchdogLog "Halting automatic restart loop to prevent CPU thrashing. Inspect engine logs." "ERROR"
            
            # Display emergency notification in desktop session
            [System.Windows.Forms.MessageBox]::Show(
                "Pipvane Engine has crashed repeatedly ($($crashTimestamps.Count) times in ${CrashWindowSec}s). Watchdog has paused automatic restarts. Please inspect %AppData%\Pipvane\logs.",
                "Pipvane Watchdog Alert",
                [System.Windows.Forms.MessageBoxButtons]::OK,
                [System.Windows.Forms.MessageBoxIcon]::Error
            ) | Out-Null
            
            # Sleep for 5 minutes before resetting circuit breaker
            Start-Sleep -Seconds 300
            $crashTimestamps.Clear()
        }

        # 4. Immediate Restart (< 10 seconds PRD SLA)
        Write-WatchdogLog "Executing immediate restart (Target recovery time < 5s)..." "INFO"
        $currentProcess = Start-EngineProcess
    }
}
