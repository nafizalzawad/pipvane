<#
.SYNOPSIS
    Pipvane v3: Sprint 13 - Hyper-V Windows VM Configuration Script.
.DESCRIPTION
    Automates provisioning and configuration of a dedicated local Hyper-V VM
    strictly constrained to 2 vCPUs and 4GB RAM to validate terminal footprint
    and engine stability under max licensed limit (7 accounts) during a 48-hour soak.
#>

[CmdletBinding()]
param(
    [string]$VMName = "Pipvane-SoakLab-2c4g",
    [int64]$MemoryBytes = 4GB,
    [int]$ProcessorCount = 2,
    [string]$VHDPath = "C:\Hyper-V\Virtual Hard Disks\Pipvane-SoakLab.vhdx",
    [string]$SwitchName = "Default Switch"
)

$ErrorActionPreference = "Stop"

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "Pipvane v3: Hardware Constrained VM Provisioner (Sprint 13)" -ForegroundColor Cyan
Write-Host "Target VM Name:       $VMName" -ForegroundColor Yellow
Write-Host "Processor Count:      $ProcessorCount vCPUs (Strict Cap)" -ForegroundColor Yellow
Write-Host "Memory Allocation:    4 GB RAM (Static / No Dynamic Ballooning)" -ForegroundColor Yellow
Write-Host "=================================================================" -ForegroundColor Cyan

# 1. Verify Hyper-V feature is present
$hyperv = Get-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V -ErrorAction SilentlyContinue
if ($hyperv -and $hyperv.State -ne "Enabled") {
    Write-Warning "Hyper-V feature is not enabled. Please enable Hyper-V via Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V -All."
}

# 2. Check if VM already exists
$existingVM = Get-VM -Name $VMName -ErrorAction SilentlyContinue
if ($existingVM) {
    Write-Host "Existing VM '$VMName' detected. Re-applying hardware limits..." -ForegroundColor Yellow
    if ($existingVM.State -ne "Off") {
        Write-Host "Stopping VM '$VMName'..."
        Stop-VM -Name $VMName -Force
    }
} else {
    Write-Host "Creating new VM '$VMName'..." -ForegroundColor Green
    New-VM -Name $VMName -MemoryStartupBytes $MemoryBytes -SwitchName $SwitchName -Generation 2
}

# 3. Enforce Strict Hardware Constraints: Exactly 2 vCPUs and 4GB Static RAM
Write-Host "Enforcing 2 vCPUs and 4GB RAM constraints..." -ForegroundColor Green

# Static Memory (Disables Dynamic Memory / Ballooning to enforce strict 4GB boundary)
Set-VMMemory -VMName $VMName `
    -DynamicMemoryEnabled $false `
    -StartupBytes $MemoryBytes

# Set vCPU count to exactly 2
Set-VMProcessor -VMName $VMName `
    -Count $ProcessorCount `
    -Reserve 100 `
    -Maximum 100

Write-Host "VM '$VMName' successfully configured with 2 vCPUs and 4GB RAM." -ForegroundColor Green
Write-Host "Ready for Pipvane 48-Hour Multi-Terminal Soak Test." -ForegroundColor Green
