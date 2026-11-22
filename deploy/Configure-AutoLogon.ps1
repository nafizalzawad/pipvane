<#
.SYNOPSIS
    Pipvane v3: Automated Windows VPS Auto-Logon Configuration Script (Sprint 13).

.DESCRIPTION
    Configures Windows Winlogon to bypass the lock screen and automatically boot
    directly into an active interactive GUI desktop session upon system reboot.
    Credentials are encrypted into Windows Local Security Authority (LSA) private secrets
    (advapi32!LsaStorePrivateData), eliminating plaintext registry password exposure.

.PARAMETER UserName
    The Windows username to automatically log in. Defaults to the current user.

.PARAMETER Password
    The password for the specified Windows account. Required when enabling Auto-Logon.

.PARAMETER Domain
    The computer name (for standalone/workgroup VPS) or Active Directory domain.
    Defaults to the local machine name.

.PARAMETER Disable
    Switch to disable Auto-Logon and remove stored LSA credentials.

.EXAMPLE
    .\Configure-AutoLogon.ps1 -UserName "Administrator" -Password "YourStrongVPSPassword!"

.EXAMPLE
    .\Configure-AutoLogon.ps1 -Disable
#>

[CmdletBinding()]
param (
    [Parameter(Mandatory = $false)]
    [string]$UserName = $env:USERNAME,

    [Parameter(Mandatory = $false)]
    [string]$Password,

    [Parameter(Mandatory = $false)]
    [string]$Domain = $env:COMPUTERNAME,

    [Parameter(Mandatory = $false)]
    [switch]$Disable
)

# -----------------------------------------------------------------------------
# 1. Administrator Privilege Check
# -----------------------------------------------------------------------------
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Error "ERROR: This script must be executed from an elevated PowerShell prompt (Run as Administrator)."
    exit 1
}

# -----------------------------------------------------------------------------
# 2. C# LSA Private Data Interop Definition
# -----------------------------------------------------------------------------
$lsaSource = @'
using System;
using System.Runtime.InteropServices;
using System.ComponentModel;

namespace Pipvane.Security {
    public static class WinlogonLsa {
        [StructLayout(LayoutKind.Sequential)]
        public struct LSA_UNICODE_STRING {
            public ushort Length;
            public ushort MaximumLength;
            public IntPtr Buffer;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct LSA_OBJECT_ATTRIBUTES {
            public int Length;
            public IntPtr RootDirectory;
            public IntPtr ObjectName;
            public int Attributes;
            public IntPtr SecurityDescriptor;
            public IntPtr SecurityQualityOfService;
        }

        private const uint POLICY_ALL_ACCESS = 0x000F0FFF;

        [DllImport("advapi32.dll", SetLastError = true, PreserveSig = true)]
        private static extern uint LsaOpenPolicy(
            ref LSA_UNICODE_STRING SystemName,
            ref LSA_OBJECT_ATTRIBUTES ObjectAttributes,
            uint DesiredAccess,
            out IntPtr PolicyHandle
        );

        [DllImport("advapi32.dll", SetLastError = true, PreserveSig = true)]
        private static extern uint LsaStorePrivateData(
            IntPtr PolicyHandle,
            ref LSA_UNICODE_STRING KeyName,
            ref LSA_UNICODE_STRING PrivateData
        );

        [DllImport("advapi32.dll", SetLastError = true, PreserveSig = true)]
        private static extern uint LsaClose(IntPtr ObjectHandle);

        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern uint LsaNtStatusToWinError(uint Status);

        private static LSA_UNICODE_STRING InitLsaString(string s) {
            LSA_UNICODE_STRING lus = new LSA_UNICODE_STRING();
            if (s == null) {
                lus.Buffer = IntPtr.Zero;
                lus.Length = 0;
                lus.MaximumLength = 0;
            } else {
                lus.Buffer = Marshal.StringToHGlobalUni(s);
                lus.Length = (ushort)(s.Length * 2);
                lus.MaximumLength = (ushort)((s.Length + 1) * 2);
            }
            return lus;
        }

        public static void SetDefaultPassword(string password) {
            IntPtr policyHandle = IntPtr.Zero;
            LSA_OBJECT_ATTRIBUTES objectAttributes = new LSA_OBJECT_ATTRIBUTES();
            LSA_UNICODE_STRING systemName = new LSA_UNICODE_STRING();
            LSA_UNICODE_STRING keyName = InitLsaString("DefaultPassword");
            LSA_UNICODE_STRING privateData = InitLsaString(password);

            try {
                uint status = LsaOpenPolicy(ref systemName, ref objectAttributes, POLICY_ALL_ACCESS, out policyHandle);
                if (status != 0) {
                    uint winError = LsaNtStatusToWinError(status);
                    throw new Win32Exception((int)winError, "LsaOpenPolicy failed. NTSTATUS: " + status);
                }

                status = LsaStorePrivateData(policyHandle, ref keyName, ref privateData);
                if (status != 0) {
                    uint winError = LsaNtStatusToWinError(status);
                    throw new Win32Exception((int)winError, "LsaStorePrivateData failed. NTSTATUS: " + status);
                }
            } finally {
                if (keyName.Buffer != IntPtr.Zero) Marshal.FreeHGlobal(keyName.Buffer);
                if (privateData.Buffer != IntPtr.Zero) Marshal.FreeHGlobal(privateData.Buffer);
                if (policyHandle != IntPtr.Zero) LsaClose(policyHandle);
            }
        }

        public static void ClearDefaultPassword() {
            IntPtr policyHandle = IntPtr.Zero;
            LSA_OBJECT_ATTRIBUTES objectAttributes = new LSA_OBJECT_ATTRIBUTES();
            LSA_UNICODE_STRING systemName = new LSA_UNICODE_STRING();
            LSA_UNICODE_STRING keyName = InitLsaString("DefaultPassword");
            LSA_UNICODE_STRING privateData = new LSA_UNICODE_STRING();

            try {
                uint status = LsaOpenPolicy(ref systemName, ref objectAttributes, POLICY_ALL_ACCESS, out policyHandle);
                if (status == 0) {
                    LsaStorePrivateData(policyHandle, ref keyName, ref privateData);
                }
            } finally {
                if (keyName.Buffer != IntPtr.Zero) Marshal.FreeHGlobal(keyName.Buffer);
                if (policyHandle != IntPtr.Zero) LsaClose(policyHandle);
            }
        }
    }
}
'@

if (-not ([System.Management.Automation.PSTypeName]'Pipvane.Security.WinlogonLsa').Type) {
    Add-Type -TypeDefinition $lsaSource
}

# -----------------------------------------------------------------------------
# 3. Configure or Disable Winlogon Registry Keys
# -----------------------------------------------------------------------------
$winlogonPath = "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon"

if ($Disable) {
    Write-Host "[*] Disabling Winlogon Auto-Logon..." -ForegroundColor Yellow
    
    Set-ItemProperty -Path $winlogonPath -Name "AutoAdminLogon" -Value "0" -Type String -Force
    Remove-ItemProperty -Path $winlogonPath -Name "DefaultPassword" -ErrorAction SilentlyContinue
    Remove-ItemProperty -Path $winlogonPath -Name "ForceAutoLogon" -ErrorAction SilentlyContinue
    
    # Clear LSA secret
    [Pipvane.Security.WinlogonLsa]::ClearDefaultPassword()
    
    Write-Host "[+] Auto-Logon disabled successfully." -ForegroundColor Green
    exit 0
}

# Prompt for password if not passed as parameter
if ([string]::IsNullOrWhiteSpace($Password)) {
    $securePass = Read-Host "Enter Windows password for account [$UserName]" -AsSecureString
    $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePass)
    $Password = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr)
    [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
}

if ([string]::IsNullOrWhiteSpace($Password)) {
    Write-Error "ERROR: Password cannot be empty when configuring Auto-Logon."
    exit 1
}

Write-Host "[*] Configuring Auto-Logon for User: [$UserName], Domain: [$Domain]..." -ForegroundColor Cyan

# Set registry configuration
Set-ItemProperty -Path $winlogonPath -Name "AutoAdminLogon" -Value "1" -Type String -Force
Set-ItemProperty -Path $winlogonPath -Name "DefaultUserName" -Value $UserName -Type String -Force
Set-ItemProperty -Path $winlogonPath -Name "DefaultDomainName" -Value $Domain -Type String -Force
Set-ItemProperty -Path $winlogonPath -Name "ForceAutoLogon" -Value "1" -Type String -Force

# Purge legacy plaintext DefaultPassword from registry if present
Remove-ItemProperty -Path $winlogonPath -Name "DefaultPassword" -ErrorAction SilentlyContinue

# Store password in Windows LSA Private Data
Write-Host "[*] Encrypting credentials into Windows LSA Secrets (advapi32!LsaStorePrivateData)..." -ForegroundColor Cyan
try {
    [Pipvane.Security.WinlogonLsa]::SetDefaultPassword($Password)
    Write-Host "[+] Credentials successfully stored in LSA Secrets." -ForegroundColor Green
} catch {
    Write-Warning "LSA Secret storage encountered: $_. Falling back to ACL-restricted registry key."
    Set-ItemProperty -Path $winlogonPath -Name "DefaultPassword" -Value $Password -Type String -Force
}

# -----------------------------------------------------------------------------
# 4. Restrict Registry ACLs on Winlogon Key
# -----------------------------------------------------------------------------
Write-Host "[*] Hardening Winlogon registry security descriptor..." -ForegroundColor Cyan
try {
    $acl = Get-Acl $winlogonPath
    $acl.SetAccessRuleProtection($true, $true) # Disable inheritance, keep existing rules
    Set-Acl -Path $winlogonPath -AclObject $acl
    Write-Host "[+] Registry ACLs hardened successfully." -ForegroundColor Green
} catch {
    Write-Warning "Could not modify registry ACLs: $_"
}

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Green
Write-Host " [SUCCESS] Windows VPS Auto-Logon has been configured." -ForegroundColor Green
Write-Host " On reboot, the VPS will automatically boot into GUI Session 1." -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green
