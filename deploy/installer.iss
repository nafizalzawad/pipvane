; Pipvane v3 Enterprise Windows Installer Script
#define MyAppName "Pipvane"
#define MyAppVersion "3.0.1"
#define MyAppPublisher "Pipvane Software Inc."
#define MyAppURL "https://pipvane.com"
#define MyAppExeName "pipvane-engine.exe"

[Setup]
AppId={{8B84C421-E810-4A09-9E15-6E9D59C20A77}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={autopf}\{#MyAppName}
DisableProgramGroupPage=yes
OutputBaseFilename=Pipvane-Setup
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
ArchitecturesInstallIn64BitMode=x64

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
Source: "..\dist\bin\engine\pipvane-engine.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\dist\bin\bridge\*"; DestDir: "{app}\bridge"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent
