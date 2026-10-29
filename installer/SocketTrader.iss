; SocketTrader — Windows installer (Inno Setup 6).
;
; Built by the release workflow on a clean Windows runner right after
; build.ps1 has produced dist\SocketTrader (the PyInstaller onedir app):
;
;     iscc /DAppVersion=1.0.0 /DFileVersion=1.0.0.0 installer\SocketTrader.iss
;
; and attached to the GitHub release as SocketTrader-Setup-<version>.exe.
; Double-clicking it installs per user (no admin rights) into
; %LOCALAPPDATA%\SocketTrader, puts SocketTrader in the Start Menu and
; (optionally) on the desktop with the app's own icon and App ID, registers
; the uninstaller in Settings > Apps, copies the ATM strategy templates into
; NinjaTrader's templates folder (never over one you edited), and can start
; the app at the end. Running it again over an existing install upgrades
; it in place; the config, P&L archive and log live in the user folder and
; are never touched by install or uninstall.

#ifndef AppVersion
  #define AppVersion "0.0.0"
#endif
#ifndef FileVersion
  #define FileVersion "0.0.0.0"
#endif
#define AppName "SocketTrader"
#define AppPublisher "VoidOrigin"
#define AppURL "https://github.com/42U/socket-trader"
#define AppExe "SocketTrader.exe"
#define AppUserModelId "VoidOrigin.SocketTrader"

[Setup]
; Fixed per product: Windows uses it to recognise an upgrade of the same app.
AppId={{047B78B5-A384-40EA-BD7C-F6D8B0309057}
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppURL}
AppSupportURL={#AppURL}/issues
AppUpdatesURL={#AppURL}/releases
VersionInfoVersion={#FileVersion}
VersionInfoProductTextVersion={#AppVersion}
VersionInfoDescription={#AppName} setup
DefaultDirName={localappdata}\{#AppName}
DisableProgramGroupPage=yes
DisableDirPage=auto
; Per-user install: no UAC prompt, and the same folder install.bat uses.
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0
OutputDir=..\dist\installer
OutputBaseFilename={#AppName}-Setup-{#AppVersion}
SetupIconFile=..\images\sockettrader.ico
UninstallDisplayIcon={app}\{#AppExe}
UninstallDisplayName={#AppName}
LicenseFile=..\LICENSE
Compression=lzma2/max
SolidCompression=yes
WizardStyle=modern
; A running SocketTrader holds this mutex (apply_windows_app_identity): the
; wizard then asks to close it instead of failing half-way on a locked exe.
AppMutex={#AppUserModelId}
CloseApplications=yes
RestartApplications=no
ShowLanguageDialog=no

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"

[Files]
; The whole onedir build: SocketTrader.exe, _internal\, and the extras
; build.ps1 copies beside it (README, templates, AddOn source, images).
Source: "..\dist\SocketTrader\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
; ATM templates NinjaTrader loads by name. Only when NinjaTrader 8 is
; installed for this user, never over a template that already exists
; (an edited one is the operator's), and left in place on uninstall.
Source: "..\strategy\*.xml"; DestDir: "{code:NtTemplates}"; Flags: onlyifdoesntexist uninsneveruninstall skipifsourcedoesntexist; Check: NinjaTraderPresent

[Icons]
Name: "{autoprograms}\{#AppName}"; Filename: "{app}\{#AppExe}"; WorkingDir: "{app}"; IconFilename: "{app}\images\sockettrader.ico"; AppUserModelID: "{#AppUserModelId}"; Comment: "NinjaTrader signal gateway"
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExe}"; WorkingDir: "{app}"; IconFilename: "{app}\images\sockettrader.ico"; AppUserModelID: "{#AppUserModelId}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#AppExe}"; Description: "{cm:LaunchProgram,{#StringChange(AppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
; Byte-code caches the app may write beside itself.
Type: filesandordirs; Name: "{app}\_internal\__pycache__"

[Code]
// NinjaTrader 8 keeps its user files under Documents (OneDrive-redirected
// Documents included — {userdocs} follows the redirection).
function NtRoot(): String;
begin
  Result := ExpandConstant('{userdocs}\NinjaTrader 8');
end;

function NinjaTraderPresent(): Boolean;
begin
  Result := DirExists(NtRoot());
end;

function NtTemplates(Param: String): String;
begin
  Result := NtRoot() + '\templates\AtmStrategy';
end;

// Upgrading over an older per-user install made by install.bat: that
// installer registered its own uninstaller under a different key, which
// would leave two "SocketTrader" entries in Settings > Apps. Drop the old
// registration (the files are replaced by this install anyway).
procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
    RegDeleteKeyIncludingSubkeys(HKCU, 'Software\Microsoft\Windows\CurrentVersion\Uninstall\SocketTrader');
end;
