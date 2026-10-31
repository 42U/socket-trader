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
; Always offer the destination page, upgrades included, so the operator can
; put the app on another drive (a C: with no room left is a real case).
; An upgrade pre-fills the folder the previous version used; a silent
; upgrade (the in-app update) keeps it without asking.
DisableDirPage=no
UsePreviousAppDir=yes
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
; Templates NinjaTrader loads by name. strategy\ holds two kinds that
; NinjaTrader keeps in different folders — it shows "Unable to load ATM
; strategy template from file ..." for a stop strategy template found in
; the ATM folder — so build.ps1 sorts them into dist\templates\<kind>
; (scripts\sort_templates.py) and each kind is installed to its own folder.
; Only when NinjaTrader 8 is installed for this user, never over a
; template that already exists (an edited one is the operator's), and
; left in place on uninstall.
Source: "..\dist\templates\AtmStrategy\*.xml"; DestDir: "{code:NtTemplates|AtmStrategy}"; Flags: onlyifdoesntexist uninsneveruninstall skipifsourcedoesntexist; Check: NinjaTraderPresent
Source: "..\dist\templates\StopStrategy\*.xml"; DestDir: "{code:NtTemplates|StopStrategy}"; Flags: onlyifdoesntexist uninsneveruninstall skipifsourcedoesntexist; Check: NinjaTraderPresent

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

// {code:NtTemplates|AtmStrategy} or {code:NtTemplates|StopStrategy}.
function NtTemplates(Kind: String): String;
begin
  Result := NtRoot() + '\templates\' + Kind;
end;

// A stop strategy template document: <StopStrategy> at the top and no
// <AtmStrategy> (ATM templates embed a <StopStrategy> element of their own).
function IsStopStrategyDoc(const FileName: String): Boolean;
var
  Raw: AnsiString;
  S: String;
begin
  Result := False;
  if LoadStringFromFile(FileName, Raw) then
  begin
    S := Raw;
    Result := (Pos('<StopStrategy', S) > 0) and (Pos('<AtmStrategy', S) = 0);
  end;
end;

// 1.0.0's Setup.exe copied every template into templates\AtmStrategy, the
// stop strategy template included, and NinjaTrader then showed "Unable to
// load ATM strategy template from file '...\algoNQmed.xml'" on each machine
// it was installed on. Remove what that left behind: a file in the ATM
// folder that has the name of a stop template we ship AND is itself a stop
// strategy document. An operator's own ATM template of the same name is an
// ATM document and is left alone.
procedure RemoveMisplacedStopTemplates();
var
  Rec: TFindRec;
  Shipped, AtmDir, Stray: String;
begin
  if not NinjaTraderPresent() then
    exit;
  Shipped := ExpandConstant('{app}\strategy');
  AtmDir := NtTemplates('AtmStrategy');
  if FindFirst(Shipped + '\*.xml', Rec) then
  begin
    try
      repeat
        if IsStopStrategyDoc(Shipped + '\' + Rec.Name) then
        begin
          Stray := AtmDir + '\' + Rec.Name;
          if FileExists(Stray) and IsStopStrategyDoc(Stray) then
          begin
            if DeleteFile(Stray) then
              Log('removed misplaced stop strategy template ' + Stray)
            else
              Log('could not remove misplaced stop strategy template ' + Stray);
          end;
        end;
      until not FindNext(Rec);
    finally
      FindClose(Rec);
    end;
  end;
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
  begin
    // Upgrading over an older per-user install made by install.bat: that
    // installer registered its own uninstaller under a different key, which
    // would leave two "SocketTrader" entries in Settings > Apps. Drop the old
    // registration (the files are replaced by this install anyway).
    RegDeleteKeyIncludingSubkeys(HKCU, 'Software\Microsoft\Windows\CurrentVersion\Uninstall\SocketTrader');
    RemoveMisplacedStopTemplates();
  end;
end;
