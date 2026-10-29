<#
.SYNOPSIS
    One-step SocketTrader installer for Windows.

.DESCRIPTION
    Installs everything SocketTrader needs and puts a shortcut on the desktop.
    Intended to be run by double-clicking install.bat — no command line, no
    admin rights, and deliberately NO WSL2: SocketTrader runs natively on
    Windows, which is also where NinjaTrader runs, so there is nothing to
    gain from a Linux layer in between.

    Steps, each of which is skipped if already satisfied:
      1. Python 3.10+            (via winget, or a direct download link)
      2. SocketTrader itself     (into %LOCALAPPDATA%\SocketTrader)
      3. Python dependencies     (pip), then a proof the app imports
      4. ATM strategy templates  (copied into NinjaTrader's templates folder)
      5. Desktop + Start Menu shortcut
      6. A check that NinjaTrader's Automated Trading Interface is switched on

.PARAMETER Force
    Overwrite ATM strategy templates that already exist. Off by default so a
    re-run never clobbers a template you have edited yourself.

.PARAMETER InstallDir
    Where to install. Defaults to %LOCALAPPDATA%\SocketTrader.

.PARAMETER FromGitHub
    Always download the published release, even when this script sits in
    a checkout that has SocketTrader.py next to it. By default a checkout
    installs ITSELF: running install.bat from an unpacked zip or a local
    edit must install those files, not silently replace them with whatever
    GitHub currently has (which is how a newer local build got rolled back
    to an older published one).
#>
[CmdletBinding()]
param(
    [switch]$Force,
    [switch]$FromGitHub,
    [string]$InstallDir = (Join-Path $env:LOCALAPPDATA 'SocketTrader')
)

$ErrorActionPreference = 'Stop'
$RepoZip = 'https://github.com/42U/socket-trader/archive/refs/heads/main.zip'
$MinPython = [version]'3.10'

function Write-Step { param($n, $t) Write-Host "`n[$n/6] $t" -ForegroundColor Cyan }
function Write-Ok   { param($t) Write-Host "      OK   $t" -ForegroundColor Green }
function Write-Info { param($t) Write-Host "      ...  $t" -ForegroundColor Gray }
function Write-Warn { param($t) Write-Host "      !    $t" -ForegroundColor Yellow }

function Get-PythonCommand {
    # The py launcher is the reliable way to find a specific version; fall
    # back to whatever `python` resolves to (which on a clean Windows is the
    # Microsoft Store stub that does nothing useful, hence the version test).
    foreach ($candidate in @(
            @{ Exe = 'py';     Args = @('-3', '-c', 'import sys;print(sys.version_info[0],sys.version_info[1])') },
            @{ Exe = 'python'; Args = @('-c', 'import sys;print(sys.version_info[0],sys.version_info[1])') })) {
        $exe = Get-Command $candidate.Exe -ErrorAction SilentlyContinue
        if (-not $exe) { continue }
        try {
            $out = & $candidate.Exe @($candidate.Args) 2>$null
            if ($LASTEXITCODE -ne 0 -or -not $out) { continue }
            $parts = ($out -split '\s+')
            $ver = [version]"$($parts[0]).$($parts[1])"
            if ($ver -ge $MinPython) {
                return [pscustomobject]@{
                    Exe = $candidate.Exe
                    Prefix = if ($candidate.Exe -eq 'py') { @('-3') } else { @() }
                    Version = $ver
                }
            }
        } catch { continue }
    }
    return $null
}

function Get-NinjaTraderRoot {
    # Documents is frequently redirected into OneDrive, so ask Windows where
    # it actually is rather than assuming %USERPROFILE%\Documents.
    $roots = @()
    foreach ($docs in @([Environment]::GetFolderPath('MyDocuments'),
                        (Join-Path $env:USERPROFILE 'Documents'),
                        (Join-Path $env:USERPROFILE 'OneDrive\Documents'))) {
        if ($docs -and (Test-Path $docs)) {
            $nt = Join-Path $docs 'NinjaTrader 8'
            if ((Test-Path $nt) -and ($roots -notcontains $nt)) { $roots += $nt }
        }
    }
    if ($roots.Count -gt 0) { return $roots[0] }
    return $null
}

Write-Host ""
Write-Host "  SOCKET TRADER - Windows installer" -ForegroundColor White
Write-Host "  ---------------------------------" -ForegroundColor DarkGray
Write-Host "  Installs to: $InstallDir" -ForegroundColor DarkGray

# ---- 1. Python -----------------------------------------------------------
Write-Step 1 'Checking for Python 3.10 or newer'
$py = Get-PythonCommand
if ($py) {
    Write-Ok "Python $($py.Version) found"
} else {
    Write-Info 'Not found - installing via winget (this can take a few minutes)'
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        Write-Warn 'winget is unavailable on this machine.'
        Write-Host  '      Install Python from https://www.python.org/downloads/' -ForegroundColor Yellow
        Write-Host  '      During setup TICK "Add python.exe to PATH", then run this again.' -ForegroundColor Yellow
        throw 'Python is required.'
    }
    winget install --id Python.Python.3.12 --source winget `
        --accept-source-agreements --accept-package-agreements --silent | Out-Null
    # winget updates PATH for new processes only; refresh this session's copy.
    $env:Path = [Environment]::GetEnvironmentVariable('Path', 'Machine') + ';' +
                [Environment]::GetEnvironmentVariable('Path', 'User')
    $py = Get-PythonCommand
    if (-not $py) {
        Write-Warn 'Python installed but not visible yet.'
        Write-Host  '      Close this window, open a new one, and run the installer again.' -ForegroundColor Yellow
        throw 'Python not on PATH yet.'
    }
    Write-Ok "Python $($py.Version) installed"
}

# ---- 2. SocketTrader files ----------------------------------------------
function Get-LocalVersion { param($dir)
    $f = Join-Path $dir 'SocketTrader.py'
    if (-not (Test-Path $f)) { return $null }
    $m = Select-String -Path $f -Pattern '^__version__\s*=\s*"([^"]+)"' | Select-Object -First 1
    if ($m) { return $m.Matches[0].Groups[1].Value } else { return $null }
}
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$localSrc = if (-not $FromGitHub -and (Test-Path (Join-Path $here 'SocketTrader.py'))) { $here } else { $null }
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$installedBefore = Get-LocalVersion $InstallDir
if ($localSrc -and ((Resolve-Path $localSrc).Path -eq (Resolve-Path $InstallDir).Path)) {
    Write-Step 2 'SocketTrader files'
    Write-Ok "Running from the install folder itself (v$installedBefore) - files left as they are"
} elseif ($localSrc) {
    Write-Step 2 "Installing SocketTrader from this folder"
    Write-Info "$localSrc  (v$(Get-LocalVersion $localSrc))"
    if ($installedBefore) { Write-Info "replacing installed v$installedBefore" }
    # Everything except caches and the launcher the installer writes itself.
    Get-ChildItem $localSrc -Force | Where-Object { $_.Name -notin @('__pycache__', 'SocketTrader.cmd', '.git') } |
        ForEach-Object { Copy-Item $_.FullName (Join-Path $InstallDir $_.Name) -Recurse -Force }
    Write-Ok "Installed to $InstallDir"
} else {
    Write-Step 2 'Downloading SocketTrader'
    $zip = Join-Path $env:TEMP 'socket-trader.zip'
    $stage = Join-Path $env:TEMP 'socket-trader-stage'
    if (Test-Path $stage) { Remove-Item $stage -Recurse -Force }
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    Invoke-WebRequest -Uri $RepoZip -OutFile $zip -UseBasicParsing
    Expand-Archive -Path $zip -DestinationPath $stage -Force
    $src = Get-ChildItem $stage -Directory | Select-Object -First 1
    $downloaded = Get-LocalVersion $src.FullName
    if ($installedBefore -and $downloaded -and ([version]$downloaded -lt [version]$installedBefore)) {
        Write-Warn "GitHub has v$downloaded but v$installedBefore is installed - NOT downgrading."
        Write-Warn "Run install.bat from the folder that holds the newer files, or pass -FromGitHub to force."
    } else {
        Copy-Item (Join-Path $src.FullName '*') $InstallDir -Recurse -Force
        Write-Ok "Installed v$downloaded to $InstallDir"
    }
    Remove-Item $zip, $stage -Recurse -Force -ErrorAction SilentlyContinue
}

# ---- 3. Dependencies -----------------------------------------------------
Write-Step 3 'Installing Python packages'
$reqs = Join-Path $InstallDir 'requirements.txt'
& $py.Exe @($py.Prefix) -m pip install --upgrade pip --quiet
& $py.Exe @($py.Prefix) -m pip install -r $reqs --quiet
if ($LASTEXITCODE -ne 0) { throw 'pip install failed.' }
Write-Ok 'Dependencies installed'

# Prove the app actually starts with THIS interpreter before calling it
# installed — a broken dependency or a stub Python surfaces here, at
# install time, instead of at the user's first launch. Same check CI runs.
$appVer = & $py.Exe @($py.Prefix) -c "import sys; sys.path.insert(0, r'$InstallDir'); import SocketTrader; print(SocketTrader.__version__)" 2>&1
if ($LASTEXITCODE -ne 0) { throw "SocketTrader failed its import check: $appVer" }
Write-Ok "SocketTrader v$appVer imports cleanly"

# ---- 4. ATM strategy templates ------------------------------------------
Write-Step 4 'Installing ATM strategy templates into NinjaTrader'
$ntRoot = Get-NinjaTraderRoot
$xml = Get-ChildItem (Join-Path $InstallDir 'strategy') -Filter *.xml -ErrorAction SilentlyContinue
if (-not $ntRoot) {
    Write-Warn 'NinjaTrader 8 folder not found - is NinjaTrader installed?'
    Write-Host  "      Copy $InstallDir\strategy\*.xml into" -ForegroundColor Yellow
    Write-Host  '      Documents\NinjaTrader 8\templates\AtmStrategy\ once it is.' -ForegroundColor Yellow
} elseif (-not $xml) {
    Write-Warn 'No strategy templates found in the download.'
} else {
    # NinjaTrader keeps ATM and Stop templates in SEPARATE folders and will
    # not see one filed under the other, so route each file by its actual
    # root element rather than assuming they are all ATMs.
    $atm  = Join-Path $ntRoot 'templates\AtmStrategy'
    $stop = Join-Path $ntRoot 'templates\StopStrategy'
    New-Item -ItemType Directory -Force -Path $atm  | Out-Null
    New-Item -ItemType Directory -Force -Path $stop | Out-Null
    $copied = 0; $kept = 0; $stopCount = 0
    foreach ($f in $xml) {
        # -Raw cannot be combined with -TotalCount; PowerShell rejects the
        # pair at run time, which killed this whole step on real installs.
        $head = (Get-Content $f.FullName -TotalCount 5) -join "`n"
        # <StopStrategy> also appears NESTED inside an ATM template, so only
        # treat it as a stop template when it is the element right after the
        # <NinjaTrader> root.
        $isStop = $head -match '<NinjaTrader>\s*<StopStrategy'
        $target = if ($isStop) { $stop } else { $atm }
        $dest = Join-Path $target $f.Name
        if ((Test-Path $dest) -and -not $Force) { $kept++; continue }
        Copy-Item $f.FullName $dest -Force
        $copied++
        if ($isStop) { $stopCount++ }
    }
    Write-Ok "$copied template(s) installed ($($copied - $stopCount) ATM, $stopCount stop)"
    if ($kept -gt 0) {
        Write-Info "$kept already existed and were left alone (re-run with -Force to replace)"
    }
}

# ---- 5. Shortcuts --------------------------------------------------------
Write-Step 5 'Creating shortcuts'
$launcher = Join-Path $InstallDir 'SocketTrader.cmd'
@"
@echo off
cd /d "%~dp0"
$($py.Exe) $($py.Prefix -join ' ') SocketTrader.py
pause
"@ | Set-Content -Path $launcher -Encoding ASCII

# Windows identifies a running app by its AppUserModelID. A shortcut that
# carries one is how a script-based app gets treated like a real one: the
# taskbar groups and pins it as SocketTrader (not "Python"), with this
# icon, and the app stamps the same ID on itself at start-up. The ID is
# written through the shell link's property store (there is no cmdlet).
$AppId = 'VoidOrigin.SocketTrader'
$AppIdSource = @"
using System;
using System.Runtime.InteropServices;
namespace STInstall {
  [StructLayout(LayoutKind.Sequential, Pack = 4)]
  public struct PROPERTYKEY { public Guid fmtid; public uint pid;
    public PROPERTYKEY(Guid g, uint p) { fmtid = g; pid = p; } }
  [StructLayout(LayoutKind.Explicit)]
  public struct PROPVARIANT { [FieldOffset(0)] public ushort vt; [FieldOffset(8)] public IntPtr p; }
  [ComImport, Guid("886D8EEB-8CF2-4446-8D02-CDBA1DBDCF99"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
  public interface IPropertyStore {
    int GetCount(out uint c); int GetAt(uint i, out PROPERTYKEY k);
    int GetValue(ref PROPERTYKEY k, out PROPVARIANT v); int SetValue(ref PROPERTYKEY k, ref PROPVARIANT v);
    int Commit(); }
  [ComImport, Guid("0000010b-0000-0000-C000-000000000046"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
  public interface IPersistFile {
    void GetClassID(out Guid id); [PreserveSig] int IsDirty();
    void Load([MarshalAs(UnmanagedType.LPWStr)] string f, uint mode);
    void Save([MarshalAs(UnmanagedType.LPWStr)] string f, [MarshalAs(UnmanagedType.Bool)] bool remember);
    void SaveCompleted([MarshalAs(UnmanagedType.LPWStr)] string f);
    void GetCurFile([MarshalAs(UnmanagedType.LPWStr)] out string f); }
  [ComImport, Guid("00021401-0000-0000-C000-000000000046")] public class ShellLink { }
  public static class AppId {
    public static void Set(string lnk, string id) {
      object o = new ShellLink();
      var pf = (IPersistFile)o; pf.Load(lnk, 2);
      var ps = (IPropertyStore)o;
      var key = new PROPERTYKEY(new Guid("9F4C2855-9F79-4B39-A8D0-E1D42DE1D5F3"), 5);
      var pv = new PROPVARIANT(); pv.vt = 31; pv.p = Marshal.StringToCoTaskMemUni(id);
      try { ps.SetValue(ref key, ref pv); ps.Commit(); pf.Save(lnk, true); }
      finally { Marshal.FreeCoTaskMem(pv.p); }
    } } }
"@
$appIdReady = $false
try { Add-Type -TypeDefinition $AppIdSource -ErrorAction Stop; $appIdReady = $true }
catch { Write-Warn "App ID helper unavailable ($($_.Exception.Message)) - shortcuts still work" }

$shell = New-Object -ComObject WScript.Shell
$ico = Join-Path $InstallDir 'images\sockettrader.ico'
foreach ($dir in @([Environment]::GetFolderPath('Desktop'),
                   (Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs'))) {
    if (-not (Test-Path $dir)) { continue }
    $path = Join-Path $dir 'SocketTrader.lnk'
    $lnk = $shell.CreateShortcut($path)
    $lnk.TargetPath = $launcher
    $lnk.WorkingDirectory = $InstallDir
    $lnk.Description = 'SocketTrader - NinjaTrader signal gateway'
    # The shortcut's icon is also what the console window and taskbar show
    # when the app is launched through it.
    if (Test-Path $ico) { $lnk.IconLocation = "$ico,0" }
    $lnk.Save()
    if ($appIdReady) {
        try { [STInstall.AppId]::Set($path, $AppId) }
        catch { Write-Warn "could not stamp the App ID on $path ($($_.Exception.Message))" }
    }
}
Write-Ok 'Desktop and Start Menu shortcuts created'

# Register with Windows the way an installer does, per user (no admin):
# Settings > Apps shows SocketTrader with its icon, version and an
# Uninstall button. Uninstall removes the program folder and shortcuts and
# leaves the data home (config, P&L history) alone — that is a trading
# record, not program files.
$uninst = Join-Path $InstallDir 'uninstall.cmd'
$desktopLnk = Join-Path ([Environment]::GetFolderPath('Desktop')) 'SocketTrader.lnk'   # Desktop may be OneDrive-redirected
@"
@echo off
echo Removing SocketTrader program files and shortcuts (your config and P&L history are kept).
del /q "$desktopLnk" 2>nul
del /q "%APPDATA%\Microsoft\Windows\Start Menu\Programs\SocketTrader.lnk" 2>nul
reg delete "HKCU\Software\Microsoft\Windows\CurrentVersion\Uninstall\SocketTrader" /f >nul 2>&1
cd /d "%TEMP%"
rmdir /s /q "$InstallDir"
echo Done.
pause
"@ | Set-Content -Path $uninst -Encoding ASCII
try {
    $ver = Get-LocalVersion $InstallDir
    $key = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\SocketTrader'
    New-Item -Path $key -Force | Out-Null
    Set-ItemProperty -Path $key -Name DisplayName -Value 'SocketTrader'
    Set-ItemProperty -Path $key -Name DisplayVersion -Value ($(if ($ver) { $ver } else { '0' }))
    Set-ItemProperty -Path $key -Name Publisher -Value 'VoidOrigin'
    Set-ItemProperty -Path $key -Name InstallLocation -Value $InstallDir
    Set-ItemProperty -Path $key -Name DisplayIcon -Value "$ico,0"
    Set-ItemProperty -Path $key -Name UninstallString -Value "`"$uninst`""
    Set-ItemProperty -Path $key -Name NoModify -Value 1 -Type DWord
    Set-ItemProperty -Path $key -Name NoRepair -Value 1 -Type DWord
    Set-ItemProperty -Path $key -Name InstallDate -Value (Get-Date -Format 'yyyyMMdd')
    Write-Ok 'Registered in Settings > Apps (with uninstaller)'
} catch { Write-Warn "could not register with Windows ($($_.Exception.Message))" }

# ---- 6. NinjaTrader ATI check -------------------------------------------
Write-Step 6 'Checking NinjaTrader Automated Trading Interface'
$atiOpen = $false
try {
    $client = New-Object Net.Sockets.TcpClient
    $ok = $client.BeginConnect('127.0.0.1', 36973, $null, $null).AsyncWaitHandle.WaitOne(800)
    $atiOpen = $ok -and $client.Connected
    $client.Close()
} catch { $atiOpen = $false }

if ($atiOpen) {
    Write-Ok 'NinjaTrader ATI is reachable on port 36973'
} else {
    Write-Warn 'NinjaTrader is not listening on port 36973.'
    Write-Host  '      This is normal if NinjaTrader is closed. If it IS open, switch the' -ForegroundColor Yellow
    Write-Host  '      interface on - it is off by default and cannot be enabled from here:' -ForegroundColor Yellow
    Write-Host  '        NinjaTrader > Tools > Options > Automated trading interface' -ForegroundColor Yellow
    Write-Host  '        tick "AT Interface", leave Server port on 36973, click OK,' -ForegroundColor Yellow
    Write-Host  '        then restart NinjaTrader.' -ForegroundColor Yellow
}

Write-Host ""
Write-Host "  Done. Launch SocketTrader from the desktop shortcut." -ForegroundColor Green
Write-Host "  First run asks for your server, token and account - nothing else." -ForegroundColor DarkGray
Write-Host "  Re-run this installer any time to update: your config and any" -ForegroundColor DarkGray
Write-Host "  templates you have edited are left untouched." -ForegroundColor DarkGray
Write-Host ""
