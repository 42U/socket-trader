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
#>
[CmdletBinding()]
param(
    [switch]$Force,
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
Write-Step 2 'Downloading SocketTrader'
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$zip = Join-Path $env:TEMP 'socket-trader.zip'
$stage = Join-Path $env:TEMP 'socket-trader-stage'
if (Test-Path $stage) { Remove-Item $stage -Recurse -Force }
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Invoke-WebRequest -Uri $RepoZip -OutFile $zip -UseBasicParsing
Expand-Archive -Path $zip -DestinationPath $stage -Force
$src = Get-ChildItem $stage -Directory | Select-Object -First 1
Copy-Item (Join-Path $src.FullName '*') $InstallDir -Recurse -Force
Remove-Item $zip, $stage -Recurse -Force -ErrorAction SilentlyContinue
Write-Ok "Installed to $InstallDir"

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
        $head = Get-Content $f.FullName -TotalCount 5 -Raw
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

$shell = New-Object -ComObject WScript.Shell
foreach ($dir in @([Environment]::GetFolderPath('Desktop'),
                   (Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs'))) {
    if (-not (Test-Path $dir)) { continue }
    $lnk = $shell.CreateShortcut((Join-Path $dir 'SocketTrader.lnk'))
    $lnk.TargetPath = $launcher
    $lnk.WorkingDirectory = $InstallDir
    $lnk.Description = 'SocketTrader - NinjaTrader signal gateway'
    $lnk.Save()
}
Write-Ok 'Desktop and Start Menu shortcuts created'

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
