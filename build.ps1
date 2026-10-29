<#
.SYNOPSIS
    Build SocketTrader.exe — a self-contained Windows app (no Python needed
    on the machine that runs it).

.DESCRIPTION
    Uses PyInstaller in a private virtual environment (.venv-build, created
    here, never your system Python) to produce dist\SocketTrader\:

        SocketTrader.exe          the app: console UI + web UI, icon and
                                  version resource embedded
        _internal\                Python runtime, dependencies, tzdata,
                                  and the bundled resources (strategy
                                  templates, AddOn source, images)
        images\, strategy\, addon\, README.md, PROP_RULES.md, LICENSE
                                  copied beside the exe so the installer
                                  and the operator can find them

    The version comes from __version__ in SocketTrader.py and is written
    into the exe's file properties (right-click > Properties > Details).

    Then install it:    .\install.bat        (picks the exe up automatically)
    Or hand it on:      zip dist\SocketTrader and run install.bat from the
                        unpacked folder on the other machine.

.PARAMETER Install
    Run install.ps1 right after a successful build.

.PARAMETER Clean
    Delete build\, dist\ and .venv-build first.
#>
[CmdletBinding()]
param(
    [switch]$Install,
    [switch]$Clean
)
$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $here

function Step { param($t) Write-Host "`n== $t" -ForegroundColor Cyan }
function Ok   { param($t) Write-Host "   OK  $t" -ForegroundColor Green }

if ($Clean) {
    Step 'Cleaning'
    foreach ($d in @('build', 'dist', '.venv-build')) {
        if (Test-Path $d) { Remove-Item $d -Recurse -Force; Ok "removed $d" }
    }
}

# ---- version -------------------------------------------------------------
$m = Select-String -Path 'SocketTrader.py' -Pattern '^__version__\s*=\s*"([^"]+)"' | Select-Object -First 1
if (-not $m) { throw 'could not read __version__ from SocketTrader.py' }
$version = $m.Matches[0].Groups[1].Value
$parts = ($version -split '[^0-9]+' | Where-Object { $_ -ne '' } | Select-Object -First 3) + @(0, 0, 0)
$v4 = "$($parts[0]),$($parts[1]),$($parts[2]),0"
Step "Building SocketTrader v$version"

# ---- python / venv --------------------------------------------------------
$py = $null
# Arguments after the interpreter name. ($c[1..0] is NOT empty in PowerShell:
# a descending range yields two elements, which fed `python $null python -c …`.)
function Rest { param($a) if ($a.Count -gt 1) { return @($a[1..($a.Count - 1)]) } else { return @() } }
foreach ($c in @(@('py', '-3'), @('python'))) {
    try {
        $out = & $c[0] @(Rest $c) -c 'import sys;print(sys.version_info[0],sys.version_info[1])' 2>$null
        if ($LASTEXITCODE -eq 0 -and $out) {
            $p = $out -split '\s+'
            if ([version]"$($p[0]).$($p[1])" -ge [version]'3.10') { $py = $c; break }
        }
    } catch {}
}
if (-not $py) { throw 'Python 3.10+ is needed to BUILD (not to run the result). Install it from python.org.' }
$venv = Join-Path $here '.venv-build'
if (-not (Test-Path (Join-Path $venv 'Scripts\python.exe'))) {
    & $py[0] @(Rest $py) -m venv $venv
    if ($LASTEXITCODE -ne 0) { throw 'could not create the build venv' }
}
$vpy = Join-Path $venv 'Scripts\python.exe'
& $vpy -m pip install --upgrade pip --quiet
& $vpy -m pip install -r requirements.txt --quiet
& $vpy -m pip install 'pyinstaller>=6.6,<7' --quiet
if ($LASTEXITCODE -ne 0) { throw 'pip install failed' }
Ok "build environment ready ($(& $vpy --version))"

# ---- version resource -----------------------------------------------------
$verFile = Join-Path $here 'build\version_info.txt'
New-Item -ItemType Directory -Force -Path (Split-Path $verFile) | Out-Null
@"
VSVersionInfo(
  ffi=FixedFileInfo(filevers=($v4), prodvers=($v4), mask=0x3f, flags=0x0, OS=0x40004, fileType=0x1, subtype=0x0, date=(0, 0)),
  kids=[
    StringFileInfo([StringTable('040904B0', [
      StringStruct('CompanyName', 'VoidOrigin'),
      StringStruct('FileDescription', 'SocketTrader - NinjaTrader signal gateway'),
      StringStruct('FileVersion', '$version'),
      StringStruct('InternalName', 'SocketTrader'),
      StringStruct('LegalCopyright', 'MIT License'),
      StringStruct('OriginalFilename', 'SocketTrader.exe'),
      StringStruct('ProductName', 'SocketTrader'),
      StringStruct('ProductVersion', '$version')])]),
    VarFileInfo([VarStruct('Translation', [1033, 1200])])
  ]
)
"@ | Set-Content -Path $verFile -Encoding ASCII

# ---- pyinstaller ----------------------------------------------------------
Step 'Running PyInstaller'
$args = @(
    '--noconfirm', '--clean', '--onedir', '--console',
    '--name', 'SocketTrader',
    '--icon', 'images\sockettrader.ico',
    '--version-file', $verFile,
    '--add-data', 'images;images',
    '--add-data', 'strategy;strategy',
    '--add-data', 'addon;addon',
    '--collect-data', 'tzdata',       # zoneinfo on Windows: America/New_York lives here
    '--collect-data', 'pyfiglet',     # banner fonts
    '--collect-submodules', 'websockets',
    '--hidden-import', 'colorama',
    '--exclude-module', 'pytest',
    '--exclude-module', 'test_sockettrader',
    'SocketTrader.py'
)
& $vpy -m PyInstaller @args
if ($LASTEXITCODE -ne 0) { throw 'PyInstaller failed' }
$dist = Join-Path $here 'dist\SocketTrader'
$exe = Join-Path $dist 'SocketTrader.exe'
if (-not (Test-Path $exe)) { throw "expected $exe" }

# Operator-facing extras beside the exe (the copies inside _internal are
# what the app itself reads).
foreach ($extra in @('README.md', 'PROP_RULES.md', 'LICENSE', 'install.ps1', 'install.bat', 'images', 'strategy', 'addon')) {
    if (Test-Path $extra) { Copy-Item $extra (Join-Path $dist (Split-Path $extra -Leaf)) -Recurse -Force }
}

# ---- smoke test -----------------------------------------------------------
Step 'Smoke test'
$reported = & $exe --version 2>&1
if ($LASTEXITCODE -ne 0 -or "$reported" -notmatch [regex]::Escape($version)) {
    throw "the built exe did not report its version (got: $reported)"
}
Ok "$reported"
$fv = (Get-Item $exe).VersionInfo
Ok "file properties: $($fv.ProductName) $($fv.ProductVersion) ($($fv.CompanyName))"
$size = [math]::Round((Get-ChildItem $dist -Recurse | Measure-Object Length -Sum).Sum / 1MB, 1)
Ok "dist\SocketTrader is $size MB"

if ($Install) {
    Step 'Installing'
    & (Join-Path $dist 'install.ps1')
}
Write-Host ""
Write-Host "  Built dist\SocketTrader\SocketTrader.exe (v$version)." -ForegroundColor Green
Write-Host "  Install with .\install.bat (it uses the exe automatically), or zip dist\SocketTrader." -ForegroundColor DarkGray
Write-Host ""
