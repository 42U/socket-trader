@echo off
REM Double-click me. This is the only file a new user needs to touch.
REM
REM PowerShell refuses to run downloaded .ps1 files by default, which is the
REM single biggest reason "just run the installer" fails for non-technical
REM users. Launching it this way bypasses that for THIS run only, without
REM changing any machine-wide setting.

title SocketTrader installer
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install.ps1" %*

if errorlevel 1 (
  echo.
  echo   Installation did not finish. The message above says why.
  echo.
)
pause
