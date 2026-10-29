@echo off
REM Double-click to build SocketTrader.exe (needs Python 3.10+ only on the
REM machine that builds; the result runs without Python). Output lands in
REM dist\SocketTrader\. Run install.bat afterwards to install the exe.
title SocketTrader build
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0build.ps1" %*
if errorlevel 1 (
  echo.
  echo   Build did not finish. The message above says why.
  echo.
)
pause
