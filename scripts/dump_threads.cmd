@echo off
rem Thread dump of the running SocketTrader (for a hang report).
rem Double-click. Writes thread-dump.txt next to this script's parent
rem folder and shows it. Needs Python 3 (py launcher) to fetch py-spy.
setlocal
set "OUT=%~dp0..\thread-dump.txt"
set "PID="
for /f "tokens=2 delims=," %%p in ('tasklist /fi "imagename eq SocketTrader.exe" /fo csv /nh 2^>nul') do set "PID=%%~p"
if not defined PID (
  for /f "tokens=2 delims=," %%p in ('tasklist /fi "windowtitle eq SocketTrader*" /fo csv /nh 2^>nul') do set "PID=%%~p"
)
if not defined PID (
  echo SocketTrader.exe is not running. > "%OUT%"
  goto :show
)
echo SocketTrader pid %PID% - fetching py-spy (first time only)...
py -3 -m pip install --quiet --disable-pip-version-check py-spy >nul 2>&1 || python -m pip install --quiet --disable-pip-version-check py-spy >nul 2>&1
set "PYSPY="
for /f "delims=" %%s in ('py -3 -c "import sysconfig,os;print(os.path.join(sysconfig.get_path('scripts'),'py-spy.exe'))" 2^>nul') do set "PYSPY=%%s"
if not exist "%PYSPY%" for /f "delims=" %%s in ('python -c "import sysconfig,os;print(os.path.join(sysconfig.get_path('scripts'),'py-spy.exe'))" 2^>nul') do set "PYSPY=%%s"
if not exist "%PYSPY%" (
  echo py-spy could not be installed - is Python 3 installed? > "%OUT%"
  goto :show
)
(
  echo SocketTrader pid %PID%  %DATE% %TIME%
  echo.
  "%PYSPY%" dump --pid %PID% --nonblocking 2>&1
  echo.
  echo ---- with locals ----
  "%PYSPY%" dump --pid %PID% --nonblocking --locals 2>&1
) > "%OUT%"
:show
type "%OUT%"
echo.
echo Written to %OUT%
pause
