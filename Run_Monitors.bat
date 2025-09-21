@echo off
setlocal
cd /d "%~dp0"
set SCRIPT=dash_app.py

REM Prefer the windowed Python launcher (no console)
where pyw >NUL 2>&1
if %ERRORLEVEL%==0 (
  start "" pyw -3 "%SCRIPT%"
  goto :EOF
)

REM Try pythonw.exe
where pythonw >NUL 2>&1
if %ERRORLEVEL%==0 (
  start "" pythonw "%SCRIPT%"
  goto :EOF
)

REM Fallbacks (a console may appear)
where py >NUL 2>&1
if %ERRORLEVEL%==0 (
  start "" py -3 "%SCRIPT%"
  goto :EOF
)

where python >NUL 2>&1
if %ERRORLEVEL%==0 (
  start "" python "%SCRIPT%"
  goto :EOF
)

echo Could not find Python. Please install Python 3.x or add it to PATH.
pause

