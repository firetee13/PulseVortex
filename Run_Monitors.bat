@echo off
setlocal
cd /d "%~dp0"
set SCRIPT=dash_app.py

echo Starting EASY Insight Monitor GUI...
echo The timelapse setups and TP/SL hits monitors will start automatically.
echo Access the dashboard at http://127.0.0.1:8050
echo.

REM Prefer the windowed Python launcher (no console)
where pyw >NUL 2>&1
if %ERRORLEVEL%==0 (
  start "EASY Insight Monitor" pyw -3 "%SCRIPT%"
  goto :EOF
)

REM Try pythonw.exe
where pythonw >NUL 2>&1
if %ERRORLEVEL%==0 (
  start "EASY Insight Monitor" pythonw "%SCRIPT%"
  goto :EOF
)

REM Fallbacks (a console may appear)
where py >NUL 2>&1
if %ERRORLEVEL%==0 (
  start "EASY Insight Monitor" py -3 "%SCRIPT%"
  goto :EOF
)

where python >NUL 2>&1
if %ERRORLEVEL%==0 (
  start "EASY Insight Monitor" python "%SCRIPT%"
  goto :EOF
)

echo Could not find Python. Please install Python 3.x or add it to PATH.
pause

