@echo off
setlocal
chcp 65001 >nul
pushd "%~dp0"

where uv >nul 2>nul
if not errorlevel 1 goto use_uv
if exist ".venv\Scripts\python.exe" goto use_venv

echo TUI could not start: uv or the project virtual environment was not found.
set "TUI_EXIT_CODE=1"
goto finish

:use_uv
uv run --locked python -X utf8 -m campus_watch.tui
set "TUI_EXIT_CODE=%ERRORLEVEL%"
goto finish

:use_venv
".venv\Scripts\python.exe" -X utf8 -m campus_watch.tui
set "TUI_EXIT_CODE=%ERRORLEVEL%"

:finish
popd
if not "%TUI_EXIT_CODE%"=="0" pause
endlocal & exit /b %TUI_EXIT_CODE%
