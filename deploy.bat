@echo off
setlocal

rem --- Setup memo ---
rem git clone https://github.com/tomo-account/01_data_loader.git
rem git remote set-url origin https://github.com/tomo-account/01_data_loader.git
rem ------------------

echo === Deploy to GitHub ===
cd /d "%~dp0"

echo === Check Current Directory ===
echo Current: %CD%

echo === Git Add ===
git add .
if %errorlevel% neq 0 (
    echo Git add failed.
    pause
    exit /b
)

echo === Git Commit ===
set /p COMMIT_MSG="Commit message (Enter to use 'Update'): "
if "%COMMIT_MSG%"=="" set COMMIT_MSG=Update
git commit -m "%COMMIT_MSG%" || echo No changes to commit

echo === Git Push ===
git push origin main
if %errorlevel% neq 0 (
    echo Push failed.
    pause
    exit /b
)

echo === Done ===
pause
