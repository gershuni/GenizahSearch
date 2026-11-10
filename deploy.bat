@echo off
setlocal DisableDelayedExpansion
rem Usage: run deploy.bat (a double-click is fine) and paste the full commit id when asked: the id the
rem   publishing step reported, after the full CI passed on it. The server deploys exactly that commit
rem   (see deploy.sh); it must already be published.
rem   The id is read with set /p and never from the command line: cmd would run an "&" or "|" inside an
rem   argument before any check. Only a checked 40-character id ever reaches the ssh line below.
set "ID="
set /p "ID=Full commit id to deploy: "
if not defined ID goto usage
powershell -NoProfile -Command "if ($env:ID -cmatch '^[0-9a-f]{40}\z') { exit 0 } else { exit 1 }" || goto usage
ssh genizah-prod "cd ~/GenizahSearch && ./deploy.sh master-main %ID%"
set "RC=%ERRORLEVEL%"
pause
exit /b %RC%
:usage
echo Paste the full 40-character commit id when asked; it must already be published. Nothing was changed.
pause
exit /b 2
