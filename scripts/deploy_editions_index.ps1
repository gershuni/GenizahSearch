# Build the editions index on the web server and put it in place (editions search plan, section 3.7).
#
# For the first install, and after a main-index rebuild on the server (the editions index places each
# edition on the main index's pages, so a new browse map means a new generation). A sidecar deploy
# rebuilds it itself: scripts/deploy_fgp_sidecar.ps1 and scripts/deploy_pgp_sidecar.ps1, whenever the
# server has an editions index.
#
# The owner runs this. The order is the plan's:
#   1. the server has the code (the build and its gate);
#   2. build Editions.new from the served sidecars and gate it, while the site keeps serving;
#   3. stop the service (which closes every reader), put the generation in place (the old one kept as
#      Editions.prev), start it;
#   4. check what is served.
# If step 2 fails nothing is touched. If step 3 or 4 fails the previous generation is put back -- on a first
# install, which has none, the new one is set aside (Editions.failed) and the site serves without editions --
# and the service started again; the script says whether that rollback worked. Every step is followed by an
# exit-code check; nothing after a failed step runs.
#
#   powershell -File scripts/deploy_editions_index.ps1            # build, gate, swap, restart, check
#   powershell -File scripts/deploy_editions_index.ps1 -DryRun    # print the steps only
param(
    [string]$RemoteHost = "genizah-prod",
    [string]$RemoteRepo = $env:GENIZAH_REMOTE_REPO,
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

function Fail([string]$message) {
    [Console]::Error.WriteLine("")
    [Console]::Error.WriteLine("DEPLOY ABORTED: $message")
    exit 1
}

# Whether a directory exists on the server, read from an explicit answer: a failed connection
# (ssh exits 255) is never taken for "absent" -- the deploy stops instead (Codex review of PR #18).
function Test-RemoteDir([string]$dir) {
    $answer = @(ssh $RemoteHost "if test -d $dir; then echo present; else echo absent; fi")
    $code = $LASTEXITCODE
    $last = if ($answer.Count) { "$($answer[-1])".Trim() } else { "" }
    if ($code -ne 0 -or ($last -ne "present" -and $last -ne "absent")) {
        Fail "could not ask the server whether $dir exists (ssh exit $code); the served files are untouched"
    }
    return ($last -eq "present")
}

# After a failed step with the service stopped: run *undo* on the server (or nothing), start
# genizah-web whatever the undo did, and say what is true -- the undo's status and the start's are
# both checked before the script reports a recovery (Codex review of PR #18).
function Invoke-Recovery([string]$undo, [string]$why, [string]$recovered) {
    if (-not $undo) { $undo = "true" }
    ssh $RemoteHost "$undo; s=`$?; sudo systemctl start genizah-web; sleep 5; systemctl is-active genizah-web || exit 1; exit `$s"
    if ($LASTEXITCODE -eq 0) { Fail "$why; $recovered" }
    Fail "$why; THE RECOVERY FAILED TOO (exit $LASTEXITCODE): check the server by hand (genizah-web, the sidecar, Genizah_Index)"
}

if (-not $RemoteRepo) { Fail "set GENIZAH_REMOTE_REPO (the server checkout, e.g. in your user environment) or pass -RemoteRepo" }
$Py = "./venv/bin/python"
$Build = "nice -n 19 ionice -c 3 $Py scripts/build_editions_index.py --force --out Genizah_Index/Editions.new"
$Gate = "nice -n 19 ionice -c 3 $Py scripts/verify_editions_index.py --gen Genizah_Index/Editions.new"
$Promote = "$Py scripts/build_editions_index.py --promote-only Genizah_Index/Editions.new"
# Filled in at step 1: --restore-previous when an index was already installed, --set-aside-live on a
# first install. The service is started whatever the undo did; the exit code is the undo's, then the
# service's (Codex review of PR #18).
$Undo = "--restore-previous"
function Rollback-Command { "sudo systemctl stop genizah-web || exit 2; $Py scripts/build_editions_index.py $Undo; rc=`$?; sudo systemctl start genizah-web; sleep 5; systemctl is-active genizah-web || exit 1; exit `$rc" }
function Invoke-Rollback([string]$why) {
    ssh $RemoteHost "cd $RemoteRepo && $(Rollback-Command)"
    if ($LASTEXITCODE -eq 0) {
        Fail "$why; rolled back ($Undo) and genizah-web is running again"
    }
    Fail "$why; THE ROLLBACK FAILED TOO (exit $LASTEXITCODE): check $RemoteRepo/Genizah_Index and genizah-web by hand"
}

if ($DryRun) {
    Write-Host "Dry run. Would run on ${RemoteHost}, in ${RemoteRepo}:"
    Write-Host "  1. test -f scripts/build_editions_index.py && test -f scripts/verify_editions_index.py"
    Write-Host "  2. $Build"
    Write-Host "     $Gate"
    Write-Host "  3. sudo systemctl stop genizah-web; $Promote; sudo systemctl start genizah-web"
    Write-Host "  4. systemctl is-active genizah-web; $Py scripts/build_editions_index.py --check-live"
    Write-Host "  on a failure at 3 or 4: $(Rollback-Command)   (--set-aside-live instead on a first install)"
    exit 0
}

Write-Host "== 1/4 the server has the code"
ssh $RemoteHost "cd $RemoteRepo && test -f scripts/build_editions_index.py && test -f scripts/verify_editions_index.py"
if ($LASTEXITCODE -ne 0) { Fail "the server's checkout has no editions build (deploy the code first); nothing was touched" }
if (-not (Test-RemoteDir "$RemoteRepo/Genizah_Index/Editions")) {
    $Undo = "--set-aside-live"
    Write-Host "   a first install: there is no index to fall back to; a rollback would set the new one aside"
}

Write-Host "== 2/4 build Editions.new from the served sidecars and gate it (the site keeps serving)"
ssh $RemoteHost "cd $RemoteRepo && $Build && $Gate"
if ($LASTEXITCODE -ne 0) { Fail "the build or its gate failed (exit $LASTEXITCODE); the served index is untouched. Genizah_Index/Editions.new is left for a look" }

Write-Host "== 3/4 stop genizah-web, put the generation in place (the old one kept as Editions.prev), start it"
ssh $RemoteHost "sudo systemctl stop genizah-web"
if ($LASTEXITCODE -ne 0) { Fail "genizah-web did not stop (exit $LASTEXITCODE); nothing was swapped" }
ssh $RemoteHost "cd $RemoteRepo && $Promote"
if ($LASTEXITCODE -ne 0) {
    # A refused or failed promotion leaves the served generation in place (promote_generation
    # puts it back when its second rename fails).
    Invoke-Recovery "" "the promotion failed (exit $LASTEXITCODE)" "the served generation is unchanged and genizah-web is running again"
}
ssh $RemoteHost "sudo systemctl start genizah-web"
if ($LASTEXITCODE -ne 0) { Invoke-Rollback "genizah-web did not start (exit $LASTEXITCODE)" }

Write-Host "== 4/4 check what is served"
ssh $RemoteHost "sleep 5 && systemctl is-active genizah-web && cd $RemoteRepo && $Py scripts/build_editions_index.py --check-live"
if ($LASTEXITCODE -ne 0) { Invoke-Rollback "the served index failed its check (exit $LASTEXITCODE)" }

Write-Host ""
Write-Host "The editions index is in place and genizah-web is running. EDITIONS_SEARCH_ENABLED decides who searches it."
Write-Host "Rollback: ssh $RemoteHost `"cd $RemoteRepo && $(Rollback-Command)`""
exit 0
