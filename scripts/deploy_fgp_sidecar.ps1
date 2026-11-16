# Deploy the gate-4 FGP candidate to the web server as fgp_data/fgp_transcriptions.db.
#
# The owner runs this (scp to the server is blocked for Claude sessions). The FGP sidecar is read only by
# shared/fgp_service.py, which discovers its table and reads every column defensively, so the candidate's extra
# columns need no code deploy; this is a data swap and a restart.
#
# The order is the Codex review's of 2026-10-26: check the file here (the passing gate-4 run built it), upload it
# under a temporary name, check the checksum and the database again ON THE SERVER, keep the served file for a
# rollback, swap, restart, and check what is served. Every step is followed by an exit-code check; nothing after a
# failed step runs. The swap is two renames in one directory while the service still holds the old file open;
# the restart makes it read the new one.
#
#   powershell -File scripts/deploy_fgp_sidecar.ps1 -Rows 59062            # check, upload, verify, swap, restart
#   powershell -File scripts/deploy_fgp_sidecar.ps1 -Rows 59062 -DryRun    # local check only; print the rest
#
# Rollback (prints the exact names at the end):
#   ssh <host> "cd <repo>/fgp_data && mv fgp_transcriptions.db.prev-<stamp> fgp_transcriptions.db && sudo systemctl restart genizah-web"
#
# The editions index (editions search plan section 3.7). Where the server has one
# (Genizah_Index/Editions), it is derived from this sidecar and is rebuilt with it: after the uploaded
# copy is checked, Editions.new is built from it and gated while the site keeps serving; then the
# service is STOPPED (which closes every reader), the sidecar swapped and the generation put in place,
# and the service started. If the build or its gate fails nothing is touched. If the swap, the start or
# the final check fails, BOTH the sidecar and the generation are put back from their .prev copies and
# the service is started again. Without an editions index the steps are as before.
param(
    [Parameter(Mandatory = $true)][int]$Rows,
    [string]$Version = "2.0.0",
    [string]$Candidate = "fgp_data/inventory/fgp_transcriptions.candidate.db",
    [string]$Summary = "fgp_data/inventory/gate4_summary.json",
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

$Stamp = Get-Date -Format "yyyyMMdd-HHmm"
$Py = "./venv/bin/python"
$EdBuild = "nice -n 19 ionice -c 3 $Py scripts/build_editions_index.py --force --out Genizah_Index/Editions.new --fgp-db fgp_data/fgp_transcriptions.db.new"
$EdGate = "nice -n 19 ionice -c 3 $Py scripts/verify_editions_index.py --gen Genizah_Index/Editions.new --fgp-db fgp_data/fgp_transcriptions.db.new"
$EdPromote = "$Py scripts/build_editions_index.py --promote-only Genizah_Index/Editions.new"
$EdRestore = "$Py scripts/build_editions_index.py --restore-previous"
$EdCheck = "$Py scripts/build_editions_index.py --check-live"
if (-not $RemoteRepo) { Fail "set GENIZAH_REMOTE_REPO (the server checkout, e.g. in your user environment) or pass -RemoteRepo" }
$RemoteDir = "$RemoteRepo/fgp_data"
$Served = "fgp_transcriptions.db"
$Incoming = "fgp_transcriptions.db.new"
$Previous = "fgp_transcriptions.db.prev-$Stamp"

Write-Host "== 1/6 local check: $Candidate is the file the passing gate-4 run built"
python scripts/check_fgp_sidecar.py --db $Candidate --version $Version --rows $Rows --gate4-summary $Summary
if ($LASTEXITCODE -ne 0) { Fail "the local check refused $Candidate (exit $LASTEXITCODE); nothing was uploaded" }
$Sha = (Get-FileHash -Algorithm SHA256 $Candidate).Hash.ToLower()
Write-Host "   sha256 $Sha"

if ($DryRun) {
    Write-Host ""
    Write-Host "Dry run. Would now run:"
    Write-Host "  scp $Candidate ${RemoteHost}:$RemoteDir/$Incoming"
    Write-Host "  scp scripts/check_fgp_sidecar.py ${RemoteHost}:/tmp/check_fgp_sidecar.py"
    Write-Host "  ssh: check $Incoming (sha256 $Sha, integrity, version $Version, $Rows rows)"
    Write-Host "  where Genizah_Index/Editions exists:"
    Write-Host "    ssh: $EdBuild"
    Write-Host "         $EdGate"
    Write-Host "    ssh: sudo systemctl stop genizah-web; mv $Served $Previous; mv $Incoming $Served; $EdPromote; sudo systemctl start genizah-web"
    Write-Host "  otherwise:"
    Write-Host "    ssh: mv $Served $Previous; mv $Incoming $Served; sudo systemctl restart genizah-web"
    Write-Host "  ssh: check the served $Served again; systemctl is-active genizah-web (and $EdCheck)"
    exit 0
}

Write-Host "== 2/6 upload under a temporary name: ${RemoteHost}:$RemoteDir/$Incoming"
scp $Candidate "${RemoteHost}:$RemoteDir/$Incoming"
if ($LASTEXITCODE -ne 0) { Fail "scp of the sidecar failed (exit $LASTEXITCODE); the served file is untouched" }
scp scripts/check_fgp_sidecar.py "${RemoteHost}:/tmp/check_fgp_sidecar.py"
if ($LASTEXITCODE -ne 0) { Fail "scp of the checker failed (exit $LASTEXITCODE); the served file is untouched" }

Write-Host "== 3/6 check the uploaded copy on the server"
ssh $RemoteHost "cd $RemoteDir && python3 /tmp/check_fgp_sidecar.py --db $Incoming --sha256 $Sha --version $Version --rows $Rows"
if ($LASTEXITCODE -ne 0) { Fail "the uploaded copy failed its check (exit $LASTEXITCODE); the served file is untouched; remove $RemoteDir/$Incoming" }

$HasEditions = Test-RemoteDir "$RemoteRepo/Genizah_Index/Editions"
# Puts BOTH back -- each attempted whatever the other did -- starts the service whatever happened,
# and exits non-zero when either undo or the start failed, so the message says what is true (Codex
# review of PR #18).
$RollbackBoth = "sudo systemctl stop genizah-web || exit 2; cd $RemoteDir && mv $Previous $Served; s=`$?; cd $RemoteRepo && $EdRestore; e=`$?; sudo systemctl start genizah-web; sleep 5; systemctl is-active genizah-web || exit 1; test `$s -eq 0 && test `$e -eq 0"
function Invoke-RollbackBoth([string]$why) {
    ssh $RemoteHost $RollbackBoth
    if ($LASTEXITCODE -eq 0) {
        Fail "$why; the sidecar and the editions generation were put back and genizah-web is running again"
    }
    Fail "$why; THE ROLLBACK FAILED TOO (exit $LASTEXITCODE): check $RemoteDir ($Served, $Previous), $RemoteRepo/Genizah_Index and genizah-web by hand"
}
if ($HasEditions) {
    Write-Host "== 3b/6 the server has an editions index: build Editions.new from the new sidecar and gate it (the site keeps serving)"
    ssh $RemoteHost "cd $RemoteRepo && $EdBuild && $EdGate"
    if ($LASTEXITCODE -ne 0) { Fail "the editions build or its gate failed (exit $LASTEXITCODE); nothing was swapped; remove $RemoteDir/$Incoming (Genizah_Index/Editions.new is left for a look)" }

    Write-Host "== 4/6 stop genizah-web, keep the served file as $Previous, swap, put the editions generation in place"
    ssh $RemoteHost "sudo systemctl stop genizah-web"
    if ($LASTEXITCODE -ne 0) { Fail "genizah-web did not stop (exit $LASTEXITCODE); nothing was swapped; remove $RemoteDir/$Incoming" }
    ssh $RemoteHost "set -e; cd $RemoteDir; test -f $Served; mv $Served $Previous; mv $Incoming $Served"
    if ($LASTEXITCODE -ne 0) {
        # The first rename may have run: the served file then waits as $Previous; put it back.
        Invoke-Recovery "cd $RemoteDir && { test -e $Served || mv $Previous $Served; }" "the sidecar swap failed (exit $LASTEXITCODE)" "$Served is the served file again and genizah-web is running"
    }
    ssh $RemoteHost "cd $RemoteRepo && $EdPromote"
    if ($LASTEXITCODE -ne 0) {
        # The served generation is still in place (promote_generation puts it back on a failure).
        Invoke-Recovery "cd $RemoteDir && mv $Previous $Served" "the editions promotion failed (exit $LASTEXITCODE)" "the sidecar was put back and genizah-web is running again"
    }

    Write-Host "== 5/6 start genizah-web"
    ssh $RemoteHost "sudo systemctl start genizah-web && sleep 5 && systemctl is-active genizah-web"
    if ($LASTEXITCODE -ne 0) {
        Invoke-RollbackBoth "genizah-web did not start (exit $LASTEXITCODE)"
    }
} else {
    Write-Host "== 4/6 keep the served file as $Previous and swap"
    ssh $RemoteHost "set -e; cd $RemoteDir; test -f $Served; mv $Served $Previous; mv $Incoming $Served"
    if ($LASTEXITCODE -ne 0) {
        $why = "the swap failed (exit $LASTEXITCODE)"
        # The first rename may have run: the served file then waits as $Previous; put it back
        # (the service still runs and reads it after its next restart; Codex review of PR #18).
        ssh $RemoteHost "cd $RemoteDir && { test -e $Served || mv $Previous $Served; }"
        if ($LASTEXITCODE -eq 0) { Fail "$why; $Served is the served file again; nothing else was changed" }
        Fail "$why; $Served COULD NOT BE PUT BACK (exit $LASTEXITCODE): restore it from $Previous by hand"
    }

    Write-Host "== 5/6 restart genizah-web"
    ssh $RemoteHost "sudo systemctl restart genizah-web && sleep 5 && systemctl is-active genizah-web"
    if ($LASTEXITCODE -ne 0) { Fail "the restart failed (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`"" }
}

Write-Host "== 6/6 check the served file"
ssh $RemoteHost "cd $RemoteDir && python3 /tmp/check_fgp_sidecar.py --db $Served --sha256 $Sha --version $Version --rows $Rows"
if ($LASTEXITCODE -ne 0) {
    if ($HasEditions) {
        Invoke-RollbackBoth "the served file failed its check (exit $LASTEXITCODE)"
    }
    Fail "the served file failed its check (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`""
}
if ($HasEditions) {
    ssh $RemoteHost "cd $RemoteRepo && $EdCheck"
    if ($LASTEXITCODE -ne 0) {
        Invoke-RollbackBoth "the served editions generation failed its check (exit $LASTEXITCODE)"
    }
}

Write-Host ""
Write-Host "Deployed the FGP sidecar ($Rows rows, version $Version) and restarted genizah-web."
Write-Host "Smoke-check by hand: a reading-desk page with an FGP source, a search hit with an FGP source, the /api transcription endpoint."
if ($HasEditions) {
    Write-Host "The editions index was rebuilt from it and put in place."
    Write-Host "Rollback: ssh $RemoteHost `"$RollbackBoth`""
} else {
    Write-Host "Rollback: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`""
}
exit 0
