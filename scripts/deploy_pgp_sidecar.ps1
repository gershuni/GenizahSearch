# Deploy pgp_data/pgp.db to the web server -- ONLY if the shipping guard passes.
#
# WHY A SCRIPT. The guide used to document this as a bash `&&` chain. Deploys from this
# machine run in PowerShell 5.1 with native OpenSSH (bash resolves to the WSL stub, which
# has no ~/.ssh and cannot see C:\ paths -- see the 2026-08-19 note), and in PowerShell 5.1
# `&&` is a PARSE ERROR: the whole statement is rejected, the operator retypes it as two
# lines, and the second line -- the scp -- runs whether or not the guard passed. The web app
# reads pgp_translations through TranslationService, so an unguarded upload can republish
# the withheld corpus that build_app.bat blocks for the desktop.
#
# CODE BEFORE DATA, for this sidecar. "Deploy DBs first, then code" holds for a refresh
# that only changes rows. It is WRONG for one that changes the SCHEMA. On 2026-09-22 the
# refreshed pgp.db was uploaded ahead of PR #357, as this guide then instructed. It added
# `documents.doc_relation`, SQL NULL for 29,226 of 36,642 rows, and the code on the server
# read it as `.get('doc_relation', '')` -- a default that fires only on a MISSING KEY, so a
# present-but-NULL column returned None and `'Edition' in None` raised. That killed the whole
# browse-enrichment pass (images, folios, Cambridge, pagination, attribution), 16 times in
# 8 minutes of live traffic, until the sidecar was rolled back. So step 2 below refuses to
# upload until the server's checkout already contains the commit this sidecar was built from.
#
# WHY THAT COMMIT AND NOT A NARROWER ONE. This check first tried to name the modules that read
# the sidecar and require only THEIR last commit. Four review rounds found four consumers that
# enumeration had missed -- one reached the file through a second service, one through a lazy
# in-function import, one through an import spelling the detector could not parse. Every miss
# fails OPEN: the upload proceeds. So the enumeration is gone. The sidecar is built from this
# working tree, so the rule is simply that the server must be running this tree's code. It
# costs an occasional code deploy for unrelated commits; it cannot be defeated by a consumer
# nobody remembered.
#
# Every step here is followed by an exit-code check. Nothing after a failed step runs.
#
# THE UPLOAD LANDS UNDER A TEMPORARY NAME (2026-11, the editions search plan section 3.7), as the FGP
# script's does: pgp.db.new, then a swap that keeps the served file as pgp.db.prev-<stamp>. Where the
# server has an editions index (Genizah_Index/Editions), it is derived from this sidecar and is rebuilt
# with it: Editions.new is built from pgp.db.new and gated while the site keeps serving; then the
# service is STOPPED (closing every reader), the sidecar swapped and the generation put in place, and
# the service started. If the build or its gate fails nothing is touched; if the swap, the start or the
# final check fails, both are put back from their .prev copies and the service started again.
#
#   powershell -File scripts/deploy_pgp_sidecar.ps1            # guard, code check, upload, restart
#   powershell -File scripts/deploy_pgp_sidecar.ps1 -DryRun    # guard only; print the rest
param(
    [string]$RemoteHost = "genizah-prod",
    [string]$RemotePath = "",
    [string]$RemoteRepo = $env:GENIZAH_REMOTE_REPO,
    [string]$Sidecar = "pgp_data/pgp.db",
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
if (-not $RemotePath) { $RemotePath = "$RemoteRepo/pgp_data/" }
$RemoteDir = $RemotePath.TrimEnd('/')
$Stamp = Get-Date -Format "yyyyMMdd-HHmm"
$Served = "pgp.db"
$Incoming = "pgp.db.new"
$Previous = "pgp.db.prev-$Stamp"
$Py = "./venv/bin/python"
$EdBuild = "nice -n 19 ionice -c 3 $Py scripts/build_editions_index.py --force --out Genizah_Index/Editions.new --pgp-db $RemoteDir/$Incoming"
$EdGate = "nice -n 19 ionice -c 3 $Py scripts/verify_editions_index.py --gen Genizah_Index/Editions.new --pgp-db $RemoteDir/$Incoming"
$EdPromote = "$Py scripts/build_editions_index.py --promote-only Genizah_Index/Editions.new"
$EdRestore = "$Py scripts/build_editions_index.py --restore-previous"
$EdCheck = "$Py scripts/build_editions_index.py --check-live"
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

Write-Host "== 1/4 shipping guard: $Sidecar"
python scripts/check_shipping_sidecar.py --sidecar $Sidecar
if ($LASTEXITCODE -ne 0) { Fail "the shipping guard refused $Sidecar (exit $LASTEXITCODE); nothing was uploaded" }

if ($DryRun) {
    Write-Host ""
    Write-Host "Dry run. Would now run:"
    Write-Host "  ssh $RemoteHost (confirm the server is running this tree's code)"
    Write-Host "  scp $Sidecar ${RemoteHost}:$RemoteDir/$Incoming"
    Write-Host "  where Genizah_Index/Editions exists:"
    Write-Host "    ssh: $EdBuild"
    Write-Host "         $EdGate"
    Write-Host "    ssh: sudo systemctl stop genizah-web; mv $Served $Previous; mv $Incoming $Served; $EdPromote; sudo systemctl start genizah-web; $EdCheck"
    Write-Host "  otherwise:"
    Write-Host "    ssh: mv $Served $Previous; mv $Incoming $Served; sudo systemctl restart genizah-web"
    exit 0
}

Write-Host "== 2/4 the RUNNING server must already have the code this sidecar was built from"
# The revision comes out of the SIDECAR, not out of the current checkout. pgp_data/*.db is
# gitignored, so a database built at commit A survives a later checkout or commit of B; using
# HEAD here would label it B and clear it against B's code. export_pgp_sidecar.py stamps
# meta.source_revision (and meta.source_dirty) at build time, and this reads that back.
# @(...) and NOT `| Select-Object -First 1`: Select-Object stops the pipeline as soon as it
# has its one item, which can terminate the native process mid-write and leave $LASTEXITCODE
# at -1 -- a deploy that aborts on a repository where nothing is wrong.
$ContractLines = @(python scripts/check_shipping_sidecar.py --sidecar $Sidecar --print-source-revision)
if ($LASTEXITCODE -ne 0) { Fail "could not bind $Sidecar to a repository revision (exit $LASTEXITCODE; the reason is printed above). Nothing was uploaded" }
$Contract = $ContractLines[0]
if (-not $Contract) { Fail "the source-revision check printed nothing; nothing was uploaded" }
Write-Host "   this sidecar was built from $Contract"
# And the server must report what it is RUNNING, not what is checked out. `git reset --hard`
# can land new code while the old process keeps serving -- a checkout that contains the
# revision proves nothing about the reader in memory, and if the restart at step 4 then fails
# the stale reader meets the new sidecar indefinitely. deploy.sh writes .deployed_revision
# after a successful restart, so that file is the only honest answer.
ssh $RemoteHost "set -e; cd $RemoteRepo; test -s .deployed_revision; git fetch -q origin; git merge-base --is-ancestor $Contract `$(cat .deployed_revision)"
if ($LASTEXITCODE -ne 0) { Fail "the running genizah-web does not have $Contract, the commit this sidecar was built from -- or $RemoteRepo/.deployed_revision is missing, which means the service has not completed a ./deploy.sh run since this check was introduced. A checkout alone is not enough: the process in memory is what reads the sidecar. Deploy the code first (ssh $RemoteHost 'cd $RemoteRepo; ./deploy.sh master-main <published commit id>'), then re-run this. Nothing was uploaded" }

Write-Host "== 3/4 upload under a temporary name: $Sidecar -> ${RemoteHost}:$RemoteDir/$Incoming"
scp $Sidecar "${RemoteHost}:$RemoteDir/$Incoming"
if ($LASTEXITCODE -ne 0) { Fail "scp failed (exit $LASTEXITCODE); the served file is untouched" }

$HasEditions = Test-RemoteDir "$RemoteRepo/Genizah_Index/Editions"
if ($HasEditions) {
    Write-Host "== 3b/4 the server has an editions index: build Editions.new from the new sidecar and gate it (the site keeps serving)"
    ssh $RemoteHost "cd $RemoteRepo && $EdBuild && $EdGate"
    if ($LASTEXITCODE -ne 0) { Fail "the editions build or its gate failed (exit $LASTEXITCODE); nothing was swapped; remove $RemoteDir/$Incoming (Genizah_Index/Editions.new is left for a look)" }

    Write-Host "== 4/4 stop genizah-web, keep the served file as $Previous, swap, put the editions generation in place, start"
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
    ssh $RemoteHost "sudo systemctl start genizah-web && sleep 5 && systemctl is-active genizah-web && cd $RemoteRepo && $EdCheck"
    if ($LASTEXITCODE -ne 0) {
        Invoke-RollbackBoth "genizah-web or the served editions generation failed its check (exit $LASTEXITCODE)"
    }
    Write-Host ""
    Write-Host "Deployed $Sidecar, rebuilt the editions index from it, and started genizah-web."
    Write-Host "Rollback: ssh $RemoteHost `"$RollbackBoth`""
    exit 0
}

Write-Host "== 4/4 keep the served file as $Previous, swap, restart genizah-web"
ssh $RemoteHost "set -e; cd $RemoteDir; test -f $Served; mv $Served $Previous; mv $Incoming $Served"
if ($LASTEXITCODE -ne 0) {
    $why = "the swap failed (exit $LASTEXITCODE)"
    # The first rename may have run: the served file then waits as $Previous; put it back
    # (the service still runs and reads it after its next restart; Codex review of PR #18).
    ssh $RemoteHost "cd $RemoteDir && { test -e $Served || mv $Previous $Served; }"
    if ($LASTEXITCODE -eq 0) { Fail "$why; $Served is the served file again; nothing else was changed" }
    Fail "$why; $Served COULD NOT BE PUT BACK (exit $LASTEXITCODE): restore it from $Previous by hand"
}
ssh $RemoteHost "sudo systemctl restart genizah-web"
if ($LASTEXITCODE -ne 0) { Fail "restart failed (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`"" }

Write-Host ""
Write-Host "Deployed $Sidecar and restarted genizah-web."
Write-Host "Rollback: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`""
exit 0
