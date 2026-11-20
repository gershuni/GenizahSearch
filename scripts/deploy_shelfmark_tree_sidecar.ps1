# Deploy the shelf tree sidecar to the web server as shelfmark_tree_data/shelfmark_tree.db (the shelf tree
# picker plan, section 3.6, kept in the private repository).
#
# Run by hand from a machine with server access (scp to the server is blocked for Claude sessions). The file is
# read only by shared/shelfmark_tree_service.py, which opens it read-only and refuses a file that fails its
# checks, so a bad upload hides "Pick from the shelf" and leaves Browse's previous / next in today's order rather
# than breaking a page; this script makes sure a bad upload is never served at all. The picker shows once the
# file is served (SHELFMARK_TREE_ENABLED is on unless set off in .env).
#
# The order is the other sidecars' (scripts/deploy_catalog_text_sidecar.ps1): the gate on the file here (with
# the FGP dump it was built from), upload under a temporary name, the digest and the gate again ON THE SERVER
# (which has libraries.csv but no FGP dump), keep the served file for a rollback, swap, restart, the gate on
# what is served. Every step is followed by an exit-code check; nothing after a failed step runs. The CODE must
# be on the server first: step 2 checks that. The gate takes seconds (the file is about 60 MB).
#
# Build the file first, on the machine that has the FGP dump and the libraries.csv the server also has:
#   python scripts/build_shelfmark_tree.py
# then:
#   powershell -File scripts/deploy_shelfmark_tree_sidecar.ps1 -Leaves 266673            # check, upload, verify, swap, restart
#   powershell -File scripts/deploy_shelfmark_tree_sidecar.ps1 -Leaves 266673 -DryRun    # local check only; print the rest
#
# Rollback (prints the exact names at the end; on a first deploy there is no previous file, so a rollback
# removes the served file, and the picker hides):
#   ssh <host> "cd <repo>/shelfmark_tree_data && mv shelfmark_tree.db.prev-<stamp> shelfmark_tree.db && sudo systemctl restart genizah-web"
param(
    [Parameter(Mandatory = $true)][int]$Leaves,
    [string]$Candidate = "shelfmark_tree_data/shelfmark_tree.db",
    [string]$Fist = "fist_data/FIST.db",
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

$Stamp = Get-Date -Format "yyyyMMdd-HHmm"
# A dry run is a local check: it needs no server path (the printed steps then show a placeholder).
if (-not $RemoteRepo) {
    if (-not $DryRun) { Fail "set GENIZAH_REMOTE_REPO (the server checkout, e.g. in your user environment) or pass -RemoteRepo" }
    $RemoteRepo = "<GENIZAH_REMOTE_REPO>"
}
if (-not (Test-Path $Candidate)) { Fail "no file at ${Candidate}; build it with python scripts/build_shelfmark_tree.py" }
$RemoteDir = "$RemoteRepo/shelfmark_tree_data"
$Served = "shelfmark_tree.db"
$Incoming = "shelfmark_tree.db.new"
$Previous = "shelfmark_tree.db.prev-$Stamp"
# The server's own Python for the web app (venv/bin/python, as deploy.sh activates it): the gate imports the
# repository's modules, and the system python3 lacks their dependencies. It reads the server's libraries.csv.
$Gate = "venv/bin/python scripts/verify_shelfmark_tree_sidecar.py --expect-leaves $Leaves"
# A first deploy has no served file to keep; a rollback then removes the served file (the picker hides).
$Rollback = "cd $RemoteDir && if [ -f $Previous ]; then mv $Previous $Served; else rm -f $Served; fi && sudo systemctl restart genizah-web"

Write-Host "== 1/7 local gate: $Candidate, built from $Fist"
if (-not (Test-Path $Fist)) { Fail "no FGP dump at ${Fist}: the local gate checks the file was built from it (pass -Fist)" }
python scripts/verify_shelfmark_tree_sidecar.py --db $Candidate --expect-leaves $Leaves --fist $Fist
if ($LASTEXITCODE -ne 0) { Fail "the local gate refused $Candidate (exit $LASTEXITCODE); nothing was uploaded" }
$Sha = (Get-FileHash -Algorithm SHA256 $Candidate).Hash.ToLower()
Write-Host "   sha256 $Sha"

if ($DryRun) {
    Write-Host ""
    Write-Host "Dry run. Would now run:"
    Write-Host "  ssh: test that $RemoteRepo/venv/bin/python, $RemoteRepo/scripts/verify_shelfmark_tree_sidecar.py and shared/shelfmark_tree_service.py exist (the code is deployed)"
    Write-Host "  scp $Candidate ${RemoteHost}:$RemoteDir/$Incoming"
    Write-Host "  ssh: sha256sum -c on $Incoming ($Sha); $Gate --db shelfmark_tree_data/$Incoming"
    Write-Host "  ssh: mv $Served $Previous (if it exists); mv $Incoming $Served; sudo systemctl restart genizah-web"
    Write-Host "  ssh: $Gate --db shelfmark_tree_data/$Served; systemctl is-active genizah-web"
    exit 0
}

Write-Host "== 2/7 the gate is on the server (the code is deployed)"
ssh $RemoteHost "test -x $RemoteRepo/venv/bin/python && test -f $RemoteRepo/scripts/verify_shelfmark_tree_sidecar.py && test -f $RemoteRepo/scripts/build_shelfmark_tree.py && test -f $RemoteRepo/shared/shelfmark_tree_service.py && test -f $RemoteRepo/web/shelfmark_tree_assets.py && test -f $RemoteRepo/libraries.csv && mkdir -p $RemoteDir"
if ($LASTEXITCODE -ne 0) { Fail "scripts/verify_shelfmark_tree_sidecar.py, web/shelfmark_tree_assets.py, libraries.csv or venv/bin/python is not on the server: deploy the code first (git pull), then run this again" }

Write-Host "== 3/7 upload under a temporary name: ${RemoteHost}:$RemoteDir/$Incoming"
scp $Candidate "${RemoteHost}:$RemoteDir/$Incoming"
if ($LASTEXITCODE -ne 0) { Fail "scp of the sidecar failed (exit $LASTEXITCODE); the served file is untouched" }

Write-Host "== 4/7 check the uploaded copy on the server: digest, then the gate"
ssh $RemoteHost "cd $RemoteDir && echo '$Sha  $Incoming' | sha256sum -c - && cd $RemoteRepo && $Gate --db shelfmark_tree_data/$Incoming"
if ($LASTEXITCODE -ne 0) { Fail "the uploaded copy failed its check (exit $LASTEXITCODE); the served file is untouched; remove $RemoteDir/$Incoming" }

Write-Host "== 5/7 keep the served file as $Previous (if any) and swap"
ssh $RemoteHost "set -e; cd $RemoteDir; if [ -f $Served ]; then mv $Served $Previous; fi; mv $Incoming $Served"
if ($LASTEXITCODE -ne 0) { Fail "the swap failed (exit $LASTEXITCODE); check $RemoteDir by hand: restore $Served from $Previous if it is missing" }

Write-Host "== 6/7 restart genizah-web (the web app opens the shelf tree once at startup)"
ssh $RemoteHost "sudo systemctl restart genizah-web && sleep 5 && systemctl is-active genizah-web"
if ($LASTEXITCODE -ne 0) { Fail "the restart failed (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"$Rollback`"" }

Write-Host "== 7/7 the gate on the served file"
ssh $RemoteHost "cd $RemoteRepo && $Gate --db shelfmark_tree_data/$Served"
if ($LASTEXITCODE -ne 0) { Fail "the served file failed its gate (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"$Rollback`"" }

Write-Host ""
Write-Host "Deployed the shelf tree ($Leaves leaves) and restarted genizah-web."
Write-Host "Smoke-check by hand: /browse shows 'Pick from the shelf'; open T-S AS 39.7 and see its place on the shelf;"
Write-Host "previous / next go to T-S AS 39.6 / 39.8. The journal line '[init] Shelf tree served from ...' must appear in:"
Write-Host "journalctl -u genizah-web -n 50"
Write-Host "Rollback: ssh $RemoteHost `"$Rollback`""
exit 0
