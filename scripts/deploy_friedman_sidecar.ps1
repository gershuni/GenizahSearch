# Deploy the Friedman dictionary sidecar to the web server as friedman_data/friedman.db.
#
# The owner runs this (scp to the server is blocked for Claude sessions). The sidecar is read only by
# shared/friedman_service.py, which opens it read-only and refuses a file that fails its checks, so a bad upload
# hides the tab rather than breaking a page; this script makes sure a bad upload never gets served at all.
#
# The order is the Blau deploy's (scripts/deploy_blau_sidecar.ps1): the gate on the file here, upload under a
# temporary name, the digest and the gate again ON THE SERVER, keep the served file for a rollback, swap,
# restart, the gate on what is served. Every step is followed by an exit-code check; nothing after a failed
# step runs. The gate is scripts/verify_friedman_sidecar.py, which needs only the file and the code (it
# recomputes every token row from the texts the file holds), so the CODE must be on the server first: step 2
# checks that. The tab itself stays off until FRIEDMAN_DICTIONARY_ENABLED=1 is in the server's .env; this
# script does not touch the switch.
#
#   powershell -File scripts/deploy_friedman_sidecar.ps1 -Units 7797 -IndexRows 3292 -Tokens 198096          # check, upload, verify, swap, restart
#   powershell -File scripts/deploy_friedman_sidecar.ps1 -Units 7797 -IndexRows 3292 -Tokens 198096 -DryRun  # local check only; print the rest
#
# The three counts are the sidecar's own meta (python -c "import sqlite3; ..." or the last build's output).
# Rollback (prints the exact names at the end):
#   ssh <host> "cd <repo>/friedman_data && mv friedman.db.prev-<stamp> friedman.db && sudo systemctl restart genizah-web"
param(
    [Parameter(Mandatory = $true)][int]$Units,
    [Parameter(Mandatory = $true)][int]$IndexRows,
    [Parameter(Mandatory = $true)][int]$Tokens,
    [string]$Candidate = "friedman_data/friedman.db",
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
if (-not $RemoteRepo) { Fail "set GENIZAH_REMOTE_REPO (the server checkout, e.g. in your user environment) or pass -RemoteRepo" }
$RemoteDir = "$RemoteRepo/friedman_data"
$Served = "friedman.db"
$Incoming = "friedman.db.new"
$Previous = "friedman.db.prev-$Stamp"
$Gate = "python3 scripts/verify_friedman_sidecar.py --expect-units $Units --expect-index-rows $IndexRows --expect-tokens $Tokens"

Write-Host "== 1/7 local gate: $Candidate"
python scripts/verify_friedman_sidecar.py --db $Candidate --expect-units $Units --expect-index-rows $IndexRows --expect-tokens $Tokens
if ($LASTEXITCODE -ne 0) { Fail "the local gate refused $Candidate (exit $LASTEXITCODE); nothing was uploaded" }
$Sha = (Get-FileHash -Algorithm SHA256 $Candidate).Hash.ToLower()
Write-Host "   sha256 $Sha"

if ($DryRun) {
    Write-Host ""
    Write-Host "Dry run. Would now run:"
    Write-Host "  ssh: test that $RemoteRepo/scripts/verify_friedman_sidecar.py exists (the code is deployed)"
    Write-Host "  scp $Candidate ${RemoteHost}:$RemoteDir/$Incoming"
    Write-Host "  ssh: sha256sum -c on $Incoming ($Sha); $Gate --db friedman_data/$Incoming"
    Write-Host "  ssh: mv $Served $Previous (if it exists); mv $Incoming $Served; sudo systemctl restart genizah-web"
    Write-Host "  ssh: $Gate --db friedman_data/$Served; systemctl is-active genizah-web"
    exit 0
}

Write-Host "== 2/7 the gate is on the server (the code of PR #487 is deployed)"
ssh $RemoteHost "test -f $RemoteRepo/scripts/verify_friedman_sidecar.py && test -f $RemoteRepo/shared/friedman_service.py && mkdir -p $RemoteDir"
if ($LASTEXITCODE -ne 0) { Fail "scripts/verify_friedman_sidecar.py is not on the server: deploy the code first (git pull), then run this again" }

Write-Host "== 3/7 upload under a temporary name: ${RemoteHost}:$RemoteDir/$Incoming"
scp $Candidate "${RemoteHost}:$RemoteDir/$Incoming"
if ($LASTEXITCODE -ne 0) { Fail "scp of the sidecar failed (exit $LASTEXITCODE); the served file is untouched" }

Write-Host "== 4/7 check the uploaded copy on the server: digest, then the gate"
ssh $RemoteHost "cd $RemoteDir && echo '$Sha  $Incoming' | sha256sum -c - && cd $RemoteRepo && $Gate --db friedman_data/$Incoming"
if ($LASTEXITCODE -ne 0) { Fail "the uploaded copy failed its check (exit $LASTEXITCODE); the served file is untouched; remove $RemoteDir/$Incoming" }

Write-Host "== 5/7 keep the served file as $Previous (if any) and swap"
ssh $RemoteHost "set -e; cd $RemoteDir; if [ -f $Served ]; then mv $Served $Previous; fi; mv $Incoming $Served"
if ($LASTEXITCODE -ne 0) { Fail "the swap failed (exit $LASTEXITCODE); check $RemoteDir by hand: restore $Served from $Previous if it is missing" }

Write-Host "== 6/7 restart genizah-web (the service checks the sidecar once at startup)"
ssh $RemoteHost "sudo systemctl restart genizah-web && sleep 5 && systemctl is-active genizah-web"
if ($LASTEXITCODE -ne 0) { Fail "the restart failed (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`"" }

Write-Host "== 7/7 the gate on the served file"
ssh $RemoteHost "cd $RemoteRepo && $Gate --db friedman_data/$Served"
if ($LASTEXITCODE -ne 0) { Fail "the served file failed its gate (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`"" }

Write-Host ""
Write-Host "Deployed the Friedman sidecar ($Units units, $IndexRows index rows, $Tokens tokens) and restarted genizah-web."
Write-Host "The tab shows only with FRIEDMAN_DICTIONARY_ENABLED=1 in the server's .env (then restart again)."
Write-Host "Smoke-check by hand: open a Judeo-Arabic page on /browse, click a word, open the Friedman tab, see the status line and the credit;"
Write-Host "the journal line '[init] Friedman''s dictionary not served: ...' must NOT appear in: journalctl -u genizah-web -n 50"
Write-Host "Rollback: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`""
exit 0
