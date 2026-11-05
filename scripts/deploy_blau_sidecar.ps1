# Deploy the Blau dictionary sidecar to the web server as blau_data/blau.db.
#
# The owner runs this (scp to the server is blocked for Claude sessions). The sidecar is read only by
# shared/blau_service.py, which opens it read-only and refuses a file that fails its checks, so a bad upload
# hides the companion rather than breaking a page; this script makes sure a bad upload never gets served at all.
#
# The order is the FGP deploy's (scripts/deploy_fgp_sidecar.ps1): the gate on the file here, upload under a
# temporary name, the digest and the gate again ON THE SERVER, keep the served file for a rollback, swap,
# restart, the gate on what is served. Every step is followed by an exit-code check; nothing after a failed
# step runs. The gate is scripts/verify_blau_sidecar.py, which imports the builder and the normalizer from the
# repository, so the CODE must be on the server first (the merge of PR #472 pulled): step 2 checks that.
#
#   powershell -File scripts/deploy_blau_sidecar.ps1 -Roots 3047 -Forms 12753            # check, upload, verify, swap, restart
#   powershell -File scripts/deploy_blau_sidecar.ps1 -Roots 3047 -Forms 12753 -DryRun    # local check only; print the rest
#
# Rollback (prints the exact names at the end):
#   ssh <host> "cd /home/ubuntu/GenizahSearch/blau_data && mv blau.db.prev-<stamp> blau.db && sudo systemctl restart genizah-web"
param(
    [Parameter(Mandatory = $true)][int]$Roots,
    [Parameter(Mandatory = $true)][int]$Forms,
    [string]$Version = "1.0.0",
    [string]$Candidate = "blau_data/blau.db",
    [string]$RemoteHost = "genizah-prod",
    [string]$RemoteRepo = "/home/ubuntu/GenizahSearch",
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
$RemoteDir = "$RemoteRepo/blau_data"
$Served = "blau.db"
$Incoming = "blau.db.new"
$Previous = "blau.db.prev-$Stamp"
$Gate = "python3 scripts/verify_blau_sidecar.py --expect-roots $Roots --expect-forms $Forms --expect-version $Version"

Write-Host "== 1/7 local gate: $Candidate"
python scripts/verify_blau_sidecar.py --db $Candidate --expect-roots $Roots --expect-forms $Forms --expect-version $Version
if ($LASTEXITCODE -ne 0) { Fail "the local gate refused $Candidate (exit $LASTEXITCODE); nothing was uploaded" }
$Sha = (Get-FileHash -Algorithm SHA256 $Candidate).Hash.ToLower()
Write-Host "   sha256 $Sha"

if ($DryRun) {
    Write-Host ""
    Write-Host "Dry run. Would now run:"
    Write-Host "  ssh: test that $RemoteRepo/scripts/verify_blau_sidecar.py exists (the code is deployed)"
    Write-Host "  scp $Candidate ${RemoteHost}:$RemoteDir/$Incoming"
    Write-Host "  ssh: sha256sum -c on $Incoming ($Sha); $Gate --db blau_data/$Incoming"
    Write-Host "  ssh: mv $Served $Previous (if it exists); mv $Incoming $Served; sudo systemctl restart genizah-web"
    Write-Host "  ssh: $Gate --db blau_data/$Served; systemctl is-active genizah-web"
    exit 0
}

Write-Host "== 2/7 the gate is on the server (the code of PR #472 is deployed)"
ssh $RemoteHost "test -f $RemoteRepo/scripts/verify_blau_sidecar.py && test -f $RemoteRepo/shared/blau_normalize.py && mkdir -p $RemoteDir"
if ($LASTEXITCODE -ne 0) { Fail "scripts/verify_blau_sidecar.py is not on the server: deploy the code first (git pull), then run this again" }

Write-Host "== 3/7 upload under a temporary name: ${RemoteHost}:$RemoteDir/$Incoming"
scp $Candidate "${RemoteHost}:$RemoteDir/$Incoming"
if ($LASTEXITCODE -ne 0) { Fail "scp of the sidecar failed (exit $LASTEXITCODE); the served file is untouched" }

Write-Host "== 4/7 check the uploaded copy on the server: digest, then the gate"
ssh $RemoteHost "cd $RemoteDir && echo '$Sha  $Incoming' | sha256sum -c - && cd $RemoteRepo && $Gate --db blau_data/$Incoming"
if ($LASTEXITCODE -ne 0) { Fail "the uploaded copy failed its check (exit $LASTEXITCODE); the served file is untouched; remove $RemoteDir/$Incoming" }

Write-Host "== 5/7 keep the served file as $Previous (if any) and swap"
ssh $RemoteHost "set -e; cd $RemoteDir; if [ -f $Served ]; then mv $Served $Previous; fi; mv $Incoming $Served"
if ($LASTEXITCODE -ne 0) { Fail "the swap failed (exit $LASTEXITCODE); check $RemoteDir by hand: restore $Served from $Previous if it is missing" }

Write-Host "== 6/7 restart genizah-web (the service checks the sidecar once at startup)"
ssh $RemoteHost "sudo systemctl restart genizah-web && sleep 5 && systemctl is-active genizah-web"
if ($LASTEXITCODE -ne 0) { Fail "the restart failed (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`"" }

Write-Host "== 7/7 the gate on the served file"
ssh $RemoteHost "cd $RemoteRepo && $Gate --db blau_data/$Served"
if ($LASTEXITCODE -ne 0) { Fail "the served file failed its gate (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`"" }

Write-Host ""
Write-Host "Deployed the Blau sidecar ($Roots roots, $Forms forms, schema $Version) and restarted genizah-web."
Write-Host "Smoke-check by hand: open a Judeo-Arabic page on /browse, click a word, see the card and the credit line;"
Write-Host "the journal line '[init] Blau dictionary not served: ...' must NOT appear in: journalctl -u genizah-web -n 50"
Write-Host "Rollback: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`""
exit 0
