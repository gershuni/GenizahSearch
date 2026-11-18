# Deploy the catalogue text search sidecar to the web server as catalog_data/catalog_text.db
# (docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md section 4.5, kept in the private repository).
#
# Run by hand from a machine with server access (scp to the server is blocked for Claude sessions). The sidecar is read only by
# shared/catalog_text_service.py, which opens it read-only and refuses a file that fails its checks, so a bad
# upload hides the catalogue search rather than breaking a page; this script makes sure a bad upload is never
# served at all. The search is shown only with CATALOG_TEXT_SEARCH_ENABLED=1 in the server's .env.
#
# The order is the Blau and Lane deploys' (scripts/deploy_arabic_sidecar.ps1): the gate on the file here,
# upload under a temporary name, the digest and the gate again ON THE SERVER, keep the served file for a
# rollback, swap, restart, the gate on what is served. Every step is followed by an exit-code check; nothing
# after a failed step runs. The gate is scripts/verify_catalog_text_sidecar.py, which imports the normalizer
# from the repository, so the CODE must be on the server first: step 2 checks that. The gate reads the whole
# file (about 730 MB; a quick_check and a sample re-keyed): about three minutes on each side.
#
# Build the file first, on the machine whose FJMS, PGP and libraries files the server also has:
#   python scripts/build_catalog_text_sidecar.py
# then:
#   powershell -File scripts/deploy_catalog_text_sidecar.ps1 -Rows 2456035            # check, upload, verify, swap, restart
#   powershell -File scripts/deploy_catalog_text_sidecar.ps1 -Rows 2456035 -DryRun    # local check only; print the rest
#
# Rollback (prints the exact names at the end; on a first deploy there is no previous file, so a rollback
# removes the served file, and the catalogue search hides):
#   ssh <host> "cd <repo>/catalog_data && mv catalog_text.db.prev-<stamp> catalog_text.db && sudo systemctl restart genizah-web"
param(
    [Parameter(Mandatory = $true)][int]$Rows,
    [string]$Candidate = "catalog_data/catalog_text.db",
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
if (-not (Test-Path $Candidate)) { Fail "no file at ${Candidate}; build it with python scripts/build_catalog_text_sidecar.py" }
$RemoteDir = "$RemoteRepo/catalog_data"
$Served = "catalog_text.db"
$Incoming = "catalog_text.db.new"
$Previous = "catalog_text.db.prev-$Stamp"
$Gate = "python3 scripts/verify_catalog_text_sidecar.py --expect-rows $Rows"
# A first deploy has no served file to keep; a rollback then removes the served file (the search hides).
$Rollback = "cd $RemoteDir && if [ -f $Previous ]; then mv $Previous $Served; else rm -f $Served; fi && sudo systemctl restart genizah-web"

Write-Host "== 1/7 local gate: $Candidate"
python scripts/verify_catalog_text_sidecar.py --db $Candidate --expect-rows $Rows
if ($LASTEXITCODE -ne 0) { Fail "the local gate refused $Candidate (exit $LASTEXITCODE); nothing was uploaded" }
$Sha = (Get-FileHash -Algorithm SHA256 $Candidate).Hash.ToLower()
Write-Host "   sha256 $Sha"

if ($DryRun) {
    Write-Host ""
    Write-Host "Dry run. Would now run:"
    Write-Host "  ssh: test that $RemoteRepo/scripts/verify_catalog_text_sidecar.py and shared/catalog_text_service.py exist (the code is deployed)"
    Write-Host "  scp $Candidate ${RemoteHost}:$RemoteDir/$Incoming"
    Write-Host "  ssh: sha256sum -c on $Incoming ($Sha); $Gate --db catalog_data/$Incoming"
    Write-Host "  ssh: mv $Served $Previous (if it exists); mv $Incoming $Served; sudo systemctl restart genizah-web"
    Write-Host "  ssh: $Gate --db catalog_data/$Served; systemctl is-active genizah-web"
    exit 0
}

Write-Host "== 2/7 the gate is on the server (the code is deployed)"
ssh $RemoteHost "test -f $RemoteRepo/scripts/verify_catalog_text_sidecar.py && test -f $RemoteRepo/shared/catalog_text_normalize.py && test -f $RemoteRepo/shared/catalog_text_service.py && test -f $RemoteRepo/web/catalog_assets.py && mkdir -p $RemoteDir"
if ($LASTEXITCODE -ne 0) { Fail "scripts/verify_catalog_text_sidecar.py or web/catalog_assets.py is not on the server: deploy the code first (git pull), then run this again" }

Write-Host "== 3/7 upload under a temporary name: ${RemoteHost}:$RemoteDir/$Incoming"
scp $Candidate "${RemoteHost}:$RemoteDir/$Incoming"
if ($LASTEXITCODE -ne 0) { Fail "scp of the sidecar failed (exit $LASTEXITCODE); the served file is untouched" }

Write-Host "== 4/7 check the uploaded copy on the server: digest, then the gate"
ssh $RemoteHost "cd $RemoteDir && echo '$Sha  $Incoming' | sha256sum -c - && cd $RemoteRepo && $Gate --db catalog_data/$Incoming"
if ($LASTEXITCODE -ne 0) { Fail "the uploaded copy failed its check (exit $LASTEXITCODE); the served file is untouched; remove $RemoteDir/$Incoming" }

Write-Host "== 5/7 keep the served file as $Previous (if any) and swap"
ssh $RemoteHost "set -e; cd $RemoteDir; if [ -f $Served ]; then mv $Served $Previous; fi; mv $Incoming $Served"
if ($LASTEXITCODE -ne 0) { Fail "the swap failed (exit $LASTEXITCODE); check $RemoteDir by hand: restore $Served from $Previous if it is missing" }

Write-Host "== 6/7 restart genizah-web (the service checks the sidecar once at startup)"
ssh $RemoteHost "sudo systemctl restart genizah-web && sleep 5 && systemctl is-active genizah-web"
if ($LASTEXITCODE -ne 0) { Fail "the restart failed (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"$Rollback`"" }

Write-Host "== 7/7 the gate on the served file"
ssh $RemoteHost "cd $RemoteRepo && $Gate --db catalog_data/$Served"
if ($LASTEXITCODE -ne 0) { Fail "the served file failed its gate (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"$Rollback`"" }

Write-Host ""
Write-Host "Deployed the catalogue text sidecar ($Rows records) and restarted genizah-web."
Write-Host "With CATALOG_TEXT_SEARCH_ENABLED=1 in .env, smoke-check by hand: /search, mode Catalog (%), search cheese and"
Write-Host "Saadia with Near spellings on; the journal line '[init] Catalogue text search served from ...' must appear,"
Write-Host "and no 'Catalogue text file out of date' warning, in: journalctl -u genizah-web -n 50"
Write-Host "Rollback: ssh $RemoteHost `"$Rollback`""
exit 0
