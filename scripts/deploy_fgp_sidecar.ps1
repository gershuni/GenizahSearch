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
#   powershell -File scripts/deploy_fgp_sidecar.ps1 -Rows 59082            # check, upload, verify, swap, restart
#   powershell -File scripts/deploy_fgp_sidecar.ps1 -Rows 59082 -DryRun    # local check only; print the rest
#
# Rollback (prints the exact names at the end):
#   ssh <host> "cd /home/ubuntu/GenizahSearch/fgp_data && mv fgp_transcriptions.db.prev-<stamp> fgp_transcriptions.db && sudo systemctl restart genizah-web"
param(
    [Parameter(Mandatory = $true)][int]$Rows,
    [string]$Version = "2.0.0",
    [string]$Candidate = "fgp_data/inventory/fgp_transcriptions.candidate.db",
    [string]$Summary = "fgp_data/inventory/gate4_summary.json",
    [string]$RemoteHost = "ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com",
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
    Write-Host "  ssh: mv $Served $Previous; mv $Incoming $Served; sudo systemctl restart genizah-web"
    Write-Host "  ssh: check the served $Served again; systemctl is-active genizah-web"
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

Write-Host "== 4/6 keep the served file as $Previous and swap"
ssh $RemoteHost "set -e; cd $RemoteDir; test -f $Served; mv $Served $Previous; mv $Incoming $Served"
if ($LASTEXITCODE -ne 0) { Fail "the swap failed (exit $LASTEXITCODE); check $RemoteDir by hand: $Served must exist (restore it from $Previous if it does not)" }

Write-Host "== 5/6 restart genizah-web"
ssh $RemoteHost "sudo systemctl restart genizah-web && sleep 5 && systemctl is-active genizah-web"
if ($LASTEXITCODE -ne 0) { Fail "the restart failed (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`"" }

Write-Host "== 6/6 check the served file"
ssh $RemoteHost "cd $RemoteDir && python3 /tmp/check_fgp_sidecar.py --db $Served --sha256 $Sha --version $Version --rows $Rows"
if ($LASTEXITCODE -ne 0) { Fail "the served file failed its check (exit $LASTEXITCODE); roll back: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`"" }

Write-Host ""
Write-Host "Deployed the FGP sidecar ($Rows rows, version $Version) and restarted genizah-web."
Write-Host "Smoke-check by hand: a reading-desk page with an FGP source, a search hit with an FGP source, the /api transcription endpoint."
Write-Host "Rollback: ssh $RemoteHost `"cd $RemoteDir && mv $Previous $Served && sudo systemctl restart genizah-web`""
exit 0
