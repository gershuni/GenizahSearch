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
# Every step here is followed by an exit-code check. Nothing after a failed step runs.
#
#   powershell -File scripts/deploy_pgp_sidecar.ps1            # guard, upload, restart
#   powershell -File scripts/deploy_pgp_sidecar.ps1 -DryRun    # guard only; print the rest
param(
    [string]$RemoteHost = "ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com",
    [string]$RemotePath = "/home/ubuntu/GenizahSearch/pgp_data/",
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

Write-Host "== 1/3 shipping guard: $Sidecar"
python scripts/check_shipping_sidecar.py --sidecar $Sidecar
if ($LASTEXITCODE -ne 0) { Fail "the shipping guard refused $Sidecar (exit $LASTEXITCODE); nothing was uploaded" }

if ($DryRun) {
    Write-Host ""
    Write-Host "Dry run. Would now run:"
    Write-Host "  scp $Sidecar ${RemoteHost}:$RemotePath"
    Write-Host "  ssh $RemoteHost `"sudo systemctl restart genizah-web`""
    exit 0
}

Write-Host "== 2/3 upload: $Sidecar -> ${RemoteHost}:$RemotePath"
scp $Sidecar "${RemoteHost}:$RemotePath"
if ($LASTEXITCODE -ne 0) { Fail "scp failed (exit $LASTEXITCODE); the service was NOT restarted" }

Write-Host "== 3/3 restart genizah-web"
ssh $RemoteHost "sudo systemctl restart genizah-web"
if ($LASTEXITCODE -ne 0) { Fail "restart failed (exit $LASTEXITCODE); the new sidecar is on the server but not yet served" }

Write-Host ""
Write-Host "Deployed $Sidecar and restarted genizah-web. Now push the code (web is not continuous-deploy)."
exit 0
