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
#   powershell -File scripts/deploy_pgp_sidecar.ps1            # guard, code check, upload, restart
#   powershell -File scripts/deploy_pgp_sidecar.ps1 -DryRun    # guard only; print the rest
param(
    [string]$RemoteHost = "ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com",
    [string]$RemotePath = "/home/ubuntu/GenizahSearch/pgp_data/",
    [string]$RemoteRepo = "/home/ubuntu/GenizahSearch",
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

Write-Host "== 1/4 shipping guard: $Sidecar"
python scripts/check_shipping_sidecar.py --sidecar $Sidecar
if ($LASTEXITCODE -ne 0) { Fail "the shipping guard refused $Sidecar (exit $LASTEXITCODE); nothing was uploaded" }

if ($DryRun) {
    Write-Host ""
    Write-Host "Dry run. Would now run:"
    Write-Host "  ssh $RemoteHost (confirm the server is running this tree's code)"
    Write-Host "  scp $Sidecar ${RemoteHost}:$RemotePath"
    Write-Host "  ssh $RemoteHost `"sudo systemctl restart genizah-web`""
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
if ($LASTEXITCODE -ne 0) { Fail "the running genizah-web does not have $Contract, the commit this sidecar was built from -- or $RemoteRepo/.deployed_revision is missing, which means the service has not completed a ./deploy.sh run since this check was introduced. A checkout alone is not enough: the process in memory is what reads the sidecar. Deploy the code first (ssh $RemoteHost 'cd $RemoteRepo; ./deploy.sh master-main'), then re-run this. Nothing was uploaded" }

Write-Host "== 3/4 upload: $Sidecar -> ${RemoteHost}:$RemotePath"
scp $Sidecar "${RemoteHost}:$RemotePath"
if ($LASTEXITCODE -ne 0) { Fail "scp failed (exit $LASTEXITCODE); the service was NOT restarted" }

Write-Host "== 4/4 restart genizah-web"
ssh $RemoteHost "sudo systemctl restart genizah-web"
if ($LASTEXITCODE -ne 0) { Fail "restart failed (exit $LASTEXITCODE); the new sidecar is on the server but not yet served" }

Write-Host ""
Write-Host "Deployed $Sidecar and restarted genizah-web."
exit 0
