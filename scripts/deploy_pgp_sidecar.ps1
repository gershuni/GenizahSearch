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
# upload until the server's checkout already contains the last commit that touched the
# modules which READ this file.
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

# The modules that READ the sidecar. If the newest commit touching any of them is not yet on
# the server, the server cannot be trusted to read what we are about to upload.
#
# DERIVED, NOT HAND-LISTED. Two rounds of review each found a module missing from a list I
# had written out by hand -- first shared/translation_service.py, then web/pages/browse.py --
# so the set is now computed from three signals and pinned by an EXACT match in
# tests/test_shipping_sidecar_guard.py, which prints the corrected block when it fails:
#   1. opens pgp.db itself (a non-docstring "pgp.db" literal)
#   2. imports shared.document_service, at module level or inside a function
#   3. names the 'doc_relation' column, however it came by the dict
# Plus the exporter, which is not a reader: when IT changes the schema changes, so a server
# that does not have it has no business receiving what it produced.
$SidecarReaders = @(
    'scripts/export_pgp_sidecar.py',
    'shared/browse_service.py',
    'shared/document_service.py',
    'shared/export_dossier.py',
    'shared/fgp_service.py',
    'shared/manuscript_details.py',
    'shared/search_serializer.py',
    'shared/transcription_service.py',
    'shared/translation_service.py',
    'web/components/joins_panel.py',
    'web/document_service.py',
    'web/pages/browse.py',
    'web/pages/browse_enrichment.py',
    'web/pages/catalog_browse.py',
    'web/pages/search.py',
    'web/pages/search_results.py',
    'web/stats_service.py'
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
    Write-Host "  ssh $RemoteHost (confirm the server already has the sidecar-reading code)"
    Write-Host "  scp $Sidecar ${RemoteHost}:$RemotePath"
    Write-Host "  ssh $RemoteHost `"sudo systemctl restart genizah-web`""
    exit 0
}

Write-Host "== 2/4 the server must already run the code that reads this sidecar"
# Uncommitted reader changes first: `git log` answers with the last COMMITTED version, so a
# server holding that commit would pass while the sidecar you just built needs the edit still
# sitting in the working tree. The refresh builds a gitignored pgp.db from that tree, so this
# is the ordinary case, not a corner one.
git diff --quiet HEAD -- $SidecarReaders
if ($LASTEXITCODE -ne 0) { Fail "one of $($SidecarReaders -join ', ') differs from HEAD. The commit this check derives is the last COMMITTED one, so an uncommitted reader change would be invisible to it and the sidecar would ship ahead of its code. Commit and push the reader change first. Nothing was uploaded" }
# @(...) and NOT `| Select-Object -First 1`: Select-Object stops the pipeline as soon as it
# has its one item, which can terminate the native git process mid-write and leave
# $LASTEXITCODE at -1 -- a deploy that aborts claiming it could not read the git history,
# on a repository where nothing is wrong. Caught when the reader list grew to 17 paths.
$ContractLines = @(git log -1 --format=%H -- $SidecarReaders)
if ($LASTEXITCODE -ne 0) { Fail "could not read the local git history for the sidecar readers (exit $LASTEXITCODE); nothing was uploaded" }
$Contract = $ContractLines[0]
if (-not $Contract) { Fail "no commit found for any of: $($SidecarReaders -join ', ') -- run this from the repository, not a copy" }
Write-Host "   the readers were last changed in $Contract"
ssh $RemoteHost "set -e; cd $RemoteRepo; git fetch -q origin; git merge-base --is-ancestor $Contract HEAD"
if ($LASTEXITCODE -ne 0) { Fail "the server does not have commit $Contract, which last changed $($SidecarReaders -join ', '). A sidecar whose schema those modules do not yet understand can break the live site -- see the CODE BEFORE DATA note at the top of this script. Deploy the code first (ssh $RemoteHost 'cd $RemoteRepo; ./deploy.sh master-main'), then re-run this. Nothing was uploaded" }

Write-Host "== 3/4 upload: $Sidecar -> ${RemoteHost}:$RemotePath"
scp $Sidecar "${RemoteHost}:$RemotePath"
if ($LASTEXITCODE -ne 0) { Fail "scp failed (exit $LASTEXITCODE); the service was NOT restarted" }

Write-Host "== 4/4 restart genizah-web"
ssh $RemoteHost "sudo systemctl restart genizah-web"
if ($LASTEXITCODE -ne 0) { Fail "restart failed (exit $LASTEXITCODE); the new sidecar is on the server but not yet served" }

Write-Host ""
Write-Host "Deployed $Sidecar and restarted genizah-web."
exit 0
