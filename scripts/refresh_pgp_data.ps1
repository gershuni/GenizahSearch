# Run the PGP data refresh (docs/guides/DEPLOYMENT_TECHNICAL.md, "Refreshing the PGP data")
# as ONE sequence that stops at the first failing step.
#
# WHY A SCRIPT. The guide's block was eight commands on separate lines. Every step now
# exits non-zero when it must not be followed (a checksum mismatch, a classification that
# was not applied, an export that came back short) -- but nothing CONSUMED those exit
# codes: pasted into a console, line 5 runs after line 4 failed. It was demonstrated end to
# end: update_doc_relation.py exited 1, the export ran anyway, the shipping guard said "fit
# to ship". In PowerShell 5.1 a pasted block cannot be made to stop either (a `throw` ends
# one statement, not the paste), so the sequence has to be a file.
#
#   powershell -File scripts/refresh_pgp_data.ps1               # steps 0-3: through the import DRY RUN
#   powershell -File scripts/refresh_pgp_data.ps1 -Execute      # all steps, writing to Supabase
#   powershell -File scripts/refresh_pgp_data.ps1 -Execute -StartAt 4   # continue after reading the dry-run report
#
# Steps 0-8 run on the workstation. Deploying is a separate script, on purpose:
#   powershell -File scripts/deploy_pgp_sidecar.ps1
param(
    [switch]$Execute,
    [int]$StartAt = 0
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

function Step([int]$Number, [string]$Title, [string[]]$Command) {
    if ($Number -lt $StartAt) {
        Write-Host "-- step $Number skipped (-StartAt $StartAt): $Title"
        return
    }
    Write-Host ""
    Write-Host "== step $Number : $Title"
    Write-Host "   python $($Command -join ' ')"
    & python @Command
    if ($LASTEXITCODE -ne 0) {
        [Console]::Error.WriteLine("")
        [Console]::Error.WriteLine("REFRESH STOPPED at step $Number ($Title): exit $LASTEXITCODE.")
        [Console]::Error.WriteLine("Nothing after it has run. Fix the cause, then re-run with -StartAt $Number.")
        exit $LASTEXITCODE
    }
}

# 0. The FIST shelfmark supplement (~35,600 shelfmarks libraries.csv lacks). Without it the
#    fragment match rate falls 94.5% -> 87.5% and ~2,900 fragments lose their IIIF images.
Step 0 'FIST shelfmark supplement'               @('scripts/fist_shelfmarks_export.py')
# 1. The upstream CSVs, all pinned to one commit, with per-file checksums recorded.
Step 1 'fetch upstream CSVs (pinned)'            @('scripts/fetch_pgp_metadata.py')
# 2. transcriptions_linked.csv -- derived locally; verified against the fetch; stamped.
Step 2 'derive transcriptions_linked.csv'        @('scripts/pgp_transcriptions_export.py')
# 3. Validate the import without writing. Writes pgp_data/full_import_dry_run_report.txt
#    (full_import_report.txt is the PREVIOUS --execute's report, not this run's).
Step 3 'import: DRY RUN'                         @('scripts/import_pgp_full.py')

if (-not $Execute) {
    Write-Host ""
    Write-Host "Dry run complete. Read pgp_data/full_import_dry_run_report.txt, then:"
    Write-Host "  powershell -File scripts/refresh_pgp_data.ps1 -Execute -StartAt 4"
    exit 0
}

# 4-6. Write to Supabase. Each refuses to run on inputs that failed their checksums, and
#      each exits non-zero on anything short of complete success.
Step 4 'import: EXECUTE'                         @('scripts/import_pgp_full.py', '--execute')
Step 5 'classify doc_relation (verified by read-back)' @('scripts/update_doc_relation.py', '--execute')
Step 6 'import per-canvas sections'              @('scripts/import_pgp_sections.py', '--execute')
# 7. Rebuild the sidecar beside the live file; swap in only after validation.
Step 7 'export pgp.db'                           @('scripts/export_pgp_sidecar.py')
# 8. The same guard build_app.bat and the installer run. Deploy only if this passes.
Step 8 'shipping guard'                          @('scripts/check_shipping_sidecar.py', '--sidecar', 'pgp_data/pgp.db')

Write-Host ""
Write-Host "Refresh complete. pgp_data/pgp.db is built and fit to ship."
Write-Host "Deploy with:  powershell -File scripts/deploy_pgp_sidecar.ps1"
Write-Host "Then update web/stats_service.py::CORPUS_STATS (tests/test_seed023_stats_service.py pins it)."
exit 0
