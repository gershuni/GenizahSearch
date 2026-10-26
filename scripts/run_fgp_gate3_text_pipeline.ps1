# Gate 3 of the FGP source-folders plan: the unified text pipeline on a separate output DB.
# Runs the pipeline twice (the first run into fgp_data/inventory/gate3_run1, the second into
# fgp_data/inventory with the first run's digest) so check T01 compares two independent runs,
# and checks the second run's counts against the declared expectations.
# Usage: powershell -File scripts/run_fgp_gate3_text_pipeline.ps1
$ErrorActionPreference = "Continue"
$env:PYTHONUTF8 = "1"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if (-not (Test-Path "_tmp")) { New-Item -ItemType Directory -Path "_tmp" | Out-Null }
$log = "_tmp\fgp_gate3_run.log"
$first = "fgp_data\inventory\gate3_run1"
Write-Host "run 1 -> $first"
python scripts\fgp_gate3_text_pipeline.py --allow-single-run --out-dir $first 2>&1 | Tee-Object -FilePath $log
if ($LASTEXITCODE -ne 0) { Write-Host "run 1 FAILED (exit $LASTEXITCODE)"; exit $LASTEXITCODE }
Write-Host "run 2 -> fgp_data\inventory (compared with run 1; declared expectations)"
python scripts\fgp_gate3_text_pipeline.py --previous-summary "$first\gate3_summary.json" `
    --expect-file scripts\fgp_gate3_text_pipeline.expect.json 2>&1 | Tee-Object -FilePath $log -Append
$code = $LASTEXITCODE
Write-Host "exit code $code (log: $log)"
exit $code
