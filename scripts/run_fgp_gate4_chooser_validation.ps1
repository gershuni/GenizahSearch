# Gate 4 of the FGP source-folders plan: the chooser validated against the larger sidecar, twice.
# Run 1 writes to fgp_data/inventory/gate4_run1 (single run allowed); run 2 writes the record to
# fgp_data/inventory and must reproduce run 1's digest and the declared expectations.
# Latency and memory are measured on each run but are not part of the digest.
# -FgpDb: the sidecar the candidate is built FROM; after a local swap, fgp_data\fgp_transcriptions.pre_9110.db.
param([string]$FgpDb = "fgp_data/fgp_transcriptions.db")
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$env:PYTHONUTF8 = "1"
$log = Join-Path $root "_tmp\fgp_gate4_run.log"
New-Item -ItemType Directory -Force (Join-Path $root "_tmp") | Out-Null
New-Item -ItemType Directory -Force (Join-Path $root "fgp_data\inventory\gate4_run1") | Out-Null

"=== run 1 $(Get-Date -Format s) ===" | Tee-Object -FilePath $log
python scripts/fgp_gate4_chooser_validation.py --out-dir fgp_data/inventory/gate4_run1 --allow-single-run --fgp-db $FgpDb 2>&1 | Tee-Object -FilePath $log -Append
if ($LASTEXITCODE -ne 0) { "run 1 FAILED ($LASTEXITCODE)" | Tee-Object -FilePath $log -Append; exit $LASTEXITCODE }

"=== run 2 $(Get-Date -Format s) ===" | Tee-Object -FilePath $log -Append
python scripts/fgp_gate4_chooser_validation.py --out-dir fgp_data/inventory --fgp-db $FgpDb `
    --previous-summary fgp_data/inventory/gate4_run1/gate4_summary.json `
    --expect-file scripts/fgp_gate4_chooser_validation.expect.json 2>&1 | Tee-Object -FilePath $log -Append
$code = $LASTEXITCODE
"=== done $(Get-Date -Format s) exit $code ===" | Tee-Object -FilePath $log -Append
exit $code
