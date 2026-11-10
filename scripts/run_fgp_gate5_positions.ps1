# Gate 5 of the FGP source-folders plan: the 20-image geometry check and the positions prototype, twice.
# Run 1 writes to fgp_data/inventory/gate5_run1 (single run allowed) and fills the fetch cache; run 2 writes the
# record to fgp_data/inventory, reads the same cache, and must reproduce run 1's digest and the declared expectations.
# Latency and memory are measured on each run but are not part of the digest.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$env:PYTHONUTF8 = "1"
$log = Join-Path $root "_tmp\fgp_gate5_run.log"
New-Item -ItemType Directory -Force (Join-Path $root "_tmp") | Out-Null
New-Item -ItemType Directory -Force (Join-Path $root "fgp_data\inventory\gate5_run1") | Out-Null

"=== run 1 $(Get-Date -Format s) ===" | Tee-Object -FilePath $log
python scripts/fgp_gate5_positions.py --out-dir fgp_data/inventory/gate5_run1 --cache-dir fgp_data/inventory/gate5_cache --allow-single-run 2>&1 | Tee-Object -FilePath $log -Append
if ($LASTEXITCODE -ne 0) { "run 1 FAILED ($LASTEXITCODE)" | Tee-Object -FilePath $log -Append; exit $LASTEXITCODE }

"=== run 2 $(Get-Date -Format s) ===" | Tee-Object -FilePath $log -Append
python scripts/fgp_gate5_positions.py --out-dir fgp_data/inventory --cache-dir fgp_data/inventory/gate5_cache `
    --previous-summary fgp_data/inventory/gate5_run1/gate5_summary.json `
    --expect-file scripts/fgp_gate5_positions.expect.json 2>&1 | Tee-Object -FilePath $log -Append
$code = $LASTEXITCODE
"=== done $(Get-Date -Format s) exit $code ===" | Tee-Object -FilePath $log -Append
exit $code
