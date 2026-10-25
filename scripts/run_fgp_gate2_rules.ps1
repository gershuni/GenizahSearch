# Gate 2 of docs/plans/FGP_SOURCE_FOLDERS_ASSESSMENT.md: identity, credit, visibility and
# refresh rules over the gate-1 inventory. Exit 0 = PASS, 1 = a check failed, 2 = usage.
$env:PYTHONUTF8 = "1"
Set-Location (Split-Path -Parent $PSScriptRoot)
python scripts\fgp_gate2_rules.py `
  --out-dir fgp_data\inventory `
  --expect-file scripts\fgp_gate2_rules.expect.json
exit $LASTEXITCODE
