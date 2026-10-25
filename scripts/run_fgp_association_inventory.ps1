# Gate 1 of docs/plans/FGP_SOURCE_FOLDERS_ASSESSMENT.md: the association inventory,
# run against the declared expectations. Exit 0 = PASS, 1 = a check failed, 2 = usage.
$env:PYTHONUTF8 = "1"
Set-Location (Split-Path -Parent $PSScriptRoot)
python scripts\fgp_association_inventory.py `
  --out-dir fgp_data\inventory `
  --expect-file scripts\fgp_association_inventory.expect.json
exit $LASTEXITCODE
