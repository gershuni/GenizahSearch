## [Unreleased] -- Sidecar deploys check the uploaded file with the web app's own Python (web, branch `claude/deploy-scripts-venv-python`)

### Fixed
- **The catalogue sidecar's deploy stopped on the server although the file was sound.** The
  deploy scripts ran their server-side gate (`scripts/verify_*_sidecar.py`) with the system
  `python3`, which lacks the web app's dependencies: the catalogue gate imports
  `shared/near_spellings.py`, and the first deploy of `catalog_data/catalog_text.db` (2026-11-19)
  stopped at `ModuleNotFoundError: No module named 'rapidfuzz'` after the upload. The gate now runs
  with `venv/bin/python`, the environment `deploy.sh` activates for the web app, in all four
  scripts that have one (`deploy_catalog_text_sidecar.ps1`, `deploy_arabic_sidecar.ps1`,
  `deploy_blau_sidecar.ps1`, `deploy_friedman_sidecar.ps1`; the other three worked only because
  their gates import nothing the system Python lacks). The check that the code is deployed, which
  runs before the upload, also asks for `venv/bin/python`, so a server without it stops before
  730 MB are sent. `deploy_fgp_sidecar.ps1` is unchanged: its checker is a standalone script
  copied to `/tmp`.

### Tests
- `tests/test_deploy_sidecar_gate_python.py`: each gated script runs its gate with
  `venv/bin/python`, asks for it before the upload, and a new gated deploy script must join the
  list. It fails on the scripts as they were.
