## [Unreleased] -- Security alerts are read at session start and before a release (ci, branch `feat/security-alert-checks`)

The public repository had collected 179 Dependabot and 72 code scanning alerts: they are raised there,
and the work happens in the development repository, where no step looked at them.

- `scripts/check_security_alerts.py`: reads the open Dependabot, code scanning and secret scanning
  alerts and exits 1 while one needs a decision (high or critical and not fixed in this checkout; code
  scanning outside `tests/`; any secret). 2 means not checked. It changes nothing on GitHub.
- `scripts/init.ps1` runs it at session start; the release pre-flight runs it and stops on 1.
- `.github/workflows/dependency-audit.yml`: `pip-audit` on a PR that changes a requirements file, and
  every Monday in the public repository.
