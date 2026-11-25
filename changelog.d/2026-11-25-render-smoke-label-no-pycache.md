## [Unreleased] -- Release checks: render-smoke on labelled PRs, no bytecode caches in the installer (ci, branch `ci/render-smoke-label-and-no-pycache`)

- **Render-smoke on a labelled PR.** A PR labelled `render-smoke` runs the NiceGUI render-smoke lane
  on Linux in the development repository (`.github/workflows/dev-render-smoke.yml`), with the public
  CI's own steps; before, they first ran after publishing. The release steps ask for it.
- **The installer carries no `__pycache__`.** `GenizahSearchPro.spec` copied the `shared` folder with
  the bytecode caches of the checkout it was built in, so two builds of one commit could
  differ. The app does not need them: the modules' own `.py` files still ship.
- Tests: `tests/test_ci_workflows.py` (both labelled lanes), `tests/test_local_pyinstaller_smoke.py`.
