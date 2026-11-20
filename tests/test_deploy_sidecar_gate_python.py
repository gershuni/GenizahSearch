"""The sidecar deploy scripts run their server-side gate with the web app's own Python.

The gates (`scripts/verify_*_sidecar.py`) import the repository's modules, whose dependencies are
installed in the server's virtual environment (`venv`, which `deploy.sh` activates), not in the
system `python3`. On 2026-11-19 the first catalogue deploy stopped on the server with
`ModuleNotFoundError: No module named 'rapidfuzz'` (from `shared/near_spellings.py`), although the
uploaded file was sound and the web app itself had the module.
"""
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
GATED = ["catalog_text", "arabic", "blau", "friedman", "shelfmark_tree"]


@pytest.mark.parametrize("name", GATED)
def test_the_gate_runs_with_the_venv_python(name):
    text = (SCRIPTS / f"deploy_{name}_sidecar.ps1").read_text(encoding="utf-8")
    assert f'$Gate = "venv/bin/python scripts/verify_{name}_sidecar.py' in text
    assert "python3 scripts/" not in text


@pytest.mark.parametrize("name", GATED)
def test_a_server_without_the_venv_stops_before_the_upload(name):
    """The code-deployed check, which runs before the upload, also asks for the venv's Python."""
    text = (SCRIPTS / f"deploy_{name}_sidecar.ps1").read_text(encoding="utf-8")
    check = next(line for line in text.splitlines()
                 if line.startswith('ssh $RemoteHost "test -') and f"verify_{name}_sidecar.py" in line)
    assert "test -x $RemoteRepo/venv/bin/python" in check
    # The real upload, a line of its own (the dry run only prints it, indented, earlier).
    assert text.index(check) < text.index("\nscp $Candidate")


def test_every_gated_deploy_script_is_listed():
    """A new sidecar deploy script with a $Gate joins the list above, so it is held to the same rule."""
    gated = sorted(p.name for p in SCRIPTS.glob("deploy_*_sidecar.ps1")
                   if "$Gate = " in p.read_text(encoding="utf-8"))
    assert gated == sorted(f"deploy_{n}_sidecar.ps1" for n in GATED)
