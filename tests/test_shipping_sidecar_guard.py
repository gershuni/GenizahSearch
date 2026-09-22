# -*- coding: utf-8 -*-
"""The owner's withholding decision must survive a build on any machine.

On 2026-09-21 the owner ruled that the regenerated Hebrew PGP translations do not ship:
~28% of them are materially wrong. The website never had them, and the desktop was handled
by lifting ``pgp_translations`` out of ``pgp_data/pgp.db``, because ``GenizahSearchPro.spec``
bundles that file into every installer.

But ``pgp_data/*.db`` is gitignored. The removal is therefore local file state, not a
property of the repository -- so nothing stopped a build host that kept the old sidecar,
or anyone who ran ``scripts/restore_pgp_translations.py`` to measure against the old
corpus, from shipping it anyway. A decision that a routine build can silently reverse is
not enforced.

These tests pin the enforcement, and that ``build_app.bat`` actually invokes it.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sqlite3

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_shipping_sidecar.py"
BUILD_BAT = REPO_ROOT / "build_app.bat"


@pytest.fixture(scope="module")
def guard():
    """Import by path -- scripts/ is a flat namespace package with no __init__."""
    spec = importlib.util.spec_from_file_location("_check_shipping_sidecar", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sidecar(path, with_translations=False, with_doc_relation=True, drop_table=None):
    conn = sqlite3.connect(str(path))
    try:
        doc_cols = "pgpid INTEGER PRIMARY KEY, description TEXT"
        if with_doc_relation:
            doc_cols += ", doc_relation TEXT"
        conn.execute("CREATE TABLE documents (%s)" % doc_cols)
        for table in ("document_sources", "document_footnotes", "document_fragments"):
            conn.execute("CREATE TABLE %s (id INTEGER PRIMARY KEY)" % table)
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        if with_translations:
            conn.execute(
                "CREATE TABLE pgp_translations (pgpid INTEGER PRIMARY KEY,"
                " description_he TEXT)"
            )
            conn.execute("INSERT INTO pgp_translations VALUES (1, 'x')")
        if drop_table:
            conn.execute("DROP TABLE %s" % drop_table)
        conn.commit()
    finally:
        conn.close()
    return str(path)


def test_a_clean_sidecar_ships(guard, tmp_path):
    assert guard.check_sidecar(_sidecar(tmp_path / "pgp.db")) == []


def test_a_sidecar_carrying_the_withheld_translations_is_refused(guard, tmp_path):
    """The exact state produced by scripts/restore_pgp_translations.py."""
    problems = guard.check_sidecar(_sidecar(tmp_path / "pgp.db", with_translations=True))
    assert problems
    assert any("pgp_translations" in p for p in problems)


def test_the_override_is_explicit(guard, tmp_path):
    path = _sidecar(tmp_path / "pgp.db", with_translations=True)
    assert guard.check_sidecar(path, allow_withheld=True) == []


def test_a_pre_1_1_0_sidecar_is_refused(guard, tmp_path):
    """No documents.doc_relation means 891 translations would ship labelled as
    transcriptions -- the defect PR #357 exists to fix."""
    problems = guard.check_sidecar(_sidecar(tmp_path / "pgp.db", with_doc_relation=False))
    assert problems
    assert any("doc_relation" in p for p in problems)
    # ...but an emergency build must still be possible, knowingly.
    assert guard.check_sidecar(
        _sidecar(tmp_path / "other.db", with_doc_relation=False), allow_stale_schema=True
    ) == []


def test_a_missing_core_table_is_refused(guard, tmp_path):
    problems = guard.check_sidecar(
        _sidecar(tmp_path / "pgp.db", drop_table="document_fragments")
    )
    assert any("document_fragments" in p for p in problems)


def test_a_missing_sidecar_is_refused_and_no_stub_is_created(guard, tmp_path):
    """A bare sqlite3.connect() on a missing path creates a 0-byte file that later reads
    as 'no such table'."""
    missing = tmp_path / "nope.db"
    assert guard.check_sidecar(str(missing))
    assert not missing.exists()


def test_a_non_database_is_refused_rather_than_raising(guard, tmp_path):
    junk = tmp_path / "pgp.db"
    junk.write_bytes(b"this is not a database")
    assert guard.check_sidecar(str(junk))


def _live_bat_lines():
    """build_app.bat's executable lines, with REM comments dropped.

    Comments matter here: an earlier version of this test asserted only that the string
    'check_shipping_sidecar.py' appeared somewhere in the file, which a commented-out call
    satisfies perfectly. Mutating the real call to 'REM python scripts\\...' left the test
    green -- so the test was checking that the guard is *mentioned*, not that it *runs*.
    """
    lines = []
    for raw in BUILD_BAT.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = raw.strip()
        if not stripped or stripped.upper().startswith("REM") or stripped.startswith("::"):
            continue
        lines.append(stripped)
    return lines


def test_build_app_bat_actually_runs_the_guard():
    """A guard the build does not call is decoration."""
    lines = _live_bat_lines()
    calls = [i for i, ln in enumerate(lines) if "check_shipping_sidecar.py" in ln]
    assert calls, "build_app.bat must INVOKE the guard, not merely mention it"

    # It must run after the WAL checkpoint, or it would inspect rows still sitting in a
    # journal and could pass a sidecar whose real contents differ.
    checkpoints = [i for i, ln in enumerate(lines) if "checkpoint_sidecars.py" in ln]
    assert checkpoints, "the WAL checkpoint must also be a live call"
    assert checkpoints[0] < calls[0]

    # ...and before anything is packaged.
    builds = [i for i, ln in enumerate(lines) if "PyInstaller" in ln]
    assert builds, "build_app.bat must still invoke PyInstaller"
    assert calls[0] < builds[0]


def test_the_guards_failure_stops_the_build():
    """Without the errorlevel check the build would carry on and ship it anyway."""
    lines = _live_bat_lines()
    call = next(i for i, ln in enumerate(lines) if "check_shipping_sidecar.py" in ln)
    build = next(i for i, ln in enumerate(lines) if "PyInstaller" in ln)
    between = lines[call + 1:build]
    assert any("errorlevel 1" in ln and "exit" in ln for ln in between), (
        "a non-zero exit from check_shipping_sidecar.py must abort build_app.bat"
    )


def test_the_installer_script_checks_the_bundled_database():
    """The route I got wrong, and said so publicly.

    `CompileScriptGenizah.iss` is routinely compiled by hand (`ISCC ...`), which never
    runs `build_app.bat` and never evaluates `GenizahSearchPro.spec`. Its `[Files]`
    entries package `dist\\GenizahSearchPro` recursively, so a stale `dist` from an earlier
    build ships whatever it contains. Putting the check in the spec closed only the
    direct-PyInstaller route; the guard must also run from here, against the BUILT copy.
    """
    iss = (REPO_ROOT / "CompileScriptGenizah.iss").read_text(
        encoding="utf-8", errors="replace"
    )
    lines = [
        ln.strip() for ln in iss.splitlines()
        if ln.strip() and not ln.strip().startswith(";")
    ]
    # Only lines that actually INVOKE it count. An earlier version of this test matched
    # "--bundled" anywhere on a line mentioning the script, which the `#error` message
    # text satisfies -- so dropping the flag from the real Exec() left the test green.
    invocations = [
        i for i, ln in enumerate(lines)
        if "check_shipping_sidecar.py" in ln and "Exec(" in ln
    ]
    assert invocations, "CompileScriptGenizah.iss must INVOKE the shipping guard"
    assert all("--bundled" in lines[i] for i in invocations), (
        "the installer must check the BUILT sidecar under dist/, not the source one"
    )
    checks = invocations
    assert any("#error" in ln for ln in lines[checks[0]:checks[0] + 4]), (
        "a failed check must abort compilation, not just print"
    )

    files = next(i for i, ln in enumerate(lines) if ln.startswith("[Files]"))
    assert checks[0] < files, "the guard must run before anything is packaged"


def test_build_app_bat_checks_the_built_output_too():
    """Checking only the source sidecar leaves the artifact unverified."""
    lines = _live_bat_lines()
    bundled = [i for i, ln in enumerate(lines) if "--bundled" in ln]
    assert bundled, "build_app.bat must also check the BUILT sidecar"
    build = next(i for i, ln in enumerate(lines) if "PyInstaller" in ln)
    assert bundled[0] > build, "the bundled check only means anything after the build"


DEPLOY_PS1 = REPO_ROOT / "scripts" / "deploy_pgp_sidecar.ps1"
REFRESH_PS1 = REPO_ROOT / "scripts" / "refresh_pgp_data.ps1"


def test_the_web_deploy_is_guarded_too():
    """The web reads pgp_translations through TranslationService, so an unconditional
    scp can republish the withheld corpus that build_app.bat blocks for desktop.

    Round 3 pinned a bash `&&` chain here. An independent audit then fed that chain to the
    PowerShell 5.1 parser this project deploys from: "The token '&&' is not a valid
    statement separator" -- the whole statement is rejected, the operator retypes it as
    two lines, and the scp runs whether or not the guard passed. So the guide now hands
    the upload to scripts/deploy_pgp_sidecar.ps1, and any scp of the sidecar that the
    guide still shows must be either inside that script or explicitly `&&`-chained."""
    guide = (REPO_ROOT / "docs" / "guides" / "DEPLOYMENT_TECHNICAL.md").read_text(
        encoding="utf-8", errors="replace"
    )
    assert "deploy_pgp_sidecar.ps1" in guide, "the guide must point at the guarded deploy"

    lines = guide.splitlines()
    for i, ln in enumerate(lines):
        if "scp pgp_data/pgp.db" not in ln:
            continue
        window = "\n".join(lines[max(0, i - 2):i + 1])
        assert "check_shipping_sidecar" in window and "&&" in window, (
            "a bare `scp pgp_data/pgp.db` in the guide can republish the withheld corpus "
            "(line %d): %r -- use scripts/deploy_pgp_sidecar.ps1" % (i + 1, ln.strip())
        )


def _ps1_lines(path):
    return [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines()]


def test_the_deploy_script_uploads_only_after_the_guard_passes():
    """Order AND consumption of the exit code: guard, then a `$LASTEXITCODE -ne 0` check,
    then scp, then another check, then the restart. build_app.bat has the same shape."""
    lines = _ps1_lines(DEPLOY_PS1)
    guard = next(i for i, ln in enumerate(lines)
                 if ln.startswith("python scripts/check_shipping_sidecar.py"))
    upload = next(i for i, ln in enumerate(lines) if ln.startswith("scp "))
    restart = next(i for i, ln in enumerate(lines) if ln.startswith("ssh "))
    assert guard < upload < restart

    def checked(after, before):
        between = lines[after + 1:before]
        return any("$LASTEXITCODE -ne 0" in ln and ("Fail" in ln or "exit" in ln)
                   for ln in between)

    assert checked(guard, upload), "the guard's exit code must gate the upload"
    assert checked(upload, restart), "a failed upload must not be followed by a restart"
    code = [ln for ln in lines if not ln.startswith("#")]
    assert not any("&&" in ln for ln in code), "PowerShell 5.1: `&&` is a parse error"


def test_the_deploy_script_really_stops_when_the_guard_refuses(tmp_path):
    """Behaviour, not text: run the script (dry run, so no scp) against a sidecar carrying
    the withheld table and read what it did."""
    import shutil
    import subprocess

    powershell = shutil.which("powershell")
    if not powershell:
        pytest.skip("PowerShell not available")
    dirty = _sidecar(tmp_path / "dirty.db", with_translations=True)
    proc = subprocess.run(
        [powershell, "-NoProfile", "-File", str(DEPLOY_PS1), "-DryRun", "-Sidecar", dirty],
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    assert proc.returncode == 1
    assert "DEPLOY ABORTED" in proc.stderr
    assert "Would now run" not in proc.stdout, "the upload step must not be reached"

    clean = _sidecar(tmp_path / "clean.db")
    proc = subprocess.run(
        [powershell, "-NoProfile", "-File", str(DEPLOY_PS1), "-DryRun", "-Sidecar", clean],
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    assert proc.returncode == 0
    assert "Would now run" in proc.stdout


def test_the_refresh_runner_runs_the_documented_steps_in_order_and_stops_on_failure():
    """The guide's block was eight unchained lines; step 5 ran after step 4 exited 1."""
    text = REFRESH_PS1.read_text(encoding="utf-8")
    lines = _ps1_lines(REFRESH_PS1)
    steps = [ln for ln in lines if ln.startswith("Step ")]
    numbers = [int(ln.split()[1]) for ln in steps]
    assert numbers == list(range(9)), numbers
    for expected, ln in zip((
        "fist_shelfmarks_export.py", "fetch_pgp_metadata.py", "pgp_transcriptions_export.py",
        "import_pgp_full.py", "import_pgp_full.py', '--execute", "update_doc_relation.py",
        "import_pgp_sections.py", "export_pgp_sidecar.py", "check_shipping_sidecar.py",
    ), steps):
        assert expected in ln, (expected, ln)

    body = text[text.index("function Step"):text.index("\n}\n") + 3]
    body_lines = [ln.strip() for ln in body.splitlines()]
    assert any("$LASTEXITCODE -ne 0" in ln for ln in body_lines)
    # A STATEMENT that exits -- not the word "exit" inside the error message, which is
    # what a first version of this pin matched while `return` replaced the real exit.
    assert any(ln.startswith("exit ") for ln in body_lines), (
        "a failing step must END the run, not be printed and passed over"
    )
    # The write steps sit behind the -Execute gate, after the dry run.
    assert text.index("-not $Execute") < text.index("Step 4")
    assert not any("&&" in ln for ln in lines if not ln.startswith("#"))


def test_the_withholding_decision_is_recorded_where_it_is_enforced(guard):
    """Reversing the decision should be a one-line edit in an obvious place, with the
    reason attached -- not a silent change of behaviour somewhere else."""
    assert "pgp_translations" in guard.WITHHELD_TABLES
    assert "PGP_TRANSLATION_QUALITY.md" in guard.WITHHELD_TABLES["pgp_translations"]


def test_every_bundled_layout_is_checked_not_just_the_first(guard, tmp_path, monkeypatch):
    """CompileScriptGenizah.iss packages the whole dist tree recursively, so checking
    only the first candidate layout lets a clean `_internal` copy vouch for a stale
    `pgp_data/pgp.db` sitting beside it that still carries the withheld table."""
    clean = tmp_path / "internal.db"
    stale = tmp_path / "stale.db"
    _sidecar(clean)
    _sidecar(stale, with_translations=True)

    monkeypatch.setattr(guard, "BUNDLED_SIDECARS", (str(clean), str(stale)))
    code = guard.main(["--bundled"])
    assert code == 1, "the stale second layout must still fail the guard"


def test_bundled_passes_only_when_every_layout_is_clean(guard, tmp_path, monkeypatch):
    a, b = tmp_path / "a.db", tmp_path / "b.db"
    _sidecar(a)
    _sidecar(b)
    monkeypatch.setattr(guard, "BUNDLED_SIDECARS", (str(a), str(b)))
    assert guard.main(["--bundled"]) == 0


def test_sidecar_and_bundled_are_both_checked(guard, tmp_path, monkeypatch, capsys):
    """`--sidecar X --bundled` used to check only the bundled copies and silently drop X."""
    dirty = _sidecar(tmp_path / "source.db", with_translations=True)
    clean_bundled = _sidecar(tmp_path / "bundled.db")
    monkeypatch.setattr(guard, "BUNDLED_SIDECARS", (clean_bundled,))

    assert guard.main(["--sidecar", dirty, "--bundled"]) == 1
    err = capsys.readouterr().err
    assert "source.db" in err and "pgp_translations" in err


def test_a_view_or_a_leftover_staging_copy_of_the_withheld_table_is_caught(guard, tmp_path):
    """Name-based matching on `type='table'` let two things through: a VIEW called
    pgp_translations, which TranslationService reads exactly as it reads the table, and
    pgp_translations_restore_tmp, which restore_pgp_translations.py stages into and a
    Ctrl-C (a BaseException its `except Exception` does not see) leaves behind."""
    path = _sidecar(tmp_path / "view.db")
    conn = sqlite3.connect(path)
    conn.execute("CREATE VIEW pgp_translations AS SELECT pgpid, description AS description_he "
                 "FROM documents")
    conn.commit()
    conn.close()
    problems = guard.check_sidecar(path)
    assert any("'pgp_translations'" in p for p in problems), problems

    path = _sidecar(tmp_path / "tmp.db")
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE pgp_translations_restore_tmp (pgpid INTEGER)")
    conn.commit()
    conn.close()
    problems = guard.check_sidecar(path)
    assert any("pgp_translations_restore_tmp" in p for p in problems), problems
