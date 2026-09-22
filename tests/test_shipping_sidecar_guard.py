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
import os
import pathlib
import re
import shutil
import sqlite3
import subprocess

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


FAKE_SHA = "0123456789abcdef0123456789abcdef01234567"


def _sidecar(path, with_translations=False, with_doc_relation=True, drop_table=None,
             revision=FAKE_SHA, dirty=False):
    conn = sqlite3.connect(str(path))
    try:
        doc_cols = "pgpid INTEGER PRIMARY KEY, description TEXT"
        if with_doc_relation:
            doc_cols += ", doc_relation TEXT"
        conn.execute("CREATE TABLE documents (%s)" % doc_cols)
        for table in ("document_sources", "document_footnotes", "document_fragments"):
            conn.execute("CREATE TABLE %s (id INTEGER PRIMARY KEY)" % table)
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        # What the deploy binds to: the revision that BUILT this file, stamped at build time.
        if revision is not None:
            conn.execute("INSERT INTO meta VALUES ('source_revision', ?)", (revision,))
        conn.execute("INSERT INTO meta VALUES ('source_dirty', ?)", ("1" if dirty else "0",))
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


def test_the_source_revision_is_read_from_the_sidecar(guard, tmp_path):
    """gpt-5-codex on PR #358 (P1): the deploy used to call the CURRENT HEAD "the commit this
    sidecar was built from", but nothing read such a commit from the file. pgp_data/*.db is
    gitignored, so a database built at A survives a checkout of B and would be cleared against
    B's code. The revision now comes out of the database."""
    rev, problem = guard.read_source_revision(_sidecar(tmp_path / "ok.db"))
    assert problem is None and rev == FAKE_SHA

    rev, problem = guard.read_source_revision(_sidecar(tmp_path / "none.db", revision=None))
    assert rev is None and "no meta.source_revision" in problem
    assert "export_pgp_sidecar.py" in problem, "say how to fix it"

    rev, problem = guard.read_source_revision(_sidecar(tmp_path / "unknown.db",
                                                       revision="unknown"))
    assert rev is None and "no meta.source_revision" in problem

    rev, problem = guard.read_source_revision(_sidecar(tmp_path / "dirty.db", dirty=True))
    assert rev is None and "uncommitted changes" in problem, (
        "a sidecar built from a dirty tree came from code in no commit and cannot be bound"
    )

    rev, problem = guard.read_source_revision(str(tmp_path / "absent.db"))
    assert rev is None and "does not exist" in problem


def test_the_exporter_stamps_the_revision_it_was_built_from(tmp_path):
    """The other half: a sidecar the guard can bind is only produced if the exporter records
    the stamp. Runs the real create_meta() against a temp database."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_exporter_meta", str(REPO_ROOT / "scripts" / "export_pgp_sidecar.py"))
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # pragma: no cover - optional third-party deps
        pytest.skip("export_pgp_sidecar.py is not importable here: %s" % exc)

    conn = sqlite3.connect(str(tmp_path / "built.db"))
    try:
        module.create_meta(conn.cursor(), 1, 1, 1, 1, "https://example.supabase.co")
        meta = dict(conn.execute("SELECT key, value FROM meta"))
    finally:
        conn.close()

    assert "source_revision" in meta, "the sidecar must record the code that built it"
    assert "source_dirty" in meta and meta["source_dirty"] in ("0", "1")
    # In this checkout git answers, so the stamp must be a real sha rather than the
    # unknown-provenance fallback.
    assert len(meta["source_revision"]) == 40, meta["source_revision"]

    revision, dirty = module.source_revision()
    assert revision == meta["source_revision"] and isinstance(dirty, bool)

    # Fail CLOSED. If git cannot answer, the build has unknown provenance, which must be
    # recorded as unpinnable -- never as a clean build, which the deploy would then accept.
    def _no_git(*_args, **_kwargs):
        raise OSError(2, "No such file or directory", "git")

    real_run = module.subprocess.run
    module.subprocess.run = _no_git
    try:
        assert module.source_revision() == (None, True)
    finally:
        module.subprocess.run = real_run

    # ...and the same when git is present but answers non-zero (not a repository).
    class _Failed:
        returncode = 128
        stdout = ""

    module.subprocess.run = lambda *a, **k: _Failed()
    try:
        assert module.source_revision() == (None, True)
    finally:
        module.subprocess.run = real_run


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


def test_deploy_sh_records_the_running_revision_only_after_a_successful_restart():
    """gpt-5-codex on PR #358 (P1): a checkout is not a deployment. `git reset --hard` can land
    new code while the old process keeps serving, so the sidecar deploy must ask what is
    RUNNING. deploy.sh answers that question, and its answer is only honest if it is written
    after the restart and only when the service actually came up."""
    sh = (REPO_ROOT / "deploy.sh").read_text(encoding="utf-8", errors="replace")
    lines = [ln.strip() for ln in sh.splitlines()]

    restart = next(i for i, ln in enumerate(lines) if ln.startswith("sudo systemctl restart"))
    write = next(i for i, ln in enumerate(lines) if "> .deployed_revision" in ln)
    guard = next(i for i, ln in enumerate(lines) if "is-active" in ln and ln.startswith("if "))
    assert restart < guard < write, (
        "the running revision must be recorded after the restart and behind the liveness "
        "check, or it records code that is checked out but not serving"
    )
    assert any("rm -f .deployed_revision" in ln for ln in lines), (
        "a failed restart must CLEAR the marker, not leave a stale one that would clear a "
        "later sidecar upload against a process that never loaded this code"
    )
    assert "exit 1" in sh[sh.index("is-active"):], "a failed restart must fail the deploy"


def test_the_deploy_script_uploads_only_after_the_guard_passes():
    """Order AND consumption of the exit code: guard, then a `$LASTEXITCODE -ne 0` check,
    then the code-before-data check, then scp, then another check, then the restart.
    build_app.bat has the same shape."""
    lines = _ps1_lines(DEPLOY_PS1)
    guard = next(i for i, ln in enumerate(lines)
                 if ln.startswith("python scripts/check_shipping_sidecar.py --sidecar $Sidecar")
                 and "--print-source-revision" not in ln)
    contract = next(i for i, ln in enumerate(lines)
                    if ln.startswith("$ContractLines = @(python ")
                    and "--print-source-revision" in ln)
    ancestor = next(i for i, ln in enumerate(lines)
                    if ln.startswith("ssh ") and "--is-ancestor" in ln)
    upload = next(i for i, ln in enumerate(lines) if ln.startswith("scp "))
    restart = next(i for i, ln in enumerate(lines)
                   if ln.startswith("ssh ") and "systemctl restart" in ln)
    assert guard < contract < ancestor < upload < restart

    # STRICT form. gpt-6-astra (round 5) disabled the check with `-and $false` while
    # keeping the text a looser pin matched, and the suite stayed green.
    check = re.compile(r"^if \(\$LASTEXITCODE -ne 0\) \{ Fail ")

    def checked(after, before):
        return any(check.match(ln) for ln in lines[after + 1:before])

    assert checked(guard, contract), "the guard's exit code must gate everything after it"
    assert checked(contract, ancestor), (
        "a sidecar that cannot be bound to a revision must stop the deploy"
    )
    assert checked(ancestor, upload), (
        "the server-has-the-code check must gate the upload -- a sidecar whose schema the "
        "deployed code cannot read took the live site's browse enrichment down on 2026-09-22"
    )
    assert checked(upload, restart), "a failed upload must not be followed by a restart"
    assert any(check.match(ln) for ln in lines[restart + 1:]), (
        "a failed restart must be reported, not swallowed"
    )
    # One check per external command, and nothing else that looks like one.
    commands = [i for i, ln in enumerate(lines)
                if ln.startswith(("python ", "scp ", "ssh ", "git ", "$ContractLines = @("))]
    assert len(commands) == 5
    # Select-Object would stop the pipeline early and can kill the native process mid-write,
    # leaving $LASTEXITCODE at -1 on a perfectly healthy repository.
    assert not any("$ContractLines" in ln and "Select-Object" in ln for ln in lines), (
        "capture the revision with @(...), never through Select-Object -First"
    )
    text = DEPLOY_PS1.read_text(encoding="utf-8")
    # Four review rounds defeated an earlier design that named the modules reading the
    # sidecar: every enumeration miss failed open. It must not creep back.
    assert "$SidecarReaders" not in text, (
        "the deploy must not enumerate sidecar-reading modules -- see the CODE BEFORE DATA "
        "note in the script"
    )
    # The revision must come OUT OF THE SIDECAR. Deriving it from the checkout labels a
    # gitignored database with whatever commit happens to be current.
    assert "--print-source-revision" in text and "git rev-parse HEAD" not in text, (
        "bind the upload to meta.source_revision read from the sidecar, not to the local HEAD"
    )
    # ...and the server must answer for what it is RUNNING, not what is checked out. Pinned on
    # the COMMAND, not on the text anywhere in the file: the first version of this assertion
    # passed a mutation that compared against remote HEAD, because the error message still
    # mentioned .deployed_revision.
    ancestor_line = lines[ancestor]
    assert "cat .deployed_revision" in ancestor_line, (
        "a checkout is not a deployment: the ancestor test must run against the revision "
        "deploy.sh records after a successful restart, not against the server's HEAD -- %r"
        % ancestor_line
    )
    assert "test -s .deployed_revision" in ancestor_line, (
        "an absent marker means the service has not completed a deploy.sh run; that must "
        "fail, not compare against an empty string"
    )
    assert "--is-ancestor $Contract HEAD" not in ancestor_line, (
        "comparing against the server's HEAD is exactly the hole this replaced"
    )
    for i in commands:
        following = next(ln for ln in lines[i + 1:] if ln)
        assert check.match(following), "the line after %r must be its exit-code check" % lines[i]
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
    # Strict form: `-and $false` inside the condition kept a looser pin green.
    assert any(ln == "if ($LASTEXITCODE -ne 0) {" for ln in body_lines), body_lines
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


def test_the_withheld_table_is_caught_whatever_its_case(guard, tmp_path):
    """SQLite identifiers are case-insensitive: `CREATE TABLE PGP_TRANSLATIONS` is read by
    `SELECT ... FROM pgp_translations`. gpt-6-astra shipped one through the guard."""
    path = _sidecar(tmp_path / "caps.db")
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE PGP_TRANSLATIONS (pgpid INTEGER PRIMARY KEY, description_he TEXT)")
    conn.execute("INSERT INTO PGP_TRANSLATIONS VALUES (1, 'x')")
    conn.commit()
    # ...and the lower-case query really does read it.
    assert conn.execute("SELECT COUNT(*) FROM pgp_translations").fetchone()[0] == 1
    conn.close()
    problems = guard.check_sidecar(path)
    assert any("PGP_TRANSLATIONS" in p for p in problems), problems


# ── the .ps1 files, run for real with stubbed remote commands ─────────────────
#
# gpt-6-astra (round 6) kept every text pin green while (a) appending `; $LASTEXITCODE = 0`
# to the ssh line, (b) inserting `$LASTEXITCODE = 0` after `& python @Command` in the
# refresh runner, and (c) adding `if ($message -like "scp failed*") { return }` inside Fail.
# Each made the script report success after a failure. Text cannot pin behaviour; running
# the script can. `scp`, `ssh` and `python` are shadowed by .cmd stubs first on PATH that
# log their invocation and exit with a chosen code.


def _powershell():
    exe = shutil.which("powershell")
    if not exe or os.name != "nt":
        pytest.skip("Windows PowerShell not available")
    return exe


def _stubs(tmp_path, fail_on=None, **exit_codes):
    """Each stub logs its invocation and exits with `exit_codes[name]`. With `fail_on=<the
    full argument string of one invocation>` that invocation exits 7 instead -- so a step
    in the MIDDLE of a sequence can be made to fail, not only the first.

    Deliberately no `find`/`type` tricks: on this machine Git's usr/bin precedes System32
    on PATH, so `find /c /v ""` is GNU find walking the C: drive."""
    stubs = tmp_path / "stubs"
    stubs.mkdir()
    for name, code in exit_codes.items():
        (stubs / (name + ".cmd")).write_text(
            "@echo off\r\n"
            "echo " + name + " %* >> \"%STUB_LOG%\"\r\n"
            # ssh's arguments carry double quotes; strip them before comparing, and prefix
            # with x so an empty argument list cannot break the substitution.
            "set \"ARGS=x%*\"\r\n"
            "set \"ARGS=%ARGS:\"=%\"\r\n"
            "if \"%ARGS%\"==\"x%STUB_FAIL_ON%\" exit /b 7\r\n"
            "exit /b " + str(code) + "\r\n",
            encoding="ascii",
        )
    log = tmp_path / "stub.log"
    env = dict(os.environ, PATH=str(stubs) + os.pathsep + os.environ.get("PATH", ""),
               STUB_LOG=str(log), STUB_FAIL_ON=fail_on or "<never>")
    return env, log


def _run_ps1(script, args, env):
    proc = subprocess.run(
        [_powershell(), "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script), *args],
        capture_output=True, text=True, cwd=str(REPO_ROOT), env=env, timeout=120,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _log_lines(log):
    return [ln.split()[0] for ln in log.read_text(encoding="ascii", errors="replace").splitlines()
            if ln.strip()] if log.exists() else []


def _log_raw(log):
    """Full invocations, arguments included. `_log_lines` keeps only the command name, which
    cannot tell one ssh from another -- and the whole point of the pre-upload ssh is WHICH
    revision it asks the server about."""
    return [ln for ln in log.read_text(encoding="ascii", errors="replace").splitlines()
            if ln.strip()] if log.exists() else []


# A throwaway host/repo so the restart invocation can be named exactly in `fail_on`,
# independently of whatever the script's defaults happen to be.
TEST_HOST = "stub-host"
TEST_REPO = "/srv/stub-repo"
RESTART_ARGS = "%s sudo systemctl restart genizah-web" % TEST_HOST

# No git stub any more: the deploy runs no git at all. The revision it binds to is read out of
# the SIDECAR by check_shipping_sidecar.py (real python, real sqlite, a temp database), so
# these tests are not coupled to whatever is uncommitted in the checkout either.


def _deploy_args(sidecar):
    return ["-Sidecar", sidecar, "-RemoteHost", TEST_HOST, "-RemoteRepo", TEST_REPO]


def test_deploy_refuses_when_the_running_service_is_behind_the_sidecar(tmp_path):
    """2026-09-22, live: the refreshed pgp.db added `documents.doc_relation` (NULL for 80%
    of rows) and was uploaded to a server whose code read it with `.get(k, '')`. Browse
    enrichment raised TypeError 16 times in 8 minutes until the sidecar was rolled back.
    The FIRST ssh asks whether the RUNNING service has the sidecar's revision; a 'no' must
    stop the upload, not warn about it."""
    clean = _sidecar(tmp_path / "clean.db")
    env, log = _stubs(tmp_path, scp=0, ssh=1)
    rc, _out, err = _run_ps1(DEPLOY_PS1, _deploy_args(clean), env)
    assert rc == 1
    assert "does not have" in err and FAKE_SHA in err
    assert ".deployed_revision" in err, "the message must explain what the server is asked for"
    assert "deploy.sh master-main" in err, "the message must name the way out"
    assert _log_lines(log) == ["ssh"], "nothing may be uploaded after that refusal"
    # BEHAVIOURAL, not a text pin: read what was actually asked of the server. A version that
    # compared against the checkout's HEAD passed every text assertion here, because the error
    # message still mentioned .deployed_revision.
    asked = _log_raw(log)[0]
    assert "cat .deployed_revision" in asked, (
        "the server must be asked about the RUNNING revision, not its checkout: %r" % asked
    )
    assert FAKE_SHA in asked, "and asked about the revision that built THIS sidecar"


def test_deploy_refuses_a_sidecar_built_from_an_uncommitted_tree(tmp_path):
    """gpt-5-codex on PR #358 (P1). The server can only be checked against a COMMIT, so a
    sidecar built while tracked files differed from HEAD came from code in no commit at all
    and cannot be bound to a deployment. The stamp is read from the DATABASE, so a later
    commit cannot launder it."""
    built_dirty = _sidecar(tmp_path / "dirty_build.db", dirty=True)
    env, log = _stubs(tmp_path, scp=0, ssh=0)
    rc, _out, err = _run_ps1(DEPLOY_PS1, _deploy_args(built_dirty), env)
    assert rc == 1
    assert "uncommitted changes" in err
    assert _log_lines(log) == [], "nothing may run once the sidecar cannot be bound"


def test_deploy_refuses_a_sidecar_that_records_no_revision(tmp_path):
    """A sidecar built before the stamp existed, or by something that did not stamp it, has
    unknown provenance -- it must not be waved through on the current checkout's HEAD."""
    unstamped = _sidecar(tmp_path / "unstamped.db", revision=None)
    env, log = _stubs(tmp_path, scp=0, ssh=0)
    rc, _out, err = _run_ps1(DEPLOY_PS1, _deploy_args(unstamped), env)
    assert rc == 1
    assert "no meta.source_revision" in err
    assert "export_pgp_sidecar.py" in err, "the message must name the rebuild"
    assert _log_lines(log) == []


def test_deploy_really_stops_when_scp_fails(tmp_path):
    clean = _sidecar(tmp_path / "clean.db")
    env, log = _stubs(tmp_path, scp=1, ssh=0)
    rc, _out, err = _run_ps1(DEPLOY_PS1, _deploy_args(clean), env)
    assert rc == 1
    assert "DEPLOY ABORTED: scp failed" in err
    assert _log_lines(log) == ["ssh", "scp"], "no restart after a failed upload"


def test_deploy_really_reports_a_failed_restart(tmp_path):
    clean = _sidecar(tmp_path / "clean.db")
    env, log = _stubs(tmp_path, scp=0, ssh=0, fail_on=RESTART_ARGS)
    rc, out, err = _run_ps1(DEPLOY_PS1, _deploy_args(clean), env)
    assert rc == 1
    assert "DEPLOY ABORTED: restart failed" in err
    assert "Deployed" not in out
    assert _log_lines(log) == ["ssh", "scp", "ssh"]


def test_deploy_succeeds_end_to_end_when_every_step_does(tmp_path):
    clean = _sidecar(tmp_path / "clean.db")
    env, log = _stubs(tmp_path, scp=0, ssh=0)
    rc, out, _err = _run_ps1(DEPLOY_PS1, _deploy_args(clean), env)
    assert rc == 0
    assert "Deployed" in out
    assert FAKE_SHA in out, "the revision the server was checked against must be shown"
    assert _log_lines(log) == ["ssh", "scp", "ssh"]


def test_refresh_really_stops_at_the_first_failing_step(tmp_path):
    """python is stubbed to fail; the runner must exit with ITS code and run nothing more."""
    env, log = _stubs(tmp_path, python=7)
    rc, _out, err = _run_ps1(REFRESH_PS1, [], env)
    assert rc == 7
    assert "REFRESH STOPPED at step 0" in err
    assert _log_lines(log) == ["python"], "step 1 must not run after step 0 failed"


def test_refresh_runs_every_dry_run_step_when_they_pass(tmp_path):
    env, log = _stubs(tmp_path, python=0)
    rc, out, _err = _run_ps1(REFRESH_PS1, [], env)
    assert rc == 0
    assert "Dry run complete" in out
    assert _log_lines(log) == ["python"] * 4, "steps 0-3, then stop before -Execute"


def test_no_script_resets_the_exit_code_it_is_about_to_check():
    """Belt to the braces above: an assignment to $LASTEXITCODE anywhere in either script
    is how a check gets blinded, and there is no legitimate reason to write one."""
    for script in (DEPLOY_PS1, REFRESH_PS1):
        code = [ln for ln in script.read_text(encoding="utf-8").splitlines()
                if not ln.strip().startswith("#")]
        assert not any(re.search(r"\$LASTEXITCODE\s*=[^=]", ln) for ln in code), script.name


WRITE_STEPS = [
    (4, "scripts/import_pgp_full.py --execute"),
    (5, "scripts/update_doc_relation.py --execute"),
    (6, "scripts/import_pgp_sections.py --execute"),
    (7, "scripts/export_pgp_sidecar.py"),
    (8, "scripts/check_shipping_sidecar.py --sidecar pgp_data/pgp.db"),
]


@pytest.mark.parametrize("step, command", WRITE_STEPS)
def test_refresh_really_stops_when_a_write_step_fails(tmp_path, step, command):
    """gpt-6-astra, round 7: only step 0 failing was exercised, so
    `& python @Command; if ($Number -ge 4) { cmd /c exit 0 }` ran every write step after
    one failed, printed "Refresh complete" and exited 0 -- with every test green."""
    env, log = _stubs(tmp_path, fail_on=command, python=0)
    rc, out, err = _run_ps1(REFRESH_PS1, ["-Execute", "-StartAt", "4"], env)
    assert rc == 7
    assert "REFRESH STOPPED at step %d" % step in err
    assert "Refresh complete" not in out
    assert _log_lines(log) == ["python"] * (step - 3), "nothing after the failed step may run"


def test_refresh_runs_the_whole_write_route_when_every_step_passes(tmp_path):
    env, log = _stubs(tmp_path, python=0)
    rc, out, _err = _run_ps1(REFRESH_PS1, ["-Execute", "-StartAt", "4"], env)
    assert rc == 0
    assert "Refresh complete" in out
    assert _log_lines(log) == ["python"] * 5, "steps 4-8"


def test_refresh_execute_without_start_at_runs_all_nine_steps(tmp_path):
    env, log = _stubs(tmp_path, python=0)
    rc, out, _err = _run_ps1(REFRESH_PS1, ["-Execute"], env)
    assert rc == 0
    assert _log_lines(log) == ["python"] * 9
