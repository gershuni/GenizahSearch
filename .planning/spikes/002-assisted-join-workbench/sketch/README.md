# Spike 002 — frozen sketch archive

`join_workbench.py.txt` is a **read-only, frozen copy** of the throwaway Join
Workbench sketch built to run the v8.0.0 Joins Lab design-critique. It is stored
as `.py.txt` (not `.py`) on purpose: it must never be imported, linted, collected
by pytest, or treated as live source. It is **evidence / executable spec**, not
the production base.

## What it is
A ~1000-line PyQt6 desktop sketch (`desktop/join_workbench.py` in its day) plus 8
`# JOINS-SKETCH` hooks wired into `genizah_app.py` and `desktop/result_dialog.py`
(a "Find joins" entry from the row action, the ResultDialog, and Browse). It was
validated across ~6 UAT iterations with Hillel and reviewed by Codex twice.

## How to actually run it
This frozen `.txt` is for reading. To launch the working toy, check out the
annotated tag that preserves the runnable snapshot (file + hooks applied):

```
git checkout spike-002-joins-workbench
python genizah_app.py        # then open any result and click "Find joins"
git checkout master-main      # return
```

## Why it is NOT the build base
Per `CODEX-PRODUCTIONIZE-CRITIQUE.md` (verdict "C-stricter"): the sketch is an
**executable spec**. The v8.0.0 build extracts the validated *logic* (line-break
query composition, cross-side `(sys_id, page±1)` membership, dedup/compaction,
VS/text merge ordering, snippet/page helpers) into a shared, **unit-tested**
module behind a `SearchExecutor` adapter, then rebuilds the UI clean with
`tr()` i18n, public action APIs (no `_vs_*`), and the shared services — it does
**not** polish this file into production.

## Read these next (same folder)
- `DESKTOP-INTEGRATION-NOTES.md` — iteration A–F handoff (the validated design).
- `CODEX-CRITIQUE.md` — Genizah-scholar design critique.
- `CODEX-PRODUCTIONIZE-CRITIQUE.md` — replan-vs-tweak-vs-hybrid verdict + sequence.
- `REVERT.md` — the 8-hook table + reversal recipe used to clean the live tree.
- `../../REQUIREMENTS.md` § "Design-Critique Conclusions & Amendments" — the
  conclusions folded into the GSD requirements (JWB-05/06/10/11/12, join model).
