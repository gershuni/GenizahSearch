---
phase: 135-precision-certificate-confidence-bands
plan: 08
status: complete
completed: 2026-07-28
requirements-completed: []
tasks-completed: 1
tasks-total: 1
---

# 135-08 SUMMARY — v2 Discovery Sidecar Production Deploy

**Outcome: ✅ DEPLOYED.** The verified v2 `discovery.db` is live in production via an
atomic manifest swap, deployed ONCE, asset-first, masking-clean against the staged bytes
on the box, with `DISCOVERY_ENABLED` OFF and the rollback drilled in both directions.
Zero user-visible change.

Full step-by-step record: **`135-08-DEPLOY-LOG.md`** (the plan's declared artifact).

## What was deployed

| Field | Value |
|---|---|
| `asset_basename` | `discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff` |
| DB `content_hash` | `33499c5b…` — recomputed on the dev box, re-checked with `sha256sum` on the web box after upload |
| `frame_content_hash` | `53725098…` — matched against `docs/specs/discovery-frames-v2.md` |
| Size | 387,952,640 bytes (370.0 MB) |

Both hashes match 135-07 exactly. The plan's automated acceptance check passes.

## Deviations from the plan (both material — read these)

### 1. The plan's central precondition did not hold: production had no Discovery code

`origin/master-main` was at `7920e5e6` (2026-07-21, the atlas deploy) and the box was reset
to exactly that. Local `master-main` was **181 commits ahead, unpushed** — all of Phase 134
and 135. Missing from the box: `web/discovery_assets.py` (the loader itself),
`shared/discovery_service.py`, `web/discovery.py`, `scripts/verify_discovery_sidecar.py`,
`scripts/bench_discovery.py`.

The plan is written as "changes no user-visible surface … stages the corrected asset."
Without the code that is literally true but useless — the deploy would have been a file
copy nothing reads, and §2.4's verifier, §2.6's readiness smoke and the loader were all
absent, making three of the six `must_haves` unexecutable.

Surfaced to the owner before touching production; owner authorized push + code deploy +
asset deploy. The code delta was discovery-only and additive (5 new modules, plus
`web/feature_flags.py`, `web/main.py`, `web/pages/help.py`), no `requirements.txt` change,
every new surface gated on the fail-closed `discovery_available()`. Push was unblocked by
the 2026-07-28 owner decision withdrawing the `97cad7df` history-expunge gate; the full
local `--scan-repo --strict` passed on HEAD first.

### 2. The on-box masking gate failed on the first attempt — the gate held, no swap occurred

`--scan-repo --strict` returned 8 hits, **all in `pgp_data/pgp_backup_2026-03-11.db`** — a
172 MB manual copy from the March PGP refresh, unrelated to this deploy. The staged asset
itself was clean (`--scan-sqlite` 0 hits, `--scan-asset` 0 hits) and the verifier passed.

Root cause was a naming gap, not an exposure: `.gitignore` excluded the single literal path
`pgp_data/pgp.db`, so copies under other names stayed untracked-but-not-ignored and were
enumerated. Confirmed the live, deliberately-ignored `pgp.db` carries the same strings (4
hits, same pattern ids) — PGP scholarly metadata legitimately cites source corpora, which
is exactly why that exclusion exists. Owner chose to widen the rule rather than delete the
backup. Fixed in `759d7f76` (`pgp_data/*.db`), scoped so the two tracked report files are
unaffected. Re-ran the gate → `GATE_RESULT=PASS`, then swapped.

### 3. No `manifest.prev.json` exists (first deploy) — rollback drilled against the real prior state

§2.5's `cp` had nothing to preserve because no live manifest existed. That absence is also
the independent proof of **deploy-ONCE** (D-04): no v1 asset had ever been deployed. The
prior frame here *is* the fail-closed no-manifest state, and that is what the §3 drill
exercised — the identical atomic-repoint mechanism. From the next deploy onward §2.5 will
produce a real `manifest.prev.json`.

### 4. Scope addition: `docs/specs/discovery-budgets.md`

The plan declares only the deploy log in `files_modified`, but runbook §2.7 — which the
plan says to follow EXACTLY — instructs recording the prod-box RSS there, and STATE.md
notes this deploy unblocks the PERF-01 item deferred from 134-08 Task 3. Added §4.2
MEASURED ACTUALS (prod-box) and retitled §4. Flagging as an in-spirit deviation.

## Gate results

| Gate | Result |
|---|---|
| Dev-box `verify_discovery_sidecar.py --expected-frame-hash 53725098… --require-v2` | exit 0, all invariants |
| Dev-box `check_atlas_masking.py --scan-sqlite --scan-asset --scan-repo --strict` | exit 0 |
| On-box staged verifier (resolved from `manifest.json.candidate`, exact basename, no glob) | exit 0 |
| On-box staged strict masking | exit 0 (attempt 2; attempt 1 correctly blocked the swap) |
| `bench_discovery.py --sample 50 --warm-passes 1` on the prod box | exit 0, nonzero rows |
| Plan's automated acceptance check (both labeled hashes + candidate/prev + atomic) | PASS |
| Rollback drill (atomic repoint away → app up on prior frame → repoint forward) | PASS both directions |

## PERF-01 prod-box actuals (closes the 134-08 Task 3 deferral)

| Metric | Cap | Prod-box actual |
|---|---|---|
| Browse-enrichment added latency (p95) | ≤ 150 ms | **0.49 ms** ✓ |
| `get_work_witnesses` (p95 / max) | request cap ≤ 1.5 s | 200.77 / 357.17 ms ✓ |
| Additional RSS | ≤ 250 MB | **11.2 MB** ✓ |

## Final production state

`genizah-web` active; `/`, `/help`, `/atlas`, `/search`, `/browse` all 200. Loader
`_state.ready = True` (v2 sidecar loaded), `discovery_available() = False` —
`DISCOVERY_ENABLED` is set in neither `.env` nor any systemd drop-in, so it defaults OFF.
No user-visible change. Box `discovery_data/` holds exactly the v2 `.db` + `manifest.json`.

## Key files

- Created: `.planning/phases/135-precision-certificate-confidence-bands/135-08-DEPLOY-LOG.md`
- Modified: `docs/specs/discovery-budgets.md` (§4.2 prod-box actuals — see deviation 4)
- Modified: `.gitignore` (`759d7f76` — `pgp_data/*.db`, see deviation 2)
- Production: `discovery_data/discovery-v1-33499c5b….db` + `manifest.json` on the web box;
  `.masking_patterns` staged at mode 600 so future refreshes can re-run the gate

## Incidental findings (logged, not acted on)

1. **CI was red before this deploy** — two phase-135 tests in `tests/test_discovery_v2_bake.py`
   unconditionally asserted the presence of gitignored `discovery_data/` artifacts that can
   never exist on a runner. Their sibling in the same file already had the correct
   `pytest.skip` presence-gate. Fixed in `c730cb35`; nothing weakened (both still run and
   assert everything, including the hardcoded SHA pin, when the artifacts are present).
2. **`backup.sh` on the web box carries a cleartext Postgres password** targeting the
   `genizah_db` Postgres retired by the Jan-2026 backend removal. Untracked, pre-existing,
   untouched. Worth deleting or rotating.

## Next

135-09 (CERT-01 pre-registration + OC table + deck draw + validator) is now unblocked —
its estimand reads the deployed v2 sidecar and pins the deployed DB `content_hash`.

## Self-Check: PASSED
