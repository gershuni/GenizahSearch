# 135-08 — v2 Discovery Sidecar Production Deploy Log

**Date:** 2026-07-28 (04:25–04:46 UTC)
**Executed by:** owner-authorized session (D-04 human-approved checkpoint)
**Runbook followed:** `docs/specs/discovery-deploy.md` §2 (asset-first → candidate
manifest → staged gates → ATOMIC manifest swap → restart → smoke), §3 (rollback drill)
**Outcome:** ✅ DEPLOYED. v2 live via an atomic manifest swap, `DISCOVERY_ENABLED` OFF,
zero user-visible change, rollback drilled in both directions.

---

## 1. Deployed identity (both hashes, each labeled — Codex #12)

Matched against `docs/specs/discovery-frames-v2.md` (frozen at `cc114a74`) before upload
and re-resolved from the candidate manifest on the box before the swap.

| Field | Value |
|---|---|
| `asset_basename` | `discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff` |
| DB `content_hash` | `33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff` |
| `frame_content_hash` | `53725098ece6cf152a72425587dc2fe9119261427fc82e008a5b953dcbd2bce7` |
| `schema_version` | `discovery-v1` (`band_vocab_version` = `v2`) |
| Size on disk | 387,952,640 bytes (370.0 MB) |

**Byte-identity chain — the same `content_hash` at all three points:**

| Point | Method | Result |
|---|---|---|
| Dev box, pre-upload | streamed SHA-256 over the exact file | `33499c5b…` ✓ |
| Web box, post-upload | `sha256sum` after the temp→final rename | `33499c5b…` ✓ |
| Web box, pre-swap | manifest `content_hash` resolved from the candidate | `33499c5b…` ✓ |

---

## 2. Prerequisite gap found and closed before the deploy

The plan assumed the Discovery code was already on the web box. **It was not.**
`origin/master-main` sat at `7920e5e6` (2026-07-21, the atlas deploy) and the box was
reset to exactly that commit. Missing: `web/discovery_assets.py` (the loader),
`shared/discovery_service.py`, `web/discovery.py`, `scripts/verify_discovery_sidecar.py`,
`scripts/bench_discovery.py`. Local `master-main` was **181 commits ahead, unpushed** —
all of Phase 134 + 135.

Without the code, the deploy would have been a no-op file copy: §2.4's verifier, §2.6's
readiness smoke, and the loader itself were all absent, making three of this plan's six
`must_haves` unexecutable. Owner authorized push + code deploy + asset deploy.

Code delta to production was discovery-only and additive — `shared/discovery_band_labels.py`,
`shared/discovery_errors.py`, `shared/discovery_service.py`, `web/discovery.py`,
`web/discovery_assets.py` (all new), plus `web/feature_flags.py` (+19), `web/main.py`
(+31/−10), `web/pages/help.py` (+295). No `genizah_core.py`, no `desktop/`, no
`requirements.txt` change (so `deploy.sh`'s pip step was a no-op). Every new surface is
gated on the fail-closed `discovery_available()`.

The push was unblocked by the 2026-07-28 owner decision withdrawing the `97cad7df`
history-expunge gate (STATE.md Blockers; memory `project_git_history_msource_exposure_accepted`).
Forward discipline was honored: the full local `--scan-repo --strict` passed on HEAD
before pushing.

---

## 3. Step-by-step execution

### 3.0 Dev-box gates (§2.1) — both exit 0

```
verify_discovery_sidecar.py <DB> --expected-frame-hash 53725098… --require-v2   → exit 0
  "all invariants pass -- clean."
  coverage-gap report (non-fatal): routing_audit = {'demoted': 2062, 'kept_tie': 4208}
check_atlas_masking.py --scan-sqlite <DB> --scan-asset <DB> --scan-repo --strict → exit 0
  "no matches -- clean."
```

### 3.1 Masking-pattern file staged

`.masking_patterns` (gitignored, 716 B / 15 patterns) scp'd to the box and `chmod 600` —
same secret-handling posture as the `.env` already there. Required because the on-box gate
fails closed (exit 1) with the pattern file unset. Left in place so future refreshes
(FUT-04) can re-run the gate. It is covered by `.gitignore`, and `deploy.sh`'s
`git reset --hard` does not remove ignored untracked files, so it survives deploys.

### 3.2 Push + asset-first upload (§2.2)

```
git push origin master-main            7920e5e6..547abab2   (181 commits)
scp <DB> → discovery_data/<basename>.db.uploading           (temp name)
ssh mv -f  <basename>.db.uploading → <basename>.db          (final immutable name)
sha256sum on the box → 33499c5b…                            ✓ byte-identical
```

Uploaded while the live manifest still did not exist, so nothing could read a
half-transferred file.

### 3.3 Candidate manifest staged (§2.3)

`manifest.json` scp'd to **`manifest.json.candidate`** — deliberately NOT the live
`manifest.json`. Confirmed on the box that **no live `manifest.json` existed**, which
independently proves **deploy-ONCE** (D-04): no v1 asset had ever been deployed — Phase
134-08 Task 3 was deferred here precisely so v2 deploys once, never v1-then-v2.

### 3.4 Code deploy

`./deploy.sh master-main` → `7920e5e6..547abab2`, pip no-op, `genizah-web` restarted.
At this instant the asset and candidate manifest were staged but **no live manifest
existed**, giving a free fail-closed proof:

```
web.discovery_assets.load_discovery_state()
  ready = False    available() = False
service: active    /  200   /help  200   /atlas  200
```

The loader hid cleanly with the asset physically present but unpointed-to — exactly the
required behavior.

### 3.5 Staged verify + strict masking (§2.4) — FIRST ATTEMPT FAILED, gate held

Both gates were run **on the box** against the DB filename and frame hash resolved
**from `manifest.json.candidate`** (never the live manifest, never a `*.db` glob — N6).

**Attempt 1 → `GATE_RESULT=FAIL — DO NOT SWAP`.** The verifier passed (exit 0) and the
staged asset was clean under both `--scan-sqlite` (0 hits) and `--scan-asset` (0 hits),
but `--scan-repo --strict` returned 8 hits, **all in one unrelated file**:
`pgp_data/pgp_backup_2026-03-11.db` — a 172 MB manual copy made during the March PGP refresh.

**No swap was performed.** Root cause was a naming gap, not an exposure: `.gitignore`
excluded the single literal path `pgp_data/pgp.db`, so copies under any other name stayed
untracked-but-NOT-ignored and were enumerated by `--scan-repo`. Verified that the live,
deliberately-ignored `pgp_data/pgp.db` carries the **same** strings (4 hits, same pattern
ids #0/#2/#3) — PGP scholarly metadata legitimately cites source corpora, which is why
that exclusion exists. The file was absent from the dev box, which is why the local scan
was clean.

Closed by widening the rule to the glob `pgp_data/*.db` (commit `759d7f76`), giving backup
siblings the exact treatment `pgp.db` already had. Scoped to `*.db`, so the tracked
`full_import_report.txt` and `import_report.csv` are unaffected (verified with
`git check-ignore` and an unchanged `git ls-files pgp_data/`).

**Attempt 2 → `GATE_RESULT=PASS`:**

```
resolved-from : discovery_data/manifest.json.candidate  (never the live manifest, no glob)
STAGED_DB     : discovery_data/discovery-v1-33499c5b….db
STAGED_FRAME  : 53725098ece6cf152a72425587dc2fe9119261427fc82e008a5b953dcbd2bce7
=== VERIFIER ===   all invariants pass -- clean.      VERIFY_EXIT=0
=== MASKING  ===   no matches -- clean.               MASK_EXIT=0
GATE_RESULT=PASS
```

### 3.6 ATOMIC live-manifest swap (§2.5)

```
pre-swap: NO live manifest.json exists (first deploy)
       => no manifest.prev.json to preserve; see §5 for the rollback target
ATOMIC:  mv -f manifest.json.candidate  manifest.json     (same dir, same fs → atomic)
```

The deploy is the **atomic manifest swap**, not the DB rename — the loader resolves only
the exact `asset_basename` named in the live manifest, so before this `mv` production was
still unpointed-to.

### 3.7 Restart + flag-bypass readiness smoke (§2.6)

Restarted `genizah-web`. **No `"Discovery sidecar not loaded (fail-closed)"` line** in the
journal. Loader state after the swap:

```
ready = True     available() = False   ← flag OFF, so no surface. Correct.
```

`bench_discovery.py --sample 50 --warm-passes 1` on the box, **exit 0** (the script's
header self-labels "dev-box"; it ran on the prod box):

| Query | n | rows | p50 | p95 | max |
|---|---|---|---|---|---|
| `get_claims_for_page` | 50 | 51 | 0.29 ms | 0.49 ms | 3.12 ms |
| `get_pages_related_to_page` | 50 | 76 | 0.34 ms | 0.47 ms | 0.90 ms |
| `get_work_witnesses` | 50 | 2500 | 34.10 ms | 200.77 ms | 357.17 ms |

Nonzero rows on every call (never an empty no-op). Browse LRU disabled for the latency
pass, so these are real cache-miss worst-case queries.

### 3.8 Prod-box RSS (§2.7 — closes the PERF-01 item deferred from 134-08 Task 3)

```
RSS before load : 22.1 MB
RSS after burst : 33.3 MB
added RSS       : 11.2 MB      (cap ≤ 250 MB)   ✓
warm-burst rows : 2627
```

Recorded in `docs/specs/discovery-budgets.md` §4 as MEASURED ACTUALS (prod-box).

---

## 4. Final live state

| Check | Result |
|---|---|
| `genizah-web` | active |
| `/` · `/help` · `/atlas` · `/search` · `/browse` | 200 · 200 · 200 · 200 · 200 |
| Loader `_state.ready` | `True` (v2 sidecar loaded) |
| `discovery_available()` | `False` — `DISCOVERY_ENABLED` set in neither `.env` nor any systemd drop-in → default OFF |
| User-visible change | **None** |
| Box `discovery_data/` | exactly two files: the v2 `.db` + `manifest.json` |
| Box HEAD | `759d7f76` (later advanced to `c730cb35`, a test-only CI fix) |

---

## 5. Rollback drill (§3) — exercised in both directions

Because this is the **first** deploy, no prior asset exists, so §2.5's `cp` had nothing to
preserve and there is **no `manifest.prev.json`**. Per §3 ("if the prior manifest was not
preserved, reconstruct… the mechanism is identical"), the prior frame here *is* the
fail-closed no-manifest state, and that is what was drilled — the same atomic-repoint
mechanism a future rollback to a real `manifest.prev.json` will use.

| Step | Action | Result |
|---|---|---|
| 1 | atomic repoint AWAY (`mv manifest.json → manifest.rollback-drill.json`) + restart | `ready=False`, `available()=False`, service **active**, homepage **200** |
| 2 | atomic repoint FORWARD (`mv` back) + restart | `ready=True`, `available()=False`, service **active**, homepage **200** |

The app stayed fully up on the prior frame and came back on v2. The swap is reversible.
**From the next deploy onward, §2.5 will produce a real `manifest.prev.json`** pointing at
`discovery-v1-33499c5b…`, and rollback becomes the single documented
`mv -f manifest.prev.json manifest.json` + restart.

---

## 6. Acceptance criteria

| Criterion | Status |
|---|---|
| Log records BOTH labeled hashes matching 135-07 (DB `content_hash` + `frame_content_hash`) | ✅ §1 |
| On-box staged masking scan exit 0 | ✅ §3.5 attempt 2 |
| Candidate manifest → ATOMIC swap, both `manifest.json.candidate` and `manifest.prev.json` accounted for | ✅ §3.3, §3.6, §5 |
| Staged gates resolved from the candidate manifest, exact basename, no glob | ✅ §3.5 |
| Rollback drill recorded | ✅ §5 |
| Deploy-ONCE (no prior v1 deploy) | ✅ §3.3 — `discovery_data/` did not exist on the box |
| Flag OFF / no user surface change | ✅ §4 |
| Fail-closed loader confirmed hiding when asset unpointed-to | ✅ §3.4, §5 step 1 |

---

## 7. Follow-ups (not blocking)

1. **`backup.sh` on the web box holds a cleartext Postgres password**
   (`PGPASSWORD=genizah_secure_pwd_2024`), targeting the `genizah_db` Postgres retired by
   the Jan-2026 backend removal. Untracked, pre-existing, unrelated to this deploy, and
   untouched. Likely dead — worth deleting or rotating.
2. **`pgp_data/pgp_backup_2026-03-11.db`** (172 MB, four months old) is now correctly
   ignored but still on the box; the owner declined deletion. Reclaimable disk if wanted.
3. **CI was red before this deploy** on two phase-135 tests that unconditionally asserted
   the presence of gitignored `discovery_data/` artifacts. Fixed in `c730cb35` by applying
   the presence-gate pattern already used by their sibling in the same file.
4. `DISCOVERY_ENABLED` remains unset. Flipping it is a later, separate owner decision
   (Phase 139 REL-01), not part of this deploy.

---

*Phase 135, plan 08 — v2 production deploy (D-04, deployed ONCE, asset-first, atomic).*
