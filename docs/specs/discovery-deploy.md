# Discovery Sidecar Deploy / Rollback / Rebuild Runbook (DATA-08)

**Status:** ACTIVE. Version 1, created 2026-07-23 (Phase 134, plan 134-08).

This is the operational runbook for shipping, rolling back, and reproducibly
rebuilding the Discovery Data Spine sidecar (`discovery.db`). It is the
concrete procedure the 134-08 Task 3 human/live-server checkpoint follows and
the reference for every future refresh (FUT-04).

**Masking:** this document names NO restricted source. The Discovery corpus's
restricted literary subset is referred to only by its codename **M-source**.
Every artifact that crosses to the box passes the strict blocking masking gate
BEFORE the atomic swap (§2.4); this doc is committed and MUST stay
masking-clean.

---

## 0. Deploy shape at a glance

```
dev box:   build --release --frozen-precision-defaults
             -> verify_discovery_sidecar.py <DB> --expected-frame-hash <hex>
             -> check_atlas_masking.py --scan-sqlite <DB> --scan-asset <DB> --scan-repo --strict
web box:   scp <DB> asset-first (immutable content-hashed name)
             -> STAGE candidate manifest.json.candidate (points at the new asset_basename)
             -> verify + strict masking gate the STAGED target (exact filename resolved
                from the candidate manifest, NEVER the live/old one)
             -> ATOMIC live-manifest swap (mv manifest.json.candidate manifest.json)
             -> restart -> live smoke (flag-bypass readiness probe; DISCOVERY_ENABLED stays OFF)
rollback:  ATOMIC repoint live manifest to the PRIOR asset_basename -> restart
```

Trust boundaries: (1) **dev-box built DB → prod web box** — the staged verify +
strict masking gate must pass on the STAGED filename before the swap; (2)
**live manifest pointer → loader** — the atomic manifest swap + exact-basename
resolution IS the deploy/rollback safety mechanism.

---

## 1. On-box layout (what the loader reads)

The sidecar lives in repo-root **`discovery_data/`** on the web box — OUTSIDE
`web/static/`, so it can never be served as raw bytes (mirrors
`atlas_data/`). The whole directory is gitignored (`/discovery_data/`), so both
the `.db` and `manifest.json` are managed OUT OF BAND via `scp` — never via
`git` / `deploy.sh`. Files:

| File | Role | Mutability |
|---|---|---|
| `discovery_data/manifest.json` | The **live pointer**: `asset_basename`, `content_hash`, `frame_content_hash`, `schema_version`. | **MUTABLE** — atomically swapped on deploy/rollback. |
| `discovery_data/<asset_basename>.db` | The content-hashed sidecar payload. | **IMMUTABLE** — a new build is a new filename; old siblings are left in place. |

`web/discovery_assets.py::_resolve_versioned_db()` resolves ONLY the EXACT
`<asset_basename>.db` named in the manifest. **A sibling `*.db` that is not that
exact name is deliberately IGNORED** — this is what makes rollback safe: leaving
the prior asset on disk never causes it to be picked up. The loader runs ONCE at
startup (`load_discovery_state()` from `web/main.py`); there is no per-request
`os.path.exists`, so the process MUST be restarted after any manifest swap.

Current live frame (example, for reference — replace with the frame you are
shipping):

- `asset_basename` = `discovery-v1-8e43451300429ed4ace5e29e5513359a29674ac49731d5c969eb1d607e0ca065`
- `content_hash`   = `8e43451300429ed4ace5e29e5513359a29674ac49731d5c969eb1d607e0ca065`
- `frame_content_hash` = `17bf5601bc1ef89404ee5ccdeb1ce9616f3e3274432c4297f79b5c8a99ba6efd`
- `schema_version` = `discovery-v1`

> **Exact-filename discipline (N6):** every command below names the exact
> `.db` / manifest filename. NEVER use a `*` glob — on the dev box the shell is
> PowerShell, where a stray glob resolves unpredictably, and a glob could match
> a stale sibling and swap in the wrong asset.

---

## 2. DEPLOY (asset-first)

### 2.1 Build + verify + gate on the dev box

Build the sidecar (see §4 for the full reproducible invocation), then, on the
dev box, verify it against its own frame hash and run the strict blocking
masking gate over the exact built filename:

```powershell
# dev box (Windows / PowerShell). $DB = the exact built filename.
$env:PYTHONUTF8 = '1'
$DB = 'discovery_data\discovery-v1-<content_hash>.db'
$FRAME = '<frame_content_hash from manifest.json>'
$env:MASKING_SCAN_PATTERNS_FILE = '.masking_patterns'

python scripts/verify_discovery_sidecar.py $DB --expected-frame-hash $FRAME
python scripts/check_atlas_masking.py --scan-sqlite $DB --scan-asset $DB --scan-repo --strict
```

Both MUST exit 0. If either fails, STOP — do not upload. (These are the same
gates 134-07 already passed for the current frame; re-run them on any rebuild.)

### 2.2 Upload the asset first (temp name → final name)

`scp` the `.db` to the box's `discovery_data/` while the live `manifest.json`
still points at the PRIOR asset (so nothing reads the half-uploaded file). Upload
to a temp name, then rename to the final immutable content-hashed name once the
transfer is complete:

```bash
# web box target dir = <APP_ROOT>/discovery_data/  (repo-root on the box)
scp discovery-v1-<content_hash>.db  webbox:<APP_ROOT>/discovery_data/discovery-v1-<content_hash>.db.uploading
ssh webbox 'mv -f <APP_ROOT>/discovery_data/discovery-v1-<content_hash>.db.uploading \
                  <APP_ROOT>/discovery_data/discovery-v1-<content_hash>.db'
```

The live app is still serving the old frame — no behavior change yet.

### 2.3 STAGE a candidate manifest

Write the new manifest as `manifest.json.candidate` (NOT the live `manifest.json`
yet). It points at the new `asset_basename` and carries the new `content_hash` +
`frame_content_hash`. Copy the `manifest.json` produced by the build:

```bash
scp manifest.json  webbox:<APP_ROOT>/discovery_data/manifest.json.candidate
```

### 2.4 Verify + strict masking gate the STAGED target (pre-swap)

Resolve the DB filename FROM THE STAGED CANDIDATE manifest (never the live/old
one) and run BOTH gates over that exact staged filename ON THE BOX:

```bash
ssh webbox
cd <APP_ROOT>
export PYTHONUTF8=1
export MASKING_SCAN_PATTERNS_FILE=.masking_patterns
# resolve the staged asset_basename from the CANDIDATE manifest, not the live one:
STAGED_DB="discovery_data/$(python -c "import json;print(json.load(open('discovery_data/manifest.json.candidate'))['asset_basename'])").db"
STAGED_FRAME="$(python -c "import json;print(json.load(open('discovery_data/manifest.json.candidate'))['frame_content_hash'])")"

python scripts/verify_discovery_sidecar.py "$STAGED_DB" --expected-frame-hash "$STAGED_FRAME"
python scripts/check_atlas_masking.py --scan-sqlite "$STAGED_DB" --scan-asset "$STAGED_DB" --scan-repo --strict
```

Both MUST exit 0 on the box before the swap. This re-checks the asset AT ITS
DESTINATION (defends against a corrupt transfer) and re-runs the masking gate
against the exact bytes that will go live.

### 2.5 ATOMIC live-manifest swap

Preserve the current live manifest as `manifest.prev.json` (the rollback
target), then atomically move the candidate into place. `mv` within one
directory on one filesystem is atomic — a concurrent reader sees either the old
or the new manifest, never a torn file:

```bash
cp -f  discovery_data/manifest.json       discovery_data/manifest.prev.json   # rollback target
mv -f  discovery_data/manifest.json.candidate  discovery_data/manifest.json    # ATOMIC swap
```

### 2.6 Restart + live smoke (flag stays OFF)

Restart the web process so the startup `load_discovery_state()` picks up the new
manifest. Then run a flag-bypassing readiness smoke — `DISCOVERY_ENABLED` STAYS
OFF this phase, so no UI/surface is exposed; the smoke only proves the service
reads real rows:

```bash
# restart per the standard web-app restart (see reference_server_operations)
export PYTHONUTF8=1
python scripts/bench_discovery.py --sample 50 --warm-passes 1
```

`bench_discovery.py` uses the benchmark-only readiness predicate
(`_state.ready`, NOT the UI flag) and asserts nonzero rows — a clean run proves
the swapped-in sidecar loaded and queries return real data on the box. Confirm
the app log shows NO "Discovery sidecar not loaded (fail-closed)" line. The
public site shows nothing new (flag OFF) — correct for Phase 134.

### 2.7 Record the prod-box RSS (Task 3)

While the process is warm, sample its RSS (or run `bench_discovery.py` on the box
and read its `added RSS`) and record the number in
`docs/specs/discovery-budgets.md` §4 as **MEASURED ACTUALS (prod-box)** vs the
≤ 250 MB cap.

---

## 3. ROLLBACK

Because old asset siblings are left on disk and the loader ignores any file that
is not the manifest's exact `asset_basename`, rollback is a single atomic
manifest repoint — no file deletion, no re-upload:

```bash
mv -f  discovery_data/manifest.prev.json  discovery_data/manifest.json   # ATOMIC repoint to prior asset
# restart the web process
python scripts/bench_discovery.py --sample 50 --warm-passes 1            # confirm the app is fully up on the prior frame
```

The app comes back on the prior frame; the new asset sibling stays on disk,
inert (ignored), ready to re-point forward once the issue is resolved. If the
prior manifest was not preserved, reconstruct a candidate manifest pointing at
the prior `asset_basename` and swap that in (§2.3–2.5) — the mechanism is
identical.

**Rollback drill (Task 3):** deploy the new frame (§2), then immediately
rollback (§3) and confirm the app stays fully up on the prior frame, then
re-point forward. This proves the swap is reversible before it matters.

---

## 4. REBUILD (reproducible)

The build is fully reproducible from four pinned inputs + a precision source.
Re-using the durable `crosswalk.json` keeps every opaque `work_id` stable across
re-distillations (a deferred work added later never disturbs a shipped id). The
source-DB + crosswalk SHA-256 hashes are recorded in the built `meta` (and in
`docs/specs/discovery-frames.md`), so any rebuild is auditable against the frame.

```powershell
# dev box (Windows / PowerShell). Exact filenames only; no glob.
$env:PYTHONUTF8 = '1'
$env:MASKING_SCAN_PATTERNS_FILE = '.masking_patterns'

python scripts/build_discovery_sidecar.py `
    same_work_spike/probe/data/fullcorpus_v2.db `
    --from-approved   discovery_data/discovery-review-approved-final.csv `
    --crosswalk       discovery_data/crosswalk.json `
    --research-data-dir same_work_spike/probe/data `
    --libraries-csv   libraries.csv `
    --fjms-db         fist_data/fjms_enrichment.db `
    --out             discovery_data/discovery-v1-<new_content_hash>.db `
    --release `
    --frozen-precision-defaults
```

Notes:

- `--release` REQUIRES a precision source: either `--frozen-precision-defaults`
  (bakes the pre-registered contract precision — what the current frame uses) OR
  `--precision-spec <json>` (an explicit spec, validated against the frozen
  release band_precision). The two are one-or-the-other.
- The build writes `discovery_data/manifest.json` alongside the `.db`; its
  `asset_basename` == the `.db` stem, its `content_hash` == the DB bytes' SHA-256,
  and its `frame_content_hash` == the membership-based frame hash. The final
  filename is chosen by the build's own content hash — set `--out` to the exact
  intended name (or let the build name it and read the manifest back).
- `--include-masked-metadata` (default OFF) only ever ungates raw author/genre
  for M-source works; it NEVER ungates the M-source title. Do not pass it for a
  public release build.
- After building, run the §2.1 verify + strict masking gate before uploading.

A full rebuild + re-deploy of the SAME membership produces the SAME
`frame_content_hash` (volatile `meta` excluded from the hash), so a rebuild that
changes the frame hash means the membership changed — treat that as a NEW
versioned frame (`discovery-frames-v2.md`), not an in-place swap.

---

## 5. Cross-references

- `docs/specs/discovery-budgets.md` — PERF-01 caps + measured actuals (the
  prod-box RSS from §2.7 lands there).
- `docs/specs/discovery-frames.md` — the FROZEN frame identity/provenance +
  release-contract row counts (what the verifier checks).
- `docs/specs/discovery-sidecar-schema-v1.md` — the frozen schema + frame-hash
  recipe the verifier enforces.
- `web/discovery_assets.py` — the exact-basename manifest resolver +
  siblings-ignored fail-closed loader (the rollback mechanism).
- `reference_server_operations` (memory) — EC2 access, `deploy.sh master-main`,
  the web-app restart, screen sessions.

---

*Phase: 134-Discovery Data Spine (plan 134-08).*
*Runbook for DATA-08 (asset-first deploy + rollback + reproducible rebuild).*
