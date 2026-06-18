---
id: SEED-005
status: dormant
planted: 2026-06-18
planted_during: FGP-chooser groundwork — user floated a thin-installer / pick-your-data idea (Codex-reviewed)
trigger_when: installer size / download bloat becomes a complaint, OR a "desktop footprint / data management" milestone, OR after the FGP-chooser milestone (which ships FGP as a download and resolves its bundle-vs-download decision)
scope: Large (own multi-phase milestone). NOTE: the existing infra is download TRANSPORT only — the real build is a full artifact-lifecycle Data Manager. Realistic completeness ≈ 40–50%, not 70%.
related: SEED-004 (FGP chooser), docs/plans/FGP_CHOOSER_MILESTONE.md, gui_threads.py SidecarUpdateThread/SidecarDownloadThread, CompileScriptGenizah.iss, genizah_core.py Indexer, memory reference_desktop_data_provisioning
---

# SEED-005: Thin desktop installer + on-demand "Data Manager" (user picks which data to download)

## Why This Matters

The desktop installer bundles **~3.4 GB of data** (→ ~300 MB compressed `.exe`), most of which
many users never touch:

| DB | Size | Bundled today |
|----|------|---------------|
| fjms_enrichment.db | **1.5 GB** | yes |
| visual_similarity.db | **1.3 GB** | yes |
| nli_crossref.db | 261 MB | yes |
| pgp.db | 149 MB | yes |
| libraries_translations.db | 74 MB | yes |
| fgp_transcriptions.db | 387 MB | not yet (SEED-004; ship as DOWNLOAD) |
| libraries.csv / oxford_full_db.json | 48 MB / 8.5 MB | yes (keep bundled — small + core) |

Plus the **core corpus** `Transcriptions.txt` (**1.4 GB**, Zenodo CC-BY, downloaded manually) from
which the Tantivy index is **built locally** at first run. A thin installer that lets the user pick
what to pull slashes the footprint and lets users skip the 1.3 GB VS / 1.5 GB FJMS DBs they may
not need.

## What ALREADY EXISTS (transport only — ~40–50% of a real Data Manager)

- **`SidecarUpdateThread` + `SidecarDownloadThread`** (`gui_threads.py:855-1002`): startup-fetch a
  manifest `sidecar-versions.json` from the GitHub release tag **`data-latest`**; per-DB
  `{version,size_mb,url}`; streaming download + progress + cancel; atomic `.tmp`→`shutil.move`;
  writes to **`%LOCALAPPDATA%\GenizahSearchPro\data\{subdir}\{name}`** (the LOCALAPPDATA override
  every service checks FIRST). `service_map` wires only pgp/fjms/nli.
- Universal **graceful degradation**: missing DB → `is_available()` False → empty results, no crash.
- Per-DB version via each service's `get_version()` (reads its `meta` table).

⚠ **Codex reality-check:** this is a *download-to-LocalAppData* primitive, NOT a Data Manager. It
does not model the artifact lifecycle (absent / queued / downloading / verifying / installed /
corrupt / incompatible / cancelled / removable / retryable). Treat "70% built" as wrong — the hard
parts (state, integrity, UX, resumability, the core index) are unbuilt.

## What's NEW (the actual milestone work)

1. **Richer manifest + integrity.** `{version,size_mb,url}` is insufficient. Add per-artifact
   **`sha256`, exact byte size, schema/data version, `min_app_version`, dependencies, app-compat
   range**, and verify on download. Consider signing the manifest. Model:
   `latest.json` → immutable versioned objects (`data/2026-06-18/fjms_enrichment.db.zst`).
2. **A real "Data Manager" UI** (replaces today's update-only QMessageBox; the VS button is
   commented out): first-run picker + Settings panel showing, per artifact, size / installed
   version / source (bundled vs downloaded) / status, with download / update / remove / repair.
   Missing OPTIONAL DBs should surface as "Install FGP/PGP/… data" (not silent empty results that
   look like a data-quality bug); a missing CORE index should **disable search with an explicit
   recovery path**, not degrade silently.
3. **Thin bundle.** Inno has NO `[Components]`/`[Types]` today. Codex: `[Components]` is likely the
   *wrong* center of gravity — prefer a **truly-thin installer** (code + small core: libraries.csv,
   oxford_full_db.json) + the in-app Data Manager, plus a separate **offline/full data pack** for
   airgapped installs.
4. **Core index (the heaviest piece) — prefer a PREBUILT index as the DEFAULT.**
   `Transcriptions.txt` (1.4 GB) is Zenodo-manual and the index is built locally
   (`genizah_core.py Indexer.create_index`, hard-fails if the corpus is missing). Making users
   download 1.4 GB *and then* wait for a long local build before search works = support pain.
   - **Default: download a prebuilt Tantivy index.** But it's an OPAQUE artifact coupled to
     tantivy/tantivy-py version + schema + tokenizer/analyzer + corpus hash + build code — version
     it with `corpus_sha256` / `index_schema_hash` / `analyzer_hash` / `tantivy_version` /
     app-compat / build timestamp, and ship CC-BY attribution WITH the derived index.
   - **Keep corpus-download + local-build as fallback/repair** (background, cancellable — Phase
     97.3 patterns; never UI-thread, see [[feedback_no_auto_reindex_in_init]]).
   - **Measure before deciding**: raw vs compressed × corpus vs index — do NOT assume the index is
     smaller than the 1.4 GB source (Tantivy stores docs + positions per schema; text may compress
     better than the binary index).
5. **Windows file-handle reality:** services hold open read-only SQLite connections; runtime
   replace/remove of a DB likely needs **"apply on restart"** semantics + service reset
   (`reset_*_service()`), or it'll hit Windows sharing-violation.

## Hosting (Codex web-verified)

- **GitHub Releases:** fine for FGP + beta. Limits: ≤1000 assets/release, **<2 GiB per file**
  (fjms 1.5 GB & VS 1.3 GB fit), **no documented total-size or bandwidth limit**. BUT the manifest
  fetch hits the **unauthenticated REST API = 60 requests/hour per IP** → bites shared
  institutional networks. (Mitigate: cache the manifest, fetch the release asset directly, or move
  off the API.)
- **Production scale → object storage/CDN.** Recommended **Cloudflare R2** (no egress fees) or
  **S3 + CloudFront**. **Avoid EC2/nginx as primary** — it already choked on a 1.3 GB response
  (`proxy_max_temp_file_size`) and is an ops/support burden. Require: HTTP **Range** + `HEAD` +
  stable `Content-Length` + ETag, **retry/resume**, immutable versioned object paths, CDN logs,
  checksum verification.

## HIGH blind spots to design for

- **First-run "minimum useful install"** — define it. If core search is unavailable the app isn't
  meaningfully usable; gate the first-run flow on getting the index, not the optional DBs.
- **Partial/corrupt downloads** — resumable downloads, hash verify, stale-`.tmp` cleanup, repair.
- **Disk-space preflight** — index build/extract can need corpus + archive + temp + final index
  simultaneously.
- **Version skew** — DBs, corpus, index, app code, service schemas need a compatibility matrix.
- **Offline / airgapped** — offer an offline data pack or "import from folder/archive."
- (MEDIUM) corporate proxies/TLS inspection (respect system proxy + actionable errors); AV /
  SmartScreen flagging a small signed stub that pulls multi-GB (code-sign + stable domain +
  transparent metadata); bundled-vs-downloaded precedence shown in the UI; uninstall data-retention
  prompt. Web app unaffected (server has DBs scp'd) — desktop-only.

## Sequencing vs the FGP chooser (SEED-004) — Codex CONFIRMED

Ship **FGP as a download FIRST** inside the FGP-chooser milestone (387 MB, optional, fits the
existing sidecar pattern) — bundling it would make the installer problem worse before solving it.
Then do this full thin-installer milestone later. Two FGP guardrails from Codex:
- Show **minimal user-visible install status** for FGP in the chooser (empty FGP because the DB
  is missing must NOT look like a data-quality failure).
- Build a **small reusable "artifact status/install" widget**, NOT a one-off FGP-only download UX
  the Data Manager will later throw away.
(Thin-installer-first was rejected: it would delay FGP's value behind a much larger milestone.)

## Foundations already in place

- `SidecarUpdateThread`/`SidecarDownloadThread` (download engine + `data-latest` manifest).
- LOCALAPPDATA `data/` override resolution in every `shared/*_service.py`.
- `is_available()` graceful-degradation across all services.
- Zenodo corpus link already in the Inno post-install `[Run]` dialog (unchecked).
