# Phase 95: My Library — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in `95-CONTEXT.md` — this log preserves the alternatives considered.

**Date:** 2026-05-21
**Phase:** 95-my-library
**Areas discussed:** PDF extractor pin, Result-merger + LAB, Side-index location, QThread + cancellation, plus follow-ups on LOCAL hit interaction, Browse tab, web exposure, attribution, security, Help docs

---

## PDF extractor pin

### Q1: Which PDF extractor strategy?

| Option | Description | Selected |
|--------|-------------|----------|
| PyMuPDF only | Single extractor, ~+25 MB installer, native RTL handling; Seewald's RTL helpers become dead code | ✓ |
| PyMuPDF + pdfplumber/pypdf fallbacks | Full Seewald stack ported verbatim; RTL helpers active on fallback paths | |
| PyMuPDF + pypdf fallback only | Drop pdfplumber, keep pypdf as last-resort | |

**User's choice:** PyMuPDF only.

### Q2: REQ-4 RTL helpers — port or drop?

| Option | Description | Selected |
|--------|-------------|----------|
| Port + test as dead-code safety net | Helpers ported, acceptance tests ship, never invoked at runtime | ✓ |
| Drop helpers entirely | Mark REQ-4 superseded; re-derive from Seewald source if needed later | |
| Port behind feature flag | Helpers ported, gated behind --use-fallback-pdf-extractor flag | |

**User's choice:** Port + test as dead-code safety net.

### Q3: PyMuPDF extraction mode

| Option | Description | Selected |
|--------|-------------|----------|
| get_text('blocks') | Positioned blocks ordered by reading position; best RTL behavior | ✓ |
| get_text() (plain) | Default plain-text; occasionally re-orders RTL on multi-column | |
| get_text('dict') | Full structured output; overkill for v1 | |

**User's choice:** get_text('blocks').

### Q4: PDF page-break model

| Option | Description | Selected |
|--------|-------------|----------|
| One Tantivy doc per PDF page | Mirrors main index per-page model | ✓ |
| One Tantivy doc per whole file | Simpler ingestion, loses page-context | |
| One Tantivy doc per paragraph | Finer-grained, explodes doc count | |

**User's choice:** One Tantivy doc per PDF page.

### Q5: DOCX page-break model

| Option | Description | Selected |
|--------|-------------|----------|
| `contains_page_break` heuristic (Seewald) | Walk paragraphs, split on explicit page breaks | |
| One Tantivy doc per DOCX file | Whole file becomes one page | |
| Split every N paragraphs | Fixed paragraph window | ✓ |

**User's choice:** Split every N paragraphs.

### Q6: DOCX paragraph window size

| Option | Description | Selected |
|--------|-------------|----------|
| 30 paragraphs | Roughly matches a typical Word page | |
| 50 paragraphs | Larger units, fewer docs per file | |
| 20 paragraphs | Smaller units, finer-grained snippets | ✓ |

**User's choice:** 20 paragraphs.

### Q7: Scanned PDF handling

| Option | Description | Selected |
|--------|-------------|----------|
| Status: 'no_text_layer' if <50 chars | Clear actionable error; no rows in side-index | ✓ |
| Status: 'OK' with 0 indexed pages | Silent for the user | |
| Index the sparse text anyway | Useless rows in side-index | |

**User's choice:** Status: 'no_text_layer' if <50 chars.

### Q8: Empty-page detection

| Option | Description | Selected |
|--------|-------------|----------|
| Skip empty pages silently | <10 chars → no Tantivy doc, p_num non-contiguous | ✓ |
| Index every page including empty | Preserves p_num continuity, wastes index space | |

**User's choice:** Skip empty pages silently.

### Q9: TXT encoding fallback policy

| Option | Description | Selected |
|--------|-------------|----------|
| utf-8-sig only; on error → status='encoding_error' | Strict UTF-8 (BOM-tolerant) | |
| utf-8-sig → cp1255 fallback | Try legacy Windows Hebrew if utf-8-sig fails | |
| chardet auto-detection | Auto-detect via library; slow, occasionally wrong | |

**User's choice:** "We'll run some local tests before deciding" — deferred to planner (D-07).

---

## Result-merger + LAB-index integration

### Q10: Main search result-merger algorithm

| Option | Description | Selected |
|--------|-------------|----------|
| Query both → concat → sort by BM25 score | Schemas match, scores comparable, simplest correct | ✓ |
| Query both → interleave round-robin | Guarantees LOCAL visibility, may surface weak hits | |
| Query main first → append LOCAL | LOCAL buried at the end | |
| Query both → score-normalize → merge | Z-score normalize then merge | |

**User's choice:** Query both → concat → sort by BM25 score.

### Q11: LAB-index integration for Composition/Parallels

| Option | Description | Selected |
|--------|-------------|----------|
| Build parallel LOCAL lab side-index | LOCAL indexing run produces BOTH main + lab side-indexes | ✓ |
| Scope REQ-6 to plain search only | LOCAL filter excluded from Composition/Parallels in v1 | |
| Composition queries main side-index directly | Schema mismatch; different scoring semantics | |

**User's choice:** Build parallel LOCAL lab side-index.

### Q12: LOCAL lab side-index build trigger

| Option | Description | Selected |
|--------|-------------|----------|
| Built alongside main side-index during scan | Single indexing run produces both | ✓ |
| Built lazily on first Composition/Parallels query | Adds first-use latency | |
| Triggered by existing 'Rebuild LAB Index' button | Cleaner UX coupling, lag until rebuild | |

**User's choice:** Built alongside main side-index during scan.

### Q13: BM25 tie-break order

| Option | Description | Selected |
|--------|-------------|----------|
| Genizah first on tie | Canonical corpus wins ties | ✓ |
| LOCAL first on tie | Personal corpus wins ties | |
| No deterministic tie-break | Sort algorithm picks | |

**User's choice:** Genizah first on tie.

### Q14: Filter button labels

| Option | Description | Selected |
|--------|-------------|----------|
| Filter Local / Only Local / No Local | Mirrors Phase 93 PGP-filter wording | ✓ |
| Filter My Library / Only My Library / No My Library | Tab name; reads awkwardly | |
| Filter Personal / Only Personal / No Personal | Neutral; disconnected from tab name | |

**User's choice:** Filter Local / Only Local / No Local (HE: סנן מקומי / רק מקומי / ללא מקומי).

### Q15: Badge column strategy

| Option | Description | Selected |
|--------|-------------|----------|
| New `COL_LOCAL` column | Separate badge next to COL_PGP | |
| Unified badge column | LOCAL / PGP / both in one column | |
| User pointed out: "We have 'Src' column" | Reuse existing COL_SRC | ✓ |

**User's choice:** Reuse existing `COL_SRC` (which already shows V0.7/V0.8 transcription source) — write `source='LOCAL'` on LOCAL hits.

### Q16: COL_SRC reuse detail — color-code or plain?

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — LOCAL writes source='LOCAL', column unhides when LOCAL hits exist (plain) | No color, just text | |
| Yes + color-code the LOCAL Src cell blue (`#3498db`) | Symmetric with green PGP cell | ✓ |
| Show in both Src AND library_code='My Library' (redundant) | Two indicators | |

**User's choice:** Color-code the LOCAL Src cell blue.

### Q17: Composition/Parallels result tables — add Source column?

| Option | Description | Selected |
|--------|-------------|----------|
| Audit during plan, reuse if Src exists | Planner inspects existing layouts | |
| Add new compact 'Source' column uniformly | Standardize on adding it everywhere | ✓ |
| Library-name cell only, no badge column | Asymmetric vs main search | |

**User's choice:** Add a new compact 'Source' column to Composition/Parallels uniformly.

### Q18: Hebrew display name for LIBRARY_CODES['LOCAL']

| Option | Description | Selected |
|--------|-------------|----------|
| הספרייה שלי ("My Library") | Direct translation | ✓ |
| המסמכים שלי ("My Documents") | More literal to indexed content | |
| קבצים מקומיים ("Local Files") | Technical-flavored | |

**User's choice:** הספרייה שלי.

---

## Side-index location + storage

### Q19: Side-index path resolution

| Option | Description | Selected |
|--------|-------------|----------|
| Co-locate with main `Config.INDEX_DIR` | Inherits portable-mode rule automatically | ✓ |
| Separate `%LOCALAPPDATA%` path, ignoring portable mode | Portable users lose LOCAL on move | |
| User-configurable via MyLibraryTab setting | More flexibility, more failure modes | |

**User's choice:** Co-locate with main INDEX_DIR.

### Q20: Folder-path persistence

| Option | Description | Selected |
|--------|-------------|----------|
| `QSettings` (existing desktop pattern) | Stores under HKCU\Software\GenizahSearchPro | ✓ |
| JSON file next to the side-index | Self-contained with the index folder | |
| Same SQLite local_index.sqlite3 with config table | One file holds everything | |

**User's choice:** QSettings.

### Q21: Multiple folder support

| Option | Description | Selected |
|--------|-------------|----------|
| Single folder for v1 (SPEC default) | One folder, simpler UI | |
| Multiple folders for v1 | List management UI in MyLibraryTab | ✓ |
| Single now, multi as backlog | Defer multi-folder | |

**User's choice:** Multiple folders for v1 (deliberate expansion of SPEC default).

### Q22: machine_id derivation

| Option | Description | Selected |
|--------|-------------|----------|
| Keep SPEC default (hostname-derived) | Simple; rename invalidates cache | ✓ |
| File-pinned (generated once, persisted next to side-index) | Survives hostname renames | |
| Windows Machine GUID from registry | Stable across renames, resets on reinstall | |

**User's choice:** Keep SPEC default (hostname-derived).

### Q23: Multi-folder UX widget

| Option | Description | Selected |
|--------|-------------|----------|
| QListWidget + Add/Remove buttons | Familiar pattern, simplest | ✓ |
| QListWidget + per-row Enable checkbox | Adds disabled state | |
| Tree view (folder → file count + status) | Richer info, more UI | |

**User's choice:** QListWidget + Add/Remove buttons.

### Q24: Folder uniqueness rule

| Option | Description | Selected |
|--------|-------------|----------|
| Reject duplicates AND overlapping ancestors/descendants | Prevents duplicate indexing | ✓ |
| Reject only exact duplicates | Overlapping folders allowed | |
| Allow anything; dedupe by sys_id at index time | Most permissive | |

**User's choice:** Reject duplicates AND overlapping ancestors/descendants.

### Q25: sys_id content_hash input

| Option | Description | Selected |
|--------|-------------|----------|
| Full absolute filepath (SPEC literal) | Same file in two folders = two sys_ids | ✓ |
| File-content SHA256 (content-addressed) | Cross-folder dedup; expensive on every scan | |
| Relative path under folder root | More complex, ambiguity on move | |

**User's choice:** Full absolute filepath (SPEC literal).

### Q26: Folder removal behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Synchronous delete: remove all rows immediately | Predictable, no orphan state | ✓ |
| Mark folder disabled, sweep on next Refresh | Lower latency, stale rows until sweep | |
| Background QThread delete | UI stays responsive, async cancellation surface | |

**User's choice:** Synchronous delete.

---

## QThread + cancellation

### Q27: Tantivy commit policy

| Option | Description | Selected |
|--------|-------------|----------|
| Per-file commit (atomic per file) | Predictable, slower per-file commit cost | |
| Batch commit every N files | Faster IO, larger rollback window | ✓ |
| All-or-nothing (single commit at end) | Worst cancellation cost | |

**User's choice:** Batch commit every N files.

### Q28: Batch size

| Option | Description | Selected |
|--------|-------------|----------|
| 25 files per commit | ≤ 0.5% rework at SPEC ceiling | ✓ |
| 50 files per commit | Bigger batches, ≤ 1% rework | |
| 10 files per commit | Smaller batches, more commit overhead | |
| Auto-tune by file size | Adapts to size variance, more code | |

**User's choice:** 25 files per commit.

### Q29: Qt signal cadence

| Option | Description | Selected |
|--------|-------------|----------|
| Per-file Qt signal | progress + status row per file | ✓ |
| Every N files (batched signals) | Less Qt traffic | |
| Per-page signal | Overkill at 5K files | |

**User's choice:** Per-file Qt signal.

### Q30: Cancellation mechanism

| Option | Description | Selected |
|--------|-------------|----------|
| Cancel button + cooperative flag between files | In-flight file finishes, then exit | ✓ |
| Cancel button + abort mid-file | Faster cancel, partial pages rolled back | |
| No cancel button | Must wait for scan to complete | |

**User's choice:** Cancel button → cooperative flag check between files.

### Q31: App-start auto-rescan UX

| Option | Description | Selected |
|--------|-------------|----------|
| Silent background + non-modal toast on completion | Zero-friction; status bar indicator + toast | ✓ |
| Status bar progress only, no toast | Subtle, less noticeable | |
| Progress in MyLibraryTab only, badge if updates | User discovers when opening tab | |

**User's choice:** Silent background + non-modal toast.

### Q32: Status row truthiness during batch commit

| Option | Description | Selected |
|--------|-------------|----------|
| Show 'OK' immediately on extraction success (pre-commit) | Simpler signaling, minor mismatch on cancel | |
| Show 'Indexing…' until batch commits, then → 'OK' | Two-stage status, truthful | ✓ |
| Switch to per-file commit | Eliminates mismatch | |

**User's choice:** Show 'Indexing…' until batch commits, then → 'OK'.

### Q33: Above-ceiling warning timing

| Option | Description | Selected |
|--------|-------------|----------|
| Pre-scan: count files first, dialog if >5000 | Predictable UX, two-pass for huge folders | ✓ |
| Mid-scan: warning after 5001st file | Faster on small folders, surprising mid-scan | |
| No dialog: status-bar warning only | Soft warning; SPEC REQ-10 wants modal | |

**User's choice:** Pre-scan count.

---

## Follow-up areas (LOCAL hit interaction, Browse, Web exposure, Help, Attribution, Security)

### Q34: Click-behavior on a LOCAL hit row

| Option | Description | Selected |
|--------|-------------|----------|
| Open Browse panel with text-only view (no image) | Reuses existing browse UI | ✓ |
| Open source file in OS default app | No in-app text viewer | |
| Snippet-only inline expansion in result row | Minimal UI; loses page-navigation | |

**User's choice:** Browse panel text-only view.

### Q35: Browse tab integration

| Option | Description | Selected |
|--------|-------------|----------|
| Search-only for v1, no Browse-tab entry | Browse stays Genizah-only | ✓ |
| Add 'My Library' filter to Browse tab | LOCAL discoverable in Browse | |
| LOCAL surfaces in Browse via library filter | Almost-free if filter dropdown reads LIBRARY_CODES | |

**User's choice:** Search-only for v1, no Browse-tab entry.

### Q36: Web LIBRARY_CODES exposure

| Option | Description | Selected |
|--------|-------------|----------|
| Filter LOCAL out of web library lists at the web layer | Web never sees 'My Library' | ✓ |
| Show 'My Library' with `(desktop only)` annotation | Curiosity-driven discovery, possible confusion | |
| Define LOCAL in desktop-only constant | Breaks no-special-casing constraint | |

**User's choice:** Filter LOCAL out at the web layer.

### Q37: Seewald attribution

| Option | Description | Selected |
|--------|-------------|----------|
| About + Help page line in both apps, both languages | Visible credit | ✓ |
| Code-level comment header in shared/local_indexer.py | No user-facing credit | |
| Defer to backlog | Not in this phase | |

**User's choice:** About + Help page line.

### Q38: Open-in-OS button on LOCAL browse view

| Option | Description | Selected |
|--------|-------------|----------|
| Single 'Open file' button → os.startfile | Standard Windows | ✓ |
| Two buttons: 'Open file' + 'Open containing folder' | Reveal in Explorer addition | |
| Just 'Open file' link in result row, no panel | Plain text link | |

**User's choice:** Single 'Open file' button → os.startfile.

### Q39: Help page coverage

| Option | Description | Selected |
|--------|-------------|----------|
| New 'My Library' section on existing Help page (both apps) | Bilingual section on Help | ✓ |
| Inline tooltips only | Less discoverable | |
| Separate docs/guides/MY_LIBRARY.md | Standalone admin doc | |

**User's choice:** New section on existing Help page.

### Q40: Side-index/file-content security

| Option | Description | Selected |
|--------|-------------|----------|
| No encryption; one-line Help note about cleartext storage | Honest disclosure | |
| Encrypt content field with per-machine key | Complex; breaks Tantivy tokenization | |
| No mention; trust OS-level disk encryption silently | No Help note | ✓ |

**User's choice:** No mention; trust OS-level disk encryption.

---

## Claude's Discretion

- **D-07**: Final TXT encoding fallback policy (utf-8-sig only vs utf-8-sig + cp1255) — planner picks after local smoke tests, records in `95-NN-PLAN.md`.
- **D-12**: Exact column position for the new Composition/Parallels Source column — planner inspects layouts and picks.
- **D-32**: Final Hebrew translation of the Seewald attribution line — user-reviewed during execute (or planner picks).
- Per-file status panel column widths, button colors, exact toast styling — planner discretion.

## Deferred Ideas

- "My Library" Browse-tab filter (future phase)
- Cloud-synced Lists for LOCAL items (privacy-design caveat)
- OCR for image-only PDFs (Tesseract)
- Additional file types (`.epub`, `.md`, `.html`, `.rtf`, `.doc`)
- "Import from Seewald prototype" migration UI
- Multi-machine sync of the local index
- Live folder watch via `QFileSystemWatcher`
- PDF fallback extractors (pdfplumber/pypdf) — helpers ported as dead code, future-phase activation
- Content-addressed sys_id (file-content SHA256 dedup)
- Encrypted side-index
- "Reveal in Explorer" button on LOCAL browse panel

---

*Phase: 95-my-library*
*Discussion logged: 2026-05-21*
