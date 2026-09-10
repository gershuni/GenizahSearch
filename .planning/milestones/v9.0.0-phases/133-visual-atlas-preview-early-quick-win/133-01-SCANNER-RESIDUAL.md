# 133-01 Masking Scanner — Accepted Residual Findings (deferred to Phase 134 DATA-05)

Status: **ACCEPTED (owner decision, 2026-07-20)** — Wave-1 masking scanner
(`scripts/check_atlas_masking.py`, commits `02657578` + `bea1d7cd`) accepted as-is
after 3 opus hardening rounds + 3 Codex adversarial reviews. The scanner is the
BACKSTOP, not the only defense (the 133-02 bake emits only masking-safe fields — no
reference text/sigla — by construction; 133-06 adds a live browser-DOM scan). The one
leak that actually occurred (plain-UTF-8 SUMMARY prose) is solidly caught.

Codex closed 2/10 findings across the rounds (git NUL-delimited enumeration; empty-pattern
+ self-test bypass). The remaining findings below are recorded for the **Phase 134
permanent DATA-05 CI guard**, where the scanner is promoted and should be hardened.

## Bucket A — genuine fail-closed defects (harden in 134; low effort)
- **A1 (HEAD misclassification):** a corrupt/unreadable HEAD is treated as an unborn repo,
  silently dropping the whole HEAD surface. Fix: positively establish unborn; any other
  `rev-parse` failure → `ScanError`.
- **A2 (non-strict fail-open):** in non-strict asset mode, missing path / symlink-read /
  stat / read failures are swallowed. Fix: operational errors fatal in every mode;
  non-strict should affect only suffix selection.
- **A3 (truncated Brotli):** incremental decode output accepted without an `is_finished()`
  end-of-stream check — a truncated prefix scans only partially. Fix: require decoder
  completion after the final input byte.
- **A4 (strict-mode dir):** `--strict` is satisfied by a single file/symlink/unsupported
  root type. Fix: require a real, non-symlink directory containing ≥1 regular file.

## Bucket B — theoretical multi-encoding gaps, out-of-threat-model for this UTF-8 corpus
(near-unreachable: repo + atlas asset are UTF-8; asset is a bounded <6 MB file)
- **B1:** arbitrary *mixed* non-ASCII casing / exotic non-normalized sequences (standard
  case + NFC/NFD + casefold forms are already covered; the streaming haystack itself is
  never normalized+casefolded).
- **B2:** BOM-less UTF-16/32 evades the head-only 20%-NUL heuristic.
- **B3:** URL-encoding composed with HTML/JS encoding of the term (multi-layer).
- **B4:** streamed BOM-declared decode failures suppressed — only reachable on >256 MiB
  files (our asset is <6 MB).
- **B5:** a filename that is simultaneously URL- and HTML-encoded M-source evades
  path detection/redaction.

## Recommended 134 direction
Replace the finite precomputed byte-form set with ONE canonical
decode(all supported encodings) → unescape(URL∘HTML∘JS, bounded depth) →
NFC/NFD-normalize → casefold → match pipeline, streaming-safe, fail-CLOSED on every
operational error, applied identically to content AND path components AND diagnostics.
Full Codex round-3 detail was captured in the session scratchpad
(`codex_wave1_rereview3.log`) at decision time.
