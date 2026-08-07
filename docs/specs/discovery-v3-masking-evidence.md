# discovery-v3 — masking evidence record (gate 5 / gate 16)

**Dated 2026-08-07.** This is the pre-build masking record the bake plan's §8 readiness box says is owed.
It exists so a later reader can tell **which pattern set ran** and **what was scanned**, without any
restricted string being written down.

**D-25 posture:** this file contains counts, exit codes and hex digests only. No pattern text, no prefix,
no length. The two restricted corpora appear here — as everywhere in committed content — only as
**M-source** and **R-source**.

---

## 1. Pattern-set identity (keyed attestation)

`scripts/check_atlas_masking.py --attest`, with `MASKING_ATTESTATION_KEY` set from the gitignored
`/.masking_attest_key` (minted 2026-08-07, 32 bytes hex):

| field | value |
|---|---|
| `pattern_count` | **8** |
| `pattern_set_hmac` | `82f0a50302e17c4f61f406bdab15ba377f76aced6bf5244556c126a9f1fa0086` |
| `pattern_digests` | `84651695 558fb712 ab1aee15 bb6158fe d54bbe9c e7162ddb 4a811b41 2778f4c6` |

**Verified properties** (not assumed):

- **Reproducible** — two runs under the same key produced the identical `pattern_set_hmac`.
- **Key-binding** — a different key produced a different HMAC (`75c77035…`), so the digest is genuinely
  keyed rather than a plain hash wearing a keyed name.
- **Fails closed unkeyed** — without the key the tool omits `pattern_set_hmac` *and every per-pattern
  digest*, because an unkeyed digest is a membership **oracle**: anyone could hash a guessed term and
  compare the prefix.

⚠️ **The key must be as durable as `.masking_patterns` itself.** Rotating or losing it makes every
previously-recorded `pattern_set_hmac` unverifiable, silently downgrading this record to a bare count —
and a count alone cannot distinguish two different 8-pattern sets.

### Why 8, and not the 15 this plan used to claim

The **15 was never measured**; it was carried forward from an early draft. The arithmetic closes exactly:

| step | count |
|---|---|
| original set | 6 |
| owner adds four fingerprint forms (2026-08-06): Hebrew singular + plural, transliterated singular + plural | 10 |
| two **bare-Hebrew** forms withdrawn — ordinary Hebrew, matched **48 innocent** occurrences in our own transcriptions (47 in `web/Transcriptions_part.txt`, 1 in `web/pages/help.py`) | **8** |

Nothing is missing from the set. The number in the document was wrong.

---

## 2. Gate 16 — is the signature-vocabulary term actually caught?

The §5.0a finding said the term was absent from the scanned set, so this class of leak was uncaught.
**That verdict is falsified.** Probed by writing each candidate form to a scratch file and scanning it with
the live set — the term itself is never printed, only the exit code:

| probe | scanner verdict | reading |
|---|---|---|
| transliterated **singular** | **CAUGHT** (exit 1) | the leak class IS covered |
| transliterated **plural** | **CAUGHT** (exit 1) | both Latin-script forms covered |
| bare 5-char stem | clean (exit 0) | **expected** — this is the `FORBIDDEN_COLUMN_SUBSTRINGS` value in `scripts/v3_build_research_db.py`, cleartext in committed code by design. Making it a pattern would redden `--scan-repo` on the guard itself. |
| `src_attr_note` (neutral replacement) | clean (exit 0) | the negative control |

**Residual, stated plainly:** the bare-**Hebrew** forms remain excluded by measured decision, so for that
script the slim-DB column denylist stays the operative control — which is what it was always documented to
be, not a downgrade. This is also why the CI secret must not carry the Hebrew forms: `--scan-repo` runs in
`render-smoke-tests`, and the standing rule correctly forbids "fixing" a red scan by skipping it.

---

## 3. Full strict scan — all three surfaces

```
--attest --strict --scan-repo --scan-asset atlas_data/ \
  --scan-sqlite discovery_data/discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db
```

**Result: `no matches -- clean`, EXIT=0.** Surfaces confirmed by the tool's own line:
`repo, asset:atlas_data/, sqlite:…`

### Scanned-surface hashes (SHA-256, taken after the scan)

| surface | sha256 |
|---|---|
| `atlas_data/atlas-v1-61519a85a2d0.bin` | `61519a85a2d01b812eaee8dd73a4bfd1b25a150b402c749ac311da73535914b9` |
| `atlas_data/atlas-v1-61519a85a2d0.bin.br` | `9f7522d592a3f2522e82a2648ad385fcee6a4a72ddd264939d3d9e3284bc6313` |
| `atlas_data/manifest.json` | `eca877018873b69d0926ed8856c8bde2a0149617f5820fcbb08b65246e6bbe35` |
| `discovery_data/discovery-v1-33499c5b…db` | `33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff` |
| `discovery_data/manifest.json` | `ae393acd8d6c5c77666b0e1a1cb2c03bedd28c549cbcd4f566376e5de970963e` |

**The serving asset's content hash equals its own filename and the manifest's `content_hash`** — so this
record also attests the serving artifact was **not disturbed** by any of this work, which is a standing
constraint of the v3 bake.

**SQLite sidecars** — recorded because absence is itself a fact worth pinning (a `-wal` appearing mid-read
is how a scan can attest one state while a reader sees another):

| suffix | state |
|---|---|
| `-journal` | (absent) |
| `-wal` | (absent) |
| `-shm` | (absent) |

---

## 4. Fail-closed control

`MASKING_SCAN_PATTERNS_FILE` unset → **exit 1**, never a silent green. Re-confirmed this session.
`--attest` with no pattern file also exits 1: an attestation over an empty set is the worst possible
artifact, since it looks like evidence while attesting nothing.

**Permanent limit, not claimed away:** none of this proves the pattern set is *complete*. The scanner
cannot enumerate terms nobody told it about, and the `--self-test` needle is synthetic, so a passing scan
demonstrates mechanism plus identity — never completeness.

---

## 5. What this record does and does not discharge

**Discharged:** gate 5 (strict scan, all three surfaces, clean); gate 16 (the term is caught, measured);
the pattern-count reconciliation; the keyed attestation identity the build record owed.

**Still owed at bake time** (they describe artifacts that do not exist yet): the **post-build** hashes of
the v3 asset and its scanned surfaces, and gate 15's pre-build source-identity record. Re-run the same
keyed command against the v3 artifact once it is built and append the result here.
