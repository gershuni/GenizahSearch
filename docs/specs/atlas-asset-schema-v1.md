# Atlas Asset Binary Schema v1 (FROZEN)

**Status:** FROZEN. This document is authored BEFORE the encoder implementation
(`scripts/build_atlas_asset.py`) so that a downstream decoder (Phase 133 plan
133-04, and this same plan's own Python reference decoder used by the golden
test) can be implemented field-for-field against a stable contract. Any change
to this layout MUST bump `schema_version` and the content-hashed asset
filename (see "Versioning" below) — never silently reinterpret an existing
`schema_version=1` file.

**Scope:** This is the binary contract for the Phase 133 Visual Atlas Preview
static asset (ATLAS-01). It carries ONLY masking-safe, claim-free data: `sys_id`,
positions, algorithmic cluster/domain/library indices, catalogue titles and
shelfmarks (our own `libraries.csv` data), and FJMS domain labels. It carries
**no discovery-overlay fields** (no gold candidates, no discovery counts, no
identification claims) — see D-04 in `133-CONTEXT.md`.

## 1. File Layout Overview

```
[ fixed header: 16 bytes            ]
[ section table: 32 bytes * N        ]   (N = section_count from the fixed header)
[ section 1 data (padded to 8 bytes) ]
[ section 2 data (padded to 8 bytes) ]
...
[ section N data (padded to 8 bytes) ]
```

Every multi-byte integer and float in this file is **little-endian**. Every
section's `byte_offset` (see §3) is padded so it starts on an **8-byte
boundary** — this is stricter than the minimum alignment any single section's
dtype requires (4 or 2 bytes), so it uniformly satisfies the alignment
requirement of every typed-array view (`Float32Array`, `Uint16Array`,
`Uint32Array`, `BigUint64Array`) without per-dtype special-casing.

## 2. Fixed Header (16 bytes, offset 0)

| Field | Offset | Type | Value |
|-------|--------|------|-------|
| `magic` | 0 | 8 raw bytes | ASCII literal `ATLAS001` (no null terminator; exactly 8 bytes) |
| `schema_version` | 8 | uint32 LE | `1` for this document |
| `section_count` | 12 | uint32 LE | Number of entries in the section table that follows |

A decoder MUST verify `magic == b"ATLAS001"` and MUST refuse to decode (raise,
not guess) if `schema_version` is a value it does not implement.

## 3. Section Table (32 bytes per entry, starts at offset 16)

Each entry describes exactly one section:

| Field | Type | Meaning |
|-------|------|---------|
| `section_id` | uint32 LE | See §4 enum — identifies WHAT the section holds |
| `dtype_code` | uint32 LE | See §5 enum — identifies the primitive element type actually used |
| `elem_size` | uint32 LE | Bytes per primitive element for this section (1, 2, 4, or 8) — MUST agree with `dtype_code` |
| `count` | uint32 LE | Number of primitive elements in this section (NOT records — a 2-float (x,y) position section has `count = 2 * node_count`) |
| `byte_offset` | uint64 LE | Absolute byte offset (from file start) where this section's raw bytes begin; always a multiple of 8 |
| `byte_length` | uint64 LE | `count * elem_size` (no internal padding within a section; padding only occurs BETWEEN sections) |

The section table has `section_count` entries, back-to-back, starting at byte
16. Since `16 + 32 * section_count` is always a multiple of 8, the first
section's data always naturally begins on an 8-byte boundary with no extra
padding needed between the section table and the first section's data.

**Why some sections use a dynamic dtype (Uint16 vs Uint32):** node-cluster and
flow-cluster indices are sized to the ACTUAL number of clusters produced by a
given bake (`dtype_code` = UINT16 if `cluster_count <= 65535`, else UINT32).
The section table's own `dtype_code`/`elem_size` fields are the single source
of truth for which width was chosen — a decoder MUST read them from the table,
never assume a fixed width for these particular sections.

## 4. Section ID Enum (§3 `section_id`)

| `section_id` | Name | dtype | count semantics | Notes |
|---|------|-------|------------------|-------|
| 1 | `NODE_POS` | Float32 | `2 * node_count` | Interleaved `x0,y0,x1,y1,...`; node order is the canonical node order used by every other `NODE_*` section |
| 2 | `NODE_CLUSTER` | Uint16 or Uint32 (dynamic) | `node_count` | Index into the cluster space (0-based); the SAME cluster-index space referenced by `FLOW_SOURCE_CLUSTER`/`FLOW_TARGET_CLUSTER`/`CLUSTER_LABEL_CI` |
| 3 | `NODE_DOMAIN` | Uint16 | `node_count` | Index 0-12 into `manifest.json["domain_groups"]` (13 fixed FJMS domain groups; see §8) |
| 4 | `NODE_LIBRARY` | Uint16 | `node_count` | Index into `manifest.json["libraries"]` (bake-discovered list of library codes; see §8) |
| 5 | `NODE_PROMINENCE` | Uint16 | `node_count` | Capped node degree (`min(raw_degree, 65535)`) — a relative prominence signal, NOT a claim |
| 6 | `NODE_SYS_ID` | Uint64 (BigUint64) | `node_count` | The manuscript's `sys_id`. **SOLE representation — see §7.** |
| 7 | `NODE_TITLE_REF` | Uint32 | `2 * node_count` | Interleaved `(offset, length)` pairs into `STRING_HEAP` (§4 id 23) for this node's own catalogue title; `length == 0` means "no title" |
| 8 | `NODE_SHELFMARK_REF` | Uint32 | `2 * node_count` | Interleaved `(offset, length)` pairs into `STRING_HEAP` for this node's shelfmark |
| 9 | `EDGE_SOURCE_DELTA` | Uint32 | `edge_count` | Delta-encoded edge source index — see §6 decode algorithm |
| 10 | `EDGE_TARGET_DELTA` | Uint32 | `edge_count` | Delta-encoded edge target index — see §6 decode algorithm |
| 11 | `EDGE_CLASS` | Uint8 | `edge_count` | `0` = continuation (same-work evidence), `1` = island (citation/quotation evidence). **Never** describes a physical join (Pitfall #2) |
| 12 | `FLOW_SOURCE_CLUSTER` | Uint16 or Uint32 (matches `NODE_CLUSTER`'s dtype) | `flow_count` | Aggregate inter-cluster flow source |
| 13 | `FLOW_TARGET_CLUSTER` | Uint16 or Uint32 (matches `NODE_CLUSTER`'s dtype) | `flow_count` | Aggregate inter-cluster flow target |
| 14 | `FLOW_WEIGHT` | Float32 | `flow_count` | Combined (continuation + island) inter-cluster edge-count weight |
| 15 | `CLUSTER_LABEL_CI` | Uint16 or Uint32 (matches `NODE_CLUSTER`'s dtype) | `label_count` | Which cluster index this label describes (labels are a FILTERED subset — only clusters with `member_count >= 25` get a label) |
| 16 | `CLUSTER_LABEL_X` | Float32 | `label_count` | Cluster centroid x |
| 17 | `CLUSTER_LABEL_Y` | Float32 | `label_count` | Cluster centroid y |
| 18 | `CLUSTER_LABEL_R` | Float32 | `label_count` | Cluster layout radius |
| 19 | `CLUSTER_LABEL_N` | Uint32 | `label_count` | Cluster member count |
| 20 | `CLUSTER_LABEL_DGRP` | Uint16 | `label_count` | Cluster's dominant FJMS domain-group index (same space as `NODE_DOMAIN`) |
| 21 | `CLUSTER_LABEL_TITLE_REF` | Uint32 | `2 * label_count` | `(offset, length)` into `STRING_HEAP` for the cluster's representative catalogue title (may be empty) |
| 22 | `CLUSTER_LABEL_DOM_REF` | Uint32 | `2 * label_count` | `(offset, length)` into `STRING_HEAP` for the cluster's dominant FJMS domain TEXT label (a specific label string, distinct from the 13-way `CLUSTER_LABEL_DGRP` group index) |
| 23 | `STRING_HEAP` | Uint8 (raw bytes) | `heap_byte_length` | One shared UTF-8 byte blob; every `*_REF` section above stores `(offset, length)` byte-slices into this blob. Strings are content-deduplicated at bake time (identical strings share one heap slice) |

**Sections NOT in the binary payload (kept in `manifest.json` instead):**
domain-group EN/HE names + colors (13 static entries) and the bake-discovered
library-code list are small, fixed-shape metadata that the manifest already
carries alongside the binary (content_hash, counts, etc.) — see §8. Putting
them there avoids inflating the binary's string heap with values that are
tiny, static, and needed anyway for cross-checking the bake. `NODE_DOMAIN` and
`NODE_LIBRARY` index INTO these manifest arrays.

## 5. dtype Enum (§3 `dtype_code`)

| `dtype_code` | Name | `elem_size` | Numpy dtype string |
|---|------|---|---|
| 1 | FLOAT32 | 4 | `<f4` |
| 2 | UINT8 | 1 | `<u1` |
| 3 | UINT16 | 2 | `<u2` |
| 4 | UINT32 | 4 | `<u4` |
| 5 | UINT64 | 8 | `<u8` |

## 6. Edge Delta-Encoding — Decode Algorithm

Edges are stored pre-sorted by `(source_idx ascending, target_idx ascending)`.
Both `EDGE_SOURCE_DELTA` and `EDGE_TARGET_DELTA` are **plain unsigned Uint32
deltas — no zigzag encoding is needed**, because of how the "delta baseline"
is defined below: both values are always `>= 0` by construction, so no
sign-handling is required.

```
running_source = 0
running_target = 0
edges = []
for i in range(edge_count):
    running_source += source_delta[i]           # always >= 0 (sorted ascending)
    if source_delta[i] > 0 or i == 0:
        # new source group (or the very first edge): target is stored ABSOLUTE
        running_target = target_delta[i]
    else:
        # same source as the previous edge: target is stored as an INCREMENT
        running_target += target_delta[i]
    edges.append((running_source, running_target))
```

**Why this is always non-negative:** `source_delta[i]` is the gap to the next
DISTINCT-OR-SAME source in ascending order, so it can never be negative.
`target_delta[i]` is either an absolute node index (always `>= 0`) when the
source group just changed, or the ascending-order increment within the same
source group (also always `>= 0`, since targets are sorted ascending within
a group). This lets both delta arrays live in an unsigned Uint32 typed array
with no sign bit and no zigzag transform.

## 7. `sys_id` — Single BigUint64 Representation (No Fallback)

`NODE_SYS_ID` (§4 id 6) is the **ONLY** place `sys_id` is encoded, as
`Uint64`/`BigUint64`. There is **no string-heap fallback and no alternate
representation** for `sys_id` anywhere in this schema.

**Bake-time invariant (hard, non-negotiable):** every `sys_id` MUST be a
pure-digit value (matching `^[0-9]+$` when read as a string, or a
non-negative Python `int` when already typed) strictly less than `2**64`. The
bake asserts this for EVERY eligible node and **FAILS the bake** (raises /
exits non-zero) the instant any `sys_id` violates it — there is no code path
that silently demotes an invalid `sys_id` to a string-heap entry or drops the
node. This keeps the decoder contract single-representation: a 133-04 decoder
never needs to check "is this sys_id in the typed array or the string heap" —
it is always in `NODE_SYS_ID`.

A JS decoder MUST read `NODE_SYS_ID` via `BigUint64Array` and reconstruct the
display string via `.toString()` (or `BigInt(...)`), never via `Number(...)`
— `sys_id` values (e.g. `990001562160205171`) routinely exceed
`Number.MAX_SAFE_INTEGER` (2^53 − 1), so a `Float64`/`Number` round-trip would
silently corrupt the low digits and break `/browse?sys_id=` links. The Python
reference decoder in `scripts/build_atlas_asset.py::decode_asset` reconstructs
`sys_id` as a decimal `str` for exactly this reason, and the committed golden
fixture's expected-values JSON (`tests/fixtures/atlas/golden-v1-expected.json`)
stores every `sys_id` (and any other value `> 2**53`) as a decimal STRING —
compared via `int(str)` in Python and (for the downstream 133-04 JS golden
test) `BigInt(str)` in JavaScript — so `JSON.parse` can never silently lose
precision above 2^53 on either side of the comparison.

## 8. Companion `manifest.json` (sits alongside the binary, not inside it)

Every bake writes (or, for `--golden`, folds into `*-expected.json`) a
manifest recording the metadata a decoder and an operator both need but that
does not belong in the byte-optimized binary payload:

| Key | Meaning |
|---|---|
| `schema_version` | Must match the binary's fixed-header `schema_version` |
| `content_hash` | First 12 hex chars of `sha256(plain .bin bytes)` |
| `asset_basename` | `atlas-v1-<content_hash>` — the content-hashed filename stem (see §9) |
| `algo_version` | Free-text algorithm/version string (Louvain + force-layout + phyllotaxis pipeline identifier) |
| `seed` | The deterministic RNG seed used for Louvain + force-layout + phyllotaxis (`42`) |
| `eligible_count` | Size of the eligible connected-endpoint set (every `sys_id` appearing in ANY manuscript-pair relation, continuation OR island) |
| `placed_count` | Size of the actually-placed node set |
| `missing` | List of `sys_id` (as decimal strings) in `eligible` but not `placed` — MUST be empty for a valid bake |
| `extra` | List of `sys_id` (as decimal strings) in `placed` but not `eligible` — MUST be empty for a valid bake |
| `node_count`, `edge_count`, `cluster_count`, `flow_count`, `label_count` | Section-size counts, for quick cross-checking against the section table |
| `source_db_hash` | Hash/identifier of the source research DB snapshot used (or a synthetic marker for `--smoke`/`--golden` bakes) |
| `domain_groups` | 13-entry list of `[name_en, name_he, color_hex]`, index-matched to `NODE_DOMAIN` / `CLUSTER_LABEL_DGRP` |
| `libraries` | Bake-discovered list of library codes, index-matched to `NODE_LIBRARY` |
| `sections` | The full section table, human-readable: list of `{id, name, dtype, elem_size, count, byte_offset, byte_length}` |

**No discovery-overlay fields anywhere in this manifest** (D-04) — no gold
candidate counts, no identification scores, no claim-level statements.

## 9. Versioning & Cache Invalidation

The shipped filename is `atlas-v1-<content_hash>.bin` / `.bin.br`, where
`<content_hash>` is derived from the plain (uncompressed) payload bytes.
**Any byte change to the payload changes `content_hash`, which changes the
filename** — a rebake can never be served under a URL a browser has already
cached under `Cache-Control: immutable` (MEDIUM-4). A schema-breaking change
(new/removed/reordered sections, a changed dtype convention) additionally
bumps `schema_version` and this document's own filename suffix (e.g. a future
`atlas-asset-schema-v2.md`), so an old decoder can detect and refuse an
incompatible file rather than silently misinterpreting it.

## 10. Full Decode Algorithm (step-by-step summary)

1. Read the fixed header (16 bytes). Verify `magic == b"ATLAS001"`. Read
   `schema_version` and `section_count`.
2. Read `section_count` section-table entries (32 bytes each, starting at
   byte 16). Build a `{section_id: (dtype_code, elem_size, count,
   byte_offset, byte_length)}` map.
3. For each section, slice `file_bytes[byte_offset : byte_offset +
   byte_length]` and interpret it as a typed array of `count` elements using
   `dtype_code`'s width (§5). `STRING_HEAP` (id 23) is kept as a raw byte
   buffer, not further typed.
4. Reshape `NODE_POS` into `(node_count, 2)` pairs; every other `NODE_*`
   section is a flat `(node_count,)` array in the same node order.
5. Decode `EDGE_SOURCE_DELTA` + `EDGE_TARGET_DELTA` via the §6 algorithm to
   recover `(source_idx, target_idx)` pairs; pair index `i` with
   `EDGE_CLASS[i]` for the continuation/island class.
6. For every `*_REF` pair `(offset, length)`, decode
   `string_heap[offset : offset + length].decode('utf-8')` (empty string if
   `length == 0`).
7. Reconstruct `sys_id` for node `i` as `str(int(NODE_SYS_ID[i]))` (Python) or
   `NODE_SYS_ID[i].toString()` (JS `BigUint64Array` element) — never via a
   `Number`/`float` cast (§7).
8. Cross-reference `NODE_DOMAIN[i]` / `NODE_LIBRARY[i]` against the
   companion `manifest.json`'s `domain_groups` / `libraries` arrays for
   display names/colors — these are NOT in the binary.
