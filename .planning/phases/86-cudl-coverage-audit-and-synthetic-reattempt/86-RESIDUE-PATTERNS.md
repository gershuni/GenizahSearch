# Phase 86 Residue Pattern Adjudication

**Generated:** auto by `scripts/build_residue_patterns_artifact.py`
**Source residue:** `reports/synthetic_ambiguity_residue_dryrun.csv` (Phase 86 --dry-run rebuild; 1847 entries)
**Adjudication target:** 5+ pattern families x Accept/Reject/Spot-check
**Ranker:** BRIDGE-AWARE -- for each residue classmark, FIST candidates are scored by whether `fist_to_cudl_keys(candidate_shelfmark)` produces the residue's cudl-normalized key (100), shares a >=3-char normalized prefix (50), or matches numeric tokens (tie-break). Pass 2 MEDIUM-1.

**Pass 2 LOW Codex -- Stop rule:** ONE generation + ONE adjudication pass is the default. `Spot-check more` becomes a Deferred annotation in `cudl_coverage.md`; further adjudication requires explicit user request and a separate plan revision. Do NOT auto-loop.

Each section below shows up to 5 sample CUDL classmarks from the residue, each paired with up to 3 bridge-aware nearest-neighbour FIST candidates (InventoryId, SignatureId, UnitCatalogRec Title + GenizahTitleText snippets, score, CUDL viewer URL -- Pass 2 LOW Gemini).

**You do NOT author regex.** Each family presents a CONCRETE PROPOSED RULE specified as a FIST->CUDL transformation (Pass 2 HIGH-5) with supporting and refuting FIST shelfmark fixtures plus a false-positive risk note. Adjudicate Accept / Reject / Spot-check more.

## Pattern Family: T-S F flattened-series hypothesis (441 entries)

**Hypothesis:** CUDL `tsf1.1100` may correspond to FIST `T-S F1(1).100` -- the leading `1` of the CUDL fragment digit encodes the FIST `(N)` series digit. Direction: FIST.Shelfmark `T-S F1(1).100` should ADD CUDL key `tsf1.1100` to fist_to_cudl_keys' output (in addition to the existing (N)-stripped `tsf1.100` from D-02a Pattern 3).

**Sample fixtures (up to 5 classmarks x up to 3 FIST candidates):**

| CUDL classmark | CUDL viewer URL | FIST cand shelfmark | InventoryId | SignatureId | UnitCatalogRec Title | GenizahTitleText | Score |
| -------------- | --------------- | ------------------- | ----------- | ----------- | -------------------- | ---------------- | ----- |
| `tsf1.11` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00001/1 | `T-S F1(1).11` | 5026101 | 12314 |  |  | 102 |
| `tsf1.11` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00001/1 | `T-S F1(2).11` | 5150101 | 12314 |  |  | 102 |
| `tsf1.11` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00001/1 | `T-S F2(1).11` | 5274101 | 12314 |  |  | 52 |
| `tsf1.12` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00002/1 | `T-S F1(1).12` | 5027101 | 12314 |  |  | 102 |
| `tsf1.12` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00002/1 | `T-S F1(2).12` | 5151101 | 12314 |  |  | 102 |
| `tsf1.12` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00002/1 | `T-S F2(1).12` | 5275101 | 12314 |  |  | 52 |
| `tsf1.13` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00003/1 | `T-S F1(1).13` | 5028101 | 12314 |  |  | 102 |
| `tsf1.13` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00003/1 | `T-S F1(2).13` | 5152101 | 12314 |  |  | 102 |
| `tsf1.13` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00003/1 | `T-S F2(1).13` | 5276101 | 12314 |  |  | 52 |
| `tsf1.14` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00004/1 | `T-S F1(1).14` | 5029101 | 12314 |  |  | 102 |
| `tsf1.14` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00004/1 | `T-S F1(2).14` | 5153101 | 12314 |  |  | 102 |
| `tsf1.14` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00004/1 | `T-S F2(1).14` | 5277101 | 12314 |  |  | 52 |
| `tsf1.16` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00006/1 | `T-S F1(1).16` | 5031101 | 12314 |  |  | 102 |
| `tsf1.16` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00006/1 | `T-S F1(2).16` | 5155101 | 12314 |  |  | 102 |
| `tsf1.16` | https://cudl.lib.cam.ac.uk/view/MS-TS-F-00001-00001-00006/1 | `T-S F2(1).16` | 5279101 | 12314 |  |  | 52 |

**Proposed FIST->CUDL normalizer rule (concrete -- Pass 2 HIGH-5; do not author regex; review and adjudicate):**

```python
# Rule: T-S F flattened-series hypothesis
# Rule name (for test): tsf_flattened_series
# Direction: FIST.Shelfmark -> add CUDL key(s) to fist_to_cudl_keys' output
# FIST regex on dbo_Inventory.Shelfmark: r"^T-S F(\d+)\((\d)\)\.(\d+)$"
# Resulting CUDL key template:        tsf{series_n}.{n_digit}{fragment}
#
# Implementation sketch -- add this branch inside fist_to_cudl_keys, AFTER the
# existing 4 D-02a branches, gated to the appropriate family prefix:
#   import re
#   _RULE_RE = re.compile(r"^T-S F(\d+)\((\d)\)\.(\d+)$")
#   m = _RULE_RE.match(c)
#   if m:
#       # construct CUDL key per cudl_key_template using m.group(...)
#       keys.add(constructed_cudl_key)
```

**Supporting FIST.Shelfmark fixtures (proposed rule succeeds -- Pass 2 HIGH-5):**
- `T-S F1(1).100  -> adds tsf1.1100`
- `T-S F2(2).50   -> adds tsf2.250`
- `T-S F17(1).234 -> adds tsf17.1234`

**Refuting FIST.Shelfmark fixture (proposed rule must NOT apply -- Pass 2 HIGH-5):**
- `T-S F1.10        (no (N) -- must NOT add a flattened alias)`

**Test scaffold (Pass 2 HIGH-5 -- executor will instantiate this):**

```python
# tests/test_fist_cudl_bridge.py -- add inside TestFistToCudlKeys
def test_accepted_rule_tsf_flattened_series_fist_to_cudl(self):
    """D-02c accepted rule: FIST->CUDL direction."""
    # Positive: supporting FIST input should ADD the expected CUDL key.
    keys = fist_to_cudl_keys('T-S F1(1).100')
    assert 'tsf1.1100' in keys, f"missing key: {keys}"

def test_refute_rule_tsf_flattened_series_fist_to_cudl(self):
    """D-02c accepted rule: refuting fixture must NOT trigger."""
    keys = fist_to_cudl_keys('T-S F1.10')
    # The unexpected flat/collapsed alias from this rule must NOT appear:
    # (adjust the asserted-absent key when integrating the actual rule.)
    assert True, 'replace with: <expected-absent-key> not in keys'
```

**False-positive risk:** Risk: T-S F shelfmarks with naturally 3+ digit fragments could collide; Codex MEDIUM: rule remains prefix-gated to T-S F only.

**User decision:** [ ] Accept rule  [ ] Reject  [ ] Spot-check more (Deferred -- see stop rule)

**Rejection rationale (if Rejected):**

```
# Fill in: why this family is genuinely residual, not encoding gap.
```

## Pattern Family: T-S Ar flattened-series hypothesis (401 entries)

**Hypothesis:** Same shape as T-S F: FIST.Shelfmark `T-S Ar 18(2).34` should ADD CUDL key `tsar18.234` (the leading 2 of the CUDL fragment encodes the FIST (N) series digit).

**Sample fixtures (up to 5 classmarks x up to 3 FIST candidates):**

| CUDL classmark | CUDL viewer URL | FIST cand shelfmark | InventoryId | SignatureId | UnitCatalogRec Title | GenizahTitleText | Score |
| -------------- | --------------- | ------------------- | ----------- | ----------- | -------------------- | ---------------- | ----- |
| `tsar18.11` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00001/1 | `T-S Ar.18(1).11` | 14852103 | 14314 |  |  | 102 |
| `tsar18.11` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00001/1 | `T-S Ar.18(2).11` | 15042103 | 14314 |  |  | 102 |
| `tsar18.11` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00001/1 | `T-S Ar.11.18` | 14669103 | 29314 |  |  | 52 |
| `tsar18.12` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00002/1 | `T-S Ar.18(1).12` | 14853103 | 14314 |  |  | 102 |
| `tsar18.12` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00002/1 | `T-S Ar.18(2).12` | 15043103 | 14314 |  |  | 102 |
| `tsar18.12` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00002/1 | `T-S Ar.12.18` | 14705103 | 22212 |  |  | 52 |
| `tsar18.13` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00003/1 | `T-S Ar.18(1).13` | 14854103 | 14314 |  |  | 102 |
| `tsar18.13` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00003/1 | `T-S Ar.18(2).13` | 15044103 | 14314 |  |  | 102 |
| `tsar18.13` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00003/1 | `T-S Ar.: T-S Ar 18.34` | 2471099 | 29596814 |  |  | 51 |
| `tsar18.14` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00004/1 | `T-S Ar.18(1).14` | 14855103 | 14314 |  |  | 102 |
| `tsar18.14` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00004/1 | `T-S Ar.18(2).14` | 15045103 | 14314 |  |  | 102 |
| `tsar18.14` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00004/1 | `T-S Ar.: T-S Ar 18.34` | 2471099 | 29596814 |  |  | 51 |
| `tsar18.15` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00005/1 | `T-S Ar.18(1).15` | 14856103 | 14314 |  |  | 102 |
| `tsar18.15` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00005/1 | `T-S Ar.18(2).15` | 15046103 | 14314 |  |  | 102 |
| `tsar18.15` | https://cudl.lib.cam.ac.uk/view/MS-TS-AR-00018-00001-00005/1 | `T-S Ar.: T-S Ar 18.34` | 2471099 | 29596814 |  |  | 51 |

**Proposed FIST->CUDL normalizer rule (concrete -- Pass 2 HIGH-5; do not author regex; review and adjudicate):**

```python
# Rule: T-S Ar flattened-series hypothesis
# Rule name (for test): tsar_flattened_series
# Direction: FIST.Shelfmark -> add CUDL key(s) to fist_to_cudl_keys' output
# FIST regex on dbo_Inventory.Shelfmark: r"^T-S Ar (\d+)\((\d)\)\.(\d+)$"
# Resulting CUDL key template:        tsar{series_n}.{n_digit}{fragment}
#
# Implementation sketch -- add this branch inside fist_to_cudl_keys, AFTER the
# existing 4 D-02a branches, gated to the appropriate family prefix:
#   import re
#   _RULE_RE = re.compile(r"^T-S Ar (\d+)\((\d)\)\.(\d+)$")
#   m = _RULE_RE.match(c)
#   if m:
#       # construct CUDL key per cudl_key_template using m.group(...)
#       keys.add(constructed_cudl_key)
```

**Supporting FIST.Shelfmark fixtures (proposed rule succeeds -- Pass 2 HIGH-5):**
- `T-S Ar 18(2).34 -> adds tsar18.234`
- `T-S Ar 3(1).50  -> adds tsar3.150`
- `T-S Ar 25(1).50 -> adds tsar25.150`

**Refuting FIST.Shelfmark fixture (proposed rule must NOT apply -- Pass 2 HIGH-5):**
- `T-S Ar 3.50      (no (N) -- must NOT add a flattened alias)`

**Test scaffold (Pass 2 HIGH-5 -- executor will instantiate this):**

```python
# tests/test_fist_cudl_bridge.py -- add inside TestFistToCudlKeys
def test_accepted_rule_tsar_flattened_series_fist_to_cudl(self):
    """D-02c accepted rule: FIST->CUDL direction."""
    # Positive: supporting FIST input should ADD the expected CUDL key.
    keys = fist_to_cudl_keys('T-S Ar 18(2).34')
    assert 'tsar18.234' in keys, f"missing key: {keys}"

def test_refute_rule_tsar_flattened_series_fist_to_cudl(self):
    """D-02c accepted rule: refuting fixture must NOT trigger."""
    keys = fist_to_cudl_keys('T-S Ar 3.50')
    # The unexpected flat/collapsed alias from this rule must NOT appear:
    # (adjust the asserted-absent key when integrating the actual rule.)
    assert True, 'replace with: <expected-absent-key> not in keys'
```

**False-positive risk:** Same risk as T-S F; prefix-gate to T-S Ar.

**User decision:** [ ] Accept rule  [ ] Reject  [ ] Spot-check more (Deferred -- see stop rule)

**Rejection rationale (if Rejected):**

```
# Fill in: why this family is genuinely residual, not encoding gap.
```

## Pattern Family: T-S NS minute-fragments + letter suffixes (179 entries)

**Hypothesis:** FIST writes `T-S NS 192.minute fragments` (phrase suffix). CUDL writes as `tsns192minutefragments`. Direction: FIST.Shelfmark `T-S NS 192.minute fragments` should ADD CUDL key `tsns192minutefragments`. Letter-suffix variants (FIST `T-S NS 135.1.AA`) need separate adjudication.

**Sample fixtures (up to 5 classmarks x up to 3 FIST candidates):**

| CUDL classmark | CUDL viewer URL | FIST cand shelfmark | InventoryId | SignatureId | UnitCatalogRec Title | GenizahTitleText | Score |
| -------------- | --------------- | ------------------- | ----------- | ----------- | -------------------- | ---------------- | ----- |
| `tsns23minutefragments` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00023-MINUTE-FRAGMENTS/1 | `T-S NS: T-S NS 23 ,51` | 1814099 | 2914308 |  |  | 51 |
| `tsns23minutefragments` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00023-MINUTE-FRAGMENTS/1 | `AIU: CUL: T-S NS 246.23a` | 4929099 | 777832814 |  |  | 51 |
| `tsns23minutefragments` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00023-MINUTE-FRAGMENTS/1 | `AIU: CUL: T-S NS 246.23b` | 4930099 | 777833814 |  |  | 51 |
| `tsns29minutefragments` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00029-MINUTE-FRAGMENTS/1 | `AIU: CUL: T-S NS 30.29` | 5187099 | 781037814 |  |  | 51 |
| `tsns29minutefragments` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00029-MINUTE-FRAGMENTS/1 | `AIU: CUL: T-S NS 316.29` | 5370099 | 781875814 |  |  | 51 |
| `tsns29minutefragments` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00029-MINUTE-FRAGMENTS/1 | `AIU: CUL: T-S NS 330.29` | 5473099 | 782361814 |  |  | 51 |
| `tsns31minutefragments` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00031-MINUTE-FRAGMENTS/1 | `AIU: CUL: T-S NS 172.31` | 4835099 | 776916814 |  |  | 51 |
| `tsns31minutefragments` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00031-MINUTE-FRAGMENTS/1 | `AIU: CUL: T-S NS 266.31` | 4949099 | 780208814 |  |  | 51 |
| `tsns31minutefragments` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00031-MINUTE-FRAGMENTS/1 | `AIU: CUL: T-S NS 266.31` | 4950099 |  |  |  | 51 |
| `tsns43.61minutefragments` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00043-00061-MINUTE-FRAGMENTS/1 | `T-S NS: T-S NS 169.43` | 1811099 | 1777018 |  |  | 51 |
| `tsns43.61minutefragments` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00043-00061-MINUTE-FRAGMENTS/1 | `AIU: CUL: T-S NS 172.43` | 4848099 | 776927814 |  |  | 51 |
| `tsns43.61minutefragments` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00043-00061-MINUTE-FRAGMENTS/1 | `AIU: CUL: T-S NS 172.61` | 4870099 | 776945814 |  |  | 51 |
| `tsns58unnumbered` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00058-UNNUMBERED/1 | `AIU: CUL: T-S NS 172.58a` | 4865099 | 777036814 |  |  | 51 |
| `tsns58unnumbered` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00058-UNNUMBERED/1 | `AIU: CUL: T-S NS 172.58b` | 4866099 | 776942814 |  |  | 51 |
| `tsns58unnumbered` | https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00058-UNNUMBERED/1 | `AIU: CUL: T-S NS 30.58` | 5237099 | 781100814 |  |  | 51 |

**Proposed FIST->CUDL normalizer rule (concrete -- Pass 2 HIGH-5; do not author regex; review and adjudicate):**

```python
# Rule: T-S NS minute-fragments + letter suffixes
# Rule name (for test): tsns_minute_fragments
# Direction: FIST.Shelfmark -> add CUDL key(s) to fist_to_cudl_keys' output
# FIST regex on dbo_Inventory.Shelfmark: r"^T-S NS (\d+)\.minute fragments?$"
# Resulting CUDL key template:        tsns{ns_number}minutefragments
#
# Implementation sketch -- add this branch inside fist_to_cudl_keys, AFTER the
# existing 4 D-02a branches, gated to the appropriate family prefix:
#   import re
#   _RULE_RE = re.compile(r"^T-S NS (\d+)\.minute fragments?$")
#   m = _RULE_RE.match(c)
#   if m:
#       # construct CUDL key per cudl_key_template using m.group(...)
#       keys.add(constructed_cudl_key)
```

**Supporting FIST.Shelfmark fixtures (proposed rule succeeds -- Pass 2 HIGH-5):**
- `T-S NS 192.minute fragments -> adds tsns192minutefragments`
- `T-S NS 200.minute fragment  -> adds tsns200minutefragment`
- `T-S NS 150.minute fragments -> adds tsns150minutefragments`

**Refuting FIST.Shelfmark fixture (proposed rule must NOT apply -- Pass 2 HIGH-5):**
- `T-S NS 135.1.AA  (letter suffix -- different family, must NOT match this rule)`

**Test scaffold (Pass 2 HIGH-5 -- executor will instantiate this):**

```python
# tests/test_fist_cudl_bridge.py -- add inside TestFistToCudlKeys
def test_accepted_rule_tsns_minute_fragments_fist_to_cudl(self):
    """D-02c accepted rule: FIST->CUDL direction."""
    # Positive: supporting FIST input should ADD the expected CUDL key.
    keys = fist_to_cudl_keys('T-S NS 192.minute fragments')
    assert 'tsns192minutefragments' in keys, f"missing key: {keys}"

def test_refute_rule_tsns_minute_fragments_fist_to_cudl(self):
    """D-02c accepted rule: refuting fixture must NOT trigger."""
    keys = fist_to_cudl_keys('T-S NS 135.1.AA')
    # The unexpected flat/collapsed alias from this rule must NOT appear:
    # (adjust the asserted-absent key when integrating the actual rule.)
    assert True, 'replace with: <expected-absent-key> not in keys'
```

**False-positive risk:** Low risk: 'minute fragments' is a distinctive FIST suffix.

**User decision:** [ ] Accept rule  [ ] Reject  [ ] Spot-check more (Deferred -- see stop rule)

**Rejection rationale (if Rejected):**

```
# Fill in: why this family is genuinely residual, not encoding gap.
```

## Pattern Family: Or. single-segment ambiguity (577 entries)

**Hypothesis:** CUDL `or1080.11` may correspond to FIST `Or.1080 11.1` (sub-fragment level) -- different fragment granularities. Direction: FIST.Shelfmark `Or.1080 11.1` should ADD CUDL key `or1080.11`. User must inspect IIIF content to confirm same-physical-fragment.

**Sample fixtures (up to 5 classmarks x up to 3 FIST candidates):**

| CUDL classmark | CUDL viewer URL | FIST cand shelfmark | InventoryId | SignatureId | UnitCatalogRec Title | GenizahTitleText | Score |
| -------------- | --------------- | ------------------- | ----------- | ----------- | -------------------- | ---------------- | ----- |
| `or1080.11` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00001/1 | `CUL: Or.1080 11.45` | 2728099 | 777104814 |  |  | 52 |
| `or1080.11` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00001/1 | `CUL: Or.1080 6.11` | 2732099 | 777103814 |  |  | 52 |
| `or1080.11` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00001/1 | `Or.1080 1.11` | 136804111 | 118403 |  |  | 52 |
| `or1080.12` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00002/1 | `CUL: Or.1080 C6.12` | 2751099 | 777145814 |  |  | 52 |
| `or1080.12` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00002/1 | `Or.1080 1.12` | 136805111 | 190375 |  |  | 52 |
| `or1080.12` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00002/1 | `Or.1080 2.12` | 136897111 | 273375 |  |  | 52 |
| `or1080.13` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00003/1 | `CUL: Or.1080 C6.13` | 2752099 | 777146814 |  |  | 52 |
| `or1080.13` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00003/1 | `Or.1080 1.13` | 136806111 | 191375 |  |  | 52 |
| `or1080.13` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00003/1 | `Or.1080 2.13` | 136898111 | 274375 |  |  | 52 |
| `or1080.14` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00004/1 | `CUL: Or.1080 B14.1` | 2735099 | 777108814 |  |  | 52 |
| `or1080.14` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00004/1 | `CUL: Or.1080 B14.1` | 2736099 |  |  |  | 52 |
| `or1080.14` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00004/1 | `CUL: Or.1080 B14.1` | 2737099 |  |  |  | 52 |
| `or1080.15` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00005/1 | `CUL: Or.1080 15.26` | 2729099 | 777105814 |  |  | 52 |
| `or1080.15` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00005/1 | `CUL: Or.1080 C6.15` | 2754099 | 777148814 |  |  | 52 |
| `or1080.15` | https://cudl.lib.cam.ac.uk/view/MS-OR-01080-00001-00005/1 | `Or.1080 1.15` | 136808111 | 193375 |  |  | 52 |

**Proposed FIST->CUDL normalizer rule (concrete -- Pass 2 HIGH-5; do not author regex; review and adjudicate):**

```python
# Rule: Or. single-segment ambiguity
# Rule name (for test): or_single_segment
# Direction: FIST.Shelfmark -> add CUDL key(s) to fist_to_cudl_keys' output
# FIST regex on dbo_Inventory.Shelfmark: r"^Or\.108[01] (\d+)\.1$"
# Resulting CUDL key template:        or108{X}.{segment}
#
# Implementation sketch -- add this branch inside fist_to_cudl_keys, AFTER the
# existing 4 D-02a branches, gated to the appropriate family prefix:
#   import re
#   _RULE_RE = re.compile(r"^Or\.108[01] (\d+)\.1$")
#   m = _RULE_RE.match(c)
#   if m:
#       # construct CUDL key per cudl_key_template using m.group(...)
#       keys.add(constructed_cudl_key)
```

**Supporting FIST.Shelfmark fixtures (proposed rule succeeds -- Pass 2 HIGH-5):**
- `Or.1080 11.1 -> adds or1080.11`
- `Or.1081 5.1  -> adds or1081.5`
- `Or.1080 73.1 -> adds or1080.73`

**Refuting FIST.Shelfmark fixture (proposed rule must NOT apply -- Pass 2 HIGH-5):**
- `Or.1080 11.2 (sub-fragment 2 -- different physical fragment; must NOT collapse)`

**Test scaffold (Pass 2 HIGH-5 -- executor will instantiate this):**

```python
# tests/test_fist_cudl_bridge.py -- add inside TestFistToCudlKeys
def test_accepted_rule_or_single_segment_fist_to_cudl(self):
    """D-02c accepted rule: FIST->CUDL direction."""
    # Positive: supporting FIST input should ADD the expected CUDL key.
    keys = fist_to_cudl_keys('Or.1080 11.1')
    assert 'or1080.11' in keys, f"missing key: {keys}"

def test_refute_rule_or_single_segment_fist_to_cudl(self):
    """D-02c accepted rule: refuting fixture must NOT trigger."""
    keys = fist_to_cudl_keys('Or.1080 11.2 (sub-fragment 2 -- different physical fragment; must NOT collapse)')
    # The unexpected flat/collapsed alias from this rule must NOT appear:
    # (adjust the asserted-absent key when integrating the actual rule.)
    assert True, 'replace with: <expected-absent-key> not in keys'
```

**False-positive risk:** HIGH risk: FIST sub-fragment may be a DIFFERENT physical fragment than CUDL classmark-level. User must inspect IIIF content (single image vs sequence).

**User decision:** [ ] Accept rule  [ ] Reject  [ ] Spot-check more (Deferred -- see stop rule)

**Rejection rationale (if Rejected):**

```
# Fill in: why this family is genuinely residual, not encoding gap.
```

## Pattern Family: Mosseri exotic letter suffixes (141 entries)

**Hypothesis:** FIST `Moss. III,117.1a` already maps via D-02a Pattern 1 -- but variants like `Moss. IV,270b` (no '.1' segment) may need a separate rule. Direction: FIST.Shelfmark `Moss. IV,270b` should ADD CUDL key `mosseriv270b` (no internal dot).

**Sample fixtures (up to 5 classmarks x up to 3 FIST candidates):**

| CUDL classmark | CUDL viewer URL | FIST cand shelfmark | InventoryId | SignatureId | UnitCatalogRec Title | GenizahTitleText | Score |
| -------------- | --------------- | ------------------- | ----------- | ----------- | -------------------- | ---------------- | ----- |
| `mosserii26.1` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00026-00001/1 | `Moss. Ia,26.1` | 251850 | 135228 |  |  | 52 |
| `mosserii26.1` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00026-00001/1 | `Moss. I,1` | 1850 | 1270 |  |  | 51 |
| `mosserii26.1` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00026-00001/1 | `Moss. I,3.1` | 3850 | 3315 |  |  | 51 |
| `mosserii26.2` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00026-00002/1 | `Moss. Ia,26.2` | 252850 | 136228 |  |  | 52 |
| `mosserii26.2` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00026-00002/1 | `Moss. I,2` | 2850 | 2315 |  |  | 51 |
| `mosserii26.2` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00026-00002/1 | `Moss. I,3.2` | 4850 | 4315 |  |  | 51 |
| `mosserii26.3` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00026-00003/1 | `Moss. I,3.1` | 3850 | 3315 |  |  | 51 |
| `mosserii26.3` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00026-00003/1 | `Moss. I,3.2` | 4850 | 4315 |  |  | 51 |
| `mosserii26.3` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00026-00003/1 | `Moss. I,9.3` | 14850 | 14315 |  |  | 51 |
| `mosserii51` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00051/1 | `Moss. I,51` | 86850 | 86315 |  |  | 101 |
| `mosserii51` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00051/1 | `Moss. II,51.1` | 356850 | 797301 |  |  | 51 |
| `mosserii51` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00051/1 | `Moss. II,51.2` | 357850 | 801301 |  |  | 51 |
| `mosserii52` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00052/1 | `Moss. I,52` | 87850 | 87315 |  |  | 101 |
| `mosserii52` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00052/1 | `Moss. II,52.1` | 358850 | 356315 |  |  | 51 |
| `mosserii52` | https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-I-00052/1 | `Moss. II,52.2` | 359850 | 357315 |  |  | 51 |

**Proposed FIST->CUDL normalizer rule (concrete -- Pass 2 HIGH-5; do not author regex; review and adjudicate):**

```python
# Rule: Mosseri exotic letter suffixes
# Rule name (for test): mosseri_exotic_letter
# Direction: FIST.Shelfmark -> add CUDL key(s) to fist_to_cudl_keys' output
# FIST regex on dbo_Inventory.Shelfmark: r"^Moss\. (I{1,4}A?|I{0,3}V|VI{0,3}A?|VII{0,3}|VIII|IX|X),(\d+)([a-z])$"
# Resulting CUDL key template:        mosseri{roman_lower}{number}{letter}
#
# Implementation sketch -- add this branch inside fist_to_cudl_keys, AFTER the
# existing 4 D-02a branches, gated to the appropriate family prefix:
#   import re
#   _RULE_RE = re.compile(r"^Moss\. (I{1,4}A?|I{0,3}V|VI{0,3}A?|VII{0,3}|VIII|IX|X),(\d+)([a-z])$")
#   m = _RULE_RE.match(c)
#   if m:
#       # construct CUDL key per cudl_key_template using m.group(...)
#       keys.add(constructed_cudl_key)
```

**Supporting FIST.Shelfmark fixtures (proposed rule succeeds -- Pass 2 HIGH-5):**
- `Moss. IV,270b   -> adds mosseriv270b`
- `Moss. III,117a  -> adds mosseriii117a`
- `Moss. IX,5c     -> adds mosseriix5c`

**Refuting FIST.Shelfmark fixture (proposed rule must NOT apply -- Pass 2 HIGH-5):**
- `Moss. III,27.1  (canonical dotted form -- handled by D-02a Pattern 1, not this rule)`

**Test scaffold (Pass 2 HIGH-5 -- executor will instantiate this):**

```python
# tests/test_fist_cudl_bridge.py -- add inside TestFistToCudlKeys
def test_accepted_rule_mosseri_exotic_letter_fist_to_cudl(self):
    """D-02c accepted rule: FIST->CUDL direction."""
    # Positive: supporting FIST input should ADD the expected CUDL key.
    keys = fist_to_cudl_keys('Moss. IV,270b')
    assert 'mosseriv270b' in keys, f"missing key: {keys}"

def test_refute_rule_mosseri_exotic_letter_fist_to_cudl(self):
    """D-02c accepted rule: refuting fixture must NOT trigger."""
    keys = fist_to_cudl_keys('Moss. III,27.1')
    # The unexpected flat/collapsed alias from this rule must NOT appear:
    # (adjust the asserted-absent key when integrating the actual rule.)
    assert True, 'replace with: <expected-absent-key> not in keys'
```

**False-positive risk:** Medium risk: uppercase letter variants ('Moss. IV,270B') may exist in FIST.

**User decision:** [ ] Accept rule  [ ] Reject  [ ] Spot-check more (Deferred -- see stop rule)

**Rejection rationale (if Rejected):**

```
# Fill in: why this family is genuinely residual, not encoding gap.
```

## Pattern Family: T-S Misc multi-segment patterns (98 entries)

**Hypothesis:** FIST `T-S Misc 1.131.1` should ADD CUDL key `tsmisc1.131.1` (CUDL preserves multi-segment structure with internal dots). D-02a Pattern 4 covers Or. but not T-S Misc; this rule extends.

**Sample fixtures (up to 5 classmarks x up to 3 FIST candidates):**

| CUDL classmark | CUDL viewer URL | FIST cand shelfmark | InventoryId | SignatureId | UnitCatalogRec Title | GenizahTitleText | Score |
| -------------- | --------------- | ------------------- | ----------- | ----------- | -------------------- | ---------------- | ----- |
| `tsmisc1.92.1` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00001-00092-00001/1 | `T-S Misc.1.92` | 24090105 | 3701317 |  |  | 52 |
| `tsmisc1.92.1` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00001-00092-00001/1 | `T-S Misc.1.92(1)` | 24091105 | 3701317 |  |  | 52 |
| `tsmisc1.92.1` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00001-00092-00001/1 | `T-S Misc.1.92(2)` | 24092105 | 3701317 |  |  | 52 |
| `tsmisc1.92.2` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00001-00092-00002/1 | `T-S Misc.1.92(2)` | 24092105 | 3701317 |  |  | 53 |
| `tsmisc1.92.2` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00001-00092-00002/1 | `T-S Misc.1.2` | 24000105 | 2908801 |  |  | 52 |
| `tsmisc1.92.2` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00001-00092-00002/1 | `T-S Misc.1.92` | 24090105 | 3701317 |  |  | 52 |
| `tsmisc1.131.1` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00001-00131-00001/1 | `T-S Misc.1.131` | 24133105 | 3701317 |  |  | 52 |
| `tsmisc1.131.1` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00001-00131-00001/1 | `T-S Misc.1.131(1)` | 24134105 | 3701317 |  |  | 52 |
| `tsmisc1.131.1` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00001-00131-00001/1 | `T-S Misc.35.1` | 1156105 | 1214401 |  |  | 51 |
| `tsmisc13.31` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00013-00003-00001/1 | `AIU: CUL: T-S Misc.31.13` | 4646099 | 776808814 |  |  | 52 |
| `tsmisc13.31` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00013-00003-00001/1 | `AIU: CUL: T-S Misc.31.13` | 4647099 |  |  |  | 52 |
| `tsmisc13.31` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00013-00003-00001/1 | `AIU: CUL: T-S Misc.31.13` | 4648099 |  |  |  | 52 |
| `tsmisc13.32` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00013-00003-00002/1 | `T-S Misc.34.13` | 1097105 | 3818063 |  |  | 51 |
| `tsmisc13.32` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00013-00003-00002/1 | `T-S Misc.34.32` | 1116105 | 3818063 |  |  | 51 |
| `tsmisc13.32` | https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00013-00003-00002/1 | `T-S Misc.35.13` | 1168105 | 11902366 |  |  | 51 |

**Proposed FIST->CUDL normalizer rule (concrete -- Pass 2 HIGH-5; do not author regex; review and adjudicate):**

```python
# Rule: T-S Misc multi-segment patterns
# Rule name (for test): tsmisc_multi_segment
# Direction: FIST.Shelfmark -> add CUDL key(s) to fist_to_cudl_keys' output
# FIST regex on dbo_Inventory.Shelfmark: r"^T-S Misc (\d+)\.(\d+)\.(\d+)$"
# Resulting CUDL key template:        tsmisc{a}.{b}.{c}
#
# Implementation sketch -- add this branch inside fist_to_cudl_keys, AFTER the
# existing 4 D-02a branches, gated to the appropriate family prefix:
#   import re
#   _RULE_RE = re.compile(r"^T-S Misc (\d+)\.(\d+)\.(\d+)$")
#   m = _RULE_RE.match(c)
#   if m:
#       # construct CUDL key per cudl_key_template using m.group(...)
#       keys.add(constructed_cudl_key)
```

**Supporting FIST.Shelfmark fixtures (proposed rule succeeds -- Pass 2 HIGH-5):**
- `T-S Misc 1.131.1   -> adds tsmisc1.131.1`
- `T-S Misc 24.137.21 -> adds tsmisc24.137.21`
- `T-S Misc 5.50.3    -> adds tsmisc5.50.3`

**Refuting FIST.Shelfmark fixture (proposed rule must NOT apply -- Pass 2 HIGH-5):**
- `T-S Misc 1.131     (2 segments -- different family; must NOT match this rule)`

**Test scaffold (Pass 2 HIGH-5 -- executor will instantiate this):**

```python
# tests/test_fist_cudl_bridge.py -- add inside TestFistToCudlKeys
def test_accepted_rule_tsmisc_multi_segment_fist_to_cudl(self):
    """D-02c accepted rule: FIST->CUDL direction."""
    # Positive: supporting FIST input should ADD the expected CUDL key.
    keys = fist_to_cudl_keys('T-S Misc 1.131.1')
    assert 'tsmisc1.131.1' in keys, f"missing key: {keys}"

def test_refute_rule_tsmisc_multi_segment_fist_to_cudl(self):
    """D-02c accepted rule: refuting fixture must NOT trigger."""
    keys = fist_to_cudl_keys('T-S Misc 1.131')
    # The unexpected flat/collapsed alias from this rule must NOT appear:
    # (adjust the asserted-absent key when integrating the actual rule.)
    assert True, 'replace with: <expected-absent-key> not in keys'
```

**False-positive risk:** Low risk: T-S Misc multi-segment is distinctive.

**User decision:** [ ] Accept rule  [ ] Reject  [ ] Spot-check more (Deferred -- see stop rule)

**Rejection rationale (if Rejected):**

```
# Fill in: why this family is genuinely residual, not encoding gap.
```

---

## After Adjudication

Accepted rules: Phase 86 Plan 03 Task 3 integrates them into `shared/fist_cudl_bridge.py::fist_to_cudl_keys` as FIST->CUDL branches (Pass 2 HIGH-5) with matching unit tests named `test_accepted_rule_<rule_name>_fist_to_cudl` and `test_refute_rule_<rule_name>_fist_to_cudl` in `tests/test_fist_cudl_bridge.py`. Plan 04 then re-runs `python scripts/generate_synthetic_rows.py --apply`.

Rejected and Deferred rules: documented in this artifact (preserved) and referenced in `reports/cudl_coverage.md` (Plan 04) under 'Residue Pattern Adjudication' so future maintainers know they were evaluated and excluded by design.

Stop rule (Pass 2 LOW Codex): no implicit re-iteration; `Spot-check more` stays Deferred until explicit user request and separate plan revision.

