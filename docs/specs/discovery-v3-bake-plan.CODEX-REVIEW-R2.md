# Codex review — discovery-v3 bake, round 2

## Round-1 disposition

| Round-1 finding | Disposition | Verification |
|---|---|---|
| BLOCKER 1 — work-side offsets | **NOT CLOSED** | The plan now correctly concedes that the scalar ingest has no defined choice, but neither the projection nor its parity test exists. Deferring a human-readable locus is coherent only after a deterministic paired raw-offset projection is implemented and persisted. Gate 14 says to test a known multi-span row, but does not specify the selection rule, tie-break, or required source/evidence relation; it remains a placeholder. |
| BLOCKER 2 — router semantics | **NOT CLOSED** | `v3_routing_ingest.py` is a standalone reader. `build_discovery_sidecar.py` neither imports nor calls it; `_ingest_tier_a` still marks every Tier-A row shipped and `build_claims_and_evidence` still invokes `apply_lever1_coverage` when D-17 is enabled. The stated router ingest therefore has no effect. |
| BLOCKER 3 — novelty cache reuse | **NOT CLOSED** | No per-pair input fingerprint is implemented in the cache producer or consumer. `load_novelty_verdicts` still accepts the old grain-keyed entries after a whole-file cache hash check. The §5.0 promise is engineering owed, not a closure. |
| HIGH — research read surface | **PARTIALLY CLOSED** | The actual reader surface is now correctly identifiable, and `source_corpus` is rightly absent. But the slim DB does not carry *exactly* that surface: it retains five unused `pages` fields. |
| HIGH — 52-work classification | **PARTIALLY CLOSED** | The strict-subset conclusion supports “no new matcher population” on that narrow ground, but applying today's policy to missing ids still does not prove why those ids were historically absent from the crosswalk. |
| HIGH — MAPV2-8 decision | **PARTIALLY CLOSED** | §5.0 consistently names the 595-row exclusion and labels owner confirmation owed. That is a much better executable decision, but it is not closed until that confirmation is recorded; §8's “nothing blocking” language weakens that boundary. |
| HIGH — `shadowed_by` grain | **PARTIALLY CLOSED** | The producer-unit mixed-state halt is correctly added, but the subsequent reduction to `(page_id, ref_work)` is not validated and can silently overwrite a conflicting unit. |
| HIGH — R-source containment | **PARTIALLY CLOSED** | The slim-builder filter and post-build scan are real. The required pre-build/review consumer gate and source-table fingerprint are absent, leaving a direct alternate research DB path open. |
| MEDIUM — gates and masking attestation | **NOT CLOSED** | The plan has useful failure demonstrations, but the non-disclosing pattern-set attestation and real-pattern positive control are still explicitly owed. A synthetic self-test cannot attest completeness. |
| MEDIUM/LOW — naming and staleness | **PARTIALLY CLOSED** | `discovery-v3` versus compatibility `discovery-v1` is coherent and §5.0 is the operative table. Owner acknowledgement remains outstanding, and the router/D-17 order is intentionally left to be “re-derived” without an executable order or rule. |

## Findings

### BLOCKER — `docs/specs/discovery-v3-bake-plan.md` §5.0 decision 6 and `scripts/build_discovery_sidecar.py::_ingest_tier_a`: the offset delivery is still only a promise

Decision 6 claims that a declared page-span→reference-span projection and multi-span parity gate will deliver Stage-1 raw work-side offsets. The only implemented Tier-A projection remains `_largest_track1_span(spans_json)`, which reads page-side spans and writes no work-side coordinate at all. There is no declared relation from that chosen span to a reference span, no stored `norm_stream` work offsets, and no test against producer evidence. A non-NULL-only gate would merely certify an arbitrary scalar value.

Shipping raw offsets while deferring their human-readable locus is technically coherent, but only after the pairing rule is specified and its result is retained. Required before execution: define the source records and deterministic selection/tie-break rule, retain the selected work-side offsets with their coordinate space, halt or explicitly preserve multiplicity where projection is not one-to-one, and test known multi-span input against the producer's paired evidence.

### BLOCKER — `scripts/build_discovery_sidecar.py`, `scripts/v3_routing_ingest.py`, and plan §5.0 decision 1: the claimed router closure is not wired and cannot currently be persisted

`v3_routing_ingest.py` is never imported outside its tests. `_ingest_tier_a` still emits every row as a shipped witness, and `build_claims_and_evidence` applies the legacy 0.45 Lever-1 routine before D-17 whenever `run_d17` is set. Thus the measured demotions still happen and the standalone parity JSON is decoration.

The proposed mapping also cannot be integrated as written. Its two new reasons are not members of the builder schema's `routing_reason` CHECK constraint or `scripts/discovery_ids.py::ROUTING_REASONS`; inserting either mapped router result would fail. Even if those vocabularies were extended, `parallel` remains `evidence_kind='witness'`, so assembly derives a witness relation from span dominance. The service and panel admit it because its `routing_status` is `shipped` and render the relation chip from `claim_type`, not `routing_reason`. A quotation would consequently appear as a direct witness and could enter the main pool as same-work evidence.

The implementation must pass a verified router into Tier-A ingestion, halt on every undecided match pair, disable legacy Lever-1 for that population, represent the quotation relation in the actual claim/evidence semantics, and test the emitted sidecar and panel-facing row. It must also declare the router→D-17 order before execution; “re-derive” is not an order.

### BLOCKER — `docs/specs/discovery-v3-bake-plan.md` §5.0 decisions 3–4, `scripts/discovery_novelty_production_run.py`, and `scripts/build_discovery_sidecar.py::load_novelty_verdicts`: the cache-reuse closure has no implementation

Decision 3 claims a per-pair fingerprint over every rendered prompt field plus alias-group and model/prompt/effort hashes. The production run still keys verdicts by `(sys_id, ref_work_id)`, and the consumer accepts its frozen entry shape after checking only a whole-file cache SHA-256. Neither side stores or compares an input fingerprint. Consequently the cache can still answer a changed title, author, alias-group, or finding-aid question as a hit.

Treating an unfingerprinted record as a miss is the correct default, but it is not sufficient by itself: persist the exact normalized fingerprint on each entry and pin the fingerprint format/version and its input artifacts in run/build metadata. Then add the promised mutation test showing that a changed title becomes a miss. Until that exists, both the reuse percentage and the ≈$4 cost figure are obsolete rather than validated.

### HIGH — `scripts/v3_routing_ingest.py::load_router` and its tests: the parity gate is not a parity gate over an emitted result

`load_router` ignores the `shipped` value it reads and accepts duplicate `(page_id, canonical_work_id)` rows when their surfaces happen to agree. Such duplicates inflate `counts` while replacing the dict entry, so the report is neither at a unique router grain nor proof that surface and shipped state agree. The only “parity” test exercises `resolve_routing` in isolation; no test stages match rows, invokes the builder, or compares emitted routing/claim semantics with router decisions. Gate 10's claim of exact reproduction is therefore unproved.

### HIGH — `scripts/v3_build_research_db.py::derive_shadowed_by`: producer-grain checking is followed by an unchecked, lossy reduction

The code correctly groups evidence by `(claim_id, ref_work)` and halts a mixed unit. It then writes a single dictionary entry keyed by `(page_id, ref_work)` with no assertion that only one producer unit maps to that key, or that all mapped units have the same wholly-shadowed state and value. A wholly-unshadowed unit plus a wholly-shadowed one on the same page/work is not “mixed” within either unit; the latter silently determines the match-row value. Two shadowed units with different values are likewise last-write-wins.

The plan's claimed safety of keying after aggregation is therefore false. Halt on any many-unit page/work reduction that does not have a declared identical outcome, and add a fixture with two claim ids sharing that page/work. The present tests only cover one producer unit per output key.

### HIGH — `scripts/build_discovery_sidecar.py` and plan gate 12: R-source containment is not enforced at the consumer boundary

The slim builder drops and post-scans the prefix, but `_connect_research_ro`, `select_shown_works`, the review-only path, and the `--from-approved` path perform no prefix scan or source-table/content-fingerprint check. The latter two build candidate selection from whatever database path the operator supplies. A different research DB with rows that pass the ordinary category/genre policy can therefore reach a review artifact or sidecar without invoking `v3_build_research_db.py` at all.

The filtering code is useful defense in depth, not the required “before every build and review-artifact invocation” gate. Put the fail-closed assertion and recorded source identity at the consumer entrypoint (or a mandatory v3 driver used by all entrypoints), and test direct invocation against a planted restricted-prefix row.

### HIGH — `docs/specs/discovery-v3-bake-plan.md` §5.0 and `scripts/v3_build_research_db.py`: the frozen Tier-A count has not been shown to be deliberately pinned

The equality between the emitted slim DB's count and `_EXPECTED_TIER_A_ROWS` proves only that this transformation currently produces that number. The constant has an older generic comment and no recorded source-table fingerprint, query artifact, or provenance linking its origin to this population. `build()` returns a statistic but records no identity in the DB or manifest, and no test protects the asserted derivation. Exact numerical agreement is not evidence of causation.

Treat the count as an unproven coincidence until the expected population, source identity, derivation query, and hashes are recorded before the build. Do not change the expected value merely to make a later run pass.

### MEDIUM — `scripts/v3_bake_state.py`: the state ledger is safe for one writer, not for two processes sharing it

Atomic replace protects readers from a torn individual write, and recording only after a callback returns gives a sound single-process retry contract. It has no writer lock or compare-and-swap, however: two instances can load the same JSON, independently add different completed steps, and the later replace loses the other step. More directly, construction sweeps every matching temp name; on platforms permitting unlink of an open temp file, a second process can delete a live first writer's temp before its `os.replace`, causing that writer to fail. The sweep test only plants dead files and does not cover this race.

Either take an exclusive per-state lock for the process lifetime and fail a second launcher loudly, or document and enforce a single-process run directory with temp ownership/age checks. Also fsync the parent directory after replacement where supported if the stated power-loss durability guarantee is required.

### MEDIUM — `scripts/v3_build_research_db.py` and `scripts/build_discovery_sidecar.py`: the read-surface claim is close but not exact, and the test does not cover the real paths

The full consumer read set is:

- `track1_matches`: `page_id`, `sys_id`, `work_id`, `cat`, `genre`, `author`, `title`, `matched_letters`, `best_density`, `n_spans`, `spans_json`, and `shadowed_by`.
- `pages`: `page_id`, `provenance`, `text`, and `n_chars`.

The review-only path exercises selection, Tier-A assembly, and `PageTextIndex`; the approved real path additionally performs the release count and corpus HTR snapshot. No other research table is read. The slim DB correctly has all required fields and correctly drops `source_corpus`, but it also copies unused page fields. More importantly, `test_the_builders_own_reader_accepts_the_slim_db` calls only `select_shown_works`, not `_ingest_tier_a`, `PageTextIndex`, `_compute_htr_snapshot_hash`, review emission, or the approved path. It cannot establish the advertised compatibility.

### MEDIUM — plan §3.1 retraction and §5.0 decision 2: the 52-work result is a current-policy partition, not a historical-cause proof

The quoted counts are consistent with applying the current `cat`/genre selector, and the reported strict set inclusion is sufficient to withdraw the particular claim of a newly expanded matcher population. It does not establish that the 2,686 lacked crosswalk entries *because* of the historical D-05/D-06 decision. The selector deliberately chooses one representative occurrence, while category and genre are occurrence-level fields; it also is not an audit of crosswalk creation or owner approval history.

Call the result “current-policy drops” unless the historical provenance is audited. The decision to honour that policy may be valid, but the phrase “the real gap” overstates what has been proved, and the zero-shipped figure must be recomputed under the router actually shipped.

### MEDIUM — plan §5.0 decisions 3–4 and `scripts/discovery_novelty_production_run.py`: novelty economics remain unsubstantiated

The plan correctly says a prompt fingerprint can only reduce reuse, so the ≈$4 headline is no longer an operative estimate. `render_case` includes title, author, and assembled finding-aid text; the existing verdict key does not preserve a fingerprint of those rendered inputs. “Unfingerprinted means miss” is necessary but insufficient for reproducibility: each reusable entry needs its exact normalized-input fingerprint, and the run/build metadata must pin the fingerprint version, alias-group artifact, model/prompt/effort, and source input identities. No code, schema, or mutation test implements the title-change miss promised by gate 13.

### MEDIUM — plan §6 masking gate: the required attestation is still missing

The unset-pattern control and synthetic self-test demonstrate only scanner mechanics. The plan itself says a non-disclosing pattern-set attestation, paths/hashes scanned, and a real-pattern positive control are owed. The column denylist is a useful local compensating control, but it does not attest the broader pattern set or scan coverage. This finding remains open.

## Test assessment

I did not find a literally un-failable test among the three new test files. Several important tests are nevertheless insufficient: the router suite never reaches the builder/schema/panel; the research-DB reader test never reaches actual Tier-A ingestion or either requested build mode; the shadow suite omits the page/work collision; and the ledger suite omits concurrent writers. All can pass with the defects above.

The targeted suites could not be executed in this checkout because both available Python launchers point to unavailable interpreter installations. The findings above are static, source-grounded review results.

VERDICT: CHANGES-REQUIRED
