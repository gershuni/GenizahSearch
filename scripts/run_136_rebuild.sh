#!/usr/bin/env bash
# Phase 136 — the ONE authorized rebuild (plan 136-13, Task 1).
#
# Inputs are pinned exactly as re-verified against the LIVE v2 asset's own `meta`
# table on 2026-08-03. `--canonical-merges` names the .build.json SLIM projection,
# not the census: see the 2026-08-03 amendment in docs/specs/discovery-deploy.md.
set -euo pipefail

export PYTHONUTF8=1
export MASKING_SCAN_PATTERNS_FILE=C:/Genizahsearch/.masking_patterns

PHASE=.planning/phases/136-read-surfaces-connections-panel-work-witnesses
OUT=discovery_data/discovery-v1-136rebuild.db

rm -f "$OUT"

python scripts/build_discovery_sidecar.py \
    same_work_spike/probe/data/fullcorpus_v2.db \
    --from-approved                     discovery_data/discovery-review-approved-final.csv \
    --crosswalk                         discovery_data/crosswalk.json \
    --canonical-merges                  same_work_spike/probe/rsource/data/v2_canonical_merges.build.json \
    --canonical-merges-sha256           cc054d111b9b4a76dd69912923ba50cd2b63f7820cb632617f645c12c207429a \
    --composition-dates                 discovery_data/composition_dates.json \
    --composition-dates-sha256          2b46b4708ddccb9f26961dcb9ba6d62b23d64cc1da225d133af1be21bf2e9476 \
    --seftja-dates                      same_work_spike/probe/rsource/data/seftja_dates.json \
    --seftja-dates-sha256               0076028917c60044ac72ee36504c173b9e6decd0a5aef9890ec0f0fe934b22d7 \
    --research-data-dir                 same_work_spike/probe/data \
    --libraries-csv                     libraries.csv \
    --fjms-db                           fist_data/fjms_enrichment.db \
    --novelty-verdicts                  discovery_data/novelty_production_verdicts.json \
    --novelty-verdicts-sha256           eb6fc4f88c059bb383b3541123c14227587a614e0e501406e96c29229322c413 \
    --work-domains                      discovery_data/work_domains-v1.json \
    --work-domains-content-hash         sha256:573937731e2e31f4ad3fccd6f84aadecc7e67210bf4cda82513dfc5c4d94f605 \
    --work-author-aliases               discovery_data/work_author_aliases-v1.json \
    --work-author-aliases-content-hash  sha256:acce47f67dcde456eb477fc092294ee42546963f5d977549f53e635da65f8a64 \
    --precision-spec                    "$PHASE/136-PRECISION-SPEC.json" \
    --out                               "$OUT" \
    --release
