# Track-1 v2 — tier A/B identification ('smoke', 139,694 pages)

- reference: 5,421 works (v2, header-fix + REF-2), canonical masks: 522 works
- WIDE verification cutoff: 0.55; production boundary unchanged (0.28/<100, 0.35)
- **tier A rows: 78,875** on 45,155 pages (track1_matches — census path, shadowing downstream)
- **tier B rows: 657,205** on 58,669 pages (track1_candidates — P-stamped, census never reads)
- tier-B storage floor P >= 0.05; dropped-below-floor counts (NO silent caps): {'tierB_dropped_singleton': 14244, 'tierB_dropped_not_best': 95766, 'tierB_dropped_m_0_003': 4257, 'tierB_dropped_m_003_010': 1808, 'tierB_dropped_m_ge_010': 462}
- margin-band histogram (stored tier B): {'m_003_010': 26492, 'm_0_003': 36373, 'm_ge_010': 5919, 'not_best': 552755, 'singleton': 35666}
- P histogram (stored tier B): {0.1: 160416, 0.2: 33173, 0.3: 39740, 0.4: 15439, 0.5: 51692, 0.6: 28158, 0.7: 61776, 0.8: 82642, 0.9: 163928, 1.0: 20241}
- pages merge-flagged (excluded from tier A): 0; weak two-work flags (tier A kept): 5,400
- engine: hits 1,589,242,472, hull candidates 118,644,811, rej_short 0, rej_wide 117,306,380
- total 33 min