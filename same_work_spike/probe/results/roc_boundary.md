# Acceptance boundary calibration (2026-07-06)

Length bands: [(25, 60), (60, 100), (100, 200), (200, 400), (400, 1000000000)]

## literary_q95
thresholds per band: [25-60): 0.3, [60-100): 0.3, [100-200): 0.405, [200-400): 0.405, [400-inf): 0.405
- accepted_by_class: {'tier1_titles': 1813, 'cross': 5309, 'duplicate': 197, 'related_new': 2703, 'tier1_joins': 36, 'tier1_bh': 112}
- bh_witnesses_connected: 248
- recall_tier1_titles: 0.9805
- recall_tier1_joins: 1.0
- recall_tier1_bh: 0.9573

## combined_q95
thresholds per band: [25-60): 0.3, [60-100): 0.3, [100-200): 0.396, [200-400): 0.396, [400-inf): 0.396
- accepted_by_class: {'tier1_titles': 1808, 'cross': 4889, 'duplicate': 197, 'related_new': 2396, 'tier1_joins': 36, 'tier1_bh': 112}
- bh_witnesses_connected: 240
- recall_tier1_titles: 0.9778
- recall_tier1_joins: 1.0
- recall_tier1_bh: 0.9573

## liturgy_q95
thresholds per band: [25-60): 0.3, [60-100): 0.3, [100-200): 0.386, [200-400): 0.418, [400-inf): 0.418
- accepted_by_class: {'tier1_titles': 1820, 'cross': 4851, 'duplicate': 197, 'related_new': 2466, 'tier1_joins': 36, 'tier1_bh': 114}
- bh_witnesses_connected: 241
- recall_tier1_titles: 0.9843
- recall_tier1_joins: 1.0
- recall_tier1_bh: 0.9744

Notes: 'cross' accepted pairs are POTENTIAL FPs but include canonical shares and genuine discoveries (the ona'ah find was cross) — the 200-pair graded sampling turns this into real precision. 'duplicate' (density<=0.02) is removed by stage-0. BH connectivity counts witnesses touched by any accepted pair between two BH-witness sys_ids.