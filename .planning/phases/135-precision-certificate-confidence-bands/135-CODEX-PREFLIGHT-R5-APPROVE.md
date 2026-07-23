## Section A: Round-4 outstanding items

1. **RESOLVED — composition-date normalizer grammar.**  
   `135-04-PLAN.md:Task 1` freezes the JSON mapping, three accepted categories, normalization rules, 500–1600 CE bound, and hard rejection behavior. `135-06-PLAN.md:Tasks 2–3` requires category coverage and near-miss/out-of-range tests. `135-VALIDATION.md:135-06-02/03` makes these executable gates.

2. **RESOLVED — preregistration crosswalk hash.**  
   `135-09-PLAN.md:Task 1` includes `crosswalk_sha256` in `cert01_prereg.json` and therefore in `report_id`. `135-09-PLAN.md:Task 3`, check 12, independently compares it with deployed `meta`. `135-VALIDATION.md:135-09-01/03` verifies both presence and pinning.

3. **RESOLVED — reband versus frozen evidence IDs and display precedence.**  
   `135-04-PLAN.md:Task 1` defines rebanding as a rebuild input before evidence-ID generation and display selection. `135-06-PLAN.md:Task 2` requires regenerated IDs and recomputed display pointers, including a competing shipped sibling fixture. `135-06-PLAN.md:Task 3` independently recomputes every frozen evidence-ID tuple and every display pointer. `135-VALIDATION.md:135-06-02/03 and 135-07-01` carry these through unit and real-asset gates.

## Section B: New findings

No genuinely new BLOCKER or HIGH findings.

VERDICT: APPROVE