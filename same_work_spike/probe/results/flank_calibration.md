# Flank detector calibration + held-out validation (MAPV2-15m)

## DEV (127 graded, targets cont/island/abstain)
- FROZEN: cont_thr=0.4, island_thr=0.56
- **false STRONG-demote (Codex constraint <=1): 1**; total false-demote (incl. mild x0.75 weak) 2
- citation recall: 0% of 42 island-target cards; citation precision 0%
- **rescue of the naive-island-buried must-not-demote cards: 47/49**

### grid search (cont, island -> false_demote, cit_recall, cit_prec)

- (0.4,0.56): fd=2 recall=0% prec=0% *
- (0.4,0.58): fd=2 recall=0% prec=0%
- (0.4,0.62): fd=2 recall=0% prec=0%
- (0.42,0.56): fd=2 recall=0% prec=0%
- (0.42,0.58): fd=2 recall=0% prec=0%
- (0.42,0.62): fd=2 recall=0% prec=0%
- (0.45,0.56): fd=2 recall=0% prec=0%
- (0.45,0.58): fd=2 recall=0% prec=0%
- (0.45,0.62): fd=2 recall=0% prec=0%
- (0.48,0.56): fd=2 recall=0% prec=0%
- (0.48,0.58): fd=2 recall=0% prec=0%
- (0.48,0.62): fd=2 recall=0% prec=0%

## HELD-OUT 100 (run ONCE at frozen thresholds)
- unweighted: false-demote 1, citation recall 15%/54, precision 89%, abstain 63/100
- **post-stratified same-work false-demotion rate: 1.1%** of must-not-demote mass

(advisory multipliers only; every survivor is human-reviewed.)
