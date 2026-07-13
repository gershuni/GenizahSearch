# -*- coding: utf-8 -*-
"""Score the grader against Hillel's held-out 100. The AI layer is NOT run on
these (no leakage), so this validates the AUTOMATIC RULE tier + discovery
safety, post-stratified to a corpus estimate. Residual (rule=None) = routed to
AI+human review (correct destination for discovery candidates)."""
import json
from collections import Counter, defaultdict

PROBE = r'C:\Genizahsearch\same_work_spike\probe'
human = {h['no']: h['grade'] for h in json.load(open(
    PROBE + r'\review\full_deck\mapv2_validation_100_human.json', encoding='utf-8'))}
man = json.load(open(PROBE + r'\data\validation_100_manifest.json', encoding='utf-8'))
cards = {c['no']: c for c in man['cards']}
frame_cells = man['meta']['frame_cells']
samp = Counter(c['_cell'] for c in man['cards'])

def w(no):
    cell = cards[no]['_cell']
    return frame_cells.get(cell, 0) / max(1, samp[cell])

graded = {n: g for n, g in human.items() if g}
print(f"graded: {len(graded)}/100")

# --- rule tier (auto-labeled cards) ---
fire = [(n, cards[n]['_rule_pred'], graded[n]) for n in graded
        if cards[n]['_rule_pred']]
res = [n for n in graded if not cards[n]['_rule_pred']]
correct = sum(1 for n, p, g in fire if p == g)
print(f"\nRULE tier auto-labels {len(fire)}/{len(graded)} graded cards; "
      f"agreement with Hillel {correct}/{len(fire)} "
      f"({100*correct//max(1,len(fire))}%)")
print("residual (rule=None -> AI+human):", len(res))

# per predicted class precision
byp = defaultdict(lambda: [0, 0])
for n, p, g in fire:
    byp[p][1] += 1
    if p == g:
        byp[p][0] += 1
print("\nrule precision by predicted class (auto-accepted):")
for p, (ok, tot) in sorted(byp.items()):
    ex = Counter(g for n, pp, g in fire if pp == p and g != p)
    print(f"  {p:9s} {ok}/{tot} ({100*ok//tot}%)"
          + (f"   misses: {dict(ex)}" if ex else ""))

# --- discovery safety: did the rule auto-bury any of Hillel's discoveries? ---
h_disc = [n for n, g in graded.items() if g == 'discovery']
buried = [(n, cards[n]['_rule_pred']) for n in h_disc if cards[n]['_rule_pred']]
print(f"\nDISCOVERY SAFETY: Hillel discoveries {len(h_disc)}; "
      f"auto-labeled by rule (buried) {len(buried)}: {buried}")
routed = [n for n in h_disc if not cards[n]['_rule_pred']]
print(f"  correctly routed to review (rule=None): {len(routed)}/{len(h_disc)}")

# --- post-stratified corpus estimate ---
W = sum(w(n) for n in graded)
w_fire = sum(w(n) for n, p, g in fire)
w_correct = sum(w(n) for n, p, g in fire if p == g)
print(f"\n=== post-stratified CORPUS estimate ===")
print(f"auto-labelable (rule fires): {100*w_fire/W:.0f}% of matches")
print(f"rule precision (weighted): {100*w_correct/max(1e-9,w_fire):.0f}%")
w_disc = sum(w(n) for n in h_disc)
w_buried = sum(w(n) for n in buried and [b[0] for b in buried])
print(f"est. discoveries auto-buried (weighted): "
      f"{100*w_buried/max(1e-9,w_disc):.1f}% of discoveries")

# --- Hillel grade distribution ---
print("\nHillel grade distribution:", dict(Counter(graded.values())))
