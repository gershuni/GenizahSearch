# -*- coding: utf-8 -*-
"""Derive the V0.8 cross-manuscript repair manifest from the corpus itself.

Background: .planning/debug/v08-cross-manuscript-misattribution.md and
_tmp/V08-REPAIR-SPEC.md. Some V0.8 records carry ANOTHER manuscript's folio text,
appended and duplicated. Production is V0.8-only and V0.7 is never published, so
V0.7 serves only as the MAP that says which manuscript entity the block belongs
to; the text that gets re-filed is the V0.8 text that already exists in the
corpus, merely under the wrong record id.

This script does not edit anything. It reads Config.FILE_V8, re-derives each
record's decomposition, and writes a manifest that apply_v08_repair.py consumes.
Every entry carries a SHA-256 of the exact original record body, so the patch
no-ops loudly if the corpus is ever refreshed.

    python scripts/build_v08_repair_manifest.py
"""
import hashlib
import io
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from shared.config import Config  # noqa: E402

OUT = os.path.join(ROOT, "data", "v08_repair_manifest.json")
HEADER_RE = re.compile(r"^==> (\S+) <==\s*$")
WORD_RE = re.compile(r"[א-ת][א-ת֑-ׇ]*")
NIKUD_RE = re.compile(r"[֑-ׇ]")

# Actions, per _tmp/V08-REPAIR-SPEC.md §1 reconciled with the owner's 2026-08-14
# ruling that the harm to fix is "points at the wrong manuscript entity":
#   refile  - victim has NO V0.8 record; strip the block from the host AND re-file
#             it under the victim's own pre-existing V0.7 (IE,P,FL). Fixes the
#             wrong-entity harm with no loss of searchable text.
#   strip   - victim ALREADY has this text correctly in its own V0.8 record, so
#             removing the foreign copy loses nothing.
#   dedupe  - owner unknown; collapse the duplicate to one copy, assert nothing.
# Cases whose victim is known but whose filing is blocked (Or.12186 binder audit,
# AIU IE tie) are deliberately ABSENT: stripping them now would delete text we
# know belongs somewhere, trading a wrong pointer for no pointer.
PLAN = [
    # --- re-file: victim absent from V0.8 -------------------------------------
    {"record": "990026373060205171_IE208678903_P000002_FL208678913",
     "action": "refile", "victim_record": "990026373340205171_IE208678933_P000001_FL208678935",
     "note": "Strasbourg Ms. 4038 -> f.9 (idf 0.765, 45 rare shared)"},
    {"record": "990043939960205171_IE61676826_P000002_FL61676829",
     "action": "refile", "victim_record": "990043940120205171_IE61676800_P000001_FL61676802",
     "note": "Heidelberg Hebr. 18 -> Hebr. 19 (owner-reported case; idf 0.396)"},
    {"record": "990026964730205171_IE169327459_P000002_FL169327462",
     "action": "refile", "victim_record": "990026964360205171_IE169327449_P000001_FL169327451",
     "note": "Lehnardt 7b/5 -> 7b/4 (victim wholly absent from V0.8)"},
    # --- strip: victim already holds this text in its own V0.8 record ----------
    {"record": "990001990000205171_IE212432447_P000002_FL212432453",
     "action": "strip", "note": "Gaster/BL Or.10578P f.3; leaf already correct under 990053530800205171"},
    {"record": "990026137470205171_IE208648888_P000004_FL208648893",
     "action": "strip", "note": "Halpern 33 -> 38, which already has its own correct V0.8"},
    {"record": "990026118580205171_IE78471216_P000005_FL78471226",
     "action": "strip", "note": "Weiss Ms. 3 -> Or.12186.3, which already has its own correct V0.8"},
    {"record": "990026120480205171_IE212419653_P000006_FL212419660",
     "action": "strip", "note": "Halpern 3|4|5|6 -> 1|2, which already carries the passage"},
    # --- dedupe: owner unknown, keep the text, remove only the duplication -----
    {"record": "990043934840205171_IE61676768_P000012_FL61676781",
     "action": "dedupe", "note": "Heid. Hebr. 4 bundle; unattributed - do not delete, do not reattribute"},
]


def tokens(text):
    return [(NIKUD_RE.sub("", m.group(0)), m.start(), m.end())
            for m in WORD_RE.finditer(text) if NIKUD_RE.sub("", m.group(0))]


def longest_repeat(toks, n=20):
    words = [t[0] for t in toks]
    seen, best = {}, None
    for i in range(len(words) - n + 1):
        key = tuple(words[i:i + n])
        first = seen.setdefault(key, i)
        if first == i or i - first < n:
            continue
        length = n
        while (i + length < len(words) and first + length < i
               and words[first + length] == words[i + length]):
            length += 1
        if best is None or length > best[2]:
            best = (first, i, length)
    return best


def read_records(path, wanted):
    out, rid, buf = {}, None, []
    with io.open(path, "r", encoding="utf-8", errors="replace", newline="") as fh:
        for line in fh:
            m = HEADER_RE.match(line)
            if not m:
                if rid is not None:
                    buf.append(line)
                continue
            if rid in wanted:
                out[rid] = "".join(buf)
            rid, buf = m.group(1), []
    if rid in wanted:
        out[rid] = "".join(buf)
    return out


def main():
    wanted = {p["record"] for p in PLAN}
    victims = {p["victim_record"] for p in PLAN if p.get("victim_record")}
    print("reading %s" % Config.FILE_V8)
    bodies = read_records(Config.FILE_V8, wanted | victims)

    missing = wanted - set(bodies)
    if missing:
        raise SystemExit("records not found in the corpus: %s" % sorted(missing))
    present_victims = victims & set(bodies)
    if present_victims:
        raise SystemExit("re-file target ALREADY EXISTS in V0.8 (would duplicate): %s"
                         % sorted(present_victims))
    print("all %d host records found; all %d re-file targets confirmed absent from V0.8\n"
          % (len(wanted), len(victims)))

    entries = []
    for p in PLAN:
        body = bodies[p["record"]]
        toks = tokens(body)
        rep = longest_repeat(toks)
        if not rep:
            raise SystemExit("no repeat re-derived for %s" % p["record"])
        first, second, ln = rep
        adjacent = (second - first) == ln
        if not adjacent:
            raise SystemExit("%s is not back-to-back on re-derivation" % p["record"])

        blk_start = toks[second][1]                       # char offset of copy 2
        blk_end = toks[second + ln - 1][2]                # end of copy 2
        one_start = toks[first][1]                        # char offset of copy 1
        block_text = body[one_start:blk_end]              # both copies + any glue

        if p["action"] == "dedupe":
            # keep everything, but collapse the two copies into one
            kept = body[:blk_start] + body[blk_end:]
        else:
            # strip BOTH copies of the foreign block; keep the host's own text
            kept = body[:one_start] + body[blk_end:]

        e = {
            "record_id": p["record"],
            "action": p["action"],
            "note": p["note"],
            "original_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
            "original_chars": len(body),
            "original_tokens": len(toks),
            "repeat_tokens": ln,
            "keep_chars": len(kept),
            "kept_body": kept,
        }
        if p["action"] == "refile":
            e["victim_record_id"] = p["victim_record"]
            # the re-filed content is ONE copy of the block: V0.8 text that already
            # exists in the corpus, moved to the record id it belongs under.
            e["victim_body"] = body[one_start:toks[first + ln - 1][2]].strip() + "\n"
            e["victim_sha256"] = hashlib.sha256(e["victim_body"].encode("utf-8")).hexdigest()
        entries.append(e)

        print("%-52s %-7s %4d tok -> %4d chars kept%s"
              % (p["record"][:52], p["action"], len(toks), len(kept),
                 ("  + re-file %d chars" % len(e.get("victim_body", ""))) if p["action"] == "refile" else ""))

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with io.open(OUT, "w", encoding="utf-8") as fh:
        json.dump({
            "schema": 1,
            "generated_from": os.path.basename(Config.FILE_V8),
            "source_note": "See .planning/debug/v08-cross-manuscript-misattribution.md. "
                           "V0.7 is the identification map only; no V0.7 text is published.",
            "entries": entries,
        }, fh, ensure_ascii=False, indent=1)
    print("\nwrote %s (%d entries)" % (OUT, len(entries)))


if __name__ == "__main__":
    main()
