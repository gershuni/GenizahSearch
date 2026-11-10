# -*- coding: utf-8 -*-
"""Apply (or verify) the V0.8 cross-manuscript repair against the corpus file.

The corpus file is the only choke point that reaches all three independent
readers of it — shared/indexer.py::create_index, shared/lab_engine.py::
rebuild_lab_index, and shared/browse_map_utils.py::_repair_missing_ie_pages —
so the repair is applied to the data, not wired into one consumer.

Modes:
    --verify      (default) hash-check every manifest entry against the live
                  corpus and report; writes nothing.
    --apply OUT   stream the corpus to OUT, applying the repair.
    --check FILE  assert FILE carries the repaired state (the gate).

Every entry is guarded by a SHA-256 of the exact original record body. If the
corpus is refreshed and a record no longer matches, that entry is SKIPPED and
reported loudly — the patch never blind-fires onto changed upstream data.
"""
import argparse
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

MANIFEST = os.path.join(ROOT, "data", "v08_repair_manifest.json")
HEADER_RE = re.compile(r"^==> (\S+) <==\s*$")


def load():
    with io.open(MANIFEST, encoding="utf-8") as fh:
        m = json.load(fh)
    return {e["record_id"]: e for e in m["entries"]}


def stream(path):
    """Yield (record_id, header_line, body) over the corpus."""
    rid, head, buf = None, None, []
    with io.open(path, "r", encoding="utf-8", errors="replace", newline="") as fh:
        for line in fh:
            m = HEADER_RE.match(line)
            if not m:
                if rid is not None:
                    buf.append(line)
                continue
            if rid is not None:
                yield rid, head, "".join(buf)
            rid, head, buf = m.group(1), line, []
    if rid is not None:
        yield rid, head, "".join(buf)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", metavar="OUT")
    ap.add_argument("--check", metavar="FILE")
    args = ap.parse_args()

    entries = load()
    src = args.check or Config.FILE_V8
    print("manifest: %d entries\nsource:   %s" % (len(entries), src))

    if args.check:
        # Gate: the repaired file must show the corrected state.
        seen_hosts, seen_victims, bad = {}, {}, []
        victim_ids = {e["victim_record_id"] for e in entries.values() if e.get("victim_record_id")}
        for rid, _h, body in stream(src):
            if rid in entries:
                seen_hosts[rid] = body
            if rid in victim_ids:
                seen_victims[rid] = body
        for rid, e in entries.items():
            body = seen_hosts.get(rid)
            if body is None:
                bad.append("%s: host record missing entirely" % rid)
                continue
            if body != e["kept_body"]:
                bad.append("%s: host body is not the repaired text (%d chars, expected %d)"
                           % (rid, len(body), e["keep_chars"]))
            if hashlib.sha256(body.encode("utf-8")).hexdigest() == e["original_sha256"]:
                bad.append("%s: host still carries the ORIGINAL defective body" % rid)
            if e.get("victim_record_id"):
                vb = seen_victims.get(e["victim_record_id"])
                if vb is None:
                    bad.append("%s: re-filed victim record %s is absent"
                               % (rid, e["victim_record_id"]))
                elif hashlib.sha256(vb.encode("utf-8")).hexdigest() != e["victim_sha256"]:
                    bad.append("%s: victim record %s content does not match the manifest"
                               % (rid, e["victim_record_id"]))
        if bad:
            print("\nGATE FAILED (%d):" % len(bad))
            for b in bad:
                print("  - %s" % b)
            sys.exit(1)
        print("\nGATE PASSED: %d hosts repaired, %d victims re-filed, no original body survives"
              % (len(entries), len(victim_ids)))
        return

    if not args.apply:
        ok = skew = 0
        for rid, _h, body in stream(src):
            e = entries.get(rid)
            if not e:
                continue
            if hashlib.sha256(body.encode("utf-8")).hexdigest() == e["original_sha256"]:
                ok += 1
            else:
                skew += 1
                print("  SKEW %s — corpus body no longer matches the manifest hash" % rid)
        print("\n%d/%d entries match the live corpus exactly; %d skewed"
              % (ok, len(entries), skew))
        if skew:
            sys.exit(1)
        return

    # --apply
    applied, skipped, refiled = [], [], []
    with io.open(args.apply, "w", encoding="utf-8", newline="") as out:
        for rid, head, body in stream(src):
            e = entries.get(rid)
            if not e:
                out.write(head)
                out.write(body)
                continue
            if hashlib.sha256(body.encode("utf-8")).hexdigest() != e["original_sha256"]:
                print("  SKIPPED %s — hash mismatch, upstream data changed" % rid)
                skipped.append(rid)
                out.write(head)
                out.write(body)
                continue
            out.write(head)
            out.write(e["kept_body"])
            applied.append(rid)
            if e.get("victim_record_id"):
                out.write("==> %s <==\n" % e["victim_record_id"])
                out.write(e["victim_body"])
                refiled.append(e["victim_record_id"])
    print("\napplied %d, re-filed %d, skipped %d -> %s"
          % (len(applied), len(refiled), len(skipped), args.apply))
    if skipped:
        sys.exit(1)


if __name__ == "__main__":
    main()
