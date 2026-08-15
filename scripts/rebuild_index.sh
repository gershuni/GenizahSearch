#!/bin/bash
# Rebuild the MAIN Genizah index from the repaired corpus.
#
# Deliberately does NOT restart genizah-web. The running service keeps serving
# the old index through its open handle; swapping to the new one is a separate,
# human-watched step after the result has been checked.
#
# On failure the partial build is removed and the original index is restored to
# its path, so a reboot or the Sun/Wed restart timer always finds a valid index.

set -uo pipefail

APP=/home/ubuntu/GenizahSearch
LOG=$APP/index_rebuild.log
EXPECTED_RECORDS=948552
EXPECTED_SHA=0ac792ad3963ab5a39ce7de792f0c6065fe4c8a6528bd2663fec209d8d8bb528

cd "$APP" || exit 1
exec >>"$LOG" 2>&1

say() { echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] $*"; }

say "=== rebuild starting ==="

# --- pre-flight: refuse to build from anything but the verified repaired corpus
ACTUAL_SHA=$(sha256sum Transcriptions.txt | cut -d' ' -f1)
if [ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]; then
    say "ABORT: Transcriptions.txt is not the repaired corpus"
    say "  expected $EXPECTED_SHA"
    say "  actual   $ACTUAL_SHA"
    exit 1
fi
RECORDS=$(grep -c '^==> ' Transcriptions.txt)
if [ "$RECORDS" != "$EXPECTED_RECORDS" ]; then
    say "ABORT: expected $EXPECTED_RECORDS records, found $RECORDS"
    exit 1
fi
say "corpus verified: $RECORDS records, sha256 ok"

if [ -e Genizah_Index/tantivy_db.bak ]; then
    say "ABORT: Genizah_Index/tantivy_db.bak already exists - a previous run left state behind"
    exit 1
fi
if [ ! -d Genizah_Index/tantivy_db ]; then
    say "ABORT: Genizah_Index/tantivy_db is missing"
    exit 1
fi

say "disk before: $(df -h /home/ubuntu | tail -1)"

# --- move the live index aside; the service keeps serving it via its open inode
mv Genizah_Index/tantivy_db Genizah_Index/tantivy_db.bak || { say "ABORT: mv failed"; exit 1; }
say "index moved aside -> tantivy_db.bak"

# --- build, yielding I/O and CPU to the web process
START=$(date +%s)
nice -n 19 ionice -c 3 ./venv/bin/python build_index.py main
RC=$?
ELAPSED=$(( $(date +%s) - START ))
say "build exited rc=$RC after ${ELAPSED}s ($((ELAPSED/60)) min)"

if [ $RC -ne 0 ] || [ ! -d Genizah_Index/tantivy_db ]; then
    say "BUILD FAILED - removing partial output and restoring the original index"
    rm -rf Genizah_Index/tantivy_db
    mv Genizah_Index/tantivy_db.bak Genizah_Index/tantivy_db
    say "original index restored to its path; service untouched"
    exit 1
fi

# --- report, do not act
./venv/bin/python - <<'PY'
import tantivy
for name in ("Genizah_Index/tantivy_db", "Genizah_Index/tantivy_db.bak"):
    try:
        ix = tantivy.Index.open(name)
        ix.reload()
        print("  %-32s num_docs=%d" % (name, ix.searcher().num_docs))
    except Exception as exc:
        print("  %-32s ERROR %s" % (name, exc))
PY

say "sizes: $(du -sh Genizah_Index/tantivy_db Genizah_Index/tantivy_db.bak | tr '\n' ' ')"
say "disk after: $(df -h /home/ubuntu | tail -1)"
say "=== rebuild finished; service NOT restarted, still serving the old index ==="
