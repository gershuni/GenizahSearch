# Compact search compiler measurements — 2026-09-14

This change addresses the Unicode expansion regression in `shared/search_regex.py`
on top of merged commit `fff6c780`. It retains the interruptible `regex` matcher,
isolated research workers, resource limits, and explicit API deadlines.

## Implementation

Use native Unicode letter/number properties plus compact corrections for the
differences between the running Python's Unicode database and the dependency's
database. Compute those corrections once per process with small native scans.
Rewrite mixed character classes as single-character predicates, preserving
negation, scoped ASCII/Unicode flags, and capture numbering. Word membership is
independent of case folding, as in Python `re`.

Compilation checks the active deadline before and after native compilation and
between initialization blocks. A single native compilation is still not
interruptible in process; isolated workers remain necessary.

Review follow-up: completed Unicode scan blocks now remain cached if an explicit
budget expires during cold initialization, allowing subsequent calls to finish
without rescanning earlier blocks. Mixed classes escape literal opening brackets
before re-emission, and Lab snippets handle compilation deadline expiry as well
as matching expiry. The comment scanner is unchanged: the review's proposed
comment-parser reproducer is rejected by stdlib itself.

## Compilation benchmark

Windows, Python 3.12.14, regex 2026.9.10. Actual
`SearchEngine.build_regex_pattern`, 120 distinct generated four-letter Hebrew
tokens, 111 overlapping ten-word chunks. Exact queries use a variant provider
returning only the original term. No index is opened.

The previous adapter source was copied unchanged from the merged base before
editing. Each arm purges both compiler caches. Unicode initialization is measured
separately; it is not included in batch compilation. Exact-query arms alternate
forward/reverse order across three rounds. Supplementary cases use one round and
are directional observations, not stable latency estimates.

| Case | stdlib re | Merged adapter | Compact adapter |
| --- | ---: | ---: | ---: |
| Exact, median of three rounds | 0.463 s | 11.845 s | 1.655 s |
| Five-word gaps, one round | 0.500 s | 33.697 s | 3.472 s |
| Eight synthetic alternatives plus original per term, one round | 4.567 s | 19.991 s | 8.841 s |
| Responsa within-document AND, one round | 1.197 s | 2.111 s | 2.126 s |

Exact batch times by round:

- stdlib: 0.412, 0.463, 0.531 seconds.
- Merged adapter: 11.834, 11.845, 15.157 seconds.
- Compact adapter: 1.610, 1.655, 1.668 seconds.

The exact workload improves about **7.2×** against the merged adapter, though
it remains slower than stdlib. Unicode initialization in that run was 0.126 s
compact versus 0.141 s previous. Within-document AND with plain terms does not
contain word-class escapes, so this change does not optimize that query shape.

## Matching checks

The benchmark also checks identical spans and captures for 333 searches across
three synthetic documents in each matching-enabled round. Exact matching median:
0.0054 s compact versus 0.0103 s previous. Variant sample: 0.0190 s versus 0.0224 s.
Within-document sample: 0.0644 s versus 0.0554 s; that single small measurement
does not establish a regression or improvement in an unchanged pattern.

An initial gap matching sample with long overlapping candidates ran excessively
in stdlib and was manually stopped. The reported gap numbers come from a fresh
compilation-only run. The optional gap matching sample in the script is now small;
matching is opt-in, because stdlib does not provide a timeout.

Reproduce with a compatible project Python environment:

```powershell
git show fff6c780:shared/search_regex.py > previous-search-regex.py
python scripts/benchmark_search_compiler.py --previous previous-search-regex.py --match
python scripts/benchmark_search_compiler.py --previous previous-search-regex.py --case gaps --rounds 1
python scripts/benchmark_search_compiler.py --previous previous-search-regex.py --case variants --rounds 1 --match
python scripts/benchmark_search_compiler.py --previous previous-search-regex.py --case responsa --rounds 1 --match
```

Keep the extracted previous source outside the package import path; do not replace
the active adapter with it. Use UTF-8 when redirecting with older PowerShell.

## Validation and rollout limits

485 targeted tests passed (two dependency/event-loop deprecation warnings).
Ruff and `git diff --check` passed for the change.

Regression coverage includes exhaustive Unicode word/nonword membership with and
without IGNORECASE, Hebrew separators, mixed and negated classes, scoped flags,
captures/replacements, compilation deadline expiry, matching interruption, and
event-loop heartbeat behavior. The existing research-worker limits are unchanged.

These are local compiler and synthetic matching measurements, not end-to-end
research-platform latency. Before deployment conclusions, measure real variant
expansion and real-index composition/search completion, p50/p95 latency, queue
wait, worker/parent peak memory, and event-loop/browsing responsiveness under
concurrent load. The private review-viewer facet optimization remains separate.

## Review-fix validation

After the review fixes, 671 targeted tests passed, including Lab snippet tests,
repeated short-budget initialization, literal opening brackets in mixed classes,
exhaustive Unicode boundaries, boundary `pos`/`endpos`, and simple case folding.
Ruff and patch whitespace checks passed. The same two deprecation warnings remain.

A fresh three-round, interleaved exact-query comparison against the pre-review
compiler at `96f5418c` retained identical spans/captures. Batch compilation medians
were 0.912 s after fixes versus 0.920 s before fixes; initialization was 0.073 s
versus 0.066 s. Fixed batch rounds were 0.895/0.921/0.912 s, and pre-review rounds
were 0.920/0.935/0.918 s. This compares the two arms of this run; the absolute
times should not be compared directly with the earlier run under different load.
These fixes do not establish end-to-end production latency or concurrency safety.
