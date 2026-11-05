## P3 build-ci: The FGP gate-4 release-check test fails on one slow timing sample
Where: `tests/test_fgp_gate4_chooser_validation.py::test_release_check_requires_the_file_gate_4_validated`, `scripts/fgp_gate4_chooser_validation.py` (C6 latency, the `latency.ratio_vs_today` budget)
Opened: 2026-11-05

On CI run 41995209918 (PR #483, which touches nothing FGP) the Ubuntu shard 3/3 failed this test on
C6 alone: `C6_latency.load.cand: expected p95 <= 500.0 ms, got p95 129.73 ms, max 129.73 ms`. The
absolute budget held. What failed is the ratio check: the candidate's p95 must be at most 1.5x
today's p95 plus the floor. `check_defaults` times one load per reference manuscript, four on the
test's fixture, so the p95 is in effect the slowest single load (here p95 = max = 129.73 ms). One
slow load on a shared runner (a GC pause, a cold page) therefore fails the whole release check,
even after the one warm-up per database. The Windows shard 3/3 passed the same commit. The test cannot run
in a worktree (it needs the gitignored `ephraim_meiri_pdf_converter/pdf_to_docx.py`), so it was
not reproduced locally.

Fix sketch: in the test, not the gate (the gate's tolerances are declared, never moved), time
enough loads on the fixture for a p95 to mean something, or skip the ratio check when either side
has fewer than N samples, saying so in the report row.
