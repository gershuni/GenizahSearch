# Codex review — discovery-v3 bake, round 9

## R8-LOW disposition

**CLOSED.** Commit `7a1ee35f` strengthens the real WAL fixture in the previously missing way: after committing through a held WAL writer, the candidate-build stub opens a fresh SQLite connection and reads the table. The test asserts that reader observes both the pre-existing row and the row committed during measurement, while retaining the assertion that the main database file hash did not change. Thus the final hash mismatch now halts on sidecar state that a SQLite reader can actually observe, rather than merely on an arbitrary changed sidecar.

## Final adversarial sweep

I found no new production-code defect and no remaining un-failable test across the reviewed v3 measurement, routing, slim-research-DB, build orchestration, bake-state, and masking paths. In particular, the router and novelty gates are reached from `finalize_build`, and the changed WAL test's failure condition is connected to a real committed/read-back state change.

## Execution readiness

The implementation is safe to execute subject to the documented release-quality prerequisites.

- **Broken production code:** I found no remaining production-code blocker.
- **Owner actions owed:** add the missing restricted pattern to `.masking_patterns`; confirm the intended pattern count; run the full strict scan; and choose the novelty option (recommended: option 0, the $0 v3-input re-measurement).
- **Must record at run time:** retained-key attestation identity; scanned asset and SQLite paths with post-build hashes; and the pre-build source-identity record.

## Verification

I reviewed the specified round-8 review, changed test, §8 readiness statement, schema contract, and listed code paths. `git diff --check 7a1ee35f^ 7a1ee35f` reported no whitespace errors. The targeted pytest invocation could not start because this environment's `python.exe` fails before execution with “A specified logon session does not exist”; this review is therefore source-grounded.

VERDICT: APPROVE
