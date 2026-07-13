# Codex thorough code review — MAPV2-15 (2026-07-13, codex-cli 0.143.0)

Brief: `results/CODEX-REVIEW-BRIEF-mapv2-15.md`. **Verdict: REWORK** (before corpus-wide
trust) — canon rarity model/keying, leakage-free evaluation, audit/scope sampling+split.
Assessed as "an honest prototype foundation, but not yet sound enough for trusted
corpus-wide annotation claims." Verbatim findings:

## 1. Correctness bugs
- `build_audit_sample.py:35-37` — `--target` parsed but NEVER used; sampling uses fixed
  BUCKET_TARGET (`:124-156`); report shows 467 sampled, not the advertised 420.
- `metadata_scope.py:148-153` + `build_audit_sample.py:103-114` — "leave-target-out"
  n_matched_works claim is FALSE: count includes the current candidate; weakly circular.
- span offsets safe for v2 (spans_json from norm_stream(pages.text)[0], consumed the same),
  BUT no length/hash assertion catches stale-page/normalizer drift.
- `grader.py:83-84` — `_canon_scores()` scores only the LONGEST span; `matched_letters` is a
  SUM over merged spans → multi-span matches mis-scored.
- `shared_source.py:56-67` — claims span-union overlap but does NOT merge spans → overlapping
  spans double-counted. Superseded/unused → remove or mark deprecated.
- `canon_rarity.py:38-39` — CRC32 keys over ~7M 8-grams → thousands of expected collisions.
  `:58` — IDF `log(nw/(1+c))` can go NEGATIVE for grams in all works, contradicting
  "ubiquitous -> ~0".

## 2. Statistical / methodology
- Post-stratification formula correct (`grader.py:222-236`) IF each row is representative in
  its cell; but the floor/top-up draw oversamples rare cells and leaves huge cells with tiny
  samples (`build_audit_sample.py:131-156`) → high variance.
- SHA1-ranked draw is pseudo-random, not a probability sample with estimable variance.
- `component_key` insufficient: chain IDs depend on DB iteration order without ORDER BY
  (`build_audit_sample.py:78-83`); non-chain grouping only `sys:{sid}` (`:214-215`);
  page_text_hash is raw whitespace-stripped (not norm_stream); dedup only within the sample.
- **77% is NOT a clean held-out estimate**: measure() uses the same gold + the critic grades
  as the AI layer (`grader.py:140-145`), and TH=1.5 was selected from the same 132
  (`canon_rarity.py:31-35`). Development-set agreement, not unbiased validation.

## 3. Grader logic
- Title veto not narrow enough: same_work/name_variant → known immediately (`grader.py:103-111`)
  despite witness→known errors.
- Missing AI grade silently → 'tsarich' (`grader.py:131-135`) — hides coverage failures;
  should be explicit missing_ai / abstention.
- measure() sets scope_regime=None, genre=cat (`:162-167`) while frame() uses real
  scope_regime/genre (`:228-232`) → measured rule tier ≠ deployed rule tier.
- TH=1.5 is a tuned hyperparameter selected on the reported gold → biases 77%.

## 4. Scope detector
- n_matched_works tie-break circular as implemented, not leave-target-out (`:148-153`).
- confidence values mostly decorative (hand constants `:124-147`); only single_work>=0.8
  materially gates resolution().
- tri-state exists (`:164-174`) but downstream known-veto is driven by title_class, not
  resolution.
- `_sig()` says title order but returns sorted tokens (`:70-76`).

## 5. Canon ingestion
- `fine_cat()` default-to-Midrash (`build_canon_corpus.py:37-54`) excludes 297/393 from the
  mask; any Bavli/Mishnah/Tosefta title missed by the heuristic silently loses coverage.
- No reconciliation vs expected tractate inventory (only 25 titles eyeballed).
- UTF-8 errors='replace' (`:90-95`) can silently degrade non-UTF-8 Hebrew.

## 6. Verdict
REWORK before corpus-wide trust. Top fixes: (a) collision-safe/nonnegative canon rarity with
cache metadata + better doc granularity; (b) leakage-free held-out evaluation with missing-AI
as failure not tsarich; (c) cleaned audit/scope design — actual target, stable component IDs,
global dedup/split keys, truly leave-target-out scope tie-break.
