# Codex code-review — Quick Task 260714-9jc (SEED-030)

Model: gpt-5.5 (xhigh). Diff: `tmp/seed030_codediff.patch` (v1), `tmp/seed030_codediff_v2.patch` (v2).
Brief: `tmp/CODEX-REVIEW-BRIEF-seed030.md`.

## Round 1 — CHANGES REQUESTED

1. **HIGH** — `choose_default_source` measured `get_fgp_section_for_page(ed, page_num)`, but that
   helper treats `page_num==1` as recto and every other value as verso, while the callers pass the
   *global* displayed page number. A full recto FGP row on displayed page ≥2 was measured as empty
   → wrongly demoted / tagged "shorter than V0.8".
2. **MEDIUM** — desktop `_auto_select_pgp_edition` computed the decision but then selected the first
   `fgp_edition` combo item, ignoring the policy's chosen best edition (web used the chosen one).
3. **LOW** — wiring tests are static string checks; don't exercise the integration behaviors.

## Fixes (commit `1d3ef49c`)

- Measure the **displayed whole-row `content`** (verified: no display path narrows FGP content to a
  section — `get_fgp_section_for_page` was only ever called by the new helper). Dropped the
  `page_num` parameter entirely → the recto/verso bug is structurally impossible.
- Desktop now selects the combo item whose `source_id` matches `decision['source']` (best-of-multiple
  parity), falling back to the first FGP edition.
- Replaced the section-based test with content-based + a `test_no_page_dependence` regression guard.

## Round 2 — APPROVE

> HIGH: RESOLVED. MEDIUM: RESOLVED. LOW: RESOLVED enough for approval. Whole-row measurement is
> correct — FGP display paths render `content` whole-row; measuring `content` matches what the user
> sees. No new blocking defects. **Verdict: APPROVE.**

(Codex could not run pytest in its sandbox — "logon session does not exist". Tests were run locally:
119 passed in the fgp/version-selector set.)
