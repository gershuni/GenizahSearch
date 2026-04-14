---
phase: 63
reviewers: [gemini, codex]
reviewed_at: 2026-04-14T19:00:00Z
plans_reviewed: [63-01-PLAN.md, 63-02-PLAN.md]
---

# Cross-AI Plan Review — Phase 63

## Gemini Review

### 1. Summary
The implementation plans for Phase 63 are technically sound, highly pragmatic, and demonstrate a deep understanding of the project's specific constraints (e.g., the `.claude` directory overhead and the Windows deployment target). The decision to use a narrow, high-signal Ruff ruleset ensures that the CI rollout provides immediate value (fixing a real F821 bug and cleaning up ~270 unused imports) without drowning the transition in stylistic bike-shedding. The two-file dependency strategy (`requirements.txt` vs `requirements-lock.txt`) is an industry-standard approach for applications that balances developer flexibility with deployment reproducibility.

### 2. Strengths
- **Fast-Fail CI Design:** Sequencing the `lint-and-docs` job before the matrixed `tests` job is efficient, saving runner minutes by failing early on syntax or documentation errors.
- **Empirical Baseline:** The plan is based on a specific audit of existing violations (286 total), ensuring the "zero violation baseline" is achievable within the phase.
- **Platform Alignment:** Including a Windows runner in the CI matrix is critical for a PyQt6 application, as it catches path-handling issues and binary compatibility problems that Ubuntu runners miss.
- **Scoped Linting:** Choosing a surgical Ruff ruleset (`E9`, `F401`, `F811`, `F821`) instead of a broad sweep prevents the "wall of red" effect and focuses on code correctness and hygiene.
- **Exclude Logic:** Explicitly excluding `.claude` and `extension/` prevents the CI from being overwhelmed by non-core artifacts or third-party worktrees.

### 3. Concerns
- **The "Lockfile Deadlock" (MEDIUM):** Plan 63-01 configures `ci.yml` to install from `requirements-lock.txt`, but Plan 63-02 actually creates that file. Any push or PR triggered by the completion of 63-01 will fail CI because the lockfile is missing.
- **Windows CI Stability (LOW):** Windows runners are notoriously slower and more prone to transient network or environment setup failures (especially with PyQt6 dependencies). This may require `timeout-minutes` or specific setup-python caching to be reliable.
- **Manual Re-export Maintenance (LOW):** Using `# noqa: F401` for re-exports is functional, but can be brittle. If the project uses `__init__.py` files for API surface area, a more robust approach (like `__all__`) might be preferable long-term.
- **`check_docs.py` Dependencies (LOW):** The `lint-and-docs` job needs to ensure that any dependencies required by `scripts/check_docs.py` are installed, even if it isn't running the full test suite.

### 4. Suggestions
- **Bootstrap Adjustment:** Modify Plan 63-01 to temporarily use `pip install -r requirements.txt` in the CI file, and update it to `requirements-lock.txt` in Plan 63-02. Alternatively, move the creation of the lockfile to the end of Plan 63-01 so the CI is functional immediately upon commit.
- **Pre-commit Hook Mention:** While CI is the primary goal, the `DEVELOPER_GUIDE.md` should suggest (or the plan could include) a basic `pre-commit` configuration to allow developers to catch these Ruff violations locally before pushing.
- **Ruff Format (Optional):** Since `line-length = 120` is being set, consider adding `ruff format --check .` to the lint job as a "warning only" or non-blocking check to start socializing the idea of automated formatting.
- **Dependency Cache:** Add `actions/cache` to the CI workflow to speed up the installation of the ~115 packages identified in the research.

### 5. Risk Assessment
**Risk Level: LOW**

The plans are well-sequenced and avoid modifying core application logic, focusing instead on infrastructure. The most significant risk is the temporary breakage of CI during the "Wave 1" to "Wave 2" transition, which is easily mitigated by adjusting the order of lockfile creation. The use of a scoped Ruff ruleset effectively eliminates the risk of accidental regressions caused by aggressive auto-fixing.

---

## Codex Review

### Plan 63-01: CI workflow + ruff config + fix violations

#### 1. Summary
This plan is well scoped and mostly aligned with the repo's immediate needs: replace the docs-only workflow, add a minimal Ruff baseline, and get to zero violations before tightening anything else. The main weaknesses are sequencing and contract mismatch: as written, it creates a CI workflow that depends on a lock file that does not exist yet, and its trigger policy does not literally satisfy the roadmap's "every push" wording.

#### 2. Strengths
- The Ruff scope is intentionally narrow, which avoids turning Phase 63 into a style cleanup project.
- Excluding `.claude` is the right repo-specific choice; without it Ruff would scan a large worktree area and explode the violation count.
- `lint-and-docs` before `tests` is a good fast-fail structure.
- Replacing the docs-only workflow with one CI entry point reduces confusion.
- Running `scripts/check_docs.py` in the lightweight Ubuntu job is sensible because the script is standalone and does not need app dependencies.
- The branch target `master-main` matches the actual repo default branch.

#### 3. Concerns
- **HIGH: Lockfile doesn't exist in Wave 1.** The proposed `ci.yml` installs `requirements-lock.txt`, but Plan 63-01 explicitly says that file does not exist yet. If Wave 1 is merged or even pushed alone, CI fails immediately.
- **HIGH: Trigger policy mismatch.** The roadmap success criterion says "Every push and PR triggers" CI, but the plan only triggers `push` on `master-main`. That is a policy decision, not the stated success criterion.
- **MEDIUM: Unpinned ruff in CI.** `pip install ruff` is unpinned, so the safety net itself can drift over time.
- **MEDIUM: Test profile undefined.** `pytest tests/` is not a fully defined CI contract here. In this repo, some E2E/performance tests skip without Selenium or `Genizah_Index`, so a green run is narrower than "catches regressions on every push" suggests.
- **MEDIUM: F401 autofix risk.** Bulk `F401` autofix across the repo can remove intentional side-effect imports, and the current suite does not obviously import every top-level entrypoint.

#### 4. Suggestions
- Merge Plans 63-01 and 63-02 as one unit, or make Wave 1 use `requirements.txt` temporarily and switch to the lock file only when it exists.
- Reconcile the trigger policy with the success criteria: either run on all pushes, or rewrite the milestone text to "all PRs and default-branch pushes."
- Pin Ruff in CI, either inline in the workflow or via declared dev/CI dependencies.
- Define the CI test profile explicitly: what is expected to run, and what is expected to skip on GitHub runners.
- Add a tiny smoke layer for key modules not clearly covered by the current tests before trusting mass import cleanup.

#### 5. Risk Assessment
**MEDIUM.** The implementation is mechanically straightforward, but as written it can introduce a knowingly broken intermediate state and it underspecifies what "CI green" really means for this repo.

### Plan 63-02: Dependency pinning + DEVELOPER_GUIDE docs

#### 1. Summary
The intent is right, and the docs work is clearly needed, but this is the weaker plan. The current locking approach is not strong enough for the stated determinism goal because it freezes the ambient environment, assumes one lock file works across Ubuntu 3.10 and Windows 3.11, and never cleanly resolves where `pytest` and `ruff` belong in declared dependencies.

#### 2. Strengths
- Doing pinning after the lint/CI cleanup is the right order.
- Keeping a human-edited direct dependency file plus a generated lock file is maintainable in principle.
- Updating DEVELOPER_GUIDE.md is necessary; it is currently stale on linting and dependency workflow.
- Using a lock file in CI is better than relying on transitive resolution from bare direct dependencies.

#### 3. Concerns
- **HIGH: Cross-platform lock file.** A single `pip freeze` lock generated on one machine is not reliably reproducible across this plan's matrix of Ubuntu 3.10 and Windows 3.11. Platform- and Python-specific transitive packages can differ.
- **HIGH: Ambient environment freeze.** The plan freezes the "currently installed" environment instead of resolving from a fresh clean venv. That can capture local drift and hide fresh-install failures.
- **HIGH: Dev dependency ownership.** `requirements.txt` currently contains runtime packages only. Neither `pytest` nor `ruff` is declared there, yet the workflow and docs depend on them. The plan does not explicitly solve toolchain ownership.
- **HIGH: Requirement text mismatch.** The roadmap/BLDG wording says `pip install -r requirements.txt` on a fresh venv should be deterministic. A direct-deps-only `requirements.txt` does not satisfy that literally; only the lock file does.
- **MEDIUM: Weak verification.** `pip install -r requirements-lock.txt --dry-run` is a weak verifier compared with an actual clean install on both target environments.
- **MEDIUM: No tamper protection.** `pip freeze` without hashes is version pinning, not a tamper-resistant lock.

#### 4. Suggestions
- Generate pins and locks from clean environments, not the current dev environment.
- If this phase keeps Ubuntu 3.10 and Windows 3.11 in CI, validate locking per target. The safest fix is per-platform/per-Python lock files or a real lock generator with markers/hashes.
- Explicitly decide where `pytest` and `ruff` live. Right now the repo has no declared test/lint tool dependencies.
- Replace `--dry-run` validation with real fresh-venv install checks on the same targets CI will use.
- Align the requirement text with the chosen two-file strategy, or change the implementation if the requirement really means "all deps pinned in `requirements.txt`."
- In the docs, avoid claiming "fully reproducible on any machine" unless the lock strategy actually supports that.

#### 5. Risk Assessment
**HIGH.** Reproducibility is the core deliverable of this plan, and the current `pip freeze` strategy is too weak to guarantee it across the exact CI matrix the phase chose.

---

## Consensus Summary

### Agreed Strengths
- **Narrow Ruff ruleset** is the right call -- both reviewers praised the surgical scope (E9, F401, F811, F821) over a broad E+F sweep
- **Fast-fail CI structure** (lint-and-docs before tests) is efficient and well-designed
- **`.claude` exclusion** is critical and correctly identified by both reviewers
- **Wave ordering** (lint first, pin last) is the correct sequencing
- **Windows runner inclusion** matches the project's deployment reality

### Agreed Concerns
1. **Lockfile deadlock (HIGH)** -- Both reviewers flagged that Plan 63-01 creates a ci.yml referencing requirements-lock.txt, which doesn't exist until Plan 63-02. This creates a broken intermediate state. **Fix: use requirements.txt in Wave 1, switch to lock file in Wave 2.**
2. **Cross-platform lock file validity (MEDIUM-HIGH)** -- Codex raised that a single pip freeze may not be reproducible across Ubuntu 3.10 + Windows 3.11. Gemini implicitly accepted the approach. **Worth validating: check if CI actually installs cleanly on both platforms.**
3. **Unpinned ruff in CI (MEDIUM)** -- Both implicitly or explicitly noted that `pip install ruff` without a version pin means the linter itself can drift. **Fix: pin ruff version in CI workflow.**
4. **Dev dependency ownership (MEDIUM)** -- Codex specifically flagged that pytest and ruff have no declared home. **Fix: document that these are CI-only deps installed separately, or add a requirements-dev.txt.**

### Divergent Views
- **Overall risk**: Gemini rates the phase as LOW risk, while Codex rates Plan 63-01 as MEDIUM and Plan 63-02 as HIGH. The divergence centers on how seriously to take the cross-platform lock file issue and the ambient-environment freeze concern.
- **Pre-commit hooks**: Gemini suggested adding pre-commit config; Codex did not mention it. This is out of scope for Phase 63 per the user's decisions.
- **Trigger policy**: Codex flagged a mismatch between "every push" in success criteria and the `push: branches: [master-main]` trigger. Gemini did not flag this. The current trigger (master-main push + all PRs) is the user's explicit decision (D-04).
