# -*- coding: utf-8 -*-
"""Divergence-adjudication gate: when the computed identification and the
catalogue point at DIFFERENT works, ask a pinned LLM who is right -- with the
catalogue as the favoured prior (owner, 2026-08-30: scholars who studied the
manuscript wrote it), discounted in exactly two named situations: the
catalogue term is too general, or the page carries several works and the
catalogue names only one of them.

JSON in, JSON out, templated. The verdict is a LABEL for review ordering and
display -- never by itself a pool gate (the catalogue-never-judges rule is
about mechanical catalogue gates; this gate READS the page evidence and the
catalogue prose together and makes one auditable judgment per case).

Modes:
  --dry-run N            render N prompts to a file, zero network, masking-scanned
  --eval-gold            run over the owner-graded divergent cases (page grain)
                         and score agreement against the grades
  --run                  full divergent-pair run (pair grain)  [not yet wired]

Every outbound prompt is masking-scanned BEFORE submission (fail closed:
missing MASKING_SCAN_PATTERNS_FILE refuses to run). Real usage.cost is logged
per call against a REQUIRED --cost-ceiling. Checkpointed; resume is free.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.discovery_gate1_evidence import (  # noqa: E402
    DEFAULT_FJMS_DB,
    DEFAULT_LIBRARIES_CSV,
    DEFAULT_PGP_DB,
    _combined_catalogue_text,
    load_bib_rows,
    load_fjms_catalog_text,
    load_libraries_csv,
)

DEFAULT_REVIEW_DB = os.path.join(REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db")
DEFAULT_OUT_DIR = os.path.join(REPO_ROOT, "_tmp", "divergence_gate")
DEFAULT_CROSSWALK = os.path.join(
    REPO_ROOT, "discovery_builds", "discovery_v4_2", "build", "crosswalk_v42lit.json")

GOLD_SOURCES = (
    ("e1l", os.path.join(REPO_ROOT, "same_work_spike", "probe", "rsource", "data",
                         "e1l_blind_union_grades.json")),
    ("gen2_g", os.path.join(REPO_ROOT, "same_work_spike", "probe", "review",
                            "gen2_g_review_feedback.json")),
)
FRESH_DECK_GRADES = (
    os.path.join(os.path.expanduser("~"), "Downloads", "pool_tuning_grades.json"),
    os.path.join(os.path.expanduser("~"), "Downloads", "pool_tuning_grades (1).json"),
)

# ---------------------------------------------------------------------------
# The pinned contract. Model default per owner request 2026-08-30
# (gemini-3.8-flash, "a new version"); --model overrides for the 3.7
# comparison arm. Effort low is the family posture proven on the novelty gate.
# ---------------------------------------------------------------------------

# 3.8-flash was the owner's first choice but is not on OpenRouter yet
# (checked 2026-08-30; the ~flash-latest alias also tops out at 3.7).
# Owner ruling: "go with latest, 3.7 is good enough".
LLM_MODEL_DEFAULT = "gemini-3.7-flash"
LLM_REASONING_EFFORT = "low"

VERDICTS = (
    "catalogue_right_match_is_quotation",
    "catalogue_right_claim_mistaken",
    "both_right_multiple_works",
    "catalogue_too_general",
    "computed_right_catalogue_mismatch",
)

PROMPT_TEMPLATE = """You adjudicate a disagreement about one Cairo Genizah manuscript page. A text-matching system identified the page as carrying a specific literary WORK ("the computed identification"). The manuscript's CATALOGUE entry, written by scholars who examined the manuscript, points at something DIFFERENT. Your job: decide what this disagreement means, from the evidence given.

PRIOR: the catalogue deserves substantial weight -- it was written by scholars who studied the physical manuscript, and it is usually right about what the page mainly contains. The computed identification, however, sees letter-exact textual overlap the cataloguer may not have checked. The two common situations where BOTH are right:
  1. The catalogue term is GENERAL (a genre word, a broad container like "midrash", "halakha", "prayers") and the computed work is a compatible, more specific identity within it.
  2. The page carries SEVERAL works (very common: Bible verses interleaved with Targum, Tafsir, or a commentary; a halakhic text quoting liturgy) and the catalogue names only one of them, while the computed identification names another that is GENUINELY there.

The main FAILURE modes of the computed identification, which you must also weigh:
  - The matched text is a QUOTATION: the computed work quotes a text the page carries (or both quote a shared third text -- scripture, a fixed prayer, a legal formula), so the overlap does not make the page a copy of the computed work.
  - The computed work is simply MISTAKEN: a lookalike passage, generic phrasing, or a text family shared across many works.

INPUT: one JSON object per case:
  - "manuscript": shelfmark and library.
  - "catalogue": the catalogue's structured genre tags, its free-text identification prose, bibliography entries, and any Princeton Geniza Project description. READ THE PROSE -- a structured tag pointing elsewhere is never by itself proof of a real disagreement.
  - "computed_identification": the claimed work's title, author, domain, WHICH SECTIONS of the work matched, what fraction of the page's letters the match covers (page_coverage_percent -- high coverage means the page is mostly this text; low coverage means most of the page is something else), the number of matched pages in this manuscript, and machine flags (a relation verdict; whether the match looks like shared scripture; whether the matched section is a liturgy/formulary unit).
  - "text_samples": up to two aligned excerpts -- the manuscript page's own transcription around the match, and the computed work's text around the same match. Compare them: does the page read AS the computed work (continuous running text of it), or does it read as another text that shares a passage with it?

DECISION VALUES -- choose exactly ONE "verdict":
  - "catalogue_right_match_is_quotation": the catalogue's identification stands; the textual overlap exists because the computed work QUOTES text the page carries, or both share a third text (scripture, liturgy, a formula). The page is not a copy of the computed work.
  - "catalogue_right_claim_mistaken": the computed identification is simply wrong for this page (lookalike or generic text); the catalogue is the better account.
  - "both_right_multiple_works": the page genuinely carries the computed work IN ADDITION to what the catalogue names (interleaved or sharing the page).
  - "catalogue_too_general": the catalogue's term is broad or generic and does not contradict the computed work; the computed identification is a compatible, more specific identity.
  - "computed_right_catalogue_mismatch": the evidence shows the page IS the computed work and the catalogue entry does not fit the page's actual text. Use this ONLY when the excerpts clearly read as continuous text of the computed work AND page coverage is high; it asserts scholars erred, so it needs the strongest evidence.

STRUCTURED ABSTENTION: if the evidence given cannot support any of the five -- too short, contradictory, or you would be guessing -- respond {"abstain": true, "reason": "<short reason>"}. An abstention is a real, useful answer and is never penalized.

Respond ONLY with a single JSON object:
{"verdict": "<one of the five values>", "confidence": "<high|medium|low>", "reason": "<at most 25 words, in English>"}
OR the abstention object above. No prose outside the JSON object, and no other fields.
"""

PROMPT_SHA256 = hashlib.sha256(PROMPT_TEMPLATE.encode("utf-8")).hexdigest()

# ---------------------------------------------------------------------------
# Task 2: NEW-FIND candidacy (owner, 2026-08-30). Different question, different
# prior: here NO finding aid identifies the fragment as anything (or aids are
# silent about this work), and the computed identification proposes a NEW one.
# The gate judges whether the proposal is credible and genuinely new.
# ---------------------------------------------------------------------------

NEW_FINDS_VERDICTS = (
    "credible_new_identification",
    "plausible_needs_expert_check",
    "weak_match_generic_text",
    "actually_recorded",
    "wrong_identification",
)

NEW_FINDS_SINGLE_TAIL = """Respond ONLY with a single JSON object:
{"verdict": "<one of the five values>", "doubt": "<the ONE thing an expert should verify, at most 20 words, in English>", "reason": "<at most 25 words, in English>"}
OR the abstention object above. No prose outside the JSON object, and no other fields.
"""

NEW_FINDS_PROMPT_TEMPLATE = """You assess a CANDIDATE NEW IDENTIFICATION of one Cairo Genizah manuscript page. A text-matching system identified the page as carrying a specific literary WORK, and an automated check found that none of the finding aids (catalogue, bibliography, Princeton Geniza Project) already record this identification for this fragment. If that holds, this is a previously-unknown identification -- a real find. Your job: judge whether it is CREDIBLE and genuinely NEW, from the evidence given.

Judge THREE things, in this order:

1. IS THE PAGE REALLY A WITNESS OF THIS WORK? The matched overlap must read as CONTINUOUS RUNNING TEXT of the computed work -- not as one of the classic false positives:
   - a QUOTATION: the page carries another text that the computed work quotes, or both quote a shared third text (scripture, a fixed prayer, a legal formula, a famous aggadah);
   - GENERIC TEXT: liturgical or formulaic passages every siddur/notary shares, standard blessings, common halakhic phrases -- text that cannot single out one work;
   - a LOOKALIKE: a text family shared across many works (parallel midrashim, reused talmudic sugyot in later compilations -- late compilations quoting talmudic text are VERY common).
   Weigh page_coverage_percent (high = the page is mostly this text; low = most of the page is something else), the number of matched pages in the manuscript (several consecutive matched pages = strong), and whether the excerpts show the manuscript text following the work's own order and wording.

2. IS IT REALLY NEW? Read the catalogue prose, bibliography and PGP text yourself. If any of them in fact names this work for this fragment -- under any spelling, alias, or looser phrasing -- the automated check missed it and this is NOT a new find (verdict `actually_recorded`). A catalogue naming only a broad genre ("halakha", "midrash", "piyyut") does NOT count as recording it.

3. WHAT WOULD AN EXPERT CHECK? Every verdict must carry a `doubt`: the single most useful thing for a human scholar to verify (a specific competing source, a nusach variant, a folio to read).

DECISION VALUES -- choose exactly ONE "verdict":
  - "credible_new_identification": the excerpts read as continuous text of the computed work, coverage/context supports a witness, and no aid records it. A real find candidate.
  - "plausible_needs_expert_check": probably right, but one specific weakness remains (short match, partial coverage, a plausible competing source) -- name it in `doubt`.
  - "weak_match_generic_text": the overlap is quotation/shared/generic text that cannot identify the page as this work.
  - "actually_recorded": an aid's own text already records this identification -- not new.
  - "wrong_identification": the page is identifiably something else (say what, in `reason`).

STRUCTURED ABSTENTION: if the evidence given cannot support any of the five, respond {"abstain": true, "reason": "<short reason>"} instead of guessing. An abstention is a real, useful answer and is never penalized.

""" + NEW_FINDS_SINGLE_TAIL

NEW_FINDS_PROMPT_SHA256 = hashlib.sha256(
    NEW_FINDS_PROMPT_TEMPLATE.encode("utf-8")).hexdigest()

NEW_FINDS_BATCH_TAIL = (
    "You will be given SEVERAL numbered cases at once. Judge EACH ONE INDEPENDENTLY, "
    "exactly as if it were the only case given; a case must never influence another. "
    'Respond ONLY with a single JSON object mapping every case number to its own verdict: '
    '{"results": {"1": {"verdict": "<one of the five values>", "doubt": "<at most 20 words>", '
    '"reason": "<at most 25 words, in English>"}, "2": {"abstain": true, "reason": "<short reason>"}}}. '
    "Include EVERY case number exactly once. No prose outside the JSON object.\n"
)

NEW_FINDS_BATCH_PROMPT_TEMPLATE = (
    NEW_FINDS_PROMPT_TEMPLATE[: -len(NEW_FINDS_SINGLE_TAIL)] + NEW_FINDS_BATCH_TAIL)

# ---------------------------------------------------------------------------
# Batched variant (the novelty gate's own pattern: the system prompt is most
# of a single call's input, so batching amortizes it; batch 10 is that gate's
# measured knee). Derived from the single template by swapping the response
# tail -- the assert keeps the two from drifting apart silently.
# ---------------------------------------------------------------------------

_SINGLE_TAIL = """Respond ONLY with a single JSON object:
{"verdict": "<one of the five values>", "confidence": "<high|medium|low>", "reason": "<at most 25 words, in English>"}
OR the abstention object above. No prose outside the JSON object, and no other fields.
"""

assert PROMPT_TEMPLATE.endswith(_SINGLE_TAIL), (
    "PROMPT_TEMPLATE no longer ends with the tail the batch prompt is derived from")

_BATCH_TAIL = (
    "You will be given SEVERAL numbered cases at once. Judge EACH ONE INDEPENDENTLY, "
    "exactly as if it were the only case given; a case must never influence another. "
    'Respond ONLY with a single JSON object mapping every case number to its own verdict: '
    '{"results": {"1": {"verdict": "<one of the five values>", "confidence": "<high|medium|low>", '
    '"reason": "<at most 25 words, in English>"}, "2": {"abstain": true, "reason": "<short reason>"}}}. '
    "Include EVERY case number exactly once. No prose outside the JSON object.\n"
)

BATCH_PROMPT_TEMPLATE = PROMPT_TEMPLATE[: -len(_SINGLE_TAIL)] + _BATCH_TAIL
BATCH_PROMPT_SHA256 = hashlib.sha256(BATCH_PROMPT_TEMPLATE.encode("utf-8")).hexdigest()
DEFAULT_BATCH_SIZE = 10  # the novelty gate's measured knee -- do not raise casually

# ---------------------------------------------------------------------------
# Case building
# ---------------------------------------------------------------------------

def _clip(s: Optional[str], n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    half = n // 2
    return s[:half] + " [...] " + s[-half:]


def _excerpt(row: Mapping[str, Any], side: str) -> Dict[str, str]:
    return {
        "before": _clip(row[f"{side}_before"], 150),
        "match": _clip(row[f"{side}_match"], 600),
        "after": _clip(row[f"{side}_after"], 150),
    }


def load_pgp_texts(db_path: str) -> Dict[str, str]:
    """sys_id -> concatenated PGP description text (the SAME join the novelty
    probe's `load_pgp_texts` documents: documents keyed by pgpid, joined
    through document_fragments.document_id on sys_id)."""
    if not (db_path and os.path.isfile(db_path) and os.path.getsize(db_path) > 0):
        return {}
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    out: Dict[str, List[str]] = {}
    try:
        docinfo = {pid: desc for pid, desc in con.execute(
            "SELECT pgpid, description FROM documents WHERE description IS NOT NULL "
            "AND description != ''")}
        for sid, did in con.execute("SELECT sys_id, document_id FROM document_fragments"):
            if sid and did in docinfo:
                out.setdefault(str(sid), []).append(docinfo[did])
    finally:
        con.close()
    return {k: " || ".join(v) for k, v in out.items()}


class AidsIndex:
    def __init__(self, libraries_csv: str, fjms_db: str, pgp_db: str):
        self.libraries = load_libraries_csv(libraries_csv)
        self.fjms_catalog = load_fjms_catalog_text(fjms_db)
        self.bib_rows = load_bib_rows(fjms_db)
        self.pgp = load_pgp_texts(pgp_db)

    def catalogue_text(self, sys_id: str) -> str:
        return _combined_catalogue_text(sys_id, self.libraries, self.fjms_catalog) or ""

    def bib(self, sys_id: str) -> List[str]:
        # load_bib_rows returns 11-tuples (RunningTitle .. TitleYear); join
        # the non-null fields the way the probe's evidence bundle does
        rows = self.bib_rows.get(sys_id) or []
        return [_clip(" ".join(str(v) for v in r if v), 300) for r in rows[:6]]


def build_case(con: sqlite3.Connection, aids: AidsIndex, page_id: str, work_id: str,
               pair_stats: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """One JSON case for (page_id, work_id): top-2 rows by matched letters."""
    rows = con.execute(
        """SELECT r.*, f.coverage_ppm AS f_cov, f.router_verdict AS f_rv,
                  f.scripture_flagged AS f_scr, f.formula_kind AS f_fk,
                  f.novelty_status AS f_ns
           FROM review_row r JOIN facet_row f ON f.evidence_id = r.evidence_id
           WHERE r.page_id = ? AND r.work_id = ?
           ORDER BY r.matched_letters DESC LIMIT 2""",
        (page_id, work_id)).fetchall()
    if not rows:
        return None
    top = rows[0]
    sys_id = top["sys_id"]
    loci = sorted({r["locus_label"] for r in rows if r["locus_label"]})
    case = {
        "manuscript": {
            "shelfmark": top["shelfmark"] or "",
            "library": top["library_code"] or "",
        },
        "catalogue": {
            "structured_tags": top["catalogue_title"] or "",
            "catalogue_text": _clip(aids.catalogue_text(sys_id), 1500),
            "bibliography": aids.bib(sys_id),
            "pgp_description": _clip(aids.pgp.get(sys_id, ""), 800),
        },
        "computed_identification": {
            "work_title": top["work_title"] or "",
            "work_author": top["work_author"] or "",
            "domain": top["domain"] or "",
            "matched_sections": loci[:4],
            "page_coverage_percent": round((top["f_cov"] or 0) / 10000.0, 1),
            "matched_letters": top["matched_letters"],
            "n_matched_pages_in_manuscript": pair_stats.get("n_pages", 1),
            "relation_verdict": top["f_rv"] or "",
            "shared_scripture_flag": bool(top["f_scr"]),
            "formula_flag": top["f_fk"] or "",
        },
        "text_samples": [
            {"manuscript_side": _excerpt(r, "ms"), "work_side": _excerpt(r, "ref")}
            for r in rows
        ],
    }
    return case


def render_user_message(case: Mapping[str, Any]) -> str:
    return json.dumps(case, ensure_ascii=False, indent=1)


# ---------------------------------------------------------------------------
# Masking guard -- every outbound string, fail closed
# ---------------------------------------------------------------------------

def make_masking_scanner():
    from scripts.check_atlas_masking import build_matcher, load_patterns
    patterns = load_patterns()
    matcher = build_matcher(patterns)  # raises if empty -> fail closed

    def scan_or_die(text: str, tag: str) -> None:
        issues = matcher.scan(text.encode("utf-8"), tag)
        if issues:
            raise SystemExit(
                f"MASKING VIOLATION in outbound prompt ({tag}): {len(issues)} hit(s). "
                "Nothing was sent. Fix the case builder.")
    return scan_or_die


# ---------------------------------------------------------------------------
# Transport (the production novelty shape: echo-checked model, real usage.cost)
# ---------------------------------------------------------------------------

def make_call(api_key: str, model: str, cost_log_path: str,
              timeout: float = 300.0, max_attempts: int = 4):
    import threading

    import requests
    session = requests.Session()
    log_lock = threading.Lock()

    def call(user_msg: str, tag: str,
             system: str = PROMPT_TEMPLATE) -> Optional[Mapping[str, Any]]:
        payload = {
            "model": f"google/{model}",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_msg},
            ],
            "reasoning": {"effort": LLM_REASONING_EFFORT},
            "usage": {"include": True},
            "response_format": {"type": "json_object"},
        }
        last_exc: Optional[BaseException] = None
        for attempt in range(1, max_attempts + 1):
            try:
                resp = session.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}",
                             "Content-Type": "application/json"},
                    data=json.dumps(payload), timeout=timeout)
                resp.raise_for_status()
                data = resp.json()
                echo = data.get("model") or ""
                if model not in echo:
                    raise RuntimeError(
                        f"provider echoed unexpected model {echo!r}, expected {model!r}")
                usage = data.get("usage") or {}
                with log_lock, open(cost_log_path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps({"tag": tag, "model": echo,
                                         "cost": usage.get("cost"),
                                         "attempt": attempt}) + "\n")
                return json.loads(data["choices"][0]["message"]["content"])
            except Exception as exc:  # noqa: BLE001 -- bounded retries on a network call
                last_exc = exc
                if attempt < max_attempts:
                    time.sleep(min(2 ** attempt, 20))
        with log_lock, open(cost_log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"tag": tag, "cost": 0.0, "error": str(last_exc)}) + "\n")
        return None

    return call


def render_batch_message(cases: Sequence[Mapping[str, Any]]) -> str:
    return "\n\n".join(
        f"### CASE {i + 1}\n{render_user_message(c)}" for i, c in enumerate(cases))


def resolve_batch(raw: Optional[Mapping[str, Any]], n: int,
                  vocab: Sequence[str] = VERDICTS,
                  ) -> Optional[List[Dict[str, Optional[str]]]]:
    """All-or-nothing alignment (the novelty gate's rule): every case number
    1..n present exactly once, or None -- a partial accept could silently
    attribute one page's verdict to another. Individual payloads still fail
    closed per-case through resolve_output."""
    if not isinstance(raw, Mapping):
        return None
    results = raw.get("results")
    if not isinstance(results, Mapping):
        return None
    if {str(i) for i in range(1, n + 1)} != {str(k) for k in results.keys()}:
        return None
    return [resolve_output(results[str(i)], vocab) for i in range(1, n + 1)]


def run_batched(items: List[Dict[str, Any]], call, scan, con, aids,
                batch_size: int, max_workers: int, cost_log: str,
                ceiling: float, ckpt_path: str,
                task: Optional[Mapping[str, Any]] = None) -> int:
    """items: [{page_id, work_id, ...extra}] not yet done. Builds cases,
    sends batches in parallel, degrades a twice-misaligned batch to single
    calls. Checkpoint written on the main thread only."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    task = task or TASKS["divergence"]
    single_prompt, batch_prompt = task["single_prompt"], task["batch_prompt"]
    vocab = task["vocab"]

    prepared = []
    for it in items:
        case = build_case(con, aids, it["page_id"], it["work_id"],
                          pair_stats(con, it["page_id"], it["work_id"]))
        if case is None:
            continue
        prepared.append((it, case))
    chunks = [prepared[i:i + batch_size] for i in range(0, len(prepared), batch_size)]

    def do_chunk(idx: int, chunk):
        cases = [c for _, c in chunk]
        msg = render_batch_message(cases)
        scan(batch_prompt + "\n" + msg, f"batch:{idx}")
        for _ in range(2):
            raw = call(msg, f"batch{idx}:{len(chunk)}", system=batch_prompt)
            resolved = resolve_batch(raw, len(chunk), vocab)
            if resolved is not None:
                return resolved
        out = []  # degrade: the validated single-case contract
        for _, case in chunk:
            single_msg = render_user_message(case)
            scan(single_prompt + "\n" + single_msg, f"batch{idx}:degraded")
            out.append(resolve_output(
                call(single_msg, f"batch{idx}:single", system=single_prompt), vocab))
        return out

    n_done = 0
    chunk_iter = iter(enumerate(chunks))
    with open(ckpt_path, "a", encoding="utf-8") as ck, \
            ThreadPoolExecutor(max_workers=max_workers) as ex:
        pending: Dict[Any, Any] = {}

        def submit_next() -> bool:
            # ceiling checked before EVERY submit, so at most max_workers
            # batches are in flight past the ceiling, never the whole queue
            for idx, chunk in chunk_iter:
                if spent(cost_log) >= ceiling:
                    print(f"cost ceiling {ceiling} reached; remaining batches "
                          "skipped (resume with a higher ceiling)")
                    return False
                pending[ex.submit(do_chunk, idx, chunk)] = chunk
                return True
            return False

        for _ in range(max_workers):
            if not submit_next():
                break
        while pending:
            fut = next(as_completed(pending))
            chunk = pending.pop(fut)
            for (it, _case), res in zip(chunk, fut.result()):
                ck.write(json.dumps({
                    **it,
                    "prompt_sha": task["prompt_sha"],
                    "batch_prompt_sha": task["batch_prompt_sha"],
                    **res,
                }, ensure_ascii=False) + "\n")
                n_done += 1
            ck.flush()
            submit_next()
    return n_done


def spent(cost_log_path: str) -> float:
    total = 0.0
    if os.path.isfile(cost_log_path):
        with open(cost_log_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    total += float(json.loads(line).get("cost") or 0.0)
    return total


def resolve_output(raw: Optional[Mapping[str, Any]],
                   vocab: Sequence[str] = VERDICTS) -> Dict[str, Optional[str]]:
    """Fail closed to not_checked on anything malformed or out of vocabulary."""
    empty = {"verdict": "not_checked", "confidence": None, "doubt": None, "reason": None}
    if raw is None:
        # transport exhausted its retries -- NOT an abstention: never resumed
        # as done, never scored
        return {**empty, "verdict": "transport_failed"}
    if not isinstance(raw, Mapping):
        return dict(empty)
    if raw.get("abstain") is True:
        return {**empty, "reason": str(raw.get("reason") or "")[:200]}
    v = raw.get("verdict")
    if v not in vocab:
        return dict(empty)
    conf = raw.get("confidence")
    return {"verdict": v,
            "confidence": conf if conf in ("high", "medium", "low") else None,
            "doubt": str(raw.get("doubt") or "")[:200] or None,
            "reason": str(raw.get("reason") or "")[:200]}


# ---------------------------------------------------------------------------
# Gold set: owner-graded cards sitting on divergent pairs
# ---------------------------------------------------------------------------

GENUINE_GRADES = {"correct", "cowitness", "partial"}
NOT_WITNESS_GRADES = {"quote_ab", "quote_ba", "quote_shared", "formula", "wrong", "junk"}
GENUINE_VERDICTS = {"both_right_multiple_works", "catalogue_too_general",
                    "computed_right_catalogue_mismatch"}
NOT_WITNESS_VERDICTS = {"catalogue_right_match_is_quotation",
                        "catalogue_right_claim_mistaken"}

# ---------------------------------------------------------------------------
# Task registry: one gate runner, two pinned questions. `genuine` is the
# verdict subset that ENDORSES the computed identification as genuinely on
# the page -- the axis the owner's relation grades can score.
# (`actually_recorded` endorses the identification while denying novelty, so
# it counts as genuine for the witness axis and is reported separately.)
# ---------------------------------------------------------------------------

TASKS: Dict[str, Dict[str, Any]] = {
    "divergence": {
        "single_prompt": PROMPT_TEMPLATE,
        "batch_prompt": BATCH_PROMPT_TEMPLATE,
        "prompt_sha": PROMPT_SHA256,
        "batch_prompt_sha": BATCH_PROMPT_SHA256,
        "vocab": VERDICTS,
        "genuine": GENUINE_VERDICTS,
        "novelty_statuses": ("diverges_work", "diverges_part"),
    },
    "new_finds": {
        "single_prompt": NEW_FINDS_PROMPT_TEMPLATE,
        "batch_prompt": NEW_FINDS_BATCH_PROMPT_TEMPLATE,
        "prompt_sha": NEW_FINDS_PROMPT_SHA256,
        "batch_prompt_sha": hashlib.sha256(
            NEW_FINDS_BATCH_PROMPT_TEMPLATE.encode("utf-8")).hexdigest(),
        "vocab": NEW_FINDS_VERDICTS,
        "genuine": {"credible_new_identification", "plausible_needs_expert_check",
                    "actually_recorded"},
        "novelty_statuses": ("fills_gap", "extends"),
    },
}


def load_gold(con: sqlite3.Connection, crosswalk_path: str,
              novelty_statuses: Tuple[str, ...] = ("diverges_work", "diverges_part"),
              ) -> List[Dict[str, Any]]:
    ns_sql = "(" + ",".join("?" * len(novelty_statuses)) + ")"
    cw = json.load(open(crosswalk_path, encoding="utf-8"))
    entries: List[Tuple[str, str, str, str]] = []
    for src, path in GOLD_SOURCES:
        if not os.path.isfile(path):
            continue
        for r in json.load(open(path, encoding="utf-8")):
            pid, raw = r["id"].split("|", 1)
            wid = cw.get(raw)
            if wid:
                entries.append((pid, wid, r["grade"], src))
    # fresh decks are evidence_id-keyed
    fresh: List[Tuple[str, str]] = []
    for path in FRESH_DECK_GRADES:
        if not os.path.isfile(path):
            continue
        d = json.load(open(path, encoding="utf-8"))
        gg = d["grades"] if isinstance(d, dict) and "grades" in d else d
        if isinstance(gg, list):
            for r in gg:
                eid = r.get("id") or r.get("evidence_id") or r.get("identification_id")
                if eid and r.get("grade"):
                    fresh.append((eid, r["grade"]))
        elif isinstance(gg, Mapping):
            for eid, v in gg.items():
                fresh.append((eid, v if isinstance(v, str) else v.get("grade")))
    for eid, grade in fresh:
        row = con.execute(
            f"""SELECT r.page_id, r.work_id FROM review_row r
               JOIN facet_row f ON f.evidence_id = r.evidence_id
               WHERE r.evidence_id = ?
                 AND f.novelty_status IN {ns_sql}""",
            (eid, *novelty_statuses)).fetchone()
        if row:
            entries.append((row["page_id"], row["work_id"], grade, "fresh_deck"))

    # keep only entries whose pair IS divergent in v5 -- ONE temp-table join
    # (review_row has no page_id index; per-entry probes take minutes)
    con.execute("CREATE TEMP TABLE IF NOT EXISTS _gold_probe"
                "(page_id TEXT, work_id TEXT, PRIMARY KEY(page_id, work_id))")
    con.execute("DELETE FROM _gold_probe")
    con.executemany("INSERT OR IGNORE INTO _gold_probe VALUES (?,?)",
                    [(p, w) for p, w, _, _ in entries])
    divergent = {(p, w) for p, w in con.execute(
        f"""SELECT DISTINCT g.page_id, g.work_id FROM _gold_probe g
           JOIN review_row r ON r.work_id = g.work_id AND r.page_id = g.page_id
           JOIN facet_row f ON f.evidence_id = r.evidence_id
           WHERE f.novelty_status IN {ns_sql}""", novelty_statuses)}
    gold: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for pid, wid, grade, src in entries:
        if (pid, wid) not in divergent:
            continue
        key = (pid, wid)
        g = gold.setdefault(key, {"page_id": pid, "work_id": wid,
                                  "grades": [], "sources": set()})
        g["grades"].append(grade)
        g["sources"].add(src)
    out = []
    for g in gold.values():
        grades = set(g["grades"])
        if grades & GENUINE_GRADES and grades & NOT_WITNESS_GRADES:
            label = "mixed"
        elif grades & GENUINE_GRADES:
            label = "genuine"
        elif grades & NOT_WITNESS_GRADES:
            label = "not_witness"
        else:
            label = "unsure"
        g["gold"] = label
        g["sources"] = sorted(g["sources"])
        out.append(g)
    return out


def pair_stats(con: sqlite3.Connection, page_id: str, work_id: str) -> Dict[str, Any]:
    row = con.execute(
        """SELECT COUNT(DISTINCT r2.page_id) AS n_pages
           FROM review_row r1 JOIN review_row r2
             ON r2.sys_id = r1.sys_id AND r2.work_id = r1.work_id
           WHERE r1.page_id = ? AND r1.work_id = ?""",
        (page_id, work_id)).fetchone()
    return {"n_pages": row["n_pages"] if row else 1}


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def cmd_eval_gold(args) -> int:
    os.makedirs(args.out_dir, exist_ok=True)
    task = TASKS[args.task]
    con = sqlite3.connect(f"file:{args.review_db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    t0 = time.time()
    gold = load_gold(con, args.crosswalk, task["novelty_statuses"])
    print(f"[{time.time()-t0:.1f}s] gold loaded", flush=True)
    usable = [g for g in gold if g["gold"] in ("genuine", "not_witness")]
    print(f"gold ({args.task}): {len(gold)} graded cases, {len(usable)} usable "
          f"(genuine={sum(1 for g in usable if g['gold']=='genuine')}, "
          f"not_witness={sum(1 for g in usable if g['gold']=='not_witness')}, "
          f"mixed={sum(1 for g in gold if g['gold']=='mixed')}, "
          f"unsure={sum(1 for g in gold if g['gold']=='unsure')})")

    aids = AidsIndex(args.libraries_csv, args.fjms_db, args.pgp_db)
    print(f"[{time.time()-t0:.1f}s] aids sidecars loaded", flush=True)
    scan = make_masking_scanner()

    model_slug = args.model.replace("/", "_").replace(".", "_")
    # the original divergence eval predates task-prefixed names; keep its files
    prefix = "" if args.task == "divergence" else f"{args.task}_"
    cost_log = os.path.join(args.out_dir, f"cost_{prefix}{model_slug}.jsonl")
    ckpt_path = os.path.join(args.out_dir, f"eval_{prefix}{model_slug}.jsonl")

    if args.dry_run:
        sample_path = os.path.join(args.out_dir, f"dry_run_{args.task}_cases.json")
        rendered = []
        for g in usable[:args.dry_run]:
            case = build_case(con, aids, g["page_id"], g["work_id"],
                              pair_stats(con, g["page_id"], g["work_id"]))
            if case is None:
                continue
            msg = render_user_message(case)
            scan(task["single_prompt"] + "\n" + msg, f"dry:{g['page_id']}")
            rendered.append({"gold": g["gold"], "grades": g["grades"], "case": case})
        with open(sample_path, "w", encoding="utf-8") as fh:
            json.dump(rendered, fh, ensure_ascii=False, indent=1)
        print(f"dry run: {len(rendered)} cases rendered + masking-scanned -> {sample_path}")
        return 0

    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key and os.path.isfile(os.path.join(REPO_ROOT, ".env")):
        for line in open(os.path.join(REPO_ROOT, ".env"), encoding="utf-8"):
            if line.strip().startswith("OPENROUTER_API_KEY"):
                api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not api_key:
        print("ERROR: OPENROUTER_API_KEY not set and not in .env")
        return 2
    if args.cost_ceiling is None:
        print("ERROR: --cost-ceiling is required for a live run")
        return 2

    call = make_call(api_key, args.model, cost_log)

    done = set()
    if os.path.isfile(ckpt_path):
        with open(ckpt_path, encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    rec = json.loads(line)
                    if (rec.get("prompt_sha") == task["prompt_sha"]
                            and rec.get("verdict") != "transport_failed"):
                        done.add((rec["page_id"], rec["work_id"]))

    items = [{"page_id": g["page_id"], "work_id": g["work_id"], "gold": g["gold"],
              "grades": g["grades"], "sources": g["sources"], "model": args.model}
             for g in usable if (g["page_id"], g["work_id"]) not in done]
    n_run = run_batched(items, call, scan, con, aids,
                        batch_size=args.batch_size, max_workers=args.max_workers,
                        cost_log=cost_log, ceiling=args.cost_ceiling,
                        ckpt_path=ckpt_path, task=task)

    # score
    recs = [json.loads(l) for l in open(ckpt_path, encoding="utf-8") if l.strip()]
    recs = [r for r in recs if r.get("prompt_sha") == task["prompt_sha"]]
    by_key = {(r["page_id"], r["work_id"]): r for r in recs}
    agree = disagree = abstain = 0
    confusion: Dict[Tuple[str, str], int] = {}
    for g in usable:
        r = by_key.get((g["page_id"], g["work_id"]))
        if not r:
            continue
        v = r["verdict"]
        if v == "transport_failed":
            continue
        confusion[(g["gold"], v)] = confusion.get((g["gold"], v), 0) + 1
        if v == "not_checked":
            abstain += 1
        elif (g["gold"] == "genuine") == (v in task["genuine"]):
            agree += 1
        else:
            disagree += 1
    total = agree + disagree
    print(f"\ntask={args.task} model={args.model} prompt={task['prompt_sha'][:12]} "
          f"calls_this_run={n_run} spend_total=${spent(cost_log):.4f}")
    if total:
        print(f"AGREEMENT: {agree}/{total} = {100.0*agree/total:.1f}%  "
              f"(abstained: {abstain})")
    print("confusion (gold -> verdict):")
    for (gold_l, v), n in sorted(confusion.items(), key=lambda kv: -kv[1]):
        print(f"  {gold_l:11s} -> {v:38s} {n}")
    return 0


def cmd_run(args) -> int:
    """Production run at PAIR grain over the chosen pools: one judgment per
    (sys_id, work_id), on the pair's strongest page (highest matched_letters,
    tie -> smallest page_id -- the project's standard selector order)."""
    os.makedirs(args.out_dir, exist_ok=True)
    task = TASKS[args.task]
    con = sqlite3.connect(f"file:{args.review_db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    pools = tuple(p.strip() for p in args.pools.split(",") if p.strip())
    ns = task["novelty_statuses"]
    q = f"""SELECT sys_id, work_id, page_id, triage, novelty_status FROM (
        SELECT f.sys_id, f.work_id, r.page_id, f.triage, f.novelty_status,
               ROW_NUMBER() OVER (PARTITION BY f.sys_id, f.work_id
                 ORDER BY f.matched_letters DESC, r.page_id ASC) rn
        FROM facet_row f JOIN review_row r ON r.evidence_id = f.evidence_id
        WHERE f.novelty_status IN ({','.join('?'*len(ns))})
          AND f.triage IN ({','.join('?'*len(pools))})
    ) WHERE rn = 1"""
    pairs = [dict(r) for r in con.execute(q, (*ns, *pools))]
    print(f"task={args.task} pools={pools}: {len(pairs)} pairs", flush=True)

    model_slug = args.model.replace("/", "_").replace(".", "_")
    cost_log = os.path.join(args.out_dir, f"run_cost_{args.task}_{model_slug}.jsonl")
    ckpt_path = os.path.join(args.out_dir, f"run_{args.task}_{model_slug}.jsonl")

    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key and os.path.isfile(os.path.join(REPO_ROOT, ".env")):
        for line in open(os.path.join(REPO_ROOT, ".env"), encoding="utf-8"):
            if line.strip().startswith("OPENROUTER_API_KEY"):
                api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not api_key:
        print("ERROR: OPENROUTER_API_KEY not set and not in .env")
        return 2
    if args.cost_ceiling is None:
        print("ERROR: --cost-ceiling is required for a live run")
        return 2

    aids = AidsIndex(args.libraries_csv, args.fjms_db, args.pgp_db)
    scan = make_masking_scanner()
    call = make_call(api_key, args.model, cost_log)

    done = set()
    if os.path.isfile(ckpt_path):
        with open(ckpt_path, encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    rec = json.loads(line)
                    if (rec.get("prompt_sha") == task["prompt_sha"]
                            and rec.get("verdict") != "transport_failed"):
                        done.add((rec["page_id"], rec["work_id"]))
    items = [{**p, "model": args.model} for p in pairs
             if (p["page_id"], p["work_id"]) not in done]
    print(f"{len(done)} already done, {len(items)} to run", flush=True)
    t0 = time.time()
    n_run = run_batched(items, call, scan, con, aids,
                        batch_size=args.batch_size, max_workers=args.max_workers,
                        cost_log=cost_log, ceiling=args.cost_ceiling,
                        ckpt_path=ckpt_path, task=task)
    # verdict distribution over the whole checkpoint
    from collections import Counter
    dist = Counter()
    for line in open(ckpt_path, encoding="utf-8"):
        if line.strip():
            rec = json.loads(line)
            if rec.get("prompt_sha") == task["prompt_sha"]:
                dist[rec["verdict"]] += 1
    print(f"\ntask={args.task} model={args.model} ran={n_run} in "
          f"{(time.time()-t0)/60:.1f} min, spend_total=${spent(cost_log):.2f}")
    for v, n in dist.most_common():
        print(f"  {v:38s} {n}")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--review-db", default=DEFAULT_REVIEW_DB)
    p.add_argument("--crosswalk", default=DEFAULT_CROSSWALK)
    p.add_argument("--libraries-csv", default=DEFAULT_LIBRARIES_CSV)
    p.add_argument("--fjms-db", default=DEFAULT_FJMS_DB)
    p.add_argument("--pgp-db", default=DEFAULT_PGP_DB)
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    p.add_argument("--model", default=LLM_MODEL_DEFAULT)
    p.add_argument("--task", default="divergence", choices=sorted(TASKS))
    p.add_argument("--eval-gold", action="store_true")
    p.add_argument("--run", action="store_true",
                   help="production run at pair grain over --pools")
    p.add_argument("--pools", default="main,unclear",
                   help="comma-separated triage pools for --run")
    p.add_argument("--dry-run", type=int, default=0,
                   help="render N cases to a file, no network")
    p.add_argument("--cost-ceiling", type=float, default=None,
                   help="HARD ceiling in USD; required for live runs")
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                   help="cases per call (the novelty gate's knee is 10)")
    p.add_argument("--max-workers", type=int, default=4,
                   help="concurrent provider calls")
    args = p.parse_args(argv)
    if args.run:
        return cmd_run(args)
    if args.eval_gold or args.dry_run:
        return cmd_eval_gold(args)
    p.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
