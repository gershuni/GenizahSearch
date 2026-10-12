"""Interruptible query matching with a shared, cooperative search deadline.

Only query patterns use this adapter. Patterns retain Python ``re`` syntax and
word classes; regex's broader Unicode word definition would change Hebrew hits.
The deadline bounds matching and is checked between compilation steps; native
compilation and arbitrary blocking native work are not interruptible in process.

Patterns the engine builds itself from escaped literals (compile_generated) may
instead match with stdlib ``re``, which is ~60x faster on Extended/Maximum
prefix trees but holds the GIL: only shapes whose cost per start position is
bounded, under a subject cap derived from that cost, and never a user's own
Regex. docs/plans/DESKTOP_NATIVE_MATCHING_PLAN.md has the rules and the review.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from functools import lru_cache, wraps
import math
import os
import re
import sys
import time

import regex as _regex


class SearchBudgetExceeded(TimeoutError):
    """The search must abort rather than report silently incomplete results."""

    def __init__(self):
        super().__init__(
            "Search exceeded its time limit. Please narrow your query and try again."
        )


_deadline = ContextVar("search_regex_deadline", default=None)
_isolated = ContextVar("search_regex_isolated", default=False)
_native_matching = ContextVar("search_regex_native_matching", default=False)


@contextmanager
def isolated_matching(*, native=False):
    """Only for disposable, externally supervised search subprocesses.

    Their parent enforces resource limits and can kill native work, so elapsed
    matching time need not reject a legitimate query. UI highlighting may still
    establish its own explicit short budget inside this context.
    Set native=True only after installing GIL-independent parent-death protection.
    Engine-generated patterns (compile_generated) are exempt from the regex
    route outside this context and inside it with native=True; with
    native=False (Windows workers) they are compiled and routed as any other.
    """
    token = _isolated.set(True)
    native_token = _native_matching.set(native)
    try:
        with search_budget(0):
            yield
    finally:
        _native_matching.reset(native_token)
        _isolated.reset(token)


def _seconds(value, env_name, default, *, allow_zero=False):
    if value is None:
        value = os.environ.get(env_name, default)
    try:
        value = float(value)
    except (TypeError, ValueError):
        return default
    valid = value >= 0 if allow_zero else value > 0
    return value if math.isfinite(value) and valid else default


@contextmanager
def search_budget(seconds=None):
    """Apply an optional deadline; nested operations cannot extend it.

    Interactive research searches have no total deadline by default. A zero
    budget disables only the total deadline, never the per-operation guard.
    Explicit API and highlighting budgets still bound their worker operations.
    """
    previous = _deadline.get()
    now = time.monotonic()
    if previous is not None and now >= previous:
        raise SearchBudgetExceeded()
    # An implicit nested engine budget inherits the caller's policy. Only an
    # explicitly requested nested limit (e.g. highlighting) may shorten it.
    if seconds is None and previous is not None:
        seconds = 0
    duration = _seconds(seconds, "GENIZAH_SEARCH_BUDGET_SECONDS", 0.0, allow_zero=True)
    deadline = now + duration if duration else math.inf
    if previous is not None:
        deadline = min(previous, deadline)
    token = _deadline.set(deadline)
    try:
        yield
        if time.monotonic() >= _deadline.get():
            raise SearchBudgetExceeded()
    finally:
        _deadline.reset(token)


def bounded_search(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        with search_budget():
            return function(*args, **kwargs)
    return wrapped


def _match_timeout():
    deadline = _deadline.get()
    if _isolated.get():
        if deadline is None or math.isinf(deadline):
            return None
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SearchBudgetExceeded()
        return remaining
    limit = _seconds(None, "GENIZAH_REGEX_TIMEOUT_SECONDS", 10.0)
    if deadline is not None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SearchBudgetExceeded()
        limit = min(limit, remaining)
    return limit


def _check_deadline():
    deadline = _deadline.get()
    if deadline is not None and time.monotonic() >= deadline:
        raise SearchBudgetExceeded()


# One native (stdlib re) call may hold the GIL for about this long on degenerate
# text: an empirical responsiveness target, not a guarantee across machines.
# The constants were measured 2026-10-12 at 12-17 ns per tree level and 1.1-1.7
# ns per sibling try at full CPU clock, and up to ~30 ns a level under sustained
# load: at its cap a degenerate run took 0.5-1.1 s. compile_generated turns a
# pattern's cost per start position into its subject cap.
HOLD_BUDGET_S = 1.0
# Below this cap the native route is not worth its bookkeeping: plain compile().
NATIVE_MIN_SUBJECT_CHARS = 10_000

_cancel_check = ContextVar("search_regex_cancel_check", default=None)


@contextmanager
def cancel_check(predicate):
    """Cooperative Stop for native matching: *predicate()* True raises
    InterruptedError before the next generated call. A stdlib call cannot be
    interrupted, so this bounds the time from a Stop click to the next
    checkpoint by one call; it does not shorten a call and does not make
    QThread.terminate() safe. Set by the desktop search threads around their
    engine call."""
    token = _cancel_check.set(predicate)
    try:
        yield
    finally:
        _cancel_check.reset(token)


def _check_cancel():
    predicate = _cancel_check.get()
    if predicate is not None and predicate():
        raise InterruptedError("Search cancelled by user")



_UNICODE_BLOCK_SIZE = 4096


@lru_cache(maxsize=(sys.maxunicode + _UNICODE_BLOCK_SIZE) // _UNICODE_BLOCK_SIZE)
def _word_delta_block(start):
    """Retain completed blocks when a caller's initialization budget expires.

    Each immutable result depends only on the two Unicode databases. Keeping
    these small checkpoints lets later renders finish initialization without
    either restarting the scan or ignoring the current render's deadline.
    """
    block = ''.join(map(chr, range(start, min(start + _UNICODE_BLOCK_SIZE, sys.maxunicode + 1))))
    removed = tuple(map(ord, _regex.findall(
        r"[\p{L}\p{N}_]", re.sub(r"\w+", '', block), _regex.VERSION0,
    )))
    added = tuple(map(ord, _regex.findall(
        r"[^\p{L}\p{N}_]", re.sub(r"\W+", '', block), _regex.VERSION0,
    )))
    return added, removed


@lru_cache(maxsize=1)
def _word_predicates():
    r"""Compact word atoms with the running Python's Unicode semantics.

    Native L/N properties avoid compiling hundreds of ranges at every escape.
    Only differences between the two Unicode databases need explicit ranges.
    Disable case folding for these atoms: re's \w is unaffected by IGNORECASE.
    No captures or VERSION1 set syntax are introduced into user patterns.
    """
    added, removed = [], []
    # Compare in small blocks: bounded temporary memory, deadline checkpoints,
    # and native scans instead of over a million Python/native match calls.
    for start in range(0, sys.maxunicode + 1, _UNICODE_BLOCK_SIZE):
        _check_deadline()
        block_added, block_removed = _word_delta_block(start)
        added.extend(block_added)
        removed.extend(block_removed)

    def ranges(points):
        result = []
        i = 0
        while i < len(points):
            first = last = points[i]
            i += 1
            while i < len(points) and points[i] == last + 1:
                last = points[i]
                i += 1
            result.append(f"\\U{first:08x}" + (f"-\\U{last:08x}" if last != first else ""))
        return "".join(result)

    word = r"[\p{L}\p{N}_]"
    nonword = r"[^\p{L}\p{N}_]"
    if removed:
        word = "(?![" + ranges(removed) + "])" + word
        nonword = "(?:" + nonword + "|[" + ranges(removed) + "])"
    if added:
        word = "(?:" + word + "|[" + ranges(added) + "])"
        nonword = "(?![" + ranges(added) + "])" + nonword
    return "(?u-i:" + word + ")", "(?u-i:" + nonword + ")"


def _rewrite_class(pattern, start, word, nonword):
    """Rewrite a validated simple class as a union of single-character atoms."""
    i = start + 1
    negated = pattern[i] == "^"
    if negated:
        i += 1
    first = i
    residual, atoms = [], []
    while i < len(pattern):
        char = pattern[i]
        if char == "]" and i != first:
            break
        if char == "\\":
            escape = pattern[i + 1]
            if escape in "wW":
                atoms.append(word if escape == "w" else nonword)
            else:
                residual.append(pattern[i:i + 2])
            i += 2
            continue
        # Removing a word escape must not turn a literal into a range or
        # move a literal caret/closing bracket into a special position.
        if char in "^][" or (char == "-" and (i == first or pattern[i + 1] == "]")):
            char = "\\" + char
        residual.append(char)
        i += 1
    if not atoms:
        return pattern[start:i + 1], i + 1
    atoms = list(dict.fromkeys(atoms))
    if len(atoms) == 2:
        # A class containing both \w and \W includes every character.
        return (r'(?!)' if negated else r'[\s\S]'), i + 1
    if negated:
        complement = nonword if atoms[0] == word else word
        if residual:
            complement = '(?:(?![' + ''.join(residual) + '])' + complement + ')'
        return complement, i + 1
    if residual:
        # Express the union as NOT(NOT atom AND NOT residual). Alternation
        # lets regex's first-set optimization fold branches across -i, e.g.
        # [\Wİ] under IGNORECASE can lose the nonword U+0345. Keep both tests
        # inside assertions, then consume exactly one character.
        return ('(?:(?!(?!' + atoms[0] + ')(?![' + ''.join(residual)
                + r']))[\s\S])'), i + 1
    union = "(?:" + "|".join(atoms) + ")"
    return union, i + 1


_INLINE_FLAGS = re.compile(r"\(\?([aiLmsux]*)(?:-([imsx]+))?([:)])")
_ASCII_WORD = r"(?a-i:[a-zA-Z0-9_])"
_ASCII_NONWORD = r"(?a-i:[^a-zA-Z0-9_])"


def _preserve_word_classes(pattern, flags):
    r"""Rewrite lexical word escapes, respecting classes, comments and flags.

    Input was validated by stdlib re first. Single-character predicates keep
    VERSION0's simple sets while preserving re's definition even for [^\w...].
    """
    if not any(token in pattern for token in (r"\w", r"\W", r"\b", r"\B")):
        return pattern
    word, nonword = _word_predicates()
    result = []
    ascii_mode = bool(flags & re.ASCII)
    verbose = bool(flags & re.VERBOSE)
    stack = []
    i = 0
    while i < len(pattern):
        char = pattern[i]
        if char == "\\" and i + 1 < len(pattern):
            escape = pattern[i + 1]
            if escape in "wW":
                w, nw = (_ASCII_WORD, _ASCII_NONWORD) if ascii_mode else (word, nonword)
                result.append(w if escape == "w" else nw)
            elif escape in "bB":
                w = _ASCII_WORD if ascii_mode else word
                boundary = rf"(?:(?<!{w})(?={w})|(?<={w})(?!{w}))"
                if escape == "b":
                    result.append(boundary)
                else:
                    # Python <3.14 \B does not match an empty input.
                    nonempty = r"(?=[\s\S]|(?<=[\s\S]))" if sys.version_info < (3, 14) else ""
                    result.append(nonempty + "(?!" + boundary + ")")
            else:
                result.append(pattern[i:i + 2])
            i += 2
            continue
        if verbose and char == "#":
            end = pattern.find("\n", i)
            if end == -1:
                result.append(pattern[i:])
                break
            result.append(pattern[i:end + 1])
            i = end + 1
            continue
        if pattern.startswith("(?#", i):
            end = i + 3
            while end < len(pattern):
                if pattern[end] == "\\":
                    end += 2
                elif pattern[end] == ")":
                    break
                else:
                    end += 1
            result.append(pattern[i:end + 1])
            i = end + 1
            continue
        if char == "[":
            w, nw = (_ASCII_WORD, _ASCII_NONWORD) if ascii_mode else (word, nonword)
            rewritten, i = _rewrite_class(pattern, i, w, nw)
            result.append(rewritten)
            continue
        elif char == "(":
            match = _INLINE_FLAGS.match(pattern, i)
            if match:
                on, off, closing = match.groups()
                if closing == ":":
                    stack.append((ascii_mode, verbose))
                if "a" in on:
                    ascii_mode = True
                if "u" in on:
                    ascii_mode = False
                if "x" in on:
                    verbose = True
                if "x" in (off or ""):
                    verbose = False
                result.append(match.group())
                i = match.end()
                continue
            stack.append((ascii_mode, verbose))
        elif char == ")" and stack:
            ascii_mode, verbose = stack.pop()
        result.append(char)
        i += 1
    return "".join(result)


class Pattern:
    def __init__(self, compiled, original, flags, stdlib=None, *, generated=False,
                 native_max_chars=None, shared_chars=None, native_max_chars_shared=None):
        # The regex-module object. None for a generated pattern until a call falls
        # back to the interruptible route (see _regex_object).
        self._compiled = compiled
        # compile() hands over the pattern it validated: compiling it again cost a
        # second full compile (re's cache misses: its flags now include UNICODE).
        self._stdlib = stdlib if stdlib is not None else re.compile(original, flags)
        self.pattern = original
        self.flags = flags
        # Engine-generated (compile_generated): matches with stdlib re in this
        # process when the subject is within native_max_chars -- or, for a
        # multi-part pattern whose subject holds one of shared_chars, within the
        # smaller native_max_chars_shared; else the regex route.
        self._generated = generated
        self.native_max_chars = native_max_chars
        self._shared_chars = shared_chars
        self.native_max_chars_shared = native_max_chars_shared

    def _regex_object(self):
        """The regex-module object, compiled on first need for a generated pattern."""
        if self._compiled is None:
            _check_deadline()
            self._compiled = _regex_compile(self._stdlib)
            _check_deadline()
        return self._compiled

    def _native_allowed(self, name, args, kwargs):
        """Whether this generated call may run on stdlib re (which holds the GIL).

        sub() never does (its subject is the second argument and no accelerated
        site substitutes). Supervised Linux workers always do, unbounded, as
        today; Windows workers never (their watchdog is a Python thread). In
        process the subject's effective range must be within native_max_chars
        (the per-pattern cap from its cost per start position, so one call holds
        the GIL for about HOLD_BUDGET_S at most on degenerate text). A
        multi-part pattern's subject holding a character the inserter and
        separator classes share (readable as inside a form or between two parts
        at every boundary, so a failing match retries their combinations) gets
        the smaller cap compile_generated derived for that case.
        """
        if name == 'sub':
            return False
        if _isolated.get():
            return bool(_native_matching.get())
        string = args[0]
        pos = kwargs.get('pos', 0)
        endpos = kwargs.get('endpos')
        end = len(string) if endpos is None else min(endpos, len(string))
        cap = self.native_max_chars
        if end - pos > cap:
            return False
        if self._shared_chars is not None and self._shared_chars.search(string, pos, end) is not None:
            cap = self.native_max_chars_shared
        return end - pos <= cap

    def _call(self, name, *args, **kwargs):
        if self._generated:
            # An expired budget raises before any work; Stop is honoured between
            # calls (a stdlib call cannot be interrupted, so this is where a
            # click lands); then the route.
            _check_deadline()
            _check_cancel()
            if self._native_allowed(name, args, kwargs):
                return getattr(self._stdlib, name)(*args, **kwargs)
            compiled = self._regex_object()
            # The remaining allowance is read AFTER the lazy compile, so its time
            # cannot leave a stale timeout.
            timeout = _match_timeout()
            try:
                return getattr(compiled, name)(*args, concurrent=True, timeout=timeout, **kwargs)
            except TimeoutError as exc:
                raise SearchBudgetExceeded() from exc
        # Disposable workers are supervised by another process, which can kill
        # them even during native matching. Use Python's original matcher there:
        # large transcriptions with variant alternations are much slower in
        # regex. Explicit nested budgets (e.g. highlighting) still need regex's
        # interruptible operations, as do all other calls in the web/desktop
        # process -- except engine-generated patterns, which compile_generated
        # routes above.
        timeout = _match_timeout()
        if _isolated.get() and _native_matching.get() and timeout is None:
            return getattr(self._stdlib, name)(*args, **kwargs)
        try:
            return getattr(self._compiled, name)(*args, concurrent=True, timeout=timeout, **kwargs)
        except TimeoutError as exc:
            raise SearchBudgetExceeded() from exc

    def search(self, string, pos=0, endpos=None):
        kwargs = {} if endpos is None else {"endpos": endpos}
        return self._call("search", string, pos=pos, **kwargs)

    def match(self, string, pos=0, endpos=None):
        kwargs = {} if endpos is None else {"endpos": endpos}
        return self._call("match", string, pos=pos, **kwargs)

    def fullmatch(self, string, pos=0, endpos=None):
        kwargs = {} if endpos is None else {"endpos": endpos}
        return self._call("fullmatch", string, pos=pos, **kwargs)

    def sub(self, repl, string, count=0):
        return self._call("sub", repl, string, count=count)

    @property
    def groups(self):
        # The \w rewrite adds only non-capturing constructs, so the stdlib
        # object's count is the regex object's; a generated pattern has no
        # regex object to ask.
        return self._stdlib.groups

    @property
    def groupindex(self):
        return self._stdlib.groupindex


_REGEX_FLAGS = (
    (re.ASCII, _regex.ASCII), (re.IGNORECASE, _regex.IGNORECASE),
    (re.MULTILINE, _regex.MULTILINE), (re.DOTALL, _regex.DOTALL),
    (re.VERBOSE, _regex.VERBOSE), (re.UNICODE, _regex.UNICODE),
)


def _regex_compile(validated):
    """The regex-module object for a validated stdlib pattern: the same flags,
    the word escapes rewritten to re's definitions. Native compilation cannot
    be interrupted; callers check the deadline around it."""
    mapped = _regex.VERSION0
    for old, new in _REGEX_FLAGS:
        if validated.flags & old:
            mapped |= new
    rewritten = _preserve_word_classes(validated.pattern, validated.flags)
    _check_deadline()
    try:
        return _regex.compile(rewritten, mapped)
    except _regex.error as exc:
        raise re.error(str(exc)) from exc


def _validate(pattern, flags):
    validated = re.compile(pattern, flags)
    if not isinstance(validated.pattern, str):
        raise TypeError("search patterns must be strings")
    return validated


def compile(pattern, flags=0):
    """Compile stdlib syntax with bounded, GIL-releasing match operations."""
    # Native compilation itself cannot be interrupted here. Check on both
    # sides so an expired API budget cannot compile an entire chunk batch.
    _check_deadline()
    if isinstance(pattern, Pattern):
        if flags:
            raise ValueError("cannot process flags argument with a compiled pattern")
        return pattern
    validated = _validate(pattern, flags)
    compiled = _regex_compile(validated)
    _check_deadline()
    return Pattern(compiled, validated.pattern, validated.flags, stdlib=validated)


def compile_generated(pattern, flags=0, *, cost_per_start_ns, parts=1, shared_chars=None):
    """Compile a pattern the ENGINE built from escaped literals, to match with
    stdlib re where that is safe -- about 60x faster than the regex module on
    Extended/Maximum prefix trees (2026-10-12).

    The caller vouches for the shape (see search_engine.build_regex_pattern:
    gap 0, word characters only, caseless, prefix trees, forms <= 64 chars) and
    passes its cost per start position in nanoseconds (from the built trees:
    levels and sibling tries) -- the subject cap is HOLD_BUDGET_S over that, so
    one native call holds the GIL for about the budget at most on degenerate
    text (an empirical target, not a guarantee across machines). *shared_chars*
    is the compiled intersection of the inserter and separator classes; on a
    subject holding one, a multi-part pattern (*parts* > 1) gets a cap smaller
    by 2**(parts-2): measured 2026-10-12 on degenerate subjects with a shared
    character at every boundary, a failing match costs x0.5 the model at 2
    parts, x1 at 3, x1.7 at 4, x2.9 at 5, x4.9 at 6, x15-20 at 8 (the length
    of the run of shared characters does not matter), so that factor keeps a
    margin of at least 2x. Where the cap would be under NATIVE_MIN_SUBJECT_CHARS, or in a
    Windows worker (isolated without parent-death protection, whose watchdog is
    a Python thread), this is plain compile(): today's object, today's route.
    A pattern is built and consumed inside one isolation context; composition
    caches patterns within a run only.
    """
    _check_deadline()
    if isinstance(pattern, Pattern):
        if flags:
            raise ValueError("cannot process flags argument with a compiled pattern")
        return pattern
    if _isolated.get() and not _native_matching.get():
        return compile(pattern, flags)
    if cost_per_start_ns <= 0:
        raise ValueError("cost_per_start_ns must be positive")
    native_max_chars = int(HOLD_BUDGET_S / (cost_per_start_ns * 1e-9))
    if native_max_chars < NATIVE_MIN_SUBJECT_CHARS:
        return compile(pattern, flags)
    validated = _validate(pattern, flags)
    _check_deadline()
    return Pattern(None, validated.pattern, validated.flags, stdlib=validated, generated=True,
                   native_max_chars=native_max_chars,
                   shared_chars=shared_chars if parts > 1 else None,
                   native_max_chars_shared=native_max_chars >> max(0, parts - 2))


@lru_cache(maxsize=1)
def _every_character():
    return "".join(map(chr, range(sys.maxunicode + 1)))


@lru_cache(maxsize=4096)
def matched_chars(atom, flags=0):
    """Every character a single-character pattern matches, under both engines.

    A user's Regex pattern matches with ``regex`` in process and may match with
    stdlib ``re`` in a supervised worker (engine-generated patterns match with
    ``re`` more widely, but they never take the prefilter). A candidate
    prefilter must allow either, so return the union.
    Surrogates are dropped: indexed text is valid UTF-8 and cannot contain them.
    """
    compiled = compile(atom, flags)
    text = _every_character()
    found = set(compiled._compiled.findall(text)) | set(compiled._stdlib.findall(text))
    return frozenset(c for c in found if not 0xD800 <= ord(c) <= 0xDFFF)

