"""Interruptible query matching with a shared, cooperative search deadline.

Only query patterns use this adapter. Patterns retain Python ``re`` syntax and
word classes; regex's broader Unicode word definition would change Hebrew hits.
The deadline bounds matching and is checked between compilation steps; native
compilation and arbitrary blocking native work are not interruptible in process.
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


@contextmanager
def isolated_matching():
    """Only for disposable, externally supervised search subprocesses.

    Their parent enforces resource limits and can kill native work, so elapsed
    matching time need not reject a legitimate query. UI highlighting may still
    establish its own explicit short budget inside this context.
    """
    token = _isolated.set(True)
    try:
        with search_budget(0):
            yield
    finally:
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
        atoms.append("[" + "".join(residual) + "]")
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
    def __init__(self, compiled, original, flags):
        self._compiled = compiled
        self.pattern = original
        self.flags = flags

    def _call(self, method, *args, **kwargs):
        try:
            return method(*args, concurrent=True, timeout=_match_timeout(), **kwargs)
        except TimeoutError as exc:
            raise SearchBudgetExceeded() from exc

    def search(self, string, pos=0, endpos=None):
        kwargs = {} if endpos is None else {"endpos": endpos}
        return self._call(self._compiled.search, string, pos=pos, **kwargs)

    def match(self, string, pos=0, endpos=None):
        kwargs = {} if endpos is None else {"endpos": endpos}
        return self._call(self._compiled.match, string, pos=pos, **kwargs)

    def fullmatch(self, string, pos=0, endpos=None):
        kwargs = {} if endpos is None else {"endpos": endpos}
        return self._call(self._compiled.fullmatch, string, pos=pos, **kwargs)

    def sub(self, repl, string, count=0):
        return self._call(self._compiled.sub, repl, string, count=count)

    @property
    def groups(self):
        return self._compiled.groups

    @property
    def groupindex(self):
        return self._compiled.groupindex


def compile(pattern, flags=0):
    """Compile stdlib syntax with bounded, GIL-releasing match operations."""
    # Native compilation itself cannot be interrupted here. Check on both
    # sides so an expired API budget cannot compile an entire chunk batch.
    _check_deadline()
    if isinstance(pattern, Pattern):
        if flags:
            raise ValueError("cannot process flags argument with a compiled pattern")
        return pattern
    validated = re.compile(pattern, flags)
    if not isinstance(validated.pattern, str):
        raise TypeError("search patterns must be strings")
    mapped = _regex.VERSION0
    for old, new in (
        (re.ASCII, _regex.ASCII), (re.IGNORECASE, _regex.IGNORECASE),
        (re.MULTILINE, _regex.MULTILINE), (re.DOTALL, _regex.DOTALL),
        (re.VERBOSE, _regex.VERBOSE), (re.UNICODE, _regex.UNICODE),
    ):
        if validated.flags & old:
            mapped |= new
    rewritten = _preserve_word_classes(validated.pattern, validated.flags)
    _check_deadline()
    try:
        compiled = _regex.compile(rewritten, mapped)
    except _regex.error as exc:
        raise re.error(str(exc)) from exc
    _check_deadline()
    return Pattern(compiled, validated.pattern, validated.flags)
