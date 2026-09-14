"""Interruptible query matching with a shared, cooperative search deadline.

Only query patterns use this adapter. Patterns retain Python ``re`` syntax and
word classes; regex's broader Unicode word definition would change Hebrew hits.
The deadline bounds matching, not compilation or arbitrary blocking native work.
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


def _seconds(value, env_name, default):
    if value is None:
        value = os.environ.get(env_name, default)
    try:
        value = float(value)
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) and value > 0 else default


@contextmanager
def search_budget(seconds=None):
    """Share the outer search's deadline across nested search operations."""
    previous = _deadline.get()
    if previous is not None:
        if time.monotonic() >= previous:
            raise SearchBudgetExceeded()
        yield
        return
    duration = _seconds(seconds, "GENIZAH_SEARCH_BUDGET_SECONDS", 60.0)
    token = _deadline.set(time.monotonic() + duration)
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
    limit = _seconds(None, "GENIZAH_REGEX_TIMEOUT_SECONDS", 0.25)
    deadline = _deadline.get()
    if deadline is not None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SearchBudgetExceeded()
        limit = min(limit, remaining)
    return limit


@lru_cache(maxsize=1)
def _word_ranges():
    # Use the running Python's Unicode database, including numeric characters.
    # \p{L}/\p{N} would instead use the dependency's different Unicode version.
    words, nonwords = [], []
    start = 0
    previous = False  # U+0000 is not a word character.
    for point in range(1, sys.maxunicode + 2):
        current = point <= sys.maxunicode and (
            chr(point).isalnum() or point == 95
        )
        if point > sys.maxunicode or current != previous:
            first = f"\\U{start:08x}"
            last = f"\\U{point - 1:08x}"
            (words if previous else nonwords).append(
                first if start == point - 1 else first + "-" + last
            )
            start, previous = point, current
    return "".join(words), "".join(nonwords)


_INLINE_FLAGS = re.compile(r"\(\?([aiLmsux]*)(?:-([imsx]+))?([:)])")


def _preserve_word_classes(pattern, flags):
    r"""Rewrite lexical word escapes, respecting classes, comments and flags.

    Input was validated by stdlib re first. Explicit ranges keep VERSION0's
    simple sets while preserving re's Unicode definition even for [^\w...].
    """
    if not any(token in pattern for token in (r"\w", r"\W", r"\b", r"\B")):
        return pattern
    word, nonword = _word_ranges()
    result = []
    ascii_mode = bool(flags & re.ASCII)
    verbose = bool(flags & re.VERBOSE)
    stack = []
    in_class = False
    class_start = False
    class_initial = False
    i = 0
    while i < len(pattern):
        char = pattern[i]
        if char == "\\" and i + 1 < len(pattern):
            escape = pattern[i + 1]
            if not ascii_mode and escape in "wW":
                interior = word if escape == "w" else nonword
                result.append(interior if in_class else "[" + interior + "]")
            elif not ascii_mode and not in_class and escape in "bB":
                w = "[" + word + "]"
                boundary = rf"(?:(?<!{w})(?={w})|(?<={w})(?!{w}))"
                if escape == "b":
                    result.append(boundary)
                else:
                    # Python <3.14 \B does not match an empty input.
                    nonempty = r"(?=[\s\S]|(?<=[\s\S]))" if sys.version_info < (3, 14) else ""
                    result.append(nonempty + "(?!" + boundary + ")")
            elif ascii_mode and not in_class and escape == "B" and sys.version_info < (3, 14):
                result.append(r"(?=[\s\S]|(?<=[\s\S]))\B")
            else:
                result.append(pattern[i:i + 2])
            i += 2
            class_start = False
            class_initial = False
            continue
        if in_class:
            result.append(char)
            if char == "]" and not class_start:
                in_class = False
            if not (class_initial and char == "^"):
                class_start = False
            class_initial = False
            i += 1
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
            in_class, class_start = True, True
            class_initial = True
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
    try:
        compiled = _regex.compile(rewritten, mapped)
    except _regex.error as exc:
        raise re.error(str(exc)) from exc
    return Pattern(compiled, validated.pattern, validated.flags)
