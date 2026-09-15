"""Behavioral checks for bounded search matching and Hebrew compatibility."""

import asyncio
import concurrent.futures
import re
import threading
import time
from contextlib import nullcontext

import pytest

from shared import search_regex


@pytest.mark.parametrize("pattern", [
    r"\w+", r"\W+", r"[^\w\u0590-\u05FF']+", r"[\Wא]+",
    r"\b\w+\b", r"\B", r"(?a:\w+)\W*(?u:\w+)",
    r"[\w\u0590-\u05FF']+", r"[^]\w]+", r"[\[\]\\w]+",
    r"(?x) \w+ \# [\w] # ignored \w\n", r"(?# ignored \w)\w+",
    r"(?P<word>\w+)\s+(?P=word)",
    r"[^^]\w", r"[\^]\w", r"(?a:\B)",
])
@pytest.mark.parametrize("text", [
    "", "שלום עולם", "צ\u0307מאן צ'מאן", "שָׁלוֹם ]א[", "abc_12 Ⅳ²\u200c!",
    "abc abc", "abc#א", "x[]w\\", "éΩא 123",
])
@pytest.mark.parametrize('isolated', [False, True])
def test_word_and_hebrew_span_parity(pattern, text, isolated):
    for flags in (0, re.IGNORECASE, re.ASCII):
        old = re.compile(pattern, flags)
        new = search_regex.compile(pattern, flags)
        with search_regex.isolated_matching() if isolated else nullcontext():
            old_match, new_match = old.search(text), new.search(text)
            assert (old_match.span() if old_match else None) == (
                new_match.span() if new_match else None
            )
            assert old.sub("<hit>", text) == new.sub("<hit>", text)
        assert new.pattern == old.pattern
        assert new.flags == old.flags


def test_real_pathological_search_stops(monkeypatch):
    monkeypatch.setenv("GENIZAH_REGEX_TIMEOUT_SECONDS", "0.025")
    pattern = search_regex.compile(r"(a|aa)+$")
    started = time.monotonic()
    with pytest.raises(search_regex.SearchBudgetExceeded):
        pattern.search("a" * 10000 + "!")
    assert time.monotonic() - started < 1


def test_isolated_pattern_retains_nested_and_outside_timeouts(monkeypatch):
    monkeypatch.setenv('GENIZAH_REGEX_TIMEOUT_SECONDS', '0.025')
    with search_regex.isolated_matching():
        pattern = search_regex.compile(r'(a|aa)+$')
        with pytest.raises(search_regex.SearchBudgetExceeded):
            with search_regex.search_budget(0.025):
                pattern.search('a' * 10000 + '!')
        assert pattern.fullmatch('aaaa').span() == (0, 4)
        assert pattern.match('aaaa', 1, 3).span() == (1, 3)
    with pytest.raises(search_regex.SearchBudgetExceeded):
        pattern.search('a' * 10000 + '!')


def test_deadline_caps_many_successful_operations(monkeypatch):
    monkeypatch.setenv("GENIZAH_REGEX_TIMEOUT_SECONDS", "1")
    pattern = search_regex.compile("a")
    count = 0
    with pytest.raises(search_regex.SearchBudgetExceeded):
        with search_regex.search_budget(0.03):
            while True:
                assert pattern.search("a")
                count += 1
    assert count > 1


def test_nested_budget_reuses_outer_deadline():
    with search_regex.search_budget(1):
        outer = search_regex._deadline.get()
        with search_regex.search_budget(100):
            assert search_regex._deadline.get() == outer
    assert search_regex._deadline.get() is None


def test_budget_resets_on_exception():
    with pytest.raises(ValueError):
        with search_regex.search_budget():
            raise ValueError("caller failure")
    assert search_regex._deadline.get() is None


def test_decorator_shares_budget_and_preserves_name():
    @search_regex.bounded_search
    def inner():
        return search_regex._deadline.get()

    @search_regex.bounded_search
    def outer():
        return search_regex._deadline.get(), inner()

    first, second = outer()
    assert first == second
    assert inner.__name__ == "inner"
    assert search_regex._deadline.get() is None


def test_worker_matching_releases_gil(monkeypatch):
    monkeypatch.setenv("GENIZAH_REGEX_TIMEOUT_SECONDS", "0.15")
    pattern = search_regex.compile(r"(a|aa)+$")
    entered = threading.Event()

    def work():
        entered.set()
        with pytest.raises(search_regex.SearchBudgetExceeded):
            pattern.search("a" * 10000 + "!")

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(work)
        assert entered.wait(1)
        beats = 0
        while not future.done():
            time.sleep(0.005)
            beats += 1
        future.result()
    assert beats >= 3


def test_sub_operation_is_bounded(monkeypatch):
    monkeypatch.setenv("GENIZAH_REGEX_TIMEOUT_SECONDS", "0.025")
    with pytest.raises(search_regex.SearchBudgetExceeded):
        search_regex.compile(r"(a|aa)+$").sub("", "a" * 10000 + "!")


def test_asyncio_heartbeat_remains_responsive(monkeypatch):
    monkeypatch.setenv("GENIZAH_REGEX_TIMEOUT_SECONDS", "0.15")
    pattern = search_regex.compile(r"(a|aa)+$")

    def work():
        with pytest.raises(search_regex.SearchBudgetExceeded):
            pattern.search("a" * 10000 + "!")

    async def exercise():
        future = asyncio.get_running_loop().run_in_executor(None, work)
        ticks = 0
        while not future.done():
            await asyncio.sleep(0.005)
            ticks += 1
        await future
        return ticks

    assert asyncio.run(exercise()) >= 3


def test_compilation_error_is_stdlib_error():
    with pytest.raises(re.error):
        search_regex.compile("[")


@pytest.mark.parametrize("value", ["bad", "nan", "inf", "-1", "0"])
def test_invalid_environment_uses_finite_default(monkeypatch, value):
    monkeypatch.setenv("GENIZAH_REGEX_TIMEOUT_SECONDS", value)
    assert search_regex._match_timeout() == 10.0


def test_default_research_search_can_run_past_one_minute(monkeypatch):
    monkeypatch.delenv('GENIZAH_SEARCH_BUDGET_SECONDS', raising=False)
    monkeypatch.delenv('GENIZAH_REGEX_TIMEOUT_SECONDS', raising=False)
    clock = [100.0]
    monkeypatch.setattr(search_regex.time, 'monotonic', lambda: clock[0])
    pattern = search_regex.compile('שלום')
    with search_regex.search_budget():
        clock[0] += 3600
        assert pattern.search('שלום')
        assert search_regex._match_timeout() == 10.0
    assert search_regex._deadline.get() is None


def test_matching_receives_research_allowance(monkeypatch):
    from unittest.mock import Mock

    monkeypatch.delenv('GENIZAH_REGEX_TIMEOUT_SECONDS', raising=False)
    compiled = Mock()
    pattern = search_regex.Pattern(compiled, 'שלום', 0)
    pattern.search('שלום')
    assert compiled.search.call_args.kwargs['timeout'] == 10.0
    assert compiled.search.call_args.kwargs['concurrent'] is True


def test_explicit_budget_inside_unlimited_search_is_enforced(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(search_regex.time, 'monotonic', lambda: clock[0])
    with search_regex.search_budget(0):
        with pytest.raises(search_regex.SearchBudgetExceeded):
            with search_regex.search_budget(2):
                clock[0] += 3
                search_regex.compile('a').search('a')
        assert search_regex._match_timeout() > 0


def test_default_nested_search_preserves_api_deadline(monkeypatch):
    monkeypatch.setenv('GENIZAH_SEARCH_BUDGET_SECONDS', '0')
    clock = [100.0]
    monkeypatch.setattr(search_regex.time, 'monotonic', lambda: clock[0])
    with pytest.raises(search_regex.SearchBudgetExceeded):
        with search_regex.search_budget(30):
            with search_regex.search_budget():
                clock[0] += 31
                search_regex.compile('a').search('a')


def test_configured_total_budget_remains_available(monkeypatch):
    monkeypatch.setenv('GENIZAH_SEARCH_BUDGET_SECONDS', '120')
    clock = [100.0]
    monkeypatch.setattr(search_regex.time, 'monotonic', lambda: clock[0])
    with pytest.raises(search_regex.SearchBudgetExceeded):
        with search_regex.search_budget():
            clock[0] += 121


def test_only_supervised_worker_disables_matching_timeout(monkeypatch):
    monkeypatch.setenv('GENIZAH_REGEX_TIMEOUT_SECONDS', '0.25')
    assert search_regex._match_timeout() == 0.25
    with search_regex.isolated_matching():
        assert search_regex._match_timeout() is None
        with search_regex.search_budget(0.1):
            assert 0 < search_regex._match_timeout() <= 0.1
    assert search_regex._match_timeout() == 0.25


def test_compiled_pattern_and_groups_compatibility():
    compiled = search_regex.compile(re.compile(r"(?P<word>א+)", re.I))
    assert compiled.groups == 1
    assert compiled.groupindex == {"word": 1}
    assert compiled.fullmatch("אא").group("word") == "אא"
    assert compiled.match("אאx").span() == (0, 2)
    assert search_regex.compile(compiled) is compiled
