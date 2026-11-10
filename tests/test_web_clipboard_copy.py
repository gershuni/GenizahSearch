"""Web copy buttons copy the exact text and only say "copied" when it worked.

Before this fix ``copy_result_text`` (Quick View / Advanced View) and the search
page's ``bulk_copy_text`` built the JavaScript with only backticks escaped, so
some characters in the text were altered on the way (a literal backslash-n
became a newline), and a trailing backslash copied nothing. The "copied" toast
fired unconditionally, before and regardless of the browser's answer.

The shared helper is ``web/clipboard.py``. These tests fake the NiceGUI client
so nothing touches a browser or the network. Toast assertions check the toast
TYPE, not its text, because the UI language is process-wide.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import json
import shutil
import subprocess
import textwrap
from pathlib import Path
from unittest import mock

import pytest
from nicegui import ui

ROOT = Path(__file__).resolve().parents[1]
BS = chr(92)  # a literal backslash, so no sample depends on escape parsing

SAMPLES = [
    "a" + BS + "nb",              # backslash + n, not a newline
    "tab" + BS + "t",
    "dbl" + BS + BS,
    "u" + BS + "u05d0",           # backslash-u escape text, not the letter
    "x`y",
    "z${1+1}",
    "//HLVY" + BS,
    "trailing" + BS,
    "real\nnewline",
    "בראשית ברא אלהים",
    "lone\ud800surrogate",
    "line separator",
]
SAMPLE_IDS = [
    "bs-n", "bs-t", "double-bs", "bs-u", "backtick", "template", "hlvy",
    "trailing-bs", "newline", "hebrew", "lone-surrogate", "u2028",
]


# ---------------------------------------------------------------------------
# Fake NiceGUI client
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, env):
        self.env = env

    def __await__(self):
        self.env.awaited = True
        # After the round trip the button's slot may be gone: any code that
        # reads ui.context.client only now gets an error.
        self.env.context_broken = True
        if self.env.outcome == "timeout":
            raise TimeoutError("JavaScript did not respond within 5.0 s")
        return self.env.outcome
        yield  # pragma: no cover  (makes this a generator)


class _FakeClient:
    def __init__(self, env):
        self.env = env
        self.depth = 0

    def run_javascript(self, code, *, timeout=1.0):
        self.env.codes.append(code)
        self.env.timeouts.append(timeout)
        return _FakeResponse(self.env)

    def __enter__(self):
        self.depth += 1
        return self

    def __exit__(self, *exc):
        self.depth -= 1
        return False


class _FakeContext:
    def __init__(self, env):
        self.env = env

    @property
    def client(self):
        if self.env.context_broken:
            raise RuntimeError("The parent element this slot belongs to has been deleted.")
        return self.env.client


class _Env:
    def __init__(self, outcome):
        self.outcome = outcome
        self.codes: list[str] = []
        self.timeouts: list[float] = []
        self.notifies: list[tuple[str, str, bool, bool]] = []
        self.awaited = False
        self.context_broken = False
        self.client = _FakeClient(self)


@pytest.fixture
def fake_env():
    def make(outcome=True):
        env = _Env(outcome)

        def notify(message, *args, type=None, **kwargs):  # noqa: A002
            env.notifies.append((str(message), type, env.awaited, env.client.depth > 0))

        def run_javascript(code, *, timeout=1.0):
            return env.client.run_javascript(code, timeout=timeout)

        patches = [
            mock.patch.object(ui, "notify", notify),
            mock.patch.object(ui, "run_javascript", run_javascript),
            mock.patch.object(ui, "context", _FakeContext(env)),
        ]
        for p in patches:
            p.start()
        env._patches = patches
        return env

    envs = []

    def factory(outcome=True):
        env = make(outcome)
        envs.append(env)
        return env

    yield factory
    for env in reversed(envs):
        for p in reversed(env._patches):
            p.stop()


def _run(fn, *args, **kwargs):
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        return asyncio.run(result)
    return result


def _literal_after_write_text(code: str) -> str:
    start = code.index("writeText(") + len("writeText(")
    value, _ = json.JSONDecoder().raw_decode(code, start)
    return value


def _types(env):
    return [t for (_m, t, _a, _c) in env.notifies]


# ---------------------------------------------------------------------------
# copy_result_text (Quick View, fullscreen, Advanced View)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text", SAMPLES, ids=SAMPLE_IDS)
def test_copy_result_text_sends_exact_text(fake_env, text):
    from web.pages.search_results import copy_result_text

    env = fake_env(True)
    _run(copy_result_text, text)
    assert len(env.codes) == 1
    assert _literal_after_write_text(env.codes[0]) == text


def test_copy_result_text_no_success_toast_when_browser_refuses(fake_env):
    from web.pages.search_results import copy_result_text

    env = fake_env(False)
    _run(copy_result_text, "some text")
    assert "positive" not in _types(env)
    assert _types(env) == ["negative"]


@pytest.mark.parametrize("answer", [None, 1, "true", {}, [True]],
                         ids=["none", "one", "str-true", "empty-dict", "list"])
def test_copy_result_text_no_answer_is_a_failure(fake_env, answer):
    """Only the boolean True is a success. A JS result of None (undefined) or
    any other truthy value is not."""
    from web.pages.search_results import copy_result_text

    env = fake_env(answer)
    _run(copy_result_text, "some text")
    assert _types(env) == ["negative"]


def test_copy_result_text_no_success_toast_on_timeout(fake_env):
    from web.pages.search_results import copy_result_text

    env = fake_env("timeout")
    _run(copy_result_text, "some text")  # must not raise
    assert _types(env) == ["warning"]


def test_copy_result_text_success_toast_only_after_js_reports_true(fake_env):
    from web.pages.search_results import copy_result_text

    env = fake_env(True)
    _run(copy_result_text, "some text")
    assert _types(env) == ["positive"]
    (_msg, _type, awaited_before_toast, _in_client) = env.notifies[0]
    assert awaited_before_toast
    assert env.timeouts and env.timeouts[0] >= 5.0


def test_copy_result_text_empty_text_sends_nothing(fake_env):
    """REGRESSION GUARD (green before and after): empty text sends no JS and
    shows the existing 'No text to copy' warning."""
    from web.pages.search_results import copy_result_text

    env = fake_env(True)
    _run(copy_result_text, "")
    assert env.codes == []
    assert _types(env) == ["warning"]


def test_toast_is_shown_through_the_client_captured_before_the_await(fake_env):
    """The button's slot can be deleted during the round trip, so the helper
    reads the client BEFORE awaiting and shows the toast inside ``with client:``."""
    from web.clipboard import copy_text_to_clipboard

    env = fake_env(True)
    assert asyncio.run(copy_text_to_clipboard("x")) is True
    assert [(t, in_client) for (_m, t, _a, in_client) in env.notifies] == [("positive", True)]


def test_helper_returns_false_on_refusal_and_timeout(fake_env):
    from web.clipboard import copy_text_to_clipboard

    fake_env(False)
    assert asyncio.run(copy_text_to_clipboard("x")) is False
    fake_env("timeout")
    assert asyncio.run(copy_text_to_clipboard("x")) is False


def test_custom_success_message_is_used(fake_env):
    from web.clipboard import copy_text_to_clipboard

    env = fake_env(True)
    asyncio.run(copy_text_to_clipboard("x", success_message="3 done"))
    assert env.notifies[0][:2] == ("3 done", "positive")


# ---------------------------------------------------------------------------
# The generated JavaScript
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text", SAMPLES, ids=SAMPLE_IDS)
def test_clipboard_write_js_round_trips_exactly(text):
    from web.clipboard import clipboard_write_js

    code = clipboard_write_js(text)
    assert _literal_after_write_text(code) == text
    start = code.index("writeText(") + len("writeText(")
    _value, end = json.JSONDecoder().raw_decode(code, start)
    assert "`" not in code[:start] + code[end:]  # no template literal around it
    assert code.lstrip().startswith("(async")  # one expression, no leading `return`
    # The code must also survive NiceGUI's websocket encoder (orjson, via
    # nicegui.json), which refuses a raw lone surrogate that Python's json
    # module accepts.
    from nicegui import json as nicegui_json

    nicegui_json.dumps({"code": code})


_NODE_HARNESS = r"""
const vm = require('vm');
let input = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', (d) => { input += d; });
process.stdin.on('end', async () => {
  const cases = JSON.parse(input);
  const out = [];
  for (const c of cases) {
    const captured = [];
    let navigator;
    if (c.mode === 'resolve') {
      navigator = { clipboard: { writeText: (t) => { captured.push(t); return Promise.resolve(); } } };
    } else if (c.mode === 'reject') {
      navigator = { clipboard: { writeText: (t) => Promise.reject(new Error('NotAllowedError')) } };
    } else {
      navigator = {};
    }
    const ctx = vm.createContext({ navigator, __code: c.code });
    // NiceGUI 3.8.0 static/nicegui.js runJavascript, minus the socket emit.
    const wrapper = "new Promise((resolve) => resolve(eval(__code)))" +
      ".catch((reason) => { if (reason instanceof SyntaxError) return eval('(async() => {' + __code + '})()'); else throw reason; })";
    let result;
    let responded;
    try {
      result = await vm.runInContext(wrapper, ctx);
      responded = true;
    } catch (e) {
      result = String(e);
      responded = false;
    }
    out.push({ captured, result: result === undefined ? '__undefined__' : result, responded });
  }
  process.stdout.write(JSON.stringify(out));
});
"""


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_clipboard_js_in_node_returns_truthful_boolean(tmp_path):
    from web.clipboard import clipboard_write_js

    script = tmp_path / "harness.js"
    script.write_text(_NODE_HARNESS, encoding="utf-8")
    cases = []
    for text in SAMPLES:
        cases.append({"code": clipboard_write_js(text), "mode": "resolve"})
    cases.append({"code": clipboard_write_js("x"), "mode": "reject"})
    cases.append({"code": clipboard_write_js("x"), "mode": "missing"})
    proc = subprocess.run(
        ["node", str(script)], input=json.dumps(cases), capture_output=True,
        text=True, encoding="utf-8", timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)
    for text, res in zip(SAMPLES, out):
        assert res == {"captured": [text], "result": True, "responded": True}, text
    assert out[-2] == {"captured": [], "result": False, "responded": True}
    assert out[-1] == {"captured": [], "result": False, "responded": True}


# ---------------------------------------------------------------------------
# Guards over web/
# ---------------------------------------------------------------------------

_CLIPBOARD_TOKENS = ("writeText", "execCommand", "navigator.clipboard")

#: Existing sites that are allowed to keep their own clipboard JS, by
#: (file, enclosing def). None of them embeds Python text in a way that can
#: alter it, and none shows a success toast before the browser answers.
_ALLOWED_SITES = {
    # writes window.location.href; the toast is created inside .then()
    ("web/pages/browse.py", "_copy_share_link"),
    # a fixed '/puzzle?doc=<id>' path; awaits the call before the toast
    ("web/pages/puzzle.py", "_copy_share_link"),
    # the payload is json.dumps(text); it shows no toast at all
    ("web/citation_chip.py", "_copy_button"),
}


def _enclosing_def(tree, lineno):
    best = None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            end = getattr(node, "end_lineno", node.lineno)
            if node.lineno <= lineno <= end:
                if best is None or node.lineno >= best.lineno:
                    best = node
    return best.name if best else None


def _clipboard_violations(files: dict[str, str]) -> list[str]:
    bad = []
    for rel, src in files.items():
        if rel == "web/clipboard.py":
            continue
        lines = src.splitlines()
        tree = ast.parse(src) if rel.endswith(".py") else None
        for i, line in enumerate(lines, 1):
            if any(tok in line for tok in _CLIPBOARD_TOKENS):
                owner = _enclosing_def(tree, i) if tree is not None else None
                if (rel, owner) not in _ALLOWED_SITES:
                    bad.append(f"{rel}:{i} ({owner}): {line.strip()[:80]}")
    return bad


def _web_sources() -> dict[str, str]:
    out = {}
    for pattern in ("**/*.py", "**/*.js", "**/*.html"):
        for p in (ROOT / "web").glob(pattern):
            if "__pycache__" in p.parts:
                continue
            out[p.relative_to(ROOT).as_posix()] = p.read_text(encoding="utf-8", errors="replace")
    return out


def test_no_clipboard_write_outside_web_clipboard():
    bad = _clipboard_violations(_web_sources())
    assert not bad, "clipboard writes must go through web/clipboard.py:\n" + "\n".join(bad)


def test_clipboard_guard_can_fail():
    seeded = {"web/pages/fake.py": "def f(t):\n    ui.run_javascript('navigator.clipboard.writeText(`%s`)' % t)\n"}
    assert _clipboard_violations(seeded)
    allowed = {"web/pages/browse.py": "def _copy_share_link():\n    js = 'navigator.clipboard.writeText(x)'\n"}
    assert not _clipboard_violations(allowed)


_COPY_FUNCS = {"copy_result_text", "copy_text_to_clipboard", "bulk_copy_text"}


def _unawaited_copy_calls(files: dict[str, str]) -> list[str]:
    bad = []
    for rel, src in files.items():
        if not rel.endswith(".py"):
            continue
        tree = ast.parse(src)
        parents = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parents[child] = node
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            name = f.id if isinstance(f, ast.Name) else f.attr if isinstance(f, ast.Attribute) else None
            if name not in _COPY_FUNCS:
                continue
            parent = parents.get(node)
            if isinstance(parent, ast.Await) and parent.value is node:
                continue
            if isinstance(parent, ast.Lambda) and parent.body is node:
                continue
            bad.append(f"{rel}:{node.lineno}: {name}() is neither awaited nor a whole lambda body")
    return bad


def test_every_copy_call_is_awaited_or_a_whole_lambda_body():
    """REGRESSION GUARD (green before and after): the copy helpers are async, so
    a call whose coroutine is dropped (for example inside a tuple lambda) would
    copy nothing and show nothing."""
    bad = _unawaited_copy_calls(_web_sources())
    assert not bad, "\n".join(bad)


def test_unawaited_copy_guard_can_fail():
    seeded = {"web/pages/fake.py": textwrap.dedent('''
        def page():
            ui.button(on_click=lambda t='x': (dialog.close(), copy_result_text(t)))
            copy_text_to_clipboard('y')
        async def ok():
            await copy_text_to_clipboard('z')
            ui.button(on_click=lambda: bulk_copy_text())
    ''')}
    bad = _unawaited_copy_calls(seeded)
    assert len(bad) == 2, bad


# ---------------------------------------------------------------------------
# Call sites that cannot be driven headlessly (AST)
# ---------------------------------------------------------------------------

def _find_def(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"def {name} not found")


def test_bulk_copy_text_awaits_the_helper():
    tree = ast.parse((ROOT / "web/pages/search.py").read_text(encoding="utf-8"))
    fn = _find_def(tree, "bulk_copy_text")
    assert isinstance(fn, ast.AsyncFunctionDef), "bulk_copy_text must be async"
    awaited = [
        n for n in ast.walk(fn)
        if isinstance(n, ast.Await) and isinstance(n.value, ast.Call)
        and isinstance(n.value.func, ast.Name) and n.value.func.id == "copy_text_to_clipboard"
    ]
    assert len(awaited) == 1
    for n in ast.walk(fn):
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                and n.func.attr == "notify"):
            for kw in n.keywords:
                assert not (kw.arg == "type" and isinstance(kw.value, ast.Constant)
                            and kw.value.value == "positive"), \
                    f"unconditional success toast at line {n.lineno}"


def test_quick_view_copy_reads_current_version():
    """After a version switch (translation, PGP/FGP edition, community version)
    the Quick View Copy button must copy the text on screen, which lives in
    current_display_text['value'], not the text the view opened with."""
    tree = ast.parse((ROOT / "web/pages/search_results.py").read_text(encoding="utf-8"))
    render_content = _find_def(_find_def(tree, "open_advanced_dialog"), "render_content")
    parents = {}
    for node in ast.walk(render_content):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    assign = next(
        n for n in ast.walk(render_content)
        if isinstance(n, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "current_display_text" for t in n.targets)
    )
    block = parents[assign]
    while not isinstance(block, ast.With):
        block = parents[block]
    copy_lambdas = [
        n for n in ast.walk(block)
        if isinstance(n, ast.Lambda) and isinstance(n.body, ast.Call)
        and isinstance(n.body.func, ast.Name) and n.body.func.id == "copy_result_text"
    ]
    assert copy_lambdas, "Quick View copy button not found"
    for lam in copy_lambdas:
        names = {x.id for x in ast.walk(lam) if isinstance(x, ast.Name)}
        assert "display_text" not in names, f"line {lam.lineno} copies the opening text"
        assert "current_display_text" in names, f"line {lam.lineno}"
        keys = {x.value for x in ast.walk(lam) if isinstance(x, ast.Constant)}
        assert "html" not in keys and "value" in keys, f"line {lam.lineno}"


def test_quick_view_actions_copy_text_reads_current_version():
    """The 'Copy Text' button in the Quick View Actions card sits in the same
    dialog as the version selector. In view mode it must copy the version on
    screen (current_display_text['value']); in edit mode the editor shows the
    page text, so it keeps copying that."""
    tree = ast.parse((ROOT / "web/pages/search_results.py").read_text(encoding="utf-8"))
    render_content = _find_def(_find_def(tree, "open_advanced_dialog"), "render_content")
    buttons = []
    for n in ast.walk(render_content):
        if not (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                and n.func.attr == "button" and n.args):
            continue
        label = n.args[0]
        if (isinstance(label, ast.Call) and isinstance(label.func, ast.Name)
                and label.func.id == "tr" and label.args
                and isinstance(label.args[0], ast.Constant)
                and label.args[0].value == "Copy Text"):
            buttons.append(n)
    assert len(buttons) == 1, f"expected one ui.button(tr('Copy Text'), ...), found {len(buttons)}"
    on_click = next((k.value for k in buttons[0].keywords if k.arg == "on_click"), None)
    assert isinstance(on_click, ast.Lambda), "on_click is not a lambda"
    assert (isinstance(on_click.body, ast.Call) and isinstance(on_click.body.func, ast.Name)
            and on_click.body.func.id == "copy_result_text")
    names = {x.id for x in ast.walk(on_click) if isinstance(x, ast.Name)}
    attrs = {x.attr for x in ast.walk(on_click) if isinstance(x, ast.Attribute)}
    keys = {x.value for x in ast.walk(on_click) if isinstance(x, ast.Constant)}
    assert "current_display_text" in names, f"line {on_click.lineno} copies the opening text"
    assert "value" in keys and "html" not in keys, f"line {on_click.lineno}"
    assert "edit_mode" in attrs, f"line {on_click.lineno} ignores edit mode"


def test_copy_result_text_is_async():
    from web.pages.search_results import copy_result_text

    assert inspect.iscoroutinefunction(copy_result_text)


def test_web_clipboard_imports_stay_narrow():
    """web/clipboard.py may import only the stdlib, nicegui and web.translations,
    so every page (and nothing it pulls in) can use it."""
    tree = ast.parse((ROOT / "web/clipboard.py").read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
        elif isinstance(node, ast.Import):
            mod = node.names[0].name
        else:
            continue
        top = mod.split(".")[0]
        assert top in {"__future__", "json", "re", "nicegui"} or mod == "web.translations", mod
