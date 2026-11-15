# -*- coding: utf-8 -*-
"""The desktop sign-in survives a restart -- the one an upgrade forces.

Supabase rotates refresh tokens: each refresh returns a new pair and retires the
old refresh token. The server still accepts a retired token within ten seconds,
or when it is the parent of the current one; an older one is refused and ends
the whole session. The auth client refreshes by itself (at a start whose saved
access token has expired, and on a timer before each hourly expiry), so the
saved file must follow every refresh. It used to keep the sign-in's pair: after
an hour of use that pair was two refreshes old, and the next start signed the
user out.

These tests run the real supabase_auth client against a fake auth server with
those rules; only the HTTP transport, the clock, the refresh timer and the
keyring are replaced. No request leaves the process.
"""
import base64
import json
import threading
import types
import uuid

import httpx
import pytest

scc = pytest.importorskip("desktop.supabase_corrections_client")
if not scc.SUPABASE_AVAILABLE:
    pytest.skip("supabase is not installed", allow_module_level=True)

import supabase_auth._sync.gotrue_client as gotrue  # noqa: E402
from supabase.lib.client_options import SyncClientOptions  # noqa: E402

USER_ID = "8f5a3c1e-0000-4000-8000-000000000001"
EMAIL = "reader@example.org"
HOUR = 3600


class _Clock:
    def __init__(self):
        self.now = 1_900_000_000

    def time(self):
        return self.now


class _Timers:
    """Stands in for supabase_auth's refresh timer: records it, never starts a thread."""

    def __init__(self):
        self.pending = []

    def make(self, interval, function):
        timers = self

        class _Timer:
            def __init__(self):
                self.function = function
                self.cancelled = False

            def start(self):
                timers.pending.append(self)

            def cancel(self):
                self.cancelled = True

        return _Timer()

    def fire_live(self):
        """What the hourly refresh does: run every timer still armed."""
        live = [t for t in self.pending if not t.cancelled]
        self.pending = []
        for timer in live:
            timer.function()
        return len(live)


def _jwt(exp):
    def part(obj):
        raw = json.dumps(obj).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
    return ".".join([part({"alg": "HS256", "typ": "JWT"}),
                     part({"sub": USER_ID, "exp": exp, "role": "authenticated"}),
                     part("sig")])


class _AuthServer:
    """GoTrue's refresh-token rules for one account's session."""

    REUSE_INTERVAL = 10

    def __init__(self, clock, saved):
        self.clock = clock
        self.saved = saved
        self.active = None          # the session's current refresh token
        self.parent = {}            # every refresh token issued -> the one it replaced
        self.retired_at = {}
        self.session_ended = False
        self.access = {}
        self.refused = []
        self.logouts = []           # for each sign-out request: did the credentials file still exist?
        self.hold_logout = None     # an Event a sign-out request waits for
        self.unexpected = []

    def _pair(self, refresh):
        exp = self.clock.now + HOUR
        access = _jwt(exp)
        self.access[access] = exp
        return httpx.Response(200, json={
            "access_token": access, "refresh_token": refresh, "token_type": "bearer",
            "expires_in": HOUR, "expires_at": exp, "user": self.user()})

    def _rotate(self, old):
        new = uuid.uuid4().hex
        self.parent[new] = old
        if old is not None:
            self.retired_at[old] = self.clock.now
        self.active = new
        return self._pair(new)

    def _refuse(self, token):
        self.refused.append(token)
        return httpx.Response(400, json={"code": "refresh_token_already_used",
                                         "error_code": "refresh_token_already_used",
                                         "msg": "Invalid Refresh Token: Already Used"})

    def _refresh(self, token):
        if self.session_ended or token not in self.parent:
            return self._refuse(token)
        if token == self.active:
            return self._rotate(token)
        if self.parent.get(self.active) == token:
            return self._pair(self.active)      # the current one's parent: answered with the current pair
        if self.clock.now - self.retired_at[token] <= self.REUSE_INTERVAL:
            return self._pair(self.active)
        self.session_ended = True               # an older token: the whole session is revoked
        return self._refuse(token)

    @staticmethod
    def user():
        return {"id": USER_ID, "aud": "authenticated", "email": EMAIL,
                "app_metadata": {}, "user_metadata": {}, "created_at": "2026-01-01T00:00:00Z"}

    def handle(self, request):
        path = request.url.path
        if path.endswith("/token"):
            grant = request.url.params.get("grant_type")
            if grant == "password":
                self.parent, self.retired_at, self.session_ended = {}, {}, False
                return self._rotate(None)
            return self._refresh(json.loads(request.content or b"{}").get("refresh_token"))
        if path.endswith("/user"):
            jwt = request.headers.get("Authorization", "").removeprefix("Bearer ")
            exp = self.access.get(jwt)
            if self.session_ended or exp is None or exp <= self.clock.now:
                return httpx.Response(401, json={"code": "bad_jwt", "msg": "invalid JWT"})
            return httpx.Response(200, json=self.user())
        if path.endswith("/logout"):
            if self.hold_logout is not None:
                assert self.hold_logout.wait(10)
            jwt = request.headers.get("Authorization", "").removeprefix("Bearer ")
            self.logouts.append((self.saved.exists(), request.url.params.get("scope"), jwt in self.access))
            self.session_ended = True
            return httpx.Response(204)
        self.unexpected.append(f"{request.method} {path}")
        return httpx.Response(404, json={"msg": f"unexpected {path}"})


@pytest.fixture
def world(tmp_path, monkeypatch):
    clock = _Clock()
    timers = _Timers()
    folder = tmp_path / "corrections"
    saved = folder / "supabase_credentials.json"
    server = _AuthServer(clock, saved)
    monkeypatch.setattr(gotrue, "time", clock)
    monkeypatch.setattr(gotrue, "Timer", timers.make)
    monkeypatch.setattr(scc, "SUPABASE_ANON_KEY", scc.SUPABASE_ANON_KEY or "anon-key-for-tests")
    vault = {}
    monkeypatch.setattr(scc, "KEYRING_AVAILABLE", True)
    monkeypatch.setattr(scc, "keyring", types.SimpleNamespace(
        set_password=lambda service, name, value: vault.__setitem__((service, name), value),
        get_password=lambda service, name: vault.get((service, name)),
        delete_password=lambda service, name: vault.pop((service, name)),
    ))
    real_create = scc.create_client

    def create(url, key, *args, **kwargs):
        # Every request of the client -- auth, its admin API (sign-out), the database -- goes
        # to the fake server.
        transport = httpx.MockTransport(server.handle)
        return real_create(url, key, options=SyncClientOptions(httpx_client=httpx.Client(transport=transport)))

    monkeypatch.setattr(scc, "create_client", create)

    def start():
        """One start of the desktop app (each builds its own client, as a new process does).
        The previous run has exited, and its refresh timer with it."""
        for timer in timers.pending:
            timer.cancel()
        return scc.SupabaseCorrectionsClient(config_path=folder)

    def sign_in(client):
        def profile(user_id):
            client.current_user = scc.User(id=1, email=EMAIL, username="reader", _uuid=user_id)
        monkeypatch.setattr(client, "_load_user_profile", profile)
        assert client.login(EMAIL, "Secret123") == (True, "Login successful")

    yield types.SimpleNamespace(clock=clock, timers=timers, server=server, start=start, sign_in=sign_in,
                                saved=saved, vault=vault,
                                read=lambda: json.loads(saved.read_text("utf-8")))
    assert server.unexpected == []


def _revoke_threads():
    return {t for t in threading.enumerate() if t.name == "supabase-sign-out"}


# A sign-out request as it should arrive: the file already gone, this session only, and a
# token the server issued.
ORDERLY_SIGN_OUT = (False, "local", True)


def test_a_sign_in_survives_restarts_after_hours_of_use(world):
    client = world.start()
    world.sign_in(client)
    for _ in range(2):  # two upgrades
        # The app stays open for hours; the client refreshes before each expiry.
        for _ in range(3):
            world.clock.now += HOUR
            assert world.timers.fire_live() == 1
        # The user closes it, the installer replaces it, the new version starts.
        world.clock.now += 5 * HOUR
        client = world.start()
        assert client.is_logged_in()
        assert client.current_user is not None and client.current_user.email == EMAIL
    assert world.server.refused == []
    assert world.read()["refresh_token"] == world.server.active


def test_each_start_saves_the_pair_it_refreshed_to(world):
    world.sign_in(world.start())
    for _ in range(4):  # four starts a day apart: each one's saved access token has expired
        world.clock.now += 24 * HOUR
        before = world.read()["refresh_token"]
        assert world.start().is_logged_in()
        assert world.read()["refresh_token"] == world.server.active != before
    assert world.server.refused == []


def test_remember_me_survives_the_refreshes(world):
    client = world.start()
    world.sign_in(client)
    assert client.save_login_credentials(EMAIL, "Secret123") is True
    signed_in_with = world.read()["refresh_token"]
    world.clock.now += HOUR
    assert world.timers.fire_live() == 1
    data = world.read()
    assert data["refresh_token"] == world.server.active != signed_in_with
    assert data["saved_email"] == EMAIL and data["remember_me"] is True
    assert data["cached_user"]["email"] == EMAIL
    assert world.start().get_saved_login_credentials() == (EMAIL, "Secret123")
    client.clear_saved_login_credentials()          # unticking it keeps the sign-in
    data = world.read()
    assert "saved_email" not in data and "remember_me" not in data
    assert data["refresh_token"] == world.server.active


def test_a_background_sign_out_that_refreshes_first_does_not_bring_the_sign_in_back(world):
    client = world.start()
    world.sign_in(client)
    world.clock.now += HOUR  # expired: the dropped client's sign_out refreshes before its request
    rotated_from = world.server.active
    world.server.hold_logout = threading.Event()    # keeps the thread alive until it is found
    before = _revoke_threads()
    client.logout(revoke="background")
    (revoke,) = _revoke_threads() - before
    world.server.hold_logout.set()
    revoke.join(10)
    assert not revoke.is_alive()
    assert world.server.active != rotated_from      # the dropped client did refresh
    assert world.server.logouts == [ORDERLY_SIGN_OUT]
    assert not world.saved.exists()
    assert not world.start().is_logged_in()


def test_the_dropped_clients_timer_firing_before_its_sign_out_request_writes_nothing(world):
    client = world.start()
    world.sign_in(client)
    world.server.hold_logout = threading.Event()
    before = _revoke_threads()
    client.logout(revoke="background")
    (revoke,) = _revoke_threads() - before
    world.clock.now += HOUR
    rotated_from = world.server.active
    assert world.timers.fire_live() == 1            # still armed: its sign-out request has not returned
    assert world.server.active != rotated_from      # and it did refresh
    assert not world.saved.exists()
    world.server.hold_logout.set()
    revoke.join(10)
    assert not revoke.is_alive()
    assert world.server.logouts == [ORDERLY_SIGN_OUT]
    assert not world.saved.exists()


@pytest.mark.parametrize("expired", [True, False], ids=["expired", "fresh"])
def test_a_waited_sign_out_forgets_before_it_asks_the_server(world, expired):
    client = world.start()
    world.sign_in(client)
    if expired:
        world.clock.now += HOUR  # sign_out refreshes before it asks the server
    client.logout()
    assert world.server.logouts == [ORDERLY_SIGN_OUT]
    assert not world.saved.exists()
    assert not world.start().is_logged_in()


def test_a_file_that_cannot_be_written_does_not_cost_this_run_its_sign_in(world, monkeypatch):
    world.sign_in(world.start())
    world.clock.now += 24 * HOUR
    attempts = []

    def refuse(path, data):
        attempts.append(path)
        raise PermissionError("held by another program")

    real_write = scc.write_bytes_atomic
    monkeypatch.setattr(scc, "write_bytes_atomic", refuse)
    client = world.start()  # its restore refreshes, and saving the new pair fails
    assert attempts
    assert client.is_logged_in()
    assert client.current_user is not None and client.current_user.email == EMAIL
    assert world.read()["refresh_token"] != world.server.active
    # The file is free again: the next save writes the pair the failed one could not.
    monkeypatch.setattr(scc, "write_bytes_atomic", real_write)
    client._save_credentials()
    assert world.read()["refresh_token"] == world.server.active


def test_a_file_that_cannot_be_read_is_left_as_it_was(world, monkeypatch):
    client = world.start()
    world.sign_in(client)
    original = world.saved.read_bytes()

    def busy(path, *args, **kwargs):
        raise PermissionError("held by another program")

    monkeypatch.setattr(scc, "read_bytes", busy)
    assert client.save_login_credentials(EMAIL, "Secret123") is False
    client.clear_saved_login_credentials()
    world.clock.now += HOUR
    assert world.timers.fire_live() == 1
    assert world.saved.read_bytes() == original


def test_the_saved_file_is_written_from_the_timer_thread(world):
    client = world.start()
    world.sign_in(client)
    before = world.read()["refresh_token"]
    world.clock.now += HOUR
    fired = []
    worker = threading.Thread(target=lambda: fired.append(world.timers.fire_live()))
    worker.start()
    worker.join(10)
    assert fired == [1]
    assert world.read()["refresh_token"] == world.server.active != before
    assert not list(world.saved.parent.glob("*.tmp"))


def test_a_session_read_before_a_refresh_is_not_saved_over_it(world, monkeypatch):
    client = world.start()
    world.sign_in(client)
    auth = client._client.auth
    real_get_session = auth.get_session
    overtaken = []

    def get_session_then_refresh():
        session = real_get_session()
        if not overtaken:                       # the timer refreshes before the caller writes
            overtaken.append(session.refresh_token)
            world.clock.now += HOUR
            assert world.timers.fire_live() == 1
        return session

    monkeypatch.setattr(auth, "get_session", get_session_then_refresh)
    client.current_user.username = "renamed"
    client._save_credentials()
    assert overtaken and world.server.active != overtaken[0]
    assert world.read()["refresh_token"] == world.server.active
    assert world.read()["cached_user"]["username"] == "renamed"


def test_refresh_reports_arriving_out_of_order_save_the_session_the_client_holds(world, monkeypatch):
    """The client stores each new session, then reports it on the refreshing thread;
    a report held up on its way arrives after later ones, carrying an older pair."""
    client = world.start()
    world.sign_in(client)
    auth = client._client.auth
    real_on_event = client._on_auth_event
    held_up, release = threading.Event(), threading.Event()

    def on_event(c, event, session):
        if threading.current_thread().name == "late-report":
            held_up.set()
            assert release.wait(10)
        real_on_event(c, event, session)

    monkeypatch.setattr(client, "_on_auth_event", on_event)
    real_write = scc.write_bytes_atomic
    written = []

    def write(path, data):
        written.append(json.loads(data).get("refresh_token"))
        real_write(path, data)

    monkeypatch.setattr(scc, "write_bytes_atomic", write)
    late = threading.Thread(target=auth.refresh_session, name="late-report")
    late.start()
    assert held_up.wait(10)                 # R1 is stored; its report is held up
    first = world.server.active
    auth.refresh_session()                  # R2 and R3, reported at once
    auth.refresh_session()
    assert world.read()["refresh_token"] == world.server.active != first
    release.set()                           # R1's report arrives last
    late.join(10)
    assert not late.is_alive()
    assert first not in written             # the file never went back to R1, even for a moment
    assert world.read()["refresh_token"] == world.server.active
    world.clock.now += 24 * HOUR
    assert world.start().is_logged_in()
    assert world.server.refused == []


class _LockProbe:
    """Wraps the client's lock: says when each named thread reaches it, then takes it."""

    def __init__(self, inner, *thread_names):
        self.inner = inner
        self.reached_by = {name: threading.Event() for name in thread_names}
        self.reached = self.reached_by[thread_names[0]]

    def __enter__(self):
        reached = self.reached_by.get(threading.current_thread().name)
        if reached is not None:
            reached.set()
        return self.inner.__enter__()

    def __exit__(self, *exc):
        return self.inner.__exit__(*exc)


def test_a_report_whose_write_is_overtaken_writes_again_before_it_lets_go(world, monkeypatch):
    """R1's report reads R1 and is writing it when R2 and R3 are stored (storage takes
    no lock; their reports wait for it). R1's report must not let go of the file with
    R1 in it, two refreshes old, should the app end before the waiting reports run."""
    client = world.start()
    world.sign_in(client)
    auth = client._client.auth
    real_write = scc.write_bytes_atomic
    writing, release = threading.Event(), threading.Event()
    written_by_first = []

    def write(path, data):
        if threading.current_thread().name == "first":
            written_by_first.append(json.loads(data)["refresh_token"])
            if len(written_by_first) == 1:
                writing.set()
                assert release.wait(10)
        real_write(path, data)

    monkeypatch.setattr(scc, "write_bytes_atomic", write)
    probe = _LockProbe(client._session_lock, "second", "third")
    client._session_lock = probe
    first = threading.Thread(target=auth.refresh_session, name="first")
    first.start()
    assert writing.wait(10)                         # R1 is being written
    r1 = written_by_first[0]
    second = threading.Thread(target=auth.refresh_session, name="second")
    second.start()
    assert probe.reached_by["second"].wait(10)      # R2 stored; its report waits for the lock
    third = threading.Thread(target=auth.refresh_session, name="third")
    third.start()
    assert probe.reached_by["third"].wait(10)       # R3 stored; its report waits too
    r3 = world.server.active
    assert r3 != r1
    release.set()
    for thread in (first, second, third):
        thread.join(10)
        assert not thread.is_alive()
    assert written_by_first[-1] == r3               # what R1's report left was the current pair
    assert world.read()["refresh_token"] == r3


def test_a_refresh_saved_while_remember_me_is_being_written_is_kept(world, monkeypatch):
    client = world.start()
    world.sign_in(client)
    real_write = scc.write_bytes_atomic
    paused, release = threading.Event(), threading.Event()

    def slow(path, data):
        if threading.current_thread().name == "remember-me":
            paused.set()
            assert release.wait(10)
        real_write(path, data)

    monkeypatch.setattr(scc, "write_bytes_atomic", slow)
    probe = _LockProbe(client._session_lock, "refresh")
    client._session_lock = probe
    remember = threading.Thread(target=client.save_login_credentials, args=(EMAIL, "Secret123"),
                                name="remember-me")
    remember.start()
    assert paused.wait(10)                          # it has read the file and is about to write
    world.clock.now += HOUR
    rotated_from = world.server.active
    timer = threading.Thread(target=world.timers.fire_live, name="refresh")
    timer.start()
    assert probe.reached.wait(10)                   # the refreshed pair is at the lock
    assert world.server.active != rotated_from
    timer.join(2)                                   # with no lock it would now write, and finish
    release.set()
    remember.join(10)
    timer.join(10)
    assert not remember.is_alive() and not timer.is_alive()
    data = world.read()
    assert data["saved_email"] == EMAIL
    assert data["refresh_token"] == world.server.active
