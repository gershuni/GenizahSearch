/* Reconnect backoff (inlined into <head> on every page, so kept short; the why is in
 * web/reconnect_backoff.py). Runs before socket.io: an accessor on window.io wraps
 * io(url, opts), and the socket's `on` delays NiceGUI's RELOAD for "Implicit handshake
 * failed" / a later "timeout" by base * 2^n, jittered, n per tab in sessionStorage; a
 * hidden tab waits to be viewed. NiceGUI's first "timeout" is its switch to polling, not
 * a reload: it passes at once. A handshake refusal is never dropped; a reconnect cancels
 * a pending "timeout" reload. Every other error passes through.
 * Tests: tests/test_reconnect_backoff.py (a Node vm harness over this file). */
(function () {
  'use strict';
  if (window.__gsReconnectBackoff) return;  // installed once per page
  window.__gsReconnectBackoff = true;

  var cfg = window.GS_RECONNECT || {};
  var BASE_MS = cfg.base_ms > 0 ? cfg.base_ms : 1500;
  var CAP_MS = cfg.cap_ms > 0 ? cfg.cap_ms : 60000;
  var DELAY_MAX_MS = cfg.delay_max_ms > 0 ? cfg.delay_max_ms : 0;  // 0: socket.io's own schedule
  var STABLE_MS = 60000;   // a connection up this long ends the run of reloads
  var KEY = 'gs_reload_n', N_MAX = 30;
  var IHF = 'Implicit handshake failed';
  var memoryN = 0;         // used when sessionStorage is unavailable (private mode, blocked storage)

  function readN() {
    try {
      var n = parseInt(window.sessionStorage.getItem(KEY), 10);
      return n > 0 ? Math.min(n, N_MAX) : 0;
    } catch (e) {
      return memoryN;
    }
  }

  function writeN(n) {
    memoryN = n;
    try { window.sessionStorage.setItem(KEY, String(n)); } catch (e) { /* memory only */ }
  }

  function whenVisible(fn) {
    if (!document.hidden) { fn(); return; }
    var once = function () {
      if (document.hidden) return;
      document.removeEventListener('visibilitychange', once);
      fn();
    };
    document.addEventListener('visibilitychange', once);
  }

  // NiceGUI's own rule (nicegui.js usePollingFallback): its first "timeout" switches a
  // websocket-first page to polling instead of reloading.
  function hasPollingFallback(opts) {
    var t = opts && opts.transports;
    return !!(t && t.length && t[0] === 'websocket' && t.indexOf('polling') >= 0);
  }

  // One connect_error listener: its own fallback and its own pending reload, so a second
  // listener can never take a deferred error from another.
  function deferring(socket, handler, fallback) {
    var pending = null;  // {err, timer, cancelled}: the one reload this listener has scheduled

    function schedule(err) {
      if (pending) {  // never drop an error: a refusal (the socket is gone) wins over a timeout
        if (err.message === IHF) pending.err = err;
        return;
      }
      var n = readN();
      writeN(n + 1);
      // The cap bounds the exponential; the jitter spreads around it, so the longest
      // wait is 1.5 x cap and no two tabs share a cadence even there.
      var delay = Math.round(Math.min(CAP_MS, BASE_MS * Math.pow(2, n)) * (0.5 + Math.random()));
      try {
        if (window.posthog && typeof window.posthog.capture === 'function') {
          window.posthog.capture('reload_scheduled', {
            reason: err.message, attempt: n + 1, delay_ms: delay, hidden: !!document.hidden,
          });
        }
      } catch (e) { /* analytics must not stop the reload */ }
      var p = pending = {err: err, timer: null, cancelled: false};
      whenVisible(function () {
        if (p.cancelled) return;
        p.timer = setTimeout(function () {
          whenVisible(function () {
            if (p.cancelled) return;
            pending = null;
            handler.call(socket, p.err);
          });
        }, delay);
      });
    }

    return {
      listener: function (err) {
        var msg = err && err.message;
        if (msg === 'timeout' && fallback) {
          fallback = false;  // the switch to polling: NiceGUI does not reload on this one
          return handler.apply(this, arguments);
        }
        if (msg === 'timeout' || msg === IHF) return schedule(err);
        return handler.apply(this, arguments);
      },
      reconnected: function () {  // a pending timeout reload is moot; a refusal still reloads
        if (pending && pending.err.message === 'timeout') {
          pending.cancelled = true;
          clearTimeout(pending.timer);
          pending = null;
        }
      },
    };
  }

  function wrapSocket(socket, fallback) {
    if (!socket || typeof socket.on !== 'function') return socket;
    var on = socket.on;
    var listeners = [];
    socket.on = function (event, handler) {
      if (event !== 'connect_error' || typeof handler !== 'function') return on.apply(this, arguments);
      var d = deferring(this, handler, fallback);
      listeners.push(d);
      return on.call(this, event, d.listener);
    };
    // A connection that stays up STABLE_MS ends the run: the next reload starts from base again.
    var stable = null;
    on.call(socket, 'connect', function () {
      for (var i = 0; i < listeners.length; i++) listeners[i].reconnected();
      clearTimeout(stable);
      stable = setTimeout(function () { writeN(0); }, STABLE_MS);
    });
    var dropped = function () { clearTimeout(stable); stable = null; };
    on.call(socket, 'disconnect', dropped);
    on.call(socket, 'connect_error', dropped);
    return socket;
  }

  function install(io) {
    if (typeof io !== 'function' || io.__gsWrapped) return io;
    var wrapped = function (url, opts) {
      if (url && typeof url === 'object') { opts = url; url = undefined; }  // io(opts) form
      var merged = {};
      if (DELAY_MAX_MS) merged.reconnectionDelayMax = DELAY_MAX_MS;  // only when configured
      for (var k in (opts || {})) merged[k] = opts[k];  // the caller's keys win
      return wrapSocket(io.call(this, url, merged), hasPollingFallback(merged));
    };
    for (var name in io) wrapped[name] = io[name];  // Manager, Socket, connect, protocol, ...
    wrapped.__gsWrapped = true;
    return wrapped;
  }

  var current = install(window.io);
  try {
    Object.defineProperty(window, 'io', {
      configurable: true, enumerable: true,
      get: function () { return current; },
      set: function (value) { current = install(value); },  // socket.io.min.js: globalThis.io = ...
    });
  } catch (e) { /* io already a fixed property: nothing to intercept */ }
})();
