/* Reconnect backoff (inlined into <head> on every page, so kept short; the why is in
 * web/reconnect_backoff.py). Runs before socket.io: an accessor on window.io wraps
 * io(url, opts) to spread socket.io's retries, and the socket's `on` delays NiceGUI's
 * reload for "Implicit handshake failed" / "timeout" by base * 2^n, jittered, n per
 * tab in sessionStorage; a hidden tab waits to be viewed. Other errors pass through.
 * Tests: tests/test_reconnect_backoff.py (a Node vm harness over this file). */
(function () {
  'use strict';
  if (window.__gsReconnectBackoff) return;  // installed once per page
  window.__gsReconnectBackoff = true;

  var cfg = window.GS_RECONNECT || {};
  var BASE_MS = cfg.base_ms > 0 ? cfg.base_ms : 1500;
  var CAP_MS = cfg.cap_ms > 0 ? cfg.cap_ms : 60000;
  var DELAY_MAX_MS = cfg.delay_max_ms > 0 ? cfg.delay_max_ms : 30000;
  var STABLE_MS = 60000;   // a connection up this long ends the run of reloads
  var KEY = 'gs_reload_n', N_MAX = 30;
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

  function isDeferred(err) {
    var msg = err && err.message;
    return msg === 'Implicit handshake failed' || msg === 'timeout';
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

  var pending = false;  // one scheduled run at a time; the first one is the one that reloads

  function deferReload(socket, handler, err) {
    if (pending) return;
    pending = true;
    var n = readN();
    writeN(n + 1);
    var hidden = !!document.hidden;
    // The cap bounds the exponential; the jitter spreads around it, so the
    // longest wait is 1.5 x cap and no two tabs share a cadence even there.
    var delay = Math.round(Math.min(CAP_MS, BASE_MS * Math.pow(2, n)) * (0.5 + Math.random()));
    whenVisible(function () {
      setTimeout(function () {
        whenVisible(function () {
          pending = false;
          try {
            if (window.posthog && typeof window.posthog.capture === 'function') {
              window.posthog.capture('reload_scheduled', {
                reason: err.message, attempt: n + 1, delay_ms: delay, hidden: hidden,
              });
            }
          } catch (e) { /* analytics must not stop the reload */ }
          handler.call(socket, err);
        });
      }, delay);
    });
  }

  function wrapSocket(socket) {
    if (!socket || typeof socket.on !== 'function') return socket;
    var on = socket.on;
    socket.on = function (event, handler) {
      if (event !== 'connect_error' || typeof handler !== 'function') return on.apply(this, arguments);
      var self = this;
      return on.call(this, event, function (err) {
        if (isDeferred(err)) deferReload(self, handler, err);
        else handler.apply(this, arguments);
      });
    };
    // A connection that stays up STABLE_MS ends the run: the next reload starts from base again.
    var stable = null;
    on.call(socket, 'connect', function () {
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
      var merged = {reconnectionDelay: BASE_MS, reconnectionDelayMax: DELAY_MAX_MS, randomizationFactor: 0.9};
      for (var k in (opts || {})) merged[k] = opts[k];  // the caller's keys win
      return wrapSocket(io.call(this, url, merged));
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
