/* User-perceived latency of a few workflows, sent to PostHog as `ux_timing`.
 *
 * Start: a click (or Enter, for [data-ux-enter]) on an element tagged
 * data-ux="<workflow>". Completion: the server calls gsUx.done(...) through the
 * page's own client right after the UI update; NiceGUI runs that only after Vue
 * has patched the update into the DOM, so done() then waits for the next paint
 * (and, separately, the page image). A load the page started from its URL (no
 * click) passes nav=true and is measured from the start of the navigation.
 * Server side: web/ux_timing.py. Properties are deliberately low-cardinality:
 * no query text, shelfmark or id is added here.
 */
(function () {
  'use strict';
  var cfg = window.GS_UX || {};
  if (!cfg.rate) return;
  var sampled = Math.random() < cfg.rate;
  var PENDING_MAX_MS = 60000, IMAGE_MAX_MS = 15000, PAINT_MAX_MS = 1000;
  var waitMs = cfg.wait || {};  // per workflow, where the server's own limit is longer
  var REPLACED_MAX = 200;
  var pending = {}, replaced = {}, spoiled = {};
  var navDone = false;

  function now() { return performance.now(); }

  function mark(workflow, event, trigger) {
    if (spoiled[workflow]) return;  // its start is lost; nothing is sent until done()
    var t0 = event.timeStamp;
    // An old epoch-based timeStamp, or none: use the time the listener ran.
    if (!(t0 > 0) || t0 > now() + 1000) t0 = now();
    // Each start it replaces is kept, newest last: one is restored for every
    // action the server turns away, so several quick extra Enters unwind
    // back to the action still in progress. Past REPLACED_MAX (a held key)
    // that start would be gone, so this action goes unmeasured instead.
    var stack = replaced[workflow] || (replaced[workflow] = []);
    stack.push(pending[workflow]);
    if (stack.length > REPLACED_MAX) {
      spoiled[workflow] = true;
      delete pending[workflow];
      delete replaced[workflow];
      return;
    }
    pending[workflow] = { t0: t0, trigger: event.isTrusted ? trigger : 'key' };
  }

  document.addEventListener('click', function (e) {
    var el = e.target && e.target.closest && e.target.closest('[data-ux]');
    if (el) mark(el.getAttribute('data-ux'), e, 'click');
  }, true);
  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Enter' || e.isComposing) return;
    var el = e.target && e.target.closest && e.target.closest('[data-ux-enter]');
    if (el) mark(el.getAttribute('data-ux-enter'), e, 'enter');
  }, true);

  function afterPaint(callback) {
    var fired = false;
    function fire(timedOut) { if (!fired) { fired = true; callback(timedOut); } }
    // A hidden tab may never run requestAnimationFrame.
    setTimeout(function () { fire(true); }, PAINT_MAX_MS);
    requestAnimationFrame(function () { setTimeout(function () { fire(false); }, 0); });
  }

  function imageState(selector, since, callback) {
    var img = selector && document.querySelector(selector);
    if (!img) return callback('none', null);
    if (img.complete) return callback(img.naturalWidth ? 'loaded' : 'error', 0);
    var settled = false;
    function settle(state) {
      if (settled) return;
      settled = true;
      callback(state, state === 'timeout' ? null : Math.round(now() - since));
    }
    img.addEventListener('load', function () { settle('loaded'); }, { once: true });
    img.addEventListener('error', function () { settle('error'); }, { once: true });
    setTimeout(function () { settle('timeout'); }, IMAGE_MAX_MS);
  }

  function referrerPath() {
    try {
      var r = new URL(document.referrer);
      return r.origin === location.origin ? r.pathname : (document.referrer ? 'external' : null);
    } catch (_) { return null; }
  }

  window.gsUx = {
    done: function (workflow, outcome, serverMs, nav, imageSelector) {
      try {
        var received = now(), start = pending[workflow], base, trigger;
        var wasSpoiled = spoiled[workflow];
        delete pending[workflow];
        delete replaced[workflow];
        delete spoiled[workflow];
        if (wasSpoiled && !nav) return;
        if (nav) {
          if (navDone) return;
          navDone = true;
          base = 0;  // the start of this page's navigation
          trigger = 'nav';
        } else if (start && received - start.t0 < (waitMs[workflow] || PENDING_MAX_MS)) {
          base = start.t0;
          trigger = start.trigger;
        } else {
          return;  // no user action to measure from
        }
        afterPaint(function (paintTimedOut) {
          var painted = now();
          imageState(imageSelector, received, function (img, imgMs) {
            var navEntry = nav && performance.getEntriesByType
              ? performance.getEntriesByType('navigation')[0] : null;
            var props = {
              workflow: workflow,
              outcome: outcome,
              trigger: trigger,
              total_ms: Math.round(painted - base),
              server_ms: serverMs == null ? null : Math.round(serverMs),
              transport_ms: (trigger === 'nav' || serverMs == null) ? null
                : Math.max(0, Math.round(received - base - serverMs)),
              render_ms: Math.round(painted - received),
              img: img,
              img_ms: imgMs,
              ttfb_ms: navEntry ? Math.round(navEntry.responseStart) : null,
              from_path: nav ? referrerPath() : null,
              hidden: paintTimedOut || document.visibilityState !== 'visible',
              webdriver: navigator.webdriver === true,
              sample_rate: cfg.rate,
              v: cfg.v
            };
            window.gsUx.last = props;  // for debugging in the browser console
            if (sampled && window.posthog && window.posthog.capture) {
              window.posthog.capture('ux_timing', props);
            }
          });
        });
      } catch (_) { /* measuring must never break the page */ }
    },
    // The server turned the latest action away (e.g. Enter again while a search
    // runs): keep measuring from the action it is still working on.
    ignored: function (workflow) {
      try {
        var stack = replaced[workflow];
        if (spoiled[workflow] || !stack || !stack.length) return;
        var previous = stack.pop();
        if (previous) pending[workflow] = previous;
        else delete pending[workflow];
      } catch (_) { /* measuring must never break the page */ }
    }
  };
})();
