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
  // The page starts PostHog on an idle callback (or after 2 s); until then
  // posthog.capture does not exist. Keep trying rather than lose the fastest
  // samples (a page loaded from its URL usually finishes before that).
  var PH_WAIT_MS = 15000, PH_POLL_MS = 250;
  var pending = {}, replaced = {}, spoiled = {};
  var marks = {};  // actions started per workflow: a newer one ends the image watch
  var dones = {}, settledSeq = {};  // done() calls per workflow, and the one last settled
  var SHARP_POLL_MS = 100;
  var navDone = false;

  function now() { return performance.now(); }

  function mark(workflow, event, trigger) {
    marks[workflow] = (marks[workflow] || 0) + 1;
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

  // Where the image on screen comes from: a low-cardinality name, never a URL or an id.
  function imageSource(el) {
    var url = (el && (el.currentSrc || el.src)) || '';
    var m = /\/api\/([a-z]+)_image(?:_by_sysid)?\//.exec(url);
    if (m) return m[1];
    if (/^https:\/\/iiif\.nli\.org\.il\//.test(url)) return 'nli_direct';
    if (/^https:\/\/hebrew\.bodleian\.ox\.ac\.uk\//.test(url)) return 'oxford';
    return url ? 'other' : null;
  }

  // The image the reader ends up seeing, followed across the page being drawn again
  // (Browse draws once more after its enrichment, 2026-11-22): its full-size version
  // shown ('full'), a library fallback shown ('fallback'), every source failed
  // ('error'), the reader moved on first ('moved_on'), or 'timeout'. The first
  // image's load (`img`, `img_ms`) can be a preview, or one replaced by the next draw.
  // `doneSeq`: with a `settle` wait, an image is final only once the server has said
  // its last draw is out (gsUx.settled, after Browse's enrichment): an image already
  // sharp from the cache in the first draw is not the one shown if the second draw
  // replaces it. Without that word by IMAGE_MAX_MS, the image sharp then is reported.
  function sharpState(selector, since, workflow, doneSeq, callback) {
    if (!selector) return callback('none', null, null);
    var started = marks[workflow] || 0, seen = false, candidate = null, candidateEl = null;
    function settledNow() { return !doneSeq || settledSeq[workflow] === doneSeq; }
    (function poll() {
      var el;
      try { el = document.querySelector(selector); } catch (_) { return callback('none', null, null); }
      var elapsed = now() - since;
      if (candidateEl && candidateEl !== el) candidate = candidateEl = null;
      if (el) {
        seen = true;
        var data = el.dataset || {};
        var fell = !!(data.triedOxford || data.triedManifest || data.triedServerProxy);
        var full = el.getAttribute && el.getAttribute('data-full-src');
        var shown = el.complete && el.naturalWidth > 0;
        var kind = shown && fell ? 'fallback'
          : (shown && (!full || el.getAttribute('src') === full) ? 'full' : null);
        if (kind && !candidate) {
          candidate = [kind, Math.round(elapsed), imageSource(el)];
          candidateEl = el;
        }
        if (candidate && settledNow()) return callback(candidate[0], candidate[1], candidate[2]);
        if (el.style && el.style.display === 'none') return callback('error', null, imageSource(el));
      } else if (seen) {
        return callback('error', null, null);  // the fallback chain replaced it with its notice
      }
      if ((marks[workflow] || 0) !== started) return callback('moved_on', null, null);
      if (elapsed >= IMAGE_MAX_MS) {
        if (candidate) return callback(candidate[0], candidate[1], candidate[2]);
        return callback('timeout', null, el ? imageSource(el) : null);
      }
      setTimeout(poll, SHARP_POLL_MS);
    })();
  }

  function send(props) {
    if (!sampled) return;
    var waited = 0;
    (function attempt() {
      try {
        var ph = window.posthog;
        if (ph && typeof ph.capture === 'function') {
          ph.capture('ux_timing', props);
          return;
        }
      } catch (_) { return; }
      if (waited >= PH_WAIT_MS) return;  // PostHog never started: drop it
      waited += PH_POLL_MS;
      setTimeout(attempt, PH_POLL_MS);
    })();
  }

  function referrerPath() {
    try {
      var r = new URL(document.referrer);
      return r.origin === location.origin ? r.pathname : (document.referrer ? 'external' : null);
    } catch (_) { return null; }
  }

  window.gsUx = {
    done: function (workflow, outcome, serverMs, nav, imageSelector, settle) {
      try {
        dones[workflow] = (dones[workflow] || 0) + 1;
        var doneSeq = settle ? dones[workflow] : 0;
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
          // Both watches start now; the event goes once both have an answer.
          var first = null, sharp = null;
          function both() {
            if (!first || !sharp) return;
            var img = first[0], imgMs = first[1];
            var imgFinal = sharp[0], imgFullMs = sharp[1], imgSource = sharp[2];
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
              img_final: imgFinal,
              img_full_ms: imgFullMs,
              img_source: imgSource,
              ttfb_ms: navEntry ? Math.round(navEntry.responseStart) : null,
              from_path: nav ? referrerPath() : null,
              hidden: paintTimedOut || document.visibilityState !== 'visible',
              webdriver: navigator.webdriver === true,
              sample_rate: cfg.rate,
              v: cfg.v
            };
            window.gsUx.last = props;  // for debugging in the browser console
            send(props);
          }
          function bothSafely() {
            try { both(); } catch (_) { /* measuring must never break the page */ }
          }
          imageState(imageSelector, received, function (img, imgMs) { first = [img, imgMs]; bothSafely(); });
          sharpState(imageSelector, received, workflow, doneSeq,
                     function (f, ms, s) { sharp = [f, ms, s]; bothSafely(); });
        });
      } catch (_) { /* measuring must never break the page */ }
    },
    // The server's last draw for the latest done(workflow) is out (Browse: after its
    // enrichment), so the image on screen now is the one the reader is shown.
    settled: function (workflow) {
      try { settledSeq[workflow] = dones[workflow] || 0; } catch (_) { /* never break the page */ }
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
