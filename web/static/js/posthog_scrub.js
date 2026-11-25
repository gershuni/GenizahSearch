/* Sign-in tokens never reach PostHog (web/main.py POSTHOG_SCRIPT, `before_send`).
 *
 * Supabase's confirmation, magic-link and password-reset links land on the site with the
 * session in the address: `#access_token=...&refresh_token=...&type=signup`. PostHog records
 * the page address in `$current_url`, `$pathname`-adjacent properties, `$referrer`, the
 * person's `$initial_current_url`, and the session recording's own events, so every visitor who
 * confirmed an email or reset a password left a live session in the analytics project.
 *
 * `__ghScrubEvent` copies an event (properties, $set, $set_once, recording data), replacing
 * the value of every token-bearing parameter with "[redacted]". Strings without "token" are
 * left untouched, so the walk is cheap; the parameter names stay, so an event still shows that
 * the page was a confirmation or a reset link.
 */
(function () {
    var TOKEN_PARAM = /((?:^|[?#&\s"'])(?:access_token|refresh_token|provider_token|provider_refresh_token|token_hash|token)=)[^&#\s"']*/gi;
    // A property NAMED like a token is redacted whatever its value: `{refresh_token: "abc"}`
    // carries no `token=` for the address pattern to find. The bare name `token` too, except
    // at the top of `properties`: PostHog puts its project key there before before_send runs,
    // and an event without it is rejected.
    var TOKEN_KEY = /^(?:access_token|refresh_token|provider_token|provider_refresh_token|token_hash|token)$/i;

    function scrubString(s) {
        if (s.toLowerCase().indexOf('token') === -1) {
            return s;
        }
        return s.replace(TOKEN_PARAM, '$1[redacted]');
    }

    // Returns a COPY and never writes to what it was given: an event can hold live objects. The
    // recorder's first `$snapshot` carries `posthog.config` itself, and redacting `token` there
    // in place replaced PostHog's own project key, so every later event of the page was rejected.
    // No depth limit (a recording's page tree nests deep); `copies` (original -> its copy) stops
    // a cycle. A tree too deep for the stack throws, and the caller drops the event rather than
    // send it raw.
    function scrub(value, key, copies, isPropertiesRoot) {
        if (typeof value === 'string') {
            return scrubString(value);
        }
        if (value === null || typeof value !== 'object') {
            return value;
        }
        // What JSON would send in the value's place: a Date, a URL, a boxed number or string.
        // Before the lookup in `copies`: a toJSON may answer by its key.
        if (typeof value.toJSON === 'function') {
            value = value.toJSON(key);
        }
        if (value instanceof Number || value instanceof String || value instanceof Boolean) {
            value = value.valueOf();
        }
        if (value === null || typeof value !== 'object') {
            return typeof value === 'string' ? scrubString(value) : value;
        }
        if (copies.has(value)) {
            return copies.get(value);
        }
        var copy = Array.isArray(value) ? [] : {};
        copies.set(value, copy);
        if (Array.isArray(value)) {
            for (var i = 0; i < value.length; i++) {
                copy[i] = scrub(value[i], String(i), copies, false);
            }
            return copy;
        }
        for (var name in value) {
            // JSON leaves functions out; a copied `toJSON` would run a second time, after the scrub.
            if (Object.prototype.hasOwnProperty.call(value, name) && typeof value[name] !== 'function') {
                var keep = isPropertiesRoot && name === 'token';
                copy[name] = (!keep && TOKEN_KEY.test(name) && value[name] !== null && value[name] !== '')
                    ? '[redacted]'
                    : scrub(value[name], name, copies, false);
            }
        }
        return copy;
    }

    // Events that promise to carry no query text (the on-screen Hebrew keyboard's,
    // web/hebrew_keyboard.py). PostHog adds the page address to every event -- `$current_url`,
    // `$referrer`, the session's entry address -- and on /search that address holds `?q=`, so
    // for these events every address loses its query and fragment.
    var QUERY_FREE_EVENTS = { hebrew_keyboard_opened: true, hebrew_keyboard_used: true };
    var URL_TAIL = /^(https?:\/\/[^?#\s]*)[?#].*$/i;

    // Writes in place: it is only given scrub's copies.
    function stripAddresses(value, seen) {
        if (typeof value === 'string') {
            return value.replace(URL_TAIL, '$1');
        }
        if (value === null || typeof value !== 'object' || seen.has(value)) {
            return value;
        }
        seen.add(value);
        for (var key in value) {
            if (Object.prototype.hasOwnProperty.call(value, key)) {
                value[key] = stripAddresses(value[key], seen);
            }
        }
        return value;
    }

    window.__ghScrubEvent = function (event) {
        if (!event) {
            return event;
        }
        try {
            var copies = new WeakMap();
            var out = {};
            for (var key in event) {
                if (Object.prototype.hasOwnProperty.call(event, key)) {
                    out[key] = event[key];
                }
            }
            if (event.properties) { out.properties = scrub(event.properties, 'properties', copies, true); }
            if (event.$set) { out.$set = scrub(event.$set, '$set', copies, false); }
            if (event.$set_once) { out.$set_once = scrub(event.$set_once, '$set_once', copies, false); }
            event = out;
            if (Object.prototype.hasOwnProperty.call(QUERY_FREE_EVENTS, event.event)) {
                var seenUrl = new WeakSet();
                if (event.properties) { stripAddresses(event.properties, seenUrl); }
                if (event.$set) { stripAddresses(event.$set, seenUrl); }
                if (event.$set_once) { stripAddresses(event.$set_once, seenUrl); }
            }
        } catch (e) {
            // A scrub that fails must not send the raw event: drop it.
            return null;
        }
        return event;
    };
}());
