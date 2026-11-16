/* Sign-in tokens never reach PostHog (web/main.py POSTHOG_SCRIPT, `before_send`).
 *
 * Supabase's confirmation, magic-link and password-reset links land on the site with the
 * session in the address: `#access_token=...&refresh_token=...&type=signup`. PostHog records
 * the page address in `$current_url`, `$pathname`-adjacent properties, `$referrer`, the
 * person's `$initial_current_url`, and the session recording's own events, so every visitor who
 * confirmed an email or reset a password left a live session in the analytics project.
 *
 * `__ghScrubEvent` walks an event (properties, $set, $set_once, recording data) and replaces
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

    // No depth limit (a recording's page tree nests deep); `seen` stops a cycle. A tree too
    // deep for the stack throws, and the caller drops the event rather than send it raw.
    function scrub(value, seen, isPropertiesRoot) {
        if (typeof value === 'string') {
            return scrubString(value);
        }
        if (value === null || typeof value !== 'object' || seen.has(value)) {
            return value;
        }
        seen.add(value);
        if (Array.isArray(value)) {
            for (var i = 0; i < value.length; i++) {
                value[i] = scrub(value[i], seen, false);
            }
            return value;
        }
        for (var key in value) {
            if (Object.prototype.hasOwnProperty.call(value, key)) {
                var keep = isPropertiesRoot && key === 'token';
                value[key] = (!keep && TOKEN_KEY.test(key) && value[key] !== null && value[key] !== '')
                    ? '[redacted]'
                    : scrub(value[key], seen, false);
            }
        }
        return value;
    }

    window.__ghScrubEvent = function (event) {
        if (!event) {
            return event;
        }
        try {
            var seen = new WeakSet();
            if (event.properties) { scrub(event.properties, seen, true); }
            if (event.$set) { scrub(event.$set, seen, false); }
            if (event.$set_once) { scrub(event.$set_once, seen, false); }
        } catch (e) {
            // A scrub that fails must not send the raw event: drop it.
            return null;
        }
        return event;
    };
}());
