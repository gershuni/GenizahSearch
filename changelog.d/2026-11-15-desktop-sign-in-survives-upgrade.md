## [Unreleased] -- The desktop app keeps you signed in after an upgrade (desktop, branch `claude/post-upgrade-login-persistence-178ba1`)

The desktop app no longer asks for the password again after an upgrade or any other restart.
Each refresh of the sign-in (when the app starts, every hour while it runs, and when a request
finds it expired) replaces the saved tokens. The server accepts a replaced refresh token only
within ten seconds or when it is the one just before the current one; an older one is refused
and ends the session. The saved file used to keep the pair from the sign-in, so after an hour
of use it was two refreshes old, and the next start was refused and showed the user as signed
out; most users met it when an upgrade restarted the app.

Every new pair is now written to `~/.genizah_corrections/supabase_credentials.json` from the
auth client's own events, from whichever thread refreshed, under a lock and replaced whole
(`shared/atomic_io.write_bytes_atomic`); the newest pair is kept in memory, so a save that
failed is made good by the next one. "Remember me" is kept beside it rather than
overwritten, and a file another program holds is retried and, if still unreadable, left as it
was rather than taken for an empty one. A sign-out drops the client before it asks the server
to end the session, so a refresh that lands after it never writes the file back.

Users whose saved sign-in is already stale sign in once more after installing this version.
Gate: `tests/test_desktop_sign_in_survives_restart.py`, which runs the real `supabase_auth`
client against a fake auth server with GoTrue's refresh-token rules (reuse window, parent
token, session revoked on older reuse, revoked on sign-out).
