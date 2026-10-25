## [Unreleased] -- Admin Users tab names every account (web, branch `master-main`)

- Since the 2026-10-21 hardening took the email out of `profiles.username`, most accounts had
  no name and no username left, and /admin -> Users listed them as "Unknown". Each row now shows
  the public name, then the name the account signed in with (Google's, or the one typed at an
  email sign-up; for the admin's eyes, not published) and the email, read from `auth.users`
  through an admin-only function, `public.admin_list_user_emails()`
  (`migrations/admin_list_user_emails.sql`, re-run 2026-10-25 for the sign-in name): it answers
  only a caller whose profile has role `admin`, and `profiles` stays free of addresses. No line
  repeats the name above it.
- A lookup that fails says why above the list: no signed-in session on the server (seen
  2026-10-25: signing in again cured it), not an admin in the database, the function missing,
  or another error; the log line carries the error code.
- The tab's "Search users..." box and role filter were never connected; they now filter the
  list (name, username, sign-in name, email, affiliation; role).
- Pinned by `tests/test_admin_user_emails.py` (the rendered tab) and
  `tests/test_offloaded_readers.py` (the lookup gets the admin client).
