## [Unreleased] -- Admin Users tab names every account (web, branch `master-main`)

- Since the 2026-10-21 hardening took the email out of `profiles.username`, most accounts had
  no name and no username left, and /admin -> Users listed them as "Unknown". The tab now shows
  the account's email for them (and under the name for the others), read from `auth.users`
  through a new admin-only function, `public.admin_list_user_emails()`
  (`migrations/admin_list_user_emails.sql`): it answers only a caller whose profile has role
  `admin`, and `profiles` stays free of addresses. An account with neither name nor email reads
  "No name set". Until the migration is run the tab shows that label instead of the email.
- The tab's "Search users..." box and role filter were never connected; they now filter the
  list (name, username, email, affiliation; role).
- Pinned by `tests/test_admin_user_emails.py` and `tests/test_offloaded_readers.py` (the users
  list gets the admin client).
