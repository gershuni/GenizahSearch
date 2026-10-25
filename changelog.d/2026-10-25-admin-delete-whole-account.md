## [Unreleased] -- Admin "Delete User" deletes the account (web, branch `master-main`)

- "Delete User" on /admin -> Users deleted only the profile row. The person could still sign
  in, with no profile: no public name, never asked for one, and My Profile unable to save (two
  such accounts found 2026-10-25). It now calls `public.admin_delete_user()`
  (`migrations/admin_delete_user.sql`), which deletes the sign-in account itself, and only for
  an admin; it refuses the admin's own account and other admins' (change the role first), and
  says which link blocks a delete. The menu no longer offers it on admin rows.
- The confirmation says what happens: the person can no longer sign in, their lists and votes
  go with the account, and their corrections, comments and joins stay without a name.
- Pinned by `tests/test_admin_delete_user.py`.
