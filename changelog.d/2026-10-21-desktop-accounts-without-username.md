## [Unreleased] -- Desktop accounts without a username, and sign-up under email confirmation (desktop, branch `claude/stoic-chaum-a0eaf3`)

The desktop side of the 2026-10-21 `profiles` username hardening: a username is
now what the person typed at an email sign-up, or empty (every Google sign-up,
and the accounts whose username was their email address), and the production
project requires email confirmation. Each fix has a regression test that fails
on the old code.

### Fixed
- **Your own corrections** (draft, pending, rejected) show in the version
  selector of the Manuscript Viewer and of the Browse tab again for an account
  with no username, and for a sign-in saved before the change. The author is
  now recognised by account id, never by username. The client had also never
  kept a correction's author id, so every author without a username shared one
  entry and only the newest of them was shown, to reviewers too. A reviewer
  still sees everyone's; nobody sees anyone else's unpublished corrections.
- **Register** no longer says "You can now login" before the account is
  confirmed. It says to open the confirmation link sent to that address (the
  website's approved text, in Hebrew too), and no longer tries to write the
  profile with no session; the new-user trigger writes it from the sign-up
  form's fields, which are still sent.
- **Signing in before confirming** shows the website's approved message, in
  Hebrew too (it was English only).
- **The account name** in the corner button, the Community header and the
  corrections status line is the username, else the full name; never "None".
