## [Unreleased] -- Sign-in dead ends: "Forgot password?" and "Resend confirmation email" under the error (web, branch `claude/signin-dead-ends-27e7da`)

A failed password sign-in now offers the way out: wrong email or password shows a "Forgot password?" button under the error (opens the existing recovery dialog); an unconfirmed email shows "Resend confirmation email" (one send per 60 s per dialog, one neutral answer whatever Supabase says, PostHog `confirmation_resent` without the address). New `supabase_client.resend_confirmation_email`; `auth_state.login_error_affordance`. No What's New.
