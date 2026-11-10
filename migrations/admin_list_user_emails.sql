-- ============================================================================
-- admin_list_user_emails(): who each account is, for the admin Users tab.
--
-- migrations/profiles_no_email_username.sql (2026-10-21) took the email out of
-- profiles.username, where it had been public. Most accounts have no full name
-- and typed no username at sign-up, so the admin Users tab -- which only reads
-- profiles -- had nothing left to show for them and listed them as "Unknown".
--
-- The email lives in auth.users, which the Data API cannot read. This function
-- returns, for every account, only to a caller whose own profile has role
-- 'admin' (anyone else gets SQLSTATE 42501):
--   id, email,
--   sign_in_name -- the name in the account's sign-in data: Google's name for a
--                   Google sign-up, the full name typed at an email sign-up.
--                   For the admin's eyes only; it is published only if the
--                   person chooses it (web/public_name.py),
--   provider     -- 'email', 'google', ...
-- profiles stays free of addresses and of names nobody chose to publish.
--
-- 2026-10-24: (id, email). 2026-10-25: + sign_in_name, provider. A changed
-- return type needs DROP + CREATE; both run in one transaction, and the
-- website of 2026-10-24 reads only id and email, so it keeps working.
--
-- Idempotent: run the whole file in the SQL editor. Nothing else changes.
--
-- Verify, after running:
--   select * from public.admin_list_user_emails();
-- in the SQL editor must FAIL with "admin role required" (the editor has no
-- signed-in user, so auth.uid() is NULL). Then open /admin -> Users on the
-- website, signed in as an admin: each account shows its sign-in name and email.
--
-- Roll back:
--   drop function if exists public.admin_list_user_emails();
-- The website then says on the Users tab that the emails could not be loaded.
-- ============================================================================

begin;

drop function if exists public.admin_list_user_emails();

create function public.admin_list_user_emails()
returns table (id uuid, email text, sign_in_name text, provider text)
language plpgsql
stable
security definer
set search_path = ''
as $$
begin
  if not exists (
    select 1 from public.profiles p
    where p.id = auth.uid() and p.role = 'admin'
  ) then
    raise exception 'admin role required' using errcode = '42501';
  end if;

  return query
    select u.id,
           u.email::text,
           nullif(btrim(coalesce(u.raw_user_meta_data ->> 'full_name',
                                 u.raw_user_meta_data ->> 'name')), ''),
           u.raw_app_meta_data ->> 'provider'
    from auth.users u;
end;
$$;

-- Functions are executable by PUBLIC by default, and Supabase's default
-- privileges also grant anon: take both away, then let signed-in callers in
-- (the function itself turns away everyone who is not an admin).
revoke all on function public.admin_list_user_emails() from public, anon;
grant execute on function public.admin_list_user_emails() to authenticated;

commit;
