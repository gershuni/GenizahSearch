-- ============================================================================
-- admin_list_user_emails(): account emails for the admin Users tab (2026-10-24).
--
-- migrations/profiles_no_email_username.sql (2026-10-21) took the email out of
-- profiles.username, where it had been public. Most accounts have no full name
-- and typed no username at sign-up, so the admin Users tab -- which only reads
-- profiles -- has nothing left to show for them and lists them as "Unknown".
--
-- The email lives in auth.users, which the Data API cannot read. This function
-- returns (id, email) for every account, and only to a caller whose own
-- profile has role 'admin'; anyone else gets SQLSTATE 42501. profiles stays
-- free of addresses.
--
-- Idempotent: run the whole file in the SQL editor. Nothing else changes.
--
-- Verify, after running:
--   select * from public.admin_list_user_emails();
-- in the SQL editor must FAIL with "admin role required" (the editor has no
-- signed-in user, so auth.uid() is NULL). Then open /admin -> Users on the
-- website, signed in as an admin: the accounts without a name show their email.
--
-- Roll back:
--   drop function if exists public.admin_list_user_emails();
-- The website then shows "No name set" for those accounts again.
-- ============================================================================

begin;

create or replace function public.admin_list_user_emails()
returns table (id uuid, email text)
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
    select u.id, u.email::text
    from auth.users u;
end;
$$;

-- Functions are executable by PUBLIC by default, and Supabase's default
-- privileges also grant anon: take both away, then let signed-in callers in
-- (the function itself turns away everyone who is not an admin).
revoke all on function public.admin_list_user_emails() from public, anon;
grant execute on function public.admin_list_user_emails() to authenticated;

commit;
