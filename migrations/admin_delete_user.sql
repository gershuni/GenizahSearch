-- ============================================================================
-- admin_delete_user(): the admin Users tab's "Delete User" deletes the account
-- (2026-10-25).
--
-- Until now it deleted only the profiles row. The sign-in account stayed: the
-- person could still sign in, with no profile -- no public name, never asked
-- for one, and My Profile unable to save (found 2026-10-25 on two accounts).
-- Deleting from auth.users needs more than an admin's Data API rights, so this
-- function does it as its owner, and only for a caller whose own profile has
-- role 'admin' (anyone else gets SQLSTATE 42501). It refuses the caller's own
-- account and any other admin's (change their role first).
--
-- What goes with the account is decided by each table's foreign key to
-- auth.users; the last query of this file lists them live. In
-- supabase_setup.sql: the profile, lists, list items, projects, recent items
-- and votes are deleted with it; corrections, comments, discoveries and joins
-- stay, without an author. A foreign key with no ON DELETE action (e.g.
-- corrections.reviewed_by for an account that reviewed) refuses the delete:
-- the function then says which one, and nothing is deleted.
--
-- Idempotent: run the whole file in the SQL editor.
--
-- Verify, after running:
--   select public.admin_delete_user(gen_random_uuid());
-- in the SQL editor must FAIL with "admin role required" (no signed-in user
-- there). Then, on the website as an admin, delete a test account from
-- /admin -> Users and check it is gone from Authentication -> Users.
--
-- Roll back:
--   drop function if exists public.admin_delete_user(uuid);
-- The website then reports that the delete could not be done.
-- ============================================================================

begin;

-- The function deletes as whoever owns it, i.e. whoever runs this file.
do $$
begin
  if not has_table_privilege(current_user, 'auth.users', 'delete') then
    raise exception 'aborted -- % cannot delete from auth.users, so the function could not either', current_user;
  end if;
end $$;

create or replace function public.admin_delete_user(p_user_id uuid)
returns boolean
language plpgsql
volatile
security definer
set search_path = ''
as $$
declare
  v_count integer := 0;
  v_constraint text;
begin
  if not exists (
    select 1 from public.profiles p
    where p.id = auth.uid() and p.role = 'admin'
  ) then
    raise exception 'admin role required' using errcode = '42501';
  end if;
  if p_user_id is null or p_user_id = auth.uid() then
    raise exception 'an admin cannot delete their own account here' using errcode = '42501';
  end if;
  if exists (select 1 from public.profiles p where p.id = p_user_id and p.role = 'admin') then
    raise exception 'this account is an admin: change its role first' using errcode = '42501';
  end if;

  begin
    delete from auth.users u where u.id = p_user_id;
    get diagnostics v_count = row_count;
  exception when foreign_key_violation then
    get stacked diagnostics v_constraint = constraint_name;
    raise exception 'not deleted: the account is still referenced by %', v_constraint
      using errcode = '23503';
  end;
  return v_count = 1;
end;
$$;

-- Functions are executable by PUBLIC by default, and Supabase's default
-- privileges also grant anon: take both away, then let signed-in callers in
-- (the function itself turns away everyone who is not an admin).
revoke all on function public.admin_delete_user(uuid) from public, anon;
grant execute on function public.admin_delete_user(uuid) to authenticated;

commit;

-- What deleting an account does to each table that points at it, live.
select c.conrelid::regclass as "table",
       a.attname as "column",
       case c.confdeltype
         when 'c' then 'deleted with the account'
         when 'n' then 'kept, without the account'
         when 'd' then 'kept, set to its default'
         else 'REFUSES the delete while it points at the account'
       end as on_delete
from pg_constraint c
join pg_attribute a on a.attrelid = c.conrelid and a.attnum = any (c.conkey)
where c.contype = 'f'
  and c.confrelid = 'auth.users'::regclass
  and c.connamespace <> 'auth'::regnamespace
order by 1, 2;
