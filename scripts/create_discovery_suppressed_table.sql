-- Admin suppression of individual computed identifications (Phase 136).
-- Owner ruling, 2026-08-06: hide a clearly-wrong row from the live findings beta
-- immediately, without a re-bake.
--
-- RUN THIS BEFORE the ✕ can work. Until the table exists the suppression read
-- FAILS OPEN by design -- the findings page works normally and hides nothing, and
-- logs one WARNING per 5s cache window naming the missing table. That is the
-- intended pre-migration state, not a bug to chase.
--
-- Supabase SQL editor, or psql against the project. Idempotent: safe to re-run.
--
-- WHY A TABLE AND NOT A COLUMN IN THE ARTIFACT. `web/discovery_assets.py` verifies
-- the sidecar's SHA-256 against the manifest's `content_hash` and refuses to serve
-- on mismatch, so editing one byte of the served .db makes the whole findings page
-- clean-hide. The artifact even carries `assertion_visibility` /
-- `identity_visibility` columns that would be the natural home -- and reaching them
-- costs a re-bake, a new hash and a 393 MB upload per takedown.

CREATE TABLE IF NOT EXISTS public.discovery_suppressed (
    -- The findings row's own id, from `discovery_identification.identification_id`
    -- in the sidecar. PRIMARY KEY, so suppressing twice is idempotent rather than
    -- a duplicate row -- which is what lets the UI treat a double-click as
    -- success.
    --
    -- NO foreign key: the ids live in a SQLite sidecar Postgres cannot see, and a
    -- constraint that cannot be checked is a comment pretending to be one.
    identification_id TEXT PRIMARY KEY,
    -- WHO and WHEN, both free (they come from the insert) and both the minimum
    -- needed to undo a mistake. Deliberately NO `reason` column: the owner asked
    -- for speed and for hiding, and a reason field is the beginning of a
    -- moderation workflow nobody has ruled on.
    suppressed_by UUID REFERENCES public.profiles(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.discovery_suppressed ENABLE ROW LEVEL SECURITY;

-- READABLE BY EVERYONE, and this is deliberate rather than lax. The filter has to
-- apply to every visitor including anonymous ones -- a hidden row must be hidden
-- for all readers, not only for whoever is logged in -- so the page reads this
-- list on an anonymous render. The list contains no claim and no content: it is a
-- set of opaque digests naming rows that are NOT shown.
DROP POLICY IF EXISTS "Anyone can read discovery suppressions" ON public.discovery_suppressed;
CREATE POLICY "Anyone can read discovery suppressions" ON public.discovery_suppressed
FOR SELECT TO public
USING (true);

-- WRITTEN BY ADMINS ONLY. THIS is the security boundary -- not the admin-only ✕
-- in the UI, which merely decides whether a button is drawn. The same shape
-- `scripts/fix_rls_policies.sql` already uses for admin writes on corrections and
-- discoveries.
DROP POLICY IF EXISTS "Admins can suppress a finding" ON public.discovery_suppressed;
CREATE POLICY "Admins can suppress a finding" ON public.discovery_suppressed
FOR INSERT TO authenticated
WITH CHECK (auth.uid() IN (SELECT id FROM public.profiles WHERE role = 'admin'));

DROP POLICY IF EXISTS "Admins can update a suppression" ON public.discovery_suppressed;
CREATE POLICY "Admins can update a suppression" ON public.discovery_suppressed
FOR UPDATE TO authenticated
USING (auth.uid() IN (SELECT id FROM public.profiles WHERE role = 'admin'))
WITH CHECK (auth.uid() IN (SELECT id FROM public.profiles WHERE role = 'admin'));

-- THE UNDO PATH, and the reason no confirmation dialog guards the ✕: hiding is one
-- click to do and one click to reverse, so a dialog would cost more than the
-- mistake.
DROP POLICY IF EXISTS "Admins can unsuppress a finding" ON public.discovery_suppressed;
CREATE POLICY "Admins can unsuppress a finding" ON public.discovery_suppressed
FOR DELETE TO authenticated
USING (auth.uid() IN (SELECT id FROM public.profiles WHERE role = 'admin'));

-- REQUIRED, NOT OPTIONAL (CLAUDE.md, "Supabase Data API grants"): every public
-- table intended for supabase-js/PostgREST access needs explicit GRANTs in
-- addition to RLS and policies. Without these the policies above are correct and
-- every request still fails.
--
-- UPDATE is granted because the client uses `upsert` for idempotency: an upsert
-- that conflicts performs an UPDATE, so INSERT alone would make a second hide of
-- the same row fail.
GRANT SELECT ON public.discovery_suppressed TO anon, authenticated;
GRANT INSERT, UPDATE, DELETE ON public.discovery_suppressed TO authenticated;

-- Verify (expect the table, four policies, and a zero count):
--   SELECT COUNT(*) FROM public.discovery_suppressed;
--   SELECT policyname, cmd FROM pg_policies
--    WHERE tablename = 'discovery_suppressed' ORDER BY cmd;
