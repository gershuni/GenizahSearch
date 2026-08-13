-- Community review beta for computed identifications (Phase 137 preview).
--
-- Run in the Supabase SQL editor BEFORE deploying the enabled UI. Idempotent:
-- safe to run again when updating the functions or policies.
--
-- The public never writes this table directly. Anonymous and authenticated
-- readers submit through a SECURITY DEFINER function which validates the closed
-- vocabularies, chooses the reviewer key, throttles writes and always returns an
-- edited review to `pending`. A separate public function exposes only approved,
-- identity-free fields. Admin moderation is also an RPC so `reviewed_by` cannot
-- be forged by a client update.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS public.identification_reviews (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    identification_id TEXT NOT NULL CHECK (
        char_length(identification_id) BETWEEN 1 AND 128),
    sidecar_version TEXT NOT NULL CHECK (
        char_length(sidecar_version) BETWEEN 1 AND 200),

    -- Public artifact locator fields only. No title, shelfmark or excerpt is
    -- copied into Supabase; the admin resolves those from the served artifact.
    sys_id TEXT CHECK (sys_id IS NULL OR char_length(sys_id) <= 128),
    page_id TEXT CHECK (page_id IS NULL OR char_length(page_id) <= 300),
    page_number INTEGER CHECK (page_number IS NULL OR page_number > 0),
    work_id TEXT CHECK (work_id IS NULL OR char_length(work_id) <= 128),
    displayed_relation TEXT CHECK (
        displayed_relation IS NULL OR char_length(displayed_relation) <= 200),

    relation_verdict TEXT NOT NULL CHECK (relation_verdict IN (
        'direct_witness',
        'manuscript_quotes_work',
        'shared_source',
        'work_quotes_manuscript',
        'not_meaningful',
        'other_unsure'
    )),
    direct_novelty TEXT CHECK (direct_novelty IN (
        'potentially_new', 'already_known', 'other_unsure'
    )),
    comment TEXT CHECK (comment IS NULL OR char_length(comment) <= 1500),
    -- The assessment is always public after approval. The free-text comment is
    -- public only when the moderator explicitly checks the publication box.
    publish_comment BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT identification_reviews_direct_novelty_scope CHECK (
        relation_verdict = 'direct_witness' OR direct_novelty IS NULL),

    reviewer_user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    -- `user:<auth.uid()>` or `anonymous:<sha256(session uuid)>`. It is never
    -- returned by a public function and direct table SELECT is admin-only.
    reviewer_key TEXT NOT NULL CHECK (char_length(reviewer_key) <= 128),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'approved', 'rejected')),
    moderation_note TEXT CHECK (
        moderation_note IS NULL OR char_length(moderation_note) <= 1000),
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reviewed_at TIMESTAMPTZ,
    reviewed_by UUID REFERENCES auth.users(id) ON DELETE SET NULL,

    -- A visitor may revise an assessment, but cannot manufacture a vote count
    -- by clicking repeatedly. Every revision goes back through moderation.
    UNIQUE (identification_id, reviewer_key)
);

-- Upgrade an already-installed beta table; harmless on a clean install.
ALTER TABLE public.identification_reviews
    ADD COLUMN IF NOT EXISTS publish_comment BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS identification_reviews_pending_idx
    ON public.identification_reviews (status, updated_at DESC);
CREATE INDEX IF NOT EXISTS identification_reviews_published_idx
    ON public.identification_reviews (identification_id, reviewed_at DESC)
    WHERE status = 'approved';

ALTER TABLE public.identification_reviews ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Admins can read identification reviews"
    ON public.identification_reviews;
CREATE POLICY "Admins can read identification reviews"
ON public.identification_reviews
FOR SELECT TO authenticated
USING (EXISTS (
    SELECT 1 FROM public.profiles
    WHERE profiles.id = auth.uid() AND profiles.role = 'admin'
));

-- No direct INSERT/UPDATE/DELETE policy. All mutations are constrained RPCs.
REVOKE ALL ON TABLE public.identification_reviews FROM anon, authenticated;
GRANT SELECT ON TABLE public.identification_reviews TO authenticated;


CREATE OR REPLACE FUNCTION public.submit_identification_review_beta(
    p_identification_id TEXT,
    p_sidecar_version TEXT,
    p_relation_verdict TEXT,
    p_direct_novelty TEXT DEFAULT NULL,
    p_comment TEXT DEFAULT NULL,
    p_anonymous_key TEXT DEFAULT NULL,
    p_sys_id TEXT DEFAULT NULL,
    p_page_id TEXT DEFAULT NULL,
    p_page_number INTEGER DEFAULT NULL,
    p_work_id TEXT DEFAULT NULL,
    p_displayed_relation TEXT DEFAULT NULL
)
RETURNS TABLE (review_id UUID, review_status TEXT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    v_user_id UUID := auth.uid();
    v_reviewer_key TEXT;
    v_review_id UUID;
    v_status TEXT;
    v_recent_count INTEGER;
BEGIN
    p_identification_id := btrim(COALESCE(p_identification_id, ''));
    p_sidecar_version := btrim(COALESCE(p_sidecar_version, ''));
    p_relation_verdict := btrim(COALESCE(p_relation_verdict, ''));
    p_direct_novelty := NULLIF(btrim(COALESCE(p_direct_novelty, '')), '');
    p_comment := NULLIF(btrim(COALESCE(p_comment, '')), '');
    p_sys_id := NULLIF(btrim(COALESCE(p_sys_id, '')), '');
    p_page_id := NULLIF(btrim(COALESCE(p_page_id, '')), '');
    p_work_id := NULLIF(btrim(COALESCE(p_work_id, '')), '');
    p_displayed_relation := NULLIF(
        btrim(COALESCE(p_displayed_relation, '')), '');

    IF char_length(p_identification_id) NOT BETWEEN 1 AND 128
       OR char_length(p_sidecar_version) NOT BETWEEN 1 AND 200 THEN
        RAISE EXCEPTION 'invalid identification review target';
    END IF;
    IF p_relation_verdict NOT IN (
        'direct_witness', 'manuscript_quotes_work', 'shared_source',
        'work_quotes_manuscript', 'not_meaningful', 'other_unsure'
    ) THEN
        RAISE EXCEPTION 'invalid identification review relation';
    END IF;
    IF p_direct_novelty IS NOT NULL AND p_direct_novelty NOT IN (
        'potentially_new', 'already_known', 'other_unsure'
    ) THEN
        RAISE EXCEPTION 'invalid direct identification status';
    END IF;
    IF p_relation_verdict <> 'direct_witness' THEN
        p_direct_novelty := NULL;
    END IF;
    IF p_comment IS NOT NULL AND char_length(p_comment) > 1500 THEN
        RAISE EXCEPTION 'identification review comment too long';
    END IF;
    IF (p_sys_id IS NOT NULL AND char_length(p_sys_id) > 128)
       OR (p_page_id IS NOT NULL AND char_length(p_page_id) > 300)
       OR (p_work_id IS NOT NULL AND char_length(p_work_id) > 128)
       OR (p_displayed_relation IS NOT NULL
           AND char_length(p_displayed_relation) > 200)
       OR (p_page_number IS NOT NULL AND p_page_number < 1) THEN
        RAISE EXCEPTION 'invalid identification review context';
    END IF;

    IF v_user_id IS NOT NULL THEN
        v_reviewer_key := 'user:' || v_user_id::TEXT;
    ELSE
        p_anonymous_key := btrim(COALESCE(p_anonymous_key, ''));
        IF p_anonymous_key !~ '^[0-9a-f]{64}$' THEN
            RAISE EXCEPTION 'invalid anonymous review key';
        END IF;
        v_reviewer_key := 'anonymous:' || p_anonymous_key;
    END IF;

    -- Stop fast repeat writes and cap one session/account to 30 distinct
    -- findings per hour. Pending moderation is the second anti-spam boundary.
    IF EXISTS (
        SELECT 1 FROM public.identification_reviews
        WHERE reviewer_key = v_reviewer_key
          AND identification_id = p_identification_id
          AND updated_at > now() - interval '10 seconds'
    ) THEN
        RAISE EXCEPTION 'identification review submitted too quickly';
    END IF;
    SELECT count(*) INTO v_recent_count
    FROM public.identification_reviews
    WHERE reviewer_key = v_reviewer_key
      AND updated_at > now() - interval '1 hour';
    IF v_recent_count >= 30 AND NOT EXISTS (
        SELECT 1 FROM public.identification_reviews
        WHERE reviewer_key = v_reviewer_key
          AND identification_id = p_identification_id
    ) THEN
        RAISE EXCEPTION 'identification review rate limit reached';
    END IF;

    INSERT INTO public.identification_reviews (
        identification_id, sidecar_version, sys_id, page_id, page_number,
        work_id, displayed_relation, relation_verdict, direct_novelty, comment,
        reviewer_user_id, reviewer_key
    ) VALUES (
        p_identification_id, p_sidecar_version, p_sys_id, p_page_id,
        p_page_number, p_work_id, p_displayed_relation, p_relation_verdict,
        p_direct_novelty, p_comment, v_user_id, v_reviewer_key
    )
    ON CONFLICT (identification_id, reviewer_key) DO UPDATE SET
        sidecar_version = EXCLUDED.sidecar_version,
        sys_id = EXCLUDED.sys_id,
        page_id = EXCLUDED.page_id,
        page_number = EXCLUDED.page_number,
        work_id = EXCLUDED.work_id,
        displayed_relation = EXCLUDED.displayed_relation,
        relation_verdict = EXCLUDED.relation_verdict,
        direct_novelty = EXCLUDED.direct_novelty,
        comment = EXCLUDED.comment,
        reviewer_user_id = EXCLUDED.reviewer_user_id,
        status = 'pending',
        publish_comment = FALSE,
        moderation_note = NULL,
        reviewed_at = NULL,
        reviewed_by = NULL,
        updated_at = now()
    RETURNING id, status INTO v_review_id, v_status;

    RETURN QUERY SELECT v_review_id, v_status;
END;
$$;

REVOKE ALL ON FUNCTION public.submit_identification_review_beta(
    TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, INTEGER, TEXT, TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.submit_identification_review_beta(
    TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, INTEGER, TEXT, TEXT
) TO anon, authenticated;


CREATE OR REPLACE FUNCTION public.get_published_identification_reviews_beta(
    p_identification_id TEXT
)
RETURNS TABLE (
    relation_verdict TEXT,
    direct_novelty TEXT,
    comment TEXT,
    published_at TIMESTAMPTZ
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
    SELECT r.relation_verdict, r.direct_novelty,
           CASE WHEN r.publish_comment THEN r.comment ELSE NULL END,
           r.reviewed_at
    FROM public.identification_reviews AS r
    WHERE r.identification_id = p_identification_id
      AND r.status = 'approved'
    ORDER BY r.reviewed_at DESC NULLS LAST, r.id;
$$;

REVOKE ALL ON FUNCTION public.get_published_identification_reviews_beta(TEXT)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_published_identification_reviews_beta(TEXT)
    TO anon, authenticated;


-- One bounded public read for all identification leaves visible on a page.
-- It exposes the finding id only as the grouping key supplied by the caller;
-- no reviewer or moderation identity crosses this boundary.
CREATE OR REPLACE FUNCTION public.get_published_identification_reviews_batch_beta(
    p_identification_ids TEXT[]
)
RETURNS TABLE (
    identification_id TEXT,
    relation_verdict TEXT,
    direct_novelty TEXT,
    comment TEXT,
    published_at TIMESTAMPTZ
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
BEGIN
    IF COALESCE(array_length(p_identification_ids, 1), 0) > 100 THEN
        RAISE EXCEPTION 'too many identification review targets';
    END IF;
    RETURN QUERY
    SELECT r.identification_id, r.relation_verdict, r.direct_novelty,
           CASE WHEN r.publish_comment THEN r.comment ELSE NULL END,
           r.reviewed_at
    FROM public.identification_reviews AS r
    WHERE r.identification_id = ANY(COALESCE(p_identification_ids, ARRAY[]::TEXT[]))
      AND r.status = 'approved'
    ORDER BY r.identification_id, r.reviewed_at DESC NULLS LAST, r.id;
END;
$$;

REVOKE ALL ON FUNCTION public.get_published_identification_reviews_batch_beta(TEXT[])
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_published_identification_reviews_batch_beta(TEXT[])
    TO anon, authenticated;


CREATE OR REPLACE FUNCTION public.moderate_identification_review_beta(
    p_review_id UUID,
    p_status TEXT,
    p_moderation_note TEXT DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    v_count INTEGER;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.profiles
        WHERE profiles.id = auth.uid() AND profiles.role = 'admin'
    ) THEN
        RAISE EXCEPTION 'admin role required' USING ERRCODE = '42501';
    END IF;
    IF p_status NOT IN ('approved', 'rejected') THEN
        RAISE EXCEPTION 'invalid moderation status';
    END IF;
    p_moderation_note := NULLIF(btrim(COALESCE(p_moderation_note, '')), '');
    IF p_moderation_note IS NOT NULL
       AND char_length(p_moderation_note) > 1000 THEN
        RAISE EXCEPTION 'moderation note too long';
    END IF;

    UPDATE public.identification_reviews
    SET status = p_status,
        moderation_note = p_moderation_note,
        reviewed_at = now(),
        reviewed_by = auth.uid()
    WHERE id = p_review_id;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count = 1;
END;
$$;

REVOKE ALL ON FUNCTION public.moderate_identification_review_beta(UUID, TEXT, TEXT)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.moderate_identification_review_beta(UUID, TEXT, TEXT)
    TO authenticated;


-- Editable moderation. The moderator may correct or complete the structured
-- assessment, edit the submitted comment, and decide independently whether
-- that comment is suitable for public display.
CREATE OR REPLACE FUNCTION public.moderate_identification_review_beta_v2(
    p_review_id UUID,
    p_status TEXT,
    p_moderation_note TEXT DEFAULT NULL,
    p_relation_verdict TEXT DEFAULT NULL,
    p_direct_novelty TEXT DEFAULT NULL,
    p_comment TEXT DEFAULT NULL,
    p_publish_comment BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    v_count INTEGER;
    v_relation TEXT;
    v_novelty TEXT;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.profiles
        WHERE profiles.id = auth.uid() AND profiles.role = 'admin'
    ) THEN
        RAISE EXCEPTION 'admin role required' USING ERRCODE = '42501';
    END IF;
    IF p_status NOT IN ('approved', 'rejected') THEN
        RAISE EXCEPTION 'invalid moderation status';
    END IF;

    SELECT relation_verdict, direct_novelty
    INTO v_relation, v_novelty
    FROM public.identification_reviews
    WHERE id = p_review_id;
    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    v_relation := COALESCE(
        NULLIF(btrim(COALESCE(p_relation_verdict, '')), ''), v_relation);
    IF v_relation NOT IN (
        'direct_witness', 'manuscript_quotes_work', 'shared_source',
        'work_quotes_manuscript', 'not_meaningful', 'other_unsure'
    ) THEN
        RAISE EXCEPTION 'invalid identification review relation';
    END IF;

    v_novelty := NULLIF(btrim(COALESCE(p_direct_novelty, '')), '');
    IF v_relation <> 'direct_witness' THEN
        v_novelty := NULL;
    ELSIF v_novelty IS NOT NULL AND v_novelty NOT IN (
        'potentially_new', 'already_known', 'other_unsure'
    ) THEN
        RAISE EXCEPTION 'invalid direct identification status';
    END IF;

    p_comment := NULLIF(btrim(COALESCE(p_comment, '')), '');
    p_moderation_note := NULLIF(btrim(COALESCE(p_moderation_note, '')), '');
    IF p_comment IS NOT NULL AND char_length(p_comment) > 1500 THEN
        RAISE EXCEPTION 'identification review comment too long';
    END IF;
    IF p_moderation_note IS NOT NULL
       AND char_length(p_moderation_note) > 1000 THEN
        RAISE EXCEPTION 'moderation note too long';
    END IF;

    UPDATE public.identification_reviews
    SET relation_verdict = v_relation,
        direct_novelty = v_novelty,
        comment = p_comment,
        publish_comment = COALESCE(p_publish_comment, FALSE)
                          AND p_comment IS NOT NULL,
        status = p_status,
        moderation_note = p_moderation_note,
        reviewed_at = now(),
        reviewed_by = auth.uid(),
        updated_at = now()
    WHERE id = p_review_id;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count = 1;
END;
$$;

REVOKE ALL ON FUNCTION public.moderate_identification_review_beta_v2(
    UUID, TEXT, TEXT, TEXT, TEXT, TEXT, BOOLEAN
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.moderate_identification_review_beta_v2(
    UUID, TEXT, TEXT, TEXT, TEXT, TEXT, BOOLEAN
) TO authenticated;

-- Verification:
--   SELECT policyname, cmd FROM pg_policies
--    WHERE tablename = 'identification_reviews';
--   SELECT routine_name FROM information_schema.routines
--    WHERE routine_name LIKE '%identification_review%';
