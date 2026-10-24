-- ============================================================================
-- SUPABASE SETUP FOR GENIZAHSEARCH
-- ============================================================================
-- Run this entire file in Supabase SQL Editor (Dashboard > SQL Editor > New Query)
-- You can run it all at once, or section by section
-- ============================================================================


-- ============================================================================
-- PART 1: CREATE TABLES
-- ============================================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- PROFILES (extends Supabase auth.users)
-- ============================================
CREATE TABLE public.profiles (
    id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    username TEXT UNIQUE,
    full_name TEXT,
    affiliation TEXT,
    bio TEXT,
    role TEXT DEFAULT 'user' CHECK (role IN ('user', 'editor', 'reviewer', 'admin')),
    reputation INTEGER DEFAULT 0,
    avatar_url TEXT,
    settings JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_login TIMESTAMPTZ,
    -- true once the person chose how their name appears (migrations/profiles_public_aliases.sql)
    public_name_confirmed BOOLEAN NOT NULL DEFAULT false
);

-- ============================================
-- PROJECTS
-- ============================================
CREATE TABLE public.projects (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    color TEXT DEFAULT '#4CAF50',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- USER LISTS
-- ============================================
CREATE TABLE public.user_lists (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    name_en TEXT,
    color TEXT DEFAULT '#FFD700',
    is_default BOOLEAN DEFAULT FALSE,
    is_system BOOLEAN DEFAULT FALSE,
    project_id INTEGER REFERENCES projects(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- LIST ITEMS
-- ============================================
CREATE TABLE public.list_items (
    id SERIAL PRIMARY KEY,
    list_id INTEGER REFERENCES user_lists(id) ON DELETE CASCADE,
    sys_id TEXT NOT NULL,
    shelfmark TEXT,
    title TEXT,
    fl_id TEXT,
    page TEXT,  -- page (image number) of the entry; NULL = a folio via fl_id, or the whole manuscript
    note TEXT,
    tags JSONB DEFAULT '[]',
    added_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- RECENT ITEMS
-- ============================================
CREATE TABLE public.recent_items (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
    sys_id TEXT NOT NULL,
    shelfmark TEXT,
    title TEXT,
    fl_id TEXT,
    viewed_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- CORRECTIONS
-- ============================================
CREATE TABLE public.corrections (
    id SERIAL PRIMARY KEY,
    author_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    sys_id TEXT NOT NULL,
    shelfmark TEXT,
    page_number INTEGER,
    original_text TEXT,
    corrected_text TEXT,
    notes TEXT,
    status TEXT DEFAULT 'draft' CHECK (status IN ('draft', 'pending', 'under_review', 'approved', 'rejected', 'merged')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    reviewed_at TIMESTAMPTZ,
    reviewed_by UUID REFERENCES auth.users(id),
    rejection_reason TEXT,
    upvotes INTEGER DEFAULT 0,
    downvotes INTEGER DEFAULT 0
);

-- ============================================
-- COMMENTS
-- ============================================
CREATE TABLE public.comments (
    id SERIAL PRIMARY KEY,
    author_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    sys_id TEXT,
    shelfmark TEXT,
    page_number INTEGER,
    content TEXT NOT NULL,
    scope TEXT DEFAULT 'page' CHECK (scope IN ('page', 'manuscript', 'general')),
    is_public BOOLEAN DEFAULT TRUE,
    parent_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    reply_count INTEGER DEFAULT 0
);

-- ============================================
-- DISCOVERIES
-- ============================================
CREATE TABLE public.discoveries (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    type TEXT DEFAULT 'discovery' CHECK (type IN ('discovery', 'question', 'observation', 'correction', 'join', 'comment', 'identification', 'note')),
    title TEXT,
    content TEXT,
    shelfmarks JSONB DEFAULT '[]',
    is_anonymous BOOLEAN DEFAULT FALSE,
    is_pinned BOOLEAN DEFAULT FALSE,
    is_hidden BOOLEAN DEFAULT FALSE,
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'answered', 'closed', 'featured')),
    upvotes INTEGER DEFAULT 0,
    downvotes INTEGER DEFAULT 0,
    view_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- FRAGMENT JOINS
-- ============================================
CREATE TABLE public.fragment_joins (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    fragment_a_sys_id TEXT NOT NULL,
    fragment_a_shelfmark TEXT,
    fragment_b_sys_id TEXT NOT NULL,
    fragment_b_shelfmark TEXT,
    join_type TEXT DEFAULT 'uncertain' CHECK (join_type IN ('physical', 'content', 'uncertain')),
    confidence TEXT DEFAULT 'possible' CHECK (confidence IN ('certain', 'probable', 'possible')),
    notes TEXT,
    evidence TEXT,
    status TEXT DEFAULT 'proposed' CHECK (status IN ('proposed', 'confirmed', 'rejected')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    confirmed_by UUID REFERENCES auth.users(id),
    confirmed_at TIMESTAMPTZ
);

-- ============================================
-- PGP DOCUMENTS
-- ============================================
-- Stores PGP document metadata and transcriptions
-- Multi-fragment documents are linked via document_fragments table
CREATE TABLE public.documents (
    pgpid INTEGER PRIMARY KEY,
    shelfmark_combined TEXT,
    document_type TEXT,
    tags JSONB DEFAULT '[]',
    doc_date_original TEXT,
    doc_date_standard TEXT,
    inferred_date_display TEXT,
    description TEXT,
    transcription TEXT,
    transcription_source TEXT,
    pgp_url TEXT GENERATED ALWAYS AS
        ('https://geniza.princeton.edu/documents/' || pgpid || '/') STORED,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- DOCUMENT FRAGMENTS (PGP join linkages)
-- ============================================
-- Links PGP documents to GenizahSearch fragments via sys_id
CREATE TABLE public.document_fragments (
    id SERIAL PRIMARY KEY,
    document_id INTEGER REFERENCES documents(pgpid) ON DELETE CASCADE,
    sys_id TEXT NOT NULL,
    shelfmark TEXT,
    sequence_order INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(document_id, sys_id)
);

-- ============================================
-- VOTES TABLES
-- ============================================
CREATE TABLE public.correction_votes (
    id SERIAL PRIMARY KEY,
    correction_id INTEGER REFERENCES corrections(id) ON DELETE CASCADE,
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
    vote INTEGER CHECK (vote IN (-1, 1)),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(correction_id, user_id)
);

CREATE TABLE public.discovery_votes (
    id SERIAL PRIMARY KEY,
    discovery_id INTEGER REFERENCES discoveries(id) ON DELETE CASCADE,
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
    vote INTEGER CHECK (vote IN (-1, 1)),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(discovery_id, user_id)
);

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================
CREATE INDEX idx_profiles_username ON profiles(username);
CREATE INDEX idx_user_lists_user_id ON user_lists(user_id);
CREATE INDEX idx_list_items_list_id ON list_items(list_id);
CREATE INDEX idx_list_items_sys_id ON list_items(sys_id);
CREATE INDEX idx_recent_items_user_id ON recent_items(user_id);
CREATE INDEX idx_corrections_author_id ON corrections(author_id);
CREATE INDEX idx_corrections_sys_id ON corrections(sys_id);
CREATE INDEX idx_corrections_status ON corrections(status);
CREATE INDEX idx_comments_author_id ON comments(author_id);
CREATE INDEX idx_comments_sys_id ON comments(sys_id);
CREATE INDEX idx_discoveries_user_id ON discoveries(user_id);
CREATE INDEX idx_fragment_joins_user_id ON fragment_joins(user_id);
CREATE INDEX idx_fragment_joins_fragment_a ON fragment_joins(fragment_a_sys_id);
CREATE INDEX idx_fragment_joins_fragment_b ON fragment_joins(fragment_b_sys_id);
CREATE INDEX idx_document_fragments_sys_id ON document_fragments(sys_id);
CREATE INDEX idx_document_fragments_document_id ON document_fragments(document_id);
CREATE INDEX idx_documents_tags ON documents USING GIN (tags);
CREATE INDEX idx_documents_document_type ON documents(document_type);


-- ============================================================================
-- PART 2: ROW LEVEL SECURITY (RLS)
-- ============================================================================

-- Enable RLS on all tables
ALTER TABLE profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_lists ENABLE ROW LEVEL SECURITY;
ALTER TABLE list_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE recent_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE corrections ENABLE ROW LEVEL SECURITY;
ALTER TABLE comments ENABLE ROW LEVEL SECURITY;
ALTER TABLE discoveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE fragment_joins ENABLE ROW LEVEL SECURITY;
ALTER TABLE correction_votes ENABLE ROW LEVEL SECURITY;
ALTER TABLE discovery_votes ENABLE ROW LEVEL SECURITY;
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_fragments ENABLE ROW LEVEL SECURITY;

-- ============================================
-- PROFILES POLICIES
-- ============================================
CREATE POLICY "Profiles are viewable by everyone" ON profiles FOR SELECT USING (true);
CREATE POLICY "Users can update own profile" ON profiles FOR UPDATE USING (auth.uid() = id);
CREATE POLICY "Users can insert own profile" ON profiles FOR INSERT WITH CHECK (auth.uid() = id);

-- ============================================
-- PROJECTS POLICIES
-- ============================================
CREATE POLICY "Users can view own projects" ON projects FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users can create own projects" ON projects FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own projects" ON projects FOR UPDATE USING (auth.uid() = user_id);
CREATE POLICY "Users can delete own projects" ON projects FOR DELETE USING (auth.uid() = user_id);

-- ============================================
-- USER LISTS POLICIES
-- ============================================
CREATE POLICY "Users can view own lists" ON user_lists FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users can create own lists" ON user_lists FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own lists" ON user_lists FOR UPDATE USING (auth.uid() = user_id);
CREATE POLICY "Users can delete own lists" ON user_lists FOR DELETE USING (auth.uid() = user_id);

-- ============================================
-- LIST ITEMS POLICIES
-- ============================================
CREATE POLICY "Users can view own list items" ON list_items FOR SELECT
    USING (EXISTS (SELECT 1 FROM user_lists WHERE user_lists.id = list_items.list_id AND user_lists.user_id = auth.uid()));
CREATE POLICY "Users can create own list items" ON list_items FOR INSERT
    WITH CHECK (EXISTS (SELECT 1 FROM user_lists WHERE user_lists.id = list_items.list_id AND user_lists.user_id = auth.uid()));
CREATE POLICY "Users can update own list items" ON list_items FOR UPDATE
    USING (EXISTS (SELECT 1 FROM user_lists WHERE user_lists.id = list_items.list_id AND user_lists.user_id = auth.uid()));
CREATE POLICY "Users can delete own list items" ON list_items FOR DELETE
    USING (EXISTS (SELECT 1 FROM user_lists WHERE user_lists.id = list_items.list_id AND user_lists.user_id = auth.uid()));

-- ============================================
-- RECENT ITEMS POLICIES
-- ============================================
CREATE POLICY "Users can view own recent items" ON recent_items FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users can create own recent items" ON recent_items FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can delete own recent items" ON recent_items FOR DELETE USING (auth.uid() = user_id);

-- ============================================
-- CORRECTIONS POLICIES
-- ============================================
CREATE POLICY "Anyone can view public corrections" ON corrections FOR SELECT
    USING (status != 'draft' OR auth.uid() = author_id);
CREATE POLICY "Users can create corrections" ON corrections FOR INSERT WITH CHECK (auth.uid() = author_id);
CREATE POLICY "Users can update own corrections" ON corrections FOR UPDATE USING (auth.uid() = author_id);
CREATE POLICY "Users can delete own draft corrections" ON corrections FOR DELETE
    USING (auth.uid() = author_id AND status = 'draft');

-- ============================================
-- COMMENTS POLICIES
-- ============================================
CREATE POLICY "Anyone can view public comments" ON comments FOR SELECT
    USING (is_public = true OR auth.uid() = author_id);
CREATE POLICY "Users can create comments" ON comments FOR INSERT WITH CHECK (auth.uid() = author_id);
CREATE POLICY "Users can update own comments" ON comments FOR UPDATE USING (auth.uid() = author_id);
CREATE POLICY "Users can delete own comments" ON comments FOR DELETE USING (auth.uid() = author_id);

-- ============================================
-- DISCOVERIES POLICIES
-- ============================================
CREATE POLICY "Anyone can view public discoveries" ON discoveries FOR SELECT
    USING (is_hidden = false OR auth.uid() = user_id);
CREATE POLICY "Users can create discoveries" ON discoveries FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own discoveries" ON discoveries FOR UPDATE USING (auth.uid() = user_id);

-- ============================================
-- FRAGMENT JOINS POLICIES
-- ============================================
CREATE POLICY "Anyone can view joins" ON fragment_joins FOR SELECT
    USING (true);
CREATE POLICY "Users can create joins" ON fragment_joins FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own joins" ON fragment_joins FOR UPDATE USING (auth.uid() = user_id);

-- ============================================
-- VOTES POLICIES
-- ============================================
CREATE POLICY "Users can view all votes" ON correction_votes FOR SELECT USING (true);
CREATE POLICY "Users can create own votes" ON correction_votes FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own votes" ON correction_votes FOR UPDATE USING (auth.uid() = user_id);
CREATE POLICY "Users can delete own votes" ON correction_votes FOR DELETE USING (auth.uid() = user_id);

CREATE POLICY "Users can view all discovery votes" ON discovery_votes FOR SELECT USING (true);
CREATE POLICY "Users can create own discovery votes" ON discovery_votes FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own discovery votes" ON discovery_votes FOR UPDATE USING (auth.uid() = user_id);
CREATE POLICY "Users can delete own discovery votes" ON discovery_votes FOR DELETE USING (auth.uid() = user_id);

-- ============================================
-- PGP DOCUMENTS POLICIES (public read, no write)
-- ============================================
CREATE POLICY "Documents are publicly viewable" ON documents
FOR SELECT TO public USING (true);

CREATE POLICY "Document fragments are publicly viewable" ON document_fragments
FOR SELECT TO public USING (true);


-- ============================================================================
-- PART 2b: DATA API GRANTS
-- From 2026-10-30 Supabase no longer grants Data API access to new public
-- tables automatically, so a table created by this file without these GRANTs
-- is unreachable from supabase-js/PostgREST ("permission denied"). The grants
-- only open the door; RLS above and add_privileged_column_guards.sql decide
-- which rows and values each caller may touch.
--   anon           read-only, and only on tables with a public SELECT policy
--                  (no write policy matches anon: they all check auth.uid()).
--   authenticated  full DML on community tables; the admin paths in both apps
--                  run as authenticated too. Read-only on the PGP tables.
--   service_role   everything (bypasses RLS; used by the import scripts).
-- ============================================================================

-- Community tables that anyone may read
GRANT SELECT ON public.profiles, public.corrections, public.comments,
    public.discoveries, public.fragment_joins,
    public.correction_votes, public.discovery_votes
    TO anon;

-- Community tables (public-read and private, per-user)
GRANT SELECT, INSERT, UPDATE, DELETE ON
    public.profiles, public.projects, public.user_lists, public.list_items,
    public.recent_items, public.corrections, public.comments,
    public.discoveries, public.fragment_joins,
    public.correction_votes, public.discovery_votes
    TO authenticated, service_role;

-- SERIAL ids: an INSERT through the API needs the sequence too
GRANT USAGE, SELECT ON SEQUENCE
    public.projects_id_seq, public.user_lists_id_seq, public.list_items_id_seq,
    public.recent_items_id_seq, public.corrections_id_seq, public.comments_id_seq,
    public.discoveries_id_seq, public.fragment_joins_id_seq,
    public.correction_votes_id_seq, public.discovery_votes_id_seq
    TO authenticated, service_role;

-- PGP reference tables: public read; writes only via service_role imports
GRANT SELECT ON public.documents, public.document_fragments TO anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.documents, public.document_fragments TO service_role;
GRANT USAGE, SELECT ON SEQUENCE public.document_fragments_id_seq TO service_role;


-- ============================================================================
-- PART 3: FUNCTIONS AND TRIGGERS
-- After this file, run migrations/add_privileged_column_guards.sql: its
-- triggers are what stop a client changing role, reputation and the
-- moderation columns (the policies here check only row ownership).
-- ============================================================================

-- ============================================
-- FUNCTION: Create profile & default list for new users
-- The username is what the person typed at an email/password sign-up, never
-- the email address (profiles is readable by anon), else a generated alias.
-- Keep both functions identical to migrations/profiles_public_aliases.sql
-- (tests/test_profiles_public_aliases_sql.py).
-- ============================================
create or replace function public.new_public_alias()
returns text
language plpgsql
volatile
set search_path = ''
as $$
declare
  adjectives text[] := array[
    'Attentive', 'Bold', 'Bright', 'Candid', 'Careful', 'Cheerful', 'Clever', 'Curious',
    'Devoted', 'Diligent', 'Discerning', 'Eager', 'Earnest', 'Generous', 'Gentle', 'Hopeful',
    'Inquisitive', 'Intrepid', 'Keen', 'Kindly', 'Learned', 'Lively', 'Meticulous', 'Modest',
    'Nimble', 'Observant', 'Patient', 'Persistent', 'Quiet', 'Resourceful', 'Serene', 'Sharp-eyed',
    'Steady', 'Studious', 'Thoughtful', 'Tireless', 'Unhurried', 'Wandering', 'Watchful', 'Wise'];
  nouns text[] := array[
    'Apothecary', 'Archivist', 'Astronomer', 'Bookbinder', 'Bookseller', 'Calligrapher',
    'Cartographer', 'Cataloguer', 'Chronicler', 'Collector', 'Commentator', 'Copyist',
    'Correspondent', 'Decipherer', 'Glassmaker', 'Grammarian', 'Illuminator', 'Lexicographer',
    'Librarian', 'Linguist', 'Mathematician', 'Merchant', 'Navigator', 'Papermaker',
    'Perfumer', 'Philosopher', 'Physician', 'Poet', 'Puzzler', 'Reader', 'Scholar', 'Scribe',
    'Seeker', 'Silversmith', 'Storyteller', 'Student', 'Teacher', 'Translator', 'Traveler', 'Weaver'];
  places text[] := array[
    'Acre', 'Aden', 'Aleppo', 'Alexandria', 'Almeria', 'Baghdad', 'Cairo', 'Constantinople',
    'Cordoba', 'Damascus', 'Fez', 'Fustat', 'Jerusalem', 'Mangalore', 'Narbonne', 'Palermo',
    'Qayrawan', 'Ramla', 'Seville', 'Sijilmasa', 'Tiberias', 'Toledo', 'Tripoli', 'Tyre'];
  candidate text;
begin
  for attempt in 1..60 loop
    candidate := adjectives[1 + floor(random() * cardinality(adjectives))::int]
                 || ' ' || nouns[1 + floor(random() * cardinality(nouns))::int];
    if attempt > 20 then
      candidate := candidate || ' of ' || places[1 + floor(random() * cardinality(places))::int];
    end if;
    if attempt > 40 then
      candidate := candidate || ' ' || (2 + floor(random() * 998))::int;
    end if;
    if not exists (select 1 from public.profiles p where lower(p.username) = lower(candidate)) then
      return candidate;
    end if;
  end loop;
  -- Not reached in practice (1,600 names, then 38,400, then millions). A clash
  -- here is caught by the UNIQUE constraint and handle_new_user tries again.
  return candidate || ' ' || (100000 + floor(random() * 900000))::int;
end;
$$;

revoke all on function public.new_public_alias() from public, anon, authenticated;

create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
declare
  meta jsonb := coalesce(new.raw_user_meta_data, '{}'::jsonb);
  typed jsonb := case when jsonb_typeof(meta -> 'signup_profile') = 'object'
                      then meta -> 'signup_profile' else '{}'::jsonb end;
  v_username text;
  v_full_name text;
  v_affiliation text;
  v_confirmed boolean;
begin
  if coalesce(new.raw_app_meta_data ->> 'provider', '') = 'email' then
    v_username    := left(nullif(btrim(coalesce(typed ->> 'username',    meta ->> 'username')),    ''), 100);
    v_full_name   := left(nullif(btrim(coalesce(typed ->> 'full_name',   meta ->> 'full_name')),   ''), 200);
    v_affiliation := left(nullif(btrim(coalesce(typed ->> 'affiliation', meta ->> 'affiliation')), ''), 200);
  end if;

  if v_username is not null
     and (position('@' in v_username) > 0
          or exists (select 1 from public.profiles p where lower(p.username) = lower(v_username))) then
    v_username := null;
  end if;

  -- A username or full name the person typed is their public name; otherwise
  -- they get an alias and the website asks them once.
  v_confirmed := v_username is not null or v_full_name is not null;
  v_username := coalesce(v_username, public.new_public_alias());

  begin
    insert into public.profiles (id, username, full_name, affiliation, public_name_confirmed)
    values (new.id, v_username, v_full_name, v_affiliation, v_confirmed);
  exception when unique_violation then
    -- The username was taken between the check and the insert (two sign-ups
    -- at once): a fresh alias. Any other clash (the id) raises again here and
    -- aborts, as before.
    insert into public.profiles (id, username, full_name, affiliation, public_name_confirmed)
    values (new.id, public.new_public_alias(), v_full_name, v_affiliation, v_full_name is not null);
  end;

  insert into public.user_lists (user_id, name, name_en, is_default)
  values (new.id, 'General', 'General', true);

  return new;
end;
$$;

-- Trigger for new user signup
DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
    AFTER INSERT ON auth.users
    FOR EACH ROW EXECUTE FUNCTION public.handle_new_user();

-- ============================================
-- FUNCTION: Update timestamps automatically
-- ============================================
CREATE OR REPLACE FUNCTION public.update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply to tables with updated_at
CREATE TRIGGER update_corrections_updated_at BEFORE UPDATE ON corrections
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER update_comments_updated_at BEFORE UPDATE ON comments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER update_discoveries_updated_at BEFORE UPDATE ON discoveries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();


-- ============================================================================
-- DONE! Verify tables exist in Table Editor
-- ============================================================================
