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
    last_login TIMESTAMPTZ
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
-- PART 3: FUNCTIONS AND TRIGGERS
-- ============================================================================

-- ============================================
-- FUNCTION: Create profile & default list for new users
-- ============================================
CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS TRIGGER AS $$
BEGIN
    -- Create profile
    INSERT INTO public.profiles (id, username)
    VALUES (NEW.id, NEW.email);

    -- Create default "General" list
    INSERT INTO public.user_lists (user_id, name, name_en, is_default)
    VALUES (NEW.id, 'General', 'General', true);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

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
