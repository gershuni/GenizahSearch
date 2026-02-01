-- Fix RLS policies to use 'authenticated' role instead of 'public'
-- Run this in Supabase SQL Editor

-- ============================================
-- COMMENTS TABLE
-- ============================================
DROP POLICY IF EXISTS "Users can create comments" ON comments;
CREATE POLICY "Users can create comments" ON comments
FOR INSERT TO authenticated
WITH CHECK (auth.uid() = author_id);

DROP POLICY IF EXISTS "Users can update own comments" ON comments;
CREATE POLICY "Users can update own comments" ON comments
FOR UPDATE TO authenticated
USING (auth.uid() = author_id);

DROP POLICY IF EXISTS "Users can delete own comments" ON comments;
CREATE POLICY "Users can delete own comments" ON comments
FOR DELETE TO authenticated
USING (auth.uid() = author_id);

-- ============================================
-- CORRECTIONS TABLE (uses author_id)
-- ============================================
DROP POLICY IF EXISTS "Users can create corrections" ON corrections;
CREATE POLICY "Users can create corrections" ON corrections
FOR INSERT TO authenticated
WITH CHECK (auth.uid() = author_id);

DROP POLICY IF EXISTS "Users can update own corrections" ON corrections;
CREATE POLICY "Users can update own corrections" ON corrections
FOR UPDATE TO authenticated
USING (auth.uid() = author_id);

DROP POLICY IF EXISTS "Users can delete own corrections" ON corrections;
CREATE POLICY "Users can delete own corrections" ON corrections
FOR DELETE TO authenticated
USING (auth.uid() = author_id);

-- ============================================
-- DISCOVERIES TABLE (uses author_id)
-- ============================================
DROP POLICY IF EXISTS "Users can create discoveries" ON discoveries;
CREATE POLICY "Users can create discoveries" ON discoveries
FOR INSERT TO authenticated
WITH CHECK (auth.uid() = author_id);

DROP POLICY IF EXISTS "Users can update own discoveries" ON discoveries;
CREATE POLICY "Users can update own discoveries" ON discoveries
FOR UPDATE TO authenticated
USING (auth.uid() = author_id);

DROP POLICY IF EXISTS "Users can delete own discoveries" ON discoveries;
CREATE POLICY "Users can delete own discoveries" ON discoveries
FOR DELETE TO authenticated
USING (auth.uid() = author_id);

-- ============================================
-- FRAGMENT_JOINS TABLE (uses author_id)
-- ============================================
DROP POLICY IF EXISTS "Users can create joins" ON fragment_joins;
CREATE POLICY "Users can create joins" ON fragment_joins
FOR INSERT TO authenticated
WITH CHECK (auth.uid() = author_id);

DROP POLICY IF EXISTS "Users can update own joins" ON fragment_joins;
CREATE POLICY "Users can update own joins" ON fragment_joins
FOR UPDATE TO authenticated
USING (auth.uid() = author_id);

DROP POLICY IF EXISTS "Users can delete own joins" ON fragment_joins;
CREATE POLICY "Users can delete own joins" ON fragment_joins
FOR DELETE TO authenticated
USING (auth.uid() = author_id);

-- ============================================
-- USER_LISTS TABLE
-- ============================================
DROP POLICY IF EXISTS "Users can manage own lists" ON user_lists;
CREATE POLICY "Users can manage own lists" ON user_lists
FOR ALL TO authenticated
USING (auth.uid() = user_id)
WITH CHECK (auth.uid() = user_id);

-- ============================================
-- LIST_ITEMS TABLE
-- ============================================
DROP POLICY IF EXISTS "Users can manage own list items" ON list_items;
CREATE POLICY "Users can manage own list items" ON list_items
FOR ALL TO authenticated
USING (auth.uid() IN (SELECT user_id FROM user_lists WHERE id = list_id))
WITH CHECK (auth.uid() IN (SELECT user_id FROM user_lists WHERE id = list_id));

-- ============================================
-- PROFILES TABLE
-- ============================================
DROP POLICY IF EXISTS "Users can update own profile" ON profiles;
CREATE POLICY "Users can update own profile" ON profiles
FOR UPDATE TO authenticated
USING (auth.uid() = id)
WITH CHECK (auth.uid() = id);

-- Allow reading all profiles (for displaying usernames)
DROP POLICY IF EXISTS "Anyone can view profiles" ON profiles;
CREATE POLICY "Anyone can view profiles" ON profiles
FOR SELECT USING (true);

-- ============================================
-- CORRECTION_VOTES TABLE
-- ============================================
DROP POLICY IF EXISTS "Users can vote on corrections" ON correction_votes;
CREATE POLICY "Users can vote on corrections" ON correction_votes
FOR ALL TO authenticated
USING (auth.uid() = user_id)
WITH CHECK (auth.uid() = user_id);

-- ============================================
-- DISCOVERY_VOTES TABLE
-- ============================================
DROP POLICY IF EXISTS "Users can vote on discoveries" ON discovery_votes;
CREATE POLICY "Users can vote on discoveries" ON discovery_votes
FOR ALL TO authenticated
USING (auth.uid() = user_id)
WITH CHECK (auth.uid() = user_id);

-- ============================================
-- RECENT_ITEMS TABLE
-- ============================================
DROP POLICY IF EXISTS "Users can manage own recent items" ON recent_items;
CREATE POLICY "Users can manage own recent items" ON recent_items
FOR ALL TO authenticated
USING (auth.uid() = user_id)
WITH CHECK (auth.uid() = user_id);

-- ============================================
-- PROJECTS TABLE (if user-specific)
-- ============================================
DROP POLICY IF EXISTS "Users can manage own projects" ON projects;
CREATE POLICY "Users can manage own projects" ON projects
FOR ALL TO authenticated
USING (auth.uid() = user_id)
WITH CHECK (auth.uid() = user_id);
