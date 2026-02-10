# Changelog

All notable changes to Genizah Search Pro will be documented in this file.

---

## [5.6.1] - 2026-02-10

### Bug Fixes — User Authentication & Corrections

#### Web: Singleton Supabase Client Fix
- **Critical fix**: Web app used a shared singleton Supabase client for all users. When multiple users were logged in, the auth session belonged to whoever signed in last — causing RLS policy failures for all other users' write operations (corrections, comments, discoveries, lists, etc.)
- Added `get_user_client()` — creates a per-user Supabase client from session tokens stored in NiceGUI's per-user storage
- All 28+ write functions now use the per-user client; read-only functions remain on the efficient singleton
- Session tokens are stored during email login and Google OAuth, and refreshed automatically when expired

#### Web: Admin Panel Corrections
- Fixed admin panel not showing pending corrections — the PostgREST join between `corrections` and `profiles` failed silently because there is no direct FK between the tables (both reference `auth.users` independently). Replaced with separate queries.
- Fixed admin unable to approve/reject corrections — added RLS policies allowing admins to update/delete corrections, comments, discoveries, and fragment joins from any user
- Admin write operations now use per-user client instead of singleton

#### Web: Correction Submission UX
- Fixed "parent element deleted" error after submitting a correction — the async handler's UI slot was destroyed by `update_content()` during the submit flow. Removed all `update_content()` calls from the async handler; all feedback now uses slot-independent `ui.notify()`
- Added success notification when correction is submitted
- RLS errors (42501) now show "Session expired — please log out and log back in" instead of raw Supabase error

#### Web: Profile Password Change
- Fixed password change using singleton client — could silently fail or change wrong user's password. Now uses per-user client.

#### Desktop: Login Error Messages
- Improved error messages for common login failures:
  - "Invalid email or password" for wrong credentials
  - "Email not confirmed" for unverified accounts
  - "No account found" for non-existent emails
  - Network-specific errors for connection issues

---

## [5.6.0] - 2026-02-09

### Milestone: Desktop Parity & PGP Integration

Full integration of Princeton Geniza Project (PGP) data across both web and desktop apps.

#### PGP Data (Phases 8-9)
- Imported 35,839 PGP documents with full metadata, 9,364 sources, 22,757 footnotes, 36,155 fragment links
- Shared document_service.py for Supabase access from both apps

#### Desktop PGP Core (Phase 10)
- PGP transcriptions and metadata in desktop Browse and Result dialogs
- Per-source directionality (editions RTL, English translations LTR)

#### Virtual Reading Desk (Phase 11)
- Multi-manuscript synchronized viewer in both web and desktop apps
- Stacked images + stacked texts with fragment-level sync scrolling
- Per-fragment version selector, zoom/rotate controls, lazy loading

#### Desktop PGP Discovery (Phase 12)
- PGP badges and tag display in search results
- PGP column sorting (click to show PGP-linked manuscripts first)
- PGP joins visible in desktop JoinsDialog
- Tag-based search as a search mode in both apps

#### PGP Tag Search UX
- "PGP Tags" as a search mode in the Mode dropdown (both apps)
- Desktop: hides query row, shows tag combo in Mode row
- Web: tag select replaces query input when PGP Tags mode selected
- Tag click navigation from result dialogs and browse pages
- 251 PGP tags with curated Hebrew translations and category grouping
- 16 categories: Document Types, Law & Society, Medicine, Trade, India Book, etc.
- Language-aware display: Hebrew UI shows "עברית (English)", English UI shows English only
- Category headers as visual separators in tag dropdowns

#### Phase 13 Deferred
- Transcription Search (full-text search in PGP transcriptions) was implemented but reverted
- Reason: Tantivy index build too slow for desktop distribution
- Will revisit with server-side index architecture in a future milestone
- Full documentation preserved in docs/archive/PHASE_13_TRANSCRIPTION_SEARCH_DEFERRED.md

---

## [5.5.0] - 2026-02-04

### New Feature: In-App Software Updates

The desktop application can now download and install updates without leaving the app.

#### How It Works
1. When a new version is available, a notification bar appears at the top
2. Click "Update Now" to start the update process
3. A progress dialog shows download progress
4. After download, the installer runs automatically in silent mode
5. The app restarts with the new version

#### Technical Details
- Downloads the official installer from GitHub Releases
- Uses Inno Setup's silent mode (`/VERYSILENT /RESTARTAPPLICATIONS`)
- Installer automatically closes the running app, updates files, and restarts
- UAC prompt will appear (same as manual install) since app is in Program Files
- Falls back to opening browser if installer not found in release

#### Files Changed
- `gui_threads.py` - New `UpdateDownloaderThread` class for downloading with progress
- `genizah_app.py` - New `UpdateProgressDialog` for update UI
- `CompileScriptGenizah.iss` - Added `CloseApplications` and `RestartApplications` settings

---

## [5.4.1] - 2026-02-03

### Enhancement: "Remember Me" Login Feature

Both the desktop and web applications now support saving login credentials.

#### Desktop Application
- **"Remember me" checkbox**: New checkbox in the login dialog to opt-in to credential saving
- **Secure storage**: Password stored in Windows Credential Manager (via `keyring` library) - not in plain text files
- **Persistent across updates**: Credentials survive software updates since they're stored in user profile, not application folder
- **Easy to disable**: Uncheck "Remember me" to clear saved credentials

#### Web Application
- **"Remember me" checkbox**: New checkbox in the login dialog
- **Email remembered**: Email address saved in browser localStorage for convenience
- **Session persistence**: Login session already persists via Supabase cookies

---

## [5.4.0] - 2026-02-03

### New Feature: Library/Holding Institution Display

Every manuscript record now shows which library or collection holds the original document.

- **Coverage:** 99.99% of ~217,000 records have library codes assigned (only 14 records with missing source data)
- **Libraries identified:** 70+ institutions including Cambridge (CUL), JTS, National Library of Russia, Bodleian (Oxford), Manchester, British Library, Alliance Israélite, Library of Geneva, Senckenberg (Frankfurt), Schocken Institute, and many more

#### Web Application
- Library badge with code (e.g., "CUL") displayed in search results with full name tooltip
- Library field in Advanced View metadata cards
- Library field in browse page metadata panel
- Library column in all Excel exports (Search, Lists, Parallels)

#### Desktop Application
- New "Library" column in search results table
- Filterable/sortable like other columns
- Library column in Excel/Word exports

#### Technical Details
- New `library_code` column in `libraries.csv`
- New functions: `LIBRARY_CODES` constant, `get_library_display()`, `get_library_for_id()`
- Backward compatible with old CSV files (gracefully handles missing column)

### Enhancement: Nikud (Vowel Mark) Removal in Parallels Search

Parallels search now automatically strips Hebrew vowel marks (nikud) and cantillation marks from text before matching. This ensures consistent results whether the input text contains nikud or not.

- Affects both Lab Mode and Standard parallels search
- Also strips nikud from filter/exclude text for consistent filtering
- New function: `strip_nikud()` in `genizah_core.py`

### Enhancement: Advanced View Dialog Improvements

The Advanced View dialog (opened from search results) has been significantly enhanced:

#### Navigation & Viewing
- **Fixed navigation bug**: Results now navigate in-place without closing/reopening the dialog
- **Page navigation**: Browse pages within a manuscript using prev/next buttons
- **IIIF image viewer**: Side-by-side image panel with zoom, rotate, and pan controls
- **Fullscreen mode**: Distraction-free view with compact navigation bar
- **Image toggle**: Show/hide image panel as needed

#### Inline Editing
- Edit text directly in the Advanced View (same as Browse page)
- Save drafts, submit for review, or publish immediately (for editors/admins)
- Visual feedback: orange border for unsaved changes, green for saved
- Notes field for correction comments

#### Bug Fixes
- Fixed "Unknown" author display in version selector (now joins profiles table)
- Fixed script tag error in edit dialog (NiceGUI compatibility)

### Files Changed
- `genizah_core.py` - Core library functions, CSV loading, nikud removal
- `genizah_app.py` - Desktop table columns
- `web/services.py` - Data classes and page retrieval
- `web/pages/search.py` - Library badge display, Advanced View dialog enhancements
- `web/pages/browse.py` - Metadata panel
- `web/components/text_editor.py` - Fixed script tag in HTML
- `web/supabase_client.py` - Added profiles join to get_corrections()
- `web/export_service.py` - Export functions
- `libraries.csv` - Added library_code column

---

## [5.3.1] - 2026-02-03

### Bug Fixes

- **RTL navigation arrows:** Fixed all directional icons (arrows, chevrons, skip buttons) that were reversed in Hebrew UI mode. Icons now correctly flip direction based on language setting.
- **Removed directional icons from action buttons:** Removed `send` arrow icons from Submit/Share/Reply buttons and `arrow_forward` from Go button, as these looked incorrect in RTL mode.
- **Missing title metadata in search results:** Fixed bug where title and other metadata wasn't displayed in search results. The `get_display_data()` method now uses proper fallback logic (CSV bank → NLI cache) matching the browse page behavior.
- **Search panel auto-collapse:** Fixed scroll-based auto-collapse that wasn't working. Added proper class targeting for the results scroll area and improved JavaScript detection.
- **Search panel collapse/expand visibility:** Fixed panels not showing/hiding properly by using explicit styles with `!important` flags.
- **Advanced Options inside search panel:** Moved the Advanced Options expansion inside the collapsible search panel so it hides when the search bar collapses.
- **Search results layout overflow:** Fixed text getting cut off when zooming or resizing window. Removed `max-width` restrictions and added proper flex wrapping and word-wrap styles.
- **Removed Edit/Comment buttons from result cards:** Cleaned up search result cards by removing Edit and Send Comment buttons (still available in the detailed viewer).

### Enhancements

- **Full Text pane highlighting:** Added search term highlighting to the Full Text tab in search results, matching the highlighting in the Match pane.

### Files Changed

- `genizah_core.py` - Fixed `get_display_data()` metadata fallback
- `web/pages/search.py` - Search panel collapse, Advanced Options placement, result card layout, Full Text highlighting
- `browse.py` - Page/shelfmark navigation, Go button, Back buttons, Submit buttons
- `document.py` - Back button, page navigation
- `home.py` - Start Search, Find Parallels, Browse, View All buttons
- `discoveries.py` - Back buttons, Reply/Share buttons
- `comment_dialog.py` - Back button, Submit button
- `joins_panel.py` - Navigation indicator, Back button
- `text_editor.py` - Submit Correction button

---

## [5.3.0] - 2026-02-02

### New Feature: Cross-Paragraph Search

A new parallel search mode that finds manuscripts with text spanning paragraph boundaries, now available on **both Web and Desktop**.

- **Why it's useful:** Text within paragraphs often contains citations (Mishnah, Talmud, known phrases). Text that crosses paragraph boundaries is unlikely to be a citation, effectively filtering out noise.

- **Three search modes:**
  - **Full search** - All results (default)
  - **Cross-paragraph only** - Only matches that span paragraph breaks
  - **Combined** - All results, with boundary-crossing matches boosted

- **Customizable delimiters:** Line break, blank line (paragraph), period, colon

- **Visual indicators:**
  - Web: Amber "Cross-paragraph" badge; red `|` at boundary points in matched text
  - Desktop: 🔗 emoji prefix on scores; tooltips showing match count

- **Advanced settings:** Configurable boost factor (1.0-3.0), minimum boundary matches filter, minimum delimiter distance

- **Real-time feedback:** Desktop shows boundary count and crossing chunks before search

### Bug Fixes

- **Duplicate results fix:** Fixed bug where same manuscript appeared multiple times in Standard search when found by overlapping chunks routed to different filter maps
- **Boundary detection:** Improved to require words on BOTH sides of the boundary (not just touching)
- **Desktop boundary stats:** Fixed silent exception handling, now logs errors properly
- **Desktop translation:** Fixed fragmented translation string for cross-paragraph tooltips
- **Anonymous display bug:** Fixed discoveries showing as "Anonymous" even when user didn't check anonymous - now fetches profile data properly
- **Dialog Esc key:** Fixed Share Discovery dialog flickering when pressing Esc (removed 'persistent' prop)
- **Simplified Share Discovery:** Removed superfluous "Related manuscripts" section from dialog
- **Database constraint:** Updated discoveries type constraint to include 'identification' and 'note' types

### Technical Changes

- `CompositionThread` and `LabCompositionThread` now accept boundary parameters
- `LabSettings` stores boundary preferences (mode, delimiter, boost, min matches, min distance)
- Added temporary storage fallback for settings when `lab_engine` not initialized

### Documentation

- Updated help page with cross-paragraph search documentation (English and Hebrew)
- Updated BOUNDARY_SEARCH_SPEC.md with completed desktop implementation details

---

## [5.2.0] - 2026-02-01

### Documentation

- **Help Center rewrite:** Comprehensive bilingual help page covering Search, Parallels, Browse, Lists, and Export features
- **File index:** New `docs/FILE_INDEX.md` with comprehensive listing of all project files

### Codebase Cleanup

- **Root directory cleanup:** Removed unused directories (`backend/`, `backend_legacy/`, `frontend_web/`, `build/`, `Reports/`, `Results/`)
- **Scripts organization:** Moved utility scripts to `scripts/` folder (cleanup, verify, debug scripts)
- **Branch cleanup:** Deleted 25 stale/merged git branches

### UX Improvements

- **Search spinners:** More prominent animated spinners (bars instead of dots, larger size, pulsing text)
- **Parallels search feedback:** Spinner and status now visible in control panel without scrolling
- **Stop button:** Added to regular search (swaps with search button during search), shows partial results when stopped
- **Filter sources badge:** Shows count of enabled filter sources on the expansion header
- **Filter tooltip:** Explains filter feature in both English and Hebrew

### Header Branding

- **Dicta branding:** Header now shows "Dicta Genizah Search" with Hebrew subtitle "אתר הגניזה מבית דיקטה"
- **Mobile optimization:** Header hides on scroll down, reveals on scroll up (mobile only)
- **Responsive logo:** Text hidden on small screens, only icon shows

### Backend Migration: Supabase

- **Complete Supabase migration:** Replaced FastAPI backend with direct Supabase integration
- All authentication now handled by Supabase Auth
- User lists, corrections, and comments stored in Supabase
- Built-in rate limiting and security features

### Authentication Fixes

- **OAuth flow:** Fixed Google OAuth to use Supabase's `sign_in_with_oauth` method with proper state parameter
- **Session handling:** Implicit flow tokens properly extracted from URL hash on callback
- **Forgot password (desktop):** Added password reset link to desktop app login dialog for OAuth users
- **OAuth user guidance:** Web Google signup now shows note about setting password for desktop app login

### Row Level Security (RLS) Fixes

- **RLS policies:** Fixed all INSERT/UPDATE/DELETE policies to use `authenticated` role instead of `public`
- **Column naming:** Updated queries to use correct column names (`author_id` for comments/corrections, `user_id` for others)
- **Profile joins removed:** Removed `profiles` table joins from queries that failed without FK relationships
- **SQL script:** Added `scripts/fix_rls_policies.sql` for bulk RLS policy updates

### Community Feed & Comments

- **Feed loading:** Fixed `get_feed_items` to properly load discoveries, corrections, comments, and joins
- **Comments display:** Fixed comments to appear on browse pages (removed failing profiles join)
- **Profile page:** Fixed to load data from profile storage instead of auth user

### Lists & Projects Management

- **Management mode toggle:** New "Manage lists" button reveals edit controls
- **Icon-based actions:** Replaced dropdown menus with direct action buttons (rename, move to project, delete)
- **Improved UI:** Cleaner interface with actions hidden by default
- **Auto-sync:** Lists automatically sync between devices for logged-in users
- **Soft delete support:** Lists can be recovered after deletion

### Bug Fixes

- **Register button:** Fixed bug where clicking "Register" opened login dialog instead of register
- **Dependencies:** Added missing `gotrue` and `python-dotenv` to requirements.txt

### Documentation

- **English translation:** PRE_LAUNCH_CHECKLIST.md translated from Hebrew to English
- **Documentation reorganization:** New `docs/` structure with guides, plans, and specs

---

## [5.1.0] - 2026-01-27

### Web Platform: Dicta Genizah Search (אתר הגניזה של דיקטה)

The web platform has been rebranded to **"Dicta Genizah Search"** (אתר הגניזה של דיקטה), reflecting our partnership with DICTA. The desktop application remains "Genizah Search Pro".

### Accessibility Compliance (WCAG 2.0 / IS 5568)

- Full compliance with Israeli Standard 5568 and WCAG 2.0 AA accessibility guidelines
- Improved Hebrew RTL layout and text alignment
- Semantic headings with proper sizing
- Enhanced keyboard navigation support

### New Features

- **Automatic Text Source Filtering:** Intelligent filtering based on Sefaria text database
- **Enhanced Variant Search:** Improved letter variation handling (multi-character, 2-to-1 letters)
- **Fullscreen Edit Mode:** Image controls with splitter for side-by-side viewing
- **Fragment Joins System:** Connect related fragments via Discovery Center
- **Exclude Words UI:** Exclude specific words from search results
- **Citation Footer:** Dismissible footer with publishing guidelines

### Browse & Viewer Improvements

- Side-by-side layout for browse page
- Image drag, rotate, and wheel zoom in manuscript viewer
- Image credits/attribution for NLI and Oxford sources
- Title truncation with tooltips for long titles

### UI/UX Improvements

- Desktop app download page with website integration
- SEO metadata for social sharing
- Dark mode fixes across multiple pages
- Dismissible transcription disclaimer banner
- Creator credit in sidebar footer

### Technical Improvements

- Migrated to google-genai SDK (gemini-3-flash-preview)
- SSL certificate verification for all HTTPS requests
- Build optimizations for faster packaging
- Antivirus false positive documentation
- Server-side index building support

### Bug Fixes

- Fixed NLI and Oxford image loading issues
- Fixed version comparison for different component lengths
- Fixed RTL layout overlap and alignment issues
- Fixed theme toggle functionality
- Fixed fullscreen edit image loading

---

## [5.0.0] - 2026-01-19

### Major Release: Web Platform & Community Features

Version 5.0 marks the launch of the **Genizah Search Pro Web Platform** and introduces comprehensive **Community Features**, transforming the software into a collaborative research environment.

---

### Web Platform

- **Public Web Application:** Full-featured web interface accessible from any browser at [genizahsearch.com](https://genizahsearch.com)
- **Mobile Responsive Design:** Optimized experience for tablets and phones with adaptive layouts
- **User Authentication:** Registration, login, and profile management
- **Offline Mode:** Community features work offline and sync when reconnected

### Community Features

- **Discovery Center:** Share and explore research discoveries with the community
  - Voting system for discoveries
  - Pin important discoveries
  - Mark discoveries as answered/resolved
  - Multiple shelfmark references per discovery
  - Document lookup from discovery dialog
- **Comments System:** Add comments to manuscripts with page-specific references
  - Public and private comments
  - Draft support for work-in-progress notes
- **Corrections & Contributions:** Submit corrections to transcriptions
  - Review workflow for submitted corrections
  - Track your contributions in "My Edits & Comments" page

### Admin Features

- User management panel with role assignment
- Corrections review system for approving/rejecting submissions
- Profile editing capabilities for users

### Desktop App Integration

- New dialogs for comments and text editing
- Improved synchronization between desktop and web data
- Consistent page number handling across platforms
- Full offline mode for community tab

### Stability & Performance

- Fixed infinite timer issues causing connection problems
- Improved CSS performance for faster page loads
- Better error handling for offline scenarios
- Disabled reload mode for improved stability

---

## [4.1.1] - 2026-01-12

### Fixes
- Corrected star icon alignment in search results
- Fixed list preview image loading

---

## [4.1.0] - 2026-01

### Personal Lists Management
- New tab for creating and organizing personal manuscript lists
- Browse by list: side panel in Browse tab for navigating custom lists
- List filtering: filter search results based on personal lists

### Interface Refinements
- Compact context view at the bottom of the interface
- Reports saved to user's Documents directory
- Resolved duplicate search results issue

---

## [4.0.0] - 2025

### Major Update: From Search Engine to Research Suite

### Integrated Visual Analysis (IIIF)
- In-app viewer for high-resolution manuscript images
- Direct integration with National Library of Israel and Cambridge University Library
- Sequential page and manuscript navigation
- Built-in zoom and rotation controls

### Oxford Bodleian Integration
- Full support for Oxford Bodleian Library manuscripts
- Neubauer catalog integration
- Part-based and folio-based navigation

### Lab Mode (Experimental)
- Parallel detection algorithm based on Shmidman, Koppel, and Porat (2016)
- Rare letter encoding for spelling variation tolerance

### Additional Features
- Cross-page search
- Enhanced export (Excel, CSV, DOCX)
- Find in text with highlighting
- Composition search for parallel detection

---

## [3.6.0] and earlier

See previous release notes for historical changes.
