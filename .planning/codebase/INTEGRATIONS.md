# External Integrations

**Analysis Date:** 2026-02-05

## APIs & External Services

**National Library of Israel (NLI) - Image Delivery:**
- Primary service for manuscript images
  - IIIF Manifest API: `https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{system_id}-1/manifest`
  - IIIF Image Service: `https://iiif.nli.org.il/IIIFv21/FL{fl_id}/full/max/0/default.jpg`
  - Rosetta Delivery Fallback: `https://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_func=thumbnail&dps_pid=FL{fl_id}`
  - Integration: `web/api.py` (image proxy endpoints)
  - Caching: 5 minute TTL for FL ID lookups, 10 minute image cache
  - No authentication required (public API)

**Bodleian Libraries (Oxford) - Manuscript Data:**
- Manuscript collection data (13,000+ records)
- Integration: Referenced in `genizah_core.py` (Config.OXFORD_DB)
- Data file: `oxford_full_db.json` (bundled internally)
- No API integration (static dataset)

**Google Gemini AI:**
- AI text analysis for regex pattern generation
- SDK: `google-genai` (optional, graceful fallback if missing)
- API Endpoint: Google Gemini v1
- Model: `gemini-3-flash-preview` (configurable)
- Authentication: User-provided API key stored locally in `Genizah_Index/config.pkl`
- Integration: `genizah_core.py` AIManager class (lines 1902-2038)
- Usage: Generate Hebrew regex patterns for search refinement
- Status: Gracefully disables if library missing (HAS_GENAI check)

**OpenAI API:**
- Alternative AI provider for text analysis
- Endpoint: `https://api.openai.com/v1/chat/completions`
- Models: User-configurable (default: gpt-4)
- Authentication: User-provided API key via Bearer token
- Integration: `genizah_core.py` AIManager.send_prompt() (lines 1996-2013)
- Usage: JSON-based regex pattern generation
- Response format: JSON mode required

**Anthropic Claude API:**
- Alternative AI provider for text analysis
- Endpoint: `https://api.anthropic.com/v1/messages`
- Models: User-configurable (default: claude-3-sonnet)
- Authentication: User-provided API key via x-api-key header
- Integration: `genizah_core.py` AIManager.send_prompt() (lines 2015-2032)
- Usage: JSON-based regex pattern generation
- Max tokens: 1024

## Data Storage

**Primary Database:**
- Supabase (PostgreSQL managed cloud)
  - URL: `https://ylcpglwxompwjcufdemz.supabase.co`
  - Anon Key: `eyJ...` (JWT token in environment variables)
  - Tables: users, lists, items, corrections, comments, discoveries, joins
  - Authentication: GoTrue (Supabase auth service)
  - RLS: Row-level security on user_uuid foreign key

**Local Full-Text Index:**
- Tantivy (full-text search engine)
  - Path: `Genizah_Index/` or AppData equivalent
  - Lab Index: `Genizah_Index/lab_index/` (experimental search mode)
  - Client: `genizah_core.py` SearchEngine class

**File Storage:**
- Local filesystem only
  - Manuscript data: `libraries.csv` (217,000+ records, bundled)
  - Transcriptions: `Transcriptions.txt` (V0.8), `AllGenizah_OLD.txt` (V0.7)
  - Oxford data: `oxford_full_db.json` (bundled)
  - User exports: Reports directory in Documents or AppData

**Caching:**
- In-memory: Image cache in `web/api.py` (TTL-based, 5-10 minutes)
- Pickle cache: Metadata cache `Genizah_Index/metadata_cache.pkl`
- Browser storage: NiceGUI client-side storage (encrypted with `genizah-secret-v5`)

## Authentication & Identity

**Auth Provider:**
- Supabase GoTrue (OAuth-compatible)
  - Email/password registration
  - Session tokens stored client-side
  - JWT tokens for API requests
  - Integration: `web/supabase_client.py`, `web/auth_state.py`
  - Desktop support: `supabase_corrections_client.py`, `lists_sync.py`

**Client Libraries:**
- `supabase` (Python SDK)
- `gotrue` (Supabase auth client)
- `keyring` (Secure credential storage on Windows)

**Implementation:**
- Web: `supabase_client.py` (sign_up, sign_in, sign_out, get_session, get_current_user)
- Desktop: `supabase_corrections_client.py` (mirror of web client)
- Session persistence: Browser storage (web), pickle files (desktop)

## Monitoring & Observability

**Error Tracking:**
- None detected in codebase

**Logging:**
- Local file-based logging
  - Main log: `Genizah_Index/genizah.log` (rotating file handler)
  - Lab log: `Genizah_Index/lab/lab_genizah.log`
  - Crash log: `crash_log.txt` (desktop app)
  - Windows-safe rotating handler (graceful on file lock)
  - Integration: `genizah_core.py` SafeRotatingFileHandler class

**Analytics:**
- Google Analytics
  - Tracking ID: `G-LXT1PTKG3E`
  - Endpoint: `https://www.googletagmanager.com/gtag/js`
  - Integration: `web/main.py` (inline script in META_TAGS)
  - Scope: Web application only

## CI/CD & Deployment

**Hosting:**
- Web: NiceGUI development server (port 8081 default)
  - Can be proxied via nginx/Apache for production
  - Reload disabled in production (NICEGUI_RELOAD=false)
  - Auto-open disabled in production (NICEGUI_SHOW=false)
- Desktop: PyInstaller Windows executable bundle
  - Output: `dist/GenizahSearchPro/` or `build/GenizahSearchPro/`
  - Includes: `_internal/` directory with resources

**CI Pipeline:**
- None detected (manual builds)

**Version Management:**
- Centralized: `version.py` (APP_VERSION = "5.4.1")
- Referenced in: Web app, desktop app, documentation

## Environment Configuration

**Required Environment Variables:**

```bash
# Supabase (CRITICAL - defaults use shared development instance)
SUPABASE_URL=https://ylcpglwxompwjcufdemz.supabase.co
SUPABASE_ANON_KEY=eyJ...

# Application Settings
GENIZAH_PORT=8081
NLI_CACHE_TTL=300
IMAGE_CACHE_TTL=600
NICEGUI_RELOAD=false
NICEGUI_SHOW=false
NICEGUI_RECONNECT_TIMEOUT=30
```

**Secrets Location:**
- Supabase credentials: Environment variables or `.env` file
- AI API keys: User-provided via UI, stored in `Genizah_Index/config.pkl`
- NiceGUI storage secret: Hardcoded `genizah-secret-v5` (for browser storage encryption)

**Configuration Files:**
- `web/supabase_client.py` - Supabase URL and key defaults (lines 26-27)
- `lists_sync.py` - Fallback key reading from `.env` file
- `genizah_core.py` Config class - Paths and limits (lines 1591-1710)

## Webhooks & Callbacks

**Incoming Webhooks:**
- Supabase RLS triggers (server-side only, not exposed to client)
- No external webhook endpoints detected

**Outgoing Webhooks:**
- NLI image proxying (pull-based, not push)
- No push webhooks to external services detected

## Data Import/Export

**Data Sources (Read-only):**
- `libraries.csv` - Master metadata file (217,000 manuscript records)
- `oxford_full_db.json` - Bodleian collection
- `Transcriptions.txt` - Princeton Geniza Project data (9,364 matched records)
- All bundled with application, CSV loaded at startup

**Export Formats:**
- Excel (`.xlsx`) - Via openpyxl
- Word (`.docx`) - Via python-docx
- Plain text - Via file operations
- Implementation: `web/export_service.py`

## Rate Limiting & Quotas

**NLI Image API:**
- No explicit rate limiting detected
- Caching used to minimize requests (5-10 minute TTL)
- Timeout: 15 seconds per request

**AI Providers:**
- Rate limiting per provider (user API key subject to provider's limits)
- Timeout: 20 seconds per request
- Implemented: `genizah_core.py` AIManager.send_prompt()

**Supabase:**
- Standard Supabase tier limits apply
- RLS enforced per user_uuid

---

*Integration audit: 2026-02-05*
