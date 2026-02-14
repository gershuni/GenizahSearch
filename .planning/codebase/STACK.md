# Technology Stack

**Analysis Date:** 2026-02-05

## Languages

**Primary:**
- Python 3.10+ - Core application logic, search engine, AI integration, data processing
- JavaScript (via NiceGUI) - Web application frontend interactions

**Secondary:**
- Hebrew - Comments and documentation in codebase for domain-specific context

## Runtime

**Environment:**
- Python 3.10+

**Package Manager:**
- pip
- Lockfile: `requirements.txt`

## Frameworks

**Core Application:**
- NiceGUI (web framework) - Modern web interface at `web/main.py`
- PyQt6 - Desktop application GUI at `genizah_app.py`
- FastAPI (removed Jan 2026) - Replaced by Supabase for backend operations

**Search Engine:**
- Tantivy - Full-text search indexing and retrieval (Rust-based)
  - Location: `genizah_core.py` (SearchEngine class)
  - Index storage: `Genizah_Index/` directory (portable or AppData)
  - Lab mode indexing: `Genizah_Index/lab_index/`

**Export & Document:**
- openpyxl - Excel workbook generation
- python-docx - Word document generation

**Utilities:**
- requests - HTTP client for external APIs (NLI IIIF, image fetching)
- python-dotenv - Environment variable management
- keyring - Secure credential storage
- tqdm - Progress bars
- colorama - Terminal colors
- google-genai - Google Gemini API client (optional, graceful fallback)

## Key Dependencies

**Critical:**
- tantivy - Full-text search index engine (C/Rust FFI binding)
- supabase (Python SDK) - Cloud database and authentication
  - Package: `supabase`
  - Auth library: `gotrue` (Supabase auth client)
- PyQt6 - Desktop UI framework (5+ major widgets)

**Infrastructure:**
- nicegui - Web framework built on FastAPI
- requests - HTTP for image proxying and external APIs

**AI/ML:**
- google-genai - Google Gemini API (optional, configuration-based)
- Support for: OpenAI (via requests), Anthropic Claude (via requests)

## Configuration

**Environment Variables:**
```
SUPABASE_URL         = https://ylcpglwxompwjcufdemz.supabase.co
SUPABASE_ANON_KEY    = eyJ... (JWT token)
GENIZAH_PORT         = 8081 (default)
NLI_CACHE_TTL        = 300 (seconds, default 5 min)
IMAGE_CACHE_TTL      = 600 (seconds, default 10 min)
NICEGUI_RELOAD       = true/false (hot reload)
NICEGUI_SHOW         = true/false (auto-open browser)
NICEGUI_RECONNECT_TIMEOUT = 30 (seconds)
```

**Build Configuration:**
- App version: `version.py` (current: 5.4.1)
- Web storage secret: `genizah-secret-v5` (NiceGUI browser storage encryption)

**Data Files:**
- `libraries.csv` - Master metadata (217,000+ manuscript records)
- `oxford_full_db.json` - Bodleian Libraries dataset
- `Transcriptions.txt` (V0.8) - PGP transcription data
- `AllGenizah_OLD.txt` (V0.7) - Legacy transcription data

## Platform Requirements

**Development:**
- Windows 10+ (primary target, paths optimized for Windows)
- Python 3.10+ with pip
- Visual C++ redistributables (for Tantivy native bindings)
- LOCALAPPDATA environment variable (Windows AppData path)

**Production:**
- Web: NiceGUI development server (can be production-ready with reverse proxy)
  - Default port: 8081
  - Static files served from `web/static/`
  - Favicon: `web/static/favicon.ico`
- Desktop: Windows executable (PyInstaller bundle)
  - Bundles with `_internal/` directory containing resources
- Database: Supabase (PostgreSQL managed cloud)
- Image hosting: External (NLI IIIF, Bodleian, Rosetta)

## Data Storage Architecture

**Local (Client-side):**
- Tantivy Index: `Genizah_Index/` (full-text search index, portable or AppData)
- Image Cache: `Genizah_Index/images_cache/`
- Metadata Cache: `Genizah_Index/metadata_cache.pkl` (pickle)
- Config: `Genizah_Index/config.pkl` (AI settings, provider, keys)
- Logs: `Genizah_Index/genizah.log` (rotating file handler)

**Cloud (Supabase PostgreSQL):**
- User authentication (via GoTrue)
- User lists and items
- Corrections, comments, discoveries
- Joins/parallels
- Row-level security (RLS) based on user_id

## External Service Integrations

**Primary:**
- Supabase API - User data, lists, corrections, comments
- NLI IIIF API - Manuscript image manifests and delivery
- Google Gemini API - AI text analysis (optional, user-provided key)
- OpenAI API - Alternative AI provider (optional, user-provided key)
- Anthropic Claude API - Alternative AI provider (optional, user-provided key)

---

*Stack analysis: 2026-02-05*
