# Phase 47: Foundation + Background Removal - Research

**Researched:** 2026-03-15
**Domain:** Image processing (background removal), data modeling, SQLite sidecar persistence
**Confidence:** HIGH

## Summary

Phase 47 delivers three foundational components for the Fragment Puzzle feature: (1) a shared `PuzzleDocument`/`PuzzleFragment` data model with JSON serialization roundtrip, (2) a `joins.db` SQLite sidecar following established patterns, and (3) an HSV-based background removal engine using Pillow + NumPy that strips solid-color library scanning backgrounds from IIIF manuscript images. No canvas UI is built in this phase.

The background removal approach is well-constrained: Genizah fragment photos have solid-color backgrounds (NLI gray, Cambridge dark, Oxford cream, Manchester varies). Pillow natively supports HSV mode conversion (`Image.convert('HSV')`) and alpha channel manipulation (`putalpha()`), making this achievable without OpenCV. The user-adjustable threshold slider and original/stripped toggle are core requirements.

**Primary recommendation:** Implement background removal as a single shared module (`shared/background_removal.py`) using Pillow's HSV conversion + NumPy array masking + `putalpha()` for alpha channel. Add Pillow and NumPy as explicit dependencies in requirements.txt. Follow the `nli_crossref_service.py` singleton pattern for `shared/puzzle_service.py` and joins.db access.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Fragment identity: both sys_id + folio_label (canonical) and FL ID (cached for fast NLI image loading). FL IDs available for all images.
- PuzzleDocument contains multiple PuzzleFragments, each storing: sys_id, folio_label, fl_id, position (x, y), rotation (degrees), scale, flip_h, flip_v, bg_removal_threshold
- joins.db SQLite sidecar follows established pattern (pgp.db, fjms_enrichment.db) -- singleton service, graceful degradation, thread-safe
- Metadata is source of truth; processed images cached locally for fast reload
- One shared Python module (Pillow + NumPy) for background removal -- web calls server-side via API, desktop calls directly. Same code, same results.
- IIIF images proxied through existing web/api.py patterns
- Default ~1200px images for canvas interaction, user can toggle to full resolution when needed
- Auto-process on fragment add: fetch image -> remove background -> show stripped result. Takes 1-3 seconds.
- User can toggle between stripped and original view
- User can adjust threshold slider
- Testing strategy: 2-3 sample test images from each major library, visual preview tool for side-by-side comparison with threshold slider, manual eyeball review

### Claude's Discretion
- HSV color space ranges for each background type (blue, green, grid paper, white)
- Alpha channel handling and edge smoothing approach
- Cache directory structure and cleanup policy
- Exact joins.db schema column types and indexes

### Deferred Ideas (OUT OF SCOPE)
- "Load known join" from FJMS join groups -- Phase 52 (Community + Integration) scope
- DPI auto-calibration from IIIF physicalScale metadata -- deferred, manual resize is baseline
- Alpha feathering for smooth edges -- deferred to future enhancement (BGRM-04)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| BGRM-01 | Fragment images are automatically stripped of solid-color backgrounds (parchment shape visible) | HSV color space segmentation via Pillow + NumPy; corner-sampling for auto-detection of background color; binary mask creation with morphological cleanup |
| BGRM-02 | User can toggle between stripped and original image view | PuzzleFragment stores both original image path/URL and processed RGBA image; toggle is a display concern handled by the visual preview tool in this phase |
| BGRM-03 | User can adjust the background removal threshold | `bg_removal_threshold` field on PuzzleFragment; re-run removal pipeline with new threshold; real-time preview in visual tuning tool |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Pillow | >=10.0 | Image loading, HSV conversion, RGBA output, putalpha(), PNG export | Native HSV mode support (`Image.convert('HSV')`), `putalpha()` for alpha mask application. NOT currently in requirements.txt -- must be added explicitly. Already an indirect dependency of NiceGUI but not installed in current environment. |
| NumPy | >=1.24 (current: 2.4.3) | Vectorized pixel array operations for mask creation | Fast color distance calculation, boolean masking, morphological operations. Already installed. |
| Python stdlib colorsys | (builtin) | HSV/RGB conversion reference | Available for single-pixel conversions; Pillow's batch convert is preferred for full images |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| scipy.ndimage | >=1.10 | Gaussian blur for mask edge smoothing | Only if simple NumPy box blur produces visible artifacts. DEFERRED per user decision (alpha feathering is BGRM-04). |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Pillow HSV | OpenCV cv2.cvtColor | OpenCV adds 50MB+ dependency, BGR color confusion. Pillow HSV is sufficient for solid-color backgrounds. User decision: no OpenCV. |
| Pillow HSV | matplotlib.colors.rgb_to_hsv | Works but adds matplotlib dependency. Pillow has native HSV mode. |
| NumPy morphology | scipy.ndimage.binary_erosion/dilation | scipy is more powerful but not needed for basic cleanup. NumPy-only approach works for v1. |

**Installation:**
```bash
pip install Pillow numpy
# Add to requirements.txt:
# Pillow
# numpy
```

**IMPORTANT:** Pillow is NOT currently installed in the project environment (ModuleNotFoundError confirmed). NumPy 2.4.3 is installed. Pillow must be added to requirements.txt and installed before implementation begins.

## Architecture Patterns

### Recommended Project Structure
```
shared/
  background_removal.py    # HSV-based background removal engine
  puzzle_model.py          # PuzzleDocument, PuzzleFragment dataclasses
  puzzle_service.py        # joins.db access, serialization, singleton service
joins_data/
  joins.db                 # SQLite sidecar (created on first use, read-write)
tests/
  test_background_removal.py  # Unit tests for removal pipeline
  test_puzzle_model.py        # Roundtrip serialization tests
  test_puzzle_service.py      # joins.db CRUD tests
scripts/
  preview_background_removal.py  # Visual preview tool (threshold slider)
```

### Pattern 1: SQLite Sidecar Service (from nli_crossref_service.py)
**What:** Singleton service with auto-detect project root, graceful degradation, thread-safe mode.
**When to use:** For joins.db access.
**Example:**
```python
# Source: shared/nli_crossref_service.py (verified in codebase)
_SIDECAR_FILENAME = "joins.db"
_SIDECAR_DIR = "joins_data"

def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for libraries.csv up from this file."""
    current = Path(__file__).resolve().parent
    for _ in range(5):
        if (current / "libraries.csv").exists():
            return current
        current = current.parent
    return None

class PuzzleService:
    def __init__(self, db_path: str = None, thread_safe: bool = False):
        # ... same pattern as NliCrossrefService.__init__
```

**Key difference from other sidecars:** joins.db is READ-WRITE (user creates/updates join documents), whereas pgp.db, fjms_enrichment.db, nli_crossref.db are all read-only. This means:
- Connection opens without `?mode=ro`
- Need `CREATE TABLE IF NOT EXISTS` on first use
- Need WAL mode for concurrent access: `PRAGMA journal_mode=WAL`
- Need explicit `commit()` after writes

### Pattern 2: Dataclass Model (from reading_desk_model.py)
**What:** Shared dataclass containers for multi-fragment state.
**When to use:** PuzzleDocument and PuzzleFragment definitions.
**Example:**
```python
# Source: shared/reading_desk_model.py (verified in codebase)
@dataclass
class PuzzleFragment:
    sys_id: str
    folio_label: str          # e.g., "1r", "2v"
    fl_id: str                # NLI FL ID for image loading
    x: float = 0.0
    y: float = 0.0
    rotation: float = 0.0    # degrees
    scale: float = 1.0
    flip_h: bool = False
    flip_v: bool = False
    bg_removal_threshold: float = 30.0  # HSV color distance

@dataclass
class PuzzleDocument:
    id: str                   # UUID
    title: str = ''
    notes: str = ''
    join_type: str = 'uncertain'
    fragments: List[PuzzleFragment] = field(default_factory=list)
    created_at: str = ''
    updated_at: str = ''
```

### Pattern 3: Background Removal Pipeline (Pillow + NumPy)
**What:** HSV color-based segmentation for solid-color backgrounds.
**When to use:** Processing every fragment image after loading.
**Algorithm:**
```python
# Source: Pillow docs (Image.convert('HSV'), putalpha()) + NumPy masking
from PIL import Image, ImageFilter
import numpy as np

def remove_background(image_bytes: bytes, threshold: float = 30.0) -> bytes:
    """Remove solid-color background, return RGBA PNG bytes."""
    # 1. Load image
    img = Image.open(io.BytesIO(image_bytes)).convert('RGB')

    # 2. Convert to HSV via Pillow (native support, H scaled 0-255)
    hsv_img = img.convert('HSV')
    hsv_array = np.array(hsv_img)  # shape: (H, W, 3), dtype uint8

    # 3. Sample corners to detect background color
    h, w = hsv_array.shape[:2]
    corners = [
        hsv_array[0:20, 0:20],        # top-left
        hsv_array[0:20, w-20:w],       # top-right
        hsv_array[h-20:h, 0:20],       # bottom-left
        hsv_array[h-20:h, w-20:w],     # bottom-right
    ]
    bg_hsv = np.median(np.concatenate([c.reshape(-1, 3) for c in corners], axis=0), axis=0)

    # 4. Create mask: pixels within threshold of bg color = background (0), else foreground (255)
    diff = np.sqrt(np.sum((hsv_array.astype(float) - bg_hsv) ** 2, axis=2))
    mask = np.where(diff > threshold, 255, 0).astype(np.uint8)

    # 5. Morphological cleanup (NumPy-only, no OpenCV)
    #    Simple approach: use Pillow's MinFilter/MaxFilter for erosion/dilation
    mask_img = Image.fromarray(mask, mode='L')
    mask_img = mask_img.filter(ImageFilter.MinFilter(3))  # erode
    mask_img = mask_img.filter(ImageFilter.MaxFilter(5))  # dilate

    # 6. Apply alpha channel
    rgba = img.convert('RGBA')
    rgba.putalpha(mask_img)

    # 7. Return PNG bytes
    buf = io.BytesIO()
    rgba.save(buf, format='PNG')
    return buf.getvalue()
```

### Pattern 4: Image Proxy (from web/api.py)
**What:** Server-side image fetch and processing, served via HTTP endpoint.
**When to use:** For the web app background removal endpoint.
**Example path:** `/api/puzzle_image/{fl_id}?threshold=30`
**Existing pattern:** `web/api.py` lines 129-172 show NLI image proxy -- same pattern with processing step added.

### Anti-Patterns to Avoid
- **Client-side background removal:** Do NOT run pixel processing in the browser. Server-side Python only.
- **Storing processed images in joins.db:** Store only metadata (IIIF URLs, threshold parameters). Re-process on load from cache or re-fetch.
- **OpenCV dependency:** User decision is Pillow + NumPy only. Do not add cv2.
- **Shared canvas abstraction:** This phase has no canvas, but the data model must NOT assume any canvas API.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| RGB to HSV conversion | Manual numpy color math | `Image.convert('HSV')` | Pillow's native HSV mode handles the conversion correctly with proper scaling (H: 0-255, S: 0-255, V: 0-255) |
| Alpha channel application | Manual RGBA array manipulation | `Image.putalpha(mask_image)` | Pillow handles mode conversion, channel alignment, and edge cases |
| Morphological erosion/dilation | Custom kernel convolution | `ImageFilter.MinFilter(3)` / `ImageFilter.MaxFilter(5)` | Pillow's built-in filters are equivalent to morphological operations for binary masks |
| UUID generation | Custom ID scheme | `uuid.uuid4()` | Standard, collision-resistant |
| JSON serialization of dataclasses | Custom serializer | `dataclasses.asdict()` + `json.dumps()` | Standard Python, handles nested dataclasses |
| SQLite schema management | Manual CREATE TABLE strings | Keep simple but use `CREATE TABLE IF NOT EXISTS` | joins.db is simple enough to not need migrations framework |

**Key insight:** Pillow's native HSV mode + putalpha() + ImageFilter morphological operations cover the entire background removal pipeline without OpenCV. This is the critical technical finding.

## Common Pitfalls

### Pitfall 1: Pillow HSV Scale is 0-255, Not 0-360/0-100
**What goes wrong:** Developer assumes HSV hue is 0-360 degrees and saturation/value are 0-100% (OpenCV convention). Pillow scales ALL HSV channels to 0-255.
**Why it happens:** Different libraries use different HSV scales. OpenCV uses H:0-180, S:0-255, V:0-255. Standard math uses H:0-360, S:0-1, V:0-1. Pillow uses H:0-255, S:0-255, V:0-255.
**How to avoid:** Always work in Pillow's 0-255 scale when using `Image.convert('HSV')`. Document the scale in code comments. The threshold parameter should be in the same scale (Euclidean distance in 0-255 space).
**Warning signs:** Background removal produces all-transparent or all-opaque results.

### Pitfall 2: Corner Sampling Fails on Images with No Margin
**What goes wrong:** Some IIIF images are tightly cropped with no background margin at corners. Corner sampling returns parchment color as "background," and the entire fragment becomes transparent.
**Why it happens:** Not all library scanners leave background margins. Some digitization workflows crop tightly to the fragment.
**How to avoid:** Sample all four corners independently. If corner colors diverge significantly (std deviation > threshold), fall back to the most common color. Add a safety check: if mask removes > 90% of pixels, skip removal and return original.
**Warning signs:** Fragment disappears entirely after removal.

### Pitfall 3: joins.db is Read-Write Unlike Other Sidecars
**What goes wrong:** Developer copies the nli_crossref_service.py pattern verbatim, including `?mode=ro` in the connection URI. Writes silently fail or raise OperationalError.
**Why it happens:** All existing sidecars (pgp.db, fjms_enrichment.db, nli_crossref.db) are read-only. joins.db is the first read-write sidecar.
**How to avoid:** Open WITHOUT `?mode=ro`. Enable WAL mode (`PRAGMA journal_mode=WAL`). Add explicit `conn.commit()` after writes. Handle `sqlite3.OperationalError: database is locked` with retry.
**Warning signs:** "database is locked" errors, data not persisting across restarts.

### Pitfall 4: Large PNG Output from Background Removal
**What goes wrong:** RGBA PNG of a 1200x1800 image with alpha channel is 3-8MB (vs ~200KB for the source JPEG). Serving these via HTTP or storing in cache consumes significant bandwidth/disk.
**Why it happens:** RGBA PNGs are uncompressed compared to JPEG. Alpha channel adds 33% more data. PNG compression is lossless.
**How to avoid:** Use maximum PNG compression (`optimize=True`). Consider serving the mask separately (small grayscale image) and compositing client-side. Cache processed images to disk with cleanup policy. For the visual preview tool, show at reduced resolution.
**Warning signs:** Slow image loading, disk cache growing rapidly.

### Pitfall 5: Thread Safety for Web Background Removal
**What goes wrong:** Multiple concurrent web users trigger background removal simultaneously. Pillow/NumPy operations consume CPU, blocking the NiceGUI event loop.
**Why it happens:** Background removal is CPU-bound (array operations), not I/O-bound.
**How to avoid:** Use `run.cpu_bound()` for web, QThread for desktop (existing `gui_threads.py` pattern). Show progress indicator during processing.
**Warning signs:** Web app becomes unresponsive when one user processes an image.

## Code Examples

### Complete Background Removal Module Structure
```python
# Source: Pillow docs (Image.convert('HSV'), putalpha()), NumPy array ops
# File: shared/background_removal.py

import io
import numpy as np
from PIL import Image, ImageFilter
from typing import Tuple, Optional

# Default threshold for HSV color distance (0-255 scale per channel)
DEFAULT_THRESHOLD = 30.0
CORNER_SAMPLE_SIZE = 20  # pixels from each corner
MIN_FOREGROUND_RATIO = 0.10  # safety: if < 10% foreground, skip removal


def detect_background_color(hsv_array: np.ndarray) -> np.ndarray:
    """Sample corners of HSV array to detect dominant background color."""
    h, w = hsv_array.shape[:2]
    s = CORNER_SAMPLE_SIZE
    corners = [
        hsv_array[:s, :s],         # top-left
        hsv_array[:s, w-s:],       # top-right
        hsv_array[h-s:, :s],       # bottom-left
        hsv_array[h-s:, w-s:],     # bottom-right
    ]
    all_pixels = np.concatenate([c.reshape(-1, 3) for c in corners], axis=0)
    return np.median(all_pixels, axis=0)


def create_mask(hsv_array: np.ndarray, bg_color: np.ndarray,
                threshold: float) -> Image.Image:
    """Create binary foreground mask from HSV array and background color."""
    diff = np.sqrt(np.sum((hsv_array.astype(float) - bg_color) ** 2, axis=2))
    mask_array = np.where(diff > threshold, 255, 0).astype(np.uint8)
    mask_img = Image.fromarray(mask_array, mode='L')
    # Morphological cleanup: erode then dilate
    mask_img = mask_img.filter(ImageFilter.MinFilter(3))
    mask_img = mask_img.filter(ImageFilter.MaxFilter(5))
    return mask_img


def remove_background(image_bytes: bytes,
                      threshold: float = DEFAULT_THRESHOLD) -> bytes:
    """Remove solid-color background from image bytes.

    Returns RGBA PNG bytes with transparent background.
    If removal would eliminate too much content, returns original as RGBA PNG.
    """
    img = Image.open(io.BytesIO(image_bytes)).convert('RGB')
    hsv_img = img.convert('HSV')
    hsv_array = np.array(hsv_img)

    bg_color = detect_background_color(hsv_array)
    mask = create_mask(hsv_array, bg_color, threshold)

    # Safety check: if mask is mostly transparent, skip removal
    mask_array = np.array(mask)
    foreground_ratio = np.count_nonzero(mask_array) / mask_array.size
    if foreground_ratio < MIN_FOREGROUND_RATIO:
        rgba = img.convert('RGBA')
    else:
        rgba = img.convert('RGBA')
        rgba.putalpha(mask)

    buf = io.BytesIO()
    rgba.save(buf, format='PNG', optimize=True)
    return buf.getvalue()
```

### PuzzleDocument Serialization Roundtrip
```python
# File: shared/puzzle_model.py
import json
import uuid
from dataclasses import dataclass, field, asdict
from typing import List, Optional
from datetime import datetime

@dataclass
class PuzzleFragment:
    sys_id: str
    folio_label: str
    fl_id: str
    x: float = 0.0
    y: float = 0.0
    rotation: float = 0.0
    scale: float = 1.0
    flip_h: bool = False
    flip_v: bool = False
    bg_removal_threshold: float = 30.0

@dataclass
class PuzzleDocument:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    title: str = ''
    notes: str = ''
    join_type: str = 'uncertain'  # physical, content, uncertain
    fragments: List[PuzzleFragment] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, json_str: str) -> 'PuzzleDocument':
        data = json.loads(json_str)
        fragments = [PuzzleFragment(**f) for f in data.pop('fragments', [])]
        return cls(fragments=fragments, **data)
```

### joins.db Schema
```sql
-- File: joins_data/joins.db (created by PuzzleService on first use)

-- Schema version tracking
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', '1');

-- Join documents (user puzzle arrangements)
CREATE TABLE IF NOT EXISTS join_documents (
    id TEXT PRIMARY KEY,                    -- UUID
    title TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    join_type TEXT NOT NULL DEFAULT 'uncertain'
        CHECK (join_type IN ('physical', 'content', 'uncertain')),
    fragments_json TEXT NOT NULL,            -- JSON array of PuzzleFragment dicts
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Index for listing/sorting
CREATE INDEX IF NOT EXISTS idx_join_documents_updated
    ON join_documents(updated_at DESC);
```

**Schema notes:**
- Fragments stored as JSON in `fragments_json` column (not a separate table) -- simpler for v1, each document is self-contained
- No composite image BLOBs in this phase (deferred to Phase 50: JDOC-04)
- No user_id column yet (deferred to Phase 52: community features)
- `join_type` CHECK constraint matches REQUIREMENTS.md vocabulary
- WAL mode enabled at connection time, not in schema

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| OpenCV for all image processing | Pillow + NumPy for solid-color removal | Project decision (2026-03-15) | No 50MB+ dependency; simpler code; sufficient for solid backgrounds |
| Storing processed images as BLOBs | Metadata only; re-process from cache/IIIF | Architecture decision | joins.db stays small; images cached on disk |
| Read-only SQLite sidecars | Read-write joins.db with WAL mode | First RW sidecar in project | New pattern; needs careful connection handling |

**Deprecated/outdated:**
- OpenCV-based background removal (ARCHITECTURE.md references `cv2` but user decision overrides to Pillow-only)
- ARCHITECTURE.md mentions `shared/background_removal.py` using "OpenCV-based color segmentation" -- this is SUPERSEDED by CONTEXT.md decision for Pillow + NumPy

## Open Questions

1. **Pillow HSV color distance effectiveness on dark backgrounds**
   - What we know: Pillow converts to HSV with all channels 0-255. Euclidean distance in this space works for distinct colors.
   - What's unclear: How well does Euclidean HSV distance separate dark parchment from dark backgrounds (e.g., Cambridge dark background + aged dark parchment)?
   - Recommendation: Build the preview tool first, test with real images from each library. Threshold slider allows per-image tuning. If HSV distance alone is insufficient for dark-on-dark, consider adding a separate value channel check.

2. **Optimal default threshold value**
   - What we know: Threshold is Euclidean distance in HSV 0-255 space. Range 0-442 (max distance across all 3 channels).
   - What's unclear: What value works well as default across NLI/Cambridge/Oxford/Manchester?
   - Recommendation: Start with 30.0, tune empirically with the preview tool. Store per-fragment so user adjustments persist.

3. **Image cache directory location**
   - What we know: Desktop uses LOCALAPPDATA for sidecars. Web serves images via HTTP endpoints.
   - What's unclear: Where to cache processed RGBA PNGs for fast reload.
   - Recommendation: `{LOCALAPPDATA}/GenizahSearchPro/cache/puzzle/` for desktop. Web caches in memory (dict with TTL) or temp directory.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.0.2 |
| Config file | None (uses conftest.py for path setup) |
| Quick run command | `pytest tests/test_background_removal.py tests/test_puzzle_model.py tests/test_puzzle_service.py -x` |
| Full suite command | `pytest tests/ -x` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| BGRM-01 | Background removal produces RGBA PNG with transparent background from solid-color input | unit | `pytest tests/test_background_removal.py::test_solid_blue_background_removed -x` | -- Wave 0 |
| BGRM-01 | Safety check: over-aggressive removal returns original | unit | `pytest tests/test_background_removal.py::test_safety_check_preserves_content -x` | -- Wave 0 |
| BGRM-02 | Original image bytes preserved alongside processed output | unit | `pytest tests/test_background_removal.py::test_original_preserved -x` | -- Wave 0 |
| BGRM-03 | Threshold parameter changes mask aggressiveness | unit | `pytest tests/test_background_removal.py::test_threshold_affects_mask -x` | -- Wave 0 |
| N/A | PuzzleDocument/PuzzleFragment JSON roundtrip | unit | `pytest tests/test_puzzle_model.py::test_roundtrip_serialization -x` | -- Wave 0 |
| N/A | PuzzleFragment stores all required fields | unit | `pytest tests/test_puzzle_model.py::test_fragment_fields -x` | -- Wave 0 |
| N/A | joins.db CRUD operations | unit | `pytest tests/test_puzzle_service.py::test_create_and_load_document -x` | -- Wave 0 |
| N/A | joins.db schema creation on first use | unit | `pytest tests/test_puzzle_service.py::test_schema_creation -x` | -- Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_background_removal.py tests/test_puzzle_model.py tests/test_puzzle_service.py -x`
- **Per wave merge:** `pytest tests/ -x`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_background_removal.py` -- covers BGRM-01, BGRM-02, BGRM-03
- [ ] `tests/test_puzzle_model.py` -- covers data model roundtrip
- [ ] `tests/test_puzzle_service.py` -- covers joins.db CRUD
- [ ] `Pillow` added to requirements.txt -- required before any test runs
- [ ] Synthetic test images (solid blue/green/gray backgrounds with simple foreground shapes) created in test fixtures using Pillow

## Sources

### Primary (HIGH confidence)
- [Pillow Image Modes documentation](https://pillow.readthedocs.io/en/stable/handbook/concepts.html) -- confirmed HSV mode support with 0-255 scaling
- [Pillow Image.putalpha() documentation](https://pillow.readthedocs.io/en/stable/reference/Image.html) -- alpha channel from L-mode mask image
- [Pillow ImageFilter documentation](https://pillow.readthedocs.io/en/stable/reference/ImageFilter.html) -- MinFilter/MaxFilter for morphological ops
- [putalpha() tutorial (note.nkmk.me)](https://note.nkmk.me/en/python-pillow-putalpha/) -- verified workflow: RGB -> putalpha(L-mode mask) -> save PNG
- Existing codebase: `shared/nli_crossref_service.py` -- singleton service, graceful degradation, _find_project_root() pattern
- Existing codebase: `shared/reading_desk_model.py` -- dataclass pattern for multi-fragment state
- Existing codebase: `web/api.py` lines 129-172 -- IIIF image proxy pattern
- Existing codebase: `web/services.py` lines 124-141 -- IIIF URL builders (get_thumbnail_url, build_iiif_image_url)

### Secondary (MEDIUM confidence)
- [NumPy RGB to HSV gist](https://gist.github.com/PolarNick239/691387158ff1c41ad73c) -- pure NumPy HSV conversion (fallback if Pillow HSV has issues)
- [Pillow Issue #624: HSV mode](https://github.com/python-pillow/Pillow/issues/624) -- confirmed HSV is supported but with limited conversion paths
- `.planning/research/STACK.md` -- pre-research stack recommendations (HIGH, but some OpenCV references superseded by CONTEXT.md)
- `.planning/research/ARCHITECTURE.md` -- component boundaries and data flow (HIGH for patterns, MEDIUM for OpenCV references)
- `.planning/research/PITFALLS.md` -- domain pitfalls (HIGH)

### Tertiary (LOW confidence)
- Pillow HSV color distance effectiveness on manuscript images -- needs empirical testing (no source, domain-specific)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- Pillow HSV + putalpha verified in official docs; NumPy already installed
- Architecture: HIGH -- follows established sidecar/service patterns exactly
- Background removal algorithm: MEDIUM -- HSV approach is sound for solid colors but needs empirical tuning with real Genizah images
- Pitfalls: HIGH -- well-documented from prior research + codebase analysis

**Research date:** 2026-03-15
**Valid until:** 2026-04-15 (stable libraries, no fast-moving dependencies)
