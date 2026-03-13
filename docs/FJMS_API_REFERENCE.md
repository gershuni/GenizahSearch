# FJMS Website API Reference — Programmatic Access Guide

> Last updated: 2026-03-12
> Status: Working reference for all verified access methods

## 1. Overview

The Friedberg Jewish Manuscript Society (FJMS) runs two main web properties:

| Domain | Purpose | Tech |
|--------|---------|------|
| `fjms.genizah.org` | Portal / landing page | Static JS SPA |
| `fgp.genizah.org` | FGP application (search, browse, catalog, transcriptions) | ASP.NET WebForms + Telerik |

All programmatic access goes through `fgp.genizah.org`. The site uses:
- **ASP.NET WebForms** with `__VIEWSTATE` PostBack model
- **Telerik RadControls** for toolbars and combos
- **WCF JSON service** at `/WCFServices/AjaxWcfHelper.svc/<Method>`
- **Nested iframes** — most content loads inside `FgpFrames.aspx` parent
- **Server-side session state** — many features require session to be initialized by the parent frame

**Key constraint:** Pages accessed outside the iframe hierarchy often return empty or error results because the server-side session wasn't initialized by the parent frame's JavaScript. WCF methods and direct page access with proper cookies work best.

## 2. Authentication

### SSO Login (2-step)

**Step 1 — Get UIT token:**
```
GET https://SSO.genizah.org/login/GetLoginUIT
    ?username=Miriamg&password=Fgp123&screenWidth=1920&callback=cb
→ Returns JSONP: cb({"d": {"UIT": "94d0161c-21be-438d-a...", ...}})
```

**Step 2 — Exchange UIT for session cookie:**
```
GET https://fgp.genizah.org/FgpFrames.aspx
    ?lang=eng&UIT=<token>&mainSiteType=GenizahSite
→ Sets .ASPXAUTH cookie (session authentication)
```

### Python Implementation

```python
import requests, re

def login_fjms():
    """Login to FJMS and return authenticated session."""
    s = requests.Session()
    s.headers.update({
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })

    # Step 1: Get UIT token
    resp = s.get('https://SSO.genizah.org/login/GetLoginUIT', params={
        'username': 'Miriamg',
        'password': 'Fgp123',
        'screenWidth': '1920',
        'callback': 'cb',
    })
    m = re.search(r'"UIT":"([^"]+)"', resp.text)
    if not m:
        raise RuntimeError(f"Login failed: {resp.text[:200]}")
    uit = m.group(1)

    # Step 2: Exchange for .ASPXAUTH cookie
    s.get('https://fgp.genizah.org/FgpFrames.aspx', params={
        'lang': 'eng',
        'UIT': uit,
        'mainSiteType': 'GenizahSite',
    })

    # Required cookies for frame context
    s.cookies.set('frame', '1', domain='fgp.genizah.org')
    s.cookies.set('selLang', 'eng', domain='fgp.genizah.org')

    return s
```

**Credentials:** `Miriamg` / `Fgp123` (public demo account)

**Verification:** Check `.ASPXAUTH` in cookies:
```python
assert '.ASPXAUTH' in str(s.cookies)
```

## 3. WCF Service API

**Base URL:** `https://fgp.genizah.org/WCFServices/AjaxWcfHelper.svc/<MethodName>`

**Protocol:** HTTP POST with JSON body, returns JSON.

**Headers required:**
```python
headers = {'Content-Type': 'application/json'}
```

### 3.1 Shelfmark Lookup

#### `MobileGetInventoriesByTextualSM`

Maps a textual shelfmark to InventoryId(s).

```python
resp = s.post(
    'https://fgp.genizah.org/WCFServices/AjaxWcfHelper.svc/MobileGetInventoriesByTextualSM',
    json={'textualShelfmark': 'Add.3207'},
    headers={'Content-Type': 'application/json'}
)
# Returns:
# {"d": [{"key": "CUL: Add.3207", "value": 136719109}]}
```

- Input: partial or full shelfmark text
- Returns: array of `{key: "Library: Shelfmark", value: InventoryId}`
- InventoryId is shared with FIST.db (`dbo_Inventory.InventoryId`)

#### `GetShelfmarksByInventory`

Get display shelfmark for an InventoryId.

```python
resp = s.post(
    '.../GetShelfmarksByInventory',
    json={'inventory': '136719109'},
    headers={'Content-Type': 'application/json'}
)
# Returns: {"d": "Cambridge, CUL: ADD.3207"}
```

#### `GetTextualShelfmarkByInventory`

Get bilingual shelfmark (English:Hebrew).

```python
resp = s.post(
    '.../GetTextualShelfmarkByInventory',
    json={'InventoryId': 136719109},
    headers={'Content-Type': 'application/json'}
)
# Returns: {"d": "Cambridge, CUL: Add.3207:קיימברידג'..."}
```

### 3.2 Image / FGP Numbers

#### `MobileGetFgpsByInventoryId`

Get FGP photo numbers for an inventory.

```python
resp = s.post(
    '.../MobileGetFgpsByInventoryId',
    json={'inventoryId': 136719109},
    headers={'Content-Type': 'application/json'}
)
# Returns: {"d": "C362947;C362948;C362949;C362950"}
```

#### `GetFGPNumberGuid`

Get image GUID for an FGP number (for image viewing, NOT transcriptions).

```python
resp = s.post(
    '.../GetFGPNumberGuid',
    json={'fgp': 'C362949', 'mainFgp': 'C362949', 'IsPuzzleAdditionalImage': False},
    headers={'Content-Type': 'application/json'}
)
# Returns: {"d": "<GUID>"}
# WARNING: This GUID is for IMAGES, not transcriptions.
```

### 3.3 Session Management

#### `SetSession`

Set a named session variable.

```python
resp = s.post(
    '.../SetSession',
    json={'sessionValue': '136719109', 'currFrame': 1, 'sessionName': 'selectedInventory'},
    headers={'Content-Type': 'application/json'}
)
# Returns: {"d": null}
```

#### `SetAllSelectedShelfmarks`

Set selected shelfmarks in session.

```python
resp = s.post(
    '.../SetAllSelectedShelfmarks',
    json={'selectedShelfmarks': '136719109', 'selectedFgpNumbers': ''},
    headers={'Content-Type': 'application/json'}
)
# Returns: {"d": true}
```

#### `SaveTranscriptionStatusInSession`

Set transcription file index in session (for BtnNext/BtnPrev navigation).

```python
resp = s.post(
    '.../SaveTranscriptionStatusInSession',
    json={'value': 1, 'currFrame': 1},
    headers={'Content-Type': 'application/json'}
)
```

### 3.4 Shelfmark Search (GetShelfmarks)

#### `GetShelfmarks`

Paginated shelfmark search. Used by the Advanced Search UI.

```python
resp = s.post(
    '.../GetShelfmarks',
    json={
        'searchParams': '<format TBD>',
        'firstRec': 1,
        'lastRec': 50,
        'currFrame': 1,
        'SrcPg': 'BooleanSearch'  # or 'AdvancedSearch'
    },
    headers={'Content-Type': 'application/json'}
)
```

**Note:** The `searchParams` format is not fully reverse-engineered. It's set by the BooleanSearch page after form PostBack. The `ShowShelfmarkList()` JS function calls this method. See Section 6 for the BooleanSearch workflow.

#### `GetShelfmarksByPageIndex`

Get next page of results from a previous search.

```python
resp = s.post(
    '.../GetShelfmarksByPageIndex',
    json={'currentPageIndex': 2, 'currFrame': 1, 'SrcPg': 'BooleanSearch'},
    headers={'Content-Type': 'application/json'}
)
```

### 3.5 Complete WCF Method List

All 151+ methods are documented in `scripts/wcf_proxy.js` (86K file, auto-generated from the service). Key methods:

| Method | Parameters | Purpose |
|--------|-----------|---------|
| `MobileGetInventoriesByTextualSM` | `textualShelfmark` | Shelfmark → InventoryId |
| `MobileGetFgpsByInventoryId` | `inventoryId` | InventoryId → FGP numbers |
| `GetShelfmarksByInventory` | `inventory` | InventoryId → display shelfmark |
| `GetTextualShelfmarkByInventory` | `InventoryId` | InventoryId → bilingual shelfmark |
| `GetFGPNumberGuid` | `fgp, mainFgp, IsPuzzleAdditionalImage` | FGP → image GUID |
| `GetShelfmarks` | `searchParams, firstRec, lastRec, currFrame, SrcPg` | Paginated search |
| `GetShelfmarksByPageIndex` | `currentPageIndex, currFrame, SrcPg` | Next page |
| `GetShelfmarksByCollection` | `siteType, collectionId, currFrame, ...` | Browse by collection |
| `GetShelfmarksByVolume` | `siteType, volumeId, ...` | Browse by volume |
| `GetShelfmarksDetails` | `siteType, paramId, isVolume, frame, ...` | Detailed results |
| `SetSession` | `sessionValue, currFrame, sessionName` | Set session var |
| `SetAllSelectedShelfmarks` | `selectedShelfmarks, selectedFgpNumbers` | Set selection |
| `SaveTranscriptionStatusInSession` | `value, currFrame` | Set transcription index |
| `FillSessionWithCatRec` | `searchParams, currFrame` | Load catalog record |
| `CloneSessions` | `SourceFrameId, DestFrameId` | Copy session between frames |
| `OpenImageTrancriptionFile` | `fullpath, step` | Direct file access (broken) |

## 4. ASP.NET Pages (Direct Access)

### 4.1 Page URL Patterns

```
Parent Frame:    https://fgp.genizah.org/FgpFrames.aspx
Shelfmark Browse: https://fgp.genizah.org/SelectionPages/TextualSmPages/TextualSelection.aspx?textualSM=<shelfmark>
Transcription:   https://fgp.genizah.org/FunctionPages/Transcription/TranscriptionResults.aspx?signatureId=<neg_id>;<inv_id>&code=<code>&SigId=<sig_id>
PDF Download:    https://fgp.genizah.org/FunctionPages/filebyid.aspx?id=<GUID>&funcType=Transcription
Catalog Record:  https://fgp.genizah.org/FunctionPages/CatalogRec/CatalogRecResults.aspx
Advanced Search: https://fgp.genizah.org/SelectionPages/SearchPages/BooleanSearch/BooleanSearch.aspx?SearchParam=<N>
KWICView:        https://fgp.genizah.org/SelectionPages/KWICView/KWICViewFrames.aspx
```

### 4.2 BooleanSearch.aspx — Advanced Search (IMPORTANT)

This is the main Advanced Search page. It contains all source attribution data.

**URL:** `https://fgp.genizah.org/SelectionPages/SearchPages/BooleanSearch/BooleanSearch.aspx?SearchParam=15`

**SearchParam values** (from the KWICView dropdown):

| Value | Parameter |
|-------|-----------|
| 1 | Collection |
| 2 | Domain and Title |
| 3 | Titles (Alphabetical) |
| 4 | Author |
| 5 | Language and Script |
| 6 | Script Style |
| 7 | Vocalization |
| 8 | Material |
| 9 | No. of Columns |
| 10 | Range of Lines |
| 11 | Attributes |
| 12 | Period of Copy |
| 13 | Additional Conditions |
| 14 | Physical Status |
| **15** | **Information Source** |
| 18 | Bibliographical References |
| 22 | Computer generated data |

**SearchParam=15 (Information Source)** loads a page with 8 source panels:

| Panel | JS trigger | Content |
|-------|------------|---------|
| Panel 1 | `visibleSource('1')` | **Teams** (29 entries) — FGP research teams |
| Panel 2 | `visibleSource('2')` | **Computerized Catalogs** (9 entries) |
| Panel 3 | `visibleSource('3')` | **Printed Catalogs** (11 entries) |
| Panel 4 | `visibleSource('4')` | **Books** (22 entries) |
| Panel 5 | `visibleSource('5')` | **Site Users** (221 entries) |
| Panel 6 | `visibleSource('6')` | **Handlist** (6 entries) |
| Panel 7 | `visibleSource('7')` | **Institutions** (45 entries) |
| Panel 8 | `visibleSource('8')` | **Articles** (8 entries) |

**Total: 471 source entries across all panels.**

### 4.3 Extracting Source Data from BooleanSearch

Each source entry is a checkbox with an `alt` attribute containing **website-internal IDs**:

```html
<span alt="398">
  <input id="...SourceListCB5_ListSource_95" type="checkbox" ... />
  <label ...>Site User - Joseph Yahalom</label>
</span>
```

**Extraction code:**

```python
def extract_source_panel(session, panel_num):
    """
    Extract all source entries from a BooleanSearch panel.

    Panel numbers: 1=Teams, 2=Catalogs, 3=Printed, 4=Books,
                   5=SiteUsers, 6=Handlist, 7=Institutions, 8=Articles

    Returns: list of {web_ids: str, checkbox_id: str, name: str}
    """
    resp = session.get(
        'https://fgp.genizah.org/SelectionPages/SearchPages/BooleanSearch/'
        'BooleanSearch.aspx?SearchParam=15'
    )
    html = resp.text

    entries = re.findall(
        rf'<span alt="([^"]+)">'
        rf'<input id="([^"]*SourceListCB{panel_num}[^"]+)"[^/]*/>'
        rf'<label[^>]*>([^<]+)</label>',
        html,
        re.DOTALL
    )

    return [
        {'web_ids': alt, 'checkbox_id': cb_id, 'name': name.strip()}
        for alt, cb_id, name in entries
    ]


# Example: extract all site users
s = login_fjms()
site_users = extract_source_panel(s, panel_num=5)
# Returns 221 entries like:
# {'web_ids': '398', 'checkbox_id': '...ListSource_95', 'name': 'Site User - Joseph Yahalom'}
```

**Important:** The `web_ids` (alt attribute) are **NOT** the same as FIST.db `dbo_Signature.SubId` values. They are an FJMS website-internal ID system. Example:

| User | Website alt | FIST.db SubId |
|------|-------------|---------------|
| Gregor Schwarb | 334 | 23 |
| Eliezer Cheshin | 419 | 153 |
| Joseph Yahalom | 398 | ? (unmatched) |

Some entries have **multiple IDs** separated by semicolons (e.g., `alt="507;415"` for Tova Beeri, or `alt="105;107"` for a team).

### 4.4 Full Panel Examples

**Panel 1 (Teams) — sample entries:**
```
alt=76          FGP Aggadic Midrashim team, Chaim Milikowsky (head)
alt=104         FGP Bibliography A team, Tamar Leiter (head)
alt=105;107     FGP Bibliography Cambridge team
```

**Panel 5 (Site Users) — sample entries:**
```
alt=398         Site User - Joseph Yahalom
alt=369         Site User - Shulamit Elizur
alt=324         Site User - Mordechai Weintraub
alt=359         Site User - adiel breuer
alt=2           Site User - Roni Shweka
alt=361         Site User - Roni Shweka       (second account)
alt=1           Site User - Avi Shmidman
alt=317         Site User - Avi Shmidman      (second account)
```

**Panel 6 (Handlist) — sample entries:**
```
alt=134         Ben Sasson Menahem, Personal Handlist
alt=149         Ben-Shammai Haggai, Personal Handlist
alt=135;136     Chaim Milikowsky, Personal Handlist
```

## 5. Transcription Access

### 5.1 PDF Download (Once GUID Is Known)

```python
resp = s.get(
    'https://fgp.genizah.org/FunctionPages/filebyid.aspx',
    params={'id': '<GUID>', 'funcType': 'Transcription'}
)
# Returns PDF bytes (Content-Disposition: attachment;filename=Transcription.pdf)

import fitz  # PyMuPDF
doc = fitz.open(stream=resp.content, filetype='pdf')
text = doc[0].get_text()  # Clean Unicode Hebrew text
```

### 5.2 Navigating Transcription Files (BtnNext/BtnPrev)

Once a shelfmark is loaded in TranscriptionResults.aspx, navigate between files:

```python
def get_next_transcription(session, current_html):
    """Navigate to next transcription file via PostBack."""
    fields = extract_hidden_fields(current_html)  # __VIEWSTATE, etc.
    fields['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$BtnNext'
    fields['__EVENTARGUMENT'] = ''

    resp = session.post(
        'https://fgp.genizah.org/FunctionPages/Transcription/TranscriptionResults.aspx',
        data=fields
    )
    return resp.text
```

**Metadata available on each transcription page:**
- `File no. X of Y` — position and total count
- `LblOriginalShelfmarkContent` — shelfmark display name
- `LblSourceContent` — transcription author/edition
- `LblFileType` — file type description
- `LblFocusContent` — folio/page focus
- `lblCopyRights` — copyright notice
- `HiddenCurrentSigId` — FIST.db SignatureId
- `HiddenCurrentInventoryId` — InventoryId
- `filebyid.aspx?id=<GUID>` — PDF download GUID in iframe

### 5.3 The Navigation Problem

**TranscriptionResults.aspx requires server-side session state** set by the parent frame's JavaScript (`FunctionToolBarClientClick` in Telerik's RadToolBar). Direct URL access with any parameter combination returns empty results on fresh sessions.

**What works:** BtnNext/BtnPrev PostBack navigation AFTER a session is already loaded.
**What doesn't work:** Navigating to a NEW shelfmark programmatically without browser automation.

See `docs/FJMS_TRANSCRIPTION_SCRAPING.md` for full analysis and workaround approaches.

## 6. FIST.db Local Reference

FIST.db is a local SQLite copy of the FJMS database. It provides offline access to much of the same data.

### 6.1 Key Tables

| Table | Rows | Purpose |
|-------|------|---------|
| `dbo_Signature` | 1.56M | All signatures (source attributions) |
| `dbo_Inventory` | ~200K | Shelfmark inventory |
| `dbo_InventorySignature` | ~200K | Signature → Inventory mapping |
| `dbo_UnitCatalogRec` | ~411K | Catalog records |
| `dbo_UnitTranscription` | 56K | Transcription file references |
| `dbo_UnitFullText` | 65K | Full text records |
| `dbo_UnitBibliographyReference` | 733K | Bibliography references |
| `dbo_UnitFreeDescription` | 190K | Free-form descriptions |
| `CODE_Source` (dbo_CodeSource) | ~150 | Source/team definitions |
| `CODE_Author` | ~300 | Author definitions |
| `CODE_Domain` | ~200 | Domain classifications |
| `CODE_Library` | ~50 | Library definitions |
| `CODE_Collection` | ~200 | Collection definitions |

### 6.2 Source Attribution (SourceId/SubId System)

Every `dbo_Signature` row has `SourceId` and `SubId`:

| SourceId | Meaning | SubId maps to |
|----------|---------|--------------|
| 100 | Teams | `dbo_CodeSource.TeamCode` |
| 200 | Transcriptions | Internal file reference |
| 300 | Handlists | `CODE_Author.AuthorId` (STALE names!) |
| 400 | Institutions | `CODE_Institution.InstitutionId` |
| 500 | Catalogs | `CODE_Catalog.CatalogId` |
| 600 | Books/Titles | `CODE_Title.TitleId` |
| 850 | **Site Users** | Website user ID (NO name table in FIST.db) |

**Critical:** SourceId=850 SubIds are **NOT** the same as the BooleanSearch `alt` attribute values. They are a separate numbering system. Bridging requires matching shelfmarks between the two systems.

### 6.3 Join Paths

**Shelfmark → Catalog records:**
```sql
SELECT i.Shelfmark, ucr.*
FROM dbo_UnitCatalogRec ucr
JOIN dbo_Signature s ON s.SignatureId = ucr.SignatureId
JOIN dbo_InventorySignature isig ON isig.SetSignatureId = s.SetSignatureId
JOIN dbo_Inventory i ON i.InventoryId = isig.InventoryId
WHERE s.SourceId = 850 AND s.SubId = ?
```

**Shelfmark → Transcriptions:**
```sql
SELECT i.Shelfmark, ut.FileName, ut.FolderPath, ut.Focus
FROM dbo_UnitTranscription ut
JOIN dbo_Signature s ON ut.SignatureId = s.SignatureId
JOIN dbo_InventorySignature isig ON s.SetSignatureId = isig.SetSignatureId
JOIN dbo_Inventory i ON isig.InventoryId = i.InventoryId
WHERE s.SourceId = 200
```

**InventoryId → AlmaId (for enrichment DB):**
```sql
SELECT AlmaId, InventoryId, SiteId
FROM dbo_InventoryAlma
WHERE InventoryId = ?
```

### 6.4 FIST FileName Pattern

```
20000040017621_362949_Tc
│         │      │     └─ Tc = Transcription
│         │      └─ FGP image number (matches web C362949)
│         └─ Internal sequence
└─ Source/folder prefix
```

## 7. ID Cross-Reference

| ID | System | Shared? | Example |
|----|--------|---------|---------|
| InventoryId | FIST.db + Website | ✅ YES | `136719109` |
| SignatureId (FIST) | FIST.db + Website SigId param | ✅ YES | `1762240` |
| SetSignatureId | FIST.db only | ❌ | Internal grouping |
| FGP Number | FIST.db FileName + Website | ✅ YES | `C362949` |
| AlmaId | Enrichment DB + NLI | ✅ YES | `990025085490205171` |
| `signatureId` URL param | Website only | ❌ | `-1428201;136719109` |
| `code` URL param | Website only | ❌ | `1313` |
| BooleanSearch `alt` | Website only | ❌ | `398` (Yahalom) |
| SourceId=850 SubId | FIST.db only | ❌ | `23` (Schwarb) |

**InventoryId is the universal bridge** — it's shared between FIST.db and the website, and can be mapped to AlmaId via `dbo_InventoryAlma`.

## 8. Known Limitations

1. **No direct shelfmark→transcription navigation** without browser automation (see Section 5.3)
2. **BooleanSearch alt IDs ≠ FIST SubIds** — two separate numbering systems for site users
3. **ASP.NET ViewState** — PostBack forms require ~200KB `__VIEWSTATE` per request
4. **Session stickiness** — once a shelfmark is loaded in session, changing to another requires proper frame navigation
5. **`OpenImageTrancriptionFile` WCF method** — always returns "Index was out of range" (unknown file path format)
6. **Rate limiting** — Dicta-style throttling not observed, but be respectful (1-2 req/sec recommended)
7. **Hebrew encoding** — some pages return mojibake for Hebrew in `CODE_Library`, `CODE_Author` etc. Use `utf-8` decoding.

## 9. Files Reference

| File | Purpose |
|------|---------|
| `scripts/wcf_proxy.js` | Full WCF JS proxy (86K, all 151+ methods) |
| `scripts/fjms_site_user_web_ids.json` | 221 site users with BooleanSearch alt IDs |
| `scripts/map_site_user_subids.py` | FIST SubId → user name matching script |
| `scripts/explore_fjms_transcriptions.py` | API exploration |
| `scripts/scrape_transcription_guids.py` | GUID scraper via WCF + PostBack |
| `scripts/fix_handlist_source_names.py` | Handlist SourceName fix script |
| `docs/FJMS_TRANSCRIPTION_SCRAPING.md` | Transcription scraping research |
| `fist_data/FIST.db` | Local FIST database (1.56M signatures) |
| `fist_data/fjms_enrichment.db` | Enrichment sidecar (941MB) |
