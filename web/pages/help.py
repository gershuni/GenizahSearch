# -*- coding: utf-8 -*-
"""
Help Center - Dicta Genizah Search

Comprehensive bilingual (English/Hebrew) documentation and tutorials for researchers.
Based on the desktop Help.html but adapted for the web application.
"""

from nicegui import ui
from web.translations import get_language
from web.components.typography import h1, h2, h3


def create_help_page():
    """Create the comprehensive Help Center page with bilingual content."""

    lang = get_language()
    is_hebrew = lang == 'he'

    # Language toggle state
    show_hebrew = {'value': is_hebrew}

    with ui.column().classes('w-full max-w-4xl mx-auto gap-6 fade-in p-4'):

        # === Language Toggle ===
        with ui.row().classes('w-full items-center justify-between mb-4'):
            h1(
                'מרכז עזרה' if show_hebrew['value'] else 'Help Center',
                classes='text-3xl font-bold',
                style='color: var(--text-primary);'
            )

            def toggle_language():
                show_hebrew['value'] = not show_hebrew['value']
                ui.navigate.reload()  # Reload to switch content

            ui.button(
                'English' if show_hebrew['value'] else 'עברית',
                icon='translate',
                on_click=toggle_language
            ).props('flat dense')

        # === Content Container ===
        content_container = ui.column().classes('w-full gap-6')

        with content_container:
            if show_hebrew['value']:
                _create_hebrew_content()
            else:
                _create_english_content()


def _create_english_content():
    """Create the English help content."""

    # === Table of Contents ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('list').classes('text-2xl text-primary')
            h2('Table of Contents', classes='text-xl font-bold', style='color: var(--text-primary);')

        with ui.column().classes('gap-2'):
            toc_items = [
                ('intro', 'Introduction: How it Works'),
                ('search', 'Search'),
                ('responsa', 'Responsa-Style Search'),
                ('parallels', 'Parallels Search'),
                ('browse', 'Browse Manuscript'),
                ('lists', 'Lists'),
                ('export', 'Exporting Data'),
            ]
            for anchor, title in toc_items:
                ui.link(f'• {title}', f'#help-{anchor}').classes('text-primary hover:underline')

    # === Introduction ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-intro"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('info').classes('text-2xl text-primary')
            h2('Introduction: How it Works', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**Dicta Genizah Search** provides fast and advanced access to the transcription corpus of the "MiDRASH" project.
The platform is based on a high-speed search engine (Tantivy) and integrates unique algorithms to handle
some of the reading errors from the MiDRASH project's decoding algorithm.

**Citation Requirement:** MiDRASH transcriptions are released under CC-BY-4.0 license, meaning they can be used with proper attribution. If you use the transcriptions, please credit:

> Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments [Data set]. Zenodo.
> https://doi.org/10.5281/zenodo.17734473

The application fetches metadata and images from:
- **National Library of Israel (NLI)**
- **Bodleian Library** at Oxford
- **Cambridge University Library**

*Note:* Some library servers may block access from non-home networks (e.g., mobile hotspots).
        ''').style('color: var(--text-secondary);')

    # === Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-search"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('search').classes('text-2xl text-primary')
            h2('Search', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.label('This is the entry point for free text or metadata searches within the corpus. You can use the shortcuts shown in parentheses below to go directly to your desired search type.').style('color: var(--text-secondary);').classes('mb-4')

        h3('Search Modes', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')

        modes_data = [
            ('Exact (=)', 'Matches only the exact word or sequence of words as typed. To search with gaps between words, fill in the "Gap" field with the desired number.'),
            ('Variants (?)', 'Accounts for common letter substitutions in these texts (e.g., Dalet/Resh ד/ר, He/Het ה/ח, Vav/Yod ו/י).'),
            ('Variants Extended (??)', 'More flexibility in letter substitutions (e.g., Qof/Kaf ק/כ, Tet/Tav ט/ת). Slower and may return less relevant results.'),
            ('Variants Maximum (???)', 'Maximum flexibility. Especially slow. Use when other modes return too few results.'),
            ('Fuzzy (~)', 'Uses [Levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance) to find similar words even with decoding errors.'),
            ('Regex (/)', 'Advanced search for experienced users. Example: \\bא[א-ת]{3}\\b finds 4-letter words starting with Aleph. You can use your preferred AI engine to help build a regex pattern suited to your needs.'),
            ('Title ($)', 'Searches within the catalog titles of compositions.'),
            ('Shelfmark (#)', 'Fast search for shelfmarks (e.g., "T-S NS 13.15").'),
            ('Responsa (R)', 'Search syntax inspired by the Bar-Ilan Responsa Project, with prefix/suffix expansion, wildcards, spelling variants, and proximity gaps. Familiar to Responsa Project users; easy to learn for newcomers. See [Responsa-Style Search](#help-responsa) below.'),
        ]

        with ui.column().classes('gap-3 mb-4'):
            for mode, desc in modes_data:
                with ui.row().classes('gap-2'):
                    ui.label(f'• {mode}:').classes('font-bold min-w-40').style('color: var(--primary-700);')
                    ui.markdown(desc).style('color: var(--text-secondary);')

        h3('Variant Level Control', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
When using **Variants** mode, you can control the flexibility level:

- **Num Changes (×1, ×2, ×3):** Maximum number of character substitutions allowed per word.
  - ×1 = Very strict, fewer false positives
  - ×2 = Balanced (recommended)
  - ×3 = Lenient, may find more obscure matches
        ''').style('color: var(--text-secondary);')

    # === Responsa-Style Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-responsa"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('tune').classes('text-2xl text-primary')
            h2('Responsa-Style Search', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This mode uses a query syntax inspired by the **Bar-Ilan Responsa Project** (פרויקט השו"ת) — the widely-used
database for searching Hebrew and Aramaic texts. If you are already familiar with the Responsa Project's search
conventions, you will feel right at home. If not, the syntax is straightforward to learn and offers powerful
tools for Hebrew and Judeo-Arabic manuscript searching: prefix/suffix expansion, wildcards, spelling variants,
and proximity gaps.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Activating Responsa Mode', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')
        ui.markdown('''
- Select **Responsa (R)** from the search Mode dropdown, or
- Type `R ` (R followed by a space) at the beginning of your query

When Responsa mode is active, the standard prefix shortcuts (=, ?, ~, /, $, #) are disabled — the query uses Responsa syntax instead.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Syntax Reference', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')

        syntax_data = [
            ('#word', 'Prefix expansion — adds 24 Hebrew prefix forms (ו, ה, ב, כ, ל, מ, ש + compounds)', '#שלום finds ושלום, השלום, בשלום, etc.'),
            ('word#', 'Suffix expansion — adds 25 Hebrew suffix forms (י, ו, ם, ן, ה, ך, כם, כן, etc.)', 'שלום# finds שלומם, שלומו, שלומך, etc.'),
            ('#word#', 'Both prefix and suffix expansion', '#שלום# finds all prefix+suffix combinations'),
            ('*word', 'Wildcard prefix — any characters before the word', '*שלום matches כבשלום, etc.'),
            ('word*', 'Wildcard suffix — any characters after the word', 'שלום* matches שלומות, etc.'),
            ('%word', 'Plene/defective variants — inserts/removes ו/י for spelling variants', '%שלום finds שלום, שלם'),
            ('(a/b)', 'OR alternatives — matches any of the listed options', '(שלום/שלומות) matches either'),
            ('[N]', 'Gap of N words between terms', 'שלום [3] עולם = up to 3 words between'),
        ]

        with ui.column().classes('gap-3 mb-4'):
            for syntax, meaning, example in syntax_data:
                with ui.column().classes('gap-1 p-3 rounded').style('background: var(--bg-secondary);'):
                    with ui.row().classes('gap-2 items-center'):
                        ui.label(syntax).classes('font-bold font-mono').style('color: var(--primary-700); min-width: 80px;')
                        ui.label(meaning).style('color: var(--text-secondary);')
                    ui.label(f'Example: {example}').classes('text-sm ml-4').style('color: var(--text-tertiary, #888);')

        ui.markdown('**Tip:** Modifiers can be combined, e.g. `#%word*` applies prefix expansion + plene variants + wildcard suffix.').style('color: var(--text-secondary);').classes('mb-4')

        h3('Sub-Options', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
When Responsa mode is active, four checkboxes appear below the search field:

- **Variants**: Enable letter-variant matching (same as Variants mode) on all expanded terms
- **Judeo-Arabic (JA)**: Expand words with the Arabic definite article אל- (8 forms per word)
- **Flexible Spacing**: Tolerate spaces within words — useful for OCR errors where spaces are inserted mid-word
- **Bidirectional Gap**: Search for terms in both forward and reverse order
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Query Builder', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
The Query Builder provides a visual interface for constructing Responsa queries:

1. Click the **Query Builder** button (visible when Responsa mode is active)
2. Add 2-4 **components**, each representing a search term or group
3. Enter one or more **words** per component (multiple words = OR alternatives)
4. Toggle **per-word modifiers**: prefix (#), suffix (#), wildcard (*), plene (%), negation
5. Set the **distance** (max words) between components using the spinners
6. Watch the **live preview** update as you modify the query
7. Click **Apply** to generate the Responsa syntax and trigger a search

*Note:* The builder is one-way — changes in the builder update the search field, but editing the text field does not update the builder.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Explosion Guard', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
When a query expands beyond **500 terms**, the system automatically downgrades options to keep the search fast.
The downgrade order is: variants basic → off → JA off → plene off → suffixes off → prefixes off.
A warning notification will appear explaining which options were turned off.

**Tip:** Use more specific queries or fewer modifiers to avoid hitting the guard.
        ''').style('color: var(--text-secondary);')

    # === Parallels Search (MAIN SECTION) ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-parallels"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('compare_arrows').classes('text-2xl text-primary')
            h2('Parallels Search', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This tool is designed for researchers wishing to find **parallel texts** for a complete literary composition
(such as a Piyyut, medieval commentary, or other rare work) within the Genizah, thereby locating additional
textual witnesses—both direct and indirect.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        # How it Works
        h3('How it Works (The Mechanism)', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')
        ui.markdown('''
Unlike a regular search, the engine does **not** search for the entire text as a single unit. The process works as follows:

1. **Chunking:** The software splits your source text into small segments ("chunks") of N words each.
2. **Individual Search:** Each chunk is searched separately in the Genizah database.
3. **Scoring:** If a specific chunk is found in a manuscript, it receives a "score" based on match quality.
4. **Aggregation:** At the end of the process, the software **aggregates** the results—if a manuscript contains many matching chunks, it receives a high score and appears at the top of the list.

You can also search in Lab mode, using an algorithm based on the **Shmidman-Koppel-Porat fingerprinting method**, which encodes Hebrew words into normalized "fingerprints" that allow matching despite spelling variations common in medieval manuscripts.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        # Parameters
        h3('Important Parameters', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')

        params_data = [
            ('Chunk Size', 'The number of words in each search unit. A low value (2-3) will result in slower search and return many irrelevant results; a high value (10+) may miss true matches.'),
            ('Search Mode', 'Like regular search—Exact for precise matching, Variants for flexibility with spelling variations, Fuzzy for maximum tolerance.'),
            ('Variant Level', 'When using Variants mode, controls how many letter substitutions are allowed. Higher = more flexible but noisier.'),
            ('Num Changes', 'Maximum character changes per word in variant matching (×1, ×2, or ×3).'),
            ('Deep Scan', 'Relevant for Lab mode. A much deeper and more thorough scan. Significantly slower, but recommended for finding rare phrases or ensuring nothing is missed.'),
        ]

        with ui.column().classes('gap-3 mb-4'):
            for param, desc in params_data:
                with ui.column().classes('gap-1 p-3 rounded').style('background: var(--bg-secondary);'):
                    ui.label(param).classes('font-bold').style('color: var(--primary-700);')
                    ui.markdown(desc).style('color: var(--text-secondary);')

        # Filter Text / Sefaria
        h3('Filtering Known Sources', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
A powerful and recommended feature for reducing "noise" in your results. If your source text quotes Bible verses, Mishnah,
Talmud, or other known texts, you can **load these sources** so matches found in them are filtered separately.

**How to use:**
1. Expand the **"Filter text (exclude known sources)"** panel
2. Click **Tanakh**, **Mishnah**, or **Talmud** to load standard sources from Sefaria
3. Or click **More Sources...** to browse the full Sefaria library
4. Or click **Search Sefaria** to load any text by reference (e.g., "Rashi on Genesis 1")
5. Or click **Add Custom Text** to paste your own reference text

**What happens:**
- Matches found in the source manuscripts appear in the main results
- Matches found in your filter texts (Bible verses, etc.) appear in a separate **"Filtered Results"** section
- This helps you focus on *new* parallels rather than known quotations
- The texts will automatically load in your next search as well, until you remove them.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        # Cross-Paragraph Search
        h3('Cross-Paragraph Search', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
When searching for parallels to a text that contains paragraph breaks (e.g., a piyyut with stanzas, or a text with section divisions),
you can enable **cross-paragraph search** to specifically find manuscripts that preserve text spanning across these boundaries.

**Why is this useful?**
- Text **within** paragraphs often contains citations from other sources (Mishnah, Talmud, known phrases) or sources that quote the composition you're searching for
- Text that **crosses** paragraph boundaries is much less likely to be a citation, since citations rarely span across structural breaks
- This effectively filters out most of the "noise" and helps you find genuine textual witnesses

**How to use:**
1. Enter your text with paragraph breaks (or set a custom delimiter like period or colon)
2. Select a search mode: **Full search** (all results), **Cross-paragraph only** (only matches that cross boundaries),
   or **Combined** (all results, with boundary-crossing matches boosted)
3. Results that cross paragraph boundaries are marked with a special indicator
        ''').style('color: var(--text-secondary);').classes('mb-4')

        # Understanding Results
        h3('Understanding Results', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
Results are **grouped by manuscript** and sorted by score:

- **Max Score:** The highest-scoring match found in that manuscript
- **Avg Score:** Average score across all matches in the manuscript

Click on a result to expand and see:
- **Your Text:** The chunk from your source that matched
- **Manuscript Text:** The corresponding text from the Genizah manuscript
- Matching words are **highlighted** for easy comparison
        ''').style('color: var(--text-secondary);')

    # === Browse Manuscript ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-browse"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('menu_book').classes('text-2xl text-primary')
            h2('Browse Manuscript', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This page enables convenient continuous reading of a full manuscript, synchronized with source images.

**Loading a Manuscript:**
- Enter a **Shelfmark** in the search box
- The search is flexible and ignores spaces/punctuation (e.g., `TS NS 13 15` finds `T-S NS 13.15`)

**Features:**
- **Images:** An image viewer displays the manuscript page. You can zoom, rotate, and view in full screen.
- **Page Navigation:** Use the arrows or page dropdown to navigate between pages
- **View All:** Click to display all manuscript pages in one long scrollable view
- **Find Parallels:** Send the current page text to Parallels Search
- **View on Ktiv:** Opens the manuscript in the National Library of Israel's online catalog
- **Edit & Comment:** Submit corrections or add scholarly comments for the benefit of the entire research community, or for yourself. (Requires login)
        ''').style('color: var(--text-secondary);')

    # === Lists ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-lists"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('star').classes('text-2xl text-primary')
            h2('Lists', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Save important manuscripts to personal lists for later reference.

**Creating Lists:**
- Click the ⭐ star icon on any search result, parallel match, or browse page to add it to a list
- Create new lists to organize your research by topic, project, or any other criteria

**Managing Lists:**
- View all your lists in the **Lists** page
- Add notes to individual items
- Export lists to Excel or Word format
- Lists sync across devices when logged in

**Projects:**
- Group related lists into **Projects** for better organization
- Each project can have its own color coding
        ''').style('color: var(--text-secondary);')

    # === Export ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-export"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('download').classes('text-2xl text-primary')
            h2('Exporting Data', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
At any stage, you can export results for external use:

- **📄 Word (DOCX):** Formatted report suitable for academic work
- **📊 Excel (XLSX):** Spreadsheet with rich formatting and color highlighting of found words

**Export locations:**
- **Search Results:** Use the export buttons above the results table
- **Parallels Results:** Use the export buttons in the results header
- **Lists:** Export individual lists from the Lists page
        ''').style('color: var(--text-secondary);')

    # === Contact ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('mail').classes('text-2xl text-primary')
            h2('Feedback & Contact', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.label('For questions, bug reports, or feature requests:').style('color: var(--text-secondary);')
        ui.label('gershuni@gmail.com').classes('text-lg font-mono mt-2')


def _create_hebrew_content():
    """Create the Hebrew help content."""

    # === Table of Contents ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('list').classes('text-2xl text-primary')
            h2('תוכן העניינים', classes='text-xl font-bold', style='color: var(--text-primary);')

        with ui.column().classes('gap-2'):
            toc_items = [
                ('intro', 'הקדמה: איך זה עובד?'),
                ('search', 'חיפוש'),
                ('responsa', 'חיפוש בסגנון פרויקט השו"ת'),
                ('parallels', 'חיפוש מקבילות'),
                ('browse', 'עיון בכתב יד'),
                ('lists', 'רשימות'),
                ('export', 'ייצוא נתונים'),
            ]
            for anchor, title in toc_items:
                ui.link(f'• {title}', f'#help-{anchor}').classes('text-primary hover:underline').style('direction: rtl;')

    # === Introduction ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-intro"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('info').classes('text-2xl text-primary')
            h2('הקדמה: איך זה עובד?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**חיפוש גניזת קהיר של דיקטה** מאפשר גישה מהירה ומתקדמת לקורפוס התעתוקים של פרויקט "מדרש" (MiDRASH).
הפלטפורמה מבוססת על מנוע חיפוש מהיר (Tantivy) ומשלבת אלגוריתמים ייחודיים לטיפול בחלק משיבושי הקריאה של אלגוריתם הפענוח של פרויקט מדרש.

**דרישת ייחוס:** תעתיקי מדרש משוחררים ברישיון CC-BY-4.0, ופירוש הדבר שניתן להשתמש בהם תוך ייחוס מתאים. לכן אם אתם משתמשים בתעתיקים, אנא תנו קרדיט ל:

> Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments [Data set]. Zenodo.
> https://doi.org/10.5281/zenodo.17734473

האפליקציה מושכת מידע ותמונות מ:
- **הספרייה הלאומית של ישראל (NLI)**
- **ספריית הבודליאנה** באוקספורד
- **ספריית אוניברסיטת קיימברידג'**

*הערה:* חלק משרתי הספריות עשויים לחסום גישה מרשתות שאינן ביתיות (למשל, נקודה חמה מטלפון נייד).
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-search"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('search').classes('text-2xl text-primary')
            h2('חיפוש', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.label('זוהי נקודת הכניסה לחיפוש טקסט חופשי או מטא-דאטה בקורפוס. ניתן להשתמש בקיצורי הדרך המופיעים בסוגריים להלן כדי להגיע ישירות לסוג החיפוש המעניין אתכם.').style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4 w-full')

        h3('מצבי חיפוש', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')

        modes_data = [
            ('מדויק (=)', 'מוצא רק את המילה או את רצף המילים בדיוק כפי שנכתבו. לחיפוש עם פערים בין המילים יש למלא את התיבה "מרווח" במספר הרצוי.'),
            ('וריאנטים (?)', 'מתחשב בחילופי אותיות נפוצים (למשל: ד/ר, ה/ח, ו/י).'),
            ('וריאנטים מורחב (??)', 'גמישות רבה יותר בחילופי אותיות (למשל: ק/כ, ט/ת). איטי יותר ועשוי להחזיר תוצאות פחות רלוונטיות.'),
            ('וריאנטים מקסימלי (???)', 'גמישות מירבית. איטי במיוחד. השתמשו כשמצבים אחרים מחזירים מעט מדי תוצאות.'),
            ('עמום (~)', 'משתמש ב[מרחק לווינשטיין](https://he.wikipedia.org/wiki/%D7%9E%D7%A8%D7%97%D7%A7_%D7%9C%D7%95%D7%99%D7%A0%D7%A9%D7%98%D7%99%D7%99%D7%9F) למציאת מילים דומות גם עם שגיאות פענוח.'),
            ('ביטוי רגולרי (/)', 'חיפוש מתקדם למשתמשים מנוסים. דוגמה: \\bא[א-ת]{3}\\b מוצא מילים בנות 4 אותיות המתחילות באל"ף. תוכלו להיעזר במנוע הבינה המלאכותית המועדף עליכם כדי לבנות ביטוי רגולרי המתאים לצרכיכם.'),
            ('כותרת ($)', 'חיפוש בתוך כותרות הקטלוג של חיבורים.'),
            ('מספר מדף (#)', 'חיפוש מהיר של מספרי מדף (למשל: "T-S NS 13.15").'),
            ('רספונסה (R)', 'תחביר חיפוש בסגנון פרויקט השו"ת של אוניברסיטת בר-אילן, עם הרחבת תחיליות/סיומות, תווים כלליים, חלופות כתיב ומרווחים. מוכר למשתמשי פרויקט השו"ת; קל ללמוד גם למי שלא מכיר. ראו [חיפוש בסגנון פרויקט השו"ת](#help-responsa) להלן.'),
        ]

        with ui.column().classes('gap-3 mb-4 w-full'):
            for mode, desc in modes_data:
                with ui.row().classes('gap-2 w-full').style('direction: rtl;'):
                    ui.label(f'• {mode}:').classes('font-bold min-w-40').style('color: var(--primary-700);')
                    ui.markdown(desc).style('color: var(--text-secondary);')

        h3('בקרת רמת וריאנטים', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
בעת שימוש במצב **וריאנטים**, ניתן לשלוט ברמת הגמישות:

- **מספר שינויים (×1, ×2, ×3):** מספר מירבי של החלפות תווים המותרות למילה.
  - ×1 = מחמיר מאוד, פחות חיובי שגוי
  - ×2 = מאוזן (מומלץ)
  - ×3 = מקל, עשוי למצוא התאמות נדירות יותר
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Responsa-Style Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-responsa"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('tune').classes('text-2xl text-primary')
            h2('חיפוש בסגנון פרויקט השו"ת', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
מצב זה משתמש בתחביר חיפוש בהשראת **פרויקט השו"ת של אוניברסיטת בר-אילן** — מאגר המידע הנפוץ לחיפוש
טקסטים בעברית ובארמית. אם אתם כבר מכירים את מוסכמות החיפוש של פרויקט השו"ת, תרגישו בבית.
אם לא — התחביר פשוט ללמידה ומציע כלים עוצמתיים לחיפוש כתבי יד בעברית ובערבית-יהודית:
הרחבת תחיליות/סיומות, תווים כלליים, חלופות כתיב ומרווחים.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('הפעלת המצב', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
- בחרו **רספונסה (R)** מתפריט מצב החיפוש, או
- הקלידו `R ` (R ואחריו רווח) בתחילת השאילתה

כאשר מצב זה פעיל, קיצורי הדרך הרגילים (=, ?, ~, /, $, #) מושבתים — השאילתה משתמשת בתחביר השו"ת במקום.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('מדריך תחביר', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')

        syntax_data = [
            ('#word', 'הרחבת תחיליות — מוסיף 24 צורות תחילית עבריות (ו, ה, ב, כ, ל, מ, ש + צירופים)', '#שלום מוצא ושלום, השלום, בשלום וכו\''),
            ('word#', 'הרחבת סיומות — מוסיף 25 צורות סיומת עבריות (י, ו, ם, ן, ה, ך, כם, כן וכו\')', 'שלום# מוצא שלומם, שלומו, שלומך וכו\''),
            ('#word#', 'הרחבת תחיליות וסיומות יחד', '#שלום# מוצא את כל הצירופים'),
            ('*word', 'תו כללי בתחילת מילה — כל תווים לפני המילה', '*שלום מתאים לכבשלום וכו\''),
            ('word*', 'תו כללי בסוף מילה — כל תווים אחרי המילה', 'שלום* מתאים לשלומות וכו\''),
            ('%word', 'חלופות כתיב מלא/חסר — הוספת/הסרת ו/י', '%שלום מוצא שלום, שלם'),
            ('(a/b)', 'חלופות OR — מתאים לכל אחת מהאפשרויות', '(שלום/שלומות) מתאים לשתיהן'),
            ('[N]', 'מרווח של N מילים בין מונחים', 'שלום [3] עולם = עד 3 מילים ביניהם'),
        ]

        with ui.column().classes('gap-3 mb-4 w-full'):
            for syntax, meaning, example in syntax_data:
                with ui.column().classes('gap-1 p-3 rounded w-full').style('background: var(--bg-secondary);'):
                    with ui.row().classes('gap-2 items-center w-full').style('direction: rtl;'):
                        ui.label(syntax).classes('font-bold font-mono').style('color: var(--primary-700); min-width: 80px; direction: ltr;')
                        ui.label(meaning).style('color: var(--text-secondary);')
                    ui.label(f'דוגמה: {example}').classes('text-sm').style('color: var(--text-tertiary, #888); direction: rtl; text-align: right;')

        ui.markdown('**טיפ:** ניתן לשלב מגדירים, למשל `#%word*` מפעיל הרחבת תחיליות + חלופות כתיב + תו כללי בסוף.', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('אפשרויות משנה', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
כאשר מצב זה פעיל, מופיעות ארבע תיבות סימון מתחת לשדה החיפוש:

- **וריאנטים**: הפעלת התאמת חלופי אותיות (כמו במצב וריאנטים) על כל המונחים המורחבים
- **ערבית-יהודית (JA)**: הרחבת מילים עם ה"א הידיעה הערבית אל- (8 צורות למילה)
- **ריווח גמיש**: סובלנות לרווחים בתוך מילים — שימושי לשגיאות OCR שבהן נוספים רווחים באמצע מילה
- **מרווח דו-כיווני**: חיפוש מונחים גם בסדר קדימה וגם בסדר הפוך
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('בונה השאילתות', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
בונה השאילתות מספק ממשק ויזואלי לבניית שאילתות בתחביר השו"ת:

1. לחצו על כפתור **בונה השאילתות** (גלוי כאשר מצב שו"ת פעיל)
2. הוסיפו 2-4 **רכיבים**, כל אחד מייצג מונח חיפוש או קבוצה
3. הזינו **מילה** אחת או יותר לכל רכיב (מספר מילים = חלופות OR)
4. הפעילו **מגדירים למילה**: תחילית (#), סיומת (#), תו כללי (*), כתיב מלא/חסר (%), שלילה
5. הגדירו את ה**מרחק** (מספר מילים מרבי) בין רכיבים באמצעות הספינרים
6. צפו ב**תצוגה מקדימה חיה** המתעדכנת בזמן שאתם משנים את השאילתה
7. לחצו על **החל** כדי ליצור את שאילתת השו"ת ולהפעיל חיפוש

*הערה:* הבונה הוא חד-כיווני — שינויים בבונה מעדכנים את שדה החיפוש, אך עריכת שדה הטקסט אינה מעדכנת את הבונה.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('מגן פיצוץ', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
כאשר שאילתה מתרחבת מעבר ל-**500 מונחים**, המערכת מורידה אוטומטית אפשרויות כדי לשמור על מהירות החיפוש.
סדר ההורדה: וריאנטים בסיסיים ← כבוי ← ערבית-יהודית כבוי ← כתיב מלא/חסר כבוי ← סיומות כבוי ← תחיליות כבוי.
תופיע התראה המסבירה אילו אפשרויות כובו.

**טיפ:** השתמשו בשאילתות ספציפיות יותר או בפחות מגדירים כדי להימנע מהפעלת המגן.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Parallels Search (MAIN SECTION) ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-parallels"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('compare_arrows').classes('text-2xl text-primary')
            h2('חיפוש מקבילות', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
כלי זה מיועד לחוקרים המעוניינים למצוא **טקסטים מקבילים** לחיבור ספרותי שלם (כגון פיוט, פירוש מימי הביניים או יצירה נדירה אחרת) בתוך הגניזה, ובכך לאתר עדי נוסח נוספים – ישירים ועקיפים.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        # How it Works
        h3('איך זה עובד? (המנגנון)', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
בניגוד לחיפוש רגיל, המנוע **לא** מחפש את הטקסט כולו כמקשה אחת. התהליך מתבצע כך:

1. **חלוקה למקטעים:** התוכנה מחלקת את טקסט המקור שלכם למקטעים קטנים בני N מילים כל אחד.
2. **חיפוש פרטני:** כל מקטע נשלח לחיפוש בנפרד במאגר הגניזה.
3. **ניקוד:** אם מקטע מסוים נמצא בכתב יד, הוא מקבל "ניקוד" על פי איכות ההתאמה.
4. **צבירה:** בסוף התהליך, התוכנה **מקבצת** את התוצאות – אם כתב יד מכיל מקטעים רבים שנמצאו, הוא יקבל ציון גבוה ויופיע בראש הרשימה.

ניתן לחפש גם במצב מעבדה, על פי אלגוריתם מבוסס על **שיטת הטביעות של שמידמן-קופל-פורת**, אשר מקודדת מילים עבריות ל"טביעות" מנורמלות המאפשרות התאמה למרות שינויי כתיב הנפוצים בכתבי יד מימי הביניים.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        # Parameters
        h3('פרמטרים חשובים', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')

        params_data = [
            ('גודל מקטע', 'מספר המילים בכל יחידת חיפוש. ערך נמוך (2-3) יגרור חיפוש איטי יותר ויחזיר תוצאות לא רלוונטיות רבות; ערך גבוה (10+) עלול להחמיץ התאמות אמיתיות.'),
            ('מצב חיפוש', 'כמו בחיפוש רגיל – מדויק להתאמה מדויקת, וריאנטים לגמישות בשינויי כתיב, עמום לסבילות מירבית.'),
            ('רמת וריאנטים', 'בשימוש במצב וריאנטים, קובע כמה החלפות אותיות מותרות. גבוה יותר = גמיש יותר אך עם יותר "רעש".'),
            ('מספר שינויים', 'מספר שינויי תווים מירבי למילה בהתאמת וריאנטים (×1, ×2 או ×3).'),
            ('סריקה עמוקה', 'רלוונטי למצב המעבדה. סריקה מעמיקה ויסודית יותר. איטית משמעותית, אך מומלצת למציאת ביטויים נדירים או לוודא שלא החמצתם כלום.'),
        ]

        with ui.column().classes('gap-3 mb-4 w-full'):
            for param, desc in params_data:
                with ui.column().classes('gap-1 p-3 rounded w-full').style('background: var(--bg-secondary);'):
                    ui.label(param).classes('font-bold').style('color: var(--primary-700); direction: rtl; text-align: right;')
                    ui.markdown(desc, extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

        # Filter Text / Sefaria
        h3('סינון מקורות ידועים', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
תכונה חזקה ומומלצת להפחתת "רעש" בתוצאות. אם טקסט המקור שלכם מצטט פסוקי תנ"ך, משנה, תלמוד או טקסטים ידועים אחרים, תוכלו **לטעון מקורות אלה** כך שהתאמות שנמצאו בהם יסוננו בנפרד.

**כיצד להשתמש:**
1. הרחיבו את הפאנל **"סינון טקסט (החרג מקורות ידועים)"**
2. לחצו על **תנ"ך**, **משנה** או **תלמוד** לטעינת מקורות סטנדרטיים מספריא
3. או לחצו על **מקורות נוספים...** לעיון בספריית ספריא המלאה
4. או לחצו על **חיפוש בספריא** לטעינת כל טקסט לפי הפניה (למשל: "רש"י על בראשית א")
5. או לחצו על **הוסף טקסט מותאם** להדבקת טקסט ייחוס משלכם

**מה קורה:**
- התאמות שנמצאו בכתבי יד מקור מופיעות בתוצאות הראשיות
- התאמות שנמצאו בטקסטי הסינון שלכם (פסוקים וכו') מופיעות בקטע **"תוצאות מסוננות"** נפרד
- זה עוזר לכם להתמקד במקבילות *חדשות* ולא בציטוטים ידועים. הטקסטים ייטענו אוטומטית גם בחיפוש הבא, עד שתסירו אותם.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        # Cross-Paragraph Search
        h3('חיפוש חוצה-פסקאות', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
כאשר מחפשים מקבילות לטקסט המכיל מעברי פסקה (למשל: פיוט עם בתים, או טקסט עם חלוקה לסעיפים), ניתן להפעיל **חיפוש חוצה-פסקאות** כדי למצוא באופן ספציפי כתבי יד ששומרים על טקסט החוצה גבולות אלה.

**למה זה שימושי?**
- טקסט **בתוך** פסקאות מכיל לעיתים קרובות ציטוטים ממקורות אחרים (משנה, תלמוד, ביטויים ידועים) או מקורות שמצטטים את החיבור שאתם מחפשים
- טקסט ש**חוצה** גבולות פסקה הוא הרבה פחות סביר להיות ציטוט, מכיוון שציטוטים לעיתים רחוקות חוצים שברים מבניים
- זה מסנן ביעילות את רוב ה"רעש" ועוזר למצוא עדי נוסח אמיתיים

**כיצד להשתמש:**
1. הזינו את הטקסט עם מעברי פסקה (או הגדירו מפריד מותאם כמו נקודה או נקודתיים)
2. בחרו מצב חיפוש: **חיפוש מלא** (כל התוצאות), **חוצה-פסקאות בלבד** (רק התאמות שחוצות גבולות), או **משולב** (כל התוצאות, עם הגברת התאמות חוצות-פסקאות)
3. תוצאות שחוצות גבולות פסקה מסומנות בסימון מיוחד
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        # Understanding Results
        h3('הבנת התוצאות', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
התוצאות **מקובצות לפי כתב יד** וממוינות לפי ציון:

- **ציון מקסימלי:** ההתאמה בעלת הציון הגבוה ביותר שנמצאה בכתב היד
- **ציון ממוצע:** ממוצע הציונים של כל ההתאמות בכתב היד

לחצו על תוצאה כדי להרחיב ולראות:
- **הטקסט שלכם:** המקטע מהמקור שלכם שהותאם
- **טקסט כתב היד:** הטקסט המקביל מכתב יד הגניזה
- מילים תואמות **מודגשות** להשוואה קלה
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Browse Manuscript ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-browse"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('menu_book').classes('text-2xl text-primary')
            h2('עיון בכתב יד', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
עמוד זה מאפשר קריאה רציפה ונוחה של כתב יד שלם, בסנכרון עם תמונות המקור.

**טעינת כתב יד:**
- הזינו **מספר מדף** בתיבת החיפוש
- החיפוש גמיש ומתעלם מרווחים/סימני פיסוק (למשל: `TS NS 13 15` מוצא `T-S NS 13.15`)

**תכונות:**
- **תמונות:** צפיין תמונות מציג את עמוד כתב היד. ניתן לעשות זום, לסובב, ולצפות במסך מלא
- **ניווט בעמודים:** השתמשו בחצים או בתפריט העמודים לניווט בין עמודים
- **הצג הכל:** לחצו להצגת כל עמודי כתב היד ברצף אחד לגלילה
- **מצא מקבילות:** שליחת טקסט העמוד הנוכחי לחיפוש מקבילות
- **צפה בכתיב:** פתיחת כתב היד בקטלוג המקוון של הספרייה הלאומית
- **עריכה והערות:** הגישו תיקונים או הוסיפו הערות מחקריות לטובת כלל קהילת החוקרים, או לעצמכם. (דורש התחברות)
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Lists ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-lists"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('star').classes('text-2xl text-primary')
            h2('רשימות', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
שמרו כתבי יד חשובים ברשימות אישיות להתייחסות עתידית.

**יצירת רשימות:**
- לחצו על סמל הכוכב ⭐ בכל תוצאת חיפוש, התאמת מקבילות או עמוד עיון כדי להוסיף לרשימה
- צרו רשימות חדשות לארגון המחקר לפי נושא, פרויקט או כל קריטריון אחר

**ניהול רשימות:**
- צפו בכל הרשימות שלכם בעמוד **רשימות**
- הוסיפו הערות לפריטים בודדים
- ייצאו רשימות לפורמט Excel או Word
- רשימות מסתנכרנות בין מכשירים כאשר מחוברים

**פרויקטים:**
- קבצו רשימות קשורות ל**פרויקטים** לארגון טוב יותר
- לכל פרויקט יכול להיות קידוד צבע משלו
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Export ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-export"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('download').classes('text-2xl text-primary')
            h2('ייצוא נתונים', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
בכל שלב, ניתן לייצא תוצאות לשימוש חיצוני:

- **📄 Word (DOCX):** דוח מעוצב המתאים לעבודה אקדמית
- **📊 Excel (XLSX):** גיליון אלקטרוני עם עיצוב עשיר והדגשת צבע של מילים שנמצאו

**מיקומי ייצוא:**
- **תוצאות חיפוש:** השתמשו בכפתורי הייצוא מעל טבלת התוצאות
- **תוצאות מקבילות:** השתמשו בכפתורי הייצוא בכותרת התוצאות
- **רשימות:** ייצאו רשימות בודדות מעמוד הרשימות
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Contact ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('mail').classes('text-2xl text-primary')
            h2('משוב ויצירת קשר', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.label('לשאלות, דיווח על באגים או בקשות תכונות:').style('color: var(--text-secondary); direction: rtl; text-align: right;')
        ui.label('gershuni@gmail.com').classes('text-lg font-mono mt-2')
