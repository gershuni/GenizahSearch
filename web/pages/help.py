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
                ('pgp', 'Princeton Geniza Project (PGP) Data'),
                ('reading-desk', 'Reading Desk'),
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
            ('Variants (?, ??, ???)', 'Accounts for common letter substitutions in these texts (e.g., Dalet/Resh, He/Het, Vav/Yod). At the basic level (?) substitutions are limited; extended (??) adds pairs like Qof/Kaf, Tet/Tav; maximum (???) provides full flexibility but is slower. You can also control the number of changes per word (\u00d71 strict, \u00d72 balanced, \u00d73 lenient). In the general settings you can switch the level selector from a dropdown to a slider.'),
            ('\U0001F195 Responsa Project (R)', 'Search syntax inspired by the Bar-Ilan Responsa Project, with prefix/suffix expansion, wildcards, spelling variants, and proximity gaps. Also includes a convenient and flexible tabular query builder. Familiar to Responsa Project users; easy to learn for newcomers. See [Responsa-Style Search](#help-responsa) below.'),
            ('Fuzzy (~)', 'Uses [Levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance) to find similar words even with decoding errors.'),
            ('Regex (/)', 'Advanced search for experienced users. Example: \\b\u05d0[\u05d0-\u05ea]{3}\\b finds 4-letter words starting with Aleph. You can use your preferred AI engine to help build a regex pattern suited to your needs.'),
            ('Title ($)', 'Searches within the catalog titles of compositions.'),
            ('Shelfmark (#)', 'Fast search for shelfmarks (e.g., "T-S NS 13.15").'),
            ('\U0001F195 PGP Tags', 'Browse manuscripts by topic tags from the Princeton Geniza Project (PGP).'),
        ]

        with ui.column().classes('gap-3 mb-4'):
            for mode, desc in modes_data:
                with ui.row().classes('gap-2'):
                    ui.label(f'\u2022 {mode}:').classes('font-bold min-w-40').style('color: var(--primary-700);')
                    ui.markdown(desc).style('color: var(--text-secondary);')

    # === Responsa-Style Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-responsa"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('tune').classes('text-2xl text-primary')
            h2('Responsa-Style Search', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This mode offers two search methods inspired by the Bar-Ilan Responsa Project: a textual syntax with operators for prefixes, suffixes, plene/defective spelling and more; and an intuitive tabular search interface that builds the query for you.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        ui.markdown('''
**Activation:** Select **Responsa Project (R)** from the search Mode dropdown, or type `R ` (R followed by a space) at the beginning of your query.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Syntax', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')

        syntax_data = [
            ('#word', 'Prefixes (24 forms: \u05d5, \u05d4, \u05d1, \u05db, \u05dc, \u05de, \u05e9 + compounds)', '#\u05e9\u05dc\u05d5\u05dd \u2192 \u05d5\u05e9\u05dc\u05d5\u05dd, \u05d4\u05e9\u05dc\u05d5\u05dd, \u05d1\u05e9\u05dc\u05d5\u05dd...'),
            ('word#', 'Suffixes (25 forms: \u05d9, \u05d5, \u05dd, \u05df, \u05d4, \u05da, \u05db\u05dd, \u05db\u05df...)', '\u05e9\u05dc\u05d5\u05dd# \u2192 \u05e9\u05dc\u05d5\u05de\u05dd, \u05e9\u05dc\u05d5\u05de\u05d5, \u05e9\u05dc\u05d5\u05de\u05da...'),
            ('#word#', 'Both prefixes + suffixes', '#\u05e9\u05dc\u05d5\u05dd# \u2192 all combinations'),
            ('*word', 'Wildcard before', '*\u05e9\u05dc\u05d5\u05dd \u2192 \u05db\u05d1\u05e9\u05dc\u05d5\u05dd...'),
            ('word*', 'Wildcard after', '\u05e9\u05dc\u05d5\u05dd* \u2192 \u05e9\u05dc\u05d5\u05de\u05d5\u05ea...'),
            ('%word', 'Plene/defective spelling (insert/remove \u05d5/\u05d9)', '%\u05e9\u05dc\u05d5\u05dd \u2192 \u05e9\u05dc\u05d5\u05dd, \u05e9\u05dc\u05dd'),
            ('(a/b)', 'OR alternatives', '(\u05e9\u05dc\u05d5\u05dd/\u05e9\u05dc\u05d5\u05de\u05d5\u05ea)'),
            ('[N]', 'Gap of N words', '\u05e9\u05dc\u05d5\u05dd [3] \u05e2\u05d5\u05dc\u05dd'),
        ]

        with ui.element('table').classes('w-full mb-4').style('border-collapse: collapse;'):
            with ui.element('thead'):
                with ui.element('tr'):
                    for header in ['Syntax', 'Meaning', 'Example']:
                        with ui.element('th').style('padding: 6px 10px; border-bottom: 2px solid var(--primary-300); color: var(--text-primary); text-align: left;'):
                            ui.label(header).classes('font-bold text-sm')
            with ui.element('tbody'):
                for syntax, meaning, example in syntax_data:
                    with ui.element('tr').style('border-bottom: 1px solid var(--border-color, #e0e0e0);'):
                        with ui.element('td').style('padding: 4px 10px; white-space: nowrap;'):
                            ui.label(syntax).classes('font-mono font-bold text-sm').style('color: var(--primary-700);')
                        with ui.element('td').style('padding: 4px 10px; color: var(--text-secondary);'):
                            ui.label(meaning).classes('text-sm')
                        with ui.element('td').style('padding: 4px 10px; color: var(--text-tertiary, #888);'):
                            ui.label(example).classes('text-sm font-mono')

        ui.markdown('Modifiers can be combined, e.g. `#%word*` = prefix expansion + plene variants + wildcard suffix.').style('color: var(--text-secondary);').classes('mb-2')

        ui.markdown('*Note:* You cannot search with wildcards on both sides (`*word*`) due to search engine limitations; such a query is automatically converted to `#word#` (grammatical prefixes and suffixes).').style('color: var(--text-secondary);').classes('mb-4')

        h3('Sub-Options', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
- **Variants**: Enable letter-variant matching on all expanded terms
- **Judeo-Arabic (JA)**: Expand words with the Arabic definite article \u05d0\u05dc- (8 forms per word)
- **Flexible Spacing**: Ignore erroneous spaces within words \u2014 very useful given the many spacing errors in automatic transcription, but adds load to the query
- **Bidirectional Gap**: Search for terms in both forward and reverse order
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Tabular Search', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
1. Click the **Tabular Search** button (visible when Responsa mode is active)
2. Add 2\u20134 **components**, each representing a search term or group
3. Enter one or more **words** per component (multiple words = OR alternatives)
4. Toggle **per-word modifiers**: prefix (#), suffix (#), wildcard (*), plene (%), negation
5. Set the **distance** between components using the spinners
6. Watch the **live preview** update in real time
7. Click **Search** to execute the query
        ''').style('color: var(--text-secondary);').classes('mb-4')

        ui.markdown('*Note:* When a query expands beyond 500 terms, the system automatically downgrades options (variants, Judeo-Arabic, plene, etc.) to maintain speed, and displays a notification accordingly.').style('color: var(--text-secondary);')

    # === Parallels Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-parallels"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('compare_arrows').classes('text-2xl text-primary')
            h2('Parallels Search', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This tool is designed for researchers wishing to find **parallel texts** for a complete literary composition
(such as a Piyyut, medieval commentary, or other rare work) within the Genizah, thereby locating additional
textual witnesses\u2014both direct and indirect.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('How it Works', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')
        ui.markdown('''
Unlike a regular search, the engine does **not** search for the entire text as a single unit. The process works as follows:

1. **Chunking:** The software splits your source text into small segments ("chunks") of N words each.
2. **Individual Search:** Each chunk is searched separately in the Genizah database.
3. **Scoring:** If a specific chunk is found in a manuscript, it receives a "score" based on match quality.
4. **Aggregation:** At the end of the process, the software **aggregates** the results\u2014if a manuscript contains many matching chunks, it receives a high score and appears at the top of the list.

You can also search in Lab mode, using an algorithm based on the **Shmidman-Koppel-Porat fingerprinting method**, which encodes Hebrew words into normalized "fingerprints" that allow matching despite spelling variations common in medieval manuscripts.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Important Parameters', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')
        ui.markdown('''
- **Chunk Size:** The number of words in each search unit. A low value (2\u20133) will result in slower search and many irrelevant results; a high value (10+) may miss true matches.
- **Search Mode:** Like regular search\u2014Exact, Variants, or Fuzzy.
- **Variant Level / Num Changes:** Controls flexibility of letter substitutions (see Search Modes above).
- **Deep Scan:** Relevant for Lab mode. A much deeper and more thorough scan, significantly slower but recommended for finding rare phrases.
        ''').style('color: var(--text-secondary);').classes('mb-4')

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

Matches found in your filter texts appear in a separate **"Filtered Results"** section, so you can focus on new parallels. The texts will automatically load in your next search as well, until you remove them.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Cross-Paragraph Search', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
When searching for parallels to a text that contains paragraph breaks (e.g., a piyyut with stanzas, or a text with section divisions),
you can enable **cross-paragraph search** to specifically find manuscripts that preserve text spanning across these boundaries.

**Why is this useful?**
- Text **within** paragraphs often contains citations from other sources or sources that quote the composition you're searching for
- Text that **crosses** paragraph boundaries is much less likely to be a citation, since citations rarely span across structural breaks
- This effectively filters out most of the "noise" and helps you find genuine textual witnesses

**How to use:**
1. Enter your text with paragraph breaks (or set a custom delimiter like period or colon)
2. Select a search mode: **Full search** (all results), **Cross-paragraph only** (only matches that cross boundaries),
   or **Combined** (all results, with boundary-crossing matches boosted)
3. Results that cross paragraph boundaries are marked with a special indicator
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Understanding Results', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
Results are **grouped by manuscript** and sorted by score:
- **Max Score:** The highest-scoring match found in that manuscript
- **Avg Score:** Average score across all matches in the manuscript

Click on a result to expand and see your source chunk alongside the matching manuscript text, with matching words **highlighted** for easy comparison.
        ''').style('color: var(--text-secondary);')

    # === PGP Information ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-pgp"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('school').classes('text-2xl text-primary')
            h2('Princeton Geniza Project (PGP) Data', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
The system integrates data from the Princeton Geniza Project (PGP)\u2014a scholarly database containing approximately 36,000 cataloged documents with transcriptions, translations, descriptions, and detailed subject tagging.

**PGP Badge in Search Results**

Manuscripts with available PGP data are marked with a green "PGP" badge in search results, so you can quickly identify manuscripts with scholarly transcriptions and additional research information.

**PGP Information in Browse Manuscript**

When a manuscript has PGP data, an information panel is displayed showing:
- **Document type** and languages (e.g., Letter, Judeo-Arabic)
- **Subject tags** \u2014 clicking a tag searches for all manuscripts with that topic
- **Description** in English (with translation option)
- **Dating** (including rationale if available)
- **Link to PGP** to view the original document on the Princeton website

**Transcriptions and Translations**

When scholarly transcriptions or translations from the Princeton project are available, they appear in the version selector alongside the automatic transcription. The system automatically prefers a PGP edition (if available) over the automatic reading.

**Search by Tags**

Select **PGP Tags** from the search Mode dropdown to browse manuscripts by topic. Tags are organized into categories (Document Types, Law & Society, Medicine, Trade & Travel, and more) and displayed in a dropdown with Hebrew translations.
        ''').style('color: var(--text-secondary);')

    # === Reading Desk ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-reading-desk"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('auto_stories').classes('text-2xl text-primary')
            h2('Reading Desk', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
A side-by-side view that allows you to examine multiple manuscripts together, with synchronized images and a version selector for each fragment.

The Reading Desk is useful for any researcher who wants to compare manuscripts\u2014whether they are fragments belonging to the same PGP document (e.g., recto and verso of different leaves) or independent manuscripts you wish to examine in parallel.

**How to use:**
- Click the **Add to Reading Desk** button on the Browse Manuscript page
- Add more manuscripts from search results or from browsing other manuscripts
- Each fragment is displayed with its source image, version selector (including PGP editions if available), and extended information
- Click **Exit Reading Desk** when done
        ''').style('color: var(--text-secondary);')

    # === Browse Manuscript ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-browse"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('menu_book').classes('text-2xl text-primary')
            h2('Browse Manuscript', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This page enables convenient continuous reading of a full manuscript, synchronized with source images.

**Loading a Manuscript:** Enter a **Shelfmark** in the search box. The search is flexible and ignores spaces/punctuation (e.g., `TS NS 13 15` finds `T-S NS 13.15`).

**Features:**
- **Images:** An image viewer displays the manuscript page. You can zoom, rotate, and view in full screen
- **Page Navigation:** Use the arrows or page dropdown to navigate between pages
- **View All:** Click to display all manuscript pages in one long scrollable view
- **Find Parallels:** Send the current page text to Parallels Search
- **View on Ktiv:** Opens the manuscript in the National Library of Israel's online catalog
- **Edit & Comment:** Submit corrections or add scholarly comments for the benefit of the entire research community, or for yourself (requires login)
- **PGP Info:** If Princeton Geniza Project data is available, an information panel is displayed with transcriptions, description, tags, and dating (see [PGP Data](#help-pgp) above)
        ''').style('color: var(--text-secondary);')

    # === Lists ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-lists"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('star').classes('text-2xl text-primary')
            h2('Lists', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Save important manuscripts to personal lists for later reference.

**Creating Lists:** Click the \u2b50 star icon on any search result, parallel match, or browse page to add it to a list. Create new lists to organize your research by topic, project, or any other criteria.

**Managing Lists:**
- View all your lists in the **Lists** page
- Add notes to individual items
- Export lists to Excel or Word format
- Lists sync across devices when logged in

**Projects:** Group related lists into **Projects** for better organization. Each project can have its own color coding.
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
- **Search Results:** Export buttons above the results table
- **Parallels Results:** Export buttons in the results header
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
                ('pgp', 'מידע מפרויקט הגניזה של פרינסטון (PGP)'),
                ('reading-desk', 'שולחן קריאה (Reading Desk)'),
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
            ('וריאנטים (?, ??, ???)', 'מתחשב בחילופי אותיות נפוצים בטקסטים אלו (למשל: ד/ר, ה/ח, ו/י). ברמה הבסיסית (?) החילופים מצומצמים; ברמה המורחבת (??) מתווספים חילופים כמו ק/כ, ט/ת; ברמה המרבית (???) גמישות מירבית, איטית יותר. ניתן לשלוט גם במספר השינויים למילה (×1 מחמיר, ×2 מאוזן, ×3 מקל). בהגדרות הכלליות ניתן להחליף את בחירת הרמה מתפריט לסליידר.'),
            ('\U0001F195 פרויקט השו"ת (R)', 'חיפוש בתחביר בסגנון החיפוש המתקדם של פרויקט השו"ת של אוניברסיטת בר-אילן, עם הרחבת תחיליות/סיומות, תווים כלליים, חלופות כתיב ומרווחים. כולל גם בונה שאילתות טבלאי נוח וגמיש. מוכר למשתמשי פרויקט השו"ת; קל ללמוד גם למי שלא מכיר. ראו [חיפוש בסגנון פרויקט השו"ת](#help-responsa) להלן.'),
            ('עמום (~)', 'משתמש ב[מרחק לווינשטיין](https://he.wikipedia.org/wiki/%D7%9E%D7%A8%D7%97%D7%A7_%D7%9C%D7%95%D7%99%D7%A0%D7%A9%D7%98%D7%99%D7%99%D7%9F) למציאת מילים דומות גם עם שגיאות פענוח.'),
            ('ביטוי רגולרי (/)', 'חיפוש מתקדם למשתמשים מנוסים. דוגמה: \\bא[א-ת]{3}\\b מוצא מילים בנות 4 אותיות המתחילות באל"ף. תוכלו להיעזר במנוע הבינה המלאכותית המועדף עליכם כדי לבנות ביטוי רגולרי המתאים לצרכיכם.'),
            ('כותרת ($)', 'חיפוש בתוך כותרות הקטלוג של חיבורים.'),
            ('מספר מדף (#)', 'חיפוש מהיר של מספרי מדף (למשל: "T-S NS 13.15").'),
            ('\U0001F195 תגיות PGP', 'עיון לפי נושאים בכתבי יד שקוטלגו על ידי פרויקט הגניזה של פרינסטון (Princeton Geniza Project).'),
        ]

        with ui.column().classes('gap-3 mb-4 w-full'):
            for mode, desc in modes_data:
                with ui.row().classes('gap-2 w-full').style('direction: rtl;'):
                    ui.label(f'• {mode}:').classes('font-bold min-w-40').style('color: var(--primary-700);')
                    ui.markdown(desc).style('color: var(--text-secondary);')

    # === Responsa-Style Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-responsa"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('tune').classes('text-2xl text-primary')
            h2('חיפוש בסגנון פרויקט השו"ת', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
מצב זה מציע שתי דרכי חיפוש ברוח פרויקט השו"ת של אוניברסיטת בר-אילן: האחת בתחביר טקסטואלי עם אופרטורים לתחיליות, סיומות, כתיב מלא/חסר ועוד; והשנייה בצורת חיפוש טבלאי אינטואיטיבי, שבפועל בונה את השאילתא עבורכם.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        ui.markdown('''
**הפעלה:** בחרו **פרויקט השו"ת (R)** מתפריט מצב החיפוש, או הקלידו `R ` (R ואחריו רווח) בתחילת השאילתא.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('תחביר', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')

        syntax_data = [
            ('#מילה', 'תחיליות (24 צורות: ו, ה, ב, כ, ל, מ, ש + צירופים)', '#שלום \u2190 ושלום, השלום, בשלום...'),
            ('מילה#', 'סיומות (25 צורות: י, ו, ם, ן, ה, ך, כם, כן...)', 'שלום# \u2190 שלומם, שלומו, שלומך...'),
            ('#מילה#', 'תחיליות + סיומות יחד', '#שלום# \u2190 כל הצירופים'),
            ('*מילה', 'תו כללי לפני', '*שלום \u2190 כבשלום...'),
            ('מילה*', 'תו כללי אחרי', 'שלום* \u2190 שלומות...'),
            ('%מילה', 'כתיב מלא/חסר (הוספת/הסרת ו/י)', '%שלום \u2190 שלום, שלם'),
            ('(א/ב)', 'חלופות OR', '(שלום/שלומות)'),
            ('[N]', 'מרווח של N מילים', 'שלום [3] עולם'),
        ]

        # Render as a compact table
        with ui.element('table').classes('w-full mb-4').style('border-collapse: collapse; direction: rtl; text-align: right;'):
            with ui.element('thead'):
                with ui.element('tr'):
                    for header in ['סימן', 'משמעות', 'דוגמה']:
                        with ui.element('th').style('padding: 6px 10px; border-bottom: 2px solid var(--primary-300); color: var(--text-primary); text-align: right;'):
                            ui.label(header).classes('font-bold text-sm')
            with ui.element('tbody'):
                for syntax, meaning, example in syntax_data:
                    with ui.element('tr').style('border-bottom: 1px solid var(--border-color, #e0e0e0);'):
                        with ui.element('td').style('padding: 4px 10px; direction: ltr; text-align: left; white-space: nowrap;'):
                            ui.label(syntax).classes('font-mono font-bold text-sm').style('color: var(--primary-700);')
                        with ui.element('td').style('padding: 4px 10px; color: var(--text-secondary);'):
                            ui.label(meaning).classes('text-sm')
                        with ui.element('td').style('padding: 4px 10px; direction: ltr; text-align: left; color: var(--text-tertiary, #888);'):
                            ui.label(example).classes('text-sm font-mono')

        ui.markdown('ניתן לשלב מגדירים, למשל `#%מילה*` = תחיליות + כתיב מלא/חסר + תו כללי בסוף.', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-2')

        ui.markdown('*הערה:* לא ניתן לחפש מילה עם כוכביות משני צידיה (`*מילה*`) בגלל מגבלות מנוע החיפוש; שאילתא כזו תומר אוטומטית ל-`#מילה#` (תוספות דקדוקיות לפני ואחרי).', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('אפשרויות משנה', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
- **וריאנטים**: התאמת חלופי אותיות על כל המונחים המורחבים
- **ערבית-יהודית (JA)**: הרחבת מילים עם ה"א הידיעה הערבית אל- (8 צורות למילה)
- **ריווח גמיש**: התעלמות מרווחים שגויים בתוך מילים — שימושי מאוד בגלל ריבוי הרווחים השגויים בקריאה האוטומטית, אך מכביד על השאילתא
- **מרווח דו-כיווני**: חיפוש מונחים גם בסדר קדימה וגם בסדר הפוך
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('חיפוש טבלאי', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
1. לחצו על כפתור **חיפוש טבלאי** (גלוי כאשר מצב פרויקט השו"ת פעיל)
2. הוסיפו 2–4 **רכיבים**, כל אחד מייצג מונח חיפוש או קבוצה
3. הזינו **מילה** אחת או יותר לכל רכיב (מספר מילים = חלופות OR)
4. הפעילו **מגדירים למילה**: תחילית (#), סיומת (#), תו כללי (*), כתיב מלא/חסר (%), שלילה
5. הגדירו את ה**מרחק** בין רכיבים באמצעות הספינרים
6. צפו ב**תצוגה מקדימה חיה** המתעדכנת בזמן אמת
7. לחצו על **חפש** כדי להפעיל את החיפוש
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        ui.markdown('*הערה:* כאשר שאילתא מתרחבת מעבר ל-500 מונחים, המערכת מורידה אוטומטית אפשרויות (וריאנטים, ערבית-יהודית, כתיב וכו\') כדי לשמור על מהירות, ומציגה התראה בהתאם.', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Parallels Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-parallels"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('compare_arrows').classes('text-2xl text-primary')
            h2('חיפוש מקבילות', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
כלי זה מיועד לחוקרים המעוניינים למצוא **טקסטים מקבילים** לחיבור ספרותי שלם (כגון פיוט, פירוש מימי הביניים או יצירה נדירה אחרת) בתוך הגניזה, ובכך לאתר עדי נוסח נוספים – ישירים ועקיפים.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('איך זה עובד?', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
בניגוד לחיפוש רגיל, המנוע **לא** מחפש את הטקסט כולו כמקשה אחת. התהליך מתבצע כך:

1. **חלוקה למקטעים:** התוכנה מחלקת את טקסט המקור שלכם למקטעים קטנים בני N מילים כל אחד.
2. **חיפוש פרטני:** כל מקטע נשלח לחיפוש בנפרד במאגר הגניזה.
3. **ניקוד:** אם מקטע מסוים נמצא בכתב יד, הוא מקבל "ניקוד" על פי איכות ההתאמה.
4. **צבירה:** בסוף התהליך, התוכנה **מקבצת** את התוצאות – אם כתב יד מכיל מקטעים רבים שנמצאו, הוא יקבל ציון גבוה ויופיע בראש הרשימה.

ניתן לחפש גם במצב מעבדה, על פי אלגוריתם מבוסס על **שיטת הטביעות של שמידמן-קופל-פורת**, אשר מקודדת מילים עבריות ל"טביעות" מנורמלות המאפשרות התאמה למרות שינויי כתיב הנפוצים בכתבי יד מימי הביניים.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('פרמטרים חשובים', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
- **גודל מקטע:** מספר המילים בכל יחידת חיפוש. ערך נמוך (2–3) יגרור חיפוש איטי ותוצאות לא רלוונטיות רבות; ערך גבוה (10+) עלול להחמיץ התאמות אמיתיות.
- **מצב חיפוש:** כמו בחיפוש רגיל — מדויק, וריאנטים, או עמום.
- **רמת וריאנטים / מספר שינויים:** שליטה בגמישות חילופי האותיות (ראו מצבי חיפוש לעיל).
- **סריקה עמוקה:** רלוונטי למצב מעבדה. סריקה מעמיקה ויסודית יותר, איטית משמעותית אך מומלצת למציאת ביטויים נדירים.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('סינון מקורות ידועים', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
תכונה חזקה ומומלצת להפחתת "רעש" בתוצאות. אם טקסט המקור שלכם מצטט פסוקי תנ"ך, משנה, תלמוד או טקסטים ידועים אחרים, תוכלו **לטעון מקורות אלה** כך שהתאמות שנמצאו בהם יסוננו בנפרד.

**כיצד להשתמש:**
1. הרחיבו את הפאנל **"סינון טקסט (החרג מקורות ידועים)"**
2. לחצו על **תנ"ך**, **משנה** או **תלמוד** לטעינת מקורות סטנדרטיים מספריא
3. או לחצו על **מקורות נוספים...** לעיון בספריית ספריא המלאה
4. או לחצו על **חיפוש בספריא** לטעינת כל טקסט לפי הפניה (למשל: "רש"י על בראשית א")
5. או לחצו על **הוסף טקסט מותאם** להדבקת טקסט ייחוס משלכם

התאמות שנמצאו בטקסטי הסינון מופיעות בקטע **"תוצאות מסוננות"** נפרד, כך שתוכלו להתמקד במקבילות חדשות. הטקסטים ייטענו אוטומטית גם בחיפוש הבא, עד שתסירו אותם.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('חיפוש חוצה-פסקאות', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
כאשר מחפשים מקבילות לטקסט המכיל מעברי פסקה (למשל: פיוט עם בתים, או טקסט עם חלוקה לסעיפים), ניתן להפעיל **חיפוש חוצה-פסקאות** כדי למצוא באופן ספציפי כתבי יד ששומרים על טקסט החוצה גבולות אלה.

**למה זה שימושי?**
- טקסט **בתוך** פסקאות מכיל לעיתים קרובות ציטוטים ממקורות אחרים או מקורות שמצטטים את החיבור שאתם מחפשים
- טקסט ש**חוצה** גבולות פסקה הוא הרבה פחות סביר להיות ציטוט, מכיוון שציטוטים לעיתים רחוקות חוצים שברים מבניים
- זה מסנן ביעילות את רוב ה"רעש" ועוזר למצוא עדי נוסח אמיתיים

**כיצד להשתמש:**
1. הזינו את הטקסט עם מעברי פסקה (או הגדירו מפריד מותאם כמו נקודה או נקודתיים)
2. בחרו מצב חיפוש: **חיפוש מלא** (כל התוצאות), **חוצה-פסקאות בלבד** (רק התאמות שחוצות גבולות), או **משולב** (כל התוצאות, עם הגברת התאמות חוצות-פסקאות)
3. תוצאות שחוצות גבולות פסקה מסומנות בסימון מיוחד
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('הבנת התוצאות', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
התוצאות **מקובצות לפי כתב יד** וממוינות לפי ציון:
- **ציון מקסימלי:** ההתאמה בעלת הציון הגבוה ביותר שנמצאה בכתב היד
- **ציון ממוצע:** ממוצע הציונים של כל ההתאמות בכתב היד

לחצו על תוצאה כדי להרחיב ולראות את המקטע מהמקור שלכם לצד הטקסט המקביל מכתב יד הגניזה, עם מילים תואמות **מודגשות** להשוואה קלה.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === PGP Information ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-pgp"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('school').classes('text-2xl text-primary')
            h2('מידע מפרויקט הגניזה של פרינסטון (PGP)', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
המערכת משלבת נתונים מפרויקט הגניזה של פרינסטון (Princeton Geniza Project) — מאגר מחקרי הכולל כ-36,000 מסמכים מקוטלגים עם תעתוקים, תרגומים, תיאורים ותיוג נושאי מפורט.

**תג PGP בתוצאות חיפוש**

כתבי יד שקיים עליהם מידע מפרויקט פרינסטון מסומנים בתג ירוק "PGP" בתוצאות החיפוש, כך שתוכלו לזהות במהירות כתבי יד עם תעתוקים ומידע מחקרי נוסף.

**מידע PGP בעיון בכתב יד**

כאשר לכתב יד קיים מידע מפרויקט פרינסטון, מוצג פאנל מידע הכולל:
- **סוג מסמך** ושפות (למשל: מכתב, ערבית-יהודית)
- **תגיות נושא** — לחיצה על תגית מעבירה לחיפוש כל כתבי היד באותו נושא
- **תיאור** מחקרי באנגלית (עם אפשרות תרגום)
- **תיארוך** (כולל נימוק אם קיים)
- **קישור ל-PGP** לצפייה במסמך המקורי באתר פרינסטון

**תעתוקים ותרגומים**

כשקיימים תעתוקים או תרגומים של חוקרים מפרויקט פרינסטון, הם זמינים בבורר הגרסאות לצד התעתוק האוטומטי. המערכת מעדיפה אוטומטית תעתוק PGP (אם קיים) על פני הקריאה האוטומטית.

**חיפוש לפי תגיות**

בחרו מצב **תגיות PGP** מתפריט מצבי החיפוש כדי לעיין בכתבי יד לפי נושא. התגיות מאורגנות בקטגוריות (סוגי מסמכים, משפט וחברה, רפואה, סחר ומסעות ועוד) ומוצגות בתפריט נפתח עם תרגום לעברית.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Reading Desk ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-reading-desk"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('auto_stories').classes('text-2xl text-primary')
            h2('שולחן קריאה (Reading Desk)', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
תצוגה מקבילה המאפשרת עיון במספר כתבי יד זה לצד זה, עם תמונות מסונכרנות ובורר גרסאות לכל קטע בנפרד.

שולחן הקריאה שימושי לכל חוקר המעוניין להשוות כתבי יד — בין אם מדובר בקטעים השייכים לאותו מסמך PGP (למשל: recto ו-verso של דפים שונים) ובין אם בכתבי יד בלתי-תלויים שברצונכם לעיין בהם במקביל.

**כיצד להשתמש:**
- לחצו על כפתור **הוספה לשולחן הקריאה** בעמוד עיון בכתב יד
- הוסיפו כתבי יד נוספים מתוצאות חיפוש או מעיון בכתבי יד אחרים
- כל קטע מוצג עם תמונת המקור, בורר גרסאות (כולל תעתוקי PGP אם קיימים), ומידע מורחב
- לסיום לחצו **יציאה משולחן הקריאה**
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Browse Manuscript ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-browse"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('menu_book').classes('text-2xl text-primary')
            h2('עיון בכתב יד', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
עמוד זה מאפשר קריאה רציפה ונוחה של כתב יד שלם, בסנכרון עם תמונות המקור.

**טעינת כתב יד:** הזינו **מספר מדף** בתיבת החיפוש. החיפוש גמיש ומתעלם מרווחים/סימני פיסוק (למשל: `TS NS 13 15` מוצא `T-S NS 13.15`).

**תכונות:**
- **תמונות:** צפיין תמונות מציג את עמוד כתב היד. ניתן לעשות זום, לסובב, ולצפות במסך מלא
- **ניווט בעמודים:** השתמשו בחצים או בתפריט העמודים לניווט בין עמודים
- **הצג הכל:** לחצו להצגת כל עמודי כתב היד ברצף אחד לגלילה
- **מצא מקבילות:** שליחת טקסט העמוד הנוכחי לחיפוש מקבילות
- **צפה בכתיב:** פתיחת כתב היד בקטלוג המקוון של הספרייה הלאומית
- **עריכה והערות:** הגישו תיקונים או הוסיפו הערות מחקריות לטובת כלל קהילת החוקרים, או לעצמכם (דורש התחברות)
- **מידע PGP:** אם קיים מידע מפרויקט פרינסטון, יוצג פאנל מידע עם תעתוקים, תיאור, תגיות ותיארוך (ראו [מידע PGP](#help-pgp) לעיל)
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Lists ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-lists"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('star').classes('text-2xl text-primary')
            h2('רשימות', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
שמרו כתבי יד חשובים ברשימות אישיות להתייחסות עתידית.

**יצירת רשימות:** לחצו על סמל הכוכב ⭐ בכל תוצאת חיפוש, התאמת מקבילות או עמוד עיון כדי להוסיף לרשימה. צרו רשימות חדשות לארגון המחקר לפי נושא, פרויקט או כל קריטריון אחר.

**ניהול רשימות:**
- צפו בכל הרשימות שלכם בעמוד **רשימות**
- הוסיפו הערות לפריטים בודדים
- ייצאו רשימות לפורמט Excel או Word
- רשימות מסתנכרנות בין מכשירים כאשר מחוברים

**פרויקטים:** קבצו רשימות קשורות ל**פרויקטים** לארגון טוב יותר. לכל פרויקט יכול להיות קידוד צבע משלו.
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
- **תוצאות חיפוש:** כפתורי הייצוא מעל טבלת התוצאות
- **תוצאות מקבילות:** כפתורי הייצוא בכותרת התוצאות
- **רשימות:** ייצוא רשימות בודדות מעמוד הרשימות
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Contact ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('mail').classes('text-2xl text-primary')
            h2('משוב ויצירת קשר', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.label('לשאלות, דיווח על באגים או בקשות תכונות:').style('color: var(--text-secondary); direction: rtl; text-align: right;')
        ui.label('gershuni@gmail.com').classes('text-lg font-mono mt-2')
