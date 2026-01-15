# -*- coding: utf-8 -*-
"""
Help Center - Genizah Search Pro

Comprehensive documentation and tutorials for researchers.
"""

from nicegui import ui
from web.translations import tr


def create_help_page():
    """Create the Help Center page."""

    with ui.column().classes('w-full max-w-4xl mx-auto gap-8 fade-in'):

        # === Page Header ===
        with ui.column().classes('gap-2 mb-4'):
            ui.label(tr('Help Center')).classes('text-3xl font-bold').style('color: var(--text-primary);')
            ui.label(tr('Learn how to use Genizah Search effectively')).style('color: var(--text-secondary);')

        # === Quick Start ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('rocket_launch').classes('text-2xl').style('color: var(--primary-600);')
                ui.label(tr('Quick Start')).classes('text-xl font-bold').style('color: var(--text-primary);')

            ui.markdown('''
            ### Getting Started

            1. **Basic Search**: Enter Hebrew text in the search box and press Enter
            2. **Browse Manuscripts**: Use the Browse page to view manuscripts by shelfmark
            3. **Find Parallels**: Paste a long text to find similar passages in the Genizah

            The system supports multiple search modes - use the dropdown to select the best one for your needs.
            ''').style('color: var(--text-secondary);')

        # === Search Modes ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('search').classes('text-2xl').style('color: var(--primary-600);')
                ui.label(tr('Search Modes')).classes('text-xl font-bold').style('color: var(--text-primary);')

            with ui.column().classes('gap-4'):
                modes = [
                    ('Variants (?)', 'Standard search with basic OCR error correction. Best for most searches.',
                     'מלך → מלך, מלכ'),
                    ('Extended (??)', 'More aggressive variant generation. Use when standard search returns too few results.',
                     'מלכות → מלכות, מלכית, מלכת'),
                    ('Maximum (???)', 'Maximum variant tolerance. Use for heavily damaged or unusual texts.',
                     'Very broad matching'),
                    ('Exact (=)', 'Literal string matching with no variants. Use when you know the exact spelling.',
                     '=מלך matches only מלך'),
                    ('Fuzzy (~)', 'Levenshtein distance-based matching. Good for typos and minor variations.',
                     '~מלך matches similar words'),
                    ('Regex (/)', 'Regular expression patterns for advanced queries.',
                     '/מל[כך]+ matches מלך, מלכים, etc.'),
                    ('Shelfmark (#)', 'Search by manuscript shelfmark/call number.',
                     '#T-S 8J6.1'),
                    ('Title ($)', 'Search within manuscript titles.',
                     '$prayer book'),
                ]

                for title, desc, example in modes:
                    with ui.card().classes('p-4').style('background: var(--bg-tertiary);'):
                        ui.label(title).classes('font-bold').style('color: var(--primary-700);')
                        ui.label(desc).style('color: var(--text-secondary);')
                        with ui.row().classes('items-center gap-2 mt-2'):
                            ui.icon('code').classes('text-sm').style('color: var(--text-muted);')
                            ui.label(example).classes('font-mono text-sm').style('color: var(--text-muted);')

        # === Lab Mode ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('science').classes('text-2xl').style('color: var(--accent-blue);')
                ui.label(tr('Lab Mode')).classes('text-xl font-bold').style('color: var(--text-primary);')

            ui.markdown('''
            ### About Lab Mode

            Lab Mode uses the **Shmidman-Koppel-Porat** algorithm for detecting parallel texts.
            Instead of matching words directly, it creates "fingerprints" based on rare Hebrew letters.

            #### When to Use Lab Mode

            - **Finding parallels** in texts with many spelling variations
            - **Detecting allusions** and partial quotes
            - **Comparing manuscripts** with different orthography

            #### How It Works

            1. Each word is encoded using its 2-3 rarest letters
            2. The fingerprint is searched across the entire corpus
            3. Results are scored based on:
               - Coverage (how many query terms matched)
               - Order (are matches in the same order?)
               - Density (how close together are the matches?)

            #### Tips

            - Enable **Deep Scan** for more comprehensive results (slower)
            - Adjust **Min Score** in Settings to filter results
            - Use with **Parallels Search** for best results
            ''').style('color: var(--text-secondary);')

        # === Parallels Search ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('compare_arrows').classes('text-2xl').style('color: var(--accent-amber);')
                ui.label(tr('Parallels Search')).classes('text-xl font-bold').style('color: var(--text-primary);')

            ui.markdown('''
            ### Finding Parallel Texts

            The Parallels (Composition) Search helps you find texts in the Genizah that are similar
            to a source text you provide.

            #### How to Use

            1. **Paste your source text** (minimum 10 words)
            2. **Adjust chunk size** - how many words to search at once:
               - Smaller chunks (3-4) = more precise but slower
               - Larger chunks (6-8) = faster but may miss short parallels
            3. **Choose mode** - variants is usually best
            4. **Click Find Parallels**

            #### Understanding Results

            Results are grouped by manuscript and show:
            - **Score**: How well the text matches
            - **Source Context**: Your original text that matched
            - **Manuscript Match**: The corresponding text in the Genizah

            #### Tips

            - Start with chunk size 4-5 for most texts
            - Use the filter option to exclude known sources
            - High scores (>80) usually indicate direct parallels
            ''').style('color: var(--text-secondary);')

        # === Keyboard Shortcuts ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('keyboard').classes('text-2xl').style('color: var(--text-muted);')
                ui.label(tr('Keyboard Shortcuts')).classes('text-xl font-bold').style('color: var(--text-primary);')

            shortcuts = [
                ('/', 'Focus search input'),
                ('Ctrl + Enter', 'Execute search'),
                ('Escape', 'Close dialogs'),
                ('Arrow Left/Right', 'Navigate pages (in Browse)'),
                ('+/-', 'Zoom in/out (in Browse)'),
                ('F', 'Toggle fullscreen (in Browse)'),
            ]

            with ui.column().classes('gap-3'):
                for key, action in shortcuts:
                    with ui.row().classes('items-center gap-4'):
                        ui.label(key).classes('font-mono px-3 py-1 rounded').style(
                            'background: var(--bg-tertiary); color: var(--text-primary); min-width: 150px;'
                        )
                        ui.label(action).style('color: var(--text-secondary);')

        # === Personal Lists ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('star').classes('text-2xl').style('color: var(--accent-amber);')
                ui.label(tr('Personal Lists')).classes('text-xl font-bold').style('color: var(--text-primary);')

            ui.markdown('''
            ### Organizing Your Research

            Personal Lists help you save and organize manuscripts for your research projects.

            #### Features

            - **Create lists** with custom names and colors
            - **Add manuscripts** from search results or the browse page
            - **Add notes** to each item for your own annotations
            - **Export lists** to Excel for further analysis

            #### Tips

            - Use lists to group manuscripts by research topic
            - The star icon in search results quickly adds items to your favorites
            - Recent items are automatically tracked in the "Recently Viewed" list
            ''').style('color: var(--text-secondary);')

        # === Data Sources ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('source').classes('text-2xl').style('color: var(--text-muted);')
                ui.label(tr('Data Sources')).classes('text-xl font-bold').style('color: var(--text-primary);')

            ui.markdown('''
            ### About the Data

            **Transcriptions**: Machine-generated OCR transcriptions from the MiDRASH Project
            (Friedberg Genizah Project). Available versions:
            - **V0.8** - Latest OCR with improved accuracy
            - **V0.7** - Legacy transcriptions

            **Manuscript Images**: IIIF images from:
            - National Library of Israel (NLI)
            - Cambridge University Library
            - Oxford Bodleian Library

            **Metadata**: Library catalogs and codicological data

            #### Important Notes

            - OCR transcriptions may contain errors, especially for damaged manuscripts
            - Use variant search modes to account for OCR errors
            - Always verify readings against manuscript images when possible
            ''').style('color: var(--text-secondary);')

        # === Contact ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('feedback').classes('text-2xl').style('color: var(--primary-600);')
                ui.label(tr('Feedback')).classes('text-xl font-bold').style('color: var(--text-primary);')

            ui.markdown('''
            Have questions or suggestions? We'd love to hear from you!

            - **Report issues**: [GitHub Issues](https://github.com/anthropics/claude-code/issues)
            - **Documentation**: Check this Help Center for detailed guides

            Your feedback helps us improve Genizah Search for the research community.
            ''').style('color: var(--text-secondary);')
