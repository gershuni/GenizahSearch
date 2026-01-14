# -*- coding: utf-8 -*-
"""
Parallels (Composition Search) page for GenizahSearch web application.
Find parallel texts in the Genizah corpus with professional UI.
"""

from nicegui import ui, run
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
import asyncio
import re
import html
from collections import defaultdict

from web.services import get_service, CompositionResult
from web.translations import tr, is_rtl


# ============================================================================
# Search Mode Configuration
# ============================================================================

SEARCH_MODES = [
    ('variants', 'Variants'),
    ('variants_extended', 'Extended'),
    ('variants_maximum', 'Maximum'),
]

SORT_OPTIONS = [
    ('score', 'Sort by score'),
    ('shelfmark', 'Sort by shelfmark'),
    ('matches', 'Sort by matches'),
]


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class GroupedResult:
    """Results grouped by manuscript."""
    sys_id: str
    shelfmark: str
    title: str
    total_score: float
    match_count: int
    results: List[CompositionResult] = field(default_factory=list)
    is_expanded: bool = False


@dataclass
class ParallelsState:
    """Holds the state for the parallels search page."""
    # Input
    source_text: str = ''
    filter_text: str = ''

    # Search parameters
    mode: str = 'variants'
    chunk_size: int = 4
    max_freq: int = 100
    step_size: int = 1  # Overlap control

    # Results
    results: List[CompositionResult] = field(default_factory=list)
    grouped_results: List[GroupedResult] = field(default_factory=list)

    # UI state
    is_searching: bool = False
    is_cancelled: bool = False
    error: Optional[str] = None

    # Progress
    current_chunk: int = 0
    total_chunks: int = 0
    progress_text: str = ''

    # Filtering and sorting
    sort_by: str = 'score'
    min_score: int = 0

    # Stats
    result_count: int = 0
    manuscript_count: int = 0


# ============================================================================
# Helper Functions
# ============================================================================

def count_words(text: str) -> int:
    """Count Hebrew/Latin words in text."""
    if not text:
        return 0
    return len(re.findall(r'[\w\u0590-\u05FF]+', text))


def count_chars(text: str) -> int:
    """Count characters excluding whitespace."""
    if not text:
        return 0
    return len(text.replace(' ', '').replace('\n', '').replace('\t', ''))


def get_score_color(score: float, max_score: float) -> str:
    """Get color based on score percentage."""
    if max_score <= 0:
        return 'gray'
    ratio = score / max_score
    if ratio >= 0.7:
        return 'green'
    elif ratio >= 0.4:
        return 'amber'
    else:
        return 'red'


def get_score_badge_style(score: float, max_score: float) -> str:
    """Get badge styling based on score."""
    if max_score <= 0:
        return 'bg-gray-100 text-gray-700'
    ratio = score / max_score
    if ratio >= 0.7:
        return 'bg-green-100 text-green-800 border-green-300'
    elif ratio >= 0.4:
        return 'bg-amber-100 text-amber-800 border-amber-300'
    else:
        return 'bg-red-100 text-red-800 border-red-300'


def highlight_matched_text(text: str) -> str:
    """Convert *marked* text to HTML with highlighting.
    HTML-escapes the text first to prevent broken DOM from special chars.
    """
    if not text:
        return ''

    # First, temporarily replace asterisk markers with placeholders
    # to preserve them through HTML escaping
    placeholder_start = '\x00HIGHLIGHT_START\x00'
    placeholder_end = '\x00HIGHLIGHT_END\x00'

    # Extract markers
    text = re.sub(r'\*([^*]+)\*', placeholder_start + r'\1' + placeholder_end, text)

    # Now escape HTML entities to prevent broken DOM
    text = html.escape(text)

    # Restore markers as HTML tags
    text = text.replace(placeholder_start, '<span class="highlight-match">')
    text = text.replace(placeholder_end, '</span>')

    return text


def group_results_by_manuscript(
    results: List[CompositionResult],
    service
) -> List[GroupedResult]:
    """Group results by manuscript system ID."""
    groups: Dict[str, GroupedResult] = {}

    for result in results:
        sys_id = result.sys_id
        if not sys_id:
            sys_id = 'unknown'

        if sys_id not in groups:
            # Get metadata
            display = result.display or {}
            shelfmark = display.get('shelfmark', '') or f"ID: {sys_id}"
            title = display.get('title', '')

            groups[sys_id] = GroupedResult(
                sys_id=sys_id,
                shelfmark=shelfmark,
                title=title,
                total_score=0,
                match_count=0,
                results=[]
            )

        groups[sys_id].results.append(result)
        groups[sys_id].total_score += result.score
        groups[sys_id].match_count += 1

    return list(groups.values())


def sort_grouped_results(
    groups: List[GroupedResult],
    sort_by: str
) -> List[GroupedResult]:
    """Sort grouped results by specified criteria."""
    if sort_by == 'score':
        return sorted(groups, key=lambda g: g.total_score, reverse=True)
    elif sort_by == 'shelfmark':
        return sorted(groups, key=lambda g: g.shelfmark)
    elif sort_by == 'matches':
        return sorted(groups, key=lambda g: g.match_count, reverse=True)
    return groups


def filter_grouped_results(
    groups: List[GroupedResult],
    min_score: int
) -> List[GroupedResult]:
    """Filter groups by minimum score."""
    if min_score <= 0:
        return groups
    return [g for g in groups if g.total_score >= min_score]


# ============================================================================
# Page Styles
# ============================================================================

PARALLELS_STYLES = '''
<style>
    .parallels-input-card {
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        border-radius: 12px;
    }

    .parallels-result-card {
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.08);
        border-radius: 10px;
        border: 1px solid #e5e7eb;
        transition: all 0.2s ease;
    }

    .parallels-result-card:hover {
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.12);
        border-color: #d1d5db;
    }

    .manuscript-group-card {
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        border-radius: 8px;
        border: 1px solid #e2e8f0;
        margin-bottom: 12px;
        overflow: hidden;
    }

    .manuscript-group-header {
        background: linear-gradient(to right, #f0fdf4, #ffffff);
        padding: 12px 16px;
        cursor: pointer;
        transition: background 0.2s;
    }

    .manuscript-group-header:hover {
        background: linear-gradient(to right, #dcfce7, #f0fdf4);
    }

    .comparison-panel {
        border-radius: 8px;
        padding: 16px;
        min-height: 80px;
    }

    .source-panel {
        background-color: #eff6ff;
        border: 1px solid #bfdbfe;
    }

    .manuscript-panel {
        background-color: #f0fdf4;
        border: 1px solid #bbf7d0;
    }

    .highlight-match {
        background-color: #fef08a;
        padding: 1px 3px;
        border-radius: 3px;
        font-weight: 600;
    }

    .score-badge {
        padding: 4px 10px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.85rem;
        border: 1px solid;
    }

    .count-display {
        background-color: #f3f4f6;
        padding: 4px 12px;
        border-radius: 6px;
        font-size: 0.8rem;
        color: #6b7280;
    }

    .progress-container {
        background-color: #f9fafb;
        border-radius: 12px;
        padding: 24px;
        border: 1px solid #e5e7eb;
    }

    .param-slider-label {
        font-size: 0.85rem;
        color: #4b5563;
        font-weight: 500;
    }

    .summary-card {
        background: linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%);
        border: 1px solid #a7f3d0;
        border-radius: 10px;
        padding: 16px 24px;
    }

    .param-group {
        background-color: #f9fafb;
        border-radius: 8px;
        padding: 12px 16px;
        border: 1px solid #e5e7eb;
    }
</style>
'''


# ============================================================================
# Main Page Creator
# ============================================================================

def create_parallels_page():
    """Create the parallels search page UI."""
    state = ParallelsState()
    service = get_service()

    # UI references
    results_container = None
    progress_container = None
    word_count_label = None
    char_count_label = None
    progress_bar = None
    progress_text_label = None

    # Add custom styles
    ui.add_head_html(PARALLELS_STYLES)

    # ========================================================================
    # Event Handlers
    # ========================================================================

    def update_counts():
        """Update word and character count displays."""
        words = count_words(state.source_text)
        chars = count_chars(state.source_text)
        if word_count_label:
            word_count_label.set_text(f"{tr('Words')}: {words}")
        if char_count_label:
            char_count_label.set_text(f"{tr('Characters')}: {chars}")

    async def do_search():
        """Execute the composition search in background thread."""
        # Validate input
        words = count_words(state.source_text)
        if words < 10:
            state.error = tr('Enter at least 10 words')
            update_results()
            return

        if not service.is_ready:
            state.error = tr('Service not available')
            update_results()
            return

        # Reset state
        state.is_searching = True
        state.is_cancelled = False
        state.error = None
        state.results = []
        state.grouped_results = []
        state.current_chunk = 0
        state.total_chunks = 0
        state.progress_text = tr('Initializing search...')

        update_results()
        update_progress()

        def run_composition_search():
            """Run composition search in background thread."""
            return service.composition_search(
                full_text=state.source_text.strip(),
                mode=state.mode,
                chunk_size=state.chunk_size,
                max_freq=state.max_freq,
                filter_text=state.filter_text.strip() if state.filter_text else None,
                limit=200
            )

        try:
            # Execute search in background thread to avoid blocking UI
            results = await run.io_bound(run_composition_search)

            if state.is_cancelled:
                state.error = tr('Search cancelled')
            else:
                state.results = results

                # Group by manuscript
                state.grouped_results = group_results_by_manuscript(results, service)
                state.grouped_results = sort_grouped_results(
                    state.grouped_results, state.sort_by
                )
                state.grouped_results = filter_grouped_results(
                    state.grouped_results, state.min_score
                )

                # Update stats
                state.result_count = len(results)
                state.manuscript_count = len(state.grouped_results)
                state.error = None

        except Exception as e:
            state.error = str(e)
            state.results = []
            state.grouped_results = []
            state.result_count = 0
            state.manuscript_count = 0

        finally:
            state.is_searching = False
            update_results()

    def cancel_search():
        """Cancel the ongoing search."""
        state.is_cancelled = True
        state.progress_text = tr('Cancelling...')
        update_progress()

    def update_progress():
        """Update progress display."""
        if progress_container:
            progress_container.clear()

            if not state.is_searching:
                return

            with progress_container:
                with ui.column().classes('w-full progress-container'):
                    with ui.row().classes('w-full items-center justify-between mb-4'):
                        ui.label(tr('Searching for parallels...')).classes(
                            'font-medium text-green-800'
                        )
                        ui.button(
                            tr('Cancel'),
                            icon='close',
                            on_click=cancel_search
                        ).props('flat color=red dense')

                    # Progress bar
                    ui.linear_progress(
                        value=0.5,
                        show_value=False
                    ).props('indeterminate color=green')

                    # Status text
                    ui.label(state.progress_text).classes(
                        'text-sm text-gray-600 mt-2'
                    )

                    ui.label(tr('This may take a while for long texts...')).classes(
                        'text-xs text-gray-400 mt-1'
                    )

    def apply_filters():
        """Apply sorting and filtering to results."""
        if not state.results:
            return

        state.grouped_results = group_results_by_manuscript(state.results, service)
        state.grouped_results = sort_grouped_results(
            state.grouped_results, state.sort_by
        )
        state.grouped_results = filter_grouped_results(
            state.grouped_results, state.min_score
        )
        state.manuscript_count = len(state.grouped_results)
        update_results()

    def update_results():
        """Update the results display."""
        if not results_container:
            return

        results_container.clear()

        with results_container:
            # Show progress if searching
            if state.is_searching:
                update_progress()
                return

            # Show error
            if state.error:
                with ui.card().classes('w-full p-4 bg-red-50 border border-red-200 rounded-lg'):
                    with ui.row().classes('items-center gap-3'):
                        ui.icon('error', color='red', size='sm')
                        ui.label(state.error).classes('text-red-700')
                return

            # No results state
            if not state.results:
                if state.source_text and count_words(state.source_text) >= 10:
                    with ui.column().classes('w-full items-center py-16'):
                        ui.icon('search_off', size='4rem').classes('text-gray-300')
                        ui.label(tr('No parallels found')).classes(
                            'text-gray-500 mt-4 text-lg'
                        )
                        ui.label(tr('Try adjusting your search parameters')).classes(
                            'text-gray-400 text-sm mt-2'
                        )
                return

            # Results summary - inlined for proper context handling
            with ui.row().classes('w-full summary-card mb-6'):
                with ui.row().classes('items-center gap-6 flex-wrap'):
                    with ui.row().classes('items-center gap-2'):
                        ui.icon('check_circle', color='green', size='sm')
                        ui.label(
                            f"{state.result_count} {tr('parallels found')}"
                        ).classes('font-bold text-green-800 text-lg')

                    ui.separator().props('vertical')

                    with ui.row().classes('items-center gap-2'):
                        ui.icon('library_books', color='green', size='sm')
                        ui.label(
                            f"{tr('in')} {state.manuscript_count} {tr('manuscripts')}"
                        ).classes('text-green-700')

                    if state.grouped_results:
                        top_max_score = max(g.total_score for g in state.grouped_results)
                        ui.separator().props('vertical')
                        with ui.row().classes('items-center gap-2'):
                            ui.icon('stars', color='amber', size='sm')
                            ui.label(f"{tr('Top score')}: {top_max_score:.0f}").classes(
                                'text-gray-600'
                            )

            # Filtering controls - inlined for proper context handling
            with ui.row().classes('w-full items-center justify-between mb-4 flex-wrap gap-4'):
                with ui.row().classes('items-center gap-3'):
                    ui.label(tr('Sort by')).classes('text-sm text-gray-600')
                    ui.select(
                        {key: tr(label) for key, label in SORT_OPTIONS},
                        value=state.sort_by,
                        on_change=lambda e: (
                            setattr(state, 'sort_by', e.value),
                            apply_filters()
                        )
                    ).classes('w-40').props('outlined dense')

                with ui.row().classes('items-center gap-3'):
                    ui.label(tr('Min score')).classes('text-sm text-gray-600')
                    ui.number(
                        value=state.min_score,
                        min=0,
                        max=10000,
                        step=50,
                        on_change=lambda e: (
                            setattr(state, 'min_score', int(e.value or 0)),
                            apply_filters()
                        )
                    ).classes('w-24').props('outlined dense')

            # Results list - fully inlined for proper context handling
            max_score = max(g.total_score for g in state.grouped_results) if state.grouped_results else 1

            for group in state.grouped_results:
                score_style = get_score_badge_style(group.total_score, max_score)

                with ui.card().classes('w-full manuscript-group-card'):
                    with ui.expansion(
                        text='',
                        icon='menu_book',
                        value=False
                    ).classes('w-full').props('dense header-class=manuscript-group-header'):
                        with ui.row().classes('w-full items-center justify-between'):
                            with ui.row().classes('items-center gap-3'):
                                ui.label(group.shelfmark).classes(
                                    'font-bold text-green-800 text-lg'
                                )
                                if group.title:
                                    ui.label(f"- {group.title[:50]}{'...' if len(group.title) > 50 else ''}").classes(
                                        'text-gray-600 rtl-text hebrew-text'
                                    )

                            with ui.row().classes('items-center gap-3'):
                                ui.badge(
                                    f"{group.match_count} {tr('matches')}",
                                    color='blue'
                                ).props('outline')

                                ui.html(
                                    f'<span class="score-badge {score_style}">'
                                    f'{tr("Score")}: {group.total_score:.0f}</span>',
                                    sanitize=False
                                )

                                ui.button(
                                    tr('View'),
                                    icon='visibility',
                                    on_click=lambda g=group: ui.navigate.to(f'/browse/{g.sys_id}')
                                ).props('flat dense color=green')

                        with ui.column().classes('w-full gap-4 p-4 bg-gray-50'):
                            for i, result in enumerate(group.results[:10]):
                                score_color = get_score_color(result.score, max_score)

                                with ui.card().classes('w-full parallels-result-card p-4'):
                                    with ui.row().classes('w-full items-center justify-between mb-3'):
                                        with ui.row().classes('items-center gap-2'):
                                            ui.badge(f"#{i + 1}", color=score_color).props('outline')
                                            ui.badge(
                                                f"{tr('Score')}: {result.score:.0f}",
                                                color=score_color
                                            )

                                        ui.button(
                                            icon='open_in_new',
                                            on_click=lambda r=result: ui.navigate.to(f'/browse/{r.sys_id}')
                                        ).props('flat dense round').tooltip(tr('View manuscript'))

                                    with ui.row().classes('w-full gap-4'):
                                        with ui.column().classes('flex-1'):
                                            ui.label(tr('Your text')).classes(
                                                'text-sm font-medium text-blue-700 mb-2'
                                            )
                                            with ui.element('div').classes('comparison-panel source-panel'):
                                                if result.src_snippet:
                                                    highlighted = highlight_matched_text(result.src_snippet[:600])
                                                    ui.html(
                                                        f'<div class="rtl-text hebrew-text text-sm" '
                                                        f'style="line-height: 1.8">{highlighted}</div>',
                                                        sanitize=False
                                                    )
                                                else:
                                                    ui.label(tr('No context available')).classes(
                                                        'text-gray-400 italic text-sm'
                                                    )

                                        with ui.column().classes('flex-1'):
                                            ui.label(tr('Manuscript text')).classes(
                                                'text-sm font-medium text-green-700 mb-2'
                                            )
                                            with ui.element('div').classes('comparison-panel manuscript-panel'):
                                                if result.ms_snippet:
                                                    highlighted = highlight_matched_text(result.ms_snippet[:600])
                                                    ui.html(
                                                        f'<div class="rtl-text hebrew-text text-sm" '
                                                        f'style="line-height: 1.8">{highlighted}</div>',
                                                        sanitize=False
                                                    )
                                                else:
                                                    ui.label(tr('No text available')).classes(
                                                        'text-gray-400 italic text-sm'
                                                    )

                            if len(group.results) > 10:
                                ui.label(
                                    f"... {tr('and')} {len(group.results) - 10} {tr('more matches')}"
                                ).classes('text-gray-500 text-sm italic')

    # ========================================================================
    # Main Layout
    # ========================================================================

    with ui.column().classes('w-full max-w-6xl mx-auto p-4'):
        # Page title
        with ui.column().classes('text-center mb-6'):
            ui.label(tr('Find Parallels')).classes(
                'text-3xl font-bold text-green-800 mb-2'
            )
            ui.label(tr('Discover parallel texts in the Genizah corpus')).classes(
                'text-gray-600'
            )

        # ====================================================================
        # Input Section
        # ====================================================================

        with ui.card().classes('w-full parallels-input-card p-6 mb-6'):
            # Source text input
            with ui.column().classes('w-full gap-2'):
                with ui.row().classes('w-full items-center justify-between'):
                    ui.label(tr('Source text')).classes('font-medium text-lg')

                    # Count displays
                    with ui.row().classes('items-center gap-3'):
                        word_count_label = ui.label(f"{tr('Words')}: 0").classes(
                            'count-display'
                        )
                        char_count_label = ui.label(f"{tr('Characters')}: 0").classes(
                            'count-display'
                        )

                source_textarea = ui.textarea(
                    placeholder=tr('Paste your Hebrew text here (minimum 10 words)...'),
                    value=state.source_text
                ).classes('w-full rtl-text hebrew-text').props(
                    'outlined rows=10 autogrow'
                ).on('input', lambda: update_counts()
                ).on('keydown.ctrl.enter', do_search)  # Ctrl+Enter to search

                source_textarea.bind_value(state, 'source_text')

            # Filter text (expandable)
            with ui.expansion(
                tr('Filter text (exclude known sources)'),
                icon='filter_alt'
            ).classes('w-full mt-4'):
                ui.label(tr('Matches containing text from this field will be filtered out')).classes(
                    'text-sm text-gray-500 mb-3'
                )
                filter_textarea = ui.textarea(
                    placeholder=tr('Paste text to exclude from results...'),
                    value=state.filter_text
                ).classes('w-full rtl-text hebrew-text').props(
                    'outlined rows=4'
                )
                filter_textarea.bind_value(state, 'filter_text')

            ui.separator().classes('my-4')

            # ================================================================
            # Search Parameters
            # ================================================================

            with ui.row().classes('w-full gap-6 flex-wrap'):
                # Search mode
                with ui.column().classes('param-group'):
                    ui.label(tr('Search mode')).classes('param-slider-label mb-2')
                    mode_select = ui.select(
                        {mode: tr(label) for mode, label in SEARCH_MODES},
                        value=state.mode
                    ).classes('w-40').props('outlined dense')
                    mode_select.bind_value(state, 'mode')

                # Chunk size slider
                with ui.column().classes('param-group flex-1 min-w-48'):
                    with ui.row().classes('w-full items-center justify-between'):
                        ui.label(tr('Chunk size')).classes('param-slider-label')
                        chunk_value_label = ui.label(str(state.chunk_size)).classes(
                            'font-bold text-green-700'
                        )

                    chunk_slider = ui.slider(
                        min=2,
                        max=15,
                        step=1,
                        value=state.chunk_size
                    ).classes('w-full').props('label-always color=green')

                    def on_chunk_change(e):
                        state.chunk_size = int(e.value)
                        chunk_value_label.set_text(str(int(e.value)))

                    chunk_slider.on('update:model-value', on_chunk_change)

                    ui.label(tr('Words per search chunk')).classes(
                        'text-xs text-gray-400 mt-1'
                    )

                # Max frequency slider
                with ui.column().classes('param-group flex-1 min-w-48'):
                    with ui.row().classes('w-full items-center justify-between'):
                        ui.label(tr('Max frequency')).classes('param-slider-label')
                        freq_value_label = ui.label(str(state.max_freq)).classes(
                            'font-bold text-green-700'
                        )

                    freq_slider = ui.slider(
                        min=10,
                        max=500,
                        step=10,
                        value=state.max_freq
                    ).classes('w-full').props('label-always color=green')

                    def on_freq_change(e):
                        state.max_freq = int(e.value)
                        freq_value_label.set_text(str(int(e.value)))

                    freq_slider.on('update:model-value', on_freq_change)

                    ui.label(tr('Skip common phrases')).classes(
                        'text-xs text-gray-400 mt-1'
                    )

                # Step size (overlap)
                with ui.column().classes('param-group'):
                    ui.label(tr('Overlap')).classes('param-slider-label mb-2')
                    step_select = ui.select(
                        {
                            1: tr('Maximum (step=1)'),
                            2: tr('Medium (step=2)'),
                            4: tr('Minimal (step=4)'),
                        },
                        value=state.step_size
                    ).classes('w-44').props('outlined dense')
                    step_select.bind_value(state, 'step_size')

            # Search button
            with ui.row().classes('w-full justify-center mt-6'):
                ui.button(
                    tr('Find Parallels'),
                    icon='compare_arrows',
                    on_click=do_search
                ).props('color=green size=lg').classes('px-8')

        # ====================================================================
        # Service Status
        # ====================================================================

        if not service.is_ready:
            with ui.card().classes('w-full p-4 bg-yellow-50 border border-yellow-300 mb-4'):
                with ui.row().classes('items-center gap-3'):
                    ui.icon('warning', color='orange')
                    ui.label(tr('Service not available')).classes('text-yellow-800')
                    ui.label(
                        tr('Please ensure the search index is loaded')
                    ).classes('text-yellow-600 text-sm')

        # ====================================================================
        # Progress Container
        # ====================================================================

        progress_container = ui.column().classes('w-full')

        # ====================================================================
        # Results Container
        # ====================================================================

        results_container = ui.column().classes('w-full')
