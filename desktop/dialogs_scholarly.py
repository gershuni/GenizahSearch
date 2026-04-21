"""Scholarly reference dialogs extracted from genizah_app.py (v7.9 decomposition)."""

from PyQt6.QtWidgets import (
    QAbstractItemView, QApplication, QCheckBox, QComboBox, QDialog,
    QHBoxLayout, QHeaderView, QLabel, QLineEdit, QPushButton,
    QTableWidget, QTableWidgetItem, QTextBrowser, QVBoxLayout,
)
from PyQt6.QtCore import Qt, QUrl
from PyQt6.QtGui import QDesktopServices, QPalette

from genizah_core import CURRENT_LANG, load_app_config, tr

class FjmsBibliographyDialog(QDialog):
    """FJMS bibliography dialog with structured table."""

    def __init__(self, fjms_entries, sys_id='', shelfmark='', parent=None):
        super().__init__(parent)
        from shared.fjms_service import format_page_ref, _ts_symbol
        self.entries = fjms_entries
        self.sys_id = sys_id
        self._format_page_ref = format_page_ref
        self._ts_symbol = _ts_symbol
        self.setWindowTitle(f"{tr('Bibliography FJMS')} \u2014 {shelfmark}" if shelfmark else tr('Bibliography FJMS'))
        self.setMinimumSize(900, 500)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)

        # Filter row 1: text + type
        filter_row = QHBoxLayout()
        self.text_filter = QLineEdit()
        self.text_filter.setPlaceholderText(tr('Filter by author, title...'))
        self.text_filter.textChanged.connect(self._filter_rows)
        filter_row.addWidget(self.text_filter, 1)
        self.type_combo = QComboBox()
        for label, val in [(tr('All'), 'All'), (tr('Discussion'), 'Discussion'),
                           (tr('Mentioned'), 'Mentioned'), (tr('Index'), 'Index')]:
            self.type_combo.addItem(label, val)
        self.type_combo.currentIndexChanged.connect(lambda _: self._filter_rows())
        filter_row.addWidget(QLabel(tr('Type') + ':'))
        filter_row.addWidget(self.type_combo)
        layout.addLayout(filter_row)

        # Filter row 2: checkboxes
        check_row = QHBoxLayout()
        self.chk_transcription = QCheckBox(tr('Has Transcription'))
        self.chk_transcription.toggled.connect(self._filter_rows)
        check_row.addWidget(self.chk_transcription)
        self.chk_translation = QCheckBox(tr('Has Translation'))
        self.chk_translation.toggled.connect(self._filter_rows)
        check_row.addWidget(self.chk_translation)
        check_row.addStretch()
        layout.addLayout(check_row)

        # Table: Author, Article/Title, Year, Vol., Pages, Type, T, S
        headers = [tr('Author'), tr('Article/Title'), tr('Year'), tr('Vol.'),
                    tr('Pages'), tr('Type'), tr('col_T'), tr('col_S')]
        self.table = QTableWidget(len(fjms_entries), len(headers))
        self.table.setHorizontalHeaderLabels(headers)
        self.table.horizontalHeader().model().setHeaderData(6, Qt.Orientation.Horizontal, tr('Transcription'), Qt.ItemDataRole.ToolTipRole)
        self.table.horizontalHeader().model().setHeaderData(7, Qt.Orientation.Horizontal, tr('Translation'), Qt.ItemDataRole.ToolTipRole)
        self.table.setSortingEnabled(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        for col_idx in (6, 7):
            self.table.setColumnWidth(col_idx, 36)

        is_heb = CURRENT_LANG == 'he'
        for row, e in enumerate(fjms_entries):
            if is_heb:
                author = (e.get('article_author_heb') or e.get('article_author_eng') or '').strip()
            else:
                author = (e.get('article_author_eng') or e.get('article_author_heb') or '').strip()
            item0 = QTableWidgetItem(author)
            item0.setData(Qt.ItemDataRole.UserRole, row)
            self.table.setItem(row, 0, item0)
            article_name = (e.get('article_name') or '').strip()
            if is_heb:
                running_title = (e.get('running_title_heb') or e.get('running_title')
                                 or e.get('title_acronym_heb') or e.get('title_acronym') or '').strip()
            else:
                running_title = (e.get('running_title') or e.get('title_acronym') or '').strip()
            self.table.setItem(row, 1, QTableWidgetItem(article_name if article_name else running_title))
            year = str(e.get('title_year') or '').strip()
            self.table.setItem(row, 2, QTableWidgetItem(year if year and year != 'None' else ''))
            vol = str(e.get('volume') or '').strip()
            self.table.setItem(row, 3, QTableWidgetItem(vol if vol and vol != 'None' else ''))
            self.table.setItem(row, 4, QTableWidgetItem(format_page_ref(e)))
            mt = (e.get('mention_type') or '').strip()
            self.table.setItem(row, 5, QTableWidgetItem(tr(mt) if mt and mt != 'None' else ''))
            self.table.setItem(row, 6, QTableWidgetItem(_ts_symbol(e.get('transcription_type'))))
            self.table.setItem(row, 7, QTableWidgetItem(_ts_symbol(e.get('translation_type'))))

        self.table.resizeColumnsToContents()
        for col_idx in (6, 7):
            self.table.setColumnWidth(col_idx, 36)
        self.table.setSortingEnabled(True)
        self.table.currentCellChanged.connect(self._on_row_selected)
        layout.addWidget(self.table, 1)

        # Detail panel
        self.detail_panel = QTextBrowser()
        self.detail_panel.setMaximumHeight(80)
        self.detail_panel.setVisible(False)
        self.detail_panel.setStyleSheet("border: 1px solid #ccc; padding: 4px; font-size: 12px;")
        layout.addWidget(self.detail_panel)

        # Bottom row
        bottom_row = QHBoxLayout()
        if sys_id:
            ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{sys_id}"
            btn_ktiv = QPushButton(tr('Open in KTIV'))
            btn_ktiv.clicked.connect(lambda: QDesktopServices.openUrl(QUrl(ktiv_url)))
            bottom_row.addWidget(btn_ktiv)
        bottom_row.addStretch()
        btn_close = QPushButton(tr('Close'))
        btn_close.clicked.connect(self.close)
        bottom_row.addWidget(btn_close)
        layout.addLayout(bottom_row)

    def _filter_rows(self):
        text_val = self.text_filter.text().strip().lower()
        type_val = self.type_combo.currentData() or 'All'
        need_trans = self.chk_transcription.isChecked()
        need_transl = self.chk_translation.isChecked()
        skip_vals = ('', 'None', 'Unknown')

        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            orig_idx = item.data(Qt.ItemDataRole.UserRole) if item else -1
            if not isinstance(orig_idx, int) or orig_idx < 0 or orig_idx >= len(self.entries):
                continue
            e = self.entries[orig_idx]
            show = True
            mt = (e.get('mention_type') or '').strip()
            if type_val != 'All' and mt != type_val:
                show = False
            if show and need_trans:
                tt = (e.get('transcription_type') or '').strip()
                if not tt or tt in skip_vals:
                    show = False
            if show and need_transl:
                tl = (e.get('translation_type') or '').strip()
                if not tl or tl in skip_vals:
                    show = False
            if show and text_val:
                searchable = ' '.join([
                    e.get('article_author_eng') or '', e.get('article_author_heb') or '',
                    e.get('article_name') or '', e.get('running_title') or '',
                    e.get('running_title_heb') or '', e.get('title_acronym') or '',
                    e.get('title_acronym_heb') or '',
                ]).lower()
                if text_val not in searchable:
                    show = False
            self.table.setRowHidden(row, not show)

    @staticmethod
    def _safe(val):
        """Return stripped string or empty string for None/placeholder values."""
        s = (val or '').strip()
        return s if s and s != 'None' else ''

    def _on_row_selected(self, row, col, prev_row, prev_col):
        item = self.table.item(row, 0)
        orig_idx = item.data(Qt.ItemDataRole.UserRole) if item else -1
        if isinstance(orig_idx, int) and 0 <= orig_idx < len(self.entries):
            e = self.entries[orig_idx]
            parts = []
            article = self._safe(e.get('article_name'))
            if article:
                parts.append(f"{tr('Article')}: {article}")
            author_heb = self._safe(e.get('article_author_heb'))
            if author_heb:
                parts.append(f"{tr('Author')}: {author_heb}")
            tt = self._safe(e.get('transcription_type'))
            if tt:
                parts.append(f"{tr('Transcription')}: {tr(tt)}")
            tl = self._safe(e.get('translation_type'))
            if tl:
                parts.append(f"{tr('Translation')}: {tr(tl)}")
            cat = self._safe(e.get('catalog_acronym'))
            if cat:
                parts.append(f"{tr('Catalog')}: {cat}")
            # Extended fields
            evol = self._safe(e.get('e_volume'))
            if evol:
                parts.append(f"{tr('Vol.')} (EN): {evol}")
            jdate = self._safe(e.get('journal_date'))
            if jdate:
                parts.append(f"{tr('Date')}: {jdate}")
            cat_entry = self._safe(e.get('catalog_entry'))
            if cat_entry:
                parts.append(f"{tr('Catalog')} #: {cat_entry}")
            comment = self._safe(e.get('comment'))
            if comment:
                parts.append(f"{tr('Comment')}: {comment}")
            note = self._safe(e.get('note_for_display'))
            if note:
                parts.append(f"{tr('Note')}: {note}")
            if parts:
                self.detail_panel.setPlainText('\n'.join(parts))
                self.detail_panel.setVisible(True)
            else:
                self.detail_panel.setVisible(False)
        else:
            self.detail_panel.setVisible(False)


class FjmsCatalogDialog(QDialog):
    """Dialog showing FJMS catalog records with multi-team scholarly descriptions.

    Mirrors the FIST web interface "Cataloging Data Details" view:
    teams as columns, fields as rows, grouped into 5 labeled sections:
    1. Shelfmark Description  2. Content Description  3. Script Description
    4. Format Description  5. Miscellaneous
    """

    def __init__(self, detail: dict, sys_id: str = '', shelfmark: str = '', parent=None):
        super().__init__(parent)
        self.setWindowTitle(f'{tr("Catalog Records")} \u2014 {shelfmark}' if shelfmark else tr('Catalog Records'))
        self.setMinimumSize(800, 500)
        self.resize(900, 650)
        self._detail = detail
        self._shelfmark = shelfmark or ''
        self.sys_id = sys_id or ''
        self._cat_toggle_state = {}  # field_key -> bool (True = showing original)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)

        # Header — use palette text color so it works in dark mode
        palette = QApplication.palette()
        is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
        header_color = '#bb86fc' if is_dark else '#6c3483'
        header = QLabel(f'<h3 style="color: {header_color};">{tr("Catalog Records")} \u2014 {shelfmark}</h3>')
        layout.addWidget(header)

        # Content browser — rely on app-level RTL layout direction for Hebrew.
        # Qt's QTextBrowser inherits RTL from the application, so we use plain
        # LTR HTML (no text-align or column reversal) and let Qt handle alignment.
        self.text_browser = QTextBrowser()
        self.text_browser.setOpenExternalLinks(False)
        self.text_browser.anchorClicked.connect(self._on_anchor_clicked)
        self.text_browser.setHtml(self._build_html(detail, shelfmark=shelfmark or ''))
        layout.addWidget(self.text_browser)

        # Close button
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        close_btn = QPushButton(tr("Close"))
        close_btn.clicked.connect(self.accept)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)

    def _on_anchor_clicked(self, url):
        """Handle anchor clicks: toggle translation or open external links."""
        url_str = url.toString()
        if url_str.startswith('cat-toggle:'):
            field_key = url_str[len('cat-toggle:'):]
            self._cat_toggle_state[field_key] = not self._cat_toggle_state.get(field_key, False)
            scroll_pos = self.text_browser.verticalScrollBar().value()
            self.text_browser.setHtml(self._build_html(self._detail, shelfmark=self._shelfmark))
            self.text_browser.verticalScrollBar().setValue(scroll_pos)
        elif url_str.startswith('http'):
            from PyQt6.QtGui import QDesktopServices
            QDesktopServices.openUrl(url)

    def _build_html(self, detail: dict, shelfmark: str = '') -> str:
        """Build HTML table mirroring FIST Cataloging Data Details view."""
        from shared.fjms_service import parse_textual_frame, split_textual_frames, get_team_display_name, get_team_header_name, is_team_source, GENERIC_SOURCE_NAMES, get_catalog_source_he

        records = detail.get("records", [])
        running_titles = detail.get("running_titles", {})
        sizes = detail.get("sizes", {})
        fields = detail.get("fields", {})
        free_descriptions = detail.get("free_descriptions", [])
        full_texts = detail.get("full_texts", [])
        textual_frames = detail.get("textual_frames", {})
        mentions = detail.get("mentions", {})

        is_heb = CURRENT_LANG == 'he'
        import html as _html_esc

        # Translation service — initialized once for all sections
        _show_trans = load_app_config().get('show_translations', False)
        _trans_svc = None
        if _show_trans:
            try:
                from shared.translation_service import TranslationService
                _trans_svc = TranslationService()
                if not _trans_svc.fjms_available():
                    _trans_svc.close()
                    _trans_svc = None
            except Exception:
                _trans_svc = None  # Translation service unavailable; features degrade gracefully

        # Clickable translation toggle badge style (used in RunningTitle, FreeDesc, FullText)
        _badge_style = (
            'color: #0369a1; font-size: 10px; text-decoration: none; '
            'background: #e0f2fe; padding: 1px 4px; border-radius: 3px;'
        )

        # Dark mode detection — define color palette for HTML
        palette = QApplication.palette()
        text_color = palette.color(QPalette.ColorRole.Text).name()
        base_color = palette.color(QPalette.ColorRole.Base).name()
        is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
        c = {
            'text': text_color,
            'base': base_color,
            'muted': '#777' if is_dark else '#999',
            'border': '#444' if is_dark else '#eee',
            'section_bg': '#2d1f3d' if is_dark else '#f3e5f5',
            'section_text': '#bb86fc' if is_dark else '#6c3483',
            'label': '#aaa' if is_dark else '#555',
            'header_border': '#9b59b6',
            'full_text_bg': '#2a2a2a' if is_dark else '#fafafa',
            'author_muted': '#888' if is_dark else 'gray',
        }
        # Store for use in helper methods
        self._colors = c

        # Group records by source_name to get team columns, skipping generic sources
        teams = []
        team_map = {}
        for rec in records:
            sn = rec.get("source_name") or tr("Unknown")
            if sn in GENERIC_SOURCE_NAMES:
                continue
            if sn not in team_map:
                team_map[sn] = len(teams)
                teams.append({
                    "source_name": sn,
                    "source_name_heb": rec.get("source_name_heb") or sn,
                    "records": [],
                })
            teams[team_map[sn]]["records"].append(rec)

        num_teams = len(teams)
        total_cols = num_teams + 1  # label column + team columns

        if num_teams == 0 and not free_descriptions and not full_texts:
            return f'<p style="color: {c["muted"]};">No catalog data available</p>'

        # Calculate column widths for table-layout:fixed
        label_width = 130
        team_col_width = max(150, (700 - label_width) // max(num_teams, 1)) if num_teams > 0 else 150

        html_parts = []
        html_parts.append(
            f'<table style="width:100%; border-collapse:collapse; table-layout:fixed; '
            f'font-family:Arial; font-size:13px; color:{c["text"]};">'
        )
        # Column width definitions — RTL: reverse column order (team cols first,
        # label last) so Hebrew readers see labels on the right.  Qt handles
        # text alignment within cells automatically via app-level RTL.
        if num_teams > 0:
            html_parts.append('<colgroup>')
            if is_heb:
                for _ in teams:
                    html_parts.append(f'<col style="width:{team_col_width}px;"/>')
                html_parts.append(f'<col style="width:{label_width}px;"/>')
            else:
                html_parts.append(f'<col style="width:{label_width}px;"/>')
                for _ in teams:
                    html_parts.append(f'<col style="width:{team_col_width}px;"/>')
            html_parts.append('</colgroup>')

        if num_teams > 0:
            # === Team header row ===
            team_ths = []
            for team in teams:
                header_name = get_team_header_name(team["source_name"], is_heb=is_heb)
                # For non-team sources (e.g. FJMS site users), use Hebrew name when available
                if is_heb and header_name == team["source_name"] and team.get("source_name_heb"):
                    header_name = team["source_name_heb"]
                team_ths.append(
                    f'<th style="padding:8px; border-bottom:2px solid {c["header_border"]}; '
                    f'overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="{header_name}">'
                    f'{header_name}</th>'
                )
            empty_th = f'<th style="padding:8px;"></th>'
            html_parts.append('<tr>')
            if is_heb:
                html_parts.extend(team_ths)
                html_parts.append(empty_th)
            else:
                html_parts.append(empty_th)
                html_parts.extend(team_ths)
            html_parts.append('</tr>')

            # === Section 1: Shelfmark Description ===
            html_parts.append(self._section_row(tr('Shelfmark Description'), total_cols))

            # Shelfmark
            if shelfmark:
                sm_vals = [shelfmark] * num_teams
                html_parts.append(self._field_row(tr('Shelfmark'), sm_vals, is_heb))

            # Source — "{Author}, Head of {Team}" for teams, raw name for catalogs
            source_vals = []
            for team in teams:
                sn = team["source_name"]
                first_rec = team["records"][0] if team["records"] else None
                author = ""
                if first_rec:
                    a = first_rec.get("author_text")
                    if a and str(a).strip():
                        author = str(a).strip()
                if is_team_source(sn) and author:
                    header = get_team_header_name(sn, is_heb=is_heb)
                    if is_heb:
                        source_vals.append(f"{author}, ראש {header}")
                    else:
                        source_vals.append(f"{author}, Head of {header}")
                elif is_team_source(sn):
                    source_vals.append(get_team_display_name(sn, is_heb=is_heb))
                else:
                    if is_heb:
                        # Priority: catalog mapping > DB Hebrew > English fallback
                        sn_mapped = get_catalog_source_he(sn)
                        if sn_mapped != sn:
                            sn_display = sn_mapped
                        else:
                            db_heb = team.get("source_name_heb")
                            sn_display = db_heb if (db_heb and db_heb != sn) else sn
                    else:
                        sn_display = sn
                    source_vals.append(sn_display or sn)
            html_parts.append(self._field_row(tr('Source'), source_vals, is_heb))

            # Number of Folios
            folio_vals = []
            for team in teams:
                folios = [self._fmt_int(r.get("num_folio")) for r in team["records"]
                          if r.get("num_folio") and str(r["num_folio"]).strip() and str(r["num_folio"]).strip() != '0']
                folios = self._dedup_preserve_order(folios)
                folio_vals.append(', '.join(folios) if folios else '')
            html_parts.append(self._field_row(tr('Number of Folios'), folio_vals, is_heb))

            # Number of Bifolios
            bifolio_vals = []
            for team in teams:
                bifolios = [self._fmt_int(r.get("num_bifolio")) for r in team["records"]
                            if r.get("num_bifolio") and str(r["num_bifolio"]).strip() and str(r["num_bifolio"]).strip() != '0']
                bifolios = self._dedup_preserve_order(bifolios)
                bifolio_vals.append(', '.join(bifolios) if bifolios else '')
            html_parts.append(self._field_row(tr('Number of Bifolios'), bifolio_vals, is_heb))

            # === Section 2: Content Description ===
            html_parts.append(self._section_row(tr('Content Description'), total_cols))

            # Domain
            domain_vals = []
            for team in teams:
                categories = []
                for rec in team["records"]:
                    tf_eng = rec.get("textual_frame_eng") or ""
                    tf_heb = rec.get("textual_frame_heb") or ""
                    tf = tf_heb if is_heb and tf_heb else tf_eng
                    if tf:
                        parts = split_textual_frames(tf)
                        if not parts and tf.strip():
                            parts = [tf.strip()]
                        for part in parts:
                            cat, content = parse_textual_frame(part)
                            display_parts = []
                            if cat:
                                display_parts.append(f"[{cat}]")
                            if content:
                                display_parts.append(content)
                            if display_parts:
                                categories.append(' '.join(display_parts))
                categories = self._dedup_preserve_order(categories)
                domain_vals.append('; '.join(categories) if categories else '')
            html_parts.append(self._field_row(tr('Domain'), domain_vals, is_heb))

            # Running Title (with clickable translation toggle)
            # Collect all UnitCatalogRecIds to batch-fetch translations
            _rt_trans_map = {}
            if _trans_svc:
                _all_rt_rec_ids = []
                for team in teams:
                    for rec in team["records"]:
                        rec_id = rec.get("unit_catalog_rec_id")
                        if rec_id and rec_id in running_titles:
                            _all_rt_rec_ids.append(rec_id)
                if _all_rt_rec_ids:
                    _rt_trans_map = _trans_svc.get_fjms_translations_by_signature_ids(
                        'RunningTitle', list(set(_all_rt_rec_ids))
                    )

            rt_vals = []
            for team in teams:
                titles = []
                seen_rt_origs = set()
                for rec in team["records"]:
                    rec_id = rec.get("unit_catalog_rec_id")
                    if rec_id and rec_id in running_titles:
                        for rt in running_titles[rec_id]:
                            rt_text = rt.get("running_title", "")
                            if rt_text and str(rt_text).strip():
                                orig = str(rt_text).strip()
                                if orig in seen_rt_origs:
                                    continue
                                seen_rt_origs.add(orig)
                                _trans_entry = _rt_trans_map.get(rec_id) if _rt_trans_map else None
                                trans = ''
                                _trans_dir = None
                                if _trans_entry and isinstance(_trans_entry, tuple):
                                    trans = str(_trans_entry[0]).strip()
                                    _trans_dir = _trans_entry[1]
                                elif _trans_entry:
                                    trans = str(_trans_entry).strip()
                                _should_swap = bool(trans and trans != orig)
                                toggle_key = f'rt_{rec_id}'
                                toggled = self._cat_toggle_state.get(toggle_key, False)
                                if _should_swap:
                                    # Direction-aware default: show user's UI language by default
                                    # en2he: trans=Hebrew, orig=English → EN UI default=orig, HE UI default=trans
                                    # he2en: trans=English, orig=Hebrew → EN UI default=trans, HE UI default=orig
                                    _show_trans_default = (is_heb if _trans_dir == 'en2he' else not is_heb)
                                    if _show_trans_default:
                                        # Original lang ≠ UI lang → show translation with badge
                                        show_text = _html_esc.escape(orig if toggled else trans)
                                        badge_label = tr('Translated') if toggled else tr('Original')
                                        titles.append(
                                            f'{show_text} '
                                            f'<a href="cat-toggle:{toggle_key}" style="{_badge_style}">{badge_label}</a>'
                                        )
                                    else:
                                        # Original lang matches UI lang → no badge needed
                                        titles.append(_html_esc.escape(orig))
                                else:
                                    titles.append(_html_esc.escape(orig))
                rt_vals.append('; '.join(titles) if titles else '')
            html_parts.append(self._field_row(tr('Running Title'), rt_vals, is_heb))

            # Detailed Content (from catalog_textual_frames)
            if textual_frames:
                # Batch-fetch TextualFrame translations (he2en) by text content
                _tf_trans_map = {}  # {original_heb_text: (english, direction)}
                if _trans_svc and not is_heb:
                    _tf_text_lookup = _trans_svc.get_fjms_translations_by_text(
                        'TextualFrame', [self.sys_id]
                    )
                    _tf_trans_map = _tf_text_lookup.get(self.sys_id, {})

                dc_vals = []
                for team in teams:
                    frames = []
                    seen_tf_origs = set()
                    for rec in team["records"]:
                        rec_id = rec.get("unit_catalog_rec_id")
                        if rec_id and rec_id in textual_frames:
                            for tf_idx, tf in enumerate(textual_frames[rec_id]):
                                heb_text = tf.get("heb")
                                eng_text = tf.get("eng")
                                if is_heb:
                                    text = heb_text if heb_text else eng_text
                                else:
                                    text = eng_text if eng_text else heb_text
                                if not text or not str(text).strip():
                                    continue
                                orig = str(text).strip()
                                if orig in seen_tf_origs:
                                    continue
                                seen_tf_origs.add(orig)
                                # In EN UI, try to find he2en translation for Hebrew text
                                _tf_entry = _tf_trans_map.get(str(heb_text).strip()) if heb_text and _tf_trans_map else None
                                if _tf_entry and not is_heb:
                                    trans_text, _tf_dir = _tf_entry
                                    if trans_text and trans_text != orig:
                                        toggle_key = f'tf_{rec_id}_{tf_idx}'
                                        toggled = self._cat_toggle_state.get(toggle_key, False)
                                        show_text = _html_esc.escape(orig if toggled else trans_text)
                                        badge_label = tr('Translated') if toggled else tr('Original')
                                        frames.append(
                                            f'{show_text} '
                                            f'<a href="cat-toggle:{toggle_key}" style="{_badge_style}">{badge_label}</a>'
                                        )
                                        continue
                                frames.append(_html_esc.escape(orig))
                    dc_vals.append('; '.join(frames) if frames else '')
                html_parts.append(self._field_row(tr('Detailed Content'), dc_vals, is_heb))

            # GenizahTitle (with translation support)
            # Batch-fetch Title translations (he2en) by alma_id
            _title_trans_map = {}  # {original_text: (english, direction)}
            if _trans_svc and not is_heb:
                _title_text_lookup = _trans_svc.get_fjms_translations_by_text(
                    'Title', [self.sys_id]
                )
                _title_trans_map = _title_text_lookup.get(self.sys_id, {})

            gt_vals = []
            for team in teams:
                titles = []
                seen_gt_origs = set()
                for rec in team["records"]:
                    gt_org = rec.get("genizah_title_org")
                    gt_eng = rec.get("genizah_title_eng")
                    if is_heb:
                        gt = gt_org if gt_org and str(gt_org).strip() else gt_eng
                    else:
                        gt = gt_eng if gt_eng and str(gt_eng).strip() else gt_org
                    if gt and str(gt).strip():
                        orig = str(gt).strip()
                        if orig in seen_gt_origs:
                            continue
                        seen_gt_origs.add(orig)
                        # In EN UI with Hebrew title (no gt_eng), try he2en translation
                        _gt_entry = None
                        if not is_heb and gt_org and not (gt_eng and str(gt_eng).strip()):
                            _gt_entry = _title_trans_map.get(str(gt_org).strip()) if _title_trans_map else None
                        if _gt_entry:
                            trans_text, _gt_dir = _gt_entry
                            if trans_text and trans_text != orig:
                                toggle_key = f'gt_{rec.get("unit_catalog_rec_id", id(rec))}'
                                toggled = self._cat_toggle_state.get(toggle_key, False)
                                show_text = _html_esc.escape(orig if toggled else trans_text)
                                badge_label = tr('Translated') if toggled else tr('Original')
                                titles.append(
                                    f'{show_text} '
                                    f'<a href="cat-toggle:{toggle_key}" style="{_badge_style}">{badge_label}</a>'
                                )
                                continue
                        titles.append(_html_esc.escape(orig))
                gt_vals.append('; '.join(titles) if titles else '')
            html_parts.append(self._field_row(tr('Title'), gt_vals, is_heb))

            # === Section 3: Mentions ===
            if mentions:
                html_parts.append(self._section_row(tr('Mentions'), total_cols))
                mention_types_ordered = ['Personalities', 'Places', 'Creations', 'Dates', 'Groups']
                all_types = set()
                for rec_id, items in mentions.items():
                    for item in items:
                        mt = item.get("mention_type")
                        if mt:
                            all_types.add(mt)
                extra_types = sorted(all_types - set(mention_types_ordered))
                type_order = [t for t in mention_types_ordered if t in all_types] + extra_types
                for mention_type in type_order:
                    mn_vals = []
                    for team in teams:
                        names = []
                        for rec in team["records"]:
                            rec_id = rec.get("unit_catalog_rec_id")
                            if rec_id and rec_id in mentions:
                                for m in mentions[rec_id]:
                                    if m.get("mention_type") == mention_type:
                                        name = m.get("mention", "")
                                        if name and str(name).strip():
                                            names.append(str(name).strip())
                        names = self._dedup_preserve_order(names)
                        mn_vals.append(', '.join(names) if names else '')
                    html_parts.append(self._field_row(tr(mention_type), mn_vals, is_heb))

            # === Section 4: Script Description ===
            html_parts.append(self._section_row(tr('Script Description'), total_cols))

            html_parts.append(self._field_category_row('GenizahLanguages', tr('Language'), teams, fields, is_heb))
            html_parts.append(self._field_category_row('TypeOfScript', tr('Script Type'), teams, fields, is_heb))
            html_parts.append(self._field_category_row('TypeOfScriptStyle', tr('Script Style'), teams, fields, is_heb))
            html_parts.append(self._field_category_row('TypeOfScriptPlace', tr('Script Place'), teams, fields, is_heb))
            html_parts.append(self._field_category_row('TypeOfVocalization', tr('Vocalization'), teams, fields, is_heb))

            # === Section 5: Format Description ===
            html_parts.append(self._section_row(tr('Format Description'), total_cols))

            # No. of Rows
            row_vals = []
            for team in teams:
                rows = [str(r.get("num_row", "")).strip() for r in team["records"]
                        if r.get("num_row") and str(r["num_row"]).strip() and str(r["num_row"]).strip() != '0']
                rows = self._dedup_preserve_order(rows)
                row_vals.append(', '.join(rows) if rows else '')
            html_parts.append(self._field_row(tr('Number of Lines'), row_vals, is_heb))

            # No. of Columns
            col_vals = []
            for team in teams:
                cols = [str(r.get("num_column", "")).strip() for r in team["records"]
                        if r.get("num_column") and str(r["num_column"]).strip() and str(r["num_column"]).strip() != '0']
                cols = self._dedup_preserve_order(cols)
                col_vals.append(', '.join(cols) if cols else '')
            html_parts.append(self._field_row(tr('Number of Columns'), col_vals, is_heb))

            # Material
            html_parts.append(self._field_category_row('FragmentMaterial', tr('Material'), teams, fields, is_heb))

            # Physical Status
            html_parts.append(self._field_category_row('FragmentStatus', tr('Physical Status'), teams, fields, is_heb))

            # Sizes
            size_vals = []
            for team in teams:
                size_parts = []
                for rec in team["records"]:
                    rec_id = rec.get("unit_catalog_rec_id")
                    if rec_id and rec_id in sizes:
                        for sz in sizes[rec_id]:
                            sx = sz.get("size_x")
                            sy = sz.get("size_y")
                            isx = sz.get("inner_size_x")
                            isy = sz.get("inner_size_y")
                            if sx and sy:
                                dim = f"{self._fmt_num(sx)} \u00d7 {self._fmt_num(sy)}"
                                if isx and isy:
                                    dim += f" ({tr('Inner Size')}: {self._fmt_num(isx)} \u00d7 {self._fmt_num(isy)})"
                                dim += " mm"
                                size_parts.append(dim)
                size_parts = self._dedup_preserve_order(size_parts)
                size_vals.append('; '.join(size_parts) if size_parts else '')
            html_parts.append(self._field_row(tr('Size'), size_vals, is_heb))

        # === Section 6: Miscellaneous ===
        if free_descriptions or full_texts:
            html_parts.append(self._section_row(tr('Miscellaneous'), total_cols if num_teams > 0 else 2))

            col_span = total_cols if num_teams > 0 else 2
            # Fetch FJMS translations for free descriptions (by signature_id)
            _fd_trans_map = {}
            if _trans_svc:
                _fd_sig_ids = [d.get('signature_id') for d in free_descriptions if d.get('signature_id')]
                if _fd_sig_ids:
                    _fd_trans_map = _trans_svc.get_fjms_translations_by_signature_ids(
                        'FreeDesc', _fd_sig_ids
                    )

            for fd_idx, desc in enumerate(free_descriptions):
                text = desc.get("text", "")
                if text and str(text).strip():
                    eng_source = desc.get("source_name")
                    if eng_source and eng_source not in ('Instatution', 'Institution'):
                        if is_team_source(eng_source):
                            source = get_team_display_name(eng_source, is_heb=is_heb)
                        elif is_heb:
                            mapped = get_catalog_source_he(eng_source)
                            if mapped != eng_source:
                                source = mapped
                            else:
                                db_heb = desc.get("source_name_heb")
                                source = db_heb if (db_heb and db_heb != eng_source) else eng_source
                        else:
                            source = eng_source
                    else:
                        source = None
                    source_html = f'<div style="font-weight:bold; font-size:11px; color:{c["section_text"]}; margin-bottom:2px;">{source}</div>' if source else ''

                    sig_id = desc.get('signature_id')
                    _fd_entry = _fd_trans_map.get(sig_id) if sig_id else None
                    trans_text = None
                    _fd_dir = None
                    if _fd_entry and isinstance(_fd_entry, tuple):
                        trans_text = _fd_entry[0]
                        _fd_dir = _fd_entry[1]
                    elif _fd_entry:
                        trans_text = _fd_entry
                    orig = str(text).strip()
                    _trans_differs = trans_text and str(trans_text).strip() != orig
                    _should_swap = bool(_trans_differs)
                    toggle_key = f'fd_{sig_id or fd_idx}'
                    toggled = self._cat_toggle_state.get(toggle_key, False)

                    if _should_swap:
                        trans = str(trans_text).strip()
                        _show_trans_default = (is_heb if _fd_dir == 'en2he' else not is_heb)
                        if _show_trans_default:
                            # Original lang ≠ UI lang → show translation with badge
                            show_text = _html_esc.escape(orig if toggled else trans)
                            badge_label = tr('Translated') if toggled else tr('Original')
                            display = (
                                f'{show_text} '
                                f'<a href="cat-toggle:{toggle_key}" style="{_badge_style}">{badge_label}</a>'
                            )
                        else:
                            # Original lang matches UI lang → no badge needed
                            display = _html_esc.escape(orig)
                    else:
                        display = _html_esc.escape(orig)

                    html_parts.append(
                        f'<tr><td colspan="{col_span}" '
                        f'style="padding:8px; border-bottom:1px solid {c["border"]};"'
                        f'>{source_html}{display}</td></tr>'
                    )

            # Full texts (scholarly descriptions) with distinct styling
            if full_texts:
                # Batch-fetch FullText translations by rowid
                _ft_trans_map = {}
                if _trans_svc:
                    _ft_rowids = [ft.get("rowid") for ft in full_texts if ft.get("rowid")]
                    if _ft_rowids:
                        _ft_trans_map = _trans_svc.get_fjms_translations_by_signature_ids(
                            'FullText', _ft_rowids
                        )

                html_parts.append(
                    f'<tr><td colspan="{col_span}" style="padding:6px 8px; font-weight:bold; '
                    f'color:{c["section_text"]}; font-size:12px;">{tr("Scholarly Description")}</td></tr>'
                )
                for ft_idx, ft in enumerate(full_texts):
                    text = ft.get("text", "")
                    if text and str(text).strip():
                        ft_rowid = ft.get("rowid")
                        orig = str(text).strip()
                        _ft_entry = _ft_trans_map.get(ft_rowid) if ft_rowid and _ft_trans_map else None
                        trans = ''
                        _ft_dir = None
                        if _ft_entry and isinstance(_ft_entry, tuple):
                            trans = str(_ft_entry[0]).strip()
                            _ft_dir = _ft_entry[1]
                        elif _ft_entry:
                            trans = str(_ft_entry).strip()
                        _should_swap = bool(trans and trans != orig)
                        toggle_key = f'ft_{ft_rowid or ft_idx}'
                        toggled = self._cat_toggle_state.get(toggle_key, False)

                        if _should_swap:
                            _show_trans_default = (is_heb if _ft_dir == 'en2he' else not is_heb)
                            if _show_trans_default:
                                # Original lang ≠ UI lang → show translation with badge
                                show_text = _html_esc.escape(orig if toggled else trans)
                                badge_label = tr('Translated') if toggled else tr('Original')
                                badge = (
                                    f' <a href="cat-toggle:{toggle_key}" style="{_badge_style}">{badge_label}</a>'
                                )
                            else:
                                # Original lang matches UI lang → no badge needed
                                show_text = _html_esc.escape(orig)
                                badge = ''
                        else:
                            show_text = _html_esc.escape(orig)
                            badge = ''
                        html_parts.append(
                            f'<tr><td colspan="{col_span}" '
                            f'style="padding:8px; border-bottom:1px solid {c["border"]}; background:{c["full_text_bg"]};"'
                            f'>{show_text}{badge}</td></tr>'
                        )

        html_parts.append('</table>')

        if _trans_svc:
            _trans_svc.close()

        return '\n'.join(html_parts)

    def _section_row(self, title: str, colspan: int) -> str:
        """Build a section header row."""
        c = self._colors
        return (
            f'<tr><td colspan="{colspan}" style="background:{c["section_bg"]}; font-weight:bold; '
            f'padding:8px; color:{c["section_text"]}; font-size:13px;">{title}</td></tr>'
        )

    @staticmethod
    def _dedup_preserve_order(values):
        """Return a list with consecutive/global string duplicates removed,
        preserving first-occurrence order. Empty/whitespace values are
        dropped. Used so that per-record loops that emit the same value
        N times (e.g. 5 Uri Ehrlich records all carrying NumFolio=6 /
        Material='קלף' / 'Qiddus, Psalms and tahanun.') render as one
        value, not a ``6, 6, 6, 6, 6`` visual repeat.
        """
        seen = set()
        out = []
        for v in values:
            if v is None:
                continue
            sv = str(v).strip()
            if not sv:
                continue
            if sv in seen:
                continue
            seen.add(sv)
            out.append(sv)
        return out

    def _field_row(self, label: str, values: list, is_heb: bool) -> str:
        """Build a field row: label + value columns. RTL: values first, label last.
        Qt handles text alignment via app-level layout direction. Returns '' if all values empty."""
        if not any(v for v in values):
            return ''
        c = self._colors
        label_cell = f'<td style="padding:6px 8px; font-weight:bold; color:{c["label"]}; vertical-align:top; word-wrap:break-word; overflow-wrap:break-word;">{label}</td>'
        value_cells = []
        for val in values:
            display = str(val).strip() if val else '\u2014'
            style = f'padding:6px 8px; border-bottom:1px solid {c["border"]}; vertical-align:top; word-wrap:break-word; overflow-wrap:break-word;'
            if not val:
                style += f' color:{c["muted"]};'
            value_cells.append(f'<td style="{style}">{display}</td>')
        if is_heb:
            return '<tr>' + ''.join(value_cells) + label_cell + '</tr>'
        return '<tr>' + label_cell + ''.join(value_cells) + '</tr>'

    def _field_category_row(self, category: str, label: str, teams: list, fields: dict, is_heb: bool) -> str:
        """Build a row for a specific FieldCategory from catalog_fields."""
        vals = []
        for team in teams:
            field_vals = []
            for rec in team["records"]:
                rec_id = rec.get("unit_catalog_rec_id")
                if rec_id and rec_id in fields:
                    cat_fields = fields[rec_id].get(category, [])
                    for fv in cat_fields:
                        val = fv.get("value_heb") if is_heb else fv.get("value")
                        if not val or not str(val).strip():
                            val = fv.get("value") or fv.get("value_heb")
                        if val and str(val).strip():
                            field_vals.append(str(val).strip())
            field_vals = self._dedup_preserve_order(field_vals)
            vals.append('; '.join(field_vals) if field_vals else '')
        return self._field_row(label, vals, is_heb)

    @staticmethod
    def _fmt_num(val) -> str:
        """Format a numeric value for size display, removing trailing .0."""
        if val is None:
            return ""
        s = str(val)
        if s.endswith('.0'):
            return s[:-2]
        return s

    @staticmethod
    def _fmt_int(val) -> str:
        """Format a numeric value as integer (2.0 → '2')."""
        if val is None:
            return ""
        s = str(val).strip()
        if s.endswith('.0'):
            return s[:-2]
        return s


class FjmsMeasurementsDialog(QDialog):
    """Dialog showing physical measurements for a manuscript.

    Displays catalog dimensions, computed per-image measurements (page size,
    margins, line count, text density), and blank image fragment dimensions.
    """

    def __init__(self, data: dict, sys_id: str = '', shelfmark: str = '', parent=None, image_side: str = None):
        import html as html_module
        super().__init__(parent)
        self._image_side = image_side
        self.setWindowTitle(f'{tr("Measurements")} \u2014 {shelfmark}' if shelfmark else tr('Measurements'))
        self.setMinimumSize(700, 450)
        self.resize(800, 600)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)

        # Header with teal color (dark mode aware)
        palette = QApplication.palette()
        is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
        header_color = '#4db6ac' if is_dark else '#00695c'
        header = QLabel(f'<h3 style="color: {header_color};">\U0001f4cf {tr("Measurements")} \u2014 {html_module.escape(shelfmark)}</h3>')
        layout.addWidget(header)

        # Content browser
        browser = QTextBrowser()
        browser.setOpenExternalLinks(False)
        layout.addWidget(browser)

        html_content = self._build_html(data, is_dark)
        browser.setHtml(html_content)

        # Close button
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        close_btn = QPushButton(tr("Close"))
        close_btn.clicked.connect(self.accept)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)

    def _build_html(self, data: dict, is_dark: bool) -> str:
        """Build HTML content for the measurements dialog."""
        import html as html_module

        text_color = '#e0e0e0' if is_dark else '#333333'
        border_color = '#555' if is_dark else '#ddd'
        header_bg = '#1a3a3a' if is_dark else '#e0f2f1'
        header_fg = '#4db6ac' if is_dark else '#00695c'
        alt_bg = '#1e1e1e' if is_dark else '#fafafa'

        css = f"""
        <style>
        body {{ color: {text_color}; font-size: 13px; }}
        h4 {{ color: {header_fg}; margin: 16px 0 8px 0; padding: 6px 10px;
              background: {header_bg}; border-radius: 4px; }}
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 12px; }}
        th, td {{ border: 1px solid {border_color}; padding: 5px 8px; text-align: left; }}
        th {{ background: {header_bg}; color: {header_fg}; font-weight: bold; }}
        tr:nth-child(even) {{ background: {alt_bg}; }}
        .dim {{ font-weight: 600; }}
        .muted {{ color: {'#888' if is_dark else '#999'}; font-style: italic; }}
        </style>
        """

        summary = data.get("summary")
        catalog_sizes = data.get("catalog_sizes", [])
        computed = data.get("computed", [])
        extra_info = data.get("extra_info", [])
        blank_images = data.get("blank_images", [])

        # Filter per-image computed data to current side when specified
        # Note: blank_images don't have Image_Side in the DB, so don't filter them
        if self._image_side and computed:
            side_lower = self._image_side.lower()
            computed = [r for r in computed if side_lower in str(r.get("Image_Side", "")).lower()]

        parts = [css]

        if not summary and not catalog_sizes and not computed and not blank_images:
            parts.append(f'<p class="muted">{html_module.escape(tr("No measurement data available"))}</p>')
            return ''.join(parts)

        # Section 1: Summary
        if summary:
            parts.append(f'<h4>\U0001f4cf {html_module.escape(tr("Physical Measurements"))}</h4>')
            parts.append('<table>')

            cw = summary.get("catalog_width_cm")
            ch = summary.get("catalog_height_cm")
            if cw is not None and ch is not None:
                parts.append(f'<tr><th>{html_module.escape(tr("Catalog Dimensions"))}</th>'
                             f'<td class="dim">{float(cw):.1f} \u00d7 {float(ch):.1f} cm</td></tr>')

            ciw = summary.get("catalog_inner_width_cm")
            cih = summary.get("catalog_inner_height_cm")
            if ciw is not None and cih is not None:
                parts.append(f'<tr><th>{html_module.escape(tr("Catalog Dimensions"))} ({html_module.escape(tr("Inner"))})</th>'
                             f'<td class="dim">{float(ciw):.1f} \u00d7 {float(cih):.1f} cm</td></tr>')

            min_w = summary.get("min_computed_width_cm")
            max_w = summary.get("max_computed_width_cm")
            min_h = summary.get("min_computed_height_cm")
            max_h = summary.get("max_computed_height_cm")
            if min_w is not None and max_w is not None:
                if abs(float(min_w) - float(max_w)) < 0.05 and abs(float(min_h) - float(max_h)) < 0.05:
                    range_str = f'{float(min_w):.1f} \u00d7 {float(min_h):.1f} cm'
                else:
                    range_str = (f'{tr("Width")}: {float(min_w):.1f}-{float(max_w):.1f} cm, '
                                 f'{tr("Height")}: {float(min_h):.1f}-{float(max_h):.1f} cm')
                parts.append(f'<tr><th>{html_module.escape(tr("Computed Dimensions"))}</th>'
                             f'<td class="dim">{html_module.escape(range_str)}</td></tr>')

            material = summary.get("material")
            if material:
                parts.append(f'<tr><th>{html_module.escape(tr("Material"))}</th>'
                             f'<td>{html_module.escape(tr(str(material)))}</td></tr>')

            size_cat = summary.get("size_category")
            if size_cat:
                parts.append(f'<tr><th>{html_module.escape(tr("Size Category"))}</th>'
                             f'<td>{html_module.escape(str(size_cat))}</td></tr>')

            min_lines = summary.get("min_num_lines")
            max_lines = summary.get("max_num_lines")
            avg_lines = summary.get("avg_num_lines")
            if min_lines is not None and max_lines is not None:
                if min_lines == max_lines:
                    lines_str = str(int(min_lines))
                else:
                    lines_str = f"{int(min_lines)}-{int(max_lines)}"
                if avg_lines is not None:
                    lines_str += f" ({tr('avg')}: {float(avg_lines):.1f})"
                parts.append(f'<tr><th>{html_module.escape(tr("Lines"))}</th>'
                             f'<td>{html_module.escape(lines_str)}</td></tr>')

            avg_density = summary.get("avg_text_density")
            if avg_density is not None:
                parts.append(f'<tr><th>{html_module.escape(tr("Text Density"))}</th>'
                             f'<td>{float(avg_density):.1f} {html_module.escape(tr("per 10cm"))}</td></tr>')

            parts.append('</table>')

        # Section 2: Catalog Sizes
        if catalog_sizes:
            parts.append(f'<h4>{html_module.escape(tr("Catalog Dimensions"))}</h4>')
            parts.append('<table><tr><th>{}</th><th>{}</th><th>{}</th><th>{}</th></tr>'.format(
                html_module.escape(tr("Page Size")),
                html_module.escape(tr("Written Area")),
                html_module.escape(tr("Margins")),
                html_module.escape(tr("Material")),
            ))
            for sz in catalog_sizes:
                sx = sz.get("SizeX_cm")
                sy = sz.get("SizeY_cm")
                isx = sz.get("InnerSizeX_cm")
                isy = sz.get("InnerSizeY_cm")
                scope = html_module.escape(str(sz.get("Measurement_Scope") or ""))
                unit = html_module.escape(str(sz.get("SizeUnit") or "cm"))

                outer = f'{float(sx):.1f} \u00d7 {float(sy):.1f} {unit}' if sx and sy else '\u2014'
                inner = f'{float(isx):.1f} \u00d7 {float(isy):.1f} {unit}' if isx and isy else '\u2014'
                parts.append(f'<tr><td class="dim">{outer}</td><td>{inner}</td>'
                             f'<td>{scope}</td><td>{unit}</td></tr>')
            parts.append('</table>')

        # Section 3: Computed Measurements
        if computed:
            parts.append(f'<h4>{html_module.escape(tr("Computed Dimensions"))}</h4>')

            # Group by Image_Side
            side_groups = {}
            ei_map = {ei.get("FGP"): ei for ei in extra_info if ei.get("FGP")}
            for row in computed:
                side = row.get("Image_Side") or "Unknown"
                side_groups.setdefault(side, []).append(row)

            for side, rows in side_groups.items():
                side_label = tr("Recto") if "recto" in str(side).lower() else (
                    tr("Verso") if "verso" in str(side).lower() else html_module.escape(str(side)))
                parts.append(f'<p><b>{side_label}</b></p>')
                parts.append('<table><tr>'
                             f'<th>FGP</th>'
                             f'<th>{html_module.escape(tr("Page Size"))}</th>'
                             f'<th>{html_module.escape(tr("Written Area"))}</th>'
                             f'<th>{html_module.escape(tr("Margins"))} (cm)</th>'
                             f'<th>{html_module.escape(tr("Lines"))}</th>'
                             f'<th>{html_module.escape(tr("Text Density"))}</th>'
                             f'<th>{html_module.escape(tr("DPI Quality"))}</th>'
                             '</tr>')

                for row in rows:
                    fgp = row.get("FGP") or ""
                    pw = row.get("Page_Width_cm")
                    ph = row.get("Page_Height_cm")
                    ww = row.get("Written_Width_cm")
                    wh = row.get("Written_Height_cm")
                    lm = row.get("Left_Margin_cm")
                    rm = row.get("Right_Margin_cm")
                    tm = row.get("Top_Margin_cm")
                    bm = row.get("Bottom_Margin_cm")
                    nl = row.get("Num_Lines")
                    density = row.get("Text_Density_per10cm")
                    dpi = row.get("DpiGrid")

                    page_dim = f'{float(pw):.1f} \u00d7 {float(ph):.1f}' if pw and ph else '\u2014'
                    written = f'{float(ww):.1f} \u00d7 {float(wh):.1f}' if ww and wh else '\u2014'

                    margin_parts = []
                    if tm is not None: margin_parts.append(f'\u2191{float(tm):.1f}')
                    if bm is not None: margin_parts.append(f'\u2193{float(bm):.1f}')
                    if lm is not None: margin_parts.append(f'\u2190{float(lm):.1f}')
                    if rm is not None: margin_parts.append(f'\u2192{float(rm):.1f}')
                    margins_str = ' '.join(margin_parts) if margin_parts else '\u2014'

                    lines_str = str(int(nl)) if nl is not None else '\u2014'
                    density_str = f'{float(density):.1f}' if density is not None else '\u2014'
                    dpi_str = html_module.escape(tr("Grid calibrated")) if dpi and float(dpi) > 0 else html_module.escape(tr("Ruler only"))

                    parts.append(f'<tr><td>{html_module.escape(str(fgp))}</td>'
                                 f'<td class="dim">{page_dim}</td><td>{written}</td>'
                                 f'<td>{margins_str}</td><td>{lines_str}</td>'
                                 f'<td>{density_str}</td><td>{dpi_str}</td></tr>')
                parts.append('</table>')

        # Section 4: Blank Images
        if blank_images:
            parts.append(f'<h4>{html_module.escape(tr("Fragment Dimensions (no text block)"))}</h4>')
            parts.append(f'<table><tr><th>FGP</th>'
                         f'<th>{html_module.escape(tr("Page Size"))}</th>'
                         f'<th>{html_module.escape(tr("Material"))}</th></tr>')
            for bi in blank_images:
                fgp = bi.get("FGP") or ""
                fw = bi.get("Fragment_Width_cm")
                fh = bi.get("Fragment_Height_cm")
                not_whole = bi.get("IsNotWhole")
                dim = f'{float(fw):.1f} \u00d7 {float(fh):.1f} cm' if fw and fh else '\u2014'
                note = f' ({html_module.escape(tr("Incomplete fragment"))})' if not_whole else ''
                parts.append(f'<tr><td>{html_module.escape(str(fgp))}</td>'
                             f'<td class="dim">{dim}{note}</td><td></td></tr>')
            parts.append('</table>')

        return ''.join(parts)


class NliBibliographyDialog(QDialog):
    """NLI bibliography dialog with MARC 581 reference strings."""

    def __init__(self, marc_strings, sys_id='', shelfmark='', parent=None):
        super().__init__(parent)
        from shared.fjms_service import _parse_marc_annotations, strip_marc_annotation_suffix, _ts_symbol
        self.marc_strings = marc_strings
        self.sys_id = sys_id
        self._ts_symbol = _ts_symbol
        self.setWindowTitle(f"{tr('Bibliography Ktiv')} \u2014 {shelfmark}" if shelfmark else tr('Bibliography Ktiv'))
        self.setMinimumSize(900, 500)

        # Pre-parse all MARC strings
        self.parsed = []
        for ms in marc_strings:
            ann = _parse_marc_annotations(ms)
            ref = strip_marc_annotation_suffix(ms)
            self.parsed.append({
                'reference': ref,
                'raw': ms,
                'mention_type': ann.get('mention_type', ''),
                'has_image': ann.get('has_image', False),
                'transcription': ann.get('transcription', ''),
                'translation': ann.get('translation', ''),
            })

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)

        # Filter row
        filter_row = QHBoxLayout()
        self.text_filter = QLineEdit()
        self.text_filter.setPlaceholderText(tr('Filter references...'))
        self.text_filter.textChanged.connect(self._filter_rows)
        filter_row.addWidget(self.text_filter, 1)
        self.type_combo = QComboBox()
        for label, val in [(tr('All'), 'All'), (tr('Discussion'), 'Discussion'),
                           (tr('Mentioned'), 'Mentioned'), (tr('Index'), 'Index')]:
            self.type_combo.addItem(label, val)
        self.type_combo.currentIndexChanged.connect(lambda _: self._filter_rows())
        filter_row.addWidget(QLabel(tr('Type') + ':'))
        filter_row.addWidget(self.type_combo)
        layout.addLayout(filter_row)

        check_row = QHBoxLayout()
        self.chk_transcription = QCheckBox(tr('Has Transcription'))
        self.chk_transcription.toggled.connect(self._filter_rows)
        check_row.addWidget(self.chk_transcription)
        self.chk_translation = QCheckBox(tr('Has Translation'))
        self.chk_translation.toggled.connect(self._filter_rows)
        check_row.addWidget(self.chk_translation)
        self.chk_image = QCheckBox(tr('Has Image'))
        self.chk_image.toggled.connect(self._filter_rows)
        check_row.addWidget(self.chk_image)
        check_row.addStretch()
        layout.addLayout(check_row)

        # Table: Reference, D, T, S, I
        headers = [tr('Reference'), tr('col_D'), tr('col_T'), tr('col_S'), tr('col_I')]
        self.table = QTableWidget(len(self.parsed), len(headers))
        self.table.setHorizontalHeaderLabels(headers)
        hdr_model = self.table.horizontalHeader().model()
        for col_idx, tooltip in [(1, tr('Discussion')), (2, tr('Transcription')),
                                  (3, tr('Translation')), (4, tr('Image'))]:
            hdr_model.setHeaderData(col_idx, Qt.Orientation.Horizontal, tooltip, Qt.ItemDataRole.ToolTipRole)
        self.table.setSortingEnabled(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        for col_idx in (1, 2, 3, 4):
            self.table.setColumnWidth(col_idx, 36)

        for row, pe in enumerate(self.parsed):
            item0 = QTableWidgetItem(pe['reference'])
            item0.setData(Qt.ItemDataRole.UserRole, row)
            self.table.setItem(row, 0, item0)
            self.table.setItem(row, 1, QTableWidgetItem('\u2713' if pe['mention_type'] == 'Discussion' else ''))
            self.table.setItem(row, 2, QTableWidgetItem(_ts_symbol(pe['transcription'])))
            self.table.setItem(row, 3, QTableWidgetItem(_ts_symbol(pe['translation'])))
            self.table.setItem(row, 4, QTableWidgetItem('\u2713' if pe['has_image'] else ''))

        self.table.resizeColumnsToContents()
        for col_idx in (1, 2, 3, 4):
            self.table.setColumnWidth(col_idx, 36)
        self.table.setSortingEnabled(True)
        self.table.currentCellChanged.connect(self._on_row_selected)
        layout.addWidget(self.table, 1)

        # Detail panel
        self.detail_panel = QTextBrowser()
        self.detail_panel.setMaximumHeight(80)
        self.detail_panel.setVisible(False)
        self.detail_panel.setStyleSheet("border: 1px solid #ccc; padding: 4px; font-size: 12px;")
        layout.addWidget(self.detail_panel)

        # Bottom row
        bottom_row = QHBoxLayout()
        if sys_id:
            ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{sys_id}"
            btn_ktiv = QPushButton(tr('Open in KTIV'))
            btn_ktiv.clicked.connect(lambda: QDesktopServices.openUrl(QUrl(ktiv_url)))
            bottom_row.addWidget(btn_ktiv)
        bottom_row.addStretch()
        btn_close = QPushButton(tr('Close'))
        btn_close.clicked.connect(self.close)
        bottom_row.addWidget(btn_close)
        layout.addLayout(bottom_row)

    def _filter_rows(self):
        text_val = self.text_filter.text().strip().lower()
        type_val = self.type_combo.currentData() or 'All'
        need_trans = self.chk_transcription.isChecked()
        need_transl = self.chk_translation.isChecked()
        need_image = self.chk_image.isChecked()
        skip_vals = ('', 'None', 'Unknown')

        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            orig_idx = item.data(Qt.ItemDataRole.UserRole) if item else -1
            if not isinstance(orig_idx, int) or orig_idx < 0 or orig_idx >= len(self.parsed):
                continue
            pe = self.parsed[orig_idx]
            show = True
            if type_val != 'All' and pe['mention_type'] != type_val:
                show = False
            if show and need_trans:
                if not pe['transcription'] or pe['transcription'] in skip_vals:
                    show = False
            if show and need_transl:
                if not pe['translation'] or pe['translation'] in skip_vals:
                    show = False
            if show and need_image:
                if not pe['has_image']:
                    show = False
            if show and text_val:
                if text_val not in pe['reference'].lower() and text_val not in pe['raw'].lower():
                    show = False
            self.table.setRowHidden(row, not show)

    def _on_row_selected(self, row, col, prev_row, prev_col):
        item = self.table.item(row, 0)
        orig_idx = item.data(Qt.ItemDataRole.UserRole) if item else -1
        if isinstance(orig_idx, int) and 0 <= orig_idx < len(self.parsed):
            pe = self.parsed[orig_idx]
            parts = [pe['raw']]
            details = []
            if pe['mention_type']:
                details.append(tr(pe['mention_type']))
            if pe['transcription']:
                details.append(f"{tr('Transcription')}: {tr(pe['transcription'])}")
            if pe['translation']:
                details.append(f"{tr('Translation')}: {tr(pe['translation'])}")
            if pe['has_image']:
                details.append(tr('Has Image'))
            if details:
                parts.append(', '.join(details))
            self.detail_panel.setPlainText('\n'.join(parts))
            self.detail_panel.setVisible(True)
        else:
            self.detail_panel.setVisible(False)



