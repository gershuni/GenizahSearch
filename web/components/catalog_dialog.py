# -*- coding: utf-8 -*-
"""
Catalog Records Dialog Component

Displays FJMS catalog data in a FIST 5-section side-by-side table layout:
teams as columns, fields as rows, grouped into 5 labeled sections.

Mirrors the FIST web interface "Cataloging Data Details" view:
1. Shelfmark Description
2. Content Description
3. Script Description
4. Format Description
5. Miscellaneous
"""

from nicegui import ui
from web.translations import tr, get_language


def show_catalog_dialog(sys_id: str, shelfmark: str, fjms_service=None):
    """
    Create and open a catalog records dialog showing multi-team scholarly data.

    Args:
        sys_id: The Alma/system ID for the manuscript.
        shelfmark: Display shelfmark for the header.
        fjms_service: FjmsService instance (or None to auto-get).
    """
    if fjms_service is None:
        from shared.fjms_service import get_fjms_service
        fjms_service = get_fjms_service(thread_safe=True)

    detail = fjms_service.get_catalog_detail(sys_id)
    records = detail.get("records", [])
    running_titles = detail.get("running_titles", {})
    sizes = detail.get("sizes", {})
    fields = detail.get("fields", {})
    free_descriptions = detail.get("free_descriptions", [])
    full_texts = detail.get("full_texts", [])
    textual_frames = detail.get("textual_frames", {})
    mentions = detail.get("mentions", {})

    lang = get_language()
    is_heb = lang == 'he'

    # Fetch FJMS translations (needed for both UI languages — direction determines usage)
    fjms_trans = {}
    try:
        from shared.translation_service import TranslationService
        tsvc = TranslationService(thread_safe=True)
        if tsvc.fjms_available():
            fjms_trans = tsvc.get_fjms_translations_batch([sys_id])
            fjms_trans = fjms_trans.get(sys_id, {})
        tsvc.close()
    except Exception:
        pass

    # Group records by source_name to get team columns, skipping generic sources
    from shared.fjms_service import GENERIC_SOURCE_NAMES
    teams = []  # list of (source_name, source_name_heb, [records])
    team_map = {}  # source_name -> index in teams
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

    num_teams = len(teams) if teams else 1

    dialog = ui.dialog().props('maximized=false full-width')

    with dialog, ui.card().classes('w-full max-w-[900px] max-h-[90vh]').style(
        'overflow: hidden; display: flex; flex-direction: column;'
    ):
        # Header with purple gradient
        with ui.row().classes('w-full items-center justify-between p-3 rounded-t').style(
            'background: linear-gradient(135deg, #6c3483, #9b59b6); color: white;'
        ):
            with ui.row().classes('items-center gap-2'):
                ui.icon('description').classes('text-xl')
                title_text = f'{tr("Catalog Records")} \u2014 {shelfmark}'
                ui.label(title_text).classes('text-lg font-bold')
            ui.button(icon='close', on_click=dialog.close).props(
                'flat dense round'
            ).classes('text-white')

        # Scrollable content area (plain div with overflow — QScrollArea needs explicit
        # height which doesn't work well in flex dialogs)
        with ui.element('div').classes('w-full').style(
            'flex: 1; overflow-y: auto; min-height: 200px;'
        ):
            if not records and not free_descriptions and not full_texts:
                with ui.column().classes('w-full items-center justify-center p-8'):
                    ui.icon('info_outline').classes('text-3xl').style('color: var(--text-muted);')
                    ui.label(tr("No catalog data available")).classes('text-sm').style(
                        'color: var(--text-muted);'
                    )
            else:
                # Horizontal scroll wrapper for tables with many team columns
                with ui.element('div').classes('w-full').style('overflow-x: auto;'):
                    with ui.column().classes('w-full gap-0 p-4'):
                        _render_catalog_table(
                            teams, running_titles, sizes, fields,
                            free_descriptions, full_texts, textual_frames, mentions, is_heb,
                            shelfmark=shelfmark, fjms_trans=fjms_trans, alma_id=sys_id,
                        )

        # Close button
        with ui.row().classes('w-full justify-end p-2'):
            ui.button(tr('Close'), on_click=dialog.close).props('flat dense')

    dialog.open()
    return dialog


def _section_header(text: str, num_cols: int):
    """Render a section header row spanning all columns."""
    with ui.row().classes('w-full items-center gap-2 py-2 px-3 mt-3 mb-1 rounded').style(
        'background: linear-gradient(135deg, #f3e8ff, #ede9fe);'
    ):
        ui.label(text).classes('text-sm font-bold').style('color: #6c3483;')


def _field_row(label: str, values: list, is_heb: bool):
    """Render a field row: label in first column, values in team columns.

    Args:
        label: Field label (translated).
        values: List of display values, one per team. None/empty = blank cell.
        is_heb: Whether Hebrew mode is active.
    """
    # Skip row entirely if all values are empty
    if not any(v for v in values):
        return

    num_teams = len(values)
    dir_style = 'direction: rtl; text-align: right;' if is_heb else ''

    with ui.row().classes('w-full items-start py-1 px-3').style(
        'border-bottom: 1px solid var(--border-light, #e5e7eb); min-height: 2em;'
    ):
        # Label column (fixed width)
        ui.label(label).classes('text-xs font-semibold shrink-0').style(
            f'width: 120px; color: var(--text-secondary); {dir_style}'
        )
        # Value columns
        for val in values:
            display_val = str(val).strip() if val else '\u2014'
            is_empty = not val
            style = f'flex: 1; min-width: 130px; {dir_style}'
            if is_empty:
                style += ' color: var(--text-muted, #9ca3af);'
            ui.label(display_val).classes('text-sm break-words').style(style)


def _render_catalog_table(teams, running_titles, sizes, fields,
                          free_descriptions, full_texts, textual_frames, mentions, is_heb,
                          shelfmark='', fjms_trans=None, alma_id=''):
    """Render the full FIST 6-section side-by-side table."""
    if fjms_trans is None:
        fjms_trans = {}
    from shared.fjms_service import get_team_display_name, get_team_header_name, is_team_source

    num_teams = len(teams)
    dir_style = 'direction: rtl; text-align: right;' if is_heb else ''

    if num_teams == 0:
        # Only free descriptions / full texts, no team data
        if free_descriptions or full_texts:
            _section_header(tr('Miscellaneous'), 1)
            _render_free_descriptions(free_descriptions, is_heb, fjms_trans=fjms_trans, alma_id=alma_id)
            _render_full_texts(full_texts, is_heb)
        return

    # === Team header row ===
    with ui.row().classes('w-full items-start py-2 px-3 mb-1 rounded').style(
        'background: var(--bg-tertiary, #f9fafb); border-bottom: 2px solid var(--border-light, #e5e7eb);'
    ):
        # Label column placeholder
        ui.label('').classes('shrink-0').style('width: 120px;')
        # Team name columns
        for team in teams:
            header_name = get_team_header_name(team["source_name"], is_heb=is_heb)
            # For non-team sources (e.g. FJMS site users), use Hebrew name when available
            if is_heb and header_name == team["source_name"] and team.get("source_name_heb"):
                header_name = team["source_name_heb"]
            with ui.column().classes('gap-0').style(f'flex: 1; min-width: 130px; {dir_style}'):
                ui.label(header_name).classes('text-sm font-bold').style('color: var(--primary-700);')

    # === Section 1: Shelfmark Description ===
    _section_header(tr('Shelfmark Description'), num_teams + 1)

    # Shelfmark
    if shelfmark:
        sm_vals = [shelfmark] * num_teams
        _field_row(tr('Shelfmark'), sm_vals, is_heb)

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
            sn_display = team.get("source_name_heb", sn) if is_heb else sn
            source_vals.append(sn_display or sn)
    _field_row(tr('Source'), source_vals, is_heb)

    # Number of Folios
    folio_vals = []
    for team in teams:
        folios = [_fmt_int(r.get("num_folio")) for r in team["records"]
                  if r.get("num_folio") and str(r["num_folio"]).strip() and str(r["num_folio"]).strip() != '0']
        folio_vals.append(', '.join(folios) if folios else None)
    _field_row(tr('Number of Folios'), folio_vals, is_heb)

    # Number of Bifolios
    bifolio_vals = []
    for team in teams:
        bifolios = [_fmt_int(r.get("num_bifolio")) for r in team["records"]
                    if r.get("num_bifolio") and str(r["num_bifolio"]).strip() and str(r["num_bifolio"]).strip() != '0']
        bifolio_vals.append(', '.join(bifolios) if bifolios else None)
    _field_row(tr('Number of Bifolios'), bifolio_vals, is_heb)

    # === Section 2: Content Description ===
    _section_header(tr('Content Description'), num_teams + 1)

    # Domain (from TextualFrame content - the [$Category$] parsed category)
    domain_vals = []
    for team in teams:
        from shared.fjms_service import parse_textual_frame
        categories = []
        for rec in team["records"]:
            tf_eng = rec.get("textual_frame_eng") or ""
            tf_heb = rec.get("textual_frame_heb") or ""
            tf = tf_heb if is_heb and tf_heb else tf_eng
            if tf:
                from shared.fjms_service import split_textual_frames
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
        domain_vals.append('; '.join(categories) if categories else None)
    _field_row(tr('Domain'), domain_vals, is_heb)

    # Running Title (with per-record translation support)
    # Fetch translations keyed by UnitCatalogRecId (not by alma_id like fjms_trans)
    _rt_trans_map = {}
    try:
        from shared.translation_service import TranslationService
        _tsvc_rt = TranslationService(thread_safe=True)
        if _tsvc_rt.fjms_available():
            _all_rt_rec_ids = []
            for team in teams:
                for rec in team["records"]:
                    rec_id = rec.get("unit_catalog_rec_id")
                    if rec_id and rec_id in running_titles:
                        _all_rt_rec_ids.append(rec_id)
            if _all_rt_rec_ids:
                _rt_trans_map = _tsvc_rt.get_fjms_translations_by_signature_ids(
                    'RunningTitle', list(set(_all_rt_rec_ids))
                )
        _tsvc_rt.close()
    except Exception:
        pass

    # Check if any team has running title data
    _any_rt = False
    for team in teams:
        for rec in team["records"]:
            rec_id = rec.get("unit_catalog_rec_id")
            if rec_id and rec_id in running_titles:
                for rt in running_titles[rec_id]:
                    if rt.get("running_title") and str(rt["running_title"]).strip():
                        _any_rt = True
                        break
            if _any_rt:
                break
        if _any_rt:
            break

    if _any_rt:
        # Inline layout (not _field_row) to support interactive toggle badges
        with ui.row().classes('w-full items-start py-1 px-3').style(
            'border-bottom: 1px solid var(--border-light, #e5e7eb); min-height: 2em;'
        ):
            # Label column (matches _field_row style)
            ui.label(tr('Running Title')).classes('text-xs font-semibold shrink-0').style(
                f'width: 120px; color: var(--text-secondary); {dir_style}'
            )
            # Per-team value columns
            for team in teams:
                titles_orig = []
                for rec in team["records"]:
                    rec_id = rec.get("unit_catalog_rec_id")
                    if rec_id and rec_id in running_titles:
                        for rt in running_titles[rec_id]:
                            rt_text = rt.get("running_title", "")
                            if rt_text and str(rt_text).strip():
                                titles_orig.append((str(rt_text).strip(), rec_id))

                if not titles_orig:
                    # Empty cell
                    ui.label('\u2014').classes('text-sm').style(
                        f'flex: 1; min-width: 130px; color: var(--text-muted, #9ca3af); {dir_style}'
                    )
                else:
                    with ui.column().classes('gap-1').style(f'flex: 1; min-width: 130px; {dir_style}'):
                        for orig_text, rec_id in titles_orig:
                            _trans_entry = _rt_trans_map.get(rec_id)
                            _is_translated = False
                            display_text = orig_text
                            display_dir = dir_style

                            if _trans_entry and isinstance(_trans_entry, tuple):
                                _trans_text_val = str(_trans_entry[0]).strip()
                                _trans_dir = _trans_entry[1]
                                # Direction-aware: en2he shows Hebrew in HE UI, he2en shows English in EN UI
                                _should_show = (
                                    (_trans_dir == 'en2he' and is_heb) or
                                    (_trans_dir == 'he2en' and not is_heb)
                                )
                                if _should_show and _trans_text_val and _trans_text_val != orig_text:
                                    display_text = _trans_text_val
                                    _is_translated = True
                                    if _trans_dir == 'en2he':
                                        display_dir = 'direction: rtl; text-align: right;'
                                    else:
                                        display_dir = ''

                            _rt_lbl = ui.label(display_text).classes('text-sm break-words').style(
                                f'line-height: 1.6; {display_dir}'
                            )

                            if _is_translated:
                                _rt_st = {'showing_original': False}
                                _rt_badge_ref = [None]
                                _orig_dir_style = dir_style
                                _trans_dir_style = display_dir

                                def _make_rt_toggle(lbl, badge_ref, orig, trans, orig_dir, trans_dir, flag):
                                    def handler():
                                        flag['showing_original'] = not flag['showing_original']
                                        if flag['showing_original']:
                                            lbl.text = orig
                                            lbl.style(f'line-height: 1.6; {orig_dir}')
                                            badge_ref[0].text = tr('Original')
                                        else:
                                            lbl.text = trans
                                            lbl.style(f'line-height: 1.6; {trans_dir}')
                                            badge_ref[0].text = tr('Translated')
                                    return handler

                                _rt_badge = ui.badge(tr('Translated'), color='light-blue').props(
                                    'dense outline'
                                ).classes('text-xs cursor-pointer')
                                _rt_badge_ref[0] = _rt_badge
                                _rt_badge.on('click', _make_rt_toggle(
                                    _rt_lbl, _rt_badge_ref, orig_text, display_text,
                                    _orig_dir_style, _trans_dir_style, _rt_st
                                ))

    # Detailed Content (from catalog_textual_frames — richer per-verse references)
    if textual_frames:
        # Batch-fetch TextualFrame translations (he2en) for EN UI
        _tf_trans_map = {}  # {original_heb_text: english_translation}
        if not is_heb and alma_id:
            try:
                from shared.translation_service import TranslationService
                _tsvc_tf = TranslationService(thread_safe=True)
                if _tsvc_tf.fjms_available():
                    _tf_lookup = _tsvc_tf.get_fjms_translations_by_text(
                        'TextualFrame', [alma_id]
                    )
                    _tf_trans_map = {k: v[0] for k, v in _tf_lookup.get(alma_id, {}).items()}
                _tsvc_tf.close()
            except Exception:
                pass

        dc_vals = []
        for team in teams:
            frames = []
            for rec in team["records"]:
                rec_id = rec.get("unit_catalog_rec_id")
                if rec_id and rec_id in textual_frames:
                    for tf in textual_frames[rec_id]:
                        heb_text = tf.get("heb")
                        eng_text = tf.get("eng")
                        if is_heb:
                            text = heb_text if heb_text else eng_text
                        else:
                            # In EN UI: prefer translation of Hebrew text if available
                            heb_key = str(heb_text).strip() if heb_text else None
                            tf_trans = _tf_trans_map.get(heb_key) if heb_key else None
                            text = tf_trans if tf_trans else (eng_text if eng_text else heb_text)
                        if text and str(text).strip():
                            frames.append(str(text).strip())
            dc_vals.append('; '.join(frames) if frames else None)
        _field_row(tr('Detailed Content'), dc_vals, is_heb)

    # GenizahTitle (with translation support)
    # Batch-fetch Title translations (he2en) for EN UI
    _title_trans_map = {}  # {original_text: english_translation}
    if not is_heb and alma_id:
        try:
            from shared.translation_service import TranslationService
            _tsvc_gt = TranslationService(thread_safe=True)
            if _tsvc_gt.fjms_available():
                _gt_lookup = _tsvc_gt.get_fjms_translations_by_text(
                    'Title', [alma_id]
                )
                _title_trans_map = {k: v[0] for k, v in _gt_lookup.get(alma_id, {}).items()}
            _tsvc_gt.close()
        except Exception:
            pass

    gt_vals = []
    for team in teams:
        titles = []
        for rec in team["records"]:
            gt_org = rec.get("genizah_title_org")
            gt_eng = rec.get("genizah_title_eng")
            if is_heb:
                gt = gt_org if gt_org and str(gt_org).strip() else gt_eng
            else:
                # EN UI: prefer English title, then translation, then Hebrew original
                if gt_eng and str(gt_eng).strip():
                    gt = gt_eng
                elif gt_org and str(gt_org).strip():
                    orig = str(gt_org).strip()
                    gt_trans = _title_trans_map.get(orig) if _title_trans_map else None
                    gt = gt_trans if gt_trans else gt_org
                else:
                    gt = None
            if gt and str(gt).strip():
                titles.append(str(gt).strip())
        gt_vals.append('; '.join(titles) if titles else None)
    _field_row(tr('Title'), gt_vals, is_heb)

    # === Section 3: Mentions ===
    if mentions:
        _section_header(tr('Mentions'), num_teams + 1)
        _render_mentions_rows(teams, mentions, is_heb)

    # === Section 4: Script Description ===
    _section_header(tr('Script Description'), num_teams + 1)

    # Language
    _render_field_category_row('GenizahLanguages', tr('Language'), teams, fields, is_heb)

    # Script Type
    _render_field_category_row('TypeOfScript', tr('Script Type'), teams, fields, is_heb)

    # Script Style
    _render_field_category_row('TypeOfScriptStyle', tr('Script Style'), teams, fields, is_heb)

    # Script Place
    _render_field_category_row('TypeOfScriptPlace', tr('Script Place'), teams, fields, is_heb)

    # Vocalization
    _render_field_category_row('TypeOfVocalization', tr('Vocalization'), teams, fields, is_heb)

    # === Section 5: Format Description ===
    _section_header(tr('Format Description'), num_teams + 1)

    # No. of Rows (NumRow)
    row_vals = []
    for team in teams:
        rows = [str(r.get("num_row", "")).strip() for r in team["records"]
                if r.get("num_row") and str(r["num_row"]).strip() and str(r["num_row"]).strip() != '0']
        row_vals.append(', '.join(rows) if rows else None)
    _field_row(tr('Number of Lines'), row_vals, is_heb)

    # Number of Columns (NumColumn)
    col_vals = []
    for team in teams:
        cols = [str(r.get("num_column", "")).strip() for r in team["records"]
                if r.get("num_column") and str(r["num_column"]).strip() and str(r["num_column"]).strip() != '0']
        col_vals.append(', '.join(cols) if cols else None)
    _field_row(tr('Number of Columns'), col_vals, is_heb)

    # Material
    _render_field_category_row('FragmentMaterial', tr('Material'), teams, fields, is_heb)

    # Physical Status
    _render_field_category_row('FragmentStatus', tr('Physical Status'), teams, fields, is_heb)

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
                        dim = f"{_fmt_num(sx)} \u00d7 {_fmt_num(sy)}"
                        if isx and isy:
                            dim += f" ({tr('Inner Size')}: {_fmt_num(isx)} \u00d7 {_fmt_num(isy)})"
                        dim += " mm"
                        size_parts.append(dim)
        size_vals.append('; '.join(size_parts) if size_parts else None)
    _field_row(tr('Size'), size_vals, is_heb)

    # === Section 6: Miscellaneous ===
    _section_header(tr('Miscellaneous'), num_teams + 1)

    _render_free_descriptions(free_descriptions, is_heb, fjms_trans=fjms_trans, alma_id=alma_id)
    _render_full_texts(full_texts, is_heb)


def _render_field_category_row(category: str, label: str, teams, fields, is_heb):
    """Render a row for a specific FieldCategory from catalog_fields."""
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
        vals.append('; '.join(field_vals) if field_vals else None)
    _field_row(label, vals, is_heb)


def _render_mentions_rows(teams, mentions, is_heb):
    """Render one row per mention type that has data across any team."""
    # Collect all mention types across all records
    mention_types_ordered = ['Personalities', 'Places', 'Creations', 'Dates', 'Groups']
    all_types = set()
    for rec_id, items in mentions.items():
        for item in items:
            mt = item.get("mention_type")
            if mt:
                all_types.add(mt)

    # Add any types not in ordered list
    extra_types = sorted(all_types - set(mention_types_ordered))
    type_order = [t for t in mention_types_ordered if t in all_types] + extra_types

    for mention_type in type_order:
        vals = []
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
            vals.append(', '.join(names) if names else None)
        _field_row(tr(mention_type), vals, is_heb)


def _render_full_texts(full_texts, is_heb):
    """Render scholarly full text descriptions with distinct styling."""
    dir_style = 'direction: rtl; text-align: right;' if is_heb else ''
    if not full_texts:
        return

    # Sub-header for full texts
    with ui.row().classes('w-full items-center gap-1 py-1 px-3 mt-2'):
        ui.label(tr('Scholarly Description')).classes('text-xs font-semibold').style(
            'color: #6c3483;'
        )

    for ft in full_texts:
        text = ft.get("text", "")
        if text and str(text).strip():
            with ui.row().classes('w-full py-2 px-3').style(
                f'border-bottom: 1px solid var(--border-light, #e5e7eb); '
                f'background: var(--bg-secondary, #fafafa); {dir_style}'
            ):
                ui.label(str(text).strip()).classes('text-sm whitespace-pre-wrap break-words').style(
                    f'flex: 1; line-height: 1.6; {dir_style}'
                )


def _render_free_descriptions(free_descriptions, is_heb, fjms_trans=None, alma_id=''):
    """Render free description texts with source attribution labels."""
    from shared.fjms_service import get_team_display_name

    dir_style = 'direction: rtl; text-align: right;' if is_heb else ''
    if not free_descriptions:
        with ui.row().classes('w-full py-1 px-3'):
            ui.label('\u2014').classes('text-sm').style('color: var(--text-muted);')
        return

    # Pre-fetch all free description translations for this alma_id in one batch
    _fd_lookup = {}  # signature_id -> translated text
    if alma_id:
        try:
            from shared.translation_service import TranslationService
            _tsvc_fd = TranslationService(thread_safe=True)
            if _tsvc_fd.fjms_available():
                sig_ids = [desc.get("signature_id") for desc in free_descriptions if desc.get("signature_id")]
                for sid in sig_ids:
                    if is_heb:
                        # Hebrew UI: fetch en2he translations for English catalog descriptions
                        he = _tsvc_fd.get_fjms_free_desc_he(alma_id, sid)
                        if he:
                            _fd_lookup[sid] = he
                    else:
                        # English UI: fetch he2en translations for Hebrew descriptions
                        en = _tsvc_fd.get_fjms_free_desc_en(alma_id, sid)
                        if en:
                            _fd_lookup[sid] = en
            _tsvc_fd.close()
        except Exception:
            pass

    for desc in free_descriptions:
        text = desc.get("text", "")
        if text and str(text).strip():
            display_text = str(text).strip()
            display_dir = dir_style
            sig_id = desc.get("signature_id")
            _is_translated = False
            if sig_id and sig_id in _fd_lookup:
                display_text = _fd_lookup[sig_id]
                if is_heb:
                    display_dir = 'direction: rtl; text-align: right;'  # Hebrew translation is RTL
                else:
                    display_dir = ''  # English translation is LTR
                _is_translated = True

            with ui.row().classes('w-full py-2 px-3').style(
                f'border-bottom: 1px solid var(--border-light, #e5e7eb); {display_dir}'
            ):
                with ui.column().classes('gap-0').style(f'flex: 1; {display_dir}'):
                    # Source attribution label — always use English key for lookup
                    eng_source = desc.get("source_name")
                    if eng_source in ('Instatution', 'Institution'):
                        eng_source = None
                    source = get_team_display_name(eng_source, is_heb=is_heb) if eng_source else None
                    with ui.row().classes('items-center gap-2'):
                        if source:
                            ui.label(source).classes('text-xs font-semibold').style(
                                f'color: var(--primary-700); {display_dir}'
                            )
                    _fd_lbl = ui.label(display_text).classes('text-sm whitespace-pre-wrap break-words').style(
                        f'line-height: 1.6; {display_dir}'
                    )
                    if _is_translated:
                        _orig_text = str(text).strip()
                        _trans_text = display_text
                        _fd_st = {'showing_original': False}
                        _fd_badge_ref = [None]
                        if is_heb:
                            # Hebrew UI, en2he: original is English (LTR), translated is Hebrew (RTL)
                            _orig_dir = ''
                            _trans_dir = 'direction: rtl; text-align: right;'
                        else:
                            # English UI, he2en: original is Hebrew (RTL), translated is English (LTR)
                            _orig_dir = 'direction: rtl; text-align: right;'
                            _trans_dir = ''
                        def _make_fd_toggle(lbl, badge_ref, orig, trans, orig_dir, trans_dir, flag):
                            def handler():
                                flag['showing_original'] = not flag['showing_original']
                                if flag['showing_original']:
                                    lbl.text = orig
                                    lbl.style(f'line-height: 1.6; {orig_dir}')
                                    badge_ref[0].text = tr('Original')
                                else:
                                    lbl.text = trans
                                    lbl.style(f'line-height: 1.6; {trans_dir}')
                                    badge_ref[0].text = tr('Translated')
                            return handler
                        _fd_badge = ui.badge(tr('Translated'), color='light-blue').props('dense outline').classes(
                            'text-xs cursor-pointer'
                        )
                        _fd_badge_ref[0] = _fd_badge
                        _fd_badge.on('click', _make_fd_toggle(
                            _fd_lbl, _fd_badge_ref, _orig_text, _trans_text,
                            _orig_dir, _trans_dir, _fd_st
                        ))


def _fmt_num(val) -> str:
    """Format a numeric value for size display, removing trailing .0."""
    if val is None:
        return ""
    s = str(val)
    if s.endswith('.0'):
        return s[:-2]
    return s


def _fmt_int(val) -> str:
    """Format a numeric value as integer (2.0 → '2')."""
    if val is None:
        return ""
    s = str(val).strip()
    if s.endswith('.0'):
        return s[:-2]
    return s


def _section_labels():
    """Return section label mapping (unused helper, kept for reference)."""
    return {
        'Shelfmark Description': tr('Shelfmark Description'),
        'Content Description': tr('Content Description'),
        'Script Description': tr('Script Description'),
        'Format Description': tr('Format Description'),
        'Miscellaneous': tr('Miscellaneous'),
    }
