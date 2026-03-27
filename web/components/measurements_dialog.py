# -*- coding: utf-8 -*-
"""
Measurements Dialog Component

Displays physical measurement data for manuscripts: catalog dimensions,
computed per-image measurements (page size, margins, line count, text density),
and blank image fragment dimensions.

Data sourced from FIST_Computed_Measurements via fjms_enrichment.db.
"""

from nicegui import ui, run
from web.translations import tr, get_language


async def show_measurements_dialog(sys_id: str, shelfmark: str, fjms_service=None, image_side: str = None):
    """Show measurements dialog with catalog + computed measurement data.

    Args:
        image_side: Optional 'recto' or 'verso' to filter per-image data to current side.
    Async to avoid blocking UI thread during data fetch (run.io_bound).
    """
    if fjms_service is None:
        from shared.fjms_service import get_fjms_service
        fjms_service = get_fjms_service(thread_safe=True)

    # Fetch data off the event loop
    data = await run.io_bound(fjms_service.get_measurements, sys_id)
    lang = get_language()
    is_heb = lang == 'he'

    dialog = ui.dialog().props('maximized=false full-width')
    with dialog, ui.card().classes('w-full max-w-[900px] max-h-[90vh]').style(
        'overflow: hidden; display: flex; flex-direction: column;'
    ):
        # Header with teal gradient (distinct from catalog indigo and bib purple)
        with ui.row().classes('w-full items-center justify-between p-3 rounded-t').style(
            'background: linear-gradient(135deg, #00695c, #26a69a); color: white;'
        ):
            with ui.row().classes('items-center gap-2'):
                ui.icon('straighten').classes('text-xl')
                ui.label(f'{tr("Measurements")} \u2014 {shelfmark}').classes('text-lg font-bold')
            ui.button(icon='close', on_click=dialog.close).props('flat dense round').classes('text-white')

        # Scrollable content area
        with ui.element('div').classes('w-full').style(
            'flex: 1; overflow-y: auto; min-height: 200px;'
        ):
            with ui.column().classes('w-full gap-0 p-4'):
                summary = data.get("summary")
                catalog_sizes = data.get("catalog_sizes", [])
                computed = data.get("computed", [])
                extra_info = data.get("extra_info", [])
                blank_images = data.get("blank_images", [])

                # Filter per-image computed data to current side when specified
                # Note: blank_images don't have Image_Side in the DB, so don't filter them
                if image_side and computed:
                    side_lower = image_side.lower()
                    computed = [r for r in computed if side_lower in str(r.get("Image_Side", "")).lower()]

                if not summary and not catalog_sizes and not computed and not blank_images:
                    with ui.column().classes('w-full items-center justify-center p-8'):
                        ui.icon('info_outline').classes('text-3xl').style('color: var(--text-muted);')
                        ui.label(tr("No measurement data available")).classes('text-sm').style(
                            'color: var(--text-muted);'
                        )
                else:
                    # Section 1: Summary (manuscript-level)
                    if summary:
                        _render_summary_section(summary, is_heb)

                    # Section 2: Catalog Sizes (per D-09, show all with source attribution)
                    if catalog_sizes:
                        _render_catalog_sizes_section(catalog_sizes, is_heb)

                    # Section 3: Computed Measurements (per-image, grouped by Image_Side)
                    if computed:
                        _render_computed_section(computed, extra_info, is_heb)

                    # Section 4: Blank Images (fragments without text blocks)
                    if blank_images:
                        _render_blank_images_section(blank_images, is_heb)

        # Close button
        with ui.row().classes('w-full justify-end p-2'):
            ui.button(tr('Close'), on_click=dialog.close).props('flat dense')

    dialog.open()
    return dialog


def _section_header(text: str, color: str = '#00695c'):
    """Render a teal section header row."""
    with ui.row().classes('w-full items-center gap-2 py-2 px-3 mt-3 mb-1 rounded').style(
        'background: linear-gradient(135deg, #e0f2f1, #b2dfdb);'
    ):
        ui.label(text).classes('text-sm font-bold').style(f'color: {color};')


def _kv_row(label: str, value: str, is_heb: bool = False):
    """Render a key-value row."""
    dir_style = 'direction: rtl; text-align: right;' if is_heb else ''
    with ui.row().classes('w-full items-start py-1 px-3').style(
        'border-bottom: 1px solid var(--border-light, #e5e7eb); min-height: 2em;'
    ):
        ui.label(label).classes('text-xs font-semibold shrink-0').style(
            f'width: 160px; color: var(--text-secondary); {dir_style}'
        )
        ui.label(value).classes('text-sm break-words').style(
            f'flex: 1; {dir_style}'
        )


def _unit_label(unit: str) -> str:
    """Translate unit label for display."""
    unit_map = {'cm': 'ס"מ', 'mm': 'מ"מ'}
    lang = get_language()
    if lang == 'he' and unit.lower() in unit_map:
        return unit_map[unit.lower()]
    return unit


def _fmt_dim(w, h, unit='cm'):
    """Format width x height with 1 decimal place."""
    if w is None or h is None:
        return None
    try:
        return f"{float(w):.1f} \u00d7 {float(h):.1f} {_unit_label(unit)}"
    except (ValueError, TypeError):
        return None


def _fmt_val(v, decimals=1):
    """Format a numeric value with given decimal places, or return None."""
    if v is None:
        return None
    try:
        return f"{float(v):.{decimals}f}"
    except (ValueError, TypeError):
        return None


def _render_summary_section(summary: dict, is_heb: bool):
    """Render manuscript-level summary section."""
    _section_header(tr("Physical Measurements"))

    # Catalog dimensions
    cw = summary.get("catalog_width_cm")
    ch = summary.get("catalog_height_cm")
    dim = _fmt_dim(cw, ch)
    if dim:
        _kv_row(tr("Catalog Dimensions"), dim, is_heb)

    # Catalog inner dimensions
    ciw = summary.get("catalog_inner_width_cm")
    cih = summary.get("catalog_inner_height_cm")
    idim = _fmt_dim(ciw, cih)
    if idim:
        _kv_row(f'{tr("Catalog Dimensions")} ({tr("Inner")})', idim, is_heb)

    # Computed dimension range
    min_w = summary.get("min_computed_width_cm")
    max_w = summary.get("max_computed_width_cm")
    min_h = summary.get("min_computed_height_cm")
    max_h = summary.get("max_computed_height_cm")
    if min_w is not None and max_w is not None:
        if abs(float(min_w) - float(max_w)) < 0.05 and abs(float(min_h) - float(max_h)) < 0.05:
            range_str = _fmt_dim(min_w, min_h) or ''
        else:
            range_str = f'{tr("Width")}: {_fmt_val(min_w)}-{_fmt_val(max_w)} {_unit_label("cm")}, {tr("Height")}: {_fmt_val(min_h)}-{_fmt_val(max_h)} {_unit_label("cm")}'
        if range_str:
            _kv_row(tr("Computed Dimensions"), range_str, is_heb)

    # Material (only if not None per D-12)
    material = summary.get("material")
    if material:
        _kv_row(tr("Material"), tr(str(material)), is_heb)

    # Size category (only if not None)
    size_cat = summary.get("size_category")
    if size_cat:
        _kv_row(tr("Size Category"), str(size_cat), is_heb)

    # Line count range
    min_lines = summary.get("min_num_lines")
    max_lines = summary.get("max_num_lines")
    avg_lines = summary.get("avg_num_lines")
    if min_lines is not None and max_lines is not None:
        if min_lines == max_lines:
            lines_str = str(int(min_lines))
        else:
            lines_str = f"{int(min_lines)}-{int(max_lines)}"
        if avg_lines is not None:
            lines_str += f" ({tr('avg')}: {_fmt_val(avg_lines)})"
        _kv_row(tr("Lines"), lines_str, is_heb)

    # Text density
    avg_density = summary.get("avg_text_density")
    if avg_density is not None:
        _kv_row(tr("Text Density"), f"{_fmt_val(avg_density)} {tr('per 10cm')}", is_heb)

    # Image counts
    total = summary.get("total_image_count")
    computed_count = summary.get("computed_image_count")
    blank_count = summary.get("blank_image_count")
    if total:
        parts = [f"{total} {tr('total')}"]
        if computed_count:
            parts.append(f"{computed_count} {tr('with text')}")
        if blank_count:
            parts.append(f"{blank_count} {tr('blank')}")
        _kv_row(tr("Images"), ', '.join(parts), is_heb)


def _render_catalog_sizes_section(catalog_sizes: list, is_heb: bool):
    """Render catalog sizes section with source attribution per D-09."""
    _section_header(tr("Catalog Dimensions"))

    for sz in catalog_sizes:
        sx = sz.get("SizeX_cm")
        sy = sz.get("SizeY_cm")
        isx = sz.get("InnerSizeX_cm")
        isy = sz.get("InnerSizeY_cm")
        scope = sz.get("Measurement_Scope") or ""
        unit = sz.get("SizeUnit") or "cm"

        dim = _fmt_dim(sx, sy, unit)
        if dim:
            label_parts = [tr("Page Size")]
            if scope:
                label_parts.append(f"({scope})")
            label = ' '.join(label_parts)
            _kv_row(label, dim, is_heb)

        idim = _fmt_dim(isx, isy, unit)
        if idim:
            _kv_row(f'{tr("Written Area")} ({scope})' if scope else tr("Written Area"), idim, is_heb)


def _render_computed_section(computed: list, extra_info: list, is_heb: bool):
    """Render per-image computed measurements, grouped by Image_Side."""
    _section_header(tr("Computed Dimensions"))

    # Build FGP -> extra_info lookup
    ei_map = {}
    for ei in extra_info:
        fgp = ei.get("FGP")
        if fgp:
            ei_map[fgp] = ei

    # Group by Image_Side
    side_groups = {}
    for row in computed:
        side = row.get("Image_Side") or "Unknown"
        side_groups.setdefault(side, []).append(row)

    many_images = len(computed) > 3

    for side, rows in side_groups.items():
        side_label = tr("Recto") if "recto" in str(side).lower() else (
            tr("Verso") if "verso" in str(side).lower() else str(side)
        )
        if many_images:
            with ui.expansion(f'{side_label} ({len(rows)} {tr("Images")})').classes(
                'w-full'
            ).props('dense'):
                for row in rows:
                    _render_computed_row(row, ei_map, is_heb)
        else:
            with ui.column().classes('w-full gap-0'):
                ui.label(side_label).classes('text-xs font-bold px-3 pt-2').style(
                    'color: #00695c;'
                )
                for row in rows:
                    _render_computed_row(row, ei_map, is_heb)


def _render_computed_row(row: dict, ei_map: dict, is_heb: bool):
    """Render a single computed measurement row."""
    fgp = row.get("FGP")
    bifolio = row.get("Bifolio_Side") or ""
    component = row.get("Component_Num")

    # Sub-header for component
    parts = []
    if fgp:
        parts.append(f"FGP {fgp}")
    if component and int(component) > 1:
        parts.append(f"#{component}")
    if bifolio:
        parts.append(f"({bifolio})")
    if parts:
        ui.label(' '.join(parts)).classes('text-xs px-3 pt-1').style(
            'color: var(--text-secondary);'
        )

    # Page dimensions
    pw = row.get("Page_Width_cm")
    ph = row.get("Page_Height_cm")
    dim = _fmt_dim(pw, ph)
    if dim:
        _kv_row(tr("Page Size"), dim, is_heb)

    # Written area
    ww = row.get("Written_Width_cm")
    wh = row.get("Written_Height_cm")
    wdim = _fmt_dim(ww, wh)
    if wdim:
        _kv_row(tr("Written Area"), wdim, is_heb)

    # Margins
    lm = _fmt_val(row.get("Left_Margin_cm"))
    rm = _fmt_val(row.get("Right_Margin_cm"))
    tm = _fmt_val(row.get("Top_Margin_cm"))
    bm = _fmt_val(row.get("Bottom_Margin_cm"))
    if any([lm, rm, tm, bm]):
        margin_parts = []
        if tm:
            margin_parts.append(f"\u2191{tm}")
        if bm:
            margin_parts.append(f"\u2193{bm}")
        if lm:
            margin_parts.append(f"\u2190{lm}")
        if rm:
            margin_parts.append(f"\u2192{rm}")
        _kv_row(tr("Margins") + f" ({_unit_label('cm')})", '  '.join(margin_parts), is_heb)

    # Line count
    num_lines = row.get("Num_Lines")
    if num_lines is not None:
        _kv_row(tr("Lines"), str(int(num_lines)), is_heb)

    # Line height
    line_h = row.get("Avg_Line_Height_Text_mm")
    if line_h is not None:
        _kv_row(tr("Line Height"), f"{_fmt_val(line_h)} {_unit_label('mm')}", is_heb)

    # Text density
    density = row.get("Text_Density_per10cm")
    if density is not None:
        _kv_row(tr("Text Density"), f"{_fmt_val(density)} {tr('per 10cm')}", is_heb)

    # DPI quality
    dpi = row.get("DpiGrid")
    if dpi is not None:
        quality = tr("Grid calibrated") if dpi and float(dpi) > 0 else tr("Ruler only")
        _kv_row(tr("DPI Quality"), quality, is_heb)

    # Extra info from extra_info table
    ei = ei_map.get(fgp, {})
    material = ei.get("Material")
    if material:
        _kv_row(tr("Material"), tr(str(material)), is_heb)


def _render_blank_images_section(blank_images: list, is_heb: bool):
    """Render fragment dimensions for images without text blocks."""
    _section_header(tr("Fragment Dimensions (no text block)"))

    many = len(blank_images) > 5

    def _render_blanks(items):
        for bi in items:
            fw = bi.get("Fragment_Width_cm")
            fh = bi.get("Fragment_Height_cm")
            dim = _fmt_dim(fw, fh)
            if dim:
                not_whole = bi.get("IsNotWhole")
                label = dim
                if not_whole:
                    label += f"  ({tr('Incomplete fragment')})"
                fgp = bi.get("FGP")
                row_label = f"FGP {fgp}" if fgp else tr("Fragment")
                _kv_row(row_label, label, is_heb)

    if many:
        with ui.expansion(f'{tr("Fragment Dimensions (no text block)")} ({len(blank_images)})').classes(
            'w-full'
        ).props('dense'):
            _render_blanks(blank_images)
    else:
        _render_blanks(blank_images)
