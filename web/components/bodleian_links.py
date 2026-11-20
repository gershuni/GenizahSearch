"""Bodleian links in the image header of Browse and Quick View."""

from nicegui import ui

from shared.codicological import bodleian_image_link_items
from web.translations import tr


def render_bodleian_links(links):
    if not links:
        return
    with ui.row().classes('items-center gap-2 text-xs'):
        ui.label(tr('Bodleian')).style('color: white;')
        for key, label, url in bodleian_image_link_items(links):
            role = 'oxford-direct-link' if key == 'current' else f'bodleian-{key}'
            ui.link(tr(label), url, new_tab=True).style('color: #9ecbff;').props(f'data-role="{role}"')
