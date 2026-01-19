import sys
import os
import asyncio
from nicegui import ui, app, run
from genizah_core import SearchEngine, MetadataManager, VariantManager

# --- הגדרות גלובליות ---
meta_mgr = None
searcher = None


def init_engine():
    global meta_mgr, searcher
    print("Loading Genizah Engine...")

    meta_mgr = MetadataManager()
    var_mgr = VariantManager()
    searcher = SearchEngine(meta_mgr, var_mgr)

    meta_mgr.start_background_loading()
    ui.notify('המנוע מוכן לעבודה!', type='positive')
    print("Engine Ready.")


@ui.page('/')
def main_page():
    # עיצוב בסיסי
    ui.colors(primary='#1976D2', secondary='#26A69A')

    # --- כותרת ---
    with ui.header().classes('bg-primary text-white shadow-md items-center gap-2 q-px-md'):
        ui.icon('library_books').classes('text-2xl')
        ui.label('Genizah Search Pilot').classes('text-xl font-bold')

    # --- פונקציות לוגיקה ---

    async def open_viewer(sys_id):
        """פתיחת חלון צפייה (דיאלוג)"""
        with ui.dialog() as dialog, ui.card().classes('w-full max-w-4xl h-[90vh] column'):
            # כותרת הדיאלוג
            with ui.row().classes('w-full justify-between items-center border-b q-pb-2'):
                ui.label(f'פריט: {sys_id}').classes('text-lg font-bold')
                ui.button(icon='close', on_click=dialog.close).props('flat round dense')

            # תוכן הדיאלוג
            content_area = ui.column().classes('w-full flex-grow overflow-auto p-4')
            with content_area:
                ui.spinner('dots').classes('size-10 self-center')

            dialog.open()

            # טעינת המידע המלא
            data = await run.io_bound(lambda: meta_mgr.get_full_data(sys_id))

            content_area.clear()
            with content_area:
                # הצגת תמונות
                if data.get('images'):
                    with ui.scroll_area().classes('w-full h-96 bg-gray-100 rounded border q-mb-4'):
                        for img in data['images']:
                            ui.image(img['url']).classes('w-full q-mb-2')

                # הצגת מטא-דאטה
                ui.label(data.get('shelfmark', 'Unknown')).classes('text-2xl font-bold text-primary')
                ui.separator()
                with ui.grid(columns=2).classes('w-full gap-2 q-mt-4'):
                    for k, v in data.get('attributes', {}).items():
                        ui.label(k).classes('font-bold text-gray-600')
                        ui.label(str(v)).classes('text-gray-900')

    async def run_search():
        """ביצוע החיפוש"""
        query = query_input.value
        if not query: return

        # ניקוי והצגת ספינר
        results_area.clear()
        with results_area:
            ui.spinner('dots').classes('size-12 self-center text-primary q-my-4')

        try:
            # שליחה למנוע
            print(f"Searching for: {query}...")
            results = await run.io_bound(
                lambda: searcher.execute_search(
                    query,
                    mode=mode_select.value,
                    gap=int(gap_input.value)
                )
            )
            print(f"Found {len(results)} results.")

            # --- הדפסת דיבאג (תסתכל בחלון השחור למטה) ---
            if results:
                print("First result keys:", results[0].keys())
            # ---------------------------------------------

        except Exception as e:
            results_area.clear()
            ui.notify(f'שגיאה: {e}', type='negative')
            print(f"Error: {e}")
            return

        # ציור התוצאות
        results_area.clear()

        with results_area:
            ui.label(f'נמצאו {len(results)} תוצאות').classes('text-gray-500 font-bold q-mb-2')

            if not results:
                ui.label('לא נמצאו תוצאות').classes('text-gray-400 self-center')
                return

            # לולאה על התוצאות
            for i, item in enumerate(results[:50]):  # מגביל ל-50 כדי לא להיתקע

                # מנסים למצוא מזהה בכל דרך אפשרית
                # הוספתי כאן הדפסה של המזהה אם הוא נמצא
                item_id = item.get('sys_id') or item.get('id') or item.get('doc_id')

                # כרטיס לכל תוצאה
                card = ui.card().classes(
                    'w-full row no-wrap items-start q-pa-md hover:shadow-lg border-l-4 border-primary cursor-pointer')

                # אם יש מזהה, מחברים לחיצה. אם אין - הכרטיס סתם מוצג.
                if item_id:
                    card.on('click', lambda _, x=item_id: open_viewer(x))
                else:
                    card.classes('opacity-70 cursor-not-allowed bg-gray-50')

                with card:
                    # תמונה ממוזערת
                    if item.get('thumb'):
                        ui.image(item['thumb']).classes('w-24 h-24 object-cover rounded bg-gray-200')

                    # פרטי טקסט
                    with ui.column().classes('q-ml-md flex-grow'):
                        # כותרת
                        shelfmark = item.get('shelfmark') or item.get('id_display') or 'Unknown Shelfmark'
                        ui.label(shelfmark).classes('text-lg font-bold text-primary')

                        # קטע טקסט (Snippet)
                        snippet = str(item.get('snippet', '(אין טקסט לתצוגה)'))
                        ui.html(snippet, sanitize=False).classes('dir-rtl text-right text-gray-800')

                        # מזהה טכני (דיבאג)
                        ui.label(f"ID: {item_id if item_id else 'MISSING'}").classes('text-xs text-gray-400 q-mt-auto')

    # --- בניית הממשק ---

    # 1. אזור החיפוש (קבוע למעלה)
    with ui.column().classes('w-full items-center q-pt-md sticky top-0 z-10 bg-white shadow-sm'):
        with ui.row().classes('w-full max-w-screen-lg gap-2 items-end q-pb-md'):
            query_input = ui.input(label='חיפוש').classes('flex-grow').props('outlined rounded')
            mode_select = ui.select(['exact', 'fuzzy', 'stems'], value='exact', label='סוג').classes('w-32')
            gap_input = ui.number(label='Gap', value=0).classes('w-20')

            # כפתור שמפעיל את החיפוש
            ui.button('חפש', icon='search', on_click=run_search).classes('h-14 px-6 shadow-md bg-primary text-white')

    # 2. אזור גלילה לתוצאות (חשוב!)
    # אנחנו עוטפים את התוצאות ב-ScrollArea כדי שיהיה אפשר לגלול
    main_scroll = ui.scroll_area().classes('w-full flex-grow')

    with main_scroll:
        results_area = ui.column().classes('w-full max-w-screen-lg self-center q-pa-md gap-3')


# אתחול
app.on_startup(init_engine)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title='Genizah Pilot v3', language='he', reload=False)