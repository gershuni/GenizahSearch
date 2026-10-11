"""Welcoming, bilingual entry points for AI-assisted manuscript research.

What is offered follows ``web.ai_listings`` (docs/plans/AI_LISTINGS_LAUNCH_PLAN.md): with no
listing live and the GPT still running, the page and the homepage strip are exactly what they
were before the listings existed; each live listing (the ChatGPT plugin, the Claude connector)
gets its own button, and from the GPT's retirement it is never offered.
"""
from nicegui import ui

from web.ai_listings import GEMINI_INSTALL, GPT_URL, MCP_URL, current  # noqa: F401 - GPT_URL re-exported
from web.components.typography import h1, h2
from web.translations import get_language

SKILL_URL = 'https://github.com/gershuni/GenizahSearch/tree/master-main/skills/cairo-genizah-research'
API_URL = 'https://github.com/gershuni/GenizahSearch/blob/master-main/docs/SEARCH_API.md'

AI_STYLES = '''
.gs-ai {color:var(--text-primary);}
.gs-ai-strip {display:flex;align-items:center;gap:12px;padding:8px 14px;
 border:1px solid var(--border-light);border-radius:10px;background:var(--bg-tertiary);width:100%;}
.gs-ai-strip.gs-ai-new {border-color:#17634d;border-width:2px;}
.gs-ai-strip-title {font-size:14px;font-weight:600;flex:1;}
.gs-ai a.gs-ai-launch, .gs-ai a.gs-ai-launch:visited {
 background:#17634d!important;color:#fff!important;text-decoration:none!important;
 display:inline-flex;align-items:center;gap:8px;border-radius:8px;padding:8px 16px;font-weight:600;}
.gs-ai a.gs-ai-launch:hover {background:#114c3b!important;}
.gs-ai a:focus-visible {outline:3px solid var(--text-primary);outline-offset:3px;}
.gs-ai-strip a {white-space:nowrap;font-size:14px;}
.gs-ai-hero {background:var(--bg-tertiary);border:1px solid var(--border-light);
 border-radius:18px;padding:28px;display:flex;flex-direction:column;gap:16px;width:100%;}
.gs-ai-grid {display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;width:100%;}
.gs-ai-option {background:var(--bg-secondary);border:1px solid var(--border-light);
 border-radius:14px;padding:22px;display:flex;flex-direction:column;gap:12px;min-width:0;}
.gs-ai-muted {color:var(--text-secondary);line-height:1.65;}
.gs-ai-prompts {display:flex;flex-wrap:wrap;gap:8px;}
.gs-ai-example {background:var(--bg-secondary);border:1px solid var(--border-light);
 border-radius:10px;padding:10px 14px;font-size:14px;}
.gs-ai-can {margin:0;padding-inline-start:20px;display:flex;flex-direction:column;gap:6px;list-style:disc;}
.gs-ai-can li {display:list-item;}
.gs-ai-code {font-family:ui-monospace,Consolas,monospace;font-size:13px;direction:ltr;text-align:left;
 background:var(--bg-tertiary);border:1px solid var(--border-light);border-radius:8px;
 padding:8px 10px;overflow-wrap:anywhere;user-select:all;}
@media(max-width:640px) {
 .gs-ai-strip {flex-wrap:wrap;gap:8px;}
 .gs-ai-strip-title {flex-basis:100%;}
 .gs-ai-grid {grid-template-columns:1fr;}
 .gs-ai-hero {padding:20px;}
}
'''


def text(en, he):
    return he if get_language() == 'he' else en


def _direction():
    return 'dir=rtl' if get_language() == 'he' else 'dir=ltr'


def launch_link():
    """The GPT, through /chatgpt (offered only while no plugin is live and the GPT runs)."""
    with ui.link(target='/chatgpt', new_tab=True).classes('gs-ai-launch').props('rel="noopener noreferrer"'):
        ui.label(text('Open in ChatGPT', 'פתיחה ב־ChatGPT'))
        ui.icon('open_in_new', size='16px').props('aria-hidden=true')


def listing_link(name, url):
    with ui.link(target=url, new_tab=True).classes('gs-ai-launch').props('rel="noopener noreferrer"'):
        ui.label(text(f'Add to {name}', f'הוספה ל־{name}'))
        ui.icon('open_in_new', size='16px').props('aria-hidden=true')


def announcement(names):
    """'New: GenizahSearch in ChatGPT and Claude', naming only what is live."""
    return text('New: GenizahSearch in ' + ' and '.join(names),
                'חדש: GenizahSearch ב־' + ' וב־'.join(names))


def create_ai_card():
    """One desktop row; wrap naturally on small screens."""
    ui.add_css(AI_STYLES)
    listings = current()
    live = listings.live
    classes = 'gs-ai gs-ai-strip' + (' gs-ai-new' if listings.announcing else '')
    with ui.element('aside').classes(classes).props(_direction()):
        title = (announcement([name for name, _ in live]) if listings.announcing
                 else text('Explore the Genizah with AI', 'חקרו את הגניזה בעזרת בינה מלאכותית (AI)'))
        ui.label(title).classes('gs-ai-strip-title')
        # The GPT stays until the ChatGPT plugin replaces it (or it retires), even
        # beside a live Claude connector, so ChatGPT readers always have a way in.
        if listings.gpt_available:
            launch_link()
        for name, url in live:
            listing_link(name, url)
        ui.link(text('More AI options', 'עוד כלי AI'), '/ai')


def create_ai_page():
    ui.add_css(AI_STYLES)
    listings = current()
    if not listings.live and listings.gpt_available:
        _gpt_page()
    else:
        _listings_page(listings)


def _hero_heading():
    ui.label(text('YOUR NEXT RESEARCH QUESTION', 'השאלה הבאה שלכם על הגניזה')).classes('text-xs font-bold tracking-wide gs-ai-muted')
    h1(text('Explore the Genizah with AI', 'חקרו את הגניזה בעזרת בינה מלאכותית (AI)'), classes='text-3xl font-bold')


def _examples():
    h2(text('Try asking…', 'אפשר להתחיל כך…'), classes='text-xl font-bold')
    with ui.element('div').classes('gs-ai-prompts'):
        for example in [
            text('Find parallels to this passage', 'מצאו מקבילות לקטע הזה'),
            text('Search this name with spelling variants', 'חפשו את השם הזה בכתיבים שונים'),
            text('Create a report with manuscript references', 'הכינו דוח עם הפניות לכתבי היד'),
        ]:
            ui.label(example).classes('gs-ai-example')


def _option(icon, title, description, label=None, url=None, code=None):
    with ui.element('section').classes('gs-ai-option'):
        ui.icon(icon, size='26px').style('color:var(--text-secondary)')
        h2(title, classes='text-lg font-bold')
        ui.label(description).classes('gs-ai-muted')
        if code:
            ui.label(code).classes('gs-ai-code').props('dir=ltr')
        if url:
            ui.link(label, url).classes('mt-auto')


def _skill_and_api_options():
    _option('auto_awesome', text('Already using an AI assistant?', 'כבר עובדים עם עוזר AI?'), text(
        'Add the research skill to a compatible assistant. The guide walks you through installation and a first search. Requires Python and internet access.',
        'הוסיפו את הסקיל למחקר לעוזר תואם. במדריך תמצאו הוראות התקנה וחיפוש ראשון. נדרשות הרצת Python וגישה לאינטרנט.',
    ), text('Explore the research skill', 'הכירו את הסקיל למחקר'), SKILL_URL)
    _option('code', text('Building a research tool?', 'בונים כלי מחקר משלכם?'), text(
        'Use the public API to search manuscripts, retrieve pages and find parallels from your own tools.',
        'ה־API הציבורי מאפשר לחפש בכתבי היד, לשלוף דפים ולאתר מקבילות מתוך הכלים שלכם.',
    ), text('Read the API guide', 'למדריך ה־API'), API_URL)


def _caution():
    ui.label(text(
        'Let AI help you explore; check its interpretations against the sources and manuscript images. Keep source credits in any research files you create.',
        'היעזרו ב־AI כדי לגלות ולחקור, ובדקו את הפרשנות מול המקורות ותצלומי כתבי היד. שמרו על הקרדיטים למקורות בקובצי המחקר שתיצרו.',
    )).classes('text-sm gs-ai-muted')


def _gpt_page():
    """The page as it was before the listings: the GPT, the skill and the API."""
    with ui.column().classes('gs-ai w-full max-w-4xl mx-auto p-4 gap-5').props(_direction()):
        with ui.element('section').classes('gs-ai-hero'):
            _hero_heading()
            ui.label(text(
                'Start with a question, a name, or a passage. Your AI assistant can help find manuscripts, compare texts, and bring you back to the sources.',
                'התחילו בשאלה, בשם או בקטע טקסט. עוזר AI יכול לעזור לכם למצוא כתבי יד, להשוות נוסחים ולחזור אל המקורות.',
            )).classes('gs-ai-muted text-lg')
            with ui.row().classes('items-center gap-4 flex-wrap'):
                launch_link()
                ui.label(text('No installation · Opens in a new tab', 'ללא התקנה · נפתח בלשונית חדשה')).classes('text-sm gs-ai-muted')
            ui.label(text('A ChatGPT account with access to GPTs is required.', 'נדרש חשבון ChatGPT עם גישה ל־GPTs.')).classes('text-xs gs-ai-muted')
        _examples()
        h2(text('Bring GenizahSearch to your workflow', 'חברו את הגניזה לכלי ה־AI שלכם'), classes='text-xl font-bold mt-2')
        with ui.element('div').classes('gs-ai-grid'):
            _skill_and_api_options()
        with ui.row().classes('w-full gap-3 items-center flex-wrap text-sm gs-ai-muted'):
            ui.badge(text('MCP · Planned', 'MCP · בתכנון')).props('outline')
            ui.label(text('Another way to connect your AI assistant. Not available yet.', 'דרך נוספת לחיבור עוזרי AI. עדיין אינה זמינה.'))
        ui.separator()
        _caution()
        ui.link(text('GPT privacy policy', 'מדיניות הפרטיות של ה־GPT'), '/api/chatgpt/privacy').classes('text-sm')


def _listings_page(listings):
    """The live listings, what they do, and every other way in."""
    live = listings.live
    with ui.column().classes('gs-ai w-full max-w-4xl mx-auto p-4 gap-5').props(_direction()):
        with ui.element('section').classes('gs-ai-hero'):
            _hero_heading()
            ui.label(text(
                'Ask in your own words. Your assistant searches the manuscripts, finds parallels, reads pages and cites them, and links you back to each page.',
                'שאלו במילים שלכם. העוזר יחפש בכתבי היד, ימצא מקבילות, יקרא דפים ויצטט אותם, ויקשר אתכם לכל דף.',
            )).classes('gs-ai-muted text-lg')
            if live:
                with ui.row().classes('items-center gap-4 flex-wrap'):
                    if listings.gpt_available:
                        launch_link()
                    for name, url in live:
                        listing_link(name, url)
                ui.label(text('No sign-up with GenizahSearch · Works in your own assistant account',
                              'ללא הרשמה ל־GenizahSearch · פועל בחשבון העוזר שלכם')).classes('text-sm gs-ai-muted')
            else:
                ui.label(text('Connect your assistant with one of the options below.',
                              'חברו את העוזר שלכם באחת הדרכים שלמטה.')).classes('text-sm gs-ai-muted')
        h2(text('What it can do', 'מה אפשר לעשות'), classes='text-xl font-bold')
        with ui.element('ul').classes('gs-ai-can'):
            for item in (
                text('Find manuscripts by a phrase or a shelfmark: exact, with spelling variants, or near spellings',
                     'מציאת כתבי יד לפי צירוף מילים או סימן מדף: במדויק, בכתיבים שונים או בכתיבים קרובים'),
                text('Find parallels to a passage, letter by letter or by word chunks',
                     'איתור מקבילות לקטע, ברמת האות או לפי מקטעי מילים'),
                text('Read a manuscript page with its transcription and a ready citation',
                     'קריאת דף בכתב יד עם התעתיק וציטוט מוכן'),
                text('Catalogue records and bibliography', 'רשומות קטלוג וביבליוגרפיה'),
            ):
                with ui.element('li'):
                    ui.label(item)
        _examples()
        h2(text('Other AI tools', 'כלי AI נוספים'), classes='text-xl font-bold mt-2')
        with ui.element('div').classes('gs-ai-grid'):
            _option('hub', text('Any MCP client', 'כל לקוח MCP'), text(
                'Connect the GenizahSearch server to any assistant that supports MCP connectors. No sign-in.',
                'חברו את שרת GenizahSearch לכל עוזר שתומך בחיבורי MCP. ללא התחברות.',
            ), code=MCP_URL)
            _option('terminal', 'Gemini CLI', text(
                'Install the GenizahSearch extension from a terminal:',
                'התקינו את התוסף של GenizahSearch משורת הפקודה:',
            ), code=GEMINI_INSTALL)
            _skill_and_api_options()
        ui.separator()
        _caution()
        with ui.row().classes('gap-4 flex-wrap text-sm'):
            ui.link(text('Privacy policy', 'מדיניות פרטיות'), '/ai/privacy')
            ui.link(text('Terms of use', 'תנאי שימוש'), '/terms')
            ui.link(text('Support', 'תמיכה'), '/support')
