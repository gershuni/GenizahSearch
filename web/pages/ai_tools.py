"""Welcoming, bilingual entry points for AI-assisted manuscript research."""
from nicegui import ui

from web.components.typography import h1, h2
from web.translations import get_language

GPT_URL = 'https://chatgpt.com/g/g-6aa8230eaf888191bb44c5cb1163a39e-genizahsearch'
SKILL_URL = 'https://github.com/gershuni/GenizahSearch/tree/master-main/skills/cairo-genizah-research'
API_URL = 'https://github.com/gershuni/GenizahSearch/blob/master-main/docs/SEARCH_API.md'

AI_STYLES = '''
.gs-ai {color:var(--text-primary);}
.gs-ai-strip {display:flex;align-items:center;gap:12px;padding:8px 14px;
 border:1px solid var(--border-light);border-radius:10px;background:var(--bg-tertiary);width:100%;}
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
 border-radius:14px;padding:22px;display:flex;flex-direction:column;gap:12px;}
.gs-ai-muted {color:var(--text-secondary);line-height:1.65;}
.gs-ai-prompts {display:flex;flex-wrap:wrap;gap:8px;}
.gs-ai-example {background:var(--bg-secondary);border:1px solid var(--border-light);
 border-radius:10px;padding:10px 14px;font-size:14px;}
@media(max-width:640px) {
 .gs-ai-strip {flex-wrap:wrap;gap:8px;}
 .gs-ai-strip-title {flex-basis:100%;}
 .gs-ai-grid {grid-template-columns:1fr;}
 .gs-ai-hero {padding:20px;}
}
'''


def text(en, he):
    return he if get_language() == 'he' else en


def launch_link():
    with ui.link(target='/chatgpt', new_tab=True).classes('gs-ai-launch').props('rel="noopener noreferrer"'):
        ui.label(text('Open in ChatGPT', 'פתיחה ב־ChatGPT'))
        ui.icon('open_in_new', size='16px').props('aria-hidden=true')


def create_ai_card():
    """One desktop row; wrap naturally on small screens."""
    ui.add_css(AI_STYLES)
    with ui.element('aside').classes('gs-ai gs-ai-strip').props(
        'dir=rtl' if get_language() == 'he' else 'dir=ltr'
    ):
        ui.label(text('Explore the Genizah with AI', 'חקרו את הגניזה בעזרת בינה מלאכותית (AI)')).classes('gs-ai-strip-title')
        launch_link()
        ui.link(text('More AI options', 'עוד כלי AI'), '/ai')


def create_ai_page():
    ui.add_css(AI_STYLES)
    with ui.column().classes('gs-ai w-full max-w-4xl mx-auto p-4 gap-5').props(
        'dir=rtl' if get_language() == 'he' else 'dir=ltr'
    ):
        with ui.element('section').classes('gs-ai-hero'):
            ui.label(text('YOUR NEXT RESEARCH QUESTION', 'השאלה הבאה שלכם על הגניזה')).classes('text-xs font-bold tracking-wide gs-ai-muted')
            h1(text('Explore the Genizah with AI', 'חקרו את הגניזה בעזרת בינה מלאכותית (AI)'), classes='text-3xl font-bold')
            ui.label(text(
                'Start with a question, a name, or a passage. Your AI assistant can help find manuscripts, compare texts, and bring you back to the sources.',
                'התחילו בשאלה, בשם או בקטע טקסט. עוזר AI יכול לעזור לכם למצוא כתבי יד, להשוות נוסחים ולחזור אל המקורות.',
            )).classes('gs-ai-muted text-lg')
            with ui.row().classes('items-center gap-4 flex-wrap'):
                launch_link()
                ui.label(text('No installation · Opens in a new tab', 'ללא התקנה · נפתח בלשונית חדשה')).classes('text-sm gs-ai-muted')
            ui.label(text('A ChatGPT account with access to GPTs is required.', 'נדרש חשבון ChatGPT עם גישה ל־GPTs.')).classes('text-xs gs-ai-muted')
        h2(text('Try asking…', 'אפשר להתחיל כך…'), classes='text-xl font-bold')
        with ui.element('div').classes('gs-ai-prompts'):
            for example in [
                text('Find parallels to this passage', 'מצאו מקבילות לקטע הזה'),
                text('Search this name with spelling variants', 'חפשו את השם הזה בכתיבים שונים'),
                text('Create a report with manuscript references', 'הכינו דוח עם הפניות לכתבי היד'),
            ]:
                ui.label(example).classes('gs-ai-example')
        h2(text('Bring GenizahSearch to your workflow', 'חברו את הגניזה לכלי ה־AI שלכם'), classes='text-xl font-bold mt-2')
        with ui.element('div').classes('gs-ai-grid'):
            options = [
                ('auto_awesome', text('Already using an AI assistant?', 'כבר עובדים עם עוזר AI?'), text(
                    'Add the research skill to a compatible assistant. The guide walks you through installation and a first search. Requires Python and internet access.',
                    'הוסיפו את הסקיל למחקר לעוזר תואם. במדריך תמצאו הוראות התקנה וחיפוש ראשון. נדרשות הרצת Python וגישה לאינטרנט.',
                ), text('Explore the research skill', 'הכירו את הסקיל למחקר'), SKILL_URL),
                ('code', text('Building a research tool?', 'בונים כלי מחקר משלכם?'), text(
                    'Use the public API to search manuscripts, retrieve pages and find parallels from your own tools.',
                    'ה־API הציבורי מאפשר לחפש בכתבי היד, לשלוף דפים ולאתר מקבילות מתוך הכלים שלכם.',
                ), text('Read the API guide', 'למדריך ה־API'), API_URL),
            ]
            for icon, title, description, label, url in options:
                with ui.element('section').classes('gs-ai-option'):
                    ui.icon(icon, size='26px').style('color:var(--text-secondary)')
                    h2(title, classes='text-lg font-bold')
                    ui.label(description).classes('gs-ai-muted')
                    ui.link(label, url).classes('mt-auto')
        with ui.row().classes('w-full gap-3 items-center flex-wrap text-sm gs-ai-muted'):
            ui.badge(text('MCP · Planned', 'MCP · בתכנון')).props('outline')
            ui.label(text('Another way to connect your AI assistant. Not available yet.', 'דרך נוספת לחיבור עוזרי AI. עדיין אינה זמינה.'))
        ui.separator()
        ui.label(text(
            'Let AI help you explore; check its interpretations against the sources and manuscript images. Keep source credits in any research files you create.',
            'היעזרו ב־AI כדי לגלות ולחקור, ובדקו את הפרשנות מול המקורות ותצלומי כתבי היד. שמרו על הקרדיטים למקורות בקובצי המחקר שתיצרו.',
        )).classes('text-sm gs-ai-muted')
        ui.link(text('GPT privacy policy', 'מדיניות הפרטיות של ה־GPT'), '/api/chatgpt/privacy').classes('text-sm')
