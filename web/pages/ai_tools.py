"""Public entry points for AI-assisted manuscript research."""
from nicegui import ui

from web.components.typography import h1, h2
from web.translations import get_language


GPT_URL = 'https://chatgpt.com/g/g-6aa8230eaf888191bb44c5cb1163a39e-genizahsearch'
SKILL_URL = 'https://github.com/gershuni/GenizahSearch/tree/master-main/skills/cairo-genizah-research'
API_URL = 'https://github.com/gershuni/GenizahSearch/blob/master-main/docs/SEARCH_API.md'


def text(en, he):
    return he if get_language() == 'he' else en


def create_ai_card():
    """Compact homepage invitation, with ordinary accessible links."""
    with ui.card().classes('w-full p-4 gap-2').style(
        'background: var(--bg-tertiary); border: 1px solid var(--border-light);'
    ):
        h2(text('Explore the Genizah with AI', 'חקרו את הגניזה בעזרת בינה מלאכותית'),
           classes='text-xl font-bold', style='color: var(--text-primary);')
        ui.label(text(
            'Ask in your own language, find manuscripts, compare passages, and create reports with source references.',
            'שאלו בשפה שלכם, חפשו בכתבי היד, השוו קטעים והפיקו דוחות מחקר עם הפניות למקורות.',
        )).style('color: var(--text-secondary);')
        with ui.row().classes('gap-4 items-center flex-wrap'):
            ui.link(text('Open in ChatGPT', 'פתיחה ב־ChatGPT'), '/chatgpt').classes(
                'px-4 py-2 rounded-lg font-semibold no-underline'
            ).style('background: var(--primary-600); color: white;')
            ui.link(text('All AI options', 'כל אפשרויות הבינה המלאכותית'), '/ai')


def create_ai_page():
    with ui.column().classes('w-full max-w-4xl mx-auto p-4 gap-5').props(
        'dir=rtl' if get_language() == 'he' else 'dir=ltr'
    ):
        h1(text('Explore the Genizah with AI', 'חקרו את הגניזה בעזרת בינה מלאכותית'),
           classes='text-3xl font-bold')
        ui.label(text(
            'Choose how to connect your AI assistant to GenizahSearch. Start with ChatGPT, or use the research skill or public API.',
            'בחרו כיצד לחבר את עוזר הבינה המלאכותית שלכם לחיפוש בגניזה. אפשר להתחיל ב־ChatGPT, להתקין את הסקיל למחקר או להשתמש ב־API הציבורי.',
        ))
        options = [
            ('ChatGPT', text(
                'Open GenizahSearch in ChatGPT and ask a research question. No local installation is needed. A ChatGPT account and access to GPTs are required; account and workspace limits apply.',
                'פתחו את GenizahSearch ב־ChatGPT ושאלו שאלת מחקר, ללא התקנה במחשב. נדרשים חשבון ChatGPT וגישה ל־GPTs, בהתאם למגבלות החשבון וסביבת העבודה.',
            ), text('Open in ChatGPT', 'פתיחה ב־ChatGPT'), '/chatgpt'),
            (text('Research skill', 'סקיל למחקר'), text(
                'For assistants that can install skills, execute Python scripts and connect to the internet. The guide covers requirements, installation and a connection test.',
                'לעוזרים שיכולים להתקין סקילים, להריץ סקריפטים של Python ולהתחבר לאינטרנט. המדריך כולל דרישות, הוראות התקנה ובדיקת חיבור.',
            ), text('Skill and installation guide', 'הסקיל ומדריך ההתקנה'), SKILL_URL),
            (text('Public API', 'API ציבורי'), text(
                'For developers building research tools: search, manuscript pages and passage parallels. Read the documentation for supported parameters and service limits.',
                'למפתחים הבונים כלי מחקר: חיפוש, עיון בדפי כתבי יד ואיתור מקבילות. בתיעוד מפורטים הפרמטרים הנתמכים ומגבלות השירות.',
            ), text('API documentation', 'תיעוד ה־API'), API_URL),
        ]
        for title, description, label, url in options:
            with ui.card().classes('w-full p-5 gap-3'):
                h2(title, classes='text-xl font-bold')
                ui.label(description)
                ui.link(label, url)
        with ui.card().classes('w-full p-5 gap-2'):
            h2(text('MCP — planned', 'MCP — בתכנון'), classes='text-xl font-bold')
            ui.label(text(
                'An official MCP connection for compatible assistants is planned. It is not available here yet.',
                'חיבור MCP רשמי לעוזרים תואמים נמצא בתכנון, וטרם זמין כאן.',
            ))
        ui.label(text(
            'Check AI interpretations against the retrieved sources and manuscript images. When creating research files, retain credits to the underlying data and editions.',
            'בדקו פרשנויות של בינה מלאכותית מול המקורות ותצלומי כתבי היד. בקובצי מחקר שמרו על הקרדיטים למקורות הנתונים ולמהדורות.',
        )).classes('text-sm').style('color: var(--text-secondary);')
        ui.link(text('GPT integration privacy policy', 'מדיניות הפרטיות של חיבור ה־GPT'),
                '/api/chatgpt/privacy')
