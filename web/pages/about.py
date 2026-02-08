# -*- coding: utf-8 -*-
"""
About Page - What is the Cairo Genizah?

A visually engaging introduction for general audiences:
- Hero banner with synagogue image
- The story in flowing text (not cards)
- Key numbers
- What you can do on this site
- Disclaimer about transcription quality
- Credits and further reading
"""

import random

from nicegui import ui
from web.translations import get_language
from web.components.typography import h2
from web.state import state


def _html(content: str):
    """Helper to emit raw HTML without sanitization."""
    return ui.html(content, sanitize=False)


def _navigate_random_fragment():
    """Navigate to a random Genizah fragment in the browse view."""
    try:
        if state.meta_mgr and state.meta_mgr.csv_bank:
            all_ids = list(state.meta_mgr.csv_bank.keys())
            random_id = random.choice(all_ids)
            ui.navigate.to(f'/browse?sys_id={random_id}')
            return
    except Exception:
        pass
    ui.navigate.to('/browse')


# Wikimedia Commons images (public domain)
# Hero: T-S K5.13 manuscript fragment - a real Genizah page, much more evocative than the renovated synagogue
HERO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/f/f7/Education_%28T-S_K5.13%29_%28cropped%29.jpg'
SCHECHTER_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/9/9e/Solomon_Schechter_studying_the_fragments_of_the_Cairo_Genizah%2C_c._1898.jpg'


def create_about_page():
    """Create the About page with bilingual content about the Cairo Genizah."""

    lang = get_language()
    is_hebrew = lang == 'he'

    # Page-level styles
    ui.add_head_html('''
    <style>
    .about-hero {
        width: 100%;
        max-width: 900px;
        height: 320px;
        border-radius: 16px;
        overflow: hidden;
        position: relative;
        margin: 0 auto;
    }
    .about-hero img {
        width: 100%;
        height: 100%;
        object-fit: cover;
        object-position: center center;
    }
    .about-hero-overlay {
        position: absolute;
        inset: 0;
        background: linear-gradient(to top, rgba(20,15,5,0.9) 0%, rgba(20,15,5,0.35) 50%, rgba(20,15,5,0.1) 100%);
        display: flex;
        align-items: flex-end;
        padding: 30px 36px;
    }
    .about-hero-overlay h1 {
        font-size: 2.4rem;
        font-weight: 800;
        color: #f5e6c8;
        line-height: 1.3;
        margin: 0;
        text-shadow: 0 2px 12px rgba(0,0,0,0.5);
    }

    .about-body {
        max-width: 720px;
        margin: 0 auto;
        padding: 0 16px;
    }
    .about-body p, .about-body .about-paragraph {
        font-size: 1.06rem;
        line-height: 2.1;
        color: var(--text-secondary);
        margin-bottom: 20px;
    }
    .about-lead {
        font-size: 1.2rem !important;
        font-weight: 300;
        border-right: 3px solid var(--primary-600, #8b6914);
        padding-right: 20px;
        margin-bottom: 32px !important;
    }
    [dir="ltr"] .about-lead {
        border-right: none;
        border-left: 3px solid var(--primary-600, #8b6914);
        padding-right: 0;
        padding-left: 20px;
    }

    .about-figure {
        margin: 32px -20px;
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 8px 40px rgba(0,0,0,0.10);
    }
    .about-figure img {
        width: 100%;
        display: block;
    }
    .about-figure figcaption {
        font-size: 0.8rem;
        color: var(--text-tertiary);
        padding: 10px 16px;
        text-align: center;
        background: var(--bg-secondary);
    }

    .about-stats {
        display: flex;
        gap: 28px;
        justify-content: center;
        flex-wrap: wrap;
        margin: 32px 0;
        padding: 24px 0;
        border-top: 1px solid var(--border-light, #e8dcc8);
        border-bottom: 1px solid var(--border-light, #e8dcc8);
    }
    .about-stat { text-align: center; }
    .about-stat-num {
        font-size: 1.9rem;
        font-weight: 800;
        color: var(--primary-700, #8b6914);
        display: block;
    }
    .about-stat-label {
        font-size: 0.8rem;
        color: var(--text-tertiary);
        margin-top: 2px;
    }

    .about-disclaimer {
        background: var(--bg-tertiary, #2d1f0e);
        border: 1px solid var(--border-medium);
        padding: 22px 26px;
        border-radius: 12px;
        margin: 32px 0;
    }
    .about-disclaimer p {
        font-size: 0.92rem !important;
        line-height: 2 !important;
        color: var(--text-secondary) !important;
    }
    .about-disclaimer strong {
        color: var(--primary-600, #c89b3c);
    }

    .about-section-break {
        width: 40px;
        height: 2px;
        background: var(--primary-600, #c89b3c);
        margin: 36px auto;
    }

    .about-tools {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 16px;
        margin: 24px 0 32px;
    }
    .about-tool {
        border: 1px solid var(--border-light, #e8dcc8);
        border-radius: 12px;
        padding: 22px 16px;
        text-align: center;
        text-decoration: none;
        color: inherit;
        transition: all 0.2s;
        cursor: pointer;
        display: block;
    }
    .about-tool:hover {
        border-color: var(--primary-600, #c89b3c);
        box-shadow: 0 4px 16px rgba(0,0,0,0.06);
        transform: translateY(-2px);
    }
    .about-tool-icon { font-size: 1.6rem; margin-bottom: 8px; }
    .about-tool h3 {
        font-size: 0.95rem;
        font-weight: 600;
        color: var(--text-primary);
        margin: 0 0 6px;
    }
    .about-tool p {
        font-size: 0.82rem !important;
        color: var(--text-tertiary) !important;
        line-height: 1.6 !important;
        margin: 0 !important;
    }

    .about-credits {
        border-top: 1px solid var(--border-light, #e8dcc8);
        padding-top: 24px;
        margin-top: 40px;
    }
    .about-credits p {
        font-size: 0.85rem !important;
        color: var(--text-tertiary) !important;
        line-height: 2 !important;
    }
    .about-credits a {
        color: var(--primary-600, #8b6914);
        text-decoration: none;
    }
    .about-credits a:hover { text-decoration: underline; }

    .about-body h2 {
        font-size: 1.1rem;
        font-weight: 700;
        color: var(--text-primary);
        margin: 28px 0 12px;
        letter-spacing: -0.01em;
    }
    .about-body h2:first-of-type {
        margin-top: 0;
    }

    @media (max-width: 640px) {
        .about-hero { height: 220px; }
        .about-hero-overlay h1 { font-size: 1.7rem; }
        .about-tools { grid-template-columns: 1fr; }
        .about-figure { margin: 24px 0; }
    }
    </style>
    ''')

    # JSON-LD structured data
    ui.add_head_html('''
    <script type="application/ld+json">
    {
        "@context": "https://schema.org",
        "@type": "AboutPage",
        "name": "What is the Cairo Genizah?",
        "alternativeHeadline": "מהי גניזת קהיר?",
        "description": "The Cairo Genizah: over 350,000 medieval manuscript fragments from the Ben Ezra Synagogue in Cairo, spanning 1,000 years of Jewish life.",
        "url": "https://GenizahSearch.com/about",
        "inLanguage": ["en", "he"],
        "isPartOf": {
            "@type": "WebSite",
            "name": "Dicta Genizah Search",
            "url": "https://GenizahSearch.com"
        },
        "about": {
            "@type": "Collection",
            "name": "Cairo Genizah",
            "description": "A collection of over 350,000 medieval Jewish manuscript fragments discovered in the Ben Ezra Synagogue in Cairo, dating from approximately 870 to 1880 CE.",
            "numberOfItems": 350000,
            "locationCreated": {
                "@type": "Place",
                "name": "Ben Ezra Synagogue",
                "address": {
                    "@type": "PostalAddress",
                    "addressLocality": "Cairo",
                    "addressCountry": "EG"
                }
            }
        },
        "mentions": [
            {
                "@type": "Person",
                "name": "Solomon Schechter",
                "description": "Scholar who recovered approximately 193,000 Genizah fragments from Cairo in 1896"
            },
            {
                "@type": "Person",
                "name": "Agnes Smith Lewis",
                "description": "Scottish scholar of Semitic languages who brought Genizah fragments to Cambridge"
            },
            {
                "@type": "Person",
                "name": "Margaret Gibson",
                "description": "Scottish scholar of Semitic languages who brought Genizah fragments to Cambridge"
            }
        ]
    }
    </script>
    ''')

    with ui.column().classes('w-full gap-0 fade-in'):

        if is_hebrew:
            _create_hebrew_content()
        else:
            _create_english_content()


def _create_hebrew_content():
    """Create the Hebrew about content."""

    # === Hero Banner ===
    _html(f'''
    <div class="about-hero">
        <img src="{HERO_IMAGE}" alt="קטע מגניזת קהיר — כתב יד עברי מימי הביניים (T-S K5.13)">
        <div class="about-hero-overlay">
            <h1>הגניזה הקהירית</h1>
        </div>
    </div>
    ''')

    # === Body ===
    with ui.column().classes('about-body w-full gap-0 mt-8').style('direction: rtl; text-align: right;'):

        _html('''
        <p class="about-paragraph about-lead">
            כשפתחו את עליית הגג של בית הכנסת בן עזרא בקהיר, מצאו שם
            את הארכיון הגדול ביותר של ימי הביניים היהודיים —
            מאות אלפי דפים שמספרים על אלף שנים של חיים.
        </p>
        ''')

        _html('<h2>מקום שמירה שהפך לארכיון</h2>')

        _html('''
        <p class="about-paragraph">
            במסורת היהודית אסור לזרוק נייר שכתוב עליו שם ה', ולכן נהגו לאסוף דפים ישנים
            בחדר מיוחד — "גניזה" — עד שיובאו לקבורה. אבל בקהיר הפכו את הגניזה למין פח אשפה
            מכובד: דחפו לשם הכל. מכתבים, חוזים, שטרי גירושין, שירי אהבה, מרשמים רפואיים,
            קמיעות, רשימות מטבח — וכמובן גם תורה, תלמוד, פיוט ופילוסופיה. והכל נשמר שם
            במשך מאות שנים, כי באקלים היבש של קהיר נייר לא נרקב.
        </p>
        ''')

        # Schechter image
        _html(f'''
        <figure class="about-figure">
            <img src="{SCHECHTER_IMAGE}" alt="שלמה שכטר בוחן קטעי גניזה">
            <figcaption>שלמה שכטר בוחן קטעי גניזה בספריית קמברידג', סביבות 1898</figcaption>
        </figure>
        ''')

        _html('<h2>הגילוי: שכטר והאחיות מסקוטלנד</h2>')

        _html('''
        <p class="about-paragraph">
            ב-1896 הגיעו לקהיר שתי אחיות סקוטיות, אגנס סמית לואיס ומרגרט גיבסון, חוקרות
            שפות שמיות. הן חזרו לקמברידג' עם כמה דפים שנראו להן מעניינים, והראו אותם לחברן
            שלמה שכטר. שכטר זיהה ביניהם את הנוסח העברי האבוד של ספר בן סירא. הוא מיהר
            לקהיר, שכנע את גבאי בית הכנסת, וחזר עם כ-193,000 קטעים.
        </p>
        ''')

        _html('''
        <p class="about-paragraph">
            אבל שכטר לא היה הראשון ולא היחיד. סוחרי עתיקות מכרו קטעים לספריות באירופה ובאמריקה
            עוד לפניו, ואחריו המשיכו לזרום דפים לכל עבר. היום הם מפוזרים ביותר מ-75 אוספים
            בקמברידג', ניו יורק, סנקט פטרבורג, אוקספורד, מנצ'סטר, לונדון, פריז, ירושלים ועוד.
        </p>
        ''')

        # Stats
        _html('''
        <div class="about-stats">
            <div class="about-stat">
                <span class="about-stat-num">~350,000</span>
                <span class="about-stat-label">קטעי כתבי יד</span>
            </div>
            <div class="about-stat">
                <span class="about-stat-num">75+</span>
                <span class="about-stat-label">ספריות בעולם</span>
            </div>
            <div class="about-stat">
                <span class="about-stat-num">~1,000</span>
                <span class="about-stat-label">שנות היסטוריה</span>
            </div>
        </div>
        ''')

        _html('<h2>יותר מדי חומר, מעט מדי זמן</h2>')

        _html('''
        <p class="about-paragraph">
            החוקרים שחקרו את הגניזה גילו עולם שלם: דמויות היסטוריות חדשות, גרסאות לא ידועות
            של ספרים מוכרים, ותיעוד מפורט של חיי יומיום. אבל הם גם הבינו את הבעיה:
            <strong>יש פשוט יותר מדי חומר</strong>. יותר ממאה שנה חוקרים עובדים על הגניזה,
            ועדיין מגלים דברים חדשים.
        </p>
        ''')

        # Section break
        _html('<div class="about-section-break"></div>')

        _html('<h2>מהגניזה לעידן הדיגיטלי</h2>')

        _html('''
        <p class="about-paragraph">
            ואז הגיעו המחשבים.
            <strong><a href="https://fjms.genizah.org/" target="_blank">פרויקט הגניזה של פרידברג</a></strong>
            אסף מאות אלפי תמונות של כל הקטעים מכל הספריות, והמידע שלו הועבר ל<strong><a href="https://web.nli.org.il/sites/nlis/he/manuscript" target="_blank">פרויקט "כתיב"</a></strong>
            של הספרייה הלאומית.
            <strong><a href="https://www.midrashproject.org/" target="_blank">פרויקט MiDRASH</a></strong>,
            בתמיכת האיחוד האירופי, פיתח בינה מלאכותית שקוראת כתבי יד עבריים —
            ובדצמבר 2024 שחרר
            <a href="https://zenodo.org/records/14054599" target="_blank">תעתוק אוטומטי</a>
            של כמעט כל הקטעים.
        </p>
        ''')

        _html('''
        <p class="about-paragraph">
            האתר פותח בתמיכת <strong><a href="https://dicta.org.il/" target="_blank">עמותת דיקטה</a></strong>,
            כדי לאפשר חיפוש ועיון בתעתוקי MiDRASH.
        </p>
        ''')

        # Disclaimer
        _html('''
        <div class="about-disclaimer">
            <p>
                <strong>הערה על דיוק התעתוקים:</strong>
                התעתוקים נוצרו באמצעות זיהוי אוטומטי (OCR) ואינם מדויקים. שונות בכתבי היד,
                דהיית דיו ובלאי החומר מקשים על הזיהוי. עם זאת, גם תעתוק חלקי מאפשר לראשונה
                חיפוש רוחבי על פני כמעט כל קורפוס הגניזה. כלי החיפוש באתר תוכננו להתמודד עם אי-דיוקים אלה.
            </p>
        </div>
        ''')

        # Tools
        h2('מה אפשר לעשות כאן', classes='text-xl font-bold mb-2', style='color: var(--text-primary);')

        _html('''
        <div class="about-tools">
            <a href="/search" class="about-tool">
                <div class="about-tool-icon">🔍</div>
                <h3>חיפוש טקסט</h3>
                <p>חפשו מילה או ביטוי בכל קטעי הגניזה. חיפוש וריאנטים מתמודד עם שגיאות הקריאה.</p>
            </a>
            <a href="/parallels" class="about-tool">
                <div class="about-tool-icon">📜</div>
                <h3>מקבילות</h3>
                <p>הדביקו טקסט מוכר — פיוט, מדרש, קטע תלמודי — ומצאו עדי נוסח חדשים.</p>
            </a>
            <a href="/browse" class="about-tool">
                <div class="about-tool-icon">📖</div>
                <h3>עיון בכתבי יד</h3>
                <p>דפדפו בתמונות כתבי היד לצד התעתוק. תקנו, הוסיפו הערות, שתפו.</p>
            </a>
        </div>
        ''')

        # CTA buttons
        with ui.row().classes('w-full justify-center gap-3 mb-8 flex-wrap'):
            ui.button('התחילו לחפש', icon='search', on_click=lambda: ui.navigate.to('/search')).props('color=primary')
            ui.button('דפדפו בכתבי יד', icon='menu_book', on_click=lambda: ui.navigate.to('/browse')).props('outline')
            ui.button('קטע גניזה אקראי', icon='casino', on_click=lambda: _navigate_random_fragment()).props('outline')

        # Credits
        _html('''
        <div class="about-credits">
            <p>
                האתר מבוסס על תעתוקי
                <a href="https://www.midrashproject.org/" target="_blank">פרויקט MiDRASH</a>
                (דניאל שטוקל בן עזרא, אבי שמידמן, נחום דרשוביץ, יהודית אולשובי-שלנגר וצוות הפרויקט).
                תודה לפרופ' משה קופל ו<a href="https://dicta.org.il/" target="_blank">דיקטה</a> על התמיכה,
                ולאלישע רוזנצווייג, אפרים מאירי, אלעזר גרשוני, איתי קגן, אלנתן חן ועדיאל ברויאר.
            </p>
            <p>
                קישורים:
                <a href="https://geniza.princeton.edu/" target="_blank">PGP פרינסטון</a> ·
                <a href="https://fjms.genizah.org/" target="_blank">פרויקט פרידברג</a> ·
                <a href="https://web.nli.org.il/sites/nlis/he/manuscript" target="_blank">כתיב</a> ·
                <a href="https://www.lib.cam.ac.uk/collections/departments/taylor-schechter-genizah-research-unit" target="_blank">גניזת קמברידג'</a>
            </p>
            <p style="font-style: italic; margin-top: 16px; padding-top: 12px; border-top: 1px solid var(--border-light, #ece4d6);">
                לזכרו של מורנו ורבנו, פרופ' מנחם כהנא ז"ל.
            </p>
            <p>יוצר האתר: הלל גרשוני · <a href="mailto:gershuni@gmail.com">gershuni@gmail.com</a></p>
        </div>
        ''')


def _create_english_content():
    """Create the English about content."""

    # === Hero Banner ===
    _html(f'''
    <div class="about-hero">
        <img src="{HERO_IMAGE}" alt="Cairo Genizah fragment — medieval Hebrew manuscript (T-S K5.13)">
        <div class="about-hero-overlay">
            <h1>The Cairo Genizah</h1>
        </div>
    </div>
    ''')

    # === Body ===
    with ui.column().classes('about-body w-full gap-0 mt-8'):

        _html('''
        <p class="about-paragraph about-lead">
            When they opened the attic of the Ben Ezra Synagogue in Cairo, they found
            the largest archive of medieval Jewish life ever discovered —
            hundreds of thousands of pages spanning a thousand years.
        </p>
        ''')

        _html('<h2>A storeroom that became an archive</h2>')

        _html('''
        <p class="about-paragraph">
            In Jewish tradition, it's forbidden to discard paper bearing God's name, so worn pages
            were stored in a special room — a "genizah" — until they could be buried. But in Cairo,
            the genizah became a dignified wastebasket: everything went in. Letters, contracts, divorce
            deeds, love poems, medical prescriptions, amulets, shopping lists — and of course Torah,
            Talmud, poetry, and philosophy. And it all survived there for centuries, because in Cairo's
            dry climate paper doesn't decay.
        </p>
        ''')

        # Schechter image
        _html(f'''
        <figure class="about-figure">
            <img src="{SCHECHTER_IMAGE}" alt="Solomon Schechter studying Genizah fragments">
            <figcaption>Solomon Schechter studying Genizah fragments at Cambridge, c. 1898</figcaption>
        </figure>
        ''')

        _html('<h2>The discovery: Schechter and the Scottish sisters</h2>')

        _html('''
        <p class="about-paragraph">
            In 1896, two Scottish sisters — Agnes Smith Lewis and Margaret Gibson, accomplished scholars
            of Semitic languages — visited Cairo and returned to Cambridge with some intriguing pages.
            They showed them to their friend Solomon Schechter, who recognized among them the lost Hebrew
            text of the Book of Ben Sira. He rushed to Cairo, convinced the synagogue wardens, and returned
            with approximately 193,000 fragments.
        </p>
        ''')

        _html('''
        <p class="about-paragraph">
            But Schechter was neither the first nor the only one. Antiquities dealers had sold fragments
            to European and American libraries before him, and more continued flowing in every direction
            afterward. Today they're scattered across more than 75 collections in Cambridge, New York,
            St. Petersburg, Oxford, Manchester, London, Paris, Jerusalem, and beyond.
        </p>
        ''')

        # Stats
        _html('''
        <div class="about-stats">
            <div class="about-stat">
                <span class="about-stat-num">~350,000</span>
                <span class="about-stat-label">manuscript fragments</span>
            </div>
            <div class="about-stat">
                <span class="about-stat-num">75+</span>
                <span class="about-stat-label">libraries worldwide</span>
            </div>
            <div class="about-stat">
                <span class="about-stat-num">~1,000</span>
                <span class="about-stat-label">years of history</span>
            </div>
        </div>
        ''')

        _html('<h2>Too much material, too little time</h2>')

        _html('''
        <p class="about-paragraph">
            Scholars who studied the Genizah discovered an entire world: new historical figures,
            unknown versions of familiar books, and detailed documentation of daily life. But they
            also understood the problem: <strong>there's simply too much material</strong>. For over
            a century scholars have been working on the Genizah, and new discoveries are still routine.
        </p>
        ''')

        # Section break
        _html('<div class="about-section-break"></div>')

        _html('<h2>From the Genizah to the digital age</h2>')

        _html('''
        <p class="about-paragraph">
            Then came the computers.
            <strong><a href="https://fjms.genizah.org/" target="_blank">The Friedberg Genizah Project</a></strong>
            collected hundreds of thousands of images of all fragments from all libraries, and its data was transferred to
            <strong><a href="https://web.nli.org.il/sites/nlis/en/manuscript" target="_blank">the Ktiv Project</a></strong>
            at the National Library of Israel.
            <strong><a href="https://www.midrashproject.org/" target="_blank">The MiDRASH Project</a></strong>,
            funded by the EU, developed AI that reads Hebrew manuscripts —
            and in December 2024 released
            <a href="https://zenodo.org/records/14054599" target="_blank">automatic transcriptions</a>
            of nearly all the fragments.
        </p>
        ''')

        _html('''
        <p class="about-paragraph">
            This site was developed with the support of <strong><a href="https://dicta.org.il/" target="_blank">Dicta</a></strong>,
            to enable searching and browsing within the MiDRASH transcriptions.
        </p>
        ''')

        # Disclaimer
        _html('''
        <div class="about-disclaimer">
            <p>
                <strong>A note on transcription accuracy:</strong>
                The transcriptions were generated by automatic text recognition (OCR) and are not fully accurate.
                Variation in scribal hands, faded ink, and worn writing surfaces all affect recognition quality.
                Nonetheless, even partial transcriptions enable, for the first time, broad search across nearly
                the entire Genizah corpus. The search tools on this site were designed to handle these inaccuracies.
            </p>
        </div>
        ''')

        # Tools
        h2('What you can do here', classes='text-xl font-bold mb-2', style='color: var(--text-primary);')

        _html('''
        <div class="about-tools">
            <a href="/search" class="about-tool">
                <div class="about-tool-icon">🔍</div>
                <h3>Text Search</h3>
                <p>Search for a word or phrase across all Genizah fragments. Variant search handles reading errors.</p>
            </a>
            <a href="/parallels" class="about-tool">
                <div class="about-tool-icon">📜</div>
                <h3>Parallels</h3>
                <p>Paste a known text — a piyyut, midrash, Talmudic passage — and find new witnesses.</p>
            </a>
            <a href="/browse" class="about-tool">
                <div class="about-tool-icon">📖</div>
                <h3>Browse Manuscripts</h3>
                <p>Browse manuscript images alongside the transcription. Correct, annotate, share.</p>
            </a>
        </div>
        ''')

        # CTA buttons
        with ui.row().classes('w-full justify-center gap-3 mb-8 flex-wrap'):
            ui.button('Start Searching', icon='search', on_click=lambda: ui.navigate.to('/search')).props('color=primary')
            ui.button('Browse Manuscripts', icon='menu_book', on_click=lambda: ui.navigate.to('/browse')).props('outline')
            ui.button('Random Fragment', icon='casino', on_click=lambda: _navigate_random_fragment()).props('outline')

        # Credits
        _html('''
        <div class="about-credits">
            <p>
                This site is based on transcriptions from the
                <a href="https://www.midrashproject.org/" target="_blank">MiDRASH Project</a>
                (Daniel Stökl Ben Ezra, Avi Shmidman, Nachum Dershowitz, Judith Olszowy-Schlanger, and team).
                Thanks to Prof. Moshe Koppel and <a href="https://dicta.org.il/" target="_blank">Dicta</a> for their support,
                and to Elisha Rosenzweig, Ephraim Meiri, Elazar Gershuni, Itai Kagan, Elnatan Chen, and Adiel Breuer.
            </p>
            <p>
                Links:
                <a href="https://geniza.princeton.edu/" target="_blank">Princeton Geniza Project</a> ·
                <a href="https://fjms.genizah.org/" target="_blank">Friedberg Genizah Project</a> ·
                <a href="https://web.nli.org.il/sites/nlis/en/manuscript" target="_blank">Ktiv (NLI)</a> ·
                <a href="https://www.lib.cam.ac.uk/collections/departments/taylor-schechter-genizah-research-unit" target="_blank">Cambridge Genizah Unit</a>
            </p>
            <p style="font-style: italic; margin-top: 16px; padding-top: 12px; border-top: 1px solid var(--border-light, #ece4d6);">
                Dedicated to the memory of our beloved teacher, Prof. Menachem Kahana z"l.
            </p>
            <p>Site creator: Hillel Gershuni · <a href="mailto:gershuni@gmail.com">gershuni@gmail.com</a></p>
        </div>
        ''')
