# -*- coding: utf-8 -*-
"""
About Page - What is the Cairo Genizah?

An introductory page for general audiences explaining:
1. The discovery story (1896-1897)
2. What is a genizah and what's in it
3. Where the fragments are today
4. The research revolution
5. Digital projects (FGP, Ktiv, MiDRASH)
6. About the transcriptions and their limitations
7. What you can do on this site
8. Further reading and credits
"""

from nicegui import ui, app
from web.translations import get_language
from web.components.typography import h1, h2, h3


def create_about_page():
    """Create the About page with bilingual content about the Cairo Genizah."""

    lang = get_language()
    is_hebrew = lang == 'he'

    # Language toggle state
    show_hebrew = {'value': is_hebrew}

    with ui.column().classes('w-full max-w-4xl mx-auto gap-6 fade-in p-4'):

        # === Language Toggle ===
        with ui.row().classes('w-full items-center justify-between mb-4'):
            h1(
                'מהי גניזת קהיר?' if show_hebrew['value'] else 'What is the Cairo Genizah?',
                classes='text-3xl font-bold',
                style='color: var(--text-primary);'
            )

            def toggle_language():
                show_hebrew['value'] = not show_hebrew['value']
                ui.navigate.reload()

            ui.button(
                'English' if show_hebrew['value'] else 'עברית',
                icon='translate',
                on_click=toggle_language
            ).props('flat dense')

        # === Content Container ===
        content_container = ui.column().classes('w-full gap-6')

        with content_container:
            if show_hebrew['value']:
                _create_hebrew_content()
            else:
                _create_english_content()


def _create_hebrew_content():
    """Create the Hebrew about content."""

    # === 1. The Discovery Story ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('auto_stories').classes('text-2xl text-primary')
            h2('סיפור הגילוי', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
בשנת 1896 הגיעו אגנס סמית לואיס ומרגרט דאנלופ גיבסון, שתי אחיות סקוטיות מלומדות, לביקור בקהיר. הן נכנסו לבית הכנסת העתיק "בן עזרא" והמדריך הראה להן את חדר הגניזה — עליית גג שאליה נדחפו במשך מאות שנים שרידים בלויים של כתבי יד.

הן לקחו איתן דפים שנראו להן מעניינים, והראו כמה מהם לידידן החוקר שלמה זלמן (סולומון) שכטר בקמברידג'. כששכטר ראה את הדפים הוא הבין מיד שמדובר במשהו יוצא דופן: הנוסח העברי המקורי של ספר בן סירא, שעד אז שרד רק בתרגומים.

שכטר נסע לקהיר, שכנע את הקהילה המקומית, וחזר לקמברידג' עם כ-193,000 קטעים. אבל הוא לא היה היחיד וגם לא היה הראשון — ולאורך השנים התפזרו מאות אלפי קטעים נוספים לספריות ברחבי העולם.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === 2. What is a Genizah? ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('help_outline').classes('text-2xl text-primary')
            h2('מה זו גניזה?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
במסורת היהודית אסור להשליך דפים שכתוב בהם שם ה'. נהגו לאסוף אותם במקום מיוחד — גניזה — עד שיובאו לקבורה. אבל בפועל הרבה אנשים דחפו לגניזה כל דבר שנכתב באותיות עבריות, שנחשבו לקדושות.

וכך הצטברו בעליית הגג של בית הכנסת בן עזרא לא רק ספרי קודש, אלא גם מכתבים אישיים ועסקיים, חוזים, כתובות ושטרי גירושין, רשימות קניות, מרשמים רפואיים, קמיעות, ואפילו דפים בערבית ובשפות אחרות שנקלעו לשם.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === 3. Where are the fragments today? ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('public').classes('text-2xl text-primary')
            h2('איפה הקטעים היום?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
שכטר לא היה הראשון ולא היחיד שעלה על האוצר. במהלך השנים התפזרו הדפים לספריות ברחבי העולם:
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

        # Collection cards
        collections = [
            ('קמברידג\'', '~193,000', 'אוסף טיילור-שכטר'),
            ('ניו יורק (JTS)', '~43,000', 'אוסף אדלר (ENA)'),
            ('סנקט פטרבורג', '~17,000', 'אוסף פירקוביץ\''),
            ('אוקספורד', '~4,000', 'ספריית הבודליאנה'),
            ('מנצ\'סטר', '~11,000', 'ספריית ג\'ון ריילנדס'),
            ('לונדון', '~7,000', 'הספרייה הבריטית'),
        ]

        with ui.row().classes('w-full flex-wrap gap-3 justify-center'):
            for name, count, desc in collections:
                with ui.card().classes('p-3 text-center').style('min-width: 140px; background: var(--bg-secondary);'):
                    ui.label(name).classes('font-bold').style('color: var(--primary-700);')
                    ui.label(count).classes('text-lg font-bold').style('color: var(--text-primary);')
                    ui.label(desc).classes('text-xs').style('color: var(--text-tertiary);')

        ui.markdown('''
ועוד עשרות אוספים קטנים יותר בפריז, בודפסט, פילדלפיה, ירושלים ועוד.

(הערה: באוקספורד יש ~4,000 קטעים אך ~25,000 עמודים — הקטעים שם גדולים במיוחד, לעתים מחברות שלמות.)
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right; margin-top: 1rem;')

    # === 4. The Research Revolution ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('science').classes('text-2xl text-primary')
            h2('מהפכה במחקר', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
החוקרים התחילו במרץ רב לסרוק את הגניזה, שעשתה **מהפכה בכל תחום שהיא רק נגעה בו**: היסטוריה, תלמוד, פיוט, מחשבת ישראל, מאגיה, ליטורגיה, מקרא, הלכה.

דמויות חדשות צצו ועלו, גרסאות מעולות של חיבורים ידועים ולא ידועים, פרטים היסטוריים חדשים — כל אלה עלו ועדיין עולים מתוך אותם דפים בלויים וקרועים.

**הבעיה?** זה פשוט יותר מדי חומר.

יש מאות אלפי דפים, מפוזרים בעשרות ספריות. יותר ממאה שנה חוקרים עובדים לקטלג ולתאר את הקטעים, והם רחוקים ממיצוי. **עד היום, גילויים חדשים מהגניזה הם דבר שבשגרה.**
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === 5. The Digital Age ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('computer').classes('text-2xl text-primary')
            h2('העידן הדיגיטלי', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**[פרויקט הגניזה של פרידברג (FGP)](https://fjms.genizah.org/)** — דב פרידברג, נדבן קנדי, גייס את פרופ' יעקב שויקה, ממפתחי [פרויקט השו"ת](https://www.responsa.co.il/) המפורסם, להקמת מפעל דיגיטלי שיאסוף ויקטלג את כל קטעי הגניזה הקהירית. הפרויקט הצליח בענק: מאות אלפי תמונות, רשימת מצאי מקיפה, מידע קטלוגי וביבליוגרפי מצוותי מומחים. במהרה הפך האתר למרכז של כל חוקר שעוסק בגניזה.

**[פרויקט "כתיב"](https://web.nli.org.il/sites/nlis/he/manuscript)** של הספרייה הלאומית, שנתמך גם הוא על ידי פרידברג, מטרתו דיגיטציה מלאה של כלל כתבי היד העבריים הידועים. כל המידע מפרויקט הגניזה של פרידברג הועבר אליו, וכיום ניתן לגשת לחלק גדול מהמידע גם באתר הספרייה הלאומית.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === 6. MiDRASH Project ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('psychology').classes('text-2xl text-primary')
            h2('פרויקט MiDRASH', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
האתר שלפניכם מבוסס על **[פרויקט MiDRASH](https://www.midrashproject.org/)** — פרויקט בתמיכת האיחוד האירופי שזכה במענק של 10 מיליון יורו. ארבעה חוקרים — אבי שמידמן ונחום דרשוביץ מישראל, ודניאל שטוקל בן עזרא ויהודית אולשובי-שלנגר מאירופה — שילבו את מיטב הידע שלהם בפלאוגרפיה, בתוכן כתבי היד ובמדעי הרוח הדיגיטליים, ופיתחו כלים לקריאה אוטומטית של כתבי יד עבריים.

זו מלאכה קשה מנשוא: לכתבי יד יש צורות שונות, הם כתובים במבנה לא אחיד, לפעמים עם הערות בצד, באמצע, מלמעלה למטה ובאלכסון. אבל הם הצליחו — לא במאה אחוז, רחוק מזה, אבל תוצאה טובה בהרבה ממה שהיינו יכולים לדמיין רק לפני עשר שנים.

בחנוכה תשפ"ה שחרר הפרויקט קובץ של תעתיקים אוטומטיים של כמעט כל קטעי הגניזה הקהירית — והאתר הזה נבנה כדי לאפשר חיפוש ועיון בתוכם.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === 7. About the Transcriptions ===
    with ui.card().classes('w-full p-6').style('border: 2px solid var(--primary-500); background: var(--bg-tertiary);'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('info').classes('text-2xl text-primary')
            h2('על התעתוקים באתר', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
התעתוקים באתר נוצרו באופן אוטומטי על ידי מערכת הבינה המלאכותית של פרויקט MiDRASH, ולא עברו הגהה אנושית. כתבי יד הם אתגר קשה לקריאה ממוחשבת: כל סופר כותב אחרת, הדיו דוהה, הקלף נשחק, ולפעמים הטקסט פשוט לא קריא. התוצאה היא שהתעתוקים מכילים שגיאות רבות — החלפות בין אותיות דומות (ד/ר, ה/ח, ו/י), מילים שהמערכת לא הצליחה לפענח, ולפעמים קריאות שגויות לחלוטין.

אז למה בכל זאת להשתמש בהם? כי עד עכשיו לא היה מאגר אחוד שאפשר לחפש בו. תעתיקים ידניים קיימים — ב-FGP (כ-4% מהקטעים), ב[פרויקט הגניזה של פרינסטון (PGP)](https://geniza.princeton.edu/), ובספרים ומאמרים מפוזרים ברחבי העולם — אבל הם מכסים רק חלק קטן מהגניזה ואינם ניתנים לחיפוש במקום אחד. התעתיק האוטומטי של MiDRASH מכסה כמעט את כל הגניזה, וגם אם הוא רחוק משלמות, הוא מאפשר לראשונה חיפוש רוחבי על פני כל החומר. כלי החיפוש באתר תוכננו לעזור להתמודד עם השגיאות ולמצוא תוצאות למרות הרעש.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === 8. Who is this for? ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('groups').classes('text-2xl text-primary')
            h2('למי האתר מיועד?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
האתר פותח בעיקר עבור חוקרים — היסטוריונים, חוקרי ספרות, בלשנים ואחרים שיודעים לקרוא כתבי יד ולהעריך את הממצאים שלהם.

אבל גם אם אינכם חוקרים, אתם מוזמנים לשוטט. אפשר לחפש מילים, לדפדף בתמונות של כתבי יד בני מאות שנים, ולהתרשם מהעושר של הגניזה. רק זכרו — אם מצאתם משהו שנראה מעניין, כדאי לבדוק עם מומחה אם זה באמת גילוי חדש.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === 9. What can you do on this site? ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('explore').classes('text-2xl text-primary')
            h2('מה אפשר לעשות באתר?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**[חיפוש טקסט](/search)** — הזינו מילה או ביטוי וקבלו רשימה של כל הקטעים שבהם הם מופיעים. מצבי החיפוש השונים מאפשרים להתמודד עם שגיאות התעתוק: חיפוש "וריאנטים" מוצא גם צורות עם אותיות מוחלפות (ואפשר לשחק עם עומק הווריאנטים), וחיפוש "עמום" (fuzzy) מוצא מילים דומות גם אם לא זהות. אפשר לחפש גם בכותרות החיבורים.

**[חיפוש מקבילות](/parallels)** — הזינו טקסט שלם (פיוט, קטע מפירוש, מקור ידוע) והמערכת תחפש קטעי גניזה שמכילים קטעים דומים. כך אפשר למצוא עדי נוסח חדשים או ציטוטים לא ידועים.

**[עיון בכתבי יד](/browse)** — דפדפו בתמונות של כתבי היד לצד התעתוק האוטומטי. אפשר לראות את המקור כדי לקרוא את כתב היד באופן בלתי אמצעי ולהתרשם ממנו. ניתן גם לערוך את הטקסט ולשלוח תיקונים, ולכתוב הערות על כתב היד.

**[תוכנת Genizah Search Pro](/download)** — כל היכולות של האתר זמינות גם בתוכנה חינמית למחשב, עם כלים נוספים לחוקרים מתקדמים.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

        # Call to action
        with ui.row().classes('w-full justify-center gap-4 mt-4 flex-wrap'):
            ui.button('התחילו לחפש', icon='search', on_click=lambda: ui.navigate.to('/search')).props('color=primary')
            ui.button('דפדפו בכתבי יד', icon='menu_book', on_click=lambda: ui.navigate.to('/browse')).props('outline')

    # === 10. Further Reading ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('link').classes('text-2xl text-primary')
            h2('קריאה נוספת', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**אתרים:**
- [פרויקט הגניזה של פרינסטון (PGP)](https://geniza.princeton.edu/)
- [כתיב — הספרייה הלאומית](https://web.nli.org.il/sites/nlis/he/manuscript)
- [יחידת מחקר הגניזה בקמברידג'](https://www.lib.cam.ac.uk/collections/departments/taylor-schechter-genizah-research-unit)

**ספרים:**
- *Sacred Trash* (אדינה הופמן ופיטר קול) — סיפור הגילוי בשפה נגישה
- *[The Illustrated Cairo Genizah](https://www.medievalists.net/2025/01/new-medieval-books-the-illustrated-cairo-genizah/)* (ניק פוסגיי ומלוני שמירר-לי) — מבוא מאויר לגניזה
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === 11. Credits ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('favorite').classes('text-2xl text-primary')
            h2('תודות', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
תודה מיוחדת לפרופ' משה קופל, מייסד וראש [דיקטה](https://dicta.org.il/), על התמיכה; לד"ר אבי שמידמן, ראש אגף הפיתוח הטכנולוגי של דיקטה ואחד מארבעת חוקרי פרויקט MiDRASH שהביאו את התעתיקים לעולם; ולאלישע רוזנצווייג, אפרים מאירי, אלעזר גרשוני, איתי קגן, אלנתן חן ועדיאל ברויאר על העצות והתמיכה.

תודה לצוות פרויקט MiDRASH המלא: דניאל שטוקל בן עזרא, מרינה רוסטו, נחום דרשוביץ, יהודית אולשובי-שלנגר, לואיג'י במבצ'י, בנימין קיסלינג, חיים לפין, נורית עזר, אלנה לולי, צפרא סיו, יצחק גילה, בראת קוראר ברכאת, שרווה גוגאוואלה, משה לביא, ורד רזיאל-קרצמר ודריה וסיוטינסקי שפירא.

ותודה למשתמשים שכבר משתמשים באתר ובתוכנת Genizah Search Pro למחשב ומשתפים בהתלהבות את התגליות שלהם.

**יוצר האתר:** הלל גרשוני ([gershuni@gmail.com](mailto:gershuni@gmail.com))

*לזכרו של מורנו ורבנו, פרופ' מנחם כהנא ז"ל.*
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')


def _create_english_content():
    """Create the English about content."""

    # === 1. The Discovery Story ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('auto_stories').classes('text-2xl text-primary')
            h2('The Discovery', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
In 1896, Agnes Smith Lewis and Margaret Dunlop Gibson, two Scottish sisters and accomplished scholars in their own right, visited Cairo. They entered the ancient Ben Ezra Synagogue, where their guide showed them the genizah chamber — an attic where worn fragments of manuscripts had been deposited for hundreds of years.

They took a few pages that seemed interesting and showed them to their friend at Cambridge, Solomon Schechter. When Schechter saw the pages, he immediately realized he was looking at something extraordinary: the original Hebrew text of the Book of Ben Sira, which until then had survived only in translations.

Schechter traveled to Cairo, convinced the local community, and returned to Cambridge with approximately 193,000 fragments. But he wasn't the only one — over the years, hundreds of thousands more fragments made their way to libraries around the world.
        ''').style('color: var(--text-secondary);')

    # === 2. What is a Genizah? ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('help_outline').classes('text-2xl text-primary')
            h2('What is a Genizah?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
In Jewish tradition, it's forbidden to throw away papers containing God's name. Such papers were collected in a special place — a genizah — until they could be buried. But in practice, many people deposited anything written in Hebrew letters into the genizah, as Hebrew letters were considered sacred.

And so, in the attic of the Ben Ezra Synagogue accumulated not only sacred books, but also personal and business letters, contracts, marriage documents and divorce deeds, shopping lists, medical prescriptions, amulets, and even pages in Arabic and other languages that found their way there.
        ''').style('color: var(--text-secondary);')

    # === 3. Where are the fragments today? ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('public').classes('text-2xl text-primary')
            h2('Where are the Fragments Today?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Schechter wasn't the first or only one to discover the treasure. Over the years, fragments spread to libraries around the world:
        ''').style('color: var(--text-secondary);')

        # Collection cards
        collections = [
            ('Cambridge', '~193,000', 'Taylor-Schechter Collection'),
            ('New York (JTS)', '~43,000', 'Adler Collection (ENA)'),
            ('St. Petersburg', '~17,000', 'Firkovich Collection'),
            ('Oxford', '~4,000', 'Bodleian Library'),
            ('Manchester', '~11,000', 'John Rylands Library'),
            ('London', '~7,000', 'British Library'),
        ]

        with ui.row().classes('w-full flex-wrap gap-3 justify-center'):
            for name, count, desc in collections:
                with ui.card().classes('p-3 text-center').style('min-width: 140px; background: var(--bg-secondary);'):
                    ui.label(name).classes('font-bold').style('color: var(--primary-700);')
                    ui.label(count).classes('text-lg font-bold').style('color: var(--text-primary);')
                    ui.label(desc).classes('text-xs').style('color: var(--text-tertiary);')

        ui.markdown('''
Plus dozens of smaller collections in Paris, Budapest, Philadelphia, Jerusalem, and more.

(Note: Oxford has ~4,000 fragments but ~25,000 folios — the fragments there are unusually large, sometimes entire quires.)
        ''').style('color: var(--text-secondary); margin-top: 1rem;')

    # === 4. The Research Revolution ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('science').classes('text-2xl text-primary')
            h2('A Research Revolution', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Scholars eagerly began combing through the Genizah, which **revolutionized every field it touched**: history, Talmud, poetry, Jewish thought, magic, liturgy, Bible studies, Jewish law.

New figures emerged, superior versions of known and unknown works, new historical details — all rising from those worn and torn pages, and still emerging today.

**The problem?** It's simply too much material.

There are hundreds of thousands of pages, scattered across dozens of libraries. For over a century, scholars have worked to catalog and describe the fragments, and they're far from finished. **To this day, new discoveries from the Genizah are routine.**
        ''').style('color: var(--text-secondary);')

    # === 5. The Digital Age ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('computer').classes('text-2xl text-primary')
            h2('The Digital Age', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**[The Friedberg Genizah Project (FGP)](https://fjms.genizah.org/)** — Dov Friedberg, a Canadian philanthropist, enlisted Prof. Yaacov Choueka, one of the developers of the renowned [Responsa Project](https://www.responsa.co.il/), to create a digital enterprise that would collect and catalog all Cairo Genizah fragments. The project succeeded enormously: hundreds of thousands of images, comprehensive inventories, and cataloging information from teams of experts. The site quickly became the center for every Genizah researcher.

**[The Ktiv Project](https://web.nli.org.il/sites/nlis/en/manuscript)** of the National Library of Israel, also supported by Friedberg, aims to fully digitize all known Hebrew manuscripts. All information from the Friedberg Genizah Project was transferred to it, and today much of this information is accessible through the National Library's website.
        ''').style('color: var(--text-secondary);')

    # === 6. MiDRASH Project ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('psychology').classes('text-2xl text-primary')
            h2('The MiDRASH Project', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This website is based on the **[MiDRASH Project](https://www.midrashproject.org/)** — an EU-funded project that received a grant of 10 million euros. Four researchers — Avi Shmidman and Nachum Dershowitz from Israel, and Daniel Stökl Ben Ezra and Judith Olszowy-Schlanger from Europe — combined their expertise in paleography, manuscript content, and digital humanities to develop tools for automatic reading of Hebrew manuscripts.

This is an enormously difficult task: manuscripts have different forms, they're written in inconsistent layouts, sometimes with notes on the side, in the middle, from top to bottom, and diagonally. But they succeeded — not 100%, far from it, but results far better than we could have imagined just ten years ago.

In December 2024, the project released a file of automatic transcriptions of nearly all Cairo Genizah fragments — and this website was built to enable searching and browsing within them.
        ''').style('color: var(--text-secondary);')

    # === 7. About the Transcriptions ===
    with ui.card().classes('w-full p-6').style('border: 2px solid var(--primary-500); background: var(--bg-tertiary);'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('info').classes('text-2xl text-primary')
            h2('About the Transcriptions', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
The transcriptions on this site were created automatically by the MiDRASH project's AI system and have not undergone human review. Manuscripts are a difficult challenge for computer reading: every scribe writes differently, ink fades, parchment wears, and sometimes the text is simply illegible. The result is that the transcriptions contain many errors — substitutions between similar letters (ד/ר, ה/ח, ו/י), words the system couldn't decipher, and sometimes completely wrong readings.

So why use them at all? Because until now there was no unified database that could be searched. Manual transcriptions exist — in FGP (about 4% of fragments), in the [Princeton Geniza Project (PGP)](https://geniza.princeton.edu/), and in books and articles scattered around the world — but they cover only a small portion of the Genizah and cannot be searched in one place. The MiDRASH automatic transcription covers almost the entire Genizah, and even if it's far from perfect, it enables for the first time a broad search across all the material. The search tools on this site were designed to help cope with errors and find results despite the noise.
        ''').style('color: var(--text-secondary);')

    # === 8. Who is this for? ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('groups').classes('text-2xl text-primary')
            h2('Who is this Site For?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This site was developed primarily for researchers — historians, literature scholars, linguists, and others who can read manuscripts and evaluate their findings.

But even if you're not a researcher, you're welcome to explore. You can search for words, browse images of centuries-old manuscripts, and appreciate the richness of the Genizah. Just remember — if you find something that seems interesting, it's worth checking with an expert whether it's truly a new discovery.
        ''').style('color: var(--text-secondary);')

    # === 9. What can you do on this site? ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('explore').classes('text-2xl text-primary')
            h2('What Can You Do on This Site?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**[Text Search](/search)** — Enter a word or phrase and get a list of all fragments where they appear. Different search modes help cope with transcription errors: "variants" search finds forms with substituted letters (and you can adjust the variant depth), and "fuzzy" search finds similar words even if not identical. You can also search within composition titles.

**[Parallels Search](/parallels)** — Enter a complete text (a piyyut, a commentary excerpt, a known source) and the system will search for Genizah fragments containing similar passages. This way you can find new textual witnesses or unknown citations.

**[Browse Manuscripts](/browse)** — Browse images of manuscripts alongside the automatic transcription. You can view the source to read the manuscript directly and get an impression of it. You can also edit the text and submit corrections, and write notes about the manuscript.

**[Genizah Search Pro](/download)** — All the site's capabilities are also available in a free desktop application, with additional tools for advanced researchers.
        ''').style('color: var(--text-secondary);')

        # Call to action
        with ui.row().classes('w-full justify-center gap-4 mt-4 flex-wrap'):
            ui.button('Start Searching', icon='search', on_click=lambda: ui.navigate.to('/search')).props('color=primary')
            ui.button('Browse Manuscripts', icon='menu_book', on_click=lambda: ui.navigate.to('/browse')).props('outline')

    # === 10. Further Reading ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('link').classes('text-2xl text-primary')
            h2('Further Reading', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**Websites:**
- [Princeton Geniza Project (PGP)](https://geniza.princeton.edu/)
- [Ktiv — National Library of Israel](https://web.nli.org.il/sites/nlis/en/manuscript)
- [Cambridge Genizah Research Unit](https://www.lib.cam.ac.uk/collections/departments/taylor-schechter-genizah-research-unit)

**Books:**
- *Sacred Trash* (Adina Hoffman & Peter Cole) — The discovery story in accessible prose
- *[The Illustrated Cairo Genizah](https://www.medievalists.net/2025/01/new-medieval-books-the-illustrated-cairo-genizah/)* (Nick Posegay & Melonie Schmierer-Lee) — An illustrated introduction to the Genizah
        ''').style('color: var(--text-secondary);')

    # === 11. Credits ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('favorite').classes('text-2xl text-primary')
            h2('Acknowledgments', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Special thanks to Prof. Moshe Koppel, founder and head of [Dicta](https://dicta.org.il/), for his support; to Dr. Avi Shmidman, head of technology development at Dicta and one of the four MiDRASH project researchers who brought these transcriptions to the world; and to Elisha Rosenzweig, Ephraim Meiri, Elazar Gershuni, Itai Kagan, Elnatan Chen, and Adiel Breuer for their advice and support.

Thanks to the full MiDRASH project team: Daniel Stökl Ben Ezra, Marina Rustow, Nachum Dershowitz, Judith Olszowy-Schlanger, Luigi Bambaci, Benjamin Kiessling, Hayim Lapin, Nurit Ezer, Elena Lolli, Tsafra Siew, Yitzchak Gila, Berat Kurar Barakat, Sharva Gogawale, Moshe Lavee, Vered Raziel-Kretzmer, and Daria Vasyutinsky Shapira.

And thanks to the users already using the site and Genizah Search Pro desktop application, who enthusiastically share their discoveries.

**Site Creator:** Hillel Gershuni ([gershuni@gmail.com](mailto:gershuni@gmail.com))

*Dedicated to the memory of our beloved teacher, Prof. Menachem Kahana z"l.*
        ''').style('color: var(--text-secondary);')
