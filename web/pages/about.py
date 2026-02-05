# -*- coding: utf-8 -*-
"""
About Page - What is the Cairo Genizah?

An introductory page for general audiences explaining:
1. What is a genizah and why the Cairo Genizah is special
2. The discovery story (1896-1897)
3. What's in the collection
4. Where the fragments are today
5. About this website and the transcription challenge
6. Further reading and resources
"""

from nicegui import ui
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

    # === Hero / Introduction ===
    with ui.card().classes('w-full p-6'):
        with ui.column().classes('gap-4'):
            # Compelling headline
            ui.label('400,000 שברי עבר — חלון לחיי יהודי ימי הביניים').classes(
                'text-2xl font-bold'
            ).style('color: var(--primary-700); direction: rtl; text-align: right;')

            ui.markdown('''
גניזת קהיר היא אחד הגילויים הארכיאולוגיים החשובים ביותר בתולדות העם היהודי.
מדובר באוסף של כ-400,000 קטעי כתבי יד מימי הביניים שהתגלו בעליית גג של בית כנסת עתיק בקהיר —
ושינו את כל מה שחשבנו שאנחנו יודעים על החיים היהודיים לפני אלף שנה.
            ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right; font-size: 1.1rem;')

    # === What is a Genizah? ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('help_outline').classes('text-2xl text-primary')
            h2('מה זו בכלל גניזה?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
במסורת היהודית, אסור להשליך לאשפה דפים שכתוב בהם שם ה'. לכן נהגו לאסוף דפים בלויים של ספרי קודש
ולאחסן אותם במקום מיוחד — **גניזה** — עד שיובאו לקבורה בבית קברות.

**אבל בקהיר קרה משהו מיוחד.**

יהודי קהיר בימי הביניים הרחיבו את המנהג: כמעט כל דבר שנכתב בעברית (ולפעמים גם בערבית באותיות עבריות)
הגיע לגניזה — גם אם לא היה בו שם קדוש. וכך נדחפו לעליית הגג של בית הכנסת בן עזרא לא רק
ספרי תורה ותפילה, אלא גם:

- 📜 **מכתבים אישיים** — אהבה, עסקים, משפחה
- 📋 **רשימות קניות** — מה קנו בשוק לפני אלף שנה
- 💍 **כתובות ושטרות גירושין** — חיי המשפחה היהודית
- 💊 **מרשמים רפואיים** — איך ריפאו מחלות
- ✨ **קמיעות ולחשים** — המאגיה של ימי הביניים
- 📖 **ספרים נדירים** — יצירות שלא שרדו בשום מקום אחר

**התוצאה:** תמונה מפורטת להפליא של החיים היהודיים בעולם הים תיכוני במשך כאלף שנה.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === The Discovery Story ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('auto_stories').classes('text-2xl text-primary')
            h2('סיפור הגילוי: 1896-1897', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
לפני מאה ומשהו שנה הגיעו **שתי אחיות סקוטיות מלומדות**, אגנס לואיס ומרגרט גיבסון, לביקור בקהיר.
הן נכנסו לבית כנסת עתיק והמדריך הראה להן את חדר הגניזה — עליית גג שאליה דחפו במשך מאות שנים
שרידים בלויים של כתבי קודש.

הן תפסו מהגניזה דף אחד שהיה נראה להן מעניין, והחליטו להביא אותו לחבר שלהן בקמברידג' —
**שלמה זלמן (סולומון) שכטר**, חוקר יהודי ממוצא רומני.

**כששכטר ראה את הדפים הוא כמעט התעלף.**

מולו עמד לראשונה הנוסח המקורי בעברית של **ספר בן סירא** — ספר מימי בית שני
ששרד עד אז רק בתרגומים ליוונית ולסורית. שכטר הבין מיד שיש עוד הרבה זהב מאיפה שזה הגיע.

הוא גייס כסף, נסע לקהיר, ושכנע את הקהילה היהודית למסור לו את תכולת הגניזה.
בשנת 1897 הוא שינע לקמברידג' **כ-193,000 קטעים** — מה שהפך לאוסף טיילור-שכטר המפורסם.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

        # Timeline visual
        with ui.row().classes('w-full justify-center my-4 flex-wrap gap-2'):
            _timeline_item('1896', 'האחיות מגלות את הגניזה', is_hebrew=True)
            ui.icon('arrow_back').classes('text-primary mx-2 hidden sm:block')
            _timeline_item('1897', 'שכטר מגיע לקהיר', is_hebrew=True)
            ui.icon('arrow_back').classes('text-primary mx-2 hidden sm:block')
            _timeline_item('1898', 'האוסף מגיע לקמברידג\'', is_hebrew=True)

    # === Where are the fragments today? ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('public').classes('text-2xl text-primary')
            h2('איפה הקטעים היום?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
שכטר לא היה הראשון ולא היחיד שעלה על האוצר. במהלך השנים התפזרו הדפים לספריות ברחבי העולם:
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

        # Collection cards
        collections = [
            ('קמברידג\'', '~193,000', 'אוסף טיילור-שכטר — הגדול ביותר', 'CUL'),
            ('ניו יורק (JTS)', '~40,000', 'אוסף אדלר — ENA', 'JTS'),
            ('סנקט פטרבורג', '~17,000', 'אוסף פירקוביץ\' — קראים', 'RNL'),
            ('אוקספורד', '~25,000', 'ספריית הבודליאנה', 'Oxford'),
            ('מנצ\'סטר', '~11,000', 'ספריית ג\'ון ריילנדס', 'Manchester'),
            ('לונדון', '~8,000', 'הספרייה הבריטית', 'BL'),
        ]

        with ui.row().classes('w-full flex-wrap gap-3 justify-center'):
            for name, count, desc, code in collections:
                with ui.card().classes('p-3 text-center').style('min-width: 140px; background: var(--bg-secondary);'):
                    ui.label(name).classes('font-bold').style('color: var(--primary-700);')
                    ui.label(count).classes('text-lg font-bold').style('color: var(--text-primary);')
                    ui.label(desc).classes('text-xs').style('color: var(--text-tertiary);')

        ui.markdown('''
**ועוד עשרות אוספים קטנים יותר** בפריז, בודפסט, פילדלפיה, ירושלים ועוד.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right; margin-top: 1rem;')

    # === The Research Revolution ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('science').classes('text-2xl text-primary')
            h2('מהפכה במחקר', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
החוקרים התחילו במרץ רב לסרוק את הגניזה, שעשתה **מהפכה בכל תחום שהיא רק נגעה בו**:
היסטוריה, תלמוד, פיוט, מחשבת ישראל, מאגיה, ליטורגיה, מקרא, הלכה.

דמויות חדשות צצו ועלו, גרסאות מעולות של חיבורים ידועים ולא ידועים, פרטים היסטוריים חדשים —
כל אלה עלו ועדיין עולים מתוך אותם דפים בלויים וקרועים.

**הבעיה?** זה פשוט יותר מדי חומר.

יש מאות אלפי דפים, מפוזרים בעשרות ספריות. יותר ממאה שנה חוקרים עובדים לקטלג ולתאר את הקטעים,
והם רחוקים ממיצוי. **עד היום, גילויים חדשים מהגניזה הם דבר שבשגרה.**
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === The Digital Age ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('computer').classes('text-2xl text-primary')
            h2('העידן הדיגיטלי', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
כמה פרויקטים דיגיטליים הזניקו את חקר הגניזה קדימה:

**פרויקט הגניזה של פרידברג (FGP)** — אלברט פרידברג, נדבן מקנדה, גייס את פרופ' יעקב שויקה
להקמת מפעל דיגיטלי שיאסוף ויקטלג את כל קטעי הגניזה. הפרויקט הצליח בענק: מאות אלפי תמונות,
רשימת מצאי מקיפה, מידע קטלוגי וביבליוגרפי ממומחים.

**פרויקט "כתיב"** של הספרייה הלאומית — דיגיטציה מלאה של כלל כתבי היד העבריים הידועים.

**פרויקט MiDRASH** — בתמיכת האיחוד האירופי, ארבעה חוקרים (אבי שמידמן, נחום דרשוביץ,
דניאל שטוקל בן עזרא ויהודית אולשובי-שלנגר) פיתחו כלים ל**קריאה אוטומטית של כתבי יד עבריים**
באמצעות בינה מלאכותית.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === About This Website - THE KEY SECTION ===
    with ui.card().classes('w-full p-6').style('border: 2px solid var(--primary-500); background: var(--bg-tertiary);'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('info').classes('text-2xl text-primary')
            h2('על האתר הזה', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
### מה זה "חיפוש גניזת קהיר"?

פרויקט MiDRASH שחרר קובץ ענק של **תעתיקים אוטומטיים** של כמעט כל קטעי הגניזה הקהירית.
האתר הזה מאפשר לחפש בתוך התעתוקים האלה בצורה מהירה וחכמה.

### למה הטקסטים לפעמים נראים מוזרים? 🤔

**שאלה מצוינת!** התעתיקים באתר נוצרו על ידי מחשב, לא על ידי אדם.

כתבי היד בגניזה נכתבו לפני מאות שנים, בכתב יד של אנשים שונים. חלקם בלויים, דהויים, מחוקים או קרועים.
המחשב מנסה "לקרוא" אותם — בדיוק כמו שאנחנו מנסים לפענח כתב יד של מישהו אחר, רק שהמחשב עושה את זה
עם מאות אלפי דפים.

**לפעמים המחשב טועה:**
- מבלבל בין **ד'** ל-**ר'** (הן נראות דומה בכתב יד!)
- מבלבל בין **ה'** ל-**ח'** או **ו'** ל-**י'**
- מפספס מילים שדהו או נמחקו
- ממציא מילים שלא קיימות

**וזה בסדר גמור.** חוקרים יודעים להתמודד עם זה.

לכן פיתחנו **כלי חיפוש חכמים** שיודעים להתחשב בטעויות האלה. כשאתם מחפשים מילה, המערכת
יכולה למצוא אותה גם אם המחשב קרא אותה בטעות בצורה קצת אחרת.

### למי האתר מיועד?

האתר פותח בעיקר עבור **חוקרים מקצועיים** — היסטוריונים, חוקרי ספרות, בלשנים ואחרים
שיודעים לקרוא כתבי יד ולהעריך את הממצאים שלהם.

**אבל גם אתם מוזמנים לשוטט!**

אפשר לחפש מילים, לדפדף בתמונות של כתבי יד בני מאות שנים, ולהתרשם מהעושר של הגניזה.
רק זכרו — אם מצאתם משהו שנראה מעניין, כדאי לבדוק עם מומחה אם זה באמת גילוי חדש. 😊
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

        # Call to action
        with ui.row().classes('w-full justify-center gap-4 mt-4 flex-wrap'):
            ui.button('התחילו לחפש', icon='search', on_click=lambda: ui.navigate.to('/search')).props('color=primary')
            ui.button('דפדפו בכתבי יד', icon='menu_book', on_click=lambda: ui.navigate.to('/browse')).props('outline')

    # === Further Reading ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('link').classes('text-2xl text-primary')
            h2('קריאה נוספת ומשאבים', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**אתרים מומלצים:**
- [פרויקט הגניזה של פרינסטון](https://geniza.princeton.edu/) — מאגר מחקרי עם כלי חיפוש מתקדמים
- [כתיב — הספרייה הלאומית](https://web.nli.org.il/sites/nlis/he/manuscript) — צפייה בתמונות כתבי יד
- [יחידת מחקר הגניזה בקמברידג'](https://www.lib.cam.ac.uk/collections/departments/taylor-schechter-genizah-research-unit) — האוסף הגדול בעולם
- [Princeton Geniza Lab](https://genizalab.princeton.edu/) — מעבדה לחקר הגניזה

**ספרים מומלצים:**
- *Sacred Trash* (אדינה הופמן ופיטר קול) — סיפור הגילוי בשפה נגישה
- *A Jewish Archive from Old Cairo* (סטפן ריף) — מבוא אקדמי מקיף
- *India Traders of the Middle Ages* (ש"ד גויטיין ומרדכי פרידמן) — סוחרים יהודים בהודו

**וידאו:**
- [הגניזה הקהירית — סיימון שאמא](https://www.pbs.org/wnet/story-jews/) (PBS, The Story of the Jews)
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Credits ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('favorite').classes('text-2xl text-primary')
            h2('תודות', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
האתר הזה לא היה אפשרי בלי:
- **פרויקט MiDRASH** — על התעתוקים האוטומטיים
- **פרויקט הגניזה של פרידברג (FGP)** — על המטא-דאטה והקטלוג
- **הספרייה הלאומית של ישראל** — על תמונות כתבי היד
- **ספריית אוניברסיטת קמברידג'** — על תמונות אוסף T-S
- **ספריית הבודליאנה, אוקספורד** — על תמונות כתבי יד

**יוצר האתר:** רפאל גרשוני ([gershuni@gmail.com](mailto:gershuni@gmail.com))
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')


def _create_english_content():
    """Create the English about content."""

    # === Hero / Introduction ===
    with ui.card().classes('w-full p-6'):
        with ui.column().classes('gap-4'):
            ui.label('400,000 Fragments of the Past — A Window into Medieval Jewish Life').classes(
                'text-2xl font-bold'
            ).style('color: var(--primary-700);')

            ui.markdown('''
The Cairo Genizah is one of the most important archaeological discoveries in Jewish history.
It's a collection of approximately 400,000 manuscript fragments from the Middle Ages,
discovered in the attic of an ancient synagogue in Cairo — transforming everything we thought
we knew about Jewish life a thousand years ago.
            ''').style('color: var(--text-secondary); font-size: 1.1rem;')

    # === What is a Genizah? ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('help_outline').classes('text-2xl text-primary')
            h2('What is a Genizah?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
In Jewish tradition, it's forbidden to throw away papers containing God's name. Instead, worn-out
sacred texts were stored in a special place — a **genizah** — until they could be buried in a cemetery.

**But something special happened in Cairo.**

Medieval Cairo's Jews expanded this practice: almost anything written in Hebrew (and sometimes Arabic
in Hebrew script) went into the genizah — even without a sacred name. And so, into the attic of the
Ben Ezra Synagogue went not only Torah scrolls and prayer books, but also:

- 📜 **Personal letters** — love, business, family
- 📋 **Shopping lists** — what people bought at the market a thousand years ago
- 💍 **Marriage contracts and divorce deeds** — Jewish family life
- 💊 **Medical prescriptions** — how diseases were treated
- ✨ **Amulets and spells** — medieval magic
- 📖 **Rare books** — works that survived nowhere else

**The result:** An astonishingly detailed picture of Jewish life in the Mediterranean world over nearly a millennium.
        ''').style('color: var(--text-secondary);')

    # === The Discovery Story ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('auto_stories').classes('text-2xl text-primary')
            h2('The Discovery: 1896-1897', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Over a century ago, **two scholarly Scottish sisters**, Agnes Lewis and Margaret Gibson, visited Cairo.
They entered an ancient synagogue where their guide showed them the genizah chamber — an attic stuffed
for centuries with worn fragments of sacred writings.

They grabbed a page that looked interesting and decided to show it to their friend at Cambridge —
**Solomon Schechter**, a Jewish scholar of Romanian origin.

**When Schechter saw the pages, he nearly fainted.**

Before him stood the first-ever Hebrew original of the **Book of Ben Sira** — a text from the Second
Temple period that had survived only in Greek and Syriac translations. Schechter immediately understood
there was much more treasure where this came from.

He raised funds, traveled to Cairo, and convinced the Jewish community to hand over the genizah's contents.
In 1897, he shipped approximately **193,000 fragments** to Cambridge — what became the famous Taylor-Schechter Collection.
        ''').style('color: var(--text-secondary);')

        # Timeline visual
        with ui.row().classes('w-full justify-center my-4 flex-wrap gap-2'):
            _timeline_item('1896', 'Sisters discover the Genizah', is_hebrew=False)
            ui.icon('arrow_forward').classes('text-primary mx-2 hidden sm:block')
            _timeline_item('1897', 'Schechter arrives in Cairo', is_hebrew=False)
            ui.icon('arrow_forward').classes('text-primary mx-2 hidden sm:block')
            _timeline_item('1898', 'Collection reaches Cambridge', is_hebrew=False)

    # === Where are the fragments today? ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('public').classes('text-2xl text-primary')
            h2('Where are the Fragments Today?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Schechter wasn't the first or only one to discover the treasure. Over the years, fragments spread to libraries worldwide:
        ''').style('color: var(--text-secondary);')

        # Collection cards
        collections = [
            ('Cambridge', '~193,000', 'Taylor-Schechter Collection', 'CUL'),
            ('New York (JTS)', '~40,000', 'Adler Collection (ENA)', 'JTS'),
            ('St. Petersburg', '~17,000', 'Firkovich Collection', 'RNL'),
            ('Oxford', '~25,000', 'Bodleian Library', 'Oxford'),
            ('Manchester', '~11,000', 'John Rylands Library', 'Manchester'),
            ('London', '~8,000', 'British Library', 'BL'),
        ]

        with ui.row().classes('w-full flex-wrap gap-3 justify-center'):
            for name, count, desc, code in collections:
                with ui.card().classes('p-3 text-center').style('min-width: 140px; background: var(--bg-secondary);'):
                    ui.label(name).classes('font-bold').style('color: var(--primary-700);')
                    ui.label(count).classes('text-lg font-bold').style('color: var(--text-primary);')
                    ui.label(desc).classes('text-xs').style('color: var(--text-tertiary);')

        ui.markdown('''
**Plus dozens of smaller collections** in Paris, Budapest, Philadelphia, Jerusalem, and more.
        ''').style('color: var(--text-secondary); margin-top: 1rem;')

    # === The Research Revolution ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('science').classes('text-2xl text-primary')
            h2('A Research Revolution', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Scholars eagerly began scanning the Genizah, which **revolutionized every field it touched**:
history, Talmud, poetry, Jewish thought, magic, liturgy, Bible studies, Jewish law.

New figures emerged, superior versions of known and unknown works, new historical details —
all rising from those worn and torn pages, and still emerging today.

**The problem?** It's simply too much material.

There are hundreds of thousands of pages, scattered across dozens of libraries. For over a century,
scholars have worked to catalog and describe the fragments, and they're far from finished.
**To this day, new discoveries from the Genizah are routine.**
        ''').style('color: var(--text-secondary);')

    # === The Digital Age ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('computer').classes('text-2xl text-primary')
            h2('The Digital Age', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Several digital projects have propelled Genizah research forward:

**The Friedberg Genizah Project (FGP)** — Albert Friedberg, a Canadian philanthropist, enlisted Prof. Yaacov Choueka
to create a digital enterprise collecting and cataloging all Genizah fragments. The project succeeded magnificently:
hundreds of thousands of images, comprehensive inventories, and expert cataloging information.

**The Ktiv Project** by the National Library of Israel — full digitization of all known Hebrew manuscripts.

**The MiDRASH Project** — with European Union support, four scholars (Avi Shmidman, Nachum Dershowitz,
Daniel Stökl Ben Ezra, and Judith Olszowy-Schlanger) developed tools for **automatic reading of Hebrew manuscripts**
using artificial intelligence.
        ''').style('color: var(--text-secondary);')

    # === About This Website - THE KEY SECTION ===
    with ui.card().classes('w-full p-6').style('border: 2px solid var(--primary-500); background: var(--bg-tertiary);'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('info').classes('text-2xl text-primary')
            h2('About This Website', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
### What is "Dicta Genizah Search"?

The MiDRASH project released a massive file of **automatic transcriptions** of nearly all Cairo Genizah fragments.
This website enables fast, intelligent searching within these transcriptions.

### Why do the texts sometimes look strange? 🤔

**Great question!** The transcriptions on this site were created by a computer, not a human.

The Genizah manuscripts were written hundreds of years ago, in different people's handwriting.
Some are worn, faded, erased, or torn. The computer tries to "read" them — just like we try to
decipher someone else's handwriting, except the computer does it with hundreds of thousands of pages.

**Sometimes the computer makes mistakes:**
- Confuses **ד** (dalet) with **ר** (resh) — they look similar in handwriting!
- Confuses **ה** (he) with **ח** (het) or **ו** (vav) with **י** (yod)
- Misses words that faded or were erased
- Invents words that don't exist

**And that's perfectly fine.** Scholars know how to handle this.

That's why we developed **smart search tools** that account for these errors. When you search for a word,
the system can find it even if the computer misread it slightly differently.

### Who is this website for?

This site was developed primarily for **professional researchers** — historians, literature scholars,
linguists, and others who can read manuscripts and evaluate their findings.

**But you're welcome to explore too!**

You can search for words, browse images of centuries-old manuscripts, and appreciate the Genizah's richness.
Just remember — if you find something interesting, it's worth checking with an expert whether it's truly a new discovery. 😊
        ''').style('color: var(--text-secondary);')

        # Call to action
        with ui.row().classes('w-full justify-center gap-4 mt-4 flex-wrap'):
            ui.button('Start Searching', icon='search', on_click=lambda: ui.navigate.to('/search')).props('color=primary')
            ui.button('Browse Manuscripts', icon='menu_book', on_click=lambda: ui.navigate.to('/browse')).props('outline')

    # === Further Reading ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('link').classes('text-2xl text-primary')
            h2('Further Reading & Resources', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**Recommended Websites:**
- [Princeton Geniza Project](https://geniza.princeton.edu/) — Research database with advanced search tools
- [Ktiv — National Library of Israel](https://web.nli.org.il/sites/nlis/en/manuscript) — View manuscript images
- [Cambridge Genizah Research Unit](https://www.lib.cam.ac.uk/collections/departments/taylor-schechter-genizah-research-unit) — The world's largest collection
- [Princeton Geniza Lab](https://genizalab.princeton.edu/) — Genizah research laboratory

**Recommended Books:**
- *Sacred Trash* (Adina Hoffman & Peter Cole) — The discovery story in accessible prose
- *A Jewish Archive from Old Cairo* (Stefan Reif) — Comprehensive academic introduction
- *India Traders of the Middle Ages* (S.D. Goitein & Mordechai Friedman) — Jewish traders in India

**Video:**
- [The Cairo Genizah — Simon Schama](https://www.pbs.org/wnet/story-jews/) (PBS, The Story of the Jews)
        ''').style('color: var(--text-secondary);')

    # === Credits ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('favorite').classes('text-2xl text-primary')
            h2('Acknowledgments', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This website wouldn't be possible without:
- **The MiDRASH Project** — for the automatic transcriptions
- **The Friedberg Genizah Project (FGP)** — for metadata and cataloging
- **The National Library of Israel** — for manuscript images
- **Cambridge University Library** — for T-S Collection images
- **The Bodleian Library, Oxford** — for manuscript images

**Website Creator:** Raphael Gershuni ([gershuni@gmail.com](mailto:gershuni@gmail.com))
        ''').style('color: var(--text-secondary);')


def _timeline_item(year: str, label: str, is_hebrew: bool = False):
    """Create a timeline item."""
    with ui.column().classes('items-center'):
        ui.label(year).classes('text-lg font-bold').style('color: var(--primary-700);')
        ui.label(label).classes('text-xs text-center').style(
            f'color: var(--text-tertiary); max-width: 100px; {"direction: rtl;" if is_hebrew else ""}'
        )
