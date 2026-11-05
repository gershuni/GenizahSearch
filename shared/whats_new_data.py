# -*- coding: utf-8 -*-
"""The entries of the version history (``shared/whats_new_history.py`` reads and checks them).

Newest first. Public, reader-facing text in English and Hebrew: what a reader can
now do or what got better, no internals, no loss figures (owner's release-text
rules). Each release adds its entry here with its release texts.
"""

HISTORY = [
    {
        "version": "9.14.0",
        "date": "2026-11-05",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Dictionaries and search improvements",
            "he": "מילונים ושיפורים בחיפוש"
        },
        "items": [
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "Judeo-Arabic dictionaries on the manuscript page: press the dictionaries button (עع) at the bottom of the text pane, then click a word to see its entry in Blau's dictionary and in Lane's Lexicon. Or press D or M and then click a word, or select words and press D or M.",
                "he": "מילונים לערבית־יהודית! בעיון בכתבי יד תוכלו ללחוץ על כפתור המילונים (עع) בתחתית חלונית הטקסט. לחיצה על מילה תציג את ערכה במילון בלאו ובלקסיקון ליין. אפשר גם ללחוץ על D או M ואז על מילה, או לבחור מילים וללחוץ על D או M."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "A search box at the top of every page: type text to search it, or a shelfmark to open the manuscript.",
                "he": "תיבת חיפוש בראש כל עמוד: הקלידו טקסט כדי לחפש אותו, או מספר מדף כדי לפתוח את כתב היד."
            },
            {
                "category": "feature",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "What's New for every version since 9.0.0, and the update notice in the desktop app shows what changed since your version.",
                "he": "פירוט מה חדש בכל גרסה מאז גרסה 9.0.0, והודעת העדכון באפליקציית שולחן העבודה מציגה מה השתנה מאז הגרסה שלכם."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Go to (Ctrl+G) opens a shelfmark or system ID in Browse by Shelfmark, and searches anything else, Hebrew, Arabic or English words, on the Search tab.",
                "he": "מעבר אל (Ctrl+G) פותח מספר מדף או מספר מערכת בעיון לפי מספר מדף, וכל טקסט אחר, מילים בעברית, בערבית או באנגלית, נשלח לחיפוש בלשונית החיפוש."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Typed Hebrew with the keyboard set to English? A search that finds nothing offers \"Did you mean …?\" with the Hebrew words.",
                "he": "הקלדתם עברית כשהמקלדת על אנגלית? חיפוש שלא מצא דבר מציע \"האם התכוונתם ל…?\" עם המילים בעברית."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Searches by shelfmark answer at once, and manuscripts opened before keep their catalogue details when the National Library of Israel's servers do not answer.",
                "he": "חיפוש לפי מספר מדף מתבצע מיידית, וכתבי יד שכבר נפתחו שומרים את פרטי הקטלוג שלהם גם כשהשרתים של הספרייה הלאומית אינם זמינים."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "Zooming the manuscript image answers at once, and a double-click zooms in on that spot.",
                "he": "הגדלת תמונת כתב היד מגיבה מיד, ולחיצה כפולה מגדילה את המקום שעליו לחצתם."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "Keyboard shortcuts: press ? on any page for the list, and / to jump to the search box. On the manuscript page the arrow keys follow the page buttons, and F works on a Hebrew keyboard too.",
                "he": "קיצורי מקלדת: הקישו ? בכל עמוד כדי לראות את הרשימה, ו-/ כדי לעבור לתיבת החיפוש. בעיון בכתבי יד מקשי החצים פועלים בכיוון של כפתורי העמודים, ו-F פועל גם במקלדת עברית."
            },
            {
                "category": "fix",
                "apps": [
                    "web"
                ],
                "en": "After a failed sign-in, \"Forgot password?\" or \"Resend confirmation email\" is offered right under the message.",
                "he": "אחרי כניסה שלא הצליחה מוצעים \"שכחת סיסמה?\" או \"שלח שוב דוא\"ל אישור\" מיד מתחת להודעה."
            }
        ]
    },
    {
        "version": "9.13.1",
        "date": "2026-11-03",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Manuscript features in the Joins Lab",
            "he": "מאפייני כתב היד במעבדת הצירופים"
        },
        "items": [
            {
                "category": "feature",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "The Joins Lab can filter its candidates by Manuscript features too — under Filter ▾ in the desktop app and More filters… on the website — and material and size are now chosen there.",
                "he": "גם במעבדת הצירופים אפשר לסנן את המועמדים לפי מאפייני כתב היד – ב\"סנן ▾\" בתוכנה וב\"סינונים נוספים…\" באתר – ושם נבחרים עכשיו גם החומר והמידות."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "The window filters the candidates already found and counts among them. While it checks, or if the catalogue data cannot be read, every candidate stays in the list, with a note saying so.",
                "he": "החלון מסנן את המועמדים שכבר נמצאו, והמספרים בו נספרים מתוכם. בזמן הבדיקה, או כשאי אפשר לקרוא את נתוני הקטלוג, כל המועמדים נשארים ברשימה, עם הודעה על כך."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "In the desktop app, \"match material\" and \"width ±2 cm of anchor\" fill in the Manuscript features from the anchor fragment.",
                "he": "בתוכנה, \"התאם חומר\" ו\"רוחב ±2 ס\"מ מהעוגן\" ממלאים את מאפייני כתב היד לפי קטע העוגן."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "On the website, the Joins Lab's table view has the same toolbar as the cards: count, Library, Domain, Sort and More filters…",
                "he": "באתר, לתצוגת הטבלה במעבדת הצירופים יש אותו סרגל כלים כמו לכרטיסים: מספר, ספרייה, תחום, מיון ו\"סינונים נוספים…\"."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "On the website, the Manuscript features window has its lists and counts ready the moment it opens.",
                "he": "באתר, הרשימות והמספרים בחלון \"מאפייני כתב היד\" מוכנים מיד עם פתיחתו."
            }
        ]
    },
    {
        "version": "9.13.0",
        "date": "2026-11-02",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Find manuscripts by features",
            "he": "איתור כתבי יד לפי המאפיינים שלהם"
        },
        "items": [
            {
                "category": "feature",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "The new Manuscript features button opens one window with what the catalogue records about each manuscript: period of copy, language and alphabet, script and region, vocalization, number of columns and lines, physical status, material, attributes, and measurements computed from the photographs.",
                "he": "הכפתור החדש \"מאפייני כתב היד\" פותח חלון אחד ובו מה שהקטלוג מתעד על כל כתב יד: תקופת העתקה, שפה ואות, כתב ואזור, ניקוד, מספר טורים ושורות, מצב פיזי, חומר, תכונות ונתונים שחושבו מן הצילומים."
            },
            {
                "category": "feature",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Tick values, or choose \"All except these\"; \"Not recorded\" finds the manuscripts the catalogue says nothing about. Each value shows how many manuscripts have it, and the window counts the matches before you apply it.",
                "he": "סמנו ערכים, או בחרו \"הכול חוץ מאלה\"; \"ללא רישום\" מוצא את כתבי היד שהקטלוג אינו מתעד בהם דבר. ליד כל ערך מופיע מספר כתבי היד שיש להם אותו, והחלון סופר את ההתאמות לפני שמחילים אותו."
            },
            {
                "category": "feature",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "On Browse by Identification the window lists the manuscripts; in \"Search only in...\" on the website and Focus Search in the desktop app it limits a search to them. Browse by Identification now lists all 255,723 manuscripts, including the 29,169 without a catalogue record.",
                "he": "ב\"עיון לפי זיהוי\" החלון מציג את כתבי היד; ב\"חפש רק ב...\" באתר וב\"מקד חיפוש\" בתוכנה הוא מגביל אליהם את החיפוש. \"עיון לפי זיהוי\" מציג עכשיו את כל 255,723 כתבי היד, ובהם 29,169 שאין להם רשומה בקטלוג."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Lines, width, height and material come from the catalogue, from the measurements, or from either; when you set several measured values, they must all hold on the same page.",
                "he": "שורות, רוחב, גובה וחומר נלקחים מהקטלוג, מהמדידה או מאחד מהם; כשקובעים כמה ערכים נמדדים, כולם צריכים להתקיים באותו עמוד."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "The period of copy now uses each catalogue record's full range of dates, shown in the results (for example 944–1100), and the catalogue records of about 30,600 more manuscripts show their source.",
                "he": "תקופת ההעתקה מתחשבת עכשיו בטווח התאריכים המלא של כל רשומת קטלוג, והתוצאות מציגות אותו (למשל 944–1100); ורשומות הקטלוג של עוד כ־30,600 כתבי יד מציגות את מקורן."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "The same features also filter searches in the public API and in GenizahSearch for ChatGPT and Claude.",
                "he": "אותם מאפיינים מסננים חיפושים גם ב-API הציבורי וב-GenizahSearch ב-ChatGPT וב-Claude."
            }
        ]
    },
    {
        "version": "9.12.1",
        "date": "2026-10-30",
        "apps": [
            "desktop"
        ],
        "title": {
            "en": "Keyboard shortcuts, round two",
            "he": "קיצורי מקלדת, סבב שני"
        },
        "items": [
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Ctrl+1 to Ctrl+7 open the tabs: in Search and Composition Search the cursor goes to the results when there are any, otherwise to the search box; Ctrl+R goes to the results list; Ctrl+O opens Go to, like Ctrl+G.",
                "he": "Ctrl+1 עד Ctrl+7 פותחים את הלשוניות: בחפש ובחיפוש חיבורים הסמן עובר לתוצאות כשיש כאלה, ואחרת לתיבת החיפוש; Ctrl+R מעביר לרשימת התוצאות; Ctrl+O פותח את \"מעבר אל\", כמו Ctrl+G."
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "Ctrl+A ticks every result in the Search list, or unticks them all.",
                "he": "Ctrl+A מסמן את כל התוצאות ברשימה של \"חפש\", או מבטל את כל הסימונים."
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "In Composition Search, the cursor goes to the first result, so Enter opens it at once.",
                "he": "ב\"חיפוש חיבורים\" הסמן עובר לתוצאה הראשונה, כך ש-Enter פותח אותה מיד."
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "In the Manuscript Viewer, Ctrl+F also brings back text hidden beside the image, and the image buttons work after a one-page document from My Library.",
                "he": "בחלון \"צפייה בכתב יד\", Ctrl+F מחזיר גם טקסט שהוסתר ליד התמונה, וכפתורי התמונות פועלים גם אחרי מסמך בן עמוד אחד מהספרייה המקומית."
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "With a Hebrew keyboard layout, the shortcuts wait while a menu such as the search history is open.",
                "he": "בפריסת מקלדת עברית, קיצורי המקלדת ממתינים כל עוד תפריט כמו היסטוריית החיפוש פתוח."
            }
        ]
    },
    {
        "version": "9.12.0",
        "date": "2026-10-30",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Keyboard shortcuts",
            "he": "קיצורי מקלדת"
        },
        "items": [
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "The cursor is in the search box as soon as the program opens, so you can type a search right away.",
                "he": "הסמן נמצא בתיבת החיפוש מרגע פתיחת התוכנה, כך שאפשר להקליד חיפוש מיד."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "When a search finds results, the cursor moves to the results: Space ticks a result, Enter opens it, and Ctrl+Enter or Ctrl+B opens it in Browse; Ctrl+F or Ctrl+E brings you back to the search box.",
                "he": "כשחיפוש מוצא תוצאות, הסמן עובר לרשימת התוצאות: מקש הרווח מסמן תוצאה, Enter פותח אותה, ו-Ctrl+Enter או Ctrl+B פותחים אותה בעיון לפי מספר מדף; Ctrl+F או Ctrl+E מחזירים לתיבת החיפוש."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "Ctrl+1 to Ctrl+7 open the tabs with the cursor in their search box; Ctrl+O opens Go to, like Ctrl+G.",
                "he": "Ctrl+1 עד Ctrl+7 פותחים את הלשוניות עם הסמן בתיבת החיפוש שלהן; Ctrl+O פותח את \"מעבר אל\", כמו Ctrl+G."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "In the Manuscript Viewer, Ctrl+Right and Ctrl+Left move to the next and previous result, Ctrl+PgDown and Ctrl+PgUp to the next and previous image, and Ctrl+B opens it in Browse.",
                "he": "בחלון \"צפייה בכתב יד\", Ctrl+חץ שמאלה ו-Ctrl+חץ ימינה עוברים לתוצאה הבאה ולקודמת, Ctrl+PgDown ו-Ctrl+PgUp לתמונה הבאה ולקודמת, ו-Ctrl+B פותח אותה בעיון לפי מספר מדף."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "All the keys on one card: Ctrl+/, the ⌨ button beside A−, or Keyboard shortcuts in the ☰ menu.",
                "he": "כל המקשים בכרטיס אחד: Ctrl+/‎, הכפתור ⌨ ליד A−‎, או \"קיצורי מקלדת\" בתפריט ☰."
            },
            {
                "category": "fix",
                "apps": [
                    "web"
                ],
                "en": "On the website, the Word export of Find Parallels now marks the matched words, as the Excel export does.",
                "he": "באתר, הייצוא ל-Word ב\"מציאת מקבילות\" מסמן עכשיו את המילים התואמות, כמו הייצוא ל-Excel."
            }
        ]
    },
    {
        "version": "9.11.1",
        "date": "2026-10-30",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Cambridge links fixed",
            "he": "תיקון הקישורים לקיימברידג'"
        },
        "items": [
            {
                "category": "fix",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Corrected and added links to the Cambridge Digital Library (CUDL).",
                "he": "תיקון והוספה של קישורים לספרייה הדיגיטלית של קיימברידג' (CUDL)."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "About 1,900 more manuscripts now link straight to their own Cambridge item, among them shelfmarks with brackets and renamed collections.",
                "he": "כ-1,900 כתבי יד נוספים מקושרים עכשיו ישירות לפריט שלהם בקיימברידג', ובהם מספרי מדף עם סוגריים ואוספים ששמם שונה."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "On pages without an automatic transcription, Quick View now offers the FGP and PGP editions to choose from, and you can page through every image of such a manuscript in Browse and Quick View.",
                "he": "בעמודים שאין להם תעתוק אוטומטי, \"מבט מהיר\" מציע עכשיו לבחור בין המהדורות של FGP ו-PGP, ואפשר לדפדף בין כל התמונות של כתב יד כזה ב\"עיון לפי מספר מדף\" וב\"מבט מהיר\"."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "A search opened from a link starts at once.",
                "he": "חיפוש שנפתח מקישור מתחיל מיד."
            }
        ]
    },
    {
        "version": "9.11.0",
        "date": "2026-10-29",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "FGP transcriptions from FGP's own files",
            "he": "העתקות פרויקט פרידברג מתוך קובצי הפרויקט עצמו"
        },
        "items": [
            {
                "category": "feature",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "About 14,600 more transcriptions of the Friedberg Genizah Project (FGP), read from the project's own Word files and line data.",
                "he": "כ-14,600 העתקות נוספות של פרויקט פרידברג לחקר הגניזה (FGP), שנקראו מקובצי הוורד ומנתוני השורות של הפרויקט עצמו."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Cleaner text in about 27,800 FGP transcriptions that were already in the program: final and vocalised letters, the order of the lines, lines that mix Hebrew and Arabic, and Arabic script now appear as the transcriber wrote them; in about 1,900 more, letters that showed as boxes or wrong symbols are restored.",
                "he": "טקסט נקי יותר בכ-27,800 העתקות FGP שכבר היו בתוכנה: אותיות סופיות ומנוקדות, סדר השורות, שורות המשלבות עברית וערבית, והכתב הערבי מופיעים עתה כפי שכתב אותם המתעתק; ובכ-1,900 העתקות נוספות שוחזרו אותיות שהופיעו כריבועים או כסימנים שגויים."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Transcriptions of the Jerusalem Talmud now distinguish completions (in square brackets), doubtful letters (with a small circle above them) and additions between the lines. Please note: these transcriptions are not the final text, which is found in \"Ginze Yerushalmi\", edited by Y. Sussmann.",
                "he": "בהעתקות התלמוד הירושלמי נערכת עתה הבחנה: השלמות בסוגריים מרובעים, אותיות מסופקות בעיגול קטן מעליהן, ותוספות בין השיטין. תשומת לב: העתקות אלה אינן הנוסח הסופי, שאותו ניתן למצוא ב\"גנזי הירושלמי\" בעריכת י' זוסמן."
            },
            {
                "category": "feature",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "A new \"?\" button beside transcriptions by FGP's Genuzot team explains the transcribers' sigla.",
                "he": "נוסף כפתור \"?\" ליד העתקות של צוות גנוזות של פרויקט פרידברג, המסביר את סימולי המתעתקים."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "In the Joins Lab and Visual Similarity, you can choose several libraries or domains, and show or hide them, to find matches faster.",
                "he": "במעבדת הצירופים ובדמיון החזותי, תוכלו לבחור כמה ספריות או תחומים, ולבחור הצגה או הסתרה שלהם, לשם מציאת התאמות מהירה יותר."
            },
            {
                "category": "fix",
                "apps": [
                    "web"
                ],
                "en": "On the website, a page without an automatic transcription now opens its FGP or Princeton Geniza Project edition, with the version menu and all the usual buttons.",
                "he": "באתר, עמוד שאין לו תעתוק אוטומטי נפתח עתה עם המהדורה של פרויקט פרידברג או של פרויקט הגניזה של פרינסטון, כולל תפריט הגרסאות וכל הכפתורים הרגילים."
            }
        ]
    },
    {
        "version": "9.10.0",
        "date": "2026-10-25",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Desktop UI improvements",
            "he": "שיפורי ממשק בגרסת שולחן העבודה"
        },
        "items": [
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "The menu can move to the side of the window: Settings > Menu: Side menu, or ☰ > Move the menu to the side. Ctrl+1 to Ctrl+7 open the sections, Ctrl+8 the Fragment Puzzle and Ctrl+9 the Joins Lab.",
                "he": "אפשר להעביר את התפריט לצד החלון: הגדרות > תפריט: תפריט צד, או ☰ > העברת התפריט לצד. Ctrl+1 עד Ctrl+7 פותחים את הלשוניות, Ctrl+8 את פאזל קטעים ו-Ctrl+9 את מעבדת צירופים."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "Go to (Ctrl+G): type a shelfmark or system number, and the manuscript opens in Browse by Shelfmark from anywhere in the program; type Hebrew or Arabic words in the same box to search the corpus for them. The last ten manuscripts you opened are listed under the box, for quick access.",
                "he": "מעבר אל (Ctrl+G): הקלידו מספר מדף או מספר מערכת, וכתב היד ייפתח בלשונית \"עיון לפי מספר מדף\" מכל מקום בתוכנה; הקלידו מילים בעברית או בערבית באותה תיבה לשם חיפוש מונחים בקורפוס. מתחת לתיבה מופיעים עשרת כתבי היד האחרונים שפתחתם, לגישה מהירה."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "Saved searches: click ☆ beside a search in the history menu of Search or Composition Search, and it stays at the top of the list; clearing the history leaves it.",
                "he": "חיפושים שמורים: לחצו על ☆ ליד חיפוש בתפריט ההיסטוריה של \"חפש\" או של \"חיפוש חיבורים\", והוא יישאר בראש הרשימה; ניקוי ההיסטוריה אינו מוחק אותו."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "Text size: Settings > Text size in result lists, Ctrl+= / Ctrl+- (Ctrl+0 for normal), or A- / A+ beside the tabs. To enlarge or shrink the transcriptions, click the A- / A+ buttons beside them.",
                "he": "גודל הטקסט: הגדרות > גודל הטקסט ברשימות התוצאות, Ctrl+= / Ctrl+- (Ctrl+0 לגודל הרגיל), או A- / A+ ליד הלשוניות. להגדלה או הקטנה של התעתיקים, לחצו על כפתורי A- / A+ לידם."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "More features: a short tip at each start (turn it off or on in Settings > Show a tip when the program starts), the window opens where and as large as it was closed, F1 opens Help at the section in front, clicking your name on the Login button opens an account menu so you no longer log out by accident, and clear explanations in the Composition Search tooltips.",
                "he": "תכונות נוספות: טיפ קצר בכל פתיחה (ניתן לבטל או להפעיל בהגדרות > הצגת טיפ בפתיחת התוכנה), החלון נפתח במקום ובגודל שבו נסגר, F1 פותח את העזרה בנושא הלשונית הפתוחה, לחיצה על שמכם בכפתור הכניסה פותחת תפריט חשבון כדי למנוע התנתקות אקראית, ונוספו הסברים ברורים בחלוניות העזר של חיפוש חיבורים."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "On the website, every contributor has a public name: an account without one shows an alias such as \"Curious Scribe\" beside its corrections, comments and joins, and the site asks once how your name should appear. Responsa-style search notes on a Hebrew page now appear in Hebrew.",
                "he": "באתר יש לכל תורם שם ציבורי: חשבון ללא שם מוצג בכינוי (כגון Curious Scribe) ליד התיקונים, ההערות והצירופים שלו, והאתר שואל פעם אחת באיזה שם להציג אתכם. ההערות בחיפוש בסגנון פרויקט השו\"ת מופיעות עתה בעברית בעמוד העברי."
            }
        ]
    },
    {
        "version": "9.9.0",
        "date": "2026-10-23",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "True wildcards in Responsa",
            "he": "כוכבית אמיתית בחיפוש בסגנון פרויקט השו\"ת"
        },
        "items": [
            {
                "category": "feature",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Better wildcard search in Responsa style (the * operator), with fuller results: a star (*) stands for any sequence of letters, anywhere in a word, final letters included. Search כהנ* גדו* and you get both כהן גדול and כהנים גדולים; the search also works across a page break in the same manuscript.",
                "he": "שיפור בחיפוש רצף תווים בסגנון פרויקט השו\"ת (שימוש באופרטור *), עם תוצאות מלאות יותר: כוכבית (*) מייצגת כל רצף אותיות, בכל מקום במילה, כולל חלופות אותיות סופיות. נסו לחפש כהנ* גדו* ותקבלו גם כהן גדול וגם כהנים גדולים; החיפוש פועל גם במעבר בין עמודים באותו כתב יד."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Every word in a Responsa-style search is a whole word, #word# searches every combination of prefixes and suffixes, and the search also finds a word with brackets inside it, such as של[ו]ם. An excluded word hides a result only when it is on the same page.",
                "he": "כל מילה בחיפוש בסגנון פרויקט השו\"ת היא מילה שלמה, #מילה# מחפש כל צירוף של תחיליות וסיומות, החיפוש מוצא גם מילה שיש בתוכה סוגריים, כגון של[ו]ם. מילה מוחרגת מסתירה תוצאה רק כשהיא באותו עמוד."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "The search index rebuilds beside the one in use: you can keep searching meanwhile, progress shows at the top of the main window, and you can choose to rebuild when you exit. Rebuilding the index speeds up searches with a star at the start of a word and completes the matches across page breaks.",
                "he": "אינדקס החיפוש נבנה מחדש לצד האינדקס הפעיל: אפשר להמשיך לחפש בזמן הבנייה, ההתקדמות מוצגת בראש החלון הראשי, ואפשר לבחור \"לבנות מחדש ביציאה\". בנייה מחדש של האינדקס מאיצה את החיפוש עם כוכבית בתחילת מילה ומשלימה את ההתאמות במעבר בין עמודים."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Better search display: the highlighted words in the results are now centred in the Snippet column.",
                "he": "שיפור בממשק החיפוש: המילים המודגשות בתוצאות החיפוש ממורכזות כעת בעמודת הקטע."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "The browser's Back button returns to a search as the page showed it, in every search mode, with its results and filters, without searching again.",
                "he": "כפתור \"חזרה\" בדפדפן מחזיר לחיפוש כפי שהוצג בעמוד, בכל סוגי החיפוש, עם התוצאות והמסננים, בלי לחפש מחדש."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "The Joins Lab and Personal Lists now fit a phone: Compare shows the anchor and the candidate together, and the anchor stays in view while you scroll",
                "he": "מעבדת צירופים ורשימות אישיות מותאמות כעת לטלפון: חלון \"השווה\" מציג את העוגן והמועמד יחד, והעוגן נשאר בטווח ראייה בזמן הגלילה"
            }
        ]
    },
    {
        "version": "9.8.3",
        "date": "2026-10-22",
        "apps": [
            "web"
        ],
        "title": {
            "en": "A compact layout at every width",
            "he": "פריסה קומפקטית בכל גודל מסך"
        },
        "items": [
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "The website is compact at every screen size, from phones to wide monitors: search, browse, the expanded result and the Quick View take less room, leaving more of the page for manuscripts and results.",
                "he": "האתר קומפקטי בכל גודל מסך, מטלפון ועד מסך רחב: החיפוש, העיון, התוצאה המורחבת והמבט המהיר תופסים פחות מקום, ויותר מהעמוד נשאר לכתבי היד ולתוצאות."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "On a laptop the search form is one row, with Advanced Options and \"Search only in...\" as buttons. Results show as compact rows by default (switch to comfortable rows and the site remembers your choice), and an expanded result shows the full text beside a thumbnail.",
                "he": "במחשב נייד טופס החיפוש הוא שורה אחת, ו\"אפשרויות מתקדמות\" ו\"חפש רק ב...\" הם כפתורים. התוצאות מוצגות כברירת מחדל בשורות צפופות (אפשר לעבור לשורות מרווחות, והאתר יזכור את הבחירה), ותוצאה מורחבת מציגה את הטקסט המלא לצד תמונה ממוזערת."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "On a phone the whole page scrolls, the search form folds into one line when a search starts, the Quick View opens as three tabs (Text, Image, Info), and Browse shows the manuscript image higher up the page.",
                "he": "בטלפון כל העמוד נגלל, טופס החיפוש מתקפל לשורה אחת כשהחיפוש מתחיל, המבט המהיר נפתח בשלוש לשוניות (טקסט, תמונה, מידע), ובעיון תמונת כתב היד מופיעה גבוה יותר בעמוד."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "Manuscript images respond to touch: pinch with two fingers to zoom, and drag with one finger to move around a zoomed image.",
                "he": "תמונות כתבי היד מגיבות למגע: צבטו בשתי אצבעות כדי להגדיל או להקטין, וגררו באצבע אחת כדי לנוע בתמונה מוגדלת."
            }
        ]
    },
    {
        "version": "9.8.2",
        "date": "2026-10-22",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Downloads and thumbnails",
            "he": "הורדות ותמונות ממוזערות"
        },
        "items": [
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Excel, Word and text downloads hold the text of the page each result shows, as on the website, so Excel files are much smaller. In Excel the Snippet marks a line break with ‖ and the Full Text uses a space, so words stay apart.",
                "he": "הייצוא ל-Excel, ל-Word ולקובץ טקסט כולל את הטקסט של העמוד שכל תוצאה מציגה, כמו באתר, ולכן קובצי ה-Excel קטנים בהרבה. ב-Excel עמודת \"קטע\" מסמנת מעבר שורה ב-‖ ועמודת \"טקסט מלא\" ברווח, כך שהמילים נשארות נפרדות."
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "The Source column appears whenever a result comes from another edition of the automatic transcription or from your local library.",
                "he": "עמודת \"מקור\" מופיעה בכל פעם שתוצאה מגיעה מתעתוק במהדורה אחרת של התעתוק האוטומטי או מהספרייה המקומית."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "Visual Similarity thumbnails from the National Library of Israel are kept on our server, so the pictures show steadily when you switch sizes, and pictures already seen keep showing even when the library's site is down.",
                "he": "התמונות הממוזערות מהספרייה הלאומית בדמיון החזותי נשמרות בשרת שלנו, כך שהתמונות מוצגות ביציבות גם כשמחליפים גודל, ותמונות שכבר נצפו ממשיכות להופיע גם כשאתר הספרייה הלאומית אינו זמין."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "Faster and smoother: saving a list change, a comment or a correction keeps the site responsive for everyone, turning a page in Browse keeps the current page in view while the next one loads, and the site is back sooner after an update.",
                "he": "מהיר וחלק יותר: שמירת שינוי ברשימה, הערה או תיקון אינה מאטה את האתר לאחרים, בדפדוף בעיון העמוד הנוכחי נשאר מוצג עד שהבא נטען, והאתר חוזר לפעול מהר יותר אחרי עדכון."
            }
        ]
    },
    {
        "version": "9.8.1",
        "date": "2026-10-21",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Reliability and sign-up",
            "he": "אמינות והרשמה"
        },
        "items": [
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Sign-up follows email confirmation: after you register, the website and the desktop app ask you to open the confirmation link sent to your email, and the username, name and affiliation you type are saved with the account. Signing in before confirming explains what to do, in Hebrew too.",
                "he": "ההרשמה כוללת אישור בדוא\"ל: לאחר ההרשמה, האתר והתוכנה מבקשים לפתוח את קישור האישור שנשלח לתיבת הדואר שלכם, ושם המשתמש, השם והשיוך המוסדי שהקלדתם נשמרים בחשבון. מי שמתחבר לפני האישור מקבל הסבר מה לעשות, גם בעברית."
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "Your own corrections (draft, pending, rejected) appear in the version selector of the Manuscript Viewer and of Browse by Shelfmark in every account, including an account with no username; the account name shows your username, or else your full name.",
                "he": "התיקונים שלכם (טיוטה, ממתין, נדחה) מופיעים בבורר הגרסאות של \"צפייה בכתב יד\" ושל \"עיון לפי מספר מדף\" בכל חשבון, גם בחשבון ללא שם משתמש; שם החשבון מציג את שם המשתמש, ואם אין כזה את השם המלא."
            },
            {
                "category": "fix",
                "apps": [
                    "web"
                ],
                "en": "The Fragment Puzzle saves your edits to a saved join as you go, and keeps your crops and the fragments' places on the canvas.",
                "he": "פאזל קטעים שומר את השינויים בצירוף שמור תוך כדי עבודה, ושומר את החיתוכים ואת מקומות הקטעים על הקנבס."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "Faster: Browse page turns for signed-in readers and page updates with many tabs open, and a dropped connection reconnects without reloading the page.",
                "he": "מהיר יותר: דפדוף בעיון למשתמשים מחוברים ועדכוני עמוד כשפתוחות לשוניות רבות, וחיבור שנותק מתחדש בלי לטעון את העמוד מחדש."
            },
            {
                "category": "fix",
                "apps": [
                    "web"
                ],
                "en": "English readers see library names and image credits in English everywhere, Browse by Identification opens with its first page of results, and the public API, MCP and the ChatGPT app match whole words in exact mode, as the website's Exact search does.",
                "he": "קוראים באנגלית רואים את שמות הספריות ואת הקרדיטים לתמונות באנגלית בכל מקום, \"עיון לפי זיהוי\" נפתח עם עמוד התוצאות הראשון, וה-API הציבורי, שרת ה-MCP ואפליקציית ChatGPT מוצאים מילים שלמות במצב exact, כמו החיפוש ה\"מדויק\" באתר."
            },
            {
                "category": "fix",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "AND, OR and NOT in a text are searched as ordinary words. On the desktop, the app closes within two seconds even when a library's server is slow to answer, and the Excel export shows \"N+\" in the result count when a list was cut off or stopped.",
                "he": "AND, OR ו-NOT בטקסט נחפשים כמילים רגילות. בתוכנה, הסגירה נמשכת עד שתי שניות גם כששרת של ספרייה מאחר לענות, וייצוא ה-Excel מציג \"N+\" במספר התוצאות כשהרשימה נקטעה או נעצרה."
            }
        ]
    },
    {
        "version": "9.8.0",
        "date": "2026-10-19",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Joins Lab upgrades",
            "he": "שדרוגים במעבדת הצירופים"
        },
        "items": [
            {
                "category": "feature",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Sort and filter the Joins Lab by library and domain: keep the search's order or group the candidates by library or by domain, filter by either, and see each candidate's domain on its card.",
                "he": "מיון וסינון במעבדת הצירופים לפי ספרייה ותחום: אפשר לשמור על סדר החיפוש או לקבץ את המועמדים לפי ספרייה או לפי תחום, לסנן לפי כל אחד מהם, ולראות על כל כרטיס את התחום שלו."
            },
            {
                "category": "feature",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Three card sizes in the grid view, with larger thumbnails in the largest size. Your choice is remembered.",
                "he": "שלושה גדלים של כרטיסים בתצוגת הרשת, ובגודל הגדול התמונות גדולות יותר. הבחירה נשמרת."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "A fuller Compare window: each side has a copy button by the shelfmark, one line with library, domain, date, size and material, and links to Ktiv, the holding library's viewer, the Princeton Geniza Project and genizahsearch.com.",
                "he": "חלון השוואה מלא יותר: בכל צד יש כפתור העתקה ליד מספר המדף, שורה אחת עם ספרייה, תחום, תאריך, גודל וחומר, וקישורים לכתיב, למציג של הספרייה שמחזיקה בקטע, לפרויקט הגניזה של פרינסטון ול-genizahsearch.com."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "On the website, Copy Link reopens the same Joins Lab screen for anyone you send it to, with your marks only if you choose, and Browse opens the fragment in a new tab.",
                "he": "באתר, „העתק קישור\" פותח את אותו מסך של מעבדת הצירופים לכל מי שמקבל את הקישור – עם הסימונים שלכם רק אם תבחרו בכך – והעיון בקטע נפתח בכרטיסייה חדשה."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "The homepage search box opens a shelfmark or a system ID directly, and runs a text search for Hebrew or Arabic text as before.",
                "he": "תיבת החיפוש בעמוד הבית פותחת ישירות מספר מדף או מספר מערכת, ולטקסט בעברית או בערבית מריצה חיפוש טקסט כמו קודם."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "The Browse page's buttons carry icons, and the Visual Similarity window offers small, medium and large thumbnails and can enlarge the original fragment.",
                "he": "לכפתורים בעמוד העיון נוספו סמלים, ובחלונית הדמיון החזותי אפשר לבחור תמונות ממוזערות קטנות, בינוניות או גדולות, ולהגדיל את הקטע המקורי."
            }
        ]
    },
    {
        "version": "9.7.0",
        "date": "2026-10-19",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Conditions in Composition Search, and letter-level search in My Library",
            "he": "תנאים בחיפוש חיבורים, וחיפוש ברמת האות בספרייה המקומית"
        },
        "items": [
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "Conditions in Composition Search: from the new \"Filter texts\" menu, add up to four texts that must appear, or must not appear, on the same image, on an adjacent image or in the same manuscript — for example, set aside the works that quote your text, such as the Rif when you search the Talmud, or search only manuscripts that carry parallels to one text on one page and to another text on the other.",
                "he": "תנאים בחיפוש חיבורים: מהתפריט החדש \"סנן טקסטים\" אפשר להוסיף עד ארבעה טקסטים שחייבים להופיע, או שאסור שיופיעו, באותה תמונה, בתמונה סמוכה או באותו כתב יד — למשל, להוציא מהתוצאות חיבורים המצטטים את הטקסט שלך, כמו הרי\"ף כשמחפשים בתלמוד, או לחפש רק כתבי יד שמכילים מקבילות לטקסט מסוים בעמוד אחד ולטקסט אחר בעמוד השני."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "Fast letter-level search in My Library too: build the index once, and then you can search for compositions quickly in My Library as well (based on a prototype by Yehudah Seewald).",
                "he": "חיפוש מהיר ברמת האות גם בספרייה המקומית. בונים את האינדקס פעם אחת, ואז ניתן לחפש חיבורים במהירות גם בספרייה המקומית (נוצר על פי אב-טיפוס שיצר יהודה זייבלד)."
            },
            {
                "category": "feature",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Shelfmarks as the NLI catalogue writes them, such as \"Cambridge University Library, Ms. T-S K 08.026\", are found wherever you type or paste a shelfmark; when one fits several records, you choose from a list. A library name at the start, such as \"Halper 77\", leads to that library's record.",
                "he": "מספרי מדף כפי שהם כתובים בקטלוג הספרייה הלאומית, כמו \"Cambridge University Library, Ms. T-S K 08.026\", נמצאים בכל מקום שבו מקלידים או מדביקים מספר מדף; כשמספר מדף מתאים לכמה רשומות, בוחרים מתוך רשימה. שם ספרייה בתחילתו, כמו \"Halper 77\", מוביל לרשומה של אותה ספרייה."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "On the website, the Visual Similarity window can show its look-alike fragments as thumbnails, and its Sort, Library and Domain filters apply in both views.",
                "he": "באתר, חלונית הדמיון החזותי יכולה להציג את הקטעים הדומים כתמונות ממוזערות, והמיון והסינון לפי ספרייה ותחום פועלים בשתי התצוגות."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Letter-level search lists every manuscript it finds, and shows a record's own photograph rather than a copy from a microfilm reel shared by many records.",
                "he": "החיפוש ברמת האות מציג כל כתב יד שנמצא, ומעדיף את הצילום של הרשומה עצמה על פני עותק מסליל מיקרופילם המשותף לרשומות רבות."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Hebrew Word exports are aligned to the right, and Help was brought in line with the programs in both languages. In the app, the standard buttons (Yes / No / OK / Cancel) are in Hebrew too.",
                "he": "קובצי Word בעברית מיושרים לימין, והעזרה עודכנה כך שתתאים לתוכנה ולאתר בשתי השפות. בתוכנה גם הכפתורים הרגילים (כן / לא / אישור / ביטול) מוצגים בעברית."
            }
        ]
    },
    {
        "version": "9.6.2",
        "date": "2026-10-12",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Faster Extended and Maximum on the desktop; GenizahSearch in ChatGPT",
            "he": "חיפושי וריאנטים מורחב ומרבי מהירים יותר בתוכנה; GenizahSearch ב־ChatGPT"
        },
        "items": [
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Extended and Maximum searches are many times faster: for the patterns the app builds itself, each page is now checked with Python's own fast matcher, with the same results. Stop answers at once.",
                "he": "חיפושי וריאנטים מורחב ומרבי מהירים פי כמה: בתבניות שהתוכנה בונה בעצמה, כל עמוד נבדק עכשיו במנוע המהיר של פייתון, עם אותן תוצאות. עצירה נענית מיד."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "The word gap (words allowed between the search words) is at most 10 in Exact, Variants and Fuzzy searches; Responsa-style and Regex searches have no limit.",
                "he": "המרווח בין המילים (כמה מילים מותרות בין מילות החיפוש) הוא לכל היותר 10 בחיפוש מדויק, וריאנטים ומקורב; בחיפוש בסגנון פרויקט השו\"ת ובביטוי רגולרי אין מגבלה."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "GenizahSearch in ChatGPT - ask about the manuscripts in your own words, from the AI tools page.",
                "he": "GenizahSearch ב־ChatGPT – שאלו על כתבי היד במילים שלכם, מעמוד כלי ה־AI."
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "Searches run to the end even when they meet a very long document.",
                "he": "חיפושים רצים עד הסוף גם כשהם נתקלים במסמך ארוך מאוד."
            }
        ]
    },
    {
        "version": "9.6.1",
        "date": "2026-10-11",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Faster Extended and Maximum",
            "he": "חיפוש מהיר בהרבה בווריאנטים מורחבים ומרביים"
        },
        "items": [
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Extended and Maximum, much faster: each word's thousands of spellings are now searched as one compact pattern, with the same results. A four-word Maximum search is ready in about 4 seconds instead of about 22, and each page is checked tens of times faster.",
                "he": "וריאנטים מורחבים ומרביים – מהר בהרבה: אלפי הכתיבים של כל מילה נבדקים עכשיו כתבנית אחת קומפקטית, עם אותן תוצאות. חיפוש מרבי בן ארבע מילים מוכן בתוך כ־4 שניות במקום כ־22, וכל עמוד נבדק מהר פי עשרות מונים."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Composition Search with Extended or Maximum variants is faster too.",
                "he": "גם חיפוש חיבורים בווריאנטים מורחבים או מרביים מהיר יותר."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Stop answers sooner during a slow search.",
                "he": "„עצור\" מגיב מהר יותר בחיפוש איטי."
            },
            {
                "category": "fix",
                "apps": [
                    "web"
                ],
                "en": "On the website, Extended and Maximum searches, also in Parallels, stop at the 3-minute limit and show what they found, as Basic searches do.",
                "he": "באתר, חיפושים בווריאנטים מורחבים ומרביים, גם בחיפוש מקבילות, נעצרים במגבלת שלוש הדקות ומציגים את מה שנמצא, כמו חיפושים בסיסיים."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "AI assistants connected to GenizahSearch can run the website's Parallels (chunk) search, and the public API offers the Extended variant level for background searches.",
                "he": "עוזרי AI המחוברים ל־GenizahSearch יכולים להריץ את חיפוש המקבילות של האתר, וה־API הציבורי מציע את רמת הווריאנטים המורחבת בחיפושי רקע."
            },
            {
                "category": "fix",
                "apps": [
                    "web"
                ],
                "en": "The website keeps working smoothly when a search, filter or lookup is interrupted, for example by a newer search or a closed tab.",
                "he": "האתר ממשיך לעבוד כרגיל כשחיפוש, סינון או בדיקה נקטעים באמצע, למשל בגלל חיפוש חדש או כרטיסייה שנסגרה."
            }
        ]
    },
    {
        "version": "9.6.0",
        "date": "2026-10-07",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Search fixes",
            "he": "תיקונים בחיפוש"
        },
        "items": [
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "On the website, the first results appear while the search is still running, each search starts a few seconds sooner, and Exact matches whole words, as in the app (שלום finds שלום, not בשלום).",
                "he": "באתר, התוצאות הראשונות מופיעות כבר בזמן החיפוש, כל חיפוש מתחיל כמה שניות מוקדם יותר, וחיפוש מדויק מוצא מילים שלמות, כמו בתוכנה (שלום מוצא שלום, ולא בשלום)."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Num Changes (×1–×3) works in every variant level, and each level keeps its own value: ×1 for Basic, ×2 for Extended and Maximum. At ×2, Extended and Maximum find more spellings and take longer; choose ×1 for a quick search.",
                "he": "„מס' שינויים\" (×1–×3) פועל בכל רמות הווריאנטים, ולכל רמה ערך משלה: ×1 לבסיסי, ×2 למורחב ולמרבי. ב־×2 החיפוש המורחב והמרבי מוצא יותר כתיבים ונמשך זמן רב יותר; לחיפוש מהיר בחרו ×1."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Regex searches every page its pattern can match, in My Library too.",
                "he": "חיפוש בביטוי רגולרי בודק כל עמוד שהתבנית יכולה להתאים לו, גם בספרייה המקומית."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "On the website, a list that reaches the search's limit shows \"N+\", and Search within results first reads the whole list. A search stops after 3 minutes and shows what it found.",
                "he": "באתר, רשימה שהגיעה למגבלת החיפוש מסומנת „N+\", והחיפוש בתוך התוצאות קורא קודם את הרשימה כולה. חיפוש נעצר אחרי שלוש דקות ומציג את מה שמצא."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Composition Search (Parallels on the website) searches a text shorter than the chunk size as one chunk, and Lab Mode always searches the end of the text. A catalog filter that cannot be applied says so instead of running the search.",
                "he": "חיפוש חיבורים (מקבילות באתר) מחפש טקסט הקצר מגודל המקטע כמקטע אחד, ו„מצב מעבדה\" מחפש תמיד גם את סוף הטקסט. מסנן קטלוג שאי אפשר להחיל מודיע על כך במקום להריץ את החיפוש."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "On the website, your settings and language are kept for you, and until you choose a language the site follows your browser's.",
                "he": "באתר, ההגדרות והשפה נשמרות לכל מבקר בנפרד, ועד שתבחרו שפה האתר מוצג בשפת הדפדפן שלכם."
            }
        ]
    },
    {
        "version": "9.5.0",
        "date": "2026-10-04",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Faster, complete search",
            "he": "חיפוש מהיר ומקיף יותר"
        },
        "items": [
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Faster search: the first results appear while the search is still running, and Exact and Variants searches take a fraction of the time (Exact שלום: from 10 seconds to under 2).",
                "he": "חיפוש מהיר יותר: התוצאות הראשונות מופיעות כבר בזמן החיפוש, וחיפושים מדויקים ובווריאנטים נמשכים זמן קצר בהרבה (שלום בחיפוש מדויק: מ־10 שניות לפחות מ־2)."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Whole words: Variants and Fuzzy match whole words, and so does Exact in the app (שלום, not בשלום). Fuzzy finds near spellings: one letter different, two in words of five letters or more.",
                "he": "מילים שלמות: וריאנטים ומקורב מוצאים מילים שלמות, וכך גם חיפוש מדויק בתוכנה (שלום, ולא בשלום). החיפוש המקורב מוצא כתיב קרוב: הבדל של אות אחת, או שתיים במילים של חמש אותיות ומעלה."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "More comprehensive results, also in searches with many results: \"Search within\" and Composition Search (Parallels on the website) with filters work on the whole set, at any size. In the app, a long list shows \"N+\", and \"Only results with all terms\" works on the full list too.",
                "he": "תוצאות מקיפות יותר, גם בחיפושים מרובי תוצאות: „חפש בתוך\" וחיפוש חיבורים (מקבילות באתר) עם מסננים פועלים על הקבוצה כולה, בכל גודל. בתוכנה, רשימה ארוכה מסומנת „N+\", וגם „רק תוצאות עם כל המונחים\" פועל על הרשימה המלאה."
            },
            {
                "category": "improvement",
                "apps": [
                    "web",
                    "desktop"
                ],
                "en": "Text position checks every occurrence of a word on a page, so more pages are found, and words with brackets, such as של[ו]ם, are found and highlighted whole.",
                "he": "„מיקום בטקסט\" בודק כל מופע של מילה בעמוד, וכך נמצאים יותר עמודים; ומילים עם סוגריים, כמו של[ו]ם, נמצאות ומודגשות בשלמותן."
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "The program closes cleanly, also while background work, such as loading thumbnails, is still running.",
                "he": "התוכנה נסגרת כראוי גם כשעבודה ברקע, כמו טעינת תמונות ממוזערות, עדיין פועלת."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "Fragment Puzzle: your saved joins stay private until you publish them, and drafts saved in your browser move into your account when you sign in. Publishing a join works for every signed-in user.",
                "he": "פאזל קטעים: הצירופים ששמרתם נשארים פרטיים עד שתפרסמו אותם, וטיוטות שנשמרו בדפדפן עוברות לחשבון שלכם כשאתם מתחברים. פרסום צירוף פועל לכל משתמש מחובר."
            }
        ]
    },
    {
        "version": "9.4.0",
        "date": "2026-09-28",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Data integrity",
            "he": "שלמות הנתונים"
        },
        "items": [
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "List sync keeps everything: an entry in two lists, and two pages of one manuscript, each keep their own copy in your account, and a note that differs is never overwritten; both versions are kept.",
                "he": "סנכרון הרשימות שומר על הכול: פריט שנמצא בשתי רשימות, ושני עמודים של אותו כתב יד, נשמרים כל אחד בנפרד בחשבונך, והערה שונה לעולם אינה נדרסת – שתי הגרסאות נשמרות."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "List sync runs in the background: every change uploads by itself, \"Sync lists now\" is on the Lists tab, and entries you remove here are removed from your account too. Entries removed on the website are shown to you and never removed silently.",
                "he": "הסנכרון פועל ברקע: כל שינוי עולה לחשבון מעצמו, „סנכרן רשימות עכשיו\" נמצא בלשונית הרשימות, ופריטים שהסרת כאן מוסרים גם מחשבונך. פריטים שהוסרו באתר מוצגים לך לבחירה, ולעולם אינם מוסרים בשקט."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Your work stays saved: lists, settings and language are saved safely and restored from a backup if a file is damaged; New keeps your Joins Lab work; excluded manuscripts stay hidden until New; only one copy of the program runs at a time.",
                "he": "העבודה שלך נשמרת: הרשימות, ההגדרות והשפה נשמרות בבטחה ומשוחזרות מגיבוי אם קובץ נפגע; „חדש\" שומר את העבודה במעבדת צירופים; כתבי יד שהוחרגו נשארים מוסתרים עד „חדש\"; ורק עותק אחד של התוכנה פועל בכל פעם."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "The Fragment Puzzle asks before unsaved work would be lost (on New, when opening another join, or when quitting), and keeps fragments whose images could not be loaded. Deleting several fragments asks first, Ctrl+Z undoes it, and the Joins Lab keeps up with quick page turns.",
                "he": "פאזל הקטעים שואל לפני שעבודה שלא נשמרה הולכת לאיבוד (בלחיצה על „חדש\", בפתיחת צירוף אחר או ביציאה מהתוכנה), ושומר גם קטעים שתמונותיהם לא נטענו. מחיקת כמה קטעים מבקשת אישור, Ctrl+Z מבטל אותה, ומעבדת הצירופים עומדת בקצב של דפדוף מהיר."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Logging out finishes at once, with the last list upload continuing in the background, and closing the program never waits for the server: anything not yet uploaded stays on this computer until your next log-in.",
                "he": "ההתנתקות מסתיימת מיד, וההעלאה האחרונה של הרשימות ממשיכה ברקע; סגירת התוכנה אינה ממתינה לשרת, ומה שטרם הועלה נשמר במחשב עד הכניסה הבאה."
            },
            {
                "category": "fix",
                "apps": [
                    "web"
                ],
                "en": "On the website's My Lists page, editing an entry's note or tags and removing an entry now save to your account, and an entry for a single page shows its page number and opens that page in Browse.",
                "he": "בדף „הרשימות שלי\" באתר, עריכת הערה או תגיות של פריט והסרת פריט נשמרות כעת בחשבונכם, ופריט של דף בודד מציג את מספר הדף ופותח את הדף הזה בעיון."
            }
        ]
    },
    {
        "version": "9.3.0",
        "date": "2026-09-24",
        "apps": [
            "desktop"
        ],
        "title": {
            "en": "Fresh PGP data, small screens, interface improvements and fixes",
            "he": "נתוני PGP מעודכנים, תצוגה מותאמת לצגים קטנים, שיפורי ממשק ותיקוני באגים"
        },
        "items": [
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Princeton Geniza Project data updated: 36,642 documents, and most fragments now link to their images.",
                "he": "נתוני פרויקט הגניזה של פרינסטון עודכנו: 36,642 מסמכים, ולרוב הקטעים יש כעת קישור לתמונות."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Small screens and high zoom: buttons shrink to icons and then move to a \"More\" menu, results fit the window without scrolling sideways, and the viewer always opens on screen.",
                "he": "מסכים קטנים והגדלה: הכפתורים מתכווצים לסמלים ואחר כך עוברים לתפריט „עוד\", התוצאות נכנסות לרוחב החלון בלי גלילה הצידה, והמציג נפתח תמיד בתוך המסך."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "In the viewer and the Browse tab: open or copy a link to the page on genizahsearch.com, enlarge or shrink the text (A−/A+), and show or hide the transcription.",
                "he": "במציג ובלשונית העיון: פתיחת הדף באתר genizahsearch.com או העתקת קישור אליו, הגדלה והקטנה של הטקסט (A−/A+), והצגה או הסתרה של התעתיק."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Composition Search: the viewer follows the result filters and shows each result's category.",
                "he": "חיפוש חיבורים: המציג מכבד את סינוני התוצאות ומראה את הקטגוריה של כל תוצאה."
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "Fixes: the Library and Shelfmark columns in the Excel export, filtered markers in the flat view, the other-side search in the Joins Lab, and Hebrew material names in Focus Search.",
                "he": "תיקונים: עמודות הספרייה ומספר המדף בייצוא ל־Excel, סימון תוצאות מסוננות בתצוגה השטוחה, החיפוש בצד השני של הדף במעבדת הצירופים, ושמות החומרים בעברית ב„מקד חיפוש\"."
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "Composition Search remembers your choice of letter-level search after a restart.",
                "he": "חיפוש חיבורים זוכר את בחירתכם בחיפוש ברמת האות גם אחרי הפעלה מחדש של התוכנה."
            }
        ]
    },
    {
        "version": "9.2.1",
        "date": "2026-09-17",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "The viewer in its own window, a hint after empty Local searches, and Oxford image links",
            "he": "חלון צפייה עצמאי, הצעה אחרי חיפוש ריק בספרייה המקומית, וקישורים לתמונות אוקספורד"
        },
        "items": [
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "The search results viewer is now a window of its own: minimize it, switch back to the main window, and keep searching while it stays open.",
                "he": "חלונית הצפייה בתוצאות החיפוש היא כעת חלון עצמאי: אפשר למזער אותה, לחזור לחלון הראשי ולהמשיך לחפש בזמן שהיא פתוחה."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "Searched your local library by accident and found nothing? A notice now says so and offers the same search in the Genizah corpus with one click.",
                "he": "חיפשתם בספרייה המקומית בטעות ולא מצאתם דבר? הודעה מציינת זאת ומציעה את אותו חיפוש במאגר הגניזה בלחיצה אחת."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Composition Search now opens on Letter-level search whenever its index is built; without an index it stays on Chunk search. A method you pick yourself is kept.",
                "he": "חיפוש חיבורים נפתח כעת בחיפוש ברמת האות בכל פעם שהאינדקס שלו בנוי; בלי אינדקס הוא נשאר בחיפוש מקטעים. שיטה שבחרתם בעצמכם נשמרת."
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "Composition Search: the search-type and frequency controls work again after switching from Letter-level back to Chunk search. Oxford images: when the images are not available for display, a link to the Bodleian site is shown.",
                "he": "חיפוש חיבורים: אפשרויות סוג החיפוש והתדירות חוזרות לפעול לאחר מעבר מחיפוש ברמת האות בחזרה לחיפוש מקטעים. תמונות אוקספורד: כאשר התמונות אינן זמינות לתצוגה, מוצג קישור לאתר הבודליאנה"
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "Ask the Genizah in ChatGPT: the GenizahSearch GPT, a research skill for other AI assistants, and the public API, from the AI tools page.",
                "he": "שאלו את הגניזה ב־ ChatGPT – ה־GPT של GenizahSearch, סקיל מחקר לעוזרי AI אחרים וה־API הציבורי, הכול מעמוד כלי ה־AI."
            },
            {
                "category": "improvement",
                "apps": [
                    "web"
                ],
                "en": "Faster, steadier search on the website: long research searches run separately, so the site stays quick while heavy searches run.",
                "he": "חיפוש מהיר ויציב יותר באתר: חיפושי מחקר ארוכים רצים בנפרד, כך שהאתר נשאר מהיר גם בזמן חיפושים כבדים."
            }
        ]
    },
    {
        "version": "9.2.0",
        "date": "2026-09-06",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Citations, PGP links, and a round of repairs",
            "he": "ציטוטים, קישורים ל־PGP וסבב תיקונים"
        },
        "items": [
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "Prominent links to the Princeton Geniza Project (PGP) - in the search result window, on the Browse tab, and on the PGP badge in the results table.",
                "he": "קישורים בולטים לפרויקט הגניזה של פרינסטון (PGP) — בחלונית תוצאות החיפוש ובטאב העיון, וכן בתגית PGP בטבלת התוצאות"
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "A new \"Cite this page\" button, naming whoever made the transcription - MiDRASH, PGP, FGP, a translator, or a community correction.",
                "he": "נוסף כפתור \"צטט דף זה\", המזכיר את מי שיצר את התעתוק — מדרש, PGP, FGP, מתרגם או תיקון קהילתי"
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "A print-view button, and a prominent citation button - for the site as a whole and for each page.",
                "he": "לחצן תצוגת הדפסה, וכפתור ציטוט בולט לאתר כולו ולכל דף בנפרד"
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "The citation bar at the foot of the app now also credits the application version.",
                "he": "באנר הציטוט בתחתית התוכנה מכיל כעת גם קרדיט לגרסת התוכנה"
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "The Search tab and the Composition Search tab now have separate exclusion lists.",
                "he": "לשונית החיפוש ולשונית חיפוש החיבורים קיבלו רשימות החרגה נפרדות"
            },
            {
                "category": "fix",
                "apps": [
                    "desktop"
                ],
                "en": "Assorted composition-search bug fixes, and Bodleian (Oxford) images now carry a prominent link to their site.",
                "he": "תיקוני באגים שונים בחיפוש חיבורים, ותמונות ספריות הבודליאנה באוקספורד קיבלו קישור בולט לאתר"
            }
        ]
    },
    {
        "version": "9.1.0",
        "date": "2026-08-27",
        "apps": [
            "desktop"
        ],
        "title": {
            "en": "Search using several witnesses of the same work",
            "he": "חיפוש באמצעות כמה עדי נוסח של אותו טקסט"
        },
        "items": [
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "Several witnesses of one work: letter-level search can now be given more than one copy of the text you are looking for. Each is searched on its own and the results are merged into one list.",
                "he": "כמה עדי נוסח של יצירה אחת: אפשר לתת לחיפוש ברמת האות יותר מעותק אחד של הטקסט שאתם מחפשים. כל אחד נבדק בנפרד והתוצאות מתמזגות לרשימה אחת."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "A Witnesses column shows how many copies point at each manuscript, and Auto-expand repeats the search with the best results as new witnesses. Witness lists are kept after a restart and in search history.",
                "he": "עמודת „עדים\" מראה כמה עדי נוסח מצביעים על כל כתב יד, ו„הרחבה אוטומטית\" חוזרת על החיפוש עם התוצאות הטובות ביותר כעדים נוספים. רשימות העדים נשמרות גם אחרי הפעלה מחדש ובהיסטוריית החיפושים."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Stop, Pause and Resume work on a search with several witnesses, keeping everything found so far, and Full Recursive Search now works with letter-level search.",
                "he": "אפשר לעצור, להשהות ולהמשיך חיפוש עם כמה עדי נוסח, וכל מה שנמצא עד אז נשמר; גם „חיפוש רקורסיבי מלא\" פועל כעת בחיפוש ברמת האות."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "The manuscript viewer now highlights letter-level matches in the page text, and new Prev / Next Manuscript buttons skip the remaining pages of a manuscript you have already read.",
                "he": "צפיין כתב היד מסמן כעת התאמות של חיפוש ברמת האות בטקסט העמוד, ולחצנים חדשים, לכתב היד הבא ולכתב היד הקודם, מדלגים על שאר עמודי כתב היד שכבר קראתם."
            },
            {
                "category": "improvement",
                "apps": [
                    "desktop"
                ],
                "en": "Help compares letter-level and chunk search with measured results, in both languages, and the Hebrew Help now includes the full letter-level search chapter and the Pause/Resume section.",
                "he": "העזרה משווה בין חיפוש ברמת האות לחיפוש מקטעים על סמך מדידות, בשתי השפות, ובעזרה בעברית נוספו הפרק המלא על חיפוש ברמת האות והפרק על השהיה והמשך."
            }
        ]
    },
    {
        "version": "9.0.0",
        "date": "2026-08-16",
        "apps": [
            "web",
            "desktop"
        ],
        "title": {
            "en": "Computed Identifications, the Visual Genizah Atlas, and Letter-Level Search",
            "he": "זיהויים מחושבים, אטלס הגניזה החזותי וחיפוש ברמת האות"
        },
        "items": [
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "Computed Identifications (beta): software has matched Genizah fragments to works we already know. Use it to look for new witnesses to a text you study — and judge each match yourself.",
                "he": "זיהויים מחושבים (בטא): אלגוריתם ממוחשב התאים קטעי גניזה לחיבורים מוכרים. השתמשו בו כדי לחפש עדי נוסח חדשים לחיבור שאתם חוקרים — ושפטו כל התאמה בעצמכם."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "Computed identifications can show the matched text: \"View text match\" puts the fragment's own passage beside the work's, with the matching words highlighted. Readers can also send their own verdict on any match.",
                "he": "זיהויים מחושבים יכולים להציג את הטקסט שהותאם: בלחיצה על „הצגת התאמת הטקסט\" מוצג הקטע מכתב היד לצד הקטע מהחיבור, והמילים התואמות מודגשות. אפשר גם לשלוח חוות דעת משלכם על כל התאמה."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "The Visual Genizah Atlas (beta): a new interactive map of textual connections across the Cairo Genizah. Positions and clusters are algorithmically derived from textual connections — proximity reflects textual similarity, not physical provenance.",
                "he": "אטלס הגניזה החזותי (בטא): מפה אינטראקטיבית חדשה של חיבורים טקסטואליים ברחבי גניזת קהיר. המיקומים והאשכולות מחושבים אלגוריתמית מתוך חיבורים טקסטואליים — הקִרבה משקפת דמיון טקסטואלי, לא מקור פיזי משותף."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "An introduction to the Genizah and this site (Start Here): a guided introduction to the Cairo Genizah, prepared searches, selected manuscripts, and research tools.",
                "he": "דף היכרות עם הגניזה ואתר הגניזה („התחילו כאן\"): מבוא מודרך לגניזת קהיר, חיפושים מוכנים, כתבי יד נבחרים וכלי מחקר."
            },
            {
                "category": "feature",
                "apps": [
                    "web"
                ],
                "en": "Letter-level parallels search — faster and more precise. Get the most from the parallels search by entering several textual witnesses.",
                "he": "חיפוש מקבילות ברמת האות - חיפוש מהיר ומדויק יותר. מצו את חיפוש המקבילות בעזרת הזנת כמה עדי נוסח לחיפוש."
            },
            {
                "category": "feature",
                "apps": [
                    "desktop"
                ],
                "en": "Letter-level search: a new way to find parallels in the Composition Search tab. It is much faster than chunk search and returns far fewer unrelated results.",
                "he": "חיפוש ברמת האות: דרך חדשה למצוא מקבילות בלשונית חיפוש חיבורים. הוא מהיר בהרבה מחיפוש מקטעים ומחזיר הרבה פחות תוצאות לא רלוונטיות."
            }
        ]
    }
]
