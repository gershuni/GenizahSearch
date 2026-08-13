# -*- coding: utf-8 -*-
"""Bilingual, data-driven welcome launchpad for first-time and expert visitors."""

from __future__ import annotations

import html
import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from nicegui import ui

from web.analytics import posthog_capture
from web.atlas_assets import atlas_preview_available
from web.components.typography import h1, h2, h3
from web.discovery_assets import discovery_available
from web.feature_flags import WEB_PUZZLE_ENABLED
from web.pages.atlas import create_embedded_atlas
from web.random_fragment import navigate_random_fragment
from web.start_content import (
    StartContentError,
    demo_url,
    live_computed_candidates,
    load_start_content,
    localized,
    manuscript_url,
    puzzle_url,
    search_url,
    work_url,
)
from web.translations import get_language


logger = logging.getLogger(__name__)

_ROUTE_COPY = {
    "explore": {
        "icon": "auto_awesome",
        "title": {
            "en": "Explore Genizah Manuscripts",
            "he": "עיינו בכתבי יד מן הגניזה",
        },
        "description": {
            "en": "Start with a real manuscript, a visual map, or a surprise from the corpus.",
            "he": "התחילו בכתב יד אמיתי, במפה חזותית או בהפתעה מתוך הקורפוס.",
        },
    },
    "search": {
        "icon": "search",
        "title": {"en": "Search with us", "he": "חפשו איתנו"},
        "description": {
            "en": "Try a prepared query, then change it once you see how the search works.",
            "he": "נסו שאילתה מוכנה, ואז שנו אותה לאחר שתראו כיצד החיפוש פועל.",
        },
    },
    "research": {
        "icon": "science",
        "title": {"en": "Work like a researcher", "he": "עבדו כמו חוקרי גניזה"},
        "description": {
            "en": "Enter the experimental tools with a concrete manuscript or task already in hand.",
            "he": "היכנסו לכלים הניסיוניים כשכבר יש בידכם כתב יד או משימה מוחשית.",
        },
    },
}

_REVOLUTIONS_COPY = (
    {
        "number": "1",
        "title": {"en": "The Genizah comes to light", "he": "הגניזה יוצאת לאור"},
        "body": {
            "en": "Solomon Schechter and other pioneers brought the manuscripts of the Cairo Genizah into the center of scholarly attention. Major collections took shape in libraries where the fragments could be preserved and studied.",
            "he": "שלמה זלמן שכטר וחלוצים נוספים הביאו את כתבי היד של גניזת קהיר אל מרכז תשומת הלב המחקרית. בספריות התגבשו אוספים גדולים שבהם יכלו הקטעים להישמר ולהיחקר.",
        },
        "links": (),
    },
    {
        "number": "2",
        "title": {"en": "A century of scholarship", "he": "מאה של מחקר"},
        "body": {
            "en": "Through the twentieth century, thousands of scholars catalogued, identified, joined, edited, and interpreted fragments. Their cumulative work turned scattered leaves into an extraordinary record of Jewish life and writing.",
            "he": "במהלך המאה העשרים אלפי חוקרות וחוקרים קִטלגו, זיהו, צירפו, ההדירו ופירשו קטעים. עבודתם המצטברת הפכה דפים מפוזרים לתיעוד יוצא דופן של החיים והיצירה היהודיים.",
        },
        "links": (),
    },
    {
        "number": "3",
        "title": {"en": "Friedberg and Ktiv bring the collections together", "he": "פרידברג וכתיב מאחדים את האוספים"},
        "body": {
            "en": "The Friedberg Genizah Project brought together high-resolution images of most Genizah fragments, catalogue records, and the work of dedicated cataloguing teams. Ktiv at the National Library of Israel, created in partnership with FJMS, carried this material into a broader international manuscript portal.",
            "he": "פרויקט הגניזה של פרידברג ריכז תמונות ברזולוציה גבוהה של מרבית קטעי הגניזה, רשומות קטלוגיות ועבודת צוותי קטלוג ייעודיים. מפעל כתיב של הספרייה הלאומית, שנוצר בשותפות עם FJMS, שילב את החומר הזה בפורטל בין־לאומי רחב יותר של כתבי יד.",
        },
        "links": (
            ("Friedberg Genizah Project (FJMS)", "https://fjms.genizah.org/"),
            ("Ktiv — National Library of Israel", "https://www.nli.org.il/en/discover/manuscripts/hebrew-manuscripts"),
        ),
    },
    {
        "number": "4",
        "title": {"en": "MiDRASH makes the corpus searchable", "he": "MiDRASH הופך את הקורפוס לבר־חיפוש"},
        "body": {
            "en": "The MiDRASH project used handwriting-text recognition (HTR) to create automatic transcriptions and make medieval Hebrew-script manuscripts searchable. These machine readings can contain errors. This website builds on that achievement with variant-aware retrieval, textual alignment and similarity, graph clustering, research tools, and curated information from many sources and libraries.",
            "he": "פרויקט MiDRASH השתמש בזיהוי טקסט בכתב יד (HTR) כדי ליצור תעתיקים אוטומטיים ולהפוך כתבי יד מימי הביניים הכתובים באותיות עבריות לבני־חיפוש. בקריאת המכונה עלולות ליפול טעויות. האתר הזה נבנה על ההישג הזה ומוסיף חיפוש המודע לחילופי כתיב, יישור ודמיון טקסטואלי, קיבוץ ברשת, כלי מחקר ומידע שנאצר ממקורות ומספריות רבות.",
        },
        "links": (("MiDRASH", "https://www.midrash.eu/"),),
    },
)

_UI_COPY = {
    "title": {"en": "Explore the Cairo Genizah", "he": "גלו את גניזת קהיר"},
    "lead": {
        "en": "Choose a way into the Cairo Genizah. Every card opens a real manuscript, search, or research tool.",
        "he": "בחרו דרך להיכנס אל גניזת קהיר. כל כרטיס פותח כתב יד, חיפוש או כלי מחקר אמיתי.",
    },
    "about_link": {
        "en": "New to the Cairo Genizah? Read its story",
        "he": "חדשים בגניזת קהיר? קראו את סיפורה",
    },
    "choose": {"en": "Choose your route", "he": "בחרו מסלול"},
    "revolutions_title": {"en": "The four revolutions in Genizah Studies", "he": "ארבע המהפכות בחקר הגניזה"},
    "revolutions_intro": {
        "en": "Access to the Genizah changed in four cumulative waves. Each rests on the people and institutions that came before it.",
        "he": "הגישה אל הגניזה השתנתה בארבעה גלים מצטברים. כל אחד מהם נשען על האנשים והמוסדות שקדמו לו.",
    },
    "revolutions_conclusion": {
        "en": "Together, these layers allow every scholar to search for a term, test joins between torn fragments, and look for unknown textual witnesses. Textual algorithms can now begin to map the Genizah as a whole: which manuscripts are connected, and which may preserve a known work? The work is only beginning, and the future is promising.",
        "he": "השכבות האלה מאפשרות לכל חוקר וחוקרת לחפש מונח, לבדוק צירופים בין קטעים קרועים ולבקש עדי נוסח שטרם זוהו. כעת אלגוריתמים טקסטואליים יכולים להתחיל למפות את הגניזה כמכלול: אילו כתבי יד קשורים זה לזה, ואילו מהם עשויים לשמר חיבור מוכר? העבודה רק בראשיתה, והעתיד מבטיח.",
    },
    "selected_manuscripts": {"en": "Selected manuscripts", "he": "כתבי יד נבחרים"},
    "selected_manuscripts_intro": {
        "en": "Move across genres: famous texts, prayers, poetry, magic, letters, legal documents, calendars, and more.",
        "he": "עברו בין סוגות: חיבורים מפורסמים, תפילות, פיוטים, מאגיה, מכתבים, מסמכים משפטיים, לוחות שנה ועוד.",
    },
    "true_random": {"en": "Open a truly random fragment", "he": "פתחו קטע אקראי באמת"},
    "atlas_title": {"en": "Discover the Visual Genizah Atlas", "he": "גלו את אטלס הגניזה החזותי"},
    "atlas_description": {
        "en": "Explore an algorithmic map of textual connections. Proximity reflects textual similarity, not physical provenance.",
        "he": "גלו מפה אלגוריתמית של קשרים טקסטואליים. קִרבה במפה משקפת דמיון טקסטואלי, לא מקור פיזי משותף.",
    },
    "atlas_embed_open": {
        "en": "Open the interactive Atlas here",
        "he": "פתחו כאן את האטלס האינטראקטיבי",
    },
    "atlas_open_full": {
        "en": "Open the full Atlas page",
        "he": "פתחו את עמוד האטלס המלא",
    },
    "prepared_searches": {"en": "Prepared searches", "he": "חיפושים מוכנים"},
    "prepared_searches_intro": {
        "en": "The site supplies the first question. Open the results, then edit the query to follow your curiosity.",
        "he": "האתר מספק את השאלה הראשונה. פתחו את התוצאות, ואז ערכו את השאילתה לפי סקרנותכם.",
    },
    "known_works": {"en": "Browse a known catalogued work", "he": "עיינו בחיבור מקוטלג מוכר"},
    "known_works_intro": {
        "en": "These links search scholarly catalogue assignments rather than manuscript transcription text.",
        "he": "הקישורים האלה מחפשים שיוכים בקטלוג המחקרי, ולא בטקסט של תעתיקי כתבי היד.",
    },
    "research_tools": {"en": "Research tools and experiments", "he": "כלי מחקר וניסויים"},
    "research_intro": {
        "en": "Start from a prepared task. Experimental results remain evidence to examine, not a scholarly verdict.",
        "he": "התחילו ממשימה מוכנה. תוצאות ניסיוניות הן ראיות לבדיקה, לא הכרעה מחקרית.",
    },
    "computed_title": {"en": "A new approach: Computed Identifications (Beta)", "he": "גישה חדשה: זיהויים ממוחשבים (בטא)"},
    "computed_description": {
        "en": "Textual algorithms compare fragments with known works across the corpus and offer matches for scholarly examination. Every match is an automatic suggestion, not a reviewed identification.",
        "he": "אלגוריתמים טקסטואליים משווים קטעים לחיבורים מוכרים ברחבי הקורפוס ומציעים התאמות לבדיקה מחקרית. כל התאמה היא הצעה אוטומטית, לא זיהוי שנבדק.",
    },
    "computed_examples_intro": {
        "en": "Explore selected examples across several genres. Open one to inspect the manuscript and its evidence, and decide for yourself.",
        "he": "עיינו בדוגמאות נבחרות מכמה סוגות. פתחו אחת מהן, בדקו את כתב היד ואת הראיות והכריעו בעצמכם.",
    },
    "computed_generic_intro": {
        "en": "Explore the experimental finding aid. Curated examples will appear here only after scholarly review.",
        "he": "עיינו בכלי האיתור הניסיוני. דוגמאות נבחרות יוצגו כאן רק לאחר בדיקה מחקרית.",
    },
    "computed_open_all": {"en": "Explore all computed matches", "he": "לעיון בכל ההתאמות הממוחשבות"},
    "other_research_tools": {"en": "More research tools", "he": "כלי מחקר נוספים"},
    "show_more": {"en": "Show me more", "he": "הציגו לי עוד"},
    "open": {"en": "Open", "he": "פתיחה"},
    "simple": {"en": "Simple", "he": "פשוט"},
    "advanced": {"en": "Advanced", "he": "מתקדם"},
    "research": {"en": "Research", "he": "מחקרי"},
    "content_error": {
        "en": "The guided examples are temporarily unavailable. You can still open Search or Browse.",
        "he": "הדוגמאות המודרכות אינן זמינות כרגע. עדיין אפשר לפתוח חיפוש או עיון בכתבי יד.",
    },
}


def _copy(key: str, lang: str) -> str:
    return _UI_COPY[key]["he" if lang == "he" else "en"]


def _track(event: str, *, route_id: str, action_id: str, difficulty: str) -> None:
    """Emit only the launchpad's bounded, content-free analytics contract."""
    posthog_capture(
        event,
        {
            "route_id": route_id,
            "action_id": action_id,
            "difficulty": difficulty,
        },
    )


def _native_card(
    *,
    title: str,
    description: str,
    href: str,
    route_id: str,
    action_id: str,
    difficulty: str,
    icon: str = "arrow_forward",
    eyebrow: str | None = None,
    image_src: str | None = None,
    image_alt: str | None = None,
    reference_href: str | None = None,
    reference_label: str | None = None,
    extra_class: str = "",
    event: str = "welcome_action_clicked",
) -> None:
    """Render an actual anchor so keyboard and browser link behavior stay native."""
    aria = html.escape(f"{title}. {description}", quote=True)

    def render_contents() -> None:
        if image_src:
            alt = html.escape(image_alt or "", quote=True)
            ui.image(image_src).classes("start-card-image").props(
                f'loading=lazy alt="{alt}"'
            )
        with ui.element("div").classes("start-card-body"):
            if eyebrow:
                ui.label(eyebrow).classes("start-eyebrow")
            with ui.row().classes("w-full items-start justify-between gap-2 no-wrap"):
                h3(title, classes="start-card-title")
                ui.icon(icon).classes("start-card-arrow").props("aria-hidden=true")
            ui.label(description).classes("start-card-description")

    def primary_link(classes: str):
        return (
            ui.link(target=href)
            .classes(classes)
            .props(f'aria-label="{aria}"')
            .on(
                "click",
                lambda: _track(
                    event,
                    route_id=route_id,
                    action_id=action_id,
                    difficulty=difficulty,
                ),
            )
        )

    if reference_href and reference_label:
        with ui.element("article").classes(f"start-card {extra_class}"):
            with primary_link("start-card-primary no-underline"):
                render_contents()
            ui.link(reference_label, reference_href, new_tab=True).classes(
                "start-card-reference"
            ).props("rel=noopener noreferrer").on(
                "click",
                lambda: _track(
                    event,
                    route_id=route_id,
                    action_id=f"{action_id}_reference",
                    difficulty=difficulty,
                ),
            )
    else:
        with primary_link(f"start-card no-underline {extra_class}"):
            render_contents()


def _show_more_collection(
    *,
    entries: Sequence[Mapping[str, Any]],
    featured_count: int,
    route_id: str,
    action_id: str,
    difficulty: str,
    render_entry: Callable[[Mapping[str, Any]], None],
    lang: str,
) -> None:
    """Show a deterministic first window and replace it in-place on demand."""
    offset = {"value": 0}
    container = ui.element("div").classes("start-card-grid w-full")

    def render_window() -> None:
        container.clear()
        if not entries:
            return
        start = offset["value"] % len(entries)
        visible = [entries[(start + i) % len(entries)] for i in range(min(featured_count, len(entries)))]
        with container:
            for entry in visible:
                render_entry(entry)

    def show_more() -> None:
        offset["value"] = (offset["value"] + featured_count) % len(entries)
        render_window()
        _track(
            "welcome_more_clicked",
            route_id=route_id,
            action_id=action_id,
            difficulty=difficulty,
        )

    render_window()
    if len(entries) > featured_count:
        with ui.row().classes("w-full justify-center mt-2"):
            ui.button(_copy("show_more", lang), icon="refresh", on_click=show_more).props(
                "outline no-caps"
            ).classes("start-touch-target").props(
                f'aria-label="{html.escape(_copy("show_more", lang), quote=True)}"'
            )


def _navigate_true_random() -> None:
    _track(
        "welcome_action_clicked",
        route_id="explore",
        action_id="true_random_fragment",
        difficulty="introductory",
    )
    navigate_random_fragment()


def _render_atlas_invitation(lang: str) -> None:
    """Offer a lazy inline Atlas while retaining the dedicated full page."""
    mounted = {"value": False}
    container = ui.column().classes("w-full gap-3").mark("start-atlas-embed")

    def mount_atlas() -> None:
        if mounted["value"]:
            return
        mounted["value"] = True
        _track(
            "welcome_action_clicked",
            route_id="explore",
            action_id="visual_atlas_embed",
            difficulty="introductory",
        )
        invitation.set_visibility(False)
        with container:
            create_embedded_atlas(instance_id="start-atlas", height_px=520)
            ui.link(_copy("atlas_open_full", lang), "/atlas").classes(
                "start-atlas-full-link"
            ).on(
                "click",
                lambda: _track(
                    "welcome_action_clicked",
                    route_id="explore",
                    action_id="visual_atlas_full",
                    difficulty="introductory",
                ),
            )

    with container:
        with ui.element("div").classes("start-atlas-invitation w-full") as invitation:
            ui.icon("hub").classes("text-3xl").props("aria-hidden=true")
            with ui.column().classes("gap-2 flex-1"):
                ui.label(_copy("atlas_description", lang)).classes(
                    "start-card-description"
                )
                with ui.row().classes("start-actions"):
                    ui.button(
                        _copy("atlas_embed_open", lang),
                        icon="open_in_full",
                        on_click=mount_atlas,
                    ).props("no-caps color=primary").classes(
                        "start-touch-target"
                    ).mark("start-atlas-open")
                    ui.link(_copy("atlas_open_full", lang), "/atlas").classes(
                        "start-action-secondary"
                    ).on(
                        "click",
                        lambda: _track(
                            "welcome_action_clicked",
                            route_id="explore",
                            action_id="visual_atlas_full",
                            difficulty="introductory",
                        ),
                    )


def _route_card(route_id: str, lang: str) -> None:
    route = _ROUTE_COPY[route_id]
    selected = "he" if lang == "he" else "en"
    title = route["title"][selected]
    description = route["description"][selected]
    _native_card(
        title=title,
        description=description,
        href=f"#start-{route_id}",
        route_id=route_id,
        action_id=f"route_{route_id}",
        difficulty="introductory" if route_id != "research" else "research",
        icon=route["icon"],
        extra_class=f"start-route-card start-route-card-{route_id}",
        event="welcome_route_selected",
    )


def _render_content_error(lang: str) -> None:
    with ui.column().classes("w-full max-w-4xl mx-auto gap-4 p-4"):
        h1(_copy("title", lang), classes="text-3xl font-bold")
        with ui.element("div").classes("start-notice"):
            ui.icon("info").props("aria-hidden=true")
            ui.label(_copy("content_error", lang))
        with ui.row().classes("gap-3 flex-wrap"):
            ui.link("חיפוש" if lang == "he" else "Search", "/search").classes(
                "start-fallback-link"
            )
            ui.link("עיון בכתבי יד" if lang == "he" else "Browse manuscripts", "/browse").classes(
                "start-fallback-link"
            )


def create_start_page() -> None:
    """Render the three-route public welcome experience inside the site shell."""
    lang = get_language()
    rtl = lang == "he"
    direction = "rtl" if rtl else "ltr"
    align = "right" if rtl else "left"

    try:
        content = load_start_content()
    except StartContentError:
        logger.exception("Start-page curation failed validation")
        _render_content_error(lang)
        return

    ui.add_head_html(
        """
        <style>
        .start-page { direction: var(--start-dir); text-align: var(--start-align); color: var(--text-primary); }
        .start-hero {
            position: relative; isolation: isolate; overflow: hidden;
            padding: clamp(1.25rem, 4vw, 2.75rem);
            border: 1px solid var(--border-light);
            border-radius: 22px;
            background:
                radial-gradient(circle at 88% 0%, color-mix(in srgb, var(--primary-600) 18%, transparent), transparent 34%),
                linear-gradient(135deg, var(--bg-card), var(--bg-secondary));
            box-shadow: var(--shadow-md);
        }
        .start-hero-lead { max-width: 760px; color: var(--text-secondary); font-size: 1.05rem; line-height: 1.75; }
        .start-about-link {
            min-height: 44px; display: inline-flex; align-items: center; gap: .45rem;
            margin-top: .65rem; color: var(--primary-700); font-weight: 650;
            text-decoration: none;
        }
        .start-about-link:hover { text-decoration: underline; text-underline-offset: 3px; }
        .start-section { scroll-margin-top: 1rem; padding-block: clamp(1.25rem, 3vw, 2.25rem); }
        .start-section + .start-section { border-top: 1px solid var(--border-light); }
        .start-section-intro { max-width: 760px; color: var(--text-secondary); line-height: 1.65; }
        .start-revolutions-summary {
            min-height: 56px; display: flex; align-items: center; justify-content: space-between;
            gap: 1rem; cursor: pointer; list-style: none; border-radius: 10px;
        }
        .start-revolutions-summary::-webkit-details-marker { display: none; }
        .start-revolutions-summary:focus-visible {
            outline: 3px solid var(--primary-400); outline-offset: 4px;
        }
        .start-revolutions-summary h2 { margin: 0; }
        .start-revolutions-chevron { flex: 0 0 auto; transition: transform .2s ease; }
        .start-revolutions-disclosure[open] .start-revolutions-chevron { transform: rotate(180deg); }
        .start-revolutions-content { padding-top: .75rem; }
        .start-card-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(min(100%, 245px), 1fr));
            gap: 1rem;
        }
        .start-card {
            display: flex;
            flex-direction: column;
            min-width: 0;
            min-height: 164px;
            color: inherit;
            border: 1px solid var(--border-light);
            border-radius: 14px;
            overflow: hidden;
            background: var(--bg-card);
            transition: transform .16s ease, border-color .16s ease, box-shadow .16s ease;
        }
        .start-card:hover { transform: translateY(-2px); border-color: var(--primary-600); box-shadow: 0 8px 24px rgba(0,0,0,.09); }
        .start-card:focus-visible { outline: 3px solid var(--primary-600); outline-offset: 3px; }
        .start-card-primary { display: flex; flex: 1; flex-direction: column; color: inherit; }
        .start-card-primary:focus-visible { outline: 3px solid var(--primary-600); outline-offset: -3px; }
        .start-card-image { width: 100%; height: 164px; background: var(--bg-secondary); }
        .start-card-image img { width: 100%; height: 164px; object-fit: cover; }
        .start-card-body { display: flex; flex: 1; flex-direction: column; gap: .5rem; padding: 1rem; }
        .start-card-title { margin: 0; color: var(--text-primary); font-size: 1.02rem; font-weight: 700; line-height: 1.45; }
        .start-card-description { color: var(--text-secondary); font-size: .9rem; line-height: 1.6; }
        .start-card-reference {
            min-height: 44px; display: flex; align-items: center; padding: .55rem 1rem;
            border-top: 1px solid var(--border-light); color: var(--primary-700);
            font-size: .85rem; font-weight: 700; text-decoration: underline;
            text-underline-offset: 2px;
        }
        .start-card-arrow { color: var(--primary-600); flex: none; }
        .start-route-card {
            min-height: 188px; border-top: 3px solid var(--primary-600);
            box-shadow: var(--shadow-sm);
        }
        .start-route-card .start-card-body { padding: 1.15rem; }
        .start-route-card .start-card-arrow {
            display: inline-flex; align-items: center; justify-content: center;
            width: 2.25rem; height: 2.25rem; border-radius: 999px;
            background: color-mix(in srgb, var(--primary-600) 12%, transparent);
        }
        .start-eyebrow {
            align-self: flex-start; padding: .2rem .55rem; border-radius: 999px;
            background: color-mix(in srgb, var(--primary-600) 13%, transparent);
            color: var(--primary-600); font-size: .72rem; font-weight: 700;
        }
        .start-touch-target { min-height: 44px; min-width: 44px; }
        .start-actions { display: flex; flex-wrap: wrap; gap: .75rem; }
        .start-action-primary, .start-action-secondary, .start-fallback-link {
            display: inline-flex; align-items: center; justify-content: center; min-height: 44px;
            border-radius: 10px; padding: .6rem 1rem; text-decoration: none; font-weight: 600;
        }
        .start-page a.start-action-primary {
            background: var(--primary-700); color: var(--text-inverse) !important;
            box-shadow: var(--shadow-sm);
        }
        .start-page a.start-action-primary:hover { filter: brightness(.96); box-shadow: var(--shadow-md); }
        .start-action-secondary, .start-fallback-link { border: 1px solid var(--border-light); color: var(--text-primary); }
        .start-notice {
            display: flex; gap: .75rem; align-items: flex-start; padding: 1rem;
            border: 1px solid var(--border-light); border-radius: 12px; color: var(--text-secondary);
            background: var(--bg-tertiary); line-height: 1.6;
        }
        .start-atlas-invitation {
            display: flex; gap: 1rem; align-items: flex-start; padding: 1.1rem;
            border: 1px solid var(--border-light); border-radius: 14px;
            background: var(--bg-card); box-shadow: var(--shadow-sm);
        }
        .start-atlas-invitation > .q-icon { color: var(--primary-600); flex: none; }
        .start-atlas-full-link {
            align-self: flex-start; min-height: 44px; display: inline-flex;
            align-items: center; color: var(--primary-600); font-weight: 600;
        }
        .start-revolution-grid {
            display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 245px), 1fr));
            gap: 1rem; padding: 0; margin: 1rem 0 0; list-style: none;
        }
        .start-revolution {
            min-width: 0; padding: 1.1rem; border: 1px solid var(--border-light);
            border-radius: 14px; background: var(--bg-tertiary);
        }
        .start-revolution-number {
            display: inline-flex; align-items: center; justify-content: center;
            width: 2rem; height: 2rem; border-radius: 999px; margin-bottom: .65rem;
            color: var(--text-inverse); background: var(--primary-700); font-weight: 800;
        }
        .start-revolution-body { color: var(--text-secondary); line-height: 1.65; }
        .start-source-links { display: flex; flex-wrap: wrap; gap: .6rem 1rem; margin-top: .75rem; }
        .start-source-link {
            min-height: 44px; display: inline-flex; align-items: center;
            color: var(--primary-600); font-weight: 600; text-decoration: underline;
            text-underline-offset: 2px;
        }
        .start-computed-feature {
            padding: clamp(1rem, 2.5vw, 1.5rem); border: 1px solid var(--primary-300);
            border-radius: 18px;
            background:
                radial-gradient(circle at 96% 0%, color-mix(in srgb, var(--primary-600) 14%, transparent), transparent 30%),
                color-mix(in srgb, var(--primary-600) 6%, var(--bg-card));
            box-shadow: var(--shadow-md);
        }
        @media (max-width: 420px) {
            .start-hero { padding: 1.1rem; border-radius: 12px; }
            .start-card-grid { grid-template-columns: 1fr; }
            .start-actions > * { width: 100%; }
        }
        @media (prefers-reduced-motion: reduce) { .start-card { transition: none; } }
        </style>
        """
    )

    with ui.column().classes("start-page w-full max-w-7xl mx-auto gap-0 fade-in px-3 sm:px-5").style(
        f"--start-dir: {direction}; --start-align: {align}; direction: {direction}; text-align: {align};"
    ).mark("start-page"):
        with ui.element("header").classes("start-hero w-full"):
            h1(_copy("title", lang), classes="text-3xl sm:text-4xl font-bold", style="margin:0;")
            ui.label(_copy("lead", lang)).classes("start-hero-lead mt-3")
            with ui.link(target="/about").classes("start-about-link"):
                ui.icon("auto_stories").props("aria-hidden=true")
                ui.label(_copy("about_link", lang))
            h2(_copy("choose", lang), classes="text-lg font-bold mt-7 mb-3")
            with ui.element("nav").classes("start-card-grid w-full").props(
                f'aria-label="{html.escape(_copy("choose", lang), quote=True)}"'
            ):
                for route_id in ("explore", "search", "research"):
                    _route_card(route_id, lang)

        with ui.element("details").classes(
            "start-section start-revolutions-disclosure w-full"
        ).props(
            "id=start-revolutions"
        ).mark("start-revolutions"):
            with ui.element("summary").classes(
                "start-revolutions-summary w-full"
            ).mark("start-revolutions-summary"):
                h2(_copy("revolutions_title", lang), classes="text-2xl font-bold")
                ui.icon("expand_more").classes("start-revolutions-chevron").props(
                    "aria-hidden=true"
                )
            with ui.element("div").classes("start-revolutions-content"):
                ui.label(_copy("revolutions_intro", lang)).classes(
                    "start-section-intro mt-2"
                )
                with ui.element("ol").classes("start-revolution-grid w-full"):
                    selected = "he" if rtl else "en"
                    for revolution in _REVOLUTIONS_COPY:
                        with ui.element("li").classes("start-revolution").mark(
                            "start-revolution-card"
                        ):
                            ui.label(str(revolution["number"])).classes(
                                "start-revolution-number"
                            ).props("aria-hidden=true")
                            h3(
                                str(revolution["title"][selected]),
                                classes="start-card-title mb-2",
                            )
                            ui.label(str(revolution["body"][selected])).classes(
                                "start-revolution-body"
                            )
                            if revolution["links"]:
                                with ui.element("div").classes("start-source-links"):
                                    for label, href in revolution["links"]:
                                        ui.link(label, href, new_tab=True).classes(
                                            "start-source-link"
                                        ).props("rel=noopener noreferrer")
                with ui.element("div").classes("start-notice mt-4"):
                    ui.icon("travel_explore").props("aria-hidden=true")
                    ui.label(_copy("revolutions_conclusion", lang))

        with ui.element("section").classes("start-section w-full").props("id=start-explore").mark("start-route-explore"):
            h2(_ROUTE_COPY["explore"]["title"][lang], classes="text-2xl font-bold")
            h3(_copy("selected_manuscripts", lang), classes="text-lg font-bold mt-5")
            ui.label(_copy("selected_manuscripts_intro", lang)).classes("start-section-intro mb-3")

            def render_manuscript(entry: Mapping[str, Any]) -> None:
                reference = entry.get("reference")
                _native_card(
                    title=localized(entry, "title", lang),
                    description=localized(entry, "description", lang),
                    href=manuscript_url(entry),
                    route_id="explore",
                    action_id=str(entry["id"]),
                    difficulty="introductory",
                    icon="menu_book",
                    eyebrow=f"{localized(entry, 'category', lang)} · {entry['shelfmark']}",
                    image_src=str(entry["thumbnail"]),
                    image_alt=localized(entry, "alt", lang),
                    reference_href=(str(reference["url"]) if reference else None),
                    reference_label=(localized(reference, "label", lang) if reference else None),
                )

            _show_more_collection(
                entries=content["manuscripts"],
                featured_count=content["featured_counts"]["manuscripts"],
                route_id="explore",
                action_id="more_manuscripts",
                difficulty="introductory",
                render_entry=render_manuscript,
                lang=lang,
            )
            with ui.element("div").classes("start-actions mt-4"):
                ui.button(
                    _copy("true_random", lang),
                    icon="casino",
                    on_click=_navigate_true_random,
                ).props("no-caps outline").classes("start-touch-target")

            if atlas_preview_available():
                h3(_copy("atlas_title", lang), classes="text-lg font-bold mt-7 mb-3")
                _render_atlas_invitation(lang)

        with ui.element("section").classes("start-section w-full").props("id=start-search").mark("start-route-search"):
            h2(_ROUTE_COPY["search"]["title"][lang], classes="text-2xl font-bold")
            h3(_copy("prepared_searches", lang), classes="text-lg font-bold mt-5")
            ui.label(_copy("prepared_searches_intro", lang)).classes("start-section-intro mb-3")

            def render_search(entry: Mapping[str, Any]) -> None:
                _native_card(
                    title=localized(entry, "title", lang),
                    description=localized(entry, "description", lang),
                    href=search_url(entry),
                    route_id="search",
                    action_id=str(entry["id"]),
                    difficulty=str(entry["difficulty"]),
                    icon="search",
                    eyebrow=_copy(str(entry["difficulty"]), lang),
                )

            _show_more_collection(
                entries=content["searches"],
                featured_count=content["featured_counts"]["searches"],
                route_id="search",
                action_id="more_searches",
                difficulty="mixed",
                render_entry=render_search,
                lang=lang,
            )

            h3(_copy("known_works", lang), classes="text-lg font-bold mt-8")
            ui.label(_copy("known_works_intro", lang)).classes("start-section-intro mb-3")

            def render_work(entry: Mapping[str, Any]) -> None:
                _native_card(
                    title=localized(entry, "title", lang),
                    description=localized(entry, "description", lang),
                    href=work_url(entry),
                    route_id="search",
                    action_id=str(entry["id"]),
                    difficulty="guided",
                    icon="category",
                )

            _show_more_collection(
                entries=content["works"],
                featured_count=content["featured_counts"]["works"],
                route_id="search",
                action_id="more_works",
                difficulty="guided",
                render_entry=render_work,
                lang=lang,
            )

        with ui.element("section").classes("start-section w-full").props("id=start-research").mark("start-route-research"):
            h2(_copy("research_tools", lang), classes="text-2xl font-bold")
            ui.label(_copy("research_intro", lang)).classes("start-section-intro mb-4")

            if discovery_available():
                with ui.element("div").classes("start-computed-feature w-full mb-7").mark(
                    "start-computed-feature"
                ):
                    h3(_copy("computed_title", lang), classes="text-xl font-bold")
                    candidates = live_computed_candidates(content)
                    intro_key = "computed_examples_intro" if candidates else "computed_generic_intro"
                    ui.label(_copy(intro_key, lang)).classes(
                        "start-section-intro mt-2 mb-3"
                    )
                    with ui.element("div").classes("start-notice mb-4"):
                        ui.icon("fact_check").props("aria-hidden=true")
                        ui.label(_copy("computed_description", lang))
                    ui.link(
                        _copy("computed_open_all", lang),
                        "/computed-identifications",
                    ).classes("start-action-primary mb-4").on(
                        "click",
                        lambda: _track(
                            "welcome_action_clicked",
                            route_id="research",
                            action_id="computed_identifications_generic",
                            difficulty="research",
                        ),
                    )

                    if candidates:

                        def render_candidate(entry: Mapping[str, Any]) -> None:
                            _native_card(
                                title=localized(entry, "title", lang),
                                description=localized(entry, "description", lang),
                                href=manuscript_url(entry, computed=True),
                                route_id="research",
                                action_id=str(entry["id"]),
                                difficulty="research",
                                icon="fact_check",
                                eyebrow=f"{localized(entry, 'category', lang)} · {entry['shelfmark']}",
                            )

                        _show_more_collection(
                            entries=candidates,
                            featured_count=content["featured_counts"]["computed_candidates"],
                            route_id="research",
                            action_id="more_computed_candidates",
                            difficulty="research",
                            render_entry=render_candidate,
                            lang=lang,
                        )

            h3(_copy("other_research_tools", lang), classes="text-lg font-bold mb-3")

            with ui.element("div").classes("start-card-grid w-full"):
                demos = content["demos"]
                if demos["parallels"]["enabled"]:
                    entry = demos["parallels"]
                    _native_card(
                        title=localized(entry, "title", lang),
                        description=localized(entry, "description", lang),
                        href=demo_url("parallels", entry),
                        route_id="research",
                        action_id=str(entry["id"]),
                        difficulty="research",
                        icon="compare_arrows",
                    )
                else:
                    _native_card(
                        title="מקבילות טקסטואליות" if rtl else "Textual parallels",
                        description="הדביקו טקסט וחפשו קטעים דומים בקורפוס." if rtl else "Paste text and look for similar passages across the corpus.",
                        href="/parallels",
                        route_id="research",
                        action_id="parallels_generic",
                        difficulty="research",
                        icon="compare_arrows",
                    )

                if demos["joins"]["enabled"]:
                    entry = demos["joins"]
                    _native_card(
                        title=localized(entry, "title", lang),
                        description=localized(entry, "description", lang),
                        href=demo_url("joins", entry),
                        route_id="research",
                        action_id=str(entry["id"]),
                        difficulty="research",
                        icon="join_inner",
                    )

                if WEB_PUZZLE_ENABLED:
                    entry = content["puzzle"]
                    _native_card(
                        title=localized(entry, "title", lang),
                        description=localized(entry, "description", lang),
                        href=puzzle_url(entry),
                        route_id="research",
                        action_id=str(entry["id"]),
                        difficulty="research",
                        icon="extension",
                    )

__all__ = ["create_start_page"]
