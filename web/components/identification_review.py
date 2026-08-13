# -*- coding: utf-8 -*-
"""Reader-facing review dialog for one computed identification."""

from __future__ import annotations

from html import escape
from typing import Any, Dict, Mapping, Optional
from urllib.parse import quote

from nicegui import run, ui

from web.identification_reviews import (
    DIRECT_NOVELTY_ALREADY_KNOWN,
    DIRECT_NOVELTY_OTHER_UNSURE,
    DIRECT_NOVELTY_POTENTIALLY_NEW,
    MAX_COMMENT_LENGTH,
    RELATION_DIRECT_WITNESS,
    RELATION_MANUSCRIPT_QUOTES_WORK,
    RELATION_NOT_MEANINGFUL,
    RELATION_OTHER_UNSURE,
    RELATION_SHARED_SOURCE,
    RELATION_WORK_QUOTES_MANUSCRIPT,
    IdentificationReviewError,
    ReviewSubmission,
    anonymous_reviewer_key,
    reviews_enabled,
    submit_review,
)
from web.safe_storage import get_session_uuid
from web.supabase_client import get_user_client


REVIEW_ACTION_CLASS = "gs-identification-review-action"
REVIEW_DIALOG_CLASS = "gs-identification-review-dialog"
REVIEW_PUBLIC_CLASS = "gs-identification-review-public"
REVIEW_PUBLIC_ITEM_CLASS = "gs-identification-review-public-item"
REVIEW_PUBLIC_RELATION_CLASS = "gs-identification-review-public-relation"
REVIEW_PUBLIC_NOVELTY_CLASS = "gs-identification-review-public-novelty"
REVIEW_PUBLIC_COMMENT_CLASS = "gs-identification-review-public-comment"
REPORT_ADDRESS = "gershuni@gmail.com"


_COPY: Dict[str, Dict[str, str]] = {
    "action": {"en": "Review this match", "he": "האם ההתאמה נכונה?"},
    "title": {
        "en": "Review this computed match",
        "he": "האם ההתאמה המחושבת נכונה?",
    },
    "beta": {"en": "Beta", "he": "בטא"},
    "question": {
        "en": "What best describes the relationship?",
        "he": "מה מתאר בצורה הטובה ביותר את הקשר?",
    },
    "direct_question": {
        "en": "If this is direct, is the identification…",
        "he": "אם זהו זיהוי ישיר, האם הזיהוי…",
    },
    "comment": {"en": "Optional comment", "he": "הערה (לא חובה)"},
    "comment_hint": {
        "en": "Add a source, correction, or explanation for the reviewer.",
        "he": "אפשר להוסיף מקור, תיקון או הסבר לבודק/ת.",
    },
    "moderation_note": {
        "en": (
            "Submissions are checked by an administrator. Approved answers and "
            "comments may be published without your identity."
        ),
        "he": (
            "ההצעות נבדקות בידי מנהל/ת. תשובות והערות שאושרו עשויות להתפרסם "
            "ללא זהות המציע/ה."
        ),
    },
    "public_title": {
        "en": "Human-reviewed assessment",
        "he": "הערכה שנבדקה ואושרה",
    },
    "public_caveat": {
        "en": "Checked and published by a project editor.",
        "he": "נבדק ופורסם בידי עורך/ת הפרויקט.",
    },
    "public_relation_label": {
        "en": "How well the match fits",
        "he": "עד כמה ההתאמה נכונה",
    },
    "public_novelty_label": {
        "en": "How new the identification is",
        "he": "עד כמה הזיהוי חדש",
    },
    "public_comment_label": {
        "en": "Published comment",
        "he": "הערה שפורסמה",
    },
    "not_assessed": {"en": "Not assessed", "he": "לא הוערך"},
    "submit": {"en": "Send for review", "he": "שליחה לבדיקה"},
    "cancel": {"en": "Cancel", "he": "ביטול"},
    "success": {
        "en": "Thank you — your assessment was sent for review.",
        "he": "תודה — ההערכה נשלחה לבדיקה.",
    },
    "choose": {
        "en": "Please choose the kind of relationship.",
        "he": "יש לבחור את סוג הקשר.",
    },
    "unavailable": {
        "en": "The review service is temporarily unavailable. You can email the report instead.",
        "he": "שירות הבדיקה אינו זמין כעת. אפשר לשלוח את הדיווח בדוא״ל במקום זאת.",
    },
    "report_problem": {"en": "Report a problem", "he": "דיווח על בעיה"},
    "email_instead": {"en": "Email instead", "he": "שליחה בדוא״ל במקום זאת"},
    "report_subject": {
        "en": "Computed identification report",
        "he": "Computed identification report",
    },
    "report_body": {
        "en": "Finding: {identification}\nData version: {version}\n\n",
        "he": "Finding: {identification}\nData version: {version}\n\n",
    },
}


_RELATION_OPTIONS = {
    "en": {
        RELATION_DIRECT_WITNESS: "Direct identification: the manuscript contains this work",
        RELATION_MANUSCRIPT_QUOTES_WORK: "The manuscript cites or refers to the work",
        RELATION_SHARED_SOURCE: "Both cite or draw on a shared source",
        RELATION_WORK_QUOTES_MANUSCRIPT: "The work cites or refers to the manuscript text",
        RELATION_NOT_MEANINGFUL: "Not a meaningful match",
        RELATION_OTHER_UNSURE: "Other / unsure",
    },
    "he": {
        RELATION_DIRECT_WITNESS: "זיהוי ישיר: כתב היד מכיל את החיבור",
        RELATION_MANUSCRIPT_QUOTES_WORK: "כתב היד מצטט את החיבור או מפנה אליו",
        RELATION_SHARED_SOURCE: "שניהם נשענים על מקור משותף או מצטטים אותו",
        RELATION_WORK_QUOTES_MANUSCRIPT: "החיבור מצטט את הטקסט שבכתב היד או מפנה אליו",
        RELATION_NOT_MEANINGFUL: "אין כאן התאמה משמעותית",
        RELATION_OTHER_UNSURE: "אחר / לא בטוח",
    },
}


_DIRECT_OPTIONS = {
    "en": {
        DIRECT_NOVELTY_POTENTIALLY_NEW: "Potentially new identification",
        DIRECT_NOVELTY_ALREADY_KNOWN: "Already known",
        DIRECT_NOVELTY_OTHER_UNSURE: "Other / unsure",
    },
    "he": {
        DIRECT_NOVELTY_POTENTIALLY_NEW: "זיהוי שעשוי להיות חדש",
        DIRECT_NOVELTY_ALREADY_KNOWN: "זיהוי שכבר ידוע",
        DIRECT_NOVELTY_OTHER_UNSURE: "אחר / לא בטוח",
    },
}


_RELATION_ICONS = {
    RELATION_DIRECT_WITNESS: "verified",
    RELATION_MANUSCRIPT_QUOTES_WORK: "format_quote",
    RELATION_SHARED_SOURCE: "account_tree",
    RELATION_WORK_QUOTES_MANUSCRIPT: "reply",
    RELATION_NOT_MEANINGFUL: "link_off",
    RELATION_OTHER_UNSURE: "help_outline",
}


_DIRECT_NOVELTY_ICONS = {
    DIRECT_NOVELTY_POTENTIALLY_NEW: "new_releases",
    DIRECT_NOVELTY_ALREADY_KNOWN: "history",
    DIRECT_NOVELTY_OTHER_UNSURE: "help_outline",
}


def _lang(lang: str) -> str:
    return "he" if lang == "he" else "en"


def review_text(key: str, lang: str = "en") -> str:
    return _COPY[key][_lang(lang)]


def relation_verdict_text(verdict: Any, lang: str = "en") -> str:
    lang = _lang(lang)
    return _RELATION_OPTIONS[lang].get(
        str(verdict or ""), _RELATION_OPTIONS[lang][RELATION_OTHER_UNSURE])


def direct_novelty_text(verdict: Any, lang: str = "en") -> str:
    lang = _lang(lang)
    return _DIRECT_OPTIONS[lang].get(
        str(verdict or ""), _DIRECT_OPTIONS[lang][DIRECT_NOVELTY_OTHER_UNSURE])


def relation_verdict_icon(verdict: Any) -> str:
    return _RELATION_ICONS.get(str(verdict or ""), "help_outline")


def direct_novelty_icon(verdict: Any) -> str:
    return _DIRECT_NOVELTY_ICONS.get(
        str(verdict or ""), "remove_circle_outline")


def _public_review_icon(
    name: str, tooltip: str, marker: str, *, color: str,
) -> None:
    ui.icon(name).classes(marker).style(
        f"font-size: 17px; color: {color}; opacity: 0.78;"
    ).props(
        f'role=img aria-label="{escape(tooltip, quote=True)}"'
    ).tooltip(tooltip)


def report_mailto(item: Mapping[str, Any], lang: str = "en",
                  sidecar_version: Any = None) -> Optional[str]:
    """Existing reproducible email channel, retained as a fallback."""
    identification = item.get("identification_id")
    if not identification or not sidecar_version:
        return None
    subject = quote(review_text("report_subject", lang))
    body = quote(review_text("report_body", lang).format(
        identification=identification, version=sidecar_version))
    return f"mailto:{REPORT_ADDRESS}?subject={subject}&body={body}"


def _context_page_number(item: Mapping[str, Any]) -> Optional[int]:
    value = item.get("first_match_page")
    if value is None:
        value = item.get("page_number")
    return value


def render_published_identification_reviews(
    reviews: Any, lang: str = "en",
) -> None:
    """Render approved human assessments as a subtle computed-card sibling."""
    rows = [dict(row) for row in (reviews or ())]
    if not rows:
        return
    lang = _lang(lang)
    provenance = (
        f'{review_text("public_title", lang)}. '
        f'{review_text("public_caveat", lang)}'
    )
    with ui.row().classes(
        f"{REVIEW_PUBLIC_CLASS} items-center gap-1 mt-1 px-1 flex-wrap"
    ).props(f'aria-label="{escape(provenance, quote=True)}"'):
        _public_review_icon(
            "fact_check", provenance, "gs-identification-review-public-source",
            color="var(--success)",
        )
        for review in rows:
            relation = str(review.get("relation_verdict") or "")
            novelty = review.get("direct_novelty")
            novelty_text = (
                direct_novelty_text(novelty, lang)
                if relation == RELATION_DIRECT_WITNESS and novelty
                else review_text("not_assessed", lang)
            )
            comment = str(review.get("comment") or "").strip()
            relation_tooltip = (
                f'{review_text("public_relation_label", lang)}: '
                f'{relation_verdict_text(relation, lang)}'
            )
            novelty_tooltip = (
                f'{review_text("public_novelty_label", lang)}: {novelty_text}'
            )
            with ui.row().classes(
                f"{REVIEW_PUBLIC_ITEM_CLASS} items-center gap-1"
            ):
                _public_review_icon(
                    relation_verdict_icon(relation), relation_tooltip,
                    REVIEW_PUBLIC_RELATION_CLASS, color="var(--success)",
                )
                _public_review_icon(
                    direct_novelty_icon(novelty), novelty_tooltip,
                    REVIEW_PUBLIC_NOVELTY_CLASS, color="var(--primary)",
                )
                if comment:
                    comment_tooltip = (
                        f'{review_text("public_comment_label", lang)}: {comment}'
                    )
                    _public_review_icon(
                        "comment", comment_tooltip, REVIEW_PUBLIC_COMMENT_CLASS,
                        color="var(--text-secondary)",
                    )


def render_identification_review_action(
    item: Mapping[str, Any], lang: str = "en", *, sidecar_version: Any = None,
    shown_relation: Any = None,
) -> None:
    """Render the beta action for one traceable identification leaf."""
    lang = _lang(lang)
    identification_id = str(item.get("identification_id") or "").strip()
    version = str(sidecar_version or "").strip()
    fallback = report_mailto(item, lang, version)
    if not identification_id or not version:
        return
    if not reviews_enabled():
        if fallback:
            ui.link(review_text("report_problem", lang), fallback).classes(
                f"{REVIEW_ACTION_CLASS} dnote text-xs")
        return

    # Findings pages render up to twenty leaf rows at once. Build the substantial
    # dialog only when its action is first opened; otherwise twenty hidden radio
    # forms would roughly double the page payload before a reader asks for one.
    dialog_state: Dict[str, Any] = {}

    def _build_dialog() -> Any:
        with ui.dialog() as dialog, ui.card().classes(
            f"{REVIEW_DIALOG_CLASS} w-full max-w-2xl p-5"
        ):
            if lang == "he":
                dialog.props("dir=rtl")
            with ui.row().classes("w-full items-center gap-2"):
                ui.label(review_text("title", lang)).classes("text-xl font-bold")
                ui.badge(review_text("beta", lang)).props("outline color=primary")

            ui.label(review_text("question", lang)).classes("font-medium mt-2")
            relation = ui.radio(_RELATION_OPTIONS[lang], value=None).classes(
                "w-full"
            ).props("aria-required=true")

            with ui.column().classes("w-full gap-1") as direct_block:
                ui.label(review_text("direct_question", lang)).classes("font-medium")
                direct_novelty = ui.radio(
                    _DIRECT_OPTIONS[lang], value=None).classes("w-full")
            direct_block.set_visibility(False)

            def _show_direct(event) -> None:
                direct_block.set_visibility(event.value == RELATION_DIRECT_WITNESS)
                if event.value != RELATION_DIRECT_WITNESS:
                    direct_novelty.value = None

            relation.on_value_change(_show_direct)

            comment = ui.textarea(
                label=review_text("comment", lang),
                placeholder=review_text("comment_hint", lang),
            ).classes("w-full").props(
                f"outlined autogrow counter maxlength={MAX_COMMENT_LENGTH}"
            )

            ui.label(review_text("moderation_note", lang)).classes("text-xs").style(
                "color: var(--text-secondary);")

            if fallback:
                ui.link(review_text("email_instead", lang), fallback).classes("text-xs")

            async def _submit() -> None:
                if relation.value not in _RELATION_OPTIONS[lang]:
                    ui.notify(review_text("choose", lang), type="warning")
                    return
                try:
                    # Read session/auth state on the UI loop. Worker threads
                    # cannot safely access NiceGUI's request-scoped storage.
                    reviewer_key = anonymous_reviewer_key(get_session_uuid())
                    client = get_user_client()
                    submission = ReviewSubmission(
                        identification_id=identification_id,
                        sidecar_version=version,
                        relation_verdict=str(relation.value),
                        direct_novelty=(
                            str(direct_novelty.value)
                            if relation.value == RELATION_DIRECT_WITNESS
                            and direct_novelty.value else None
                        ),
                        comment=comment.value,
                        anonymous_key=reviewer_key,
                        sys_id=item.get("sys_id"),
                        page_id=item.get("page_id"),
                        page_number=_context_page_number(item),
                        work_id=item.get("work_id") or item.get("display_work_id"),
                        displayed_relation=(
                            str(shown_relation) if shown_relation else
                            str(item.get("rendered_relation") or "") or None
                        ),
                    )
                    await run.io_bound(submit_review, submission, client=client)
                except IdentificationReviewError:
                    ui.notify(review_text("unavailable", lang), type="negative")
                    return
                dialog.close()
                ui.notify(review_text("success", lang), type="positive")

            with ui.row().classes("w-full justify-end gap-2"):
                ui.button(
                    review_text("cancel", lang), on_click=dialog.close).props("flat")
                ui.button(
                    review_text("submit", lang), on_click=_submit).props("color=primary")

        return dialog

    async def _open() -> None:
        if not dialog_state:
            dialog_state["dialog"] = _build_dialog()
        dialog = dialog_state["dialog"]
        dialog.open()

    ui.button(
        review_text("action", lang), icon="rate_review", on_click=_open,
    ).props("flat dense size=sm no-caps").classes(
        f"{REVIEW_ACTION_CLASS} dnote text-xs"
    )


__all__ = [
    "REPORT_ADDRESS",
    "REVIEW_ACTION_CLASS",
    "REVIEW_DIALOG_CLASS",
    "REVIEW_PUBLIC_CLASS",
    "REVIEW_PUBLIC_ITEM_CLASS",
    "direct_novelty_text",
    "relation_verdict_text",
    "render_identification_review_action",
    "render_published_identification_reviews",
    "report_mailto",
    "review_text",
]
