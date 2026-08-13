# -*- coding: utf-8 -*-
"""Reader-facing review dialog for one computed identification."""

from __future__ import annotations

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
    published_reviews,
    reviews_enabled,
    submit_review,
)
from web.safe_storage import get_session_uuid
from web.supabase_client import get_client, get_user_client


REVIEW_ACTION_CLASS = "gs-identification-review-action"
REVIEW_DIALOG_CLASS = "gs-identification-review-dialog"
REVIEW_PUBLIC_CLASS = "gs-identification-review-public"
REPORT_ADDRESS = "gershuni@gmail.com"


_COPY: Dict[str, Dict[str, str]] = {
    "action": {"en": "Review this match", "he": "בדיקת התאמה זו"},
    "title": {"en": "Review this computed match", "he": "בדיקת ההתאמה המחושבת"},
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
        "en": "Published community reviews",
        "he": "בדיקות קהילה שפורסמו",
    },
    "public_caveat": {
        "en": "Moderated responses are evidence for review, not a scholarly consensus.",
        "he": "תגובות שנבדקו הן חומר לבחינה, לא קונצנזוס מחקרי.",
    },
    "public_empty": {
        "en": "No approved community reviews yet.",
        "he": "עדיין אין בדיקות קהילה מאושרות.",
    },
    "loading": {"en": "Loading approved reviews…", "he": "הבדיקות המאושרות נטענות…"},
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


def _render_published_item(review: Mapping[str, Any], lang: str) -> None:
    relation = str(review.get("relation_verdict") or "")
    relation_label = relation_verdict_text(relation, lang)
    with ui.column().classes("w-full gap-1 p-2 rounded").style(
        "background: var(--surface-secondary);"
    ):
        ui.label(relation_label).classes("text-sm font-medium")
        novelty = review.get("direct_novelty")
        if relation == RELATION_DIRECT_WITNESS and novelty:
            ui.label(direct_novelty_text(novelty, lang)).classes(
                "text-xs").style("color: var(--text-secondary);")
        comment = str(review.get("comment") or "").strip()
        if comment:
            ui.label(comment).classes("text-sm whitespace-pre-wrap").props("dir=auto")


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

    def _build_dialog() -> tuple[Any, Any]:
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

            ui.separator()
            ui.label(review_text("public_title", lang)).classes("font-medium")
            ui.label(review_text("public_caveat", lang)).classes("text-xs").style(
                "color: var(--text-secondary);")
            with ui.column().classes(
                f"{REVIEW_PUBLIC_CLASS} w-full gap-2"
            ) as public_box:
                ui.label(review_text("loading", lang)).classes("text-sm")
        return dialog, public_box

    async def _open() -> None:
        if not dialog_state:
            dialog_state["dialog"], dialog_state["public_box"] = _build_dialog()
        dialog = dialog_state["dialog"]
        public_box = dialog_state["public_box"]
        dialog.open()
        try:
            # Public RPC output contains no reviewer key or user id.
            public_client = get_client()
            rows = await run.io_bound(
                published_reviews, identification_id, client=public_client)
        except Exception:
            rows = ()
        public_box.clear()
        with public_box:
            if not rows:
                ui.label(review_text("public_empty", lang)).classes("text-sm")
            for row in rows:
                _render_published_item(row, lang)

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
    "direct_novelty_text",
    "relation_verdict_text",
    "render_identification_review_action",
    "report_mailto",
    "review_text",
]
