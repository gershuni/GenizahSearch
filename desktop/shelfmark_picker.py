"""The desktop "Multiple shelfmarks found" picker (catalogue-shelfmark plan, row G).

Shared by the Puzzle's add box, the Joins dialog's fragment B and Share a Discovery,
which until now either took the first match or dropped the choice. Extracted from
``GenizahGUI.browse_load``'s ``QInputDialog`` block, numbered labels included: the
number keeps otherwise identical rows (``Gaster … Ms. 1358`` x 110) apart. Browse and
the other existing picker sites keep their own copy in this step.
"""
from PyQt6.QtWidgets import QInputDialog

from genizah_core import tr


def option_label(opt, idx):
    base = opt['shelfmark']
    title = (opt.get('title') or "").strip()
    if title:
        base = f"{base} | {title}"
    label = f"{idx + 1}. {base}"
    if len(label) > 60:
        label = label[:57] + "..."
    return label


def pick_shelfmark_option(parent, options):
    """Let the user choose one of ``options`` (dicts with ``sys_id``/``shelfmark``/``title``).

    Returns the chosen dict, or None when the user cancels -- which is not "not found":
    callers must stop without adding anything. A single option is returned without asking.
    """
    if not options:
        return None
    if len(options) == 1:
        return options[0]
    labels = [option_label(opt, idx) for idx, opt in enumerate(options)]
    choice, ok = QInputDialog.getItem(
        parent, tr("Shelfmark"), tr("Multiple shelfmarks found. Select one:"), labels, 0, False
    )
    if not ok or choice not in labels:
        return None
    return options[labels.index(choice)]
