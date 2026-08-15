"""Canonical, database-free FJMS domain hierarchy shared by UI surfaces."""

FJMS_PARENT_ORDER = [
    "Bible: Texts and Translations",
    "Biblical Exegesis",
    "Rabbinic Literature",
    "Halakhic Literature and Talmudic Commentaries",
    "Derashot and Later Midrashim",
    "Philosophy, Theology, Ethical literature",
    "Kabbalah",
    "Polemics",
    "Historiography and geographical descriptions",
    "Occult Sciences",
    "Sciences",
    "Liturgy and Brakhot",
    "Piyut and its Interpretation",
    "Secular Poetry",
    "Stories and Belles Lettres",
    "Philology",
    "Documentary",
    "Ritual Objects",
    "Other Religions",
    "Teaching Aids,Pen Trials,Writing Exercises,Scribblings,Jotting",
    "Ancillaries to the Main Work",
    "Unspecified (Nature of text unclear after initial inspection)",
    "Unspecified Domain",
]
FJMS_PARENT_IDX = {name: i for i, name in enumerate(FJMS_PARENT_ORDER)}

FJMS_CHILD_ORDER = {
    "Bible: Texts and Translations": [
        "Bible: Texts", "Aramaic Targumim", "Arabic Tafsir",
        "Translations into other Languages", "Apocryphal Literature",
        "Massorah", "Lists of Parshiyyot and Haftarot", "Haftarot",
    ],
    "Biblical Exegesis": [
        "Biblical Exegesis- Rabbanite", "Biblical Exegesis- Karaite",
    ],
    "Rabbinic Literature": [
        "Mishnah: Texts and Translations", "Tosefta",
        "Talmud Bavli: Texts and Anthologies", "Minor Tractates",
        "Talmud Yerushalmi", "Midrash",
    ],
    "Halakhic Literature and Talmudic Commentaries": [
        "Mishnaic Commentaries", "Talmud Bavli Commentaries",
        "Talmud Yerushalmi Commentaries", "Talmudic Commentaries",
        "Halakhic", "Sifrei Mitzvot (Rabbinical)",
        "Responsa and Halakhic Decisions", "Minhagim",
        "Talmud – Introductions and Rules",
    ],
    "Derashot and Later Midrashim": ["Derashot", "Eulogies", "Later Midrashim"],
    "Philosophy, Theology, Ethical literature": [
        "Kalam", "Philosophy", "Logic", "Ethical Literature",
        "Mystical Literature (not Kabbalah)", "Sufi Literature",
        "Hermetic Literature", "Wisdom Literature",
        "Apocalyptic Literature", "Theology",
    ],
    "Kabbalah": [
        "Heikhalot", "Zohar literature",
        "Spanish and Provencal Kabbalah", "Lurianic Kabbalah",
    ],
    "Polemics": [
        "Polemics Karaite-Rabbanite", "Polemics Jewish-Christian",
        "Polemics Jewish-Muslim", "Polemics Rabbinical",
    ],
    "Occult Sciences": [
        "Theoretical Works", "Astrology", "Alchemy", "Magic Recipes",
        "Amulets", "Shimmush Tehillim", "Predicting the Future",
        "Revealing Treasures",
    ],
    "Sciences": ["Astronomy", "Mathematics", "Medicine", "Meteorology", "Physics"],
    "Liturgy and Brakhot": [
        "Common Prayers", "Brakhot", "Prayer Commentaries",
        "Karaite Prayers", "Occasional prayer", "Liturgical additions",
        "Baqqashot and Personal Prayers", "Passover Haggadah",
    ],
    "Piyut and its Interpretation": [
        "Piyyut", "Liturgical commentary", "Piyyut Commentaries",
    ],
    "Secular Poetry": ["Dirges"],
    "Philology": ["Grammar", "Dictionaries", "Glossaries", "Cantillation notes"],
    "Documentary": [
        "Letters", "Personal Status Documents and Legal documents",
        "Business Documents", "Lists", "Communal Documents", "Court Documents",
        "Governmental Documents", "Notes/Records", "Accounts",
    ],
    "Ritual Objects": ["Mezuzot", "Tefillin", "Torah scroll", "Esther Scroll"],
    "Other Religions": ["Christian", "Muslim"],
    "Ancillaries to the Main Work": [
        "Colophons", "Title Pages", "Table of contents", "Indices",
        "Calendars", "Teaching Aids",
    ],
    "Unspecified Domain": [
        "Blank", "Illegible", "Missing",
        "Cannot be determined from the catalogue", "Unidentified",
    ],
}
FJMS_CHILD_IDX = {
    parent: {name: i for i, name in enumerate(children)}
    for parent, children in FJMS_CHILD_ORDER.items()
}

FJMS_SUBCHILD_ORDER = {
    "Massorah": [
        "Masorah that follows the text order", "Cumulative or Comparative Masorah",
        "Masorah in Arabic", "Masorah Variants",
        "Diqduqe ha-Te'amim and Qunterese ha-Masorah", "Lists and Counts",
    ],
    "Mishnah: Texts and Translations": ["Mishnah: Texts", "Mishnah: Translations"],
    "Talmud Bavli: Texts and Anthologies": [
        "Talmud Bavli", "Talmud Bavli: Anthologies",
    ],
    "Midrash": ["Halakhic Midrashim", "Aggadic Midrashim"],
    "Halakhic": [
        "Halakhic - Saadia Gaon", "Halakhic - Shmuel ben Hofni Gaon",
        "Halakhot ha-Rif and its Commentaries", "Halakhic- Gaonim",
        "Mishneh Torah and its Commentaries", "Halakhic- Rishonim and Aharonim",
        "Halakhic- Karaite",
    ],
    "Responsa and Halakhic Decisions": [
        "Responsa- Gaonim", "Responsa- Rishonim and Aharonim", "Responsa- Karaite",
    ],
    "Kalam": ["Jewish Kalam", "Muslim Kalam"],
    "Philosophy": ["Aristotelian Philosophy", "Neoplatonic Philosophy"],
    "Theology": ["Legal theory"],
    "Predicting the Future": [
        "Dream interpretation", "Goralot (Lots)", "Goralot (Lots) in Sand",
        "Predictions by Thunder", "Palmistry", "Predictions by Ticks",
        "Physiognomy", "Hemorology/Horology",
    ],
    "Astronomy": ["Calendar"],
    "Medicine": ["Medical Works", "Medical Prescriptions", "Pharmacology"],
    "Glossaries": [
        "Biblical Glossary", "Mishnaic Glossary", "Talmudic Glossary",
        "Glossary for Piyyut",
    ],
    "Personal Status Documents and Legal documents": [
        "Get Halitzah", "Ketubbot", "Legal documents",
    ],
    "Business Documents": ["Monetary Issues", "Contracts"],
    "Lists": [
        "Book lists", "Shopping lists", "Charity Lists", "Genealogical Records",
        "Property Lists", "Lists of People", "Lists of Debts", "Responsa lists",
    ],
    "Communal Documents": [
        "Communal Registers", "Writs of Appointment", "Bans and Excommunications",
    ],
    "Court Documents": ["Court Records", "Court Registers"],
}
FJMS_SUBCHILD_IDX = {
    parent: {name: i for i, name in enumerate(children)}
    for parent, children in FJMS_SUBCHILD_ORDER.items()
}


def domain_sort_key(value: str) -> tuple:
    """Return a domain's canonical ``/catalog-browse`` hierarchy position."""
    text = str(value or "").strip()
    parts = [part.strip() for part in text.split(" / ") if part.strip()]
    node = parts[-1] if parts else ""
    fallback = 9999

    if node in FJMS_PARENT_IDX:
        return (FJMS_PARENT_IDX[node], -1, -1, text.casefold())

    for parent, children in FJMS_CHILD_IDX.items():
        if node in children:
            return (FJMS_PARENT_IDX.get(parent, fallback), children[node], -1, text.casefold())

    for child, subchildren in FJMS_SUBCHILD_IDX.items():
        if node not in subchildren:
            continue
        for parent, children in FJMS_CHILD_IDX.items():
            if child in children:
                return (
                    FJMS_PARENT_IDX.get(parent, fallback),
                    children[child],
                    subchildren[node],
                    text.casefold(),
                )

    if len(parts) > 1:
        parent_key = domain_sort_key(parts[0])
        if parent_key[0] != fallback:
            return (parent_key[0], parent_key[1], fallback, text.casefold())
    return (fallback, fallback, fallback, text.casefold())
