"""The ``filters.criteria`` request shape of /api/search, /api/parallels and the
MCP tools (PR 2 of docs/plans/CATALOGUE_CRITERIA_PLAN.md, §6.4).

Built from the registry (shared/catalog_criteria.py), so a criterion added
there is accepted here with no other change (rule 1). Pydantic checks the
field names and types; ``clean_criteria(strict=True)`` checks the rest (a
source where the criterion has one source, whole-number bounds). The values a
list accepts are not listed here: ``GET /api/criteria`` serves them with their
counts, and ``validate_filter_values`` refuses one the catalogue does not
record.
"""
from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, create_model

from shared.catalog_criteria import CRITERIA, NOT_RECORDED, SECTIONS

Source = Literal['either', 'catalogue', 'measured']


class ListCriterion(BaseModel):
    """Values of a list criterion: any one matches (Attributes: any of)."""
    model_config = ConfigDict(extra='forbid')

    values: List[str] = Field(
        description=(f"Values as GET /api/criteria lists them (the catalogue's English values). "
                     f"'{NOT_RECORDED}' picks manuscripts with nothing recorded."),
    )
    mode: Optional[Literal['only', 'except']] = Field(
        default=None,
        description=("'only' (default): manuscripts with one of the values. 'except': every other "
                     "manuscript, including those with nothing recorded unless "
                     f"'{NOT_RECORDED}' is among the values."),
    )
    source: Optional[Source] = Field(
        default=None,
        description=("Only where the criterion lists sources: 'either' (default), 'catalogue' "
                     "or 'measured'."),
    )


class RangeCriterion(BaseModel):
    """A range criterion: either bound may be left open."""
    model_config = ConfigDict(extra='forbid')

    min: Optional[float] = Field(default=None, description='Lower bound, inclusive.')
    max: Optional[float] = Field(default=None, description='Upper bound, inclusive.')
    not_recorded: Optional[bool] = Field(
        default=None,
        description=('Also (or, with no bounds, only) manuscripts with nothing recorded.'),
    )
    source: Optional[Source] = Field(
        default=None,
        description=("Only where the criterion lists sources: 'either' (default), 'catalogue' "
                     "or 'measured'."),
    )


_SECTION_EN = {s.key: s.label_en for s in SECTIONS}


def _describe(criterion) -> str:
    section = _SECTION_EN[criterion.section]
    parts = [criterion.label_en if section == criterion.label_en
             else f"{criterion.label_en} ({section})"]
    if criterion.kind == 'range':
        parts.append('range' + (', whole numbers' if criterion.integer else '')
                     + (f', in {criterion.unit}' if criterion.unit else ''))
    else:
        parts.append('list')
    if criterion.sources:
        parts.append('sources: either, ' + ', '.join(criterion.sources))
    if criterion.key == 'period':
        parts.append("years CE; a catalogue record whose copy-date span overlaps the range "
                     'matches')
    return '; '.join(parts) + '.'


CriteriaModel = create_model(
    'CriteriaModel',
    __config__=ConfigDict(extra='forbid'),
    __doc__=("Manuscript features (catalogue criteria), ANDed with each other and with the other "
             "filters. Keys, values and counts: GET /api/criteria."),
    **{c.key: (Optional[ListCriterion if c.kind == 'list' else RangeCriterion],
               Field(default=None, description=_describe(c)))
       for c in CRITERIA},
)
