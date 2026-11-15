"""Reproducible expression rows from each dictionary's own text and listed verb stems."""
from __future__ import annotations

import json
import re
import sqlite3

from shared.blau_normalize import normalize_ja, lexical_key, grammatical_placeholder
from shared.ja_expression import (ALL_VERB_KINDS, scope_keys, lookup_key, PREPOSITIONS,
                                  preposition_stems)
from shared.ja_roots import StemPair, stem_numbers
from shared.ja_verb_forms import verb_form_index

CONSTRUCTION_DDL = """
CREATE TABLE constructions (id TEXT PRIMARY KEY, root_id TEXT NOT NULL, sub_id TEXT NOT NULL,
    text TEXT NOT NULL, root TEXT NOT NULL, stem INTEGER NOT NULL, source_head TEXT NOT NULL);
"""


def unit_form_rows(conn):
    """Explicit lexical alternatives, preserving the printed form as evidence."""
    from shared.ja_expression import word_keys
    rows = set()
    for uid, head in conn.execute('SELECT id, head_text FROM units'):
        head = head or ''
        literals = [r[0] for r in conn.execute('SELECT key FROM unit_keys WHERE unit_id=?', (uid,))]
        # A colon introduces a construction under the lemma before it.
        first = lexical_key(head.split(':', 1)[0])
        if ':' in head and len(first.split()) == 1:
            literals.append(first)
        # Short, vocalized parenthetical variants, not explanatory Hebrew prose.
        for note in re.findall(r'\(([^()]*)\)', head):
            if re.fullmatch(r'[א-ת\u0300-\u036f\u064b-\u065f]+', note):
                key = lexical_key(note)
                if len(key) >= 3 and any('\u064b' <= ch <= '\u065f' for ch in note):
                    literals.append(key)
                    if '\u064b' in note and key.endswith('א'):
                        rows.add((key[:-1], uid, key))
        for literal in literals:
            if ' ' not in literal and not grammatical_placeholder(literal, head):
                rows.update((alias, uid, literal) for alias in word_keys(literal))
        # Printed shadda can stand for the repeated final root consonant.
        token = head.split()[:1]
        if token and '\u0651' in token[0]:
            key = lexical_key(token[0])
            if len(key) == 2 and re.search(r'[א-ת][\u0300-\u036f\u064b-\u065f]*\u0651[^א-ת]*$', token[0]):
                rows.add((key + key[-1], uid, key))
    return sorted(rows)


def expression_material(conn, dictionary):
    original_factory = conn.row_factory
    try:
        return _expression_material(conn, dictionary)
    finally:
        conn.row_factory = original_factory


def _expression_material(conn, dictionary):
    """Scopes and inherited constructions; positions always belong to one source scope."""
    conn.row_factory = sqlite3.Row
    scopes, frames, pairs, head_stems = [], [], [], []
    if dictionary == 'friedman':
        from shared.friedman_service import construction_head, head_scope_text
        units = {r['id']: r for r in conn.execute('SELECT * FROM units ORDER BY ord')}
        roots = {}
        for row in conn.execute('SELECT unit_id, dotted FROM unit_roots'):
            roots.setdefault(row['unit_id'], []).append(row['dotted'])
        active = {}
        for uid, row in units.items():
            entry = row
            while entry['parent'] and entry['parent'] in units:
                entry = units[entry['parent']]
            eid = entry['id']
            nums = stem_numbers(row['binyan'])
            if nums:
                active[eid] = nums
                pairs.extend(StemPair(root, normalize_ja(root), n, uid, eid)
                             for root in roots.get(eid, ()) for n in nums)
            head = row['head_text'] or ''
            scopes.append(((uid, 'head', 0), head_scope_text(head)))
            # A head that opens with its entry's root is that verb in the stems of its paragraph,
            # whatever form it prints (פלח בה under IV is אפלח, יפלח in a text).
            opening = lexical_key(head_scope_text(head)).split()[:1]
            for root in roots.get(eid, ()):
                if opening and normalize_ja(opening[0]) == normalize_ja(root):
                    head_stems.extend((uid, 'head', 0, 0, f'@{normalize_ja(root)}:{n}', 1)
                                      for n in (nums or active.get(eid, ())))
            if row['parent'] and construction_head(head) and roots.get(eid):
                # Roman-numeral paragraphs govern the following subentries until the next
                # numbered paragraph. Several possible stem labels remain several tags.
                numbers = nums or active.get(eid, ())
                if numbers:
                    root = roots[eid][0]
                    for n in numbers:
                        frames.append((f'{uid}:{n}', uid, '', f'{root} {head_scope_text(head)}', root, n, head))
        for row in conn.execute('SELECT unit_id, n, quote, foreign_text FROM quotations'):
            scopes.append(((row['unit_id'], 'quotation', row['n']), row['quote']))
            if row['foreign_text'] and any('\u0621' <= c <= '\u064a' for c in row['foreign_text']):
                scopes.append(((row['unit_id'], 'arabic_quotation', row['n']), row['foreign_text']))
    else:
        from shared.blau_service import _root_label, form_head_alternatives, stem_row_numbers
        roots = {r['id']: r for r in conn.execute('SELECT * FROM roots')}
        stems = {r['id']: r for r in conn.execute('SELECT * FROM stems ORDER BY root_id, ordinal')}
        for row in stems.values():
            root = _root_label(roots[row['root_id']])
            nums = stem_row_numbers(row['binyan'], row['binyan_shift'], row['head_json'])
            pairs.extend(StemPair(root, normalize_ja(root), n, row['id'], row['root_id']) for n in nums)
            head = ' '.join(str(row[k] or '') for k in ('government', 'complement_ja')).strip()
            if head and nums:
                for n in nums:
                    frames.append((f"{row['id']}:{n}", row['root_id'], row['sub_entry_id'] or '',
                                   f'{root} {head}', root, n, head))
            arabic = str(row['complement_arabic'] or '').strip()
            if arabic and lookup_key(arabic) != lookup_key(row['complement_ja'] or '') and _governed_head(arabic):
                for n in nums:
                    frames.append((f"{row['id']}:{n}:ar", row['root_id'], row['sub_entry_id'] or '',
                                   f'{root} {arabic}', root, n, arabic))
        for row in roots.values():
            for n, name in enumerate(json.loads(row['names_json'] or '[]')):
                scopes.append((('root', row['id'], n, row['id'], None), name))
        for row in conn.execute('SELECT * FROM sub_entries'):
            for n, name in enumerate(json.loads(row['names_json'] or '[]')):
                scopes.append((('sub_entry', row['id'], n, row['root_id'], row['id']), name))
        for row in conn.execute('SELECT * FROM forms ORDER BY id'):
            for n, name in enumerate(form_head_alternatives(row['ja_text'], row['arabic_spelling'])):
                scopes.append((('form', row['id'], n, row['root_id'], row['sub_entry_id']), name))
                stem = stems.get(row['stem_id'])
                # A typed complement under a verb inherits that verb. Independent nominal
                # subentries and prose in notes/definitions/citations do not supply frames.
                if stem is not None and not row['sub_entry_id'] and _governed_head(name):
                    root = _root_label(roots[row['root_id']])
                    variant = json.loads(row['segments_json'] or '{}').get('variant_head') or {}
                    nums = stem_row_numbers(variant.get('binyan'), variant.get('binyan_shift'), json.dumps(variant))
                    nums = nums or stem_row_numbers(stem['binyan'], stem['binyan_shift'], stem['head_json'])
                    for number in nums:
                        frames.append((f"{row['id']}:{number}:{n}", row['root_id'], '',
                                       f'{root} {name}', root, number, name))
        for row in conn.execute('SELECT c.id, c.ja_text, f.root_id, f.sub_entry_id FROM citations c '
                                'JOIN forms f ON f.id=c.form_id WHERE c.ja_text IS NOT NULL'):
            scopes.append((('citation', row['id'], 0, row['root_id'], row['sub_entry_id']), row['ja_text']))
    index = verb_form_index(pairs)
    kinds = ALL_VERB_KINDS
    rows = []
    for identity, text in scopes:
        rows.extend((*identity, pos, key, peeled) for pos, key, peeled in scope_keys(text or '', index, kinds))
    present = set(rows)
    rows.extend(row for row in dict.fromkeys(head_stems) if row not in present)
    for fid, root_id, sub_id, text, root, stem, _head in frames:
        identity = ((root_id, 'construction', stem) if dictionary == 'friedman'
                    else ('construction', fid, 0, root_id, sub_id or None))
        keys = scope_keys(text, index, kinds)
        keys.append((0, f'@{normalize_ja(root)}:{stem}', 1))
        rows.extend((*identity, pos, key, peeled) for pos, key, peeled in dict.fromkeys(keys))
    return rows, frames


def _governed_head(head):
    """A printed prepositional complement, in either script, rather than a verb or noun."""
    words = lookup_key(head).split()
    return bool(words and (words[0] in PREPOSITIONS or preposition_stems(words[0])))


def rebuild_expression_tokens(conn, dictionary):
    rows, frames = expression_material(conn, dictionary)
    conn.execute('DELETE FROM tokens')
    conn.execute('DELETE FROM constructions')
    conn.executemany('INSERT INTO constructions VALUES (?,?,?,?,?,?,?)', frames)
    slots = ','.join('?' for _ in range(6 if dictionary == 'friedman' else 8))
    conn.executemany(f'INSERT INTO tokens VALUES ({slots})', rows)
    return len(rows)

def verify_search_indexes(conn, dictionary):
    """Recompute every derived relationship, spelling key and construction display."""
    from shared.blau_normalize import spelling_key
    _rows, frames = expression_material(conn, dictionary)
    stored_frames = {tuple(r) for r in conn.execute("SELECT * FROM constructions")}
    if stored_frames != set(frames):
        raise ValueError("construction relationships differ from the source paragraphs")
    if dictionary == "friedman":
        expected = set(unit_form_rows(conn))
        stored = {tuple(r) for r in conn.execute("SELECT key, unit_id, literal FROM unit_forms")}
        vocabulary = {r[0] for r in conn.execute(
            "SELECT key FROM unit_keys UNION SELECT key FROM unit_forms UNION SELECT form_key FROM index_forms")}
    else:
        from scripts.build_blau_sidecar import lookup_alias_rows
        expected = set(lookup_alias_rows(conn))
        stored = {tuple(r) for r in conn.execute("SELECT * FROM lookup_aliases")}
        vocabulary = {r[0] for r in conn.execute("SELECT norm FROM forms_norm UNION SELECT key FROM lookup_aliases")}
    if stored != expected:
        raise ValueError("lexical aliases differ from the source forms and sense relationships")
    expected_spelling = {(spelling_key(k), k) for k in vocabulary if k}
    stored_spelling = {tuple(r) for r in conn.execute("SELECT folded, literal FROM spelling_keys")}
    if stored_spelling != expected_spelling:
        raise ValueError("spelling keys differ from the indexed lexical vocabulary")
