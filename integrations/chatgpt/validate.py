"""Validate the Actions contract and optionally replay captured API payloads.

Requires jsonschema, openapi-spec-validator, and FastAPI (for compaction).
No network calls and no corpus imports.
"""
import argparse
import json
from pathlib import Path
import sys

from jsonschema import Draft202012Validator
from openapi_spec_validator import validate

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from web.chatgpt_api import MAX_RESPONSE_BYTES, compact_payload  # noqa: E402
from web.chatgpt_pagination import build_pages  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--captures', type=Path)
    args = parser.parse_args()
    root = Path(__file__).parent
    spec = json.loads((root / 'openapi.json').read_text(encoding='utf-8'))
    validate(spec)
    assert len((root / 'instructions.md').read_text(encoding='utf-8')) <= 8000
    ids = []
    for path in spec['paths'].values():
        for op in path.values():
            ids.append(op['operationId'])
            assert op['x-openai-isConsequential'] is False
            assert len(op['description']) <= 300 and len(op['summary']) <= 300
            for parameter in op.get('parameters', []):
                assert len(parameter.get('description', '')) <= 700
    assert len(ids) == len(set(ids))

    def validator(name):
        return Draft202012Validator({'$ref': '#/components/schemas/' + name,
                                     'components': spec['components']})

    requests = [
        ('SearchRequest', {'query': 'שלום', 'search_mode': 'exact', 'limit': 5}),
        ('SearchRequest', {'query': 'שלום', 'search_mode': 'fuzzy', 'limit': 5}),
        ('PassageRequest', {'method': 'passage', 'text': 'שלום עולם'}),
        ('PassageRequest', {'method': 'passage', 'witnesses': [{'text': 'א'}, {'text': 'ב'}], 'sort': 'witness_count'}),
    ]
    for name, request in requests:
        validator(name).validate(request)
    invalid = [
        {'method': 'chunk', 'text': 'abc'}, {'method': 'passage'},
        {'method': 'passage', 'text': 'abc', 'sort': 'fused'},
        {'method': 'passage', 'text': 'abc', 'witnesses': [{'text': 'def'}]},
        {'method': 'passage', 'witnesses': [{'text': 'abc', 'raw_header': 'def'}]},
    ]
    for request in invalid:
        assert not validator('PassageRequest').is_valid(request)
    print('OpenAPI valid; request combinations, action limits, and instructions checked.')
    if args.captures:
        for path in sorted(args.captures.glob('*.json')):
            if path.name == 'report.json':
                continue
            body = json.loads(path.read_text(encoding='utf-8'))
            name = ('Error' if 'error' in body else 'Capabilities' if 'features' in body
                    else 'Browse' if 'text_source' in body else 'Results')
            validator(name).validate(body)
            compact = compact_payload(body)
            assert compact is not None, path.name
            validator(name).validate(compact)
            size = len(JSONResponse(compact).body)
            assert size <= MAX_RESPONSE_BYTES
            print(f'{path.name}: original + compact contract valid, {size} compact bytes.')
            if name == 'Results':
                pages, stored = build_pages(body, 'offline-validation')
                for collection, raw_pages in pages.items():
                    count = 0
                    for raw in raw_pages:
                        page = json.loads(raw)
                        validator(name).validate(page)
                        count += len(page[collection])
                    assert count == len(body.get(collection, []))
                print(f'  All saved candidates preserved across {sum(map(len, pages.values()))} pages; {stored} stored bytes.')


if __name__ == '__main__':
    main()
