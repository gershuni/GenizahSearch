"""Build the curated GPT Actions schema without importing the website or corpus."""
import json
from pathlib import Path


def obj(properties, required=(), **extra):
    return {'type': 'object', 'properties': properties, **({'required': list(required)} if required else {}), **extra}


def array(items, **extra):
    return {'type': 'array', 'items': items, **extra}


def ref(name):
    return {'$ref': '#/components/schemas/' + name}


def build():
    string = {'type': 'string'}
    boolean = {'type': 'boolean'}
    integer = {'type': 'integer'}
    number = {'type': 'number'}
    nullable_string = {'type': ['string', 'null']}
    free_object = obj({}, additionalProperties=True)
    schemas = {
        'Locator': obj({
            'sys_id': nullable_string, 'volume_ie': nullable_string,
            'p_num': {'type': ['integer', 'string', 'null'], 'description': 'One-based page number.'},
            'uid': nullable_string, 'fl_id': nullable_string,
        }, additionalProperties=True),
        'Library': obj({'code': string, 'name': string}),
        'Warning': {'anyOf': [string, obj({'code': string, 'message': string}, additionalProperties=True)]},
        'Error': obj({'error': obj({'code': string, 'message': string}, ('code', 'message'), additionalProperties=True)}, ('error',)),
        'Filters': obj({
            **{k: array(string) for k in ('domains', 'authors', 'works', 'materials')},
            'library': array(string, description='Known library codes, e.g. CUL, JTS, Oxford. Never guess catalog filter values.'),
            'library_filter_mode': {'type': 'string', 'enum': ['include', 'exclude']},
            'date_from': integer, 'date_to': integer,
        }, additionalProperties=False),
        'ResponsaOptions': obj({k: boolean for k in ('variants', 'ja', 'flex_spacing', 'bidirectional')}, additionalProperties=False),
        'SearchRequest': obj({
            'query': {'type': 'string', 'minLength': 1, 'maxLength': 1000},
            'search_mode': {'type': 'string', 'enum': ['exact', 'variants', 'fuzzy', 'responsa', 'title', 'shelfmark'],
                            'description': 'Start exact; shelfmark resolves call numbers. Search runs as a background job; poll its returned ID.'},
            'limit': {'type': 'integer', 'minimum': 1, 'maximum': 500,
                      'description': 'Total candidates to save from ONE search, not page size. Use 100 for exact/variants/responsa/title/shelfmark; 500 for fuzzy. Other modes allow at most 100. Retrieval pages contain at most 10 rows.'},
            'gap': {'type': 'integer', 'minimum': 0, 'default': 0, 'description': 'Words allowed between terms; must be zero for title/shelfmark.'},
            'responsa_options': {**ref('ResponsaOptions'), 'description': 'Omit unless search_mode is responsa.'},
            'filters': ref('Filters'),
        }, ('query', 'search_mode', 'limit'), additionalProperties=False),
        'Witness': obj({
            'label': {'type': 'string', 'maxLength': 200},
            'text': {'type': 'string', 'minLength': 1, 'maxLength': 5000},
            'raw_header': {**string, 'description': 'Exact header returned by a tool; never construct one from a UID or shelfmark.'},
        }, additionalProperties=False, oneOf=[{'required': ['text']}, {'required': ['raw_header']}]),
        'PassageRequest': obj({
            'method': {'type': 'string', 'enum': ['passage'], 'description': 'Required explicitly: the upstream default is chunk.'},
            'text': {'type': 'string', 'minLength': 1, 'maxLength': 5000,
                     'description': 'A meaningful passage. For longer input, label and search sections separately; never silently cut it.'},
            'witnesses': array(ref('Witness'), minItems=1, maxItems=3,
                               description='Pilot limit: three witnesses of ONE work. Omit text when supplied. Keep witnesses separate.'),
            'sort': {'type': 'string', 'enum': ['fused', 'best_match', 'witness_count'], 'description': 'Only with witnesses; omit for a single text.'},
            'filters': ref('Filters'),
        }, ('method',), additionalProperties=False,
            oneOf=[{'required': ['text'], 'not': {'anyOf': [{'required': ['witnesses']}, {'required': ['sort']}]}},
                   {'required': ['witnesses'], 'not': {'required': ['text']}}]),
        'Match': obj({
            'chunk_index': {'type': ['integer', 'null']}, 'source_chunk_text': string,
            'manuscript_snippet': string, 'score': number,
        }, additionalProperties=True),
        'Result': obj({
            'uid': string, 'raw_header': string, 'locator': ref('Locator'),
            'shelfmark': string, 'title': string, 'library': ref('Library'),
            'is_synthetic': boolean, 'score': number, 'snippet': string, 'excerpt': string,
            'image_url': nullable_string, 'domains': array(string), 'dating': nullable_string,
            'match_terms': array(string), 'matches': array(ref('Match')),
            'witness_fusion': obj({'witness_count': integer, 'witness_ids': array(string),
                                   'fusion_score': number, 'best_witness_score': number}, additionalProperties=True),
        }, ('locator', 'shelfmark', 'snippet'), additionalProperties=True),
        'Results': obj({
            'schema_version': integer, 'source': string, 'generated_at': string,
            'count': integer, 'total': integer, 'results': array(ref('Result')),
            'filtered': array(ref('Result')), 'warnings': array(ref('Warning')),
            'request': obj({}, description='Effective request parameters and policy after defaulting/capping. Retain for reproducibility.', additionalProperties=True),
            'pagination': obj({
                'job_id': string, 'collection': {'type': 'string', 'enum': ['results', 'filtered']},
                'page': integer, 'offset': integer, 'returned': integer, 'available': integer,
                'results_available': integer, 'filtered_available': integer,
                'next_page': {'type': ['integer', 'null']},
            }, additionalProperties=False),
        }, ('schema_version', 'source', 'generated_at', 'count', 'total', 'results', 'warnings', 'request'), additionalProperties=True),
        'Browse': obj({
            'schema_version': integer, 'source': string, 'generated_at': string,
            'locator': ref('Locator'), 'page_indexing': string, 'shelfmark': string,
            'title': string, 'library': ref('Library'), 'is_synthetic': boolean,
            'text': string, 'text_source': {'type': 'string', 'enum': ['pgp_transcription', 'snippet', 'none']},
            'text_truncated': boolean, 'metadata': free_object, 'image': free_object,
            'warnings': array(ref('Warning')),
        }, ('schema_version', 'locator', 'text', 'text_source', 'text_truncated', 'warnings'), additionalProperties=True),
        'Capabilities': obj({
            'schema_version': integer, 'api_version': string, 'endpoints': array(string),
            'search_modes': array(string), 'features': obj({'passage': boolean, 'passage_multi_witness': boolean}),
            'parallels': free_object, 'limits': free_object, 'timeouts': free_object,
        }, ('schema_version', 'features', 'search_modes'), additionalProperties=True),
        'PendingJob': obj({
            'job_id': string, 'state': {'type': 'string', 'enum': ['queued', 'running']},
            'status': string, 'progress': array(number), 'next_action': string,
            'poll_after_seconds': integer, 'retention_seconds': integer,
        }, ('job_id', 'state', 'next_action'), additionalProperties=True),
    }

    def operation(name, description, response, request=None):
        value = {'operationId': name, 'summary': description, 'description': description,
                 'x-openai-isConsequential': False,
                 'responses': {'200': {'description': 'Successful response.', 'content': {'application/json': {'schema': ref(response)}}},
                               'default': {'description': 'Request failed. Preserve HTTP status, error code, and Retry-After when available. Proxy errors may be non-JSON.',
                                           'headers': {'Retry-After': {'schema': string, 'description': 'Suggested delay in seconds or HTTP date.'}},
                                           'content': {'application/json': {'schema': ref('Error')}}}}}
        if request:
            value['requestBody'] = {'required': True, 'content': {'application/json': {'schema': ref(request)}}}
        return value

    browse = operation('browseManuscriptPage', 'Read a returned manuscript page locator before quoting or evaluating a match. Text may be only an automatic snippet.', 'Browse')
    browse['parameters'] = [
        {'name': 'sys_id', 'in': 'query', 'required': True, 'schema': string, 'description': 'Copy locator.sys_id from a result.'},
        {'name': 'p_num', 'in': 'query', 'required': True, 'schema': {'type': 'integer', 'minimum': 1}, 'description': 'Copy the one-based locator.p_num from a result.'},
        {'name': 'volume_ie', 'in': 'query', 'required': False, 'schema': string, 'description': 'Copy locator.volume_ie when present.'},
        {'name': 'text_cap', 'in': 'query', 'required': True, 'schema': {'type': 'integer', 'minimum': 100, 'maximum': 10000, 'default': 4000}, 'description': 'Start with 4000 characters. A snippet remains a snippet even if not truncated.'},
    ]
    search = operation('searchManuscripts', 'Submit a phrase, title, or shelfmark search. Returns a job ID, not matches. Call getResearchJob until finished; never resubmit a pending search.', 'PendingJob', 'SearchRequest')
    parallels = operation('findPassageParallels', 'Submit character-level passage or multi-witness matching with method=passage. Call getResearchJob with the returned private job ID until finished.', 'PendingJob', 'PassageRequest')
    for op in (search, parallels):
        op['responses']['202'] = op['responses'].pop('200')
        op['responses']['202']['description'] = 'Accepted and queued. Poll this same job ID.'
    poll = operation('getResearchJob', 'Poll or page through ONE saved search. Follow pagination.next_page with the same private job ID to see more matches without rerunning. Use collection=filtered for demoted candidates. Null next_page means this collection is finished.', 'Results')
    poll['parameters'] = [{'name': 'job_id', 'in': 'path', 'required': True, 'schema': string,
                           'description': 'Copy the returned private job_id exactly. Never guess IDs or submit the query again while pending.'},
                          {'name': 'page', 'in': 'query', 'required': False,
                           'schema': {'type': 'integer', 'minimum': 0, 'default': 0},
                           'description': 'Start at 0, then copy pagination.next_page. Page sizes may shrink to fit the response limit.'},
                          {'name': 'collection', 'in': 'query', 'required': False,
                           'schema': {'type': 'string', 'enum': ['results', 'filtered'], 'default': 'results'},
                           'description': 'Main matches or demoted/filtered matches. Each collection has its own page sequence.'}]
    poll['responses']['202'] = {'description': 'Still running; poll this same job again.',
                                'content': {'application/json': {'schema': ref('PendingJob')}}}
    return {
        'openapi': '3.1.0',
        'info': {'title': 'GenizahSearch Research Actions', 'version': '0.3.0',
                 'description': 'Read-only Cairo Genizah research pilot: discover features, search manuscripts, browse evidence, and find passage parallels.'},
        'servers': [{'url': 'https://genizahsearch.com/api/chatgpt'}], 'security': [],
        'paths': {
            '/capabilities': {'get': operation('getGenizahCapabilities', 'Check available search modes and passage features once before research. Runtime errors remain authoritative.', 'Capabilities')},
            '/search/jobs': {'post': search},
            '/browse': {'get': browse},
            '/parallels/jobs': {'post': parallels},
            '/jobs/{job_id}': {'get': poll},
        }, 'components': {'schemas': schemas},
    }


if __name__ == '__main__':
    path = Path(__file__).with_name('openapi.json')
    path.write_text(json.dumps(build(), ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(path)
