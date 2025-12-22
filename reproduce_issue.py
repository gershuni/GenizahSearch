
import itertools

# Mock logic extracted from genizah_core.py
def build_items_mock(hits_dict, tokens):
    final_items = []
    for uid, data in hits_dict.items():
        src_indices = sorted(list(data['src_indices']))
        src_snippets = []
        # (Source context logic skipped for brevity)

        spans = sorted(data['matches'], key=lambda x: x[0])
        merged = []
        if spans:
            curr_s, curr_e = spans[0]
            for s, e in spans[1:]:
                if s <= curr_e + 20: curr_e = max(curr_e, e)
                else: merged.append((curr_s, curr_e)); curr_s, curr_e = s, e
            merged.append((curr_s, curr_e))

        score = sum(e-s for s,e in merged)
        ms_snips = []

        # ORIGINAL LOGIC: Iterate merged in order (which is sorted by start index)
        for s, e in merged:
            start = max(0, s - 60); end = min(len(data['content']), e + 60)
            ms_snips.append(data['content'][start:s] + "*" + data['content'][s:e] + "*" + data['content'][e:end])

        final_items.append({
            'score': score, 'uid': uid,
            'text': "\n...\n".join(ms_snips),
        })
    return final_items

def build_items_fixed(hits_dict, tokens):
    final_items = []
    for uid, data in hits_dict.items():
        src_indices = sorted(list(data['src_indices']))
        # (Source context logic skipped)

        spans = sorted(data['matches'], key=lambda x: x[0])
        merged = []
        if spans:
            curr_s, curr_e = spans[0]
            for s, e in spans[1:]:
                if s <= curr_e + 20: curr_e = max(curr_e, e)
                else: merged.append((curr_s, curr_e)); curr_s, curr_e = s, e
            merged.append((curr_s, curr_e))

        score = sum(e-s for s,e in merged)

        # NEW LOGIC: Collect snippets with scores, then sort
        raw_snips = []
        for s, e in merged:
            start = max(0, s - 60); end = min(len(data['content']), e + 60)
            snippet_text = data['content'][start:s] + "*" + data['content'][s:e] + "*" + data['content'][e:end]
            match_len = e - s
            raw_snips.append({'text': snippet_text, 'score': match_len, 'start': s})

        # Sort by score DESC, then by start ASC (for stability/logical order of equal matches)
        raw_snips.sort(key=lambda x: (-x['score'], x['start']))

        ms_snips = [x['text'] for x in raw_snips]

        final_items.append({
            'score': score, 'uid': uid,
            'text': "\n...\n".join(ms_snips),
        })
    return final_items

# Test Data
# Document has a small match at the beginning (length 5) and a HUGE match later (length 20)
content = "Intro Piyyut Match ... Lots of filler text ... Big Important Match Is Here and Long"
# Spans: (6, 11) -> "Piyyut", (47, 75) -> "Big Important Match Is Here and Long"
matches = [(6, 11), (47, 75)]

data = {
    'content': content,
    'matches': matches,
    'src_indices': set()
}
hits_dict = {'doc1': data}

print("--- Original Logic ---")
items_orig = build_items_mock(hits_dict, [])
print(items_orig[0]['text'])

print("\n--- Fixed Logic ---")
items_fixed = build_items_fixed(hits_dict, [])
print(items_fixed[0]['text'])
