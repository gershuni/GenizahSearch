## [Unreleased] -- Search with the scholarly transcriptions: the first results show while it runs, and the list comes sooner (web + desktop, branch `claude/search-all-early-rows`)

- **Early rows with the editions too.** A search of the automatic and the scholarly
  transcriptions together -- the website's default -- showed nothing until it was finished, in
  every text mode and in both apps. It now shows its first results while it runs, as a search
  of the automatic transcriptions alone does. Both lists decide what comes first, so only
  results no later row can move are shown (`shared/editions_preview.py`: each result's fused
  score between two bounds; the pages the editions found are looked up in the main index out
  of turn, their rows kept for their place): they are exactly the start of the finished list,
  compared at every step on the real indexes in 29 searches. On the website the editions'
  worker hands its rows to the main one (`shared/research_worker.py::write_editions_result`); the
  desktop searches the editions first. Not for a search of the scholarly transcriptions alone,
  and, as without the editions, not with a `-word` or an all-`*` query. Fuzzy waits for its
  editions part, which is slow for a common word (GS-311).
- **The list in less time.** Grouping the two lists passes over the rows that cannot be joined
  with another (`editions_search._fold_clusters`) and pauses the garbage collector
  (`shared/gc_pause.py`): 0.2-0.9 seconds instead of 1.3-4. A research worker saves results of
  up to 256 MB uncompressed and exits as soon as they are saved: 1.3 seconds instead of 7.8 for
  `#שלום`, and loading them is twice as fast. The desktop's results are handed to the window as
  they are: `pyqtSignal(list)` copied every row and member, half of it on the window's thread.
- Measured on the real indexes with the server's matching, before and after, interleaved
  (seconds to the first rows / to the whole list; no first rows before):

  | Search | Website before | Website after | Desktop before | Desktop after |
  |---|---|---|---|---|
  | Exact `שלום` | - / 9.6 | 2.6 / 5.3 | - / 9.6 | 1.2 / 5.4 |
  | Variants `שלום` | - / 11.9 | 2.7 / 8.4 | - / 11.7 | 1.2 / 7.6 |
  | Fuzzy `שלום` | - / 16.7 | 6.5 / 12.6 | - / 28.7 | 9.2 / 26.1 |
  | Responsa `#שלום` | - / 17.2 | 3.6 / 13.8 | - / 30.5 | 2.4 / 19.5 |

  The automatic transcriptions alone keep their first rows (0.8-1.2 on the website, 0.2-0.3 on
  the desktop) and gain about 2-5 seconds on the whole list on the website.
- Tests: `tests/test_editions_preview.py` (thousands of made-up searches at every step, the
  engine on a small pair of indexes, the workers' files), and in
  `tests/test_editions_parallel_jobs.py`, `tests/test_web_search_streaming.py`,
  `tests/test_search_preview.py`. Closes GS-312.
