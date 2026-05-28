# tests/fixtures/local_indexer/ — extraction fixtures

- `single_word_per_line.pdf` — Phase 96 D-F4 synthetic one-word-per-line guard (LTR no-op for RTL fix).
- `clean_sample.pdf` — Phase 96 synthetic clean PDF (Codex MEDIUM #8 control).
- `hebrew_sample.pdf` / `hebrew_sample.expected.txt` — Phase 95 D-44 PyMuPDF Hebrew extraction quality fixtures.
- `hebrew_sample.html` / `hebrew_sample.csv` — Phase 97 LD-1 structured extractor fixtures.
- `corrupt_sample.pdf` — Phase 95 corrupted-PDF error-path fixture.
- `encrypted_sample.pdf` — Phase 95 encrypted-PDF error-path fixture.
- `multipage_sample.pdf` — Phase 95 multipage extraction fixture.
- `sample.docx` / `sample.txt` — Phase 95 REQ-1 supported-file-type fixtures.
- `utf8sig_sample.txt` / `cp1255_sample.txt` / `bad_encoding.txt` — Phase 95 MEDIUM-2 TXT encoding fixtures.
- `unsupported.html` — Phase 95 REQ-1 unsupported-extension fixture.
- `hebrew_rtl_fixture.pdf` — **Phase 101 D-06.** 1-2 page excerpt from the Phase 100 UAT
  Hebrew/Judeo-Arabic book that surfaced the RTL word-order bug. Provided by Hillel Gershuni for the
  RTL extraction regression test (`test_sort_true_rtl_real_hebrew_fixture`). Used for non-commercial
  research testing only; copyright remains with the original publisher/author. The real-fixture test
  skips silently when this file is absent so downstream forks/CI without the asset stay green.
