"""SEED-022 desktop — PGPBadgeWorker emits BOTH badge sets.

The worker now feeds two results-table columns:
  * the unchanged green "PGP" badge  (link presence -> get_sys_ids_with_transcriptions)
  * the new amber scholarly-transcription column (PGP text ∪ FGP ->
    get_sys_ids_with_manual_transcriptions).

These are distinct predicates. GUI-marked (PyQt signals); runs in the gui-tests job.
"""

from PyQt6.QtWidgets import QApplication

_app = QApplication.instance() or QApplication([])


def test_pgp_badge_worker_emits_link_and_manual(monkeypatch):
    import shared.document_service as ds
    import shared.transcription_service as ts
    # Link presence (PGP badge) is a SUPERSET of readable text here.
    monkeypatch.setattr(ds, 'get_sys_ids_with_transcriptions', lambda ids: {'a', 'b', 'c'})
    monkeypatch.setattr(ts, 'get_sys_ids_with_manual_transcriptions', lambda ids: {'b'})

    from gui_threads import PGPBadgeWorker
    captured = {}
    w = PGPBadgeWorker(['a', 'b', 'c'])
    w.finished.connect(lambda pgp, manual: captured.update(pgp=pgp, manual=manual))
    w.run()  # synchronous (not .start()) -> direct-connected slot fires inline

    assert captured.get('pgp') == {'a', 'b', 'c'}   # -> green "PGP" column
    assert captured.get('manual') == {'b'}          # -> amber scholarly column


def test_pgp_badge_worker_error_emits_two_empty_sets(monkeypatch):
    import shared.document_service as ds

    def boom(ids):
        raise RuntimeError('db down')

    monkeypatch.setattr(ds, 'get_sys_ids_with_transcriptions', boom)

    from gui_threads import PGPBadgeWorker
    captured = {}
    w = PGPBadgeWorker(['a'])
    w.finished.connect(lambda pgp, manual: captured.update(pgp=pgp, manual=manual))
    w.run()

    # Must still emit the (set, set) shape so the slot signature never mismatches.
    assert captured.get('pgp') == set()
    assert captured.get('manual') == set()
