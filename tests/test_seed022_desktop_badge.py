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


def _boom(ids):
    raise RuntimeError('db down')


def _run_worker(monkeypatch, *, pgp, manual):
    """Run PGPBadgeWorker with monkeypatched source helpers (each may be a set or
    the _boom raiser) and return the captured (pgp, manual) emission."""
    import shared.document_service as ds
    import shared.transcription_service as ts
    monkeypatch.setattr(ds, 'get_sys_ids_with_transcriptions', pgp if callable(pgp) else (lambda ids: pgp))
    monkeypatch.setattr(ts, 'get_sys_ids_with_manual_transcriptions', manual if callable(manual) else (lambda ids: manual))
    from gui_threads import PGPBadgeWorker
    captured = {}
    w = PGPBadgeWorker(['a', 'b'])
    w.finished.connect(lambda p, m: captured.update(pgp=p, manual=m))
    w.run()
    return captured


def test_manual_failure_does_not_clear_pgp(monkeypatch):
    """If the new manual-union query fails, the green PGP badges must survive."""
    captured = _run_worker(monkeypatch, pgp={'a', 'b'}, manual=_boom)
    assert captured.get('pgp') == {'a', 'b'}
    assert captured.get('manual') == set()


def test_pgp_failure_does_not_clear_manual(monkeypatch):
    """Symmetric: a PGP-link failure must not wipe the manual column."""
    captured = _run_worker(monkeypatch, pgp=_boom, manual={'b'})
    assert captured.get('pgp') == set()
    assert captured.get('manual') == {'b'}


def test_both_failures_emit_two_empty_sets(monkeypatch):
    """Both fail -> still emit the (set, set) shape so the slot never mismatches."""
    captured = _run_worker(monkeypatch, pgp=_boom, manual=_boom)
    assert captured.get('pgp') == set()
    assert captured.get('manual') == set()
