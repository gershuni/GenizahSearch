"""The pause-acknowledgement accept rule — pure, no Qt.

A worker's "I have parked" message is queued, so it can arrive late: after the
user resumed, after a later pause cycle began, or even after the whole run was
stopped and another started. Accepting one of those repaints the UI as Paused
over a worker that is running.

Every case below is a real interleaving, not a hypothetical.
"""

from genizah_app import _PauseCtx


def _ctx_in_pausing(run_id=1, epoch=1):
    ctx = _PauseCtx()
    ctx.reset_for_run(run_id, mono_start=0.0)
    ctx.epoch = epoch
    ctx.state = 'pausing'
    return ctx


def test_matching_ack_is_accepted():
    assert _ctx_in_pausing(run_id=4, epoch=2).accepts_ack(4, 2) is True


def test_ack_from_an_earlier_pause_cycle_is_rejected():
    """pause -> resume -> pause, and cycle 1's ack lands during cycle 2.

    The UI is legitimately in 'pausing' at that moment, so a state check alone
    would accept it and show Paused for a worker that is running.
    """
    assert _ctx_in_pausing(run_id=1, epoch=2).accepts_ack(1, 1) is False


def test_ack_after_the_user_resumed_is_rejected():
    """The epoch still matches — only the state says the pause is over."""
    ctx = _ctx_in_pausing(run_id=1, epoch=1)
    ctx.state = 'running'
    assert ctx.accepts_ack(1, 1) is False


def test_ack_from_a_previous_run_is_rejected_even_when_the_epoch_matches():
    """The cross-run case the run token exists for.

    Epoch numbering restarts every run, so run A's queued epoch-1 ack carries the
    same number as run B's first pause. Only the run id separates them.
    """
    ctx = _ctx_in_pausing(run_id=9, epoch=1)
    assert ctx.accepts_ack(8, 1) is False        # stale run, identical epoch
    assert ctx.accepts_ack(9, 1) is True


def test_ack_is_rejected_while_idle_or_running():
    for state in ('idle', 'running', 'paused'):
        ctx = _ctx_in_pausing()
        ctx.state = state
        assert ctx.accepts_ack(1, 1) is False, state


def test_reset_for_run_rearms_the_context_completely():
    ctx = _ctx_in_pausing(run_id=1, epoch=3)
    ctx.paused_total = 12.0
    ctx.pause_started = 5.0
    ctx.local_phase_active = True

    ctx.reset_for_run(2, mono_start=99.0)

    assert (ctx.run_id, ctx.epoch, ctx.state) == (2, 0, 'running')
    assert ctx.mono_start == 99.0
    assert ctx.paused_total == 0.0
    assert ctx.pause_started == 0.0
    assert ctx.local_phase_active is False
    # A fresh run must not accept anything addressed to the old one.
    assert ctx.accepts_ack(1, 1) is False


def test_the_two_tab_contexts_are_independent():
    """A composition pause must not be able to answer a search pause."""
    search = _ctx_in_pausing(run_id=10, epoch=1)
    comp = _ctx_in_pausing(run_id=11, epoch=1)

    assert search.accepts_ack(11, 1) is False
    assert comp.accepts_ack(10, 1) is False
    assert search.accepts_ack(10, 1) is True
    assert comp.accepts_ack(11, 1) is True
