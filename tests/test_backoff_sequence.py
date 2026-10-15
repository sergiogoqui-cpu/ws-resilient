import pytest

from ws_resilient import Backoff, SequenceTracker, SeqStatus


def test_backoff_none_is_exponential_and_capped():
    b = Backoff(base=1, maximum=10, jitter="none")
    assert [b.delay(n) for n in range(6)] == [1, 2, 4, 8, 10, 10]


def test_backoff_jitter_bounds():
    full = Backoff(base=2, maximum=2, jitter="full", rng=lambda lo, hi: hi)
    assert full.delay(0) == 2
    equal = Backoff(base=2, maximum=2, jitter="equal", rng=lambda lo, hi: lo)
    assert equal.delay(0) == 1  # never collapses to ~0


def test_backoff_survives_huge_attempt_numbers():
    assert Backoff(jitter="none").delay(10_000) == 60


def test_backoff_validates_arguments():
    with pytest.raises(ValueError):
        Backoff(base=0)
    with pytest.raises(ValueError):
        Backoff(base=5, maximum=1)
    with pytest.raises(ValueError):
        Backoff().delay(-1)


def test_sequence_ok_duplicate_and_gap():
    t = SequenceTracker()
    assert t.observe(10).status is SeqStatus.FIRST
    assert t.observe(11).status is SeqStatus.OK
    assert t.observe(11).status is SeqStatus.DUPLICATE
    r = t.observe(15)
    assert r.status is SeqStatus.GAP and r.lost == 3
    assert (t.gaps, t.lost, t.duplicates) == (1, 3, 1)


def test_sequence_reset_forgets_history():
    t = SequenceTracker()
    t.observe(5)
    t.reset()
    assert t.observe(500).status is SeqStatus.FIRST


def test_sequence_step():
    t = SequenceTracker(step=2)
    t.observe(2)
    assert t.observe(4).status is SeqStatus.OK
    assert t.observe(10).lost == 2
