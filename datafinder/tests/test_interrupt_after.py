"""A timeout interrupts until the query is stopped: an interrupt that comes before the database has started the query does nothing."""
import time

from datafinder.output import interrupt_after


def test_it_interrupts_again_until_the_block_ends():
    calls: list[float] = []
    with interrupt_after(10, lambda: calls.append(time.monotonic())):
        time.sleep(0.15)
    assert len(calls) >= 3  # once the 10ms are up, and every few milliseconds after


def test_it_does_not_interrupt_a_block_that_ends_in_time():
    calls: list[float] = []
    with interrupt_after(5000, lambda: calls.append(time.monotonic())):
        pass
    assert calls == []


def test_it_stops_interrupting_when_the_block_ends_or_raises():
    calls: list[float] = []
    try:
        with interrupt_after(1, lambda: calls.append(1)):
            time.sleep(0.05)
            raise RuntimeError("the query failed")
    except RuntimeError:
        pass
    seen = len(calls)
    time.sleep(0.05)
    assert len(calls) == seen
