import asyncio
from contextlib import asynccontextmanager

import pytest

from ws_resilient import Backoff, MaxRetriesExceeded, ResilientWebSocket

HANG = object()  # recv() blocks forever (a silently dead connection)


class FakeConnection:
    def __init__(self, script):
        self._script = list(script)
        self.sent: list[str] = []

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        if not self._script:
            raise ConnectionError("closed")
        item = self._script.pop(0)
        if item is HANG:
            await asyncio.Event().wait()
        if isinstance(item, BaseException):
            raise item
        return item


class FakeServer:
    """Hands out one scripted connection per connect attempt."""

    def __init__(self, scripts, fail_connect=0):
        self.scripts = list(scripts)
        self.fail_connect = fail_connect
        self.connections: list[FakeConnection] = []

    def connect(self):
        @asynccontextmanager
        async def ctx():
            if self.fail_connect > 0:
                self.fail_connect -= 1
                raise OSError("refused")
            conn = FakeConnection(self.scripts.pop(0) if self.scripts else [])
            self.connections.append(conn)
            yield conn

        return ctx()


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=5))


async def collect(ws, n):
    out = []
    async for message in ws.messages():
        out.append(message)
        if len(out) == n:
            ws.stop()
    return out


def make(server, delays=None, **kwargs):
    async def sleep(delay):
        if delays is not None:
            delays.append(delay)

    kwargs.setdefault("backoff", Backoff(base=1, maximum=8, jitter="none"))
    return ResilientWebSocket(connect=server.connect, sleep=sleep, **kwargs)


def test_yields_messages_in_order():
    server = FakeServer([["a", "b", "c"]])
    ws = make(server)
    assert run(collect(ws, 3)) == ["a", "b", "c"]
    assert ws.stats.messages == 3 and ws.stats.connects == 1


def test_reconnects_after_error_and_resubscribes():
    server = FakeServer([["a", ConnectionError("boom")], ["b"]])
    ws = make(server, subscribe=['{"op":"sub"}'])
    assert run(collect(ws, 2)) == ["a", "b"]
    assert ws.stats.reconnects == 1 and ws.stats.errors == 1
    assert all(c.sent == ['{"op":"sub"}'] for c in server.connections)
    assert len(server.connections) == 2


def test_backoff_grows_while_failing_and_resets_after_data():
    delays: list[float] = []
    server = FakeServer([["a", ConnectionError()], [], [], ["b", ConnectionError()], ["c"]])
    ws = make(server, delays)
    assert run(collect(ws, 3)) == ["a", "b", "c"]
    # after "a": reset -> 1; two empty connections -> 1, 2; after "b": reset -> 1
    assert delays == [1, 1, 2, 1]


def test_connect_failures_are_retried_with_backoff():
    delays: list[float] = []
    server = FakeServer([["a"]], fail_connect=3)
    ws = make(server, delays)
    assert run(collect(ws, 1)) == ["a"]
    assert delays == [1, 2, 4]


def test_idle_timeout_forces_reconnect():
    server = FakeServer([["a", HANG], ["b"]])
    ws = make(server, idle_timeout=0.05)
    assert run(collect(ws, 2)) == ["a", "b"]
    assert ws.stats.idle_timeouts == 1


def test_max_retries_raises():
    server = FakeServer([], fail_connect=99)
    ws = make(server, max_retries=2)

    async def consume():
        async for _ in ws.messages():
            pass

    with pytest.raises(MaxRetriesExceeded):
        run(consume())


def test_stop_interrupts_a_blocked_recv():
    server = FakeServer([[HANG]])
    ws = make(server, idle_timeout=30)

    async def scenario():
        task = asyncio.create_task(collect(ws, 99))
        await asyncio.sleep(0.05)
        ws.stop()
        return await task

    assert run(scenario()) == []


def test_connect_and_disconnect_callbacks():
    events: list[str] = []
    server = FakeServer([["a", ConnectionError("x")], ["b"]])
    ws = make(
        server,
        on_connect=lambda: events.append("up"),
        on_disconnect=lambda exc: events.append("down" if exc else "closed"),
    )
    run(collect(ws, 2))
    assert events[:3] == ["up", "down", "up"]


def test_requires_url_or_connector():
    with pytest.raises(ValueError):
        ResilientWebSocket()
