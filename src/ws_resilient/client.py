"""A WebSocket client that keeps itself connected."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, Protocol

from .backoff import Backoff

log = logging.getLogger("ws_resilient")

Message = str | bytes


class Connection(Protocol):
    """The minimal surface the client needs from a WebSocket connection."""

    async def send(self, message: Message) -> None: ...
    async def recv(self) -> Message: ...


Connector = Callable[[], AbstractAsyncContextManager[Connection]]


class MaxRetriesExceeded(RuntimeError):
    """Raised when ``max_retries`` consecutive connection attempts yielded no data."""


@dataclass
class Stats:
    connects: int = 0
    reconnects: int = 0
    messages: int = 0
    idle_timeouts: int = 0
    errors: int = 0
    connected: bool = False
    last_error: str | None = None


def _default_connector(url: str, **kwargs: Any) -> Connector:
    import websockets  # imported lazily so tests and custom connectors don't need it

    def connect() -> AbstractAsyncContextManager[Connection]:
        return websockets.connect(url, **kwargs)

    return connect


class ResilientWebSocket:
    """Yield messages from a WebSocket, reconnecting automatically.

    - Reconnects with exponential backoff and jitter after errors, closes or silence.
    - Resends ``subscribe`` messages on every (re)connection.
    - Detects silent dead connections with ``idle_timeout`` (no message for N seconds).
    - Resets the backoff as soon as a connection delivers data.
    - Stops cleanly with :meth:`stop`, even in the middle of a wait.

    Example::

        ws = ResilientWebSocket("wss://example.com/feed", subscribe=['{"op":"sub"}'])
        async for message in ws.messages():
            handle(message)
    """

    def __init__(
        self,
        url: str = "",
        *,
        subscribe: Sequence[Message] = (),
        connect: Connector | None = None,
        idle_timeout: float = 30.0,
        backoff: Backoff | None = None,
        max_retries: int | None = None,
        on_connect: Callable[[], None] | None = None,
        on_disconnect: Callable[[BaseException | None], None] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        connect_kwargs: dict[str, Any] | None = None,
    ) -> None:
        if connect is None and not url:
            raise ValueError("pass a url or a custom connect factory")
        if idle_timeout <= 0:
            raise ValueError("idle_timeout must be > 0")
        self._connect = connect or _default_connector(
            url, **({"open_timeout": 10, "max_size": 2**20} | (connect_kwargs or {}))
        )
        self._subscribe = tuple(subscribe)
        self._idle = idle_timeout
        self._backoff = backoff or Backoff()
        self._max_retries = max_retries
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect
        self._sleep = sleep
        self._stop = asyncio.Event()
        self.stats = Stats()

    def stop(self) -> None:
        """Ask the iterator to finish. Safe to call from any task."""
        self._stop.set()

    async def messages(self) -> AsyncIterator[Message]:
        attempt = 0  # consecutive connections that delivered nothing
        first = True
        while not self._stop.is_set():
            if not first:
                self.stats.reconnects += 1
            first = False
            received = False
            error: BaseException | None = None
            try:
                async with self._connect() as conn:
                    for sub in self._subscribe:
                        await conn.send(sub)
                    self.stats.connects += 1
                    self.stats.connected = True
                    if self._on_connect:
                        self._on_connect()
                    while not self._stop.is_set():
                        message = await self._recv(conn)
                        if message is None:  # stop requested while waiting
                            break
                        received = True
                        self.stats.messages += 1
                        yield message
            except asyncio.TimeoutError as exc:
                self.stats.idle_timeouts += 1
                error = exc
                log.warning("no data for %.1fs, reconnecting", self._idle)
            except Exception as exc:  # noqa: BLE001 - any transport failure means "reconnect"
                if type(exc).__name__ == "ConnectionClosedOK":  # server closed cleanly
                    log.info("connection closed by server, reconnecting")
                else:
                    self.stats.errors += 1
                    error = exc
                    log.warning("connection lost: %r", exc)
            finally:
                if self.stats.connected:
                    self.stats.connected = False
                    if self._on_disconnect:
                        self._on_disconnect(error)
            if self._stop.is_set():
                break
            self.stats.last_error = repr(error) if error else "closed by server"
            attempt = 0 if received else attempt + 1
            if self._max_retries is not None and attempt > self._max_retries:
                raise MaxRetriesExceeded(
                    f"{attempt} consecutive attempts without data"
                ) from error
            await self._wait(self._backoff.delay(max(attempt - 1, 0)))

    async def _recv(self, conn: Connection) -> Message | None:
        recv = asyncio.ensure_future(conn.recv())
        stop = asyncio.ensure_future(self._stop.wait())
        try:
            done, _ = await asyncio.wait(
                {recv, stop}, timeout=self._idle, return_when=asyncio.FIRST_COMPLETED
            )
            if recv in done:
                return recv.result()
            if stop in done:
                return None
            raise asyncio.TimeoutError
        finally:
            for task in (recv, stop):
                if not task.done():
                    task.cancel()

    async def _wait(self, delay: float) -> None:
        if self._sleep is not None:
            await self._sleep(delay)
            return
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
