"""Run a local flaky server and watch ws-resilient reconnect and report gaps.

    python examples/sequence_gaps.py
"""

import asyncio

import websockets

from ws_resilient import Backoff, ResilientWebSocket, SequenceTracker, SeqStatus


async def flaky_server(port: int) -> None:
    n = 0

    async def handler(ws):
        nonlocal n
        for _ in range(4):
            n += 1
            await ws.send(str(n))
        n += 2  # pretend two messages were lost while the client was away
        await ws.close()

    async with websockets.serve(handler, "127.0.0.1", port):
        await asyncio.sleep(3600)


async def main() -> None:
    port = 8765
    server = asyncio.create_task(flaky_server(port))
    await asyncio.sleep(0.2)

    tracker = SequenceTracker()
    ws = ResilientWebSocket(
        f"ws://127.0.0.1:{port}",
        backoff=Backoff(base=0.2, maximum=1, jitter="equal"),
        on_connect=lambda: print("connected"),
        on_disconnect=lambda err: print("disconnected"),
    )
    seen = 0
    async for message in ws.messages():
        result = tracker.observe(int(message))
        note = f"  <- GAP, lost {result.lost}" if result.status is SeqStatus.GAP else ""
        print(f"message {message}{note}")
        seen += 1
        if seen == 12:
            ws.stop()
    print(ws.stats)
    server.cancel()


if __name__ == "__main__":
    asyncio.run(main())
