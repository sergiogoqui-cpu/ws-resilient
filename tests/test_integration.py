"""End-to-end against a real local WebSocket server that keeps dropping clients."""

import asyncio

import websockets

from ws_resilient import Backoff, ResilientWebSocket, SequenceTracker


def test_survives_server_dropping_connections():
    counter = {"n": 0}

    async def handler(ws):
        await ws.recv()  # subscription message
        for _ in range(3):
            counter["n"] += 1
            await ws.send(str(counter["n"]))
        await ws.close()  # server hangs up after 3 messages

    async def scenario():
        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            ws = ResilientWebSocket(
                f"ws://127.0.0.1:{port}",
                subscribe=["hello"],
                backoff=Backoff(base=0.01, maximum=0.05, jitter="none"),
            )
            tracker = SequenceTracker()
            got = []
            async for message in ws.messages():
                tracker.observe(int(message))
                got.append(int(message))
                if len(got) == 9:
                    ws.stop()
            return got, tracker, ws

    got, tracker, ws = asyncio.run(asyncio.wait_for(scenario(), timeout=10))
    assert got == list(range(1, 10))
    assert tracker.gaps == 0
    assert ws.stats.connects == 3 and ws.stats.reconnects == 2
