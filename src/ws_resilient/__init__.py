"""ws-resilient: a WebSocket client that reconnects with backoff and detects lost data."""

from .backoff import Backoff
from .client import Connection, MaxRetriesExceeded, ResilientWebSocket, Stats
from .sequence import SeqResult, SeqStatus, SequenceTracker

__all__ = [
    "Backoff",
    "Connection",
    "MaxRetriesExceeded",
    "ResilientWebSocket",
    "SeqResult",
    "SeqStatus",
    "SequenceTracker",
    "Stats",
]
__version__ = "0.1.0"
