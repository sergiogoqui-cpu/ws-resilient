"""Detect lost or duplicated messages in a stream with consecutive sequence numbers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class SeqStatus(str, Enum):
    FIRST = "first"  # nothing to compare against yet
    OK = "ok"  # exactly the next number
    DUPLICATE = "duplicate"  # already seen (or older)
    GAP = "gap"  # numbers were skipped: messages were lost


@dataclass(frozen=True)
class SeqResult:
    status: SeqStatus
    lost: int = 0  # how many messages were skipped (only for GAP)


class SequenceTracker:
    """Track consecutive sequence numbers (trade ids, update ids, ...).

    Call :meth:`reset` after every reconnect: numbers seen on the old connection say
    nothing about the new one, and any data between them should be treated as lost.
    """

    def __init__(self, step: int = 1) -> None:
        if step < 1:
            raise ValueError("step must be >= 1")
        self._step = step
        self._last: int | None = None
        self.gaps = 0
        self.lost = 0
        self.duplicates = 0

    @property
    def last(self) -> int | None:
        return self._last

    def observe(self, seq: int) -> SeqResult:
        last = self._last
        if last is None:
            self._last = seq
            return SeqResult(SeqStatus.FIRST)
        if seq <= last:
            self.duplicates += 1
            return SeqResult(SeqStatus.DUPLICATE)
        self._last = seq
        expected = last + self._step
        if seq == expected:
            return SeqResult(SeqStatus.OK)
        lost = (seq - expected) // self._step
        self.gaps += 1
        self.lost += lost
        return SeqResult(SeqStatus.GAP, lost=lost)

    def reset(self) -> None:
        self._last = None
