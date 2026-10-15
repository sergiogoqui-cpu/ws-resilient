"""Exponential backoff with jitter."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable, Literal

Jitter = Literal["full", "equal", "none"]


@dataclass(frozen=True)
class Backoff:
    """Delay policy for reconnect attempts.

    ``delay(n)`` grows as ``base * factor**n`` and is capped at ``maximum``.
    Jitter spreads reconnects out so many clients don't hammer a server in lockstep:

    - ``"full"``: uniform in ``[0, cap]``
    - ``"equal"``: ``cap/2`` plus uniform in ``[0, cap/2]`` (default, never ~0)
    - ``"none"``: deterministic
    """

    base: float = 1.0
    maximum: float = 60.0
    factor: float = 2.0
    jitter: Jitter = "equal"
    rng: Callable[[float, float], float] = field(default=random.uniform, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.base <= 0 or self.maximum < self.base or self.factor < 1:
            raise ValueError("require base > 0, maximum >= base and factor >= 1")

    def delay(self, attempt: int) -> float:
        """Seconds to wait before reconnect number ``attempt`` (0-based)."""
        if attempt < 0:
            raise ValueError("attempt must be >= 0")
        cap = min(self.maximum, self.base * self.factor ** min(attempt, 64))
        if self.jitter == "none":
            return cap
        if self.jitter == "full":
            return self.rng(0.0, cap)
        return cap / 2 + self.rng(0.0, cap / 2)
