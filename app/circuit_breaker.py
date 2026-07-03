from __future__ import annotations

import time
from dataclasses import dataclass, field

from app.logger import setup_logger

logger = setup_logger("llm-failover")


@dataclass
class ProviderState:
    """Runtime state for a single provider."""
    name: str
    failure_count: int = 0
    degraded: bool = False
    last_failure_time: float = 0.0
    last_recovery_attempt: float = 0.0
    total_switches_away: int = 0  # how many times we switched away from this provider


class CircuitBreaker:
    """Per-provider failure tracking and automatic degradation/recovery.

    A provider is ``degraded`` after ``failure_threshold`` consecutive failures.
    Degraded providers are skipped during provider selection.
    Every ``recovery_interval`` seconds, a degraded provider is probed.
    A single success restores it to normal.
    """

    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_interval: float = 60.0,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._recovery_interval = recovery_interval
        self._states: dict[str, ProviderState] = {}

    # ── public API ────────────────────────────────────────────────────

    def register(self, name: str) -> None:
        """Ensure a provider is tracked."""
        if name not in self._states:
            self._states[name] = ProviderState(name=name)

    def record_success(self, name: str) -> None:
        state = self._states.get(name)
        if state is None:
            return
        if state.failure_count > 0 or state.degraded:
            logger.info(
                "[CB] %s 恢复 — failure_count=%d was_degraded=%s",
                name, state.failure_count, state.degraded,
            )
        state.failure_count = 0
        state.degraded = False

    def record_failure(self, name: str) -> bool:
        """Record a failure. Returns True if provider just became degraded."""
        state = self._states.get(name)
        if state is None:
            return False
        state.failure_count += 1
        state.last_failure_time = time.monotonic()
        if state.failure_count >= self._failure_threshold and not state.degraded:
            state.degraded = True
            logger.warning(
                "[CB] %s 降级 — 连续 %d 次失败",
                name, state.failure_count,
            )
            return True
        return False

    def is_degraded(self, name: str) -> bool:
        """Check if provider is currently degraded."""
        state = self._states.get(name)
        if state is None:
            return False
        return state.degraded

    def mark_switched_away(self, name: str) -> None:
        state = self._states.get(name)
        if state is not None:
            state.total_switches_away += 1

    def get_degraded_list(self) -> list[ProviderState]:
        """Return all currently degraded providers (for recovery probing)."""
        return [s for s in self._states.values() if s.degraded]

    def needs_recovery_probe(self, name: str, now: float | None = None) -> bool:
        """Check if it's time to probe a degraded provider."""
        state = self._states.get(name)
        if state is None or not state.degraded:
            return False
        now = now or time.monotonic()
        return (now - state.last_recovery_attempt) >= self._recovery_interval

    def mark_recovery_attempted(self, name: str) -> None:
        state = self._states.get(name)
        if state is not None:
            state.last_recovery_attempt = time.monotonic()

    def summary(self) -> str:
        """Return a one-line summary for logging."""
        parts = []
        for state in sorted(self._states.values(), key=lambda s: s.name):
            status = "DOWN" if state.degraded else "UP"
            parts.append(f"{state.name}={status}(fail={state.failure_count},switch={state.total_switches_away})")
        return " | ".join(parts)
