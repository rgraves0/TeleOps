from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import resource
import time
import tracemalloc
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional

try:
    import psutil
except ImportError:
    psutil = None

from app.core.base_agent import BaseAgent
from app.core.message_bus import MessageBus


logger = logging.getLogger(__name__)


class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    DEAD = "dead"


@dataclass(slots=True)
class AgentHeartbeat:
    agent_id: str
    last_seen: float
    heartbeat_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AgentHealthSnapshot:
    agent_id: str
    status: HealthStatus
    cpu_percent: float
    memory_mb: float
    last_seen_delta: float
    event_loop_lag_ms: float
    created_at: float
    metadata: Dict[str, Any] = field(default_factory=dict)


class MonitoringAgent(BaseAgent):
    """
    Production-safe monitoring agent optimized for low-resource VPS environments.

    Features:
    - Async-first heartbeat tracking
    - Event loop lag detection
    - RAM/CPU threshold monitoring
    - Deadlock suspicion detection
    - Memory spike detection
    - Centralized health verification
    - EventBus-driven alerting
    """

    AGENT_HEARTBEAT_TIMEOUT = 45
    AGENT_WARNING_TIMEOUT = 20

    CPU_WARNING_THRESHOLD = 75.0
    CPU_CRITICAL_THRESHOLD = 92.0

    MEMORY_WARNING_MB = 700
    MEMORY_CRITICAL_MB = 900

    LOOP_LAG_WARNING_MS = 400
    LOOP_LAG_CRITICAL_MS = 1200

    HEALTH_CHECK_INTERVAL = 10
    METRIC_RETENTION = 120

    def __init__(
        self,
        message_bus: MessageBus,
        *,
        agent_id: str = "monitoring-agent",
        heartbeat_timeout: int = AGENT_HEARTBEAT_TIMEOUT,
    ) -> None:
        super().__init__(agent_id=agent_id)

        self.message_bus = message_bus
        self.heartbeat_timeout = heartbeat_timeout

        self._running = False
        self._tasks: List[asyncio.Task] = []

        self._heartbeats: Dict[str, AgentHeartbeat] = {}
        self._health_snapshots: Dict[str, AgentHealthSnapshot] = {}

        self._cpu_history: Deque[float] = deque(maxlen=self.METRIC_RETENTION)
        self._memory_history: Deque[float] = deque(maxlen=self.METRIC_RETENTION)
        self._loop_lag_history: Deque[float] = deque(maxlen=self.METRIC_RETENTION)

        self._metric_hooks: Dict[str, Callable[[Dict[str, Any]], Any]] = {}

        self._process = psutil.Process(os.getpid()) if psutil else None
        self._last_loop_tick = time.monotonic()

        tracemalloc.start()

    async def start(self) -> None:
        logger.info("Starting MonitoringAgent")

        self._running = True

        await self._register_message_handlers()

        self._tasks.extend(
            [
                asyncio.create_task(self._heartbeat_watchdog_loop()),
                asyncio.create_task(self._system_metrics_loop()),
                asyncio.create_task(self._event_loop_monitor_loop()),
                asyncio.create_task(self._health_report_loop()),
            ]
        )

        logger.info("MonitoringAgent started")

    async def stop(self) -> None:
        logger.info("Stopping MonitoringAgent")

        self._running = False

        for task in self._tasks:
            task.cancel()

        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._tasks.clear()

        logger.info("MonitoringAgent stopped")

    async def _register_message_handlers(self) -> None:
        await self.message_bus.subscribe(
            "agent.heartbeat",
            self._handle_heartbeat_event,
        )

        await self.message_bus.subscribe(
            "metrics.report",
            self._handle_metrics_event,
        )

        await self.message_bus.subscribe(
            "agent.lifecycle",
            self._handle_lifecycle_event,
        )

    async def _handle_heartbeat_event(self, payload: Dict[str, Any]) -> None:
        agent_id = payload.get("agent_id")

        if not agent_id:
            return

        current = self._heartbeats.get(agent_id)

        if current:
            current.last_seen = time.time()
            current.heartbeat_count += 1
            current.metadata = payload
        else:
            self._heartbeats[agent_id] = AgentHeartbeat(
                agent_id=agent_id,
                last_seen=time.time(),
                heartbeat_count=1,
                metadata=payload,
            )

    async def _handle_metrics_event(self, payload: Dict[str, Any]) -> None:
        metric_type = payload.get("metric_type")

        if metric_type and metric_type in self._metric_hooks:
            try:
                await self._safe_call_hook(metric_type, payload)
            except Exception:
                logger.exception(
                    "Metric hook failure for type=%s",
                    metric_type,
                )

    async def _handle_lifecycle_event(self, payload: Dict[str, Any]) -> None:
        agent_id = payload.get("agent_id")
        state = payload.get("state")

        if not agent_id:
            return

        logger.info(
            "Lifecycle event received | agent=%s state=%s",
            agent_id,
            state,
        )

    async def _safe_call_hook(
        self,
        metric_type: str,
        payload: Dict[str, Any],
    ) -> None:
        hook = self._metric_hooks.get(metric_type)

        if hook is None:
            return

        result = hook(payload)

        if asyncio.iscoroutine(result):
            await result

    async def _heartbeat_watchdog_loop(self) -> None:
        while self._running:
            try:
                await self._verify_heartbeats()
            except Exception:
                logger.exception("Heartbeat watchdog loop failure")

            await asyncio.sleep(5)

    async def _verify_heartbeats(self) -> None:
        now = time.time()

        for agent_id, heartbeat in list(self._heartbeats.items()):
            delta = now - heartbeat.last_seen

            if delta >= self.heartbeat_timeout:
                await self._emit_agent_alert(
                    agent_id=agent_id,
                    severity=HealthStatus.DEAD,
                    reason="heartbeat_timeout",
                    metadata={
                        "last_seen_seconds": round(delta, 2),
                    },
                )

            elif delta >= self.AGENT_WARNING_TIMEOUT:
                await self._emit_agent_alert(
                    agent_id=agent_id,
                    severity=HealthStatus.WARNING,
                    reason="heartbeat_delay",
                    metadata={
                        "last_seen_seconds": round(delta, 2),
                    },
                )

    async def _system_metrics_loop(self) -> None:
        while self._running:
            try:
                await self._collect_system_metrics()
            except Exception:
                logger.exception("System metrics loop failure")

            await asyncio.sleep(self.HEALTH_CHECK_INTERVAL)

    async def _collect_system_metrics(self) -> None:
        cpu_percent = self._get_cpu_percent()
        memory_mb = self._get_memory_usage_mb()

        self._cpu_history.append(cpu_percent)
        self._memory_history.append(memory_mb)

        if cpu_percent >= self.CPU_CRITICAL_THRESHOLD:
            await self._emit_system_alert(
                severity=HealthStatus.CRITICAL,
                reason="cpu_critical",
                metrics={"cpu_percent": cpu_percent},
            )

        elif cpu_percent >= self.CPU_WARNING_THRESHOLD:
            await self._emit_system_alert(
                severity=HealthStatus.WARNING,
                reason="cpu_warning",
                metrics={"cpu_percent": cpu_percent},
            )

        if memory_mb >= self.MEMORY_CRITICAL_MB:
            await self._emit_system_alert(
                severity=HealthStatus.CRITICAL,
                reason="memory_critical",
                metrics={"memory_mb": memory_mb},
            )

        elif memory_mb >= self.MEMORY_WARNING_MB:
            await self._emit_system_alert(
                severity=HealthStatus.WARNING,
                reason="memory_warning",
                metrics={"memory_mb": memory_mb},
            )

        await self._detect_memory_spike(memory_mb)

    async def _detect_memory_spike(self, current_memory: float) -> None:
        if len(self._memory_history) < 10:
            return

        avg = sum(self._memory_history) / len(self._memory_history)

        if current_memory >= avg * 1.8:
            snapshot = tracemalloc.take_snapshot()

            top_stats = snapshot.statistics("lineno")[:5]

            allocations = [
                {
                    "trace": str(stat.traceback),
                    "size_kb": round(stat.size / 1024, 2),
                }
                for stat in top_stats
            ]

            await self._emit_system_alert(
                severity=HealthStatus.CRITICAL,
                reason="memory_spike_detected",
                metrics={
                    "current_memory_mb": current_memory,
                    "average_memory_mb": round(avg, 2),
                    "allocations": allocations,
                },
            )

    async def _event_loop_monitor_loop(self) -> None:
        while self._running:
            start = time.monotonic()

            await asyncio.sleep(1)

            elapsed = (time.monotonic() - start) * 1000
            lag_ms = max(0.0, elapsed - 1000)

            self._loop_lag_history.append(lag_ms)

            if lag_ms >= self.LOOP_LAG_CRITICAL_MS:
                await self._emit_system_alert(
                    severity=HealthStatus.CRITICAL,
                    reason="event_loop_stall",
                    metrics={"loop_lag_ms": lag_ms},
                )

            elif lag_ms >= self.LOOP_LAG_WARNING_MS:
                await self._emit_system_alert(
                    severity=HealthStatus.WARNING,
                    reason="event_loop_lag",
                    metrics={"loop_lag_ms": lag_ms},
                )

    async def _health_report_loop(self) -> None:
        while self._running:
            try:
                await self._publish_health_snapshots()
            except Exception:
                logger.exception("Health reporting loop failure")

            await asyncio.sleep(15)

    async def _publish_health_snapshots(self) -> None:
        now = time.time()

        for agent_id, heartbeat in self._heartbeats.items():
            delta = now - heartbeat.last_seen

            status = self._resolve_agent_status(delta)

            snapshot = AgentHealthSnapshot(
                agent_id=agent_id,
                status=status,
                cpu_percent=self._get_cpu_percent(),
                memory_mb=self._get_memory_usage_mb(),
                last_seen_delta=round(delta, 2),
                event_loop_lag_ms=self._average_loop_lag(),
                created_at=now,
                metadata=heartbeat.metadata,
            )

            self._health_snapshots[agent_id] = snapshot

            await self.message_bus.publish(
                "monitor.health_snapshot",
                {
                    "agent_id": snapshot.agent_id,
                    "status": snapshot.status.value,
                    "cpu_percent": snapshot.cpu_percent,
                    "memory_mb": snapshot.memory_mb,
                    "last_seen_delta": snapshot.last_seen_delta,
                    "event_loop_lag_ms": snapshot.event_loop_lag_ms,
                    "created_at": snapshot.created_at,
                },
            )

    async def _emit_agent_alert(
        self,
        *,
        agent_id: str,
        severity: HealthStatus,
        reason: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload = {
            "type": "agent_alert",
            "agent_id": agent_id,
            "severity": severity.value,
            "reason": reason,
            "metadata": metadata or {},
            "timestamp": time.time(),
        }

        logger.warning("Agent alert emitted: %s", payload)

        await self.message_bus.publish(
            "monitor.alert",
            payload,
        )

    async def _emit_system_alert(
        self,
        *,
        severity: HealthStatus,
        reason: str,
        metrics: Dict[str, Any],
    ) -> None:
        payload = {
            "type": "system_alert",
            "severity": severity.value,
            "reason": reason,
            "metrics": metrics,
            "timestamp": time.time(),
        }

        logger.warning("System alert emitted: %s", payload)

        await self.message_bus.publish(
            "monitor.alert",
            payload,
        )

    def register_metric_hook(
        self,
        metric_type: str,
        callback: Callable[[Dict[str, Any]], Any],
    ) -> None:
        self._metric_hooks[metric_type] = callback

    def get_agent_snapshot(
        self,
        agent_id: str,
    ) -> Optional[AgentHealthSnapshot]:
        return self._health_snapshots.get(agent_id)

    def _resolve_agent_status(self, delta: float) -> HealthStatus:
        if delta >= self.heartbeat_timeout:
            return HealthStatus.DEAD

        if delta >= self.AGENT_WARNING_TIMEOUT:
            return HealthStatus.WARNING

        return HealthStatus.HEALTHY

    def _get_cpu_percent(self) -> float:
        if self._process:
            return float(self._process.cpu_percent(interval=None))

        try:
            load_avg = os.getloadavg()[0]
            cpu_count = os.cpu_count() or 1
            return round((load_avg / cpu_count) * 100, 2)
        except Exception:
            return 0.0

    def _get_memory_usage_mb(self) -> float:
        if self._process:
            return round(
                self._process.memory_info().rss / 1024 / 1024,
                2,
            )

        usage = resource.getrusage(resource.RUSAGE_SELF)
        return round(usage.ru_maxrss / 1024, 2)

    def _average_loop_lag(self) -> float:
        if not self._loop_lag_history:
            return 0.0

        return round(
            sum(self._loop_lag_history) / len(self._loop_lag_history),
            2,
        )
