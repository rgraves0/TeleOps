from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Deque,
    Dict,
    List,
    Optional,
    Set,
)

from app.core.message_bus import (
    MessageBus,
)

from app.tools.dynamic_router import (
    DynamicToolRouter,
    RouteContext,
    RouteDecision,
)


logger = logging.getLogger(__name__)


class AlertSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class PlanStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"


class MitigationType(str, Enum):
    MEMORY_PRESSURE = (
        "memory_pressure"
    )

    CPU_PRESSURE = (
        "cpu_pressure"
    )

    CONSECUTIVE_FAILURES = (
        "consecutive_failures"
    )

    DEADLOCK = "deadlock"

    SERVICE_DEGRADATION = (
        "service_degradation"
    )

    UNKNOWN = "unknown"


@dataclass(slots=True)
class OperationalAlert:
    alert_id: str
    source: str
    severity: AlertSeverity
    event_type: str
    message: str
    metrics: Dict[str, Any]
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class MitigationStep:
    step_id: str
    action: str
    priority: int
    parameters: Dict[str, Any]
    requires_approval: bool = False
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class OperationalPlan:
    plan_id: str
    alert_id: str
    status: PlanStatus
    mitigation_type: MitigationType
    steps: List[MitigationStep]
    reasoning: str
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class SystemBoundaryValidator:
    """
    Default Deny + RBAC boundary validator.
    """

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate_action(
        self,
        *,
        action: str,
        permissions: Set[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:
        context = RouteContext(
            requester_id=(
                "autonomous_brain"
            ),
            requester_roles={
                "system"
            },
            requester_permissions=
                permissions,
            task_type=action,
            metadata=metadata or {},
        )

        route = await self.router.route(
            task=action,
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class StrategicReasoningEngine:
    """
    Lightweight heuristic reasoning engine.

    No external agent frameworks.
    """

    MEMORY_HIGH_THRESHOLD = 85
    CPU_HIGH_THRESHOLD = 90
    FAILURE_THRESHOLD = 3

    async def analyze_alert(
        self,
        alert: OperationalAlert,
    ) -> MitigationType:
        metrics = alert.metrics

        if (
            metrics.get(
                "memory_percent",
                0,
            )
            >= self.MEMORY_HIGH_THRESHOLD
        ):
            return (
                MitigationType.MEMORY_PRESSURE
            )

        if (
            metrics.get(
                "cpu_percent",
                0,
            )
            >= self.CPU_HIGH_THRESHOLD
        ):
            return (
                MitigationType.CPU_PRESSURE
            )

        if (
            metrics.get(
                "failure_count",
                0,
            )
            >= self.FAILURE_THRESHOLD
        ):
            return (
                MitigationType.CONSECUTIVE_FAILURES
            )

        if metrics.get(
            "deadlock_detected",
            False,
        ):
            return (
                MitigationType.DEADLOCK
            )

        if metrics.get(
            "service_degraded",
            False,
        ):
            return (
                MitigationType.SERVICE_DEGRADATION
            )

        return MitigationType.UNKNOWN

    async def generate_steps(
        self,
        mitigation: MitigationType,
        alert: OperationalAlert,
    ) -> List[MitigationStep]:
        if (
            mitigation
            == MitigationType.MEMORY_PRESSURE
        ):
            return [
                MitigationStep(
                    step_id=
                        self._step_id(),
                    action=
                        "memory.compact",
                    priority=1,
                    parameters={
                        "target":
                            alert.source,
                    },
                ),
                MitigationStep(
                    step_id=
                        self._step_id(),
                    action=
                        "cache.cleanup",
                    priority=2,
                    parameters={
                        "aggressive":
                            False,
                    },
                ),
            ]

        if (
            mitigation
            == MitigationType.CPU_PRESSURE
        ):
            return [
                MitigationStep(
                    step_id=
                        self._step_id(),
                    action=
                        "throttle.tasks",
                    priority=1,
                    parameters={
                        "target":
                            alert.source,
                    },
                ),
                MitigationStep(
                    step_id=
                        self._step_id(),
                    action=
                        "degradation.enable",
                    priority=2,
                    parameters={
                        "mode":
                            "low_cpu",
                    },
                ),
            ]

        if (
            mitigation
            == MitigationType.CONSECUTIVE_FAILURES
        ):
            return [
                MitigationStep(
                    step_id=
                        self._step_id(),
                    action=
                        "service.restart",
                    priority=1,
                    parameters={
                        "service":
                            alert.source,
                    },
                ),
                MitigationStep(
                    step_id=
                        self._step_id(),
                    action=
                        "fallback.activate",
                    priority=2,
                    parameters={
                        "service":
                            alert.source,
                    },
                ),
            ]

        if (
            mitigation
            == MitigationType.DEADLOCK
        ):
            return [
                MitigationStep(
                    step_id=
                        self._step_id(),
                    action=
                        "workflow.reset",
                    priority=1,
                    parameters={
                        "target":
                            alert.source,
                    },
                    requires_approval=True,
                )
            ]

        if (
            mitigation
            == MitigationType.SERVICE_DEGRADATION
        ):
            return [
                MitigationStep(
                    step_id=
                        self._step_id(),
                    action=
                        "degradation.enable",
                    priority=1,
                    parameters={
                        "mode":
                            "safe",
                    },
                )
            ]

        return [
            MitigationStep(
                step_id=self._step_id(),
                action="observe",
                priority=1,
                parameters={
                    "source":
                        alert.source,
                },
            )
        ]

    def build_reasoning(
        self,
        mitigation: MitigationType,
        alert: OperationalAlert,
    ) -> str:
        return (
            f"Detected {mitigation.value} "
            f"from source={alert.source} "
            f"severity={alert.severity.value}. "
            f"Generated autonomous mitigation strategy."
        )

    def _step_id(
        self,
    ) -> str:
        return uuid.uuid4().hex[:12]


class OperationalPlanningLedger:
    """
    SQLite WAL operational planning ledger.
    """

    SQLITE_BUSY_TIMEOUT = 5000

    def __init__(
        self,
        *,
        database_path: str,
    ) -> None:
        self.database_path = (
            Path(database_path)
        )

        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._connection: Optional[
            sqlite3.Connection
        ] = None

    async def initialize(self) -> None:
        self._connection = sqlite3.connect(
            str(self.database_path),
            check_same_thread=False,
            isolation_level=None,
        )

        await asyncio.to_thread(
            self._configure_database
        )

        await asyncio.to_thread(
            self._create_tables
        )

    async def close(self) -> None:
        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    async def persist_plan(
        self,
        plan: OperationalPlan,
    ) -> None:
        await asyncio.to_thread(
            self._insert_plan,
            plan,
        )

    async def update_status(
        self,
        *,
        plan_id: str,
        status: PlanStatus,
    ) -> None:
        await asyncio.to_thread(
            self._update_status,
            plan_id,
            status,
        )

    async def fetch_recent_plans(
        self,
        *,
        limit: int = 25,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self._fetch_recent,
            limit,
        )

    def _configure_database(
        self,
    ) -> None:
        self._connection.execute(
            "PRAGMA journal_mode=WAL;"
        )

        self._connection.execute(
            "PRAGMA synchronous=NORMAL;"
        )

        self._connection.execute(
            "PRAGMA temp_store=MEMORY;"
        )

        self._connection.execute(
            "PRAGMA cache_size=-2000;"
        )

        self._connection.execute(
            f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT};"
        )

    def _create_tables(
        self,
    ) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS operational_plans (
                plan_id TEXT PRIMARY KEY,
                alert_id TEXT NOT NULL,
                status TEXT NOT NULL,
                mitigation_type TEXT NOT NULL,
                reasoning TEXT NOT NULL,
                steps_json TEXT NOT NULL,
                metadata TEXT NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_operational_created
            ON operational_plans(created_at)
            """
        )

    def _insert_plan(
        self,
        plan: OperationalPlan,
    ) -> None:
        serialized_steps = []

        for step in plan.steps:
            serialized_steps.append(
                {
                    "step_id":
                        step.step_id,
                    "action":
                        step.action,
                    "priority":
                        step.priority,
                    "parameters":
                        step.parameters,
                    "requires_approval":
                        step.requires_approval,
                    "metadata":
                        step.metadata,
                }
            )

        self._connection.execute(
            """
            INSERT OR REPLACE INTO operational_plans (
                plan_id,
                alert_id,
                status,
                mitigation_type,
                reasoning,
                steps_json,
                metadata,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                plan.plan_id,
                plan.alert_id,
                plan.status.value,
                plan.mitigation_type.value,
                plan.reasoning,
                json.dumps(
                    serialized_steps,
                    ensure_ascii=False,
                ),
                json.dumps(
                    plan.metadata,
                    ensure_ascii=False,
                ),
                plan.created_at,
            ),
        )

    def _update_status(
        self,
        plan_id: str,
        status: PlanStatus,
    ) -> None:
        self._connection.execute(
            """
            UPDATE operational_plans
            SET status = ?
            WHERE plan_id = ?
            """,
            (
                status.value,
                plan_id,
            ),
        )

    def _fetch_recent(
        self,
        limit: int,
    ) -> List[Dict[str, Any]]:
        cursor = self._connection.execute(
            """
            SELECT
                plan_id,
                alert_id,
                status,
                mitigation_type,
                reasoning,
                created_at
            FROM operational_plans
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )

        rows = cursor.fetchall()

        results = []

        for row in rows:
            results.append(
                {
                    "plan_id":
                        row[0],
                    "alert_id":
                        row[1],
                    "status":
                        row[2],
                    "mitigation_type":
                        row[3],
                    "reasoning":
                        row[4],
                    "created_at":
                        row[5],
                }
            )

        return results


class AutonomousOperationalPlanner:
    """
    Async-first Autonomous Ops Brain.

    Features:
    - Strategic reasoning loop
    - Alert-driven mitigation planning
    - Autonomous lifecycle control
    - SQLite WAL operational ledger
    - Event bus integration
    - RBAC boundary enforcement
    - Low-memory production-safe runtime
    """

    CLEANUP_INTERVAL = 3600

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: MessageBus,
        database_path: str = (
            "./data/operational_planner.db"
        ),
        allowed_permissions: Optional[
            Set[str]
        ] = None,
    ) -> None:
        self.router = router

        self.message_bus = (
            message_bus
        )

        self.allowed_permissions = (
            allowed_permissions
            or {
                "memory.compact",
                "cache.cleanup",
                "throttle.tasks",
                "degradation.enable",
                "service.restart",
                "fallback.activate",
                "workflow.reset",
                "observe",
            }
        )

        self._validator = (
            SystemBoundaryValidator(
                router
            )
        )

        self._reasoning = (
            StrategicReasoningEngine()
        )

        self._ledger = (
            OperationalPlanningLedger(
                database_path=
                    database_path
            )
        )

        self._running = False

        self._tasks: List[
            asyncio.Task
        ] = []

        self._plan_cache: Deque[
            str
        ] = deque(maxlen=128)

    async def start(self) -> None:
        logger.info(
            "Starting AutonomousOperationalPlanner"
        )

        await self._ledger.initialize()

        self._running = True

        self._tasks.append(
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

        self._tasks.append(
            asyncio.create_task(
                self._event_loop()
            )
        )

    async def stop(self) -> None:
        logger.info(
            "Stopping AutonomousOperationalPlanner"
        )

        self._running = False

        for task in self._tasks:
            task.cancel()

        for task in self._tasks:
            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await task

        self._tasks.clear()

        await self._ledger.close()

    async def submit_alert(
        self,
        alert: OperationalAlert,
    ) -> OperationalPlan:
        """
        Main strategic reasoning pipeline.
        """

        mitigation = (
            await self._reasoning.analyze_alert(
                alert
            )
        )

        generated_steps = (
            await self._reasoning.generate_steps(
                mitigation,
                alert,
            )
        )

        validated_steps: List[
            MitigationStep
        ] = []

        for step in generated_steps:
            if (
                step.action
                not in self.allowed_permissions
            ):
                logger.warning(
                    "Boundary rejected action | action=%s",
                    step.action,
                )

                continue

            allowed = (
                await self._validator.validate_action(
                    action=step.action,
                    permissions=
                        self.allowed_permissions,
                    metadata=
                        step.parameters,
                )
            )

            if not allowed:
                logger.warning(
                    "RBAC rejected action | action=%s",
                    step.action,
                )

                continue

            validated_steps.append(
                step
            )

        status = (
            PlanStatus.PENDING
        )

        if not validated_steps:
            status = (
                PlanStatus.BLOCKED
            )

        plan = OperationalPlan(
            plan_id=
                uuid.uuid4().hex,
            alert_id=
                alert.alert_id,
            status=status,
            mitigation_type=
                mitigation,
            steps=
                validated_steps,
            reasoning=
                self._reasoning.build_reasoning(
                    mitigation,
                    alert,
                ),
            created_at=time.time(),
            metadata={
                "source":
                    alert.source,
                "severity":
                    alert.severity.value,
            },
        )

        await self._ledger.persist_plan(
            plan
        )

        self._plan_cache.append(
            plan.plan_id
        )

        return plan

    async def execute_plan(
        self,
        plan: OperationalPlan,
    ) -> bool:
        """
        Autonomous execution controller.
        """

        if not plan.steps:
            await self._ledger.update_status(
                plan_id=plan.plan_id,
                status=
                    PlanStatus.BLOCKED,
            )

            return False

        await self._ledger.update_status(
            plan_id=plan.plan_id,
            status=
                PlanStatus.EXECUTING,
        )

        try:
            ordered_steps = sorted(
                plan.steps,
                key=lambda step:
                step.priority,
            )

            for step in ordered_steps:
                if (
                    step.requires_approval
                ):
                    logger.warning(
                        "Approval-required step skipped | action=%s",
                        step.action,
                    )

                    continue

                await self._dispatch_action(
                    step
                )

            await self._ledger.update_status(
                plan_id=plan.plan_id,
                status=
                    PlanStatus.COMPLETED,
            )

            return True

        except Exception:
            logger.exception(
                "Operational plan execution failed"
            )

            await self._ledger.update_status(
                plan_id=plan.plan_id,
                status=
                    PlanStatus.FAILED,
            )

            return False

    async def _dispatch_action(
        self,
        step: MitigationStep,
    ) -> None:
        payload = {
            "step_id":
                step.step_id,
            "action":
                step.action,
            "parameters":
                step.parameters,
            "timestamp":
                time.time(),
        }

        await self.message_bus.publish(
            topic="ops.mitigation",
            payload=payload,
        )

        logger.info(
            "Mitigation dispatched | action=%s",
            step.action,
        )

    async def _event_loop(
        self,
    ) -> None:
        """
        Event-driven reasoning loop.
        """

        async for event in (
            self.message_bus.subscribe(
                "monitoring.alert"
            )
        ):
            try:
                alert = (
                    self._deserialize_alert(
                        event
                    )
                )

                plan = (
                    await self.submit_alert(
                        alert
                    )
                )

                if (
                    plan.status
                    == PlanStatus.PENDING
                ):
                    await self.execute_plan(
                        plan
                    )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Operational reasoning loop failure"
                )

    async def recent_plans(
        self,
        *,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        return (
            await self._ledger.fetch_recent_plans(
                limit=limit
            )
        )

    async def _maintenance_loop(
        self,
    ) -> None:
        while self._running:
            try:
                await asyncio.sleep(
                    self.CLEANUP_INTERVAL
                )

                await asyncio.to_thread(
                    self._wal_checkpoint
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Operational planner maintenance failure"
                )

    def _wal_checkpoint(
        self,
    ) -> None:
        self._ledger._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def _deserialize_alert(
        self,
        payload: Dict[str, Any],
    ) -> OperationalAlert:
        return OperationalAlert(
            alert_id=payload.get(
                "alert_id",
                uuid.uuid4().hex,
            ),
            source=payload.get(
                "source",
                "unknown",
            ),
            severity=AlertSeverity(
                payload.get(
                    "severity",
                    "low",
                )
            ),
            event_type=payload.get(
                "event_type",
                "unknown",
            ),
            message=payload.get(
                "message",
                "",
            ),
            metrics=payload.get(
                "metrics",
                {},
            ),
            created_at=payload.get(
                "created_at",
                time.time(),
            ),
            metadata=payload.get(
                "metadata",
                {},
            ),
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "running":
                self._running,
            "cached_plans":
                len(
                    self._plan_cache
                ),
            "allowed_permissions":
                len(
                    self.allowed_permissions
                ),
            "timestamp":
                time.time(),
        }
