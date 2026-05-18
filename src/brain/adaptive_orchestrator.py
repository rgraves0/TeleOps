from __future__ import annotations

import asyncio
import contextlib
import heapq
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
    Awaitable,
    Callable,
    Coroutine,
    Deque,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
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


class OrchestrationState(
    str,
    Enum,
):
    PENDING = "pending"
    RUNNING = "running"
    THROTTLED = "throttled"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class PriorityLevel(
    int,
    Enum,
):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


class AdaptiveSignal(
    str,
    Enum,
):
    REDUCE_CONCURRENCY = (
        "reduce_concurrency"
    )

    PAUSE_BACKGROUND = (
        "pause_background"
    )

    ENABLE_DEGRADATION = (
        "enable_degradation"
    )

    RESTORE_NORMAL = (
        "restore_normal"
    )


@dataclass(slots=True)
class OrchestrationTask:
    task_id: str
    agent_name: str
    priority: PriorityLevel
    coroutine_factory: Callable[
        [],
        Coroutine[Any, Any, Any],
    ]
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class OrchestrationRecord:
    record_id: str
    task_id: str
    agent_name: str
    state: OrchestrationState
    priority: int
    concurrency_limit: int
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class OrchestrationBoundaryValidator:
    """
    Default Deny + RBAC guardrails.
    """

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate(
        self,
        *,
        action: str,
        permissions: Set[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:
        context = RouteContext(
            requester_id=
                "adaptive_orchestrator",
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


class SQLiteStateLedger:
    """
    SQLite WAL orchestration state ledger.
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

    async def persist_record(
        self,
        record: OrchestrationRecord,
    ) -> None:
        await asyncio.to_thread(
            self._insert_record,
            record,
        )

    async def update_state(
        self,
        *,
        task_id: str,
        state: OrchestrationState,
    ) -> None:
        await asyncio.to_thread(
            self._update_state,
            task_id,
            state,
        )

    async def recent_records(
        self,
        *,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self._recent_records,
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
            CREATE TABLE IF NOT EXISTS orchestration_states (
                record_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                agent_name TEXT NOT NULL,
                state TEXT NOT NULL,
                priority INTEGER NOT NULL,
                concurrency_limit INTEGER NOT NULL,
                created_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orch_task
            ON orchestration_states(task_id)
            """
        )

    def _insert_record(
        self,
        record: OrchestrationRecord,
    ) -> None:
        self._connection.execute(
            """
            INSERT OR REPLACE INTO orchestration_states (
                record_id,
                task_id,
                agent_name,
                state,
                priority,
                concurrency_limit,
                created_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.record_id,
                record.task_id,
                record.agent_name,
                record.state.value,
                record.priority,
                record.concurrency_limit,
                record.created_at,
                json.dumps(
                    record.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _update_state(
        self,
        task_id: str,
        state: OrchestrationState,
    ) -> None:
        self._connection.execute(
            """
            UPDATE orchestration_states
            SET state = ?
            WHERE task_id = ?
            """,
            (
                state.value,
                task_id,
            ),
        )

    def _recent_records(
        self,
        limit: int,
    ) -> List[Dict[str, Any]]:
        cursor = self._connection.execute(
            """
            SELECT
                task_id,
                agent_name,
                state,
                priority,
                concurrency_limit,
                created_at
            FROM orchestration_states
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
                    "task_id":
                        row[0],
                    "agent_name":
                        row[1],
                    "state":
                        row[2],
                    "priority":
                        row[3],
                    "concurrency_limit":
                        row[4],
                    "created_at":
                        row[5],
                }
            )

        return results


class PriorityQueueController:
    """
    Lightweight adaptive priority queue.
    """

    def __init__(
        self,
    ) -> None:
        self._queue: List[
            Tuple[
                int,
                float,
                OrchestrationTask,
            ]
        ] = []

        self._lock = (
            asyncio.Lock()
        )

    async def push(
        self,
        task: OrchestrationTask,
    ) -> None:
        async with self._lock:
            heapq.heappush(
                self._queue,
                (
                    int(task.priority),
                    task.created_at,
                    task,
                ),
            )

    async def pop(
        self,
    ) -> Optional[
        OrchestrationTask
    ]:
        async with self._lock:
            if not self._queue:
                return None

            return heapq.heappop(
                self._queue
            )[2]

    async def reprioritize(
        self,
        *,
        threshold: PriorityLevel,
    ) -> None:
        async with self._lock:
            updated = []

            while self._queue:
                (
                    priority,
                    created,
                    task,
                ) = heapq.heappop(
                    self._queue
                )

                if (
                    task.priority
                    > threshold
                ):
                    task.priority = (
                        threshold
                    )

                updated.append(
                    (
                        int(
                            task.priority
                        ),
                        created,
                        task,
                    )
                )

            for item in updated:
                heapq.heappush(
                    self._queue,
                    item,
                )

    def size(
        self,
    ) -> int:
        return len(
            self._queue
        )


class DynamicWorkflowTuner:
    """
    Adaptive concurrency tuner.
    """

    DEFAULT_CONCURRENCY = 4
    MIN_CONCURRENCY = 1
    MAX_CONCURRENCY = 8

    def __init__(
        self,
    ) -> None:
        self.current_concurrency = (
            self.DEFAULT_CONCURRENCY
        )

    def apply_signal(
        self,
        signal: AdaptiveSignal,
    ) -> int:
        if (
            signal
            == AdaptiveSignal.REDUCE_CONCURRENCY
        ):
            self.current_concurrency = max(
                self.MIN_CONCURRENCY,
                self.current_concurrency
                - 1,
            )

        elif (
            signal
            == AdaptiveSignal.ENABLE_DEGRADATION
        ):
            self.current_concurrency = (
                self.MIN_CONCURRENCY
            )

        elif (
            signal
            == AdaptiveSignal.RESTORE_NORMAL
        ):
            self.current_concurrency = min(
                self.DEFAULT_CONCURRENCY,
                self.current_concurrency
                + 1,
            )

        return (
            self.current_concurrency
        )


class AdaptiveOrchestrator:
    """
    Async-first Adaptive Orchestrator.

    Features:
    - Dynamic workflow tuning
    - Priority-based execution
    - Adaptive concurrency scaling
    - Event bus synchronization
    - SQLite WAL state persistence
    - RBAC-safe orchestration
    - Low-memory queue execution
    """

    CLEANUP_INTERVAL = 3600

    DEFAULT_ALLOWED_PERMISSIONS = {
        "orchestration.manage",
        "orchestration.sync",
        "system.throttle",
    }

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: MessageBus,
        database_path: str = (
            "./data/adaptive_orchestrator.db"
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
            or set(
                self.DEFAULT_ALLOWED_PERMISSIONS
            )
        )

        self._validator = (
            OrchestrationBoundaryValidator(
                router
            )
        )

        self._ledger = (
            SQLiteStateLedger(
                database_path=
                    database_path
            )
        )

        self._queue = (
            PriorityQueueController()
        )

        self._tuner = (
            DynamicWorkflowTuner()
        )

        self._running = False

        self._workers: List[
            asyncio.Task
        ] = []

        self._tasks: List[
            asyncio.Task
        ] = []

        self._active_tasks: Dict[
            str,
            asyncio.Task,
        ] = {}

        self._state_cache: Deque[
            str
        ] = deque(maxlen=128)

    async def start(self) -> None:
        logger.info(
            "Starting AdaptiveOrchestrator"
        )

        await self._ledger.initialize()

        self._running = True

        await self._spawn_workers()

        self._tasks.append(
            asyncio.create_task(
                self._signal_listener()
            )
        )

        self._tasks.append(
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(self) -> None:
        logger.info(
            "Stopping AdaptiveOrchestrator"
        )

        self._running = False

        for worker in self._workers:
            worker.cancel()

        for task in self._tasks:
            task.cancel()

        for worker in self._workers:
            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await worker

        for task in self._tasks:
            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await task

        self._workers.clear()

        self._tasks.clear()

        await self._ledger.close()

    async def submit_task(
        self,
        task: OrchestrationTask,
    ) -> bool:
        """
        Priority-aware task submission.
        """

        allowed = (
            await self._validator.validate(
                action=
                    "orchestration.manage",
                permissions=
                    self.allowed_permissions,
                metadata=
                    task.metadata,
            )
        )

        if not allowed:
            logger.warning(
                "Task rejected by RBAC | task=%s",
                task.task_id,
            )

            return False

        await self._queue.push(
            task
        )

        await self._persist_state(
            task=task,
            state=
                OrchestrationState.PENDING,
        )

        return True

    async def _spawn_workers(
        self,
    ) -> None:
        worker_count = (
            self._tuner.current_concurrency
        )

        for _ in range(worker_count):
            worker = (
                asyncio.create_task(
                    self._worker_loop()
                )
            )

            self._workers.append(
                worker
            )

    async def _resize_workers(
        self,
    ) -> None:
        target = (
            self._tuner.current_concurrency
        )

        current = len(
            self._workers
        )

        if target > current:
            for _ in range(
                target - current
            ):
                worker = (
                    asyncio.create_task(
                        self._worker_loop()
                    )
                )

                self._workers.append(
                    worker
                )

        elif target < current:
            removable = (
                self._workers[target:]
            )

            self._workers = (
                self._workers[:target]
            )

            for worker in removable:
                worker.cancel()

    async def _worker_loop(
        self,
    ) -> None:
        while self._running:
            try:
                task = (
                    await self._queue.pop()
                )

                if not task:
                    await asyncio.sleep(
                        0.2
                    )

                    continue

                orchestration_task = (
                    asyncio.create_task(
                        self._execute_task(
                            task
                        )
                    )
                )

                self._active_tasks[
                    task.task_id
                ] = (
                    orchestration_task
                )

                await orchestration_task

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Adaptive worker failure"
                )

    async def _execute_task(
        self,
        task: OrchestrationTask,
    ) -> None:
        await self._persist_state(
            task=task,
            state=
                OrchestrationState.RUNNING,
        )

        try:
            await task.coroutine_factory()

            await self._persist_state(
                task=task,
                state=
                    OrchestrationState.COMPLETED,
            )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "Orchestrated task failed | task=%s",
                task.task_id,
            )

            await self._persist_state(
                task=task,
                state=
                    OrchestrationState.FAILED,
            )

        finally:
            self._active_tasks.pop(
                task.task_id,
                None,
            )

    async def _persist_state(
        self,
        *,
        task: OrchestrationTask,
        state: OrchestrationState,
    ) -> None:
        record = (
            OrchestrationRecord(
                record_id=
                    uuid.uuid4().hex,
                task_id=
                    task.task_id,
                agent_name=
                    task.agent_name,
                state=state,
                priority=int(
                    task.priority
                ),
                concurrency_limit=
                    self._tuner.current_concurrency,
                created_at=
                    time.time(),
                metadata=
                    task.metadata,
            )
        )

        await self._ledger.persist_record(
            record
        )

        self._state_cache.append(
            task.task_id
        )

        await self._sync_state(
            record
        )

    async def _sync_state(
        self,
        record: OrchestrationRecord,
    ) -> None:
        allowed = (
            await self._validator.validate(
                action=
                    "orchestration.sync",
                permissions=
                    self.allowed_permissions,
            )
        )

        if not allowed:
            return

        payload = {
            "task_id":
                record.task_id,
            "agent_name":
                record.agent_name,
            "state":
                record.state.value,
            "priority":
                record.priority,
            "concurrency":
                record.concurrency_limit,
            "timestamp":
                record.created_at,
        }

        await self.message_bus.publish(
            topic="orchestration.state",
            payload=payload,
        )

    async def _signal_listener(
        self,
    ) -> None:
        """
        Adaptive signal processor.
        """

        async for event in (
            self.message_bus.subscribe(
                "system.throttle"
            )
        ):
            try:
                signal = (
                    self._map_signal(
                        event
                    )
                )

                allowed = (
                    await self._validator.validate(
                        action=
                            "system.throttle",
                        permissions=
                            self.allowed_permissions,
                        metadata=
                            event,
                    )
                )

                if not allowed:
                    logger.warning(
                        "Throttle signal rejected"
                    )

                    continue

                concurrency = (
                    self._tuner.apply_signal(
                        signal
                    )
                )

                await self._resize_workers()

                if signal in {
                    AdaptiveSignal.REDUCE_CONCURRENCY,
                    AdaptiveSignal.ENABLE_DEGRADATION,
                }:
                    await self._queue.reprioritize(
                        threshold=
                            PriorityLevel.HIGH
                    )

                await self.message_bus.publish(
                    topic=
                        "orchestration.adaptive",
                    payload={
                        "signal":
                            signal.value,
                        "concurrency":
                            concurrency,
                        "queue_size":
                            self._queue.size(),
                        "timestamp":
                            time.time(),
                    },
                )

                logger.warning(
                    "Adaptive tuning applied | signal=%s concurrency=%s",
                    signal.value,
                    concurrency,
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Adaptive signal failure"
                )

    def _map_signal(
        self,
        payload: Dict[str, Any],
    ) -> AdaptiveSignal:
        action = payload.get(
            "action",
            "",
        )

        if (
            action
            == "reduce_concurrency"
        ):
            return (
                AdaptiveSignal.REDUCE_CONCURRENCY
            )

        if (
            action
            == "pause_background_tasks"
        ):
            return (
                AdaptiveSignal.PAUSE_BACKGROUND
            )

        if (
            action
            == "enable_degradation"
        ):
            return (
                AdaptiveSignal.ENABLE_DEGRADATION
            )

        return (
            AdaptiveSignal.RESTORE_NORMAL
        )

    async def recent_states(
        self,
        *,
        limit: int = 25,
    ) -> List[Dict[str, Any]]:
        return (
            await self._ledger.recent_records(
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
                    "Adaptive orchestrator maintenance failure"
                )

    def _wal_checkpoint(
        self,
    ) -> None:
        self._ledger._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "running":
                self._running,
            "queue_size":
                self._queue.size(),
            "active_tasks":
                len(
                    self._active_tasks
                ),
            "workers":
                len(
                    self._workers
                ),
            "concurrency":
                self._tuner.current_concurrency,
            "cached_states":
                len(
                    self._state_cache
                ),
            "timestamp":
                time.time(),
        }
