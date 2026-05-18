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


class DecisionStatus(
    str,
    Enum,
):
    APPROVED = "approved"
    REJECTED = "rejected"
    FALLBACK = "fallback"
    BLOCKED = "blocked"


class DecisionSeverity(
    str,
    Enum,
):
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


class AutonomousAction(
    str,
    Enum,
):
    TASK_BLOCK = "task_block"

    RATE_LIMIT_ADJUST = (
        "rate_limit_adjust"
    )

    FAILOVER_TRIGGER = (
        "failover_trigger"
    )

    DEGRADATION_MODE = (
        "degradation_mode"
    )

    RESTART_SERVICE = (
        "restart_service"
    )

    THROTTLE_SYSTEM = (
        "throttle_system"
    )


@dataclass(slots=True)
class DecisionRequest:
    request_id: str
    source: str
    action: AutonomousAction
    severity: DecisionSeverity
    strategy_score: float
    heuristic_score: float
    predictive_score: float
    metadata: Dict[str, Any]
    created_at: float


@dataclass(slots=True)
class DecisionResult:
    decision_id: str
    request_id: str
    status: DecisionStatus
    approved_action: Optional[
        str
    ]
    confidence_score: float
    reason: str
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class PolicyGatekeeper:
    """
    Default Deny + RBAC gateway.
    """

    IMMUTABLE_DENY = {
        "self.modify",
        "runtime.patch",
        "filesystem.override",
        "force.execute",
    }

    DEFAULT_ALLOWED = {
        "decision.evaluate",
        "decision.approve",
        "decision.cache",
    }

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
        if (
            action
            in self.IMMUTABLE_DENY
        ):
            return False

        if (
            action
            not in self.DEFAULT_ALLOWED
        ):
            return False

        context = RouteContext(
            requester_id=
                "decision_engine",
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


class SQLiteDecisionLedger:
    """
    SQLite WAL operational decisions store.
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

    async def store_result(
        self,
        result: DecisionResult,
    ) -> None:
        await asyncio.to_thread(
            self._insert_result,
            result,
        )

    async def recent_results(
        self,
        *,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self._recent_results,
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
            CREATE TABLE IF NOT EXISTS decision_results (
                decision_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL,
                status TEXT NOT NULL,
                approved_action TEXT,
                confidence_score REAL NOT NULL,
                reason TEXT NOT NULL,
                created_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_decision_created
            ON decision_results(created_at)
            """
        )

    def _insert_result(
        self,
        result: DecisionResult,
    ) -> None:
        self._connection.execute(
            """
            INSERT OR REPLACE INTO decision_results (
                decision_id,
                request_id,
                status,
                approved_action,
                confidence_score,
                reason,
                created_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.decision_id,
                result.request_id,
                result.status.value,
                result.approved_action,
                result.confidence_score,
                result.reason,
                result.created_at,
                json.dumps(
                    result.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _recent_results(
        self,
        limit: int,
    ) -> List[Dict[str, Any]]:
        cursor = self._connection.execute(
            """
            SELECT
                decision_id,
                request_id,
                status,
                approved_action,
                confidence_score,
                reason,
                created_at
            FROM decision_results
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
                    "decision_id":
                        row[0],
                    "request_id":
                        row[1],
                    "status":
                        row[2],
                    "approved_action":
                        row[3],
                    "confidence_score":
                        row[4],
                    "reason":
                        row[5],
                    "created_at":
                        row[6],
                }
            )

        return results


class ConflictResolutionManager:
    """
    Static fallback matrix resolver.
    """

    FALLBACK_MATRIX = {
        AutonomousAction.FAILOVER_TRIGGER: (
            AutonomousAction.DEGRADATION_MODE
        ),
        AutonomousAction.RESTART_SERVICE: (
            AutonomousAction.THROTTLE_SYSTEM
        ),
        AutonomousAction.THROTTLE_SYSTEM: (
            AutonomousAction.RATE_LIMIT_ADJUST
        ),
    }

    def resolve(
        self,
        request: DecisionRequest,
    ) -> DecisionResult:
        composite_score = (
            (
                request.strategy_score
                * 0.4
            )
            + (
                request.heuristic_score
                * 0.3
            )
            + (
                request.predictive_score
                * 0.3
            )
        )

        if (
            composite_score
            >= 0.75
        ):
            return DecisionResult(
                decision_id=
                    uuid.uuid4().hex,
                request_id=
                    request.request_id,
                status=
                    DecisionStatus.APPROVED,
                approved_action=
                    request.action.value,
                confidence_score=
                    round(
                        composite_score,
                        4,
                    ),
                reason=
                    "Decision approved by composite scoring engine.",
                created_at=
                    time.time(),
            )

        fallback = (
            self.FALLBACK_MATRIX.get(
                request.action
            )
        )

        if fallback:
            return DecisionResult(
                decision_id=
                    uuid.uuid4().hex,
                request_id=
                    request.request_id,
                status=
                    DecisionStatus.FALLBACK,
                approved_action=
                    fallback.value,
                confidence_score=
                    round(
                        composite_score,
                        4,
                    ),
                reason=
                    "Conflict detected. Static fallback matrix applied.",
                created_at=
                    time.time(),
            )

        return DecisionResult(
            decision_id=
                uuid.uuid4().hex,
            request_id=
                request.request_id,
            status=
                DecisionStatus.REJECTED,
            approved_action=None,
            confidence_score=
                round(
                    composite_score,
                    4,
                ),
            reason=
                "Decision rejected due to insufficient confidence.",
            created_at=
                time.time(),
        )


class StateSafetyCache:
    """
    Lightweight in-memory safety cache.
    """

    CACHE_LIMIT = 256

    def __init__(
        self,
    ) -> None:
        self._cache: Dict[
            str,
            DecisionResult,
        ] = {}

        self._order: Deque[
            str
        ] = deque(
            maxlen=self.CACHE_LIMIT
        )

    def put(
        self,
        result: DecisionResult,
    ) -> None:
        self._cache[
            result.request_id
        ] = result

        self._order.append(
            result.request_id
        )

        while (
            len(self._cache)
            > self.CACHE_LIMIT
        ):
            oldest = (
                self._order.popleft()
            )

            self._cache.pop(
                oldest,
                None,
            )

    def get(
        self,
        request_id: str,
    ) -> Optional[
        DecisionResult
    ]:
        return self._cache.get(
            request_id
        )

    def snapshot(
        self,
    ) -> Dict[str, Any]:
        return {
            request_id: {
                "status":
                    result.status.value,
                "approved_action":
                    result.approved_action,
                "confidence":
                    result.confidence_score,
            }
            for (
                request_id,
                result,
            ) in self._cache.items()
        }


class OperationalDecisionEngine:
    """
    Async-first Operational Decision Engine.

    Features:
    - Final operational approval gateway
    - Conflict resolution matrix
    - Policy gatekeeper
    - Lightweight safety cache
    - SQLite WAL persistence
    - Event bus synchronization
    - Strict RBAC enforcement
    - Default Deny security
    """

    CLEANUP_INTERVAL = 3600

    DEFAULT_ALLOWED_PERMISSIONS = {
        "decision.evaluate",
        "decision.approve",
        "decision.cache",
    }

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: MessageBus,
        database_path: str = (
            "./data/decision_engine.db"
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

        self._gatekeeper = (
            PolicyGatekeeper(
                router
            )
        )

        self._ledger = (
            SQLiteDecisionLedger(
                database_path=
                    database_path
            )
        )

        self._resolver = (
            ConflictResolutionManager()
        )

        self._cache = (
            StateSafetyCache()
        )

        self._running = False

        self._tasks: List[
            asyncio.Task
        ] = []

        self._decision_cache: Deque[
            str
        ] = deque(maxlen=128)

    async def start(
        self,
    ) -> None:
        logger.info(
            "Starting OperationalDecisionEngine"
        )

        await self._ledger.initialize()

        self._running = True

        self._tasks.append(
            asyncio.create_task(
                self._decision_listener()
            )
        )

        self._tasks.append(
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(
        self,
    ) -> None:
        logger.info(
            "Stopping OperationalDecisionEngine"
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

    async def evaluate(
        self,
        request: DecisionRequest,
    ) -> DecisionResult:
        """
        Final operational approval gateway.
        """

        allowed = (
            await self._gatekeeper.validate(
                action=
                    "decision.evaluate",
                permissions=
                    self.allowed_permissions,
                metadata=
                    request.metadata,
            )
        )

        if not allowed:
            result = (
                DecisionResult(
                    decision_id=
                        uuid.uuid4().hex,
                    request_id=
                        request.request_id,
                    status=
                        DecisionStatus.BLOCKED,
                    approved_action=
                        None,
                    confidence_score=
                        0.0,
                    reason=
                        "Blocked by RBAC or policy gatekeeper.",
                    created_at=
                        time.time(),
                )
            )

            await self._finalize_result(
                result
            )

            return result

        result = (
            self._resolver.resolve(
                request
            )
        )

        if (
            result.status
            == DecisionStatus.APPROVED
        ):
            approval = (
                await self._gatekeeper.validate(
                    action=
                        "decision.approve",
                    permissions=
                        self.allowed_permissions,
                    metadata={
                        "approved_action":
                            result.approved_action
                    },
                )
            )

            if not approval:
                result.status = (
                    DecisionStatus.BLOCKED
                )

                result.reason = (
                    "Final approval denied by policy gatekeeper."
                )

                result.approved_action = (
                    None
                )

        await self._finalize_result(
            result
        )

        return result

    async def _finalize_result(
        self,
        result: DecisionResult,
    ) -> None:
        await self._ledger.store_result(
            result
        )

        self._cache.put(
            result
        )

        self._decision_cache.append(
            result.decision_id
        )

        await self._broadcast_decision(
            result
        )

    async def _broadcast_decision(
        self,
        result: DecisionResult,
    ) -> None:
        payload = {
            "decision_id":
                result.decision_id,
            "request_id":
                result.request_id,
            "status":
                result.status.value,
            "approved_action":
                result.approved_action,
            "confidence":
                result.confidence_score,
            "reason":
                result.reason,
            "timestamp":
                result.created_at,
        }

        await self.message_bus.publish(
            topic=
                "decision.finalized",
            payload=payload,
        )

    async def _decision_listener(
        self,
    ) -> None:
        """
        Autonomous decision intake loop.
        """

        async for event in (
            self.message_bus.subscribe(
                "decision.request"
            )
        ):
            try:
                request = (
                    self._deserialize_request(
                        event
                    )
                )

                await self.evaluate(
                    request
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Decision listener failure"
                )

    def _deserialize_request(
        self,
        payload: Dict[str, Any],
    ) -> DecisionRequest:
        return DecisionRequest(
            request_id=payload.get(
                "request_id",
                uuid.uuid4().hex,
            ),
            source=payload.get(
                "source",
                "unknown",
            ),
            action=
                AutonomousAction(
                    payload.get(
                        "action",
                        "task_block",
                    )
                ),
            severity=
                DecisionSeverity(
                    payload.get(
                        "severity",
                        "low",
                    )
                ),
            strategy_score=float(
                payload.get(
                    "strategy_score",
                    0.0,
                )
            ),
            heuristic_score=float(
                payload.get(
                    "heuristic_score",
                    0.0,
                )
            ),
            predictive_score=float(
                payload.get(
                    "predictive_score",
                    0.0,
                )
            ),
            metadata=payload.get(
                "metadata",
                {},
            ),
            created_at=payload.get(
                "created_at",
                time.time(),
            ),
        )

    async def recent_decisions(
        self,
        *,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        return (
            await self._ledger.recent_results(
                limit=limit
            )
        )

    async def get_cached_decision(
        self,
        request_id: str,
    ) -> Optional[
        DecisionResult
    ]:
        allowed = (
            await self._gatekeeper.validate(
                action=
                    "decision.cache",
                permissions=
                    self.allowed_permissions,
            )
        )

        if not allowed:
            return None

        return self._cache.get(
            request_id
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
                    "Decision engine maintenance failure"
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
            "cached_decisions":
                len(
                    self._decision_cache
                ),
            "cache_size":
                len(
                    self._cache.snapshot()
                ),
            "permissions":
                list(
                    self.allowed_permissions
                ),
            "timestamp":
                time.time(),
        }
