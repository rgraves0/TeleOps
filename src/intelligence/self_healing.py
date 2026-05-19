from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import sqlite3
import time
import traceback
from dataclasses import (
    dataclass,
    field,
)
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Set,
)

logger = logging.getLogger(__name__)


class ErrorCategory(
    str,
    Enum,
):
    NETWORK_TIMEOUT = (
        "network_timeout"
    )
    RATE_LIMIT = (
        "rate_limit"
    )
    PARSING_ERROR = (
        "parsing_error"
    )
    DATABASE_LOCK = (
        "database_lock"
    )
    PERMISSION_DENIED = (
        "permission_denied"
    )
    RESOURCE_EXHAUSTED = (
        "resource_exhausted"
    )
    UNKNOWN = "unknown"


class RecoveryStrategy(
    str,
    Enum,
):
    RETRY_BACKOFF = (
        "retry_backoff"
    )
    TOOL_FAILOVER = (
        "tool_failover"
    )
    STATE_ROLLBACK = (
        "state_rollback"
    )
    ABORT = "abort"


@dataclass(slots=True)
class HealingTask:
    task_id: str
    task_type: str
    payload: Dict[str, Any]
    permissions: Set[str]
    retries: int = 0
    max_retries: int = 3
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class RCAResult:
    category: ErrorCategory
    confidence: float
    reason: str
    strategy: RecoveryStrategy
    retryable: bool


@dataclass(slots=True)
class HealingResult:
    task_id: str
    success: bool
    strategy_used: str
    attempts: int
    healed_at: float
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class HealingSecurityError(
    Exception
):
    pass


class RetryLimitExceeded(
    Exception
):
    pass


class SQLiteHealingLedger:
    """
    SQLite WAL healing ledger.
    """

    SQLITE_BUSY_TIMEOUT = 5000

    def __init__(
        self,
        database_path: str,
    ) -> None:

        self.database_path = Path(
            database_path
        )

        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._connection: Optional[
            sqlite3.Connection
        ] = None

    async def initialize(
        self,
    ) -> None:

        self._connection = sqlite3.connect(
            str(self.database_path),
            check_same_thread=False,
            isolation_level=None,
        )

        await asyncio.to_thread(
            self._configure
        )

        await asyncio.to_thread(
            self._create_tables
        )

    async def close(
        self,
    ) -> None:

        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    def _configure(
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
            f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT};"
        )

    def _create_tables(
        self,
    ) -> None:

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS healing_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                category TEXT NOT NULL,
                strategy TEXT NOT NULL,
                success INTEGER NOT NULL,
                attempts INTEGER NOT NULL,
                created_at REAL NOT NULL,
                error TEXT,
                metadata TEXT NOT NULL
            )
            """
        )

    async def record_healing(
        self,
        result: HealingResult,
    ) -> None:

        await asyncio.to_thread(
            self._record_sync,
            result,
        )

    def _record_sync(
        self,
        result: HealingResult,
    ) -> None:

        self._connection.execute(
            """
            INSERT INTO healing_events (
                task_id,
                category,
                strategy,
                success,
                attempts,
                created_at,
                error,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.task_id,
                result.metadata.get(
                    "category",
                    "unknown",
                ),
                result.strategy_used,
                (
                    1
                    if result.success
                    else 0
                ),
                result.attempts,
                result.healed_at,
                result.error,
                json.dumps(
                    result.metadata
                ),
            ),
        )


class ErrorRCAAnalyzer:
    """
    Lightweight heuristic RCA analyzer.
    """

    def analyze(
        self,
        *,
        exception: Exception,
        traceback_text: str,
    ) -> RCAResult:

        error_text = (
            f"{exception} {traceback_text}"
        ).lower()

        if (
            "timeout"
            in error_text
            or "connection reset"
            in error_text
        ):
            return RCAResult(
                category=
                    ErrorCategory.NETWORK_TIMEOUT,
                confidence=0.92,
                reason=
                    "Network instability detected",
                strategy=
                    RecoveryStrategy.RETRY_BACKOFF,
                retryable=True,
            )

        if (
            "rate limit"
            in error_text
            or "429"
            in error_text
        ):
            return RCAResult(
                category=
                    ErrorCategory.RATE_LIMIT,
                confidence=0.96,
                reason=
                    "Rate limit threshold exceeded",
                strategy=
                    RecoveryStrategy.RETRY_BACKOFF,
                retryable=True,
            )

        if (
            "json"
            in error_text
            or "parse"
            in error_text
        ):
            return RCAResult(
                category=
                    ErrorCategory.PARSING_ERROR,
                confidence=0.87,
                reason=
                    "Payload parsing failure",
                strategy=
                    RecoveryStrategy.STATE_ROLLBACK,
                retryable=False,
            )

        if (
            "database is locked"
            in error_text
        ):
            return RCAResult(
                category=
                    ErrorCategory.DATABASE_LOCK,
                confidence=0.91,
                reason=
                    "SQLite lock contention",
                strategy=
                    RecoveryStrategy.RETRY_BACKOFF,
                retryable=True,
            )

        if (
            "permission"
            in error_text
            or "rbac"
            in error_text
            or "denied"
            in error_text
        ):
            return RCAResult(
                category=
                    ErrorCategory.PERMISSION_DENIED,
                confidence=0.99,
                reason=
                    "RBAC policy violation",
                strategy=
                    RecoveryStrategy.ABORT,
                retryable=False,
            )

        if (
            "memory"
            in error_text
            or "oom"
            in error_text
        ):
            return RCAResult(
                category=
                    ErrorCategory.RESOURCE_EXHAUSTED,
                confidence=0.94,
                reason=
                    "Resource exhaustion detected",
                strategy=
                    RecoveryStrategy.TOOL_FAILOVER,
                retryable=True,
            )

        return RCAResult(
            category=
                ErrorCategory.UNKNOWN,
            confidence=0.50,
            reason=
                "Unknown execution failure",
            strategy=
                RecoveryStrategy.ABORT,
            retryable=False,
        )


class SecurityBoundaryValidator:
    """
    Default Deny RBAC validator.
    """

    REQUIRED_PERMISSION = (
        "system.self_heal"
    )

    async def validate(
        self,
        *,
        task_permissions: Set[str],
        healing_permissions: Set[str],
    ) -> bool:

        if (
            self.REQUIRED_PERMISSION
            not in healing_permissions
        ):
            return False

        return (
            task_permissions
            <= healing_permissions
        )


class HealingStrategyDispatcher:
    """
    Recovery strategy runtime.
    """

    def __init__(
        self,
    ) -> None:

        self._tool_failovers: Dict[
            str,
            str,
        ] = {}

    async def register_failover(
        self,
        *,
        primary_tool: str,
        fallback_tool: str,
    ) -> None:

        self._tool_failovers[
            primary_tool
        ] = fallback_tool

    async def retry_backoff(
        self,
        *,
        task: HealingTask,
    ) -> None:

        delay = min(
            (
                2
                ** task.retries
            )
            + random.uniform(
                0.1,
                0.8,
            ),
            30,
        )

        await asyncio.sleep(
            delay
        )

    async def tool_failover(
        self,
        *,
        task: HealingTask,
    ) -> Dict[str, Any]:

        current_tool = (
            task.metadata.get(
                "tool"
            )
        )

        fallback = (
            self._tool_failovers.get(
                current_tool
            )
        )

        if not fallback:
            raise RuntimeError(
                "No fallback tool available"
            )

        task.metadata[
            "tool"
        ] = fallback

        return {
            "fallback_tool":
                fallback
        }

    async def rollback_state(
        self,
        *,
        task: HealingTask,
    ) -> Dict[str, Any]:

        rollback_state = (
            task.metadata.get(
                "last_stable_state",
                {},
            )
        )

        return {
            "rolled_back":
                True,
            "state":
                rollback_state,
        }


class AutonomousRecoveryOrchestrator:
    """
    Async-first self-healing engine.

    Features:
    - RCA analysis
    - Autonomous recovery
    - Exponential retry
    - State rollback
    - Tool failover
    - Default Deny RBAC
    """

    WAL_CHECKPOINT_INTERVAL = 1800

    def __init__(
        self,
        *,
        healing_permissions: Set[str],
        database_path: str = (
            "./data/self_healing.db"
        ),
    ) -> None:

        self.healing_permissions = (
            healing_permissions
        )

        self.ledger = (
            SQLiteHealingLedger(
                database_path
            )
        )

        self.rca = (
            ErrorRCAAnalyzer()
        )

        self.validator = (
            SecurityBoundaryValidator()
        )

        self.dispatcher = (
            HealingStrategyDispatcher()
        )

        self._running = False

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting AutonomousRecoveryOrchestrator"
        )

        await self.ledger.initialize()

        self._running = True

        self._maintenance_task = (
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(
        self,
    ) -> None:

        logger.info(
            "Stopping AutonomousRecoveryOrchestrator"
        )

        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._maintenance_task

        await self.ledger.close()

    async def execute_with_healing(
        self,
        *,
        task: HealingTask,
        operation: Callable[
            [],
            Awaitable[Any],
        ],
    ) -> HealingResult:

        authorized = (
            await self.validator.validate(
                task_permissions=
                    task.permissions,
                healing_permissions=
                    self.healing_permissions,
            )
        )

        if not authorized:
            raise HealingSecurityError(
                "Healing permission denied"
            )

        while (
            task.retries
            <= task.max_retries
        ):
            try:
                await operation()

                result = HealingResult(
                    task_id=
                        task.task_id,
                    success=True,
                    strategy_used=
                        "normal_execution",
                    attempts=
                        task.retries,
                    healed_at=
                        time.time(),
                    metadata={
                        "category":
                            "none"
                    },
                )

                await self.ledger.record_healing(
                    result
                )

                return result

            except Exception as exc:

                trace = (
                    traceback.format_exc()
                )

                rca_result = (
                    self.rca.analyze(
                        exception=exc,
                        traceback_text=
                            trace,
                    )
                )

                logger.warning(
                    "Self-healing triggered "
                    "task=%s "
                    "category=%s "
                    "strategy=%s",
                    task.task_id,
                    rca_result.category.value,
                    rca_result.strategy.value,
                )

                if (
                    task.retries
                    >= task.max_retries
                ):
                    raise RetryLimitExceeded(
                        f"Max retries exceeded for {task.task_id}"
                    )

                if (
                    not rca_result.retryable
                ):
                    result = HealingResult(
                        task_id=
                            task.task_id,
                        success=False,
                        strategy_used=
                            rca_result.strategy.value,
                        attempts=
                            task.retries,
                        healed_at=
                            time.time(),
                        error=
                            str(exc),
                        metadata={
                            "category":
                                rca_result.category.value,
                            "reason":
                                rca_result.reason,
                        },
                    )

                    await self.ledger.record_healing(
                        result
                    )

                    return result

                await self._execute_strategy(
                    task=task,
                    strategy=
                        rca_result.strategy,
                )

                task.retries += 1

        raise RetryLimitExceeded(
            "Healing execution halted"
        )

    async def _execute_strategy(
        self,
        *,
        task: HealingTask,
        strategy: RecoveryStrategy,
    ) -> None:

        if (
            strategy
            == RecoveryStrategy.RETRY_BACKOFF
        ):
            await self.dispatcher.retry_backoff(
                task=task
            )

            return

        if (
            strategy
            == RecoveryStrategy.TOOL_FAILOVER
        ):
            await self.dispatcher.tool_failover(
                task=task
            )

            return

        if (
            strategy
            == RecoveryStrategy.STATE_ROLLBACK
        ):
            await self.dispatcher.rollback_state(
                task=task
            )

            return

        raise RuntimeError(
            "Autonomous recovery aborted"
        )

    async def register_tool_failover(
        self,
        *,
        primary_tool: str,
        fallback_tool: str,
    ) -> None:

        await self.dispatcher.register_failover(
            primary_tool=
                primary_tool,
            fallback_tool=
                fallback_tool,
        )

    async def _maintenance_loop(
        self,
    ) -> None:

        while self._running:
            try:
                await asyncio.sleep(
                    self.WAL_CHECKPOINT_INTERVAL
                )

                await asyncio.to_thread(
                    self._wal_checkpoint
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.error(
                    traceback.format_exc()
                )

    def _wal_checkpoint(
        self,
    ) -> None:

        self.ledger._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:

        return {
            "running":
                self._running,
            "healing_permissions":
                len(
                    self.healing_permissions
                ),
            "timestamp":
                time.time(),
        }


DEFAULT_SELF_HEALING_ENGINE = (
    AutonomousRecoveryOrchestrator
)
