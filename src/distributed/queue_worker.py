from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import secrets
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


class TaskStatus(
    str,
    Enum,
):
    PENDING = "pending"
    FETCHED = "fetched"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    REJECTED = "rejected"


@dataclass(slots=True)
class DistributedTask:
    task_id: str
    queue_name: str
    task_type: str
    payload: Dict[str, Any]
    permissions: Set[str]
    created_at: float
    priority: int = 100
    status: TaskStatus = (
        TaskStatus.PENDING
    )
    assigned_node: Optional[
        str
    ] = None
    signature: str = ""
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class TaskResult:
    task_id: str
    success: bool
    result: Dict[str, Any]
    completed_at: float
    error: Optional[str] = None


class RBACValidationError(
    Exception
):
    pass


class PayloadSignatureError(
    Exception
):
    pass


class SQLiteDistributedQueue:
    """
    SQLite WAL-backed distributed FIFO queue.
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
            "PRAGMA cache_size=-1500;"
        )

        self._connection.execute(
            f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT};"
        )

    def _create_tables(
        self,
    ) -> None:

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS distributed_tasks (
                task_id TEXT PRIMARY KEY,
                queue_name TEXT NOT NULL,
                task_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                permissions TEXT NOT NULL,
                created_at REAL NOT NULL,
                priority INTEGER NOT NULL,
                status TEXT NOT NULL,
                assigned_node TEXT,
                signature TEXT NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS distributed_results (
                task_id TEXT PRIMARY KEY,
                success INTEGER NOT NULL,
                result TEXT NOT NULL,
                completed_at REAL NOT NULL,
                error TEXT
            )
            """
        )

    async def enqueue(
        self,
        task: DistributedTask,
    ) -> None:

        await asyncio.to_thread(
            self._enqueue_sync,
            task,
        )

    def _enqueue_sync(
        self,
        task: DistributedTask,
    ) -> None:

        self._connection.execute(
            """
            INSERT INTO distributed_tasks (
                task_id,
                queue_name,
                task_type,
                payload,
                permissions,
                created_at,
                priority,
                status,
                assigned_node,
                signature,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task.task_id,
                task.queue_name,
                task.task_type,
                json.dumps(
                    task.payload
                ),
                json.dumps(
                    list(
                        task.permissions
                    )
                ),
                task.created_at,
                task.priority,
                task.status.value,
                task.assigned_node,
                task.signature,
                json.dumps(
                    task.metadata
                ),
            ),
        )

    async def fetch_next(
        self,
        *,
        node_id: str,
        queue_name: str,
    ) -> Optional[
        DistributedTask
    ]:

        return await asyncio.to_thread(
            self._fetch_next_sync,
            node_id,
            queue_name,
        )

    def _fetch_next_sync(
        self,
        node_id: str,
        queue_name: str,
    ) -> Optional[
        DistributedTask
    ]:

        conn = self._connection

        conn.execute(
            "BEGIN IMMEDIATE"
        )

        try:
            cursor = conn.execute(
                """
                SELECT
                    task_id,
                    queue_name,
                    task_type,
                    payload,
                    permissions,
                    created_at,
                    priority,
                    status,
                    assigned_node,
                    signature,
                    metadata
                FROM distributed_tasks
                WHERE queue_name = ?
                AND status = ?
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
                """,
                (
                    queue_name,
                    TaskStatus.PENDING.value,
                ),
            )

            row = cursor.fetchone()

            if not row:
                conn.execute(
                    "COMMIT"
                )

                return None

            task_id = row[0]

            conn.execute(
                """
                UPDATE distributed_tasks
                SET status = ?,
                    assigned_node = ?
                WHERE task_id = ?
                """,
                (
                    TaskStatus.FETCHED.value,
                    node_id,
                    task_id,
                ),
            )

            conn.execute(
                "COMMIT"
            )

            return DistributedTask(
                task_id=row[0],
                queue_name=row[1],
                task_type=row[2],
                payload=json.loads(
                    row[3]
                ),
                permissions=set(
                    json.loads(
                        row[4]
                    )
                ),
                created_at=row[5],
                priority=row[6],
                status=TaskStatus(
                    row[7]
                ),
                assigned_node=row[8],
                signature=row[9],
                metadata=json.loads(
                    row[10]
                ),
            )

        except Exception:
            conn.execute(
                "ROLLBACK"
            )
            raise

    async def update_status(
        self,
        *,
        task_id: str,
        status: TaskStatus,
    ) -> None:

        await asyncio.to_thread(
            self._update_status_sync,
            task_id,
            status,
        )

    def _update_status_sync(
        self,
        task_id: str,
        status: TaskStatus,
    ) -> None:

        self._connection.execute(
            """
            UPDATE distributed_tasks
            SET status = ?
            WHERE task_id = ?
            """,
            (
                status.value,
                task_id,
            ),
        )

    async def save_result(
        self,
        result: TaskResult,
    ) -> None:

        await asyncio.to_thread(
            self._save_result_sync,
            result,
        )

    def _save_result_sync(
        self,
        result: TaskResult,
    ) -> None:

        self._connection.execute(
            """
            INSERT OR REPLACE INTO distributed_results (
                task_id,
                success,
                result,
                completed_at,
                error
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                result.task_id,
                1 if result.success else 0,
                json.dumps(
                    result.result
                ),
                result.completed_at,
                result.error,
            ),
        )


class PayloadSignatureValidator:
    """
    HMAC payload signature validator.
    """

    def __init__(
        self,
        cluster_secret: str,
    ) -> None:

        self.cluster_secret = (
            cluster_secret.encode(
                "utf-8"
            )
        )

    def sign_payload(
        self,
        payload: Dict[str, Any],
    ) -> str:

        serialized = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

        return hmac.new(
            self.cluster_secret,
            serialized,
            hashlib.sha256,
        ).hexdigest()

    def verify(
        self,
        *,
        payload: Dict[str, Any],
        signature: str,
    ) -> bool:

        expected = (
            self.sign_payload(
                payload
            )
        )

        return hmac.compare_digest(
            expected,
            signature,
        )


class TaskPermissionValidator:
    """
    Default Deny RBAC validator.
    """

    async def validate(
        self,
        *,
        task_permissions: Set[str],
        worker_permissions: Set[str],
    ) -> bool:

        if not task_permissions:
            return False

        return (
            task_permissions
            <= worker_permissions
        )


class RemoteExecutionEngine:
    """
    Async remote execution runtime.
    """

    def __init__(
        self,
    ) -> None:

        self._handlers: Dict[
            str,
            Callable[
                [Dict[str, Any]],
                Awaitable[
                    Dict[str, Any]
                ],
            ],
        ] = {}

    async def register_handler(
        self,
        *,
        task_type: str,
        handler: Callable[
            [Dict[str, Any]],
            Awaitable[
                Dict[str, Any]
            ],
        ],
    ) -> None:

        self._handlers[
            task_type
        ] = handler

    async def execute(
        self,
        *,
        task_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:

        handler = self._handlers.get(
            task_type
        )

        if not handler:
            raise RuntimeError(
                f"No handler registered for {task_type}"
            )

        return await handler(
            payload
        )


class TaskStateSynchronizer:
    """
    Distributed state synchronization.
    """

    def __init__(
        self,
    ) -> None:

        self._runtime_state: Dict[
            str,
            Dict[str, Any],
        ] = {}

    async def sync(
        self,
        *,
        task_id: str,
        state: Dict[str, Any],
    ) -> None:

        self._runtime_state[
            task_id
        ] = {
            "updated_at":
                time.time(),
            **state,
        }

    async def get_state(
        self,
        task_id: str,
    ) -> Optional[
        Dict[str, Any]
    ]:

        return self._runtime_state.get(
            task_id
        )


class DistributedQueueWorker:
    """
    Async-first distributed worker runtime.

    Features:
    - SQLite WAL distributed queue
    - FIFO task distribution
    - Remote execution engine
    - Cryptographic signature validation
    - Default Deny RBAC
    - Non-blocking execution
    """

    FETCH_INTERVAL = 2
    WAL_CHECKPOINT_INTERVAL = 1800

    def __init__(
        self,
        *,
        node_id: str,
        queue_name: str,
        cluster_secret: str,
        worker_permissions: Set[str],
        database_path: str = (
            "./data/distributed_queue.db"
        ),
    ) -> None:

        self.node_id = node_id
        self.queue_name = (
            queue_name
        )

        self.worker_permissions = (
            worker_permissions
        )

        self.queue = (
            SQLiteDistributedQueue(
                database_path
            )
        )

        self.signature_validator = (
            PayloadSignatureValidator(
                cluster_secret
            )
        )

        self.permission_validator = (
            TaskPermissionValidator()
        )

        self.execution_engine = (
            RemoteExecutionEngine()
        )

        self.state_sync = (
            TaskStateSynchronizer()
        )

        self._running = False

        self._worker_task: Optional[
            asyncio.Task
        ] = None

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting DistributedQueueWorker"
        )

        await self.queue.initialize()

        self._running = True

        self._worker_task = (
            asyncio.create_task(
                self._worker_loop()
            )
        )

        self._maintenance_task = (
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(
        self,
    ) -> None:

        logger.info(
            "Stopping DistributedQueueWorker"
        )

        self._running = False

        for task in (
            self._worker_task,
            self._maintenance_task,
        ):
            if task:
                task.cancel()

                with contextlib.suppress(
                    asyncio.CancelledError
                ):
                    await task

        await self.queue.close()

    async def enqueue_task(
        self,
        *,
        task_type: str,
        payload: Dict[str, Any],
        permissions: Set[str],
        priority: int = 100,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> str:

        signature = (
            self.signature_validator.sign_payload(
                payload
            )
        )

        task = DistributedTask(
            task_id=
                secrets.token_hex(16),
            queue_name=
                self.queue_name,
            task_type=
                task_type,
            payload=payload,
            permissions=
                permissions,
            created_at=
                time.time(),
            priority=priority,
            signature=
                signature,
            metadata=
                metadata or {},
        )

        await self.queue.enqueue(
            task
        )

        return task.task_id

    async def register_handler(
        self,
        *,
        task_type: str,
        handler: Callable[
            [Dict[str, Any]],
            Awaitable[
                Dict[str, Any]
            ],
        ],
    ) -> None:

        await self.execution_engine.register_handler(
            task_type=
                task_type,
            handler=handler,
        )

    async def _worker_loop(
        self,
    ) -> None:

        while self._running:
            try:
                task = (
                    await self.queue.fetch_next(
                        node_id=
                            self.node_id,
                        queue_name=
                            self.queue_name,
                    )
                )

                if not task:
                    await asyncio.sleep(
                        self.FETCH_INTERVAL
                    )
                    continue

                await self._process_task(
                    task
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.error(
                    traceback.format_exc()
                )

    async def _process_task(
        self,
        task: DistributedTask,
    ) -> None:

        try:
            valid_signature = (
                self.signature_validator.verify(
                    payload=
                        task.payload,
                    signature=
                        task.signature,
                )
            )

            if not valid_signature:
                raise PayloadSignatureError(
                    "Invalid payload signature"
                )

            authorized = (
                await self.permission_validator.validate(
                    task_permissions=
                        task.permissions,
                    worker_permissions=
                        self.worker_permissions,
                )
            )

            if not authorized:
                raise RBACValidationError(
                    "RBAC validation failed"
                )

            await self.queue.update_status(
                task_id=
                    task.task_id,
                status=
                    TaskStatus.RUNNING,
            )

            await self.state_sync.sync(
                task_id=
                    task.task_id,
                state={
                    "status":
                        "running",
                    "node":
                        self.node_id,
                },
            )

            result_payload = (
                await self.execution_engine.execute(
                    task_type=
                        task.task_type,
                    payload=
                        task.payload,
                )
            )

            result = TaskResult(
                task_id=
                    task.task_id,
                success=True,
                result=
                    result_payload,
                completed_at=
                    time.time(),
            )

            await self.queue.save_result(
                result
            )

            await self.queue.update_status(
                task_id=
                    task.task_id,
                status=
                    TaskStatus.COMPLETED,
            )

            await self.state_sync.sync(
                task_id=
                    task.task_id,
                state={
                    "status":
                        "completed",
                    "result":
                        result_payload,
                },
            )

        except Exception as exc:

            logger.error(
                "Distributed task failed: %s",
                exc,
            )

            result = TaskResult(
                task_id=
                    task.task_id,
                success=False,
                result={},
                completed_at=
                    time.time(),
                error=str(exc),
            )

            await self.queue.save_result(
                result
            )

            await self.queue.update_status(
                task_id=
                    task.task_id,
                status=
                    TaskStatus.FAILED,
            )

            await self.state_sync.sync(
                task_id=
                    task.task_id,
                state={
                    "status":
                        "failed",
                    "error":
                        str(exc),
                },
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

        self.queue._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:

        return {
            "node_id":
                self.node_id,
            "queue_name":
                self.queue_name,
            "worker_permissions":
                len(
                    self.worker_permissions
                ),
            "running":
                self._running,
            "timestamp":
                time.time(),
        }


DEFAULT_DISTRIBUTED_QUEUE_WORKER = (
    DistributedQueueWorker
)
