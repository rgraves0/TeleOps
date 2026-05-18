from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import sqlite3
import time
import traceback
import uuid
import zlib
from collections import deque
from dataclasses import (
    dataclass,
    field,
)
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


@dataclass(slots=True)
class RuntimeSnapshot:
    snapshot_id: str
    workflow_id: str
    owner_id: str
    component: str
    compressed_payload: bytes
    checksum: str
    permissions: List[str]
    roles: List[str]
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class RecoveryResult:
    success: bool
    workflow_id: str
    restored_payload: Optional[
        Dict[str, Any]
    ]
    reason: Optional[str]
    restored_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class ContextPermissionValidator:
    """
    Default Deny + RBAC validator.
    """

    REQUIRED_PERMISSION = (
        "snapshot.recover"
    )

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate(
        self,
        *,
        requester_id: str,
        workflow_owner: str,
        permissions: Set[str],
        roles: Set[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:

        if (
            self.REQUIRED_PERMISSION
            not in permissions
        ):
            return False

        if (
            requester_id
            != workflow_owner
            and "admin"
            not in roles
        ):
            return False

        context = RouteContext(
            requester_id=
                requester_id,
            requester_roles=
                roles,
            requester_permissions=
                permissions,
            task_type=
                "snapshot.recover",
            metadata=metadata or {},
        )

        route = await self.router.route(
            task=
                "snapshot.recover",
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class StateSerializer:
    """
    Lightweight compressed serializer.
    """

    COMPRESSION_LEVEL = 6

    @classmethod
    def serialize(
        cls,
        payload: Dict[str, Any],
    ) -> bytes:

        encoded = json.dumps(
            payload,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

        return zlib.compress(
            encoded,
            cls.COMPRESSION_LEVEL,
        )

    @classmethod
    def deserialize(
        cls,
        payload: bytes,
    ) -> Dict[str, Any]:

        decompressed = (
            zlib.decompress(
                payload
            )
        )

        return json.loads(
            decompressed.decode(
                "utf-8"
            )
        )

    @classmethod
    def checksum(
        cls,
        payload: bytes,
    ) -> str:
        return hashlib.sha256(
            payload
        ).hexdigest()


class SnapshotSQLiteStore:
    """
    SQLite WAL snapshot persistence.
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

    async def persist_snapshot(
        self,
        snapshot: RuntimeSnapshot,
    ) -> None:

        await asyncio.to_thread(
            self._persist_snapshot,
            snapshot,
        )

    async def latest_snapshot(
        self,
        workflow_id: str,
    ) -> Optional[
        RuntimeSnapshot
    ]:

        row = await asyncio.to_thread(
            self._latest_snapshot,
            workflow_id,
        )

        if not row:
            return None

        return RuntimeSnapshot(
            snapshot_id=row[0],
            workflow_id=row[1],
            owner_id=row[2],
            component=row[3],
            compressed_payload=row[4],
            checksum=row[5],
            permissions=json.loads(
                row[6]
            ),
            roles=json.loads(
                row[7]
            ),
            created_at=row[8],
            metadata=json.loads(
                row[9]
            ),
        )

    async def all_latest_snapshots(
        self,
    ) -> List[
        RuntimeSnapshot
    ]:

        rows = await asyncio.to_thread(
            self._all_latest_snapshots
        )

        snapshots: List[
            RuntimeSnapshot
        ] = []

        for row in rows:
            snapshots.append(
                RuntimeSnapshot(
                    snapshot_id=row[0],
                    workflow_id=row[1],
                    owner_id=row[2],
                    component=row[3],
                    compressed_payload=row[4],
                    checksum=row[5],
                    permissions=json.loads(
                        row[6]
                    ),
                    roles=json.loads(
                        row[7]
                    ),
                    created_at=row[8],
                    metadata=json.loads(
                        row[9]
                    ),
                )
            )

        return snapshots

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
            CREATE TABLE IF NOT EXISTS runtime_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                component TEXT NOT NULL,
                compressed_payload BLOB NOT NULL,
                checksum TEXT NOT NULL,
                permissions TEXT NOT NULL,
                roles TEXT NOT NULL,
                created_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_snapshot_workflow
            ON runtime_snapshots(workflow_id, created_at)
            """
        )

    def _persist_snapshot(
        self,
        snapshot: RuntimeSnapshot,
    ) -> None:

        self._connection.execute(
            """
            INSERT INTO runtime_snapshots (
                snapshot_id,
                workflow_id,
                owner_id,
                component,
                compressed_payload,
                checksum,
                permissions,
                roles,
                created_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.snapshot_id,
                snapshot.workflow_id,
                snapshot.owner_id,
                snapshot.component,
                snapshot.compressed_payload,
                snapshot.checksum,
                json.dumps(
                    snapshot.permissions
                ),
                json.dumps(
                    snapshot.roles
                ),
                snapshot.created_at,
                json.dumps(
                    snapshot.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _latest_snapshot(
        self,
        workflow_id: str,
    ) -> Optional[Any]:

        cursor = self._connection.execute(
            """
            SELECT
                snapshot_id,
                workflow_id,
                owner_id,
                component,
                compressed_payload,
                checksum,
                permissions,
                roles,
                created_at,
                metadata
            FROM runtime_snapshots
            WHERE workflow_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (workflow_id,),
        )

        return cursor.fetchone()

    def _all_latest_snapshots(
        self,
    ) -> List[Any]:

        cursor = self._connection.execute(
            """
            SELECT rs1.*
            FROM runtime_snapshots rs1
            INNER JOIN (
                SELECT workflow_id,
                MAX(created_at) AS max_created
                FROM runtime_snapshots
                GROUP BY workflow_id
            ) rs2
            ON rs1.workflow_id = rs2.workflow_id
            AND rs1.created_at = rs2.max_created
            """
        )

        return cursor.fetchall()


class RuntimeSnapshotEngine:
    """
    Runtime snapshot taker.
    """

    def __init__(
        self,
        *,
        snapshot_store: SnapshotSQLiteStore,
    ) -> None:

        self.snapshot_store = (
            snapshot_store
        )

        self._snapshot_counter = 0

    async def snapshot(
        self,
        *,
        workflow_id: str,
        owner_id: str,
        component: str,
        payload: Dict[str, Any],
        permissions: Set[str],
        roles: Set[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> RuntimeSnapshot:

        compressed = (
            StateSerializer.serialize(
                payload
            )
        )

        checksum = (
            StateSerializer.checksum(
                compressed
            )
        )

        snapshot = RuntimeSnapshot(
            snapshot_id=
                uuid.uuid4().hex,
            workflow_id=
                workflow_id,
            owner_id=
                owner_id,
            component=
                component,
            compressed_payload=
                compressed,
            checksum=
                checksum,
            permissions=list(
                permissions
            ),
            roles=list(
                roles
            ),
            created_at=
                time.time(),
            metadata=
                metadata or {},
        )

        await self.snapshot_store.persist_snapshot(
            snapshot
        )

        self._snapshot_counter += 1

        return snapshot

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "snapshots":
                self._snapshot_counter
        }


class CrashRecoveryManager:
    """
    Point-in-time crash recovery.
    """

    def __init__(
        self,
        *,
        snapshot_store: SnapshotSQLiteStore,
        validator: ContextPermissionValidator,
        message_bus: Optional[
            MessageBus
        ] = None,
    ) -> None:

        self.snapshot_store = (
            snapshot_store
        )

        self.validator = (
            validator
        )

        self.message_bus = (
            message_bus
        )

        self._recovery_counter = 0

        self._failed_recoveries = 0

        self._recent_recoveries: Deque[
            str
        ] = deque(maxlen=128)

    async def recover_workflow(
        self,
        *,
        workflow_id: str,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
    ) -> RecoveryResult:

        snapshot = (
            await self.snapshot_store.latest_snapshot(
                workflow_id
            )
        )

        if not snapshot:
            return RecoveryResult(
                success=False,
                workflow_id=
                    workflow_id,
                restored_payload=
                    None,
                reason=
                    "Snapshot not found",
                restored_at=
                    time.time(),
            )

        allowed = (
            await self.validator.validate(
                requester_id=
                    requester_id,
                workflow_owner=
                    snapshot.owner_id,
                permissions=
                    permissions,
                roles=roles,
                metadata={
                    "workflow_id":
                        workflow_id
                },
            )
        )

        if not allowed:
            self._failed_recoveries += 1

            await self._emit_alert(
                "snapshot_recovery_denied",
                {
                    "workflow_id":
                        workflow_id,
                    "requester_id":
                        requester_id,
                },
            )

            return RecoveryResult(
                success=False,
                workflow_id=
                    workflow_id,
                restored_payload=
                    None,
                reason=
                    "RBAC denied",
                restored_at=
                    time.time(),
            )

        checksum = (
            StateSerializer.checksum(
                snapshot.compressed_payload
            )
        )

        if (
            checksum
            != snapshot.checksum
        ):
            self._failed_recoveries += 1

            return RecoveryResult(
                success=False,
                workflow_id=
                    workflow_id,
                restored_payload=
                    None,
                reason=
                    "Snapshot checksum mismatch",
                restored_at=
                    time.time(),
            )

        try:
            restored = (
                StateSerializer.deserialize(
                    snapshot.compressed_payload
                )
            )

            self._recovery_counter += 1

            self._recent_recoveries.append(
                workflow_id
            )

            await self._emit_alert(
                "workflow_recovered",
                {
                    "workflow_id":
                        workflow_id,
                    "component":
                        snapshot.component,
                },
            )

            return RecoveryResult(
                success=True,
                workflow_id=
                    workflow_id,
                restored_payload=
                    restored,
                reason=None,
                restored_at=
                    time.time(),
                metadata={
                    "snapshot_id":
                        snapshot.snapshot_id
                },
            )

        except Exception as exc:
            self._failed_recoveries += 1

            logger.exception(
                "Recovery deserialization failure"
            )

            return RecoveryResult(
                success=False,
                workflow_id=
                    workflow_id,
                restored_payload=
                    None,
                reason=str(
                    exc
                ),
                restored_at=
                    time.time(),
            )

    async def boot_recovery(
        self,
    ) -> List[
        RecoveryResult
    ]:
        """
        Boot-time recovery scan.
        """

        results: List[
            RecoveryResult
        ] = []

        snapshots = (
            await self.snapshot_store.all_latest_snapshots()
        )

        for snapshot in snapshots:
            try:
                restored = (
                    StateSerializer.deserialize(
                        snapshot.compressed_payload
                    )
                )

                results.append(
                    RecoveryResult(
                        success=True,
                        workflow_id=
                            snapshot.workflow_id,
                        restored_payload=
                            restored,
                        reason=None,
                        restored_at=
                            time.time(),
                    )
                )

            except Exception:
                logger.exception(
                    "Boot recovery failure"
                )

                results.append(
                    RecoveryResult(
                        success=False,
                        workflow_id=
                            snapshot.workflow_id,
                        restored_payload=
                            None,
                        reason=
                            "Corrupted snapshot",
                        restored_at=
                            time.time(),
                    )
                )

        return results

    async def _emit_alert(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> None:

        if not self.message_bus:
            return

        await self.message_bus.publish(
            topic=
                "recovery.events",
            payload={
                "type":
                    event_type,
                "timestamp":
                    time.time(),
                **payload,
            },
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "recoveries":
                self._recovery_counter,
            "failed_recoveries":
                self._failed_recoveries,
            "recent_recoveries":
                len(
                    self._recent_recoveries
                ),
        }


class CrashRecoveryRuntime:
    """
    Async-first crash recovery runtime.

    Features:
    - Runtime snapshots
    - Point-in-time recovery
    - Compressed serialization
    - SQLite WAL persistence
    - RBAC recovery validation
    - Boot-time restoration
    """

    SNAPSHOT_INTERVAL = 120
    MAINTENANCE_INTERVAL = 1800

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: Optional[
            MessageBus
        ] = None,
        database_path: str = (
            "./data/runtime_snapshots.db"
        ),
    ) -> None:

        self.router = router

        self.message_bus = (
            message_bus
        )

        self._store = (
            SnapshotSQLiteStore(
                database_path=
                    database_path
            )
        )

        self._validator = (
            ContextPermissionValidator(
                router
            )
        )

        self._snapshot_engine = (
            RuntimeSnapshotEngine(
                snapshot_store=
                    self._store
            )
        )

        self._recovery_manager = (
            CrashRecoveryManager(
                snapshot_store=
                    self._store,
                validator=
                    self._validator,
                message_bus=
                    message_bus,
            )
        )

        self._running = False

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting CrashRecoveryRuntime"
        )

        await self._store.initialize()

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
            "Stopping CrashRecoveryRuntime"
        )

        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._maintenance_task

        await self._store.close()

    async def create_snapshot(
        self,
        *,
        workflow_id: str,
        owner_id: str,
        component: str,
        payload: Dict[str, Any],
        permissions: Set[str],
        roles: Set[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> RuntimeSnapshot:

        return await self._snapshot_engine.snapshot(
            workflow_id=
                workflow_id,
            owner_id=
                owner_id,
            component=
                component,
            payload=
                payload,
            permissions=
                permissions,
            roles=roles,
            metadata=
                metadata,
        )

    async def recover(
        self,
        *,
        workflow_id: str,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
    ) -> RecoveryResult:

        return await self._recovery_manager.recover_workflow(
            workflow_id=
                workflow_id,
            requester_id=
                requester_id,
            permissions=
                permissions,
            roles=roles,
        )

    async def boot_restore(
        self,
    ) -> List[
        RecoveryResult
    ]:

        return await self._recovery_manager.boot_recovery()

    async def _maintenance_loop(
        self,
    ) -> None:

        while self._running:
            try:
                await asyncio.sleep(
                    self.MAINTENANCE_INTERVAL
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

        self._store._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "running":
                self._running,
            "snapshot_engine":
                self._snapshot_engine.stats(),
            "recovery_manager":
                self._recovery_manager.stats(),
            "timestamp":
                time.time(),
        }


DEFAULT_CRASH_RECOVERY = (
    CrashRecoveryRuntime
)
