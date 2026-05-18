from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import logging
import os
import sqlite3
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Deque,
    Dict,
    Optional,
    Set,
)

from cryptography.fernet import (
    Fernet,
    InvalidToken,
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
class EncryptedStateRecord:
    state_id: str
    state_key: str
    encrypted_payload: bytes
    created_at: float
    updated_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class StateAccessResult:
    success: bool
    payload: Optional[
        Dict[str, Any]
    ]
    reason: Optional[str]
    timestamp: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class StateRBACValidator:
    """
    Default Deny + RBAC validator.
    """

    READ_PERMISSION = (
        "state.read"
    )

    WRITE_PERMISSION = (
        "state.write"
    )

    CORE_ROLES = {
        "core",
        "system",
    }

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        action: str,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:

        if not (
            roles
            & self.CORE_ROLES
        ):
            return False

        required = (
            self.READ_PERMISSION
            if action == "read"
            else self.WRITE_PERMISSION
        )

        if required not in permissions:
            return False

        context = RouteContext(
            requester_id=
                requester_id,
            requester_roles=
                roles,
            requester_permissions=
                permissions,
            task_type=
                f"encrypted_state.{action}",
            metadata=metadata or {},
        )

        route = await self.router.route(
            task=
                f"encrypted_state.{action}",
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class EnvironmentKeyProvider:
    """
    Environment-based master key.
    """

    ENV_NAME = (
        "TELEOPS_STATE_KEY"
    )

    @classmethod
    def load_key(
        cls,
    ) -> bytes:
        raw = os.getenv(
            cls.ENV_NAME
        )

        if not raw:
            raise RuntimeError(
                "Missing TELEOPS_STATE_KEY environment variable"
            )

        digest = hashlib.sha256(
            raw.encode("utf-8")
        ).digest()

        return base64.urlsafe_b64encode(
            digest
        )


class SecureStateEncryptor:
    """
    Symmetric encrypted state engine.
    """

    def __init__(
        self,
        *,
        master_key: bytes,
    ) -> None:
        self._fernet = Fernet(
            master_key
        )

    def encrypt(
        self,
        payload: Dict[str, Any],
    ) -> bytes:
        encoded = json.dumps(
            payload,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

        return self._fernet.encrypt(
            encoded
        )

    def decrypt(
        self,
        encrypted: bytes,
    ) -> Dict[str, Any]:
        try:
            raw = (
                self._fernet.decrypt(
                    encrypted
                )
            )

            return json.loads(
                raw.decode("utf-8")
            )

        except InvalidToken:
            raise ValueError(
                "Invalid encrypted state payload"
            )


class MemoryStateZeroingHook:
    """
    Memory overwrite mitigation.
    """

    @staticmethod
    def zero_bytes(
        buffer: bytearray,
    ) -> None:
        for index in range(
            len(buffer)
        ):
            buffer[index] = 0

    @classmethod
    def secure_cleanup(
        cls,
        payload: bytes,
    ) -> None:
        mutable = bytearray(
            payload
        )

        cls.zero_bytes(
            mutable
        )


class SQLiteEncryptedStateStore:
    """
    SQLite WAL encrypted state store.
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
            self._configure_database
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

    async def persist_state(
        self,
        record: EncryptedStateRecord,
    ) -> None:
        await asyncio.to_thread(
            self._insert_state,
            record,
        )

    async def load_state(
        self,
        state_key: str,
    ) -> Optional[
        EncryptedStateRecord
    ]:
        row = await asyncio.to_thread(
            self._load_state,
            state_key,
        )

        if not row:
            return None

        return EncryptedStateRecord(
            state_id=row[0],
            state_key=row[1],
            encrypted_payload=row[2],
            created_at=row[3],
            updated_at=row[4],
            metadata=json.loads(
                row[5]
            ),
        )

    async def delete_state(
        self,
        state_key: str,
    ) -> None:
        await asyncio.to_thread(
            self._delete_state,
            state_key,
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
            "PRAGMA cache_size=-1000;"
        )

        self._connection.execute(
            f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT};"
        )

    def _create_tables(
        self,
    ) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS encrypted_states (
                state_id TEXT PRIMARY KEY,
                state_key TEXT UNIQUE NOT NULL,
                encrypted_payload BLOB NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_state_key
            ON encrypted_states(state_key)
            """
        )

    def _insert_state(
        self,
        record: EncryptedStateRecord,
    ) -> None:
        self._connection.execute(
            """
            INSERT OR REPLACE INTO encrypted_states (
                state_id,
                state_key,
                encrypted_payload,
                created_at,
                updated_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                record.state_id,
                record.state_key,
                record.encrypted_payload,
                record.created_at,
                record.updated_at,
                json.dumps(
                    record.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _load_state(
        self,
        state_key: str,
    ) -> Optional[Any]:
        cursor = self._connection.execute(
            """
            SELECT
                state_id,
                state_key,
                encrypted_payload,
                created_at,
                updated_at,
                metadata
            FROM encrypted_states
            WHERE state_key = ?
            LIMIT 1
            """,
            (state_key,),
        )

        return cursor.fetchone()

    def _delete_state(
        self,
        state_key: str,
    ) -> None:
        self._connection.execute(
            """
            DELETE FROM encrypted_states
            WHERE state_key = ?
            """,
            (state_key,),
        )


class EncryptedRuntimeCache:
    """
    Encrypted in-memory cache.
    """

    CACHE_LIMIT = 128

    def __init__(
        self,
    ) -> None:
        self._cache: Dict[
            str,
            bytes,
        ] = {}

        self._timestamps: Dict[
            str,
            float,
        ] = {}

    def put(
        self,
        state_key: str,
        encrypted_payload: bytes,
    ) -> None:
        if (
            len(self._cache)
            >= self.CACHE_LIMIT
        ):
            oldest = min(
                self._timestamps,
                key=self._timestamps.get,
            )

            self.remove(
                oldest
            )

        self._cache[
            state_key
        ] = encrypted_payload

        self._timestamps[
            state_key
        ] = time.time()

    def get(
        self,
        state_key: str,
    ) -> Optional[bytes]:
        payload = self._cache.get(
            state_key
        )

        if payload:
            self._timestamps[
                state_key
            ] = time.time()

        return payload

    def remove(
        self,
        state_key: str,
    ) -> None:
        payload = self._cache.pop(
            state_key,
            None,
        )

        self._timestamps.pop(
            state_key,
            None,
        )

        if payload:
            MemoryStateZeroingHook.secure_cleanup(
                payload
            )

    def clear(
        self,
    ) -> None:
        for key in list(
            self._cache.keys()
        ):
            self.remove(
                key
            )


class AsyncStorageSyncBridge:
    """
    Async SQLite sync bridge.
    """

    def __init__(
        self,
        *,
        store: SQLiteEncryptedStateStore,
    ) -> None:
        self.store = store

        self._queue: asyncio.Queue[
            EncryptedStateRecord
        ] = asyncio.Queue(
            maxsize=512
        )

        self._worker: Optional[
            asyncio.Task
        ] = None

        self._running = False

    async def start(
        self,
    ) -> None:
        self._running = True

        self._worker = (
            asyncio.create_task(
                self._sync_loop()
            )
        )

    async def stop(
        self,
    ) -> None:
        self._running = False

        if self._worker:
            self._worker.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._worker

    async def enqueue(
        self,
        record: EncryptedStateRecord,
    ) -> None:
        await self._queue.put(
            record
        )

    async def _sync_loop(
        self,
    ) -> None:
        while self._running:
            try:
                record = (
                    await self._queue.get()
                )

                await self.store.persist_state(
                    record
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Encrypted state sync failure"
                )


class EncryptedOperationalStateEngine:
    """
    Async-first encrypted operational state engine.

    Features:
    - Runtime encrypted memory states
    - SQLite WAL encrypted persistence
    - Memory zeroization hooks
    - RBAC state-level enforcement
    - Async synchronization
    - Default Deny architecture
    """

    CLEANUP_INTERVAL = 1800

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: Optional[
            MessageBus
        ] = None,
        database_path: str = (
            "./data/encrypted_state.db"
        ),
    ) -> None:
        self.router = router

        self.message_bus = (
            message_bus
        )

        self._validator = (
            StateRBACValidator(
                router
            )
        )

        self._encryptor = (
            SecureStateEncryptor(
                master_key=
                    EnvironmentKeyProvider.load_key()
            )
        )

        self._store = (
            SQLiteEncryptedStateStore(
                database_path=
                    database_path
            )
        )

        self._cache = (
            EncryptedRuntimeCache()
        )

        self._sync = (
            AsyncStorageSyncBridge(
                store=self._store
            )
        )

        self._running = False

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

        self._access_counter = 0

        self._denied_counter = 0

        self._state_history: Deque[
            str
        ] = deque(maxlen=128)

    async def start(
        self,
    ) -> None:
        logger.info(
            "Starting EncryptedOperationalStateEngine"
        )

        await self._store.initialize()

        await self._sync.start()

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
            "Stopping EncryptedOperationalStateEngine"
        )

        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._maintenance_task

        await self._sync.stop()

        self._cache.clear()

        await self._store.close()

    async def set_state(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        state_key: str,
        payload: Dict[str, Any],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:
        """
        Encrypted state persistence.
        """

        allowed = (
            await self._validator.validate(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                roles=roles,
                action="write",
                metadata=metadata,
            )
        )

        if not allowed:
            self._denied_counter += 1

            await self._emit_alert(
                requester_id=
                    requester_id,
                action=
                    "state.write.denied",
                state_key=
                    state_key,
            )

            return False

        encrypted = (
            self._encryptor.encrypt(
                payload
            )
        )

        record = (
            EncryptedStateRecord(
                state_id=
                    uuid.uuid4().hex,
                state_key=
                    state_key,
                encrypted_payload=
                    encrypted,
                created_at=
                    time.time(),
                updated_at=
                    time.time(),
                metadata=
                    metadata or {},
            )
        )

        self._cache.put(
            state_key,
            encrypted,
        )

        await self._sync.enqueue(
            record
        )

        self._state_history.append(
            state_key
        )

        return True

    async def get_state(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        state_key: str,
    ) -> StateAccessResult:
        """
        Secure encrypted state retrieval.
        """

        allowed = (
            await self._validator.validate(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                roles=roles,
                action="read",
                metadata={
                    "state_key":
                        state_key
                },
            )
        )

        if not allowed:
            self._denied_counter += 1

            await self._emit_alert(
                requester_id=
                    requester_id,
                action=
                    "state.read.denied",
                state_key=
                    state_key,
            )

            return StateAccessResult(
                success=False,
                payload=None,
                reason=
                    "Access denied by RBAC",
                timestamp=
                    time.time(),
            )

        encrypted = (
            self._cache.get(
                state_key
            )
        )

        if not encrypted:
            record = (
                await self._store.load_state(
                    state_key
                )
            )

            if not record:
                return StateAccessResult(
                    success=False,
                    payload=None,
                    reason=
                        "State not found",
                    timestamp=
                        time.time(),
                )

            encrypted = (
                record.encrypted_payload
            )

            self._cache.put(
                state_key,
                encrypted,
            )

        payload = (
            self._encryptor.decrypt(
                encrypted
            )
        )

        self._access_counter += 1

        return StateAccessResult(
            success=True,
            payload=payload,
            reason=None,
            timestamp=
                time.time(),
            metadata={
                "state_key":
                    state_key
            },
        )

    async def delete_state(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        state_key: str,
    ) -> bool:
        allowed = (
            await self._validator.validate(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                roles=roles,
                action="write",
                metadata={
                    "state_key":
                        state_key
                },
            )
        )

        if not allowed:
            self._denied_counter += 1

            return False

        self._cache.remove(
            state_key
        )

        await self._store.delete_state(
            state_key
        )

        return True

    async def _emit_alert(
        self,
        *,
        requester_id: str,
        action: str,
        state_key: str,
    ) -> None:
        if not self.message_bus:
            return

        payload = {
            "type":
                "encrypted_state_violation",
            "requester_id":
                requester_id,
            "action":
                action,
            "state_key":
                state_key,
            "timestamp":
                time.time(),
        }

        await self.message_bus.publish(
            topic="security.alert",
            payload=payload,
        )

    async def _maintenance_loop(
        self,
    ) -> None:
        while self._running:
            try:
                await asyncio.sleep(
                    self.CLEANUP_INTERVAL
                )

                self._cache.clear()

                await asyncio.to_thread(
                    self._wal_checkpoint
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Encrypted state maintenance failure"
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
            "state_reads":
                self._access_counter,
            "denied_requests":
                self._denied_counter,
            "cached_states":
                len(
                    self._cache._cache
                ),
            "history_size":
                len(
                    self._state_history
                ),
            "timestamp":
                time.time(),
        }


DEFAULT_ENCRYPTED_STATE_ENGINE = (
    EncryptedOperationalStateEngine
)
