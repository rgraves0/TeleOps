from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import hmac
import json
import logging
import os
import secrets
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
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
class SecretRecord:
    secret_id: str
    secret_name: str
    encrypted_payload: bytes
    created_at: float
    updated_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class SecretAccessResult:
    success: bool
    secret_value: Optional[str]
    reason: Optional[str]
    timestamp: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class SecretRBACValidator:
    """
    Default Deny + RBAC enforcement.
    """

    REQUIRED_PERMISSION = (
        "secret.read"
    )

    ADMIN_PERMISSION = (
        "secret.admin"
    )

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate_read(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:
        if (
            self.REQUIRED_PERMISSION
            not in permissions
        ):
            return False

        context = RouteContext(
            requester_id=
                requester_id,
            requester_roles={
                "system"
            },
            requester_permissions=
                permissions,
            task_type=
                "secret.read",
            metadata=metadata or {},
        )

        route = await self.router.route(
            task="secret.read",
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )

    async def validate_write(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:
        if (
            self.ADMIN_PERMISSION
            not in permissions
        ):
            return False

        context = RouteContext(
            requester_id=
                requester_id,
            requester_roles={
                "system"
            },
            requester_permissions=
                permissions,
            task_type=
                "secret.write",
            metadata=metadata or {},
        )

        route = await self.router.route(
            task="secret.write",
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class EnvironmentKeyExtractor:
    """
    Master key loader.
    """

    ENV_KEY_NAME = (
        "TELEOPS_MASTER_KEY"
    )

    @classmethod
    def load_key(
        cls,
    ) -> bytes:
        raw = os.getenv(
            cls.ENV_KEY_NAME
        )

        if not raw:
            raise RuntimeError(
                "Missing TELEOPS_MASTER_KEY environment variable"
            )

        digest = hashlib.sha256(
            raw.encode("utf-8")
        ).digest()

        return base64.urlsafe_b64encode(
            digest
        )


class CryptoPayloadCipher:
    """
    Fernet symmetric encryption wrapper.
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
        payload: str,
    ) -> bytes:
        return self._fernet.encrypt(
            payload.encode("utf-8")
        )

    def decrypt(
        self,
        encrypted: bytes,
    ) -> str:
        try:
            decrypted = (
                self._fernet.decrypt(
                    encrypted
                )
            )

            return decrypted.decode(
                "utf-8"
            )

        except InvalidToken:
            raise ValueError(
                "Invalid encrypted payload"
            )


class MemoryOverwriteMitigator:
    """
    In-memory secret zeroization.
    """

    @staticmethod
    def secure_zero(
        buffer: bytearray,
    ) -> None:
        for index in range(
            len(buffer)
        ):
            buffer[index] = 0

    @classmethod
    def secure_string_cleanup(
        cls,
        secret: str,
    ) -> None:
        mutable = bytearray(
            secret.encode("utf-8")
        )

        cls.secure_zero(
            mutable
        )


class SQLiteSecretsStore:
    """
    SQLite WAL encrypted secrets storage.
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

    async def store_secret(
        self,
        record: SecretRecord,
    ) -> None:
        await asyncio.to_thread(
            self._insert_secret,
            record,
        )

    async def load_secret(
        self,
        secret_name: str,
    ) -> Optional[
        SecretRecord
    ]:
        row = await asyncio.to_thread(
            self._load_secret,
            secret_name,
        )

        if not row:
            return None

        return SecretRecord(
            secret_id=row[0],
            secret_name=row[1],
            encrypted_payload=row[2],
            created_at=row[3],
            updated_at=row[4],
            metadata=json.loads(
                row[5]
            ),
        )

    async def delete_secret(
        self,
        secret_name: str,
    ) -> None:
        await asyncio.to_thread(
            self._delete_secret,
            secret_name,
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
            CREATE TABLE IF NOT EXISTS secrets (
                secret_id TEXT PRIMARY KEY,
                secret_name TEXT UNIQUE NOT NULL,
                encrypted_payload BLOB NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_secret_name
            ON secrets(secret_name)
            """
        )

    def _insert_secret(
        self,
        record: SecretRecord,
    ) -> None:
        self._connection.execute(
            """
            INSERT OR REPLACE INTO secrets (
                secret_id,
                secret_name,
                encrypted_payload,
                created_at,
                updated_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                record.secret_id,
                record.secret_name,
                record.encrypted_payload,
                record.created_at,
                record.updated_at,
                json.dumps(
                    record.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _load_secret(
        self,
        secret_name: str,
    ) -> Optional[Any]:
        cursor = self._connection.execute(
            """
            SELECT
                secret_id,
                secret_name,
                encrypted_payload,
                created_at,
                updated_at,
                metadata
            FROM secrets
            WHERE secret_name = ?
            LIMIT 1
            """,
            (secret_name,),
        )

        return cursor.fetchone()

    def _delete_secret(
        self,
        secret_name: str,
    ) -> None:
        self._connection.execute(
            """
            DELETE FROM secrets
            WHERE secret_name = ?
            """,
            (secret_name,),
        )


class InMemorySecretManager:
    """
    Lightweight low-latency cache.
    """

    CACHE_LIMIT = 64

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

    def store(
        self,
        secret_name: str,
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

            self.destroy(
                oldest
            )

        self._cache[
            secret_name
        ] = encrypted_payload

        self._timestamps[
            secret_name
        ] = time.time()

    def load(
        self,
        secret_name: str,
    ) -> Optional[bytes]:
        payload = self._cache.get(
            secret_name
        )

        if payload:
            self._timestamps[
                secret_name
            ] = time.time()

        return payload

    def destroy(
        self,
        secret_name: str,
    ) -> None:
        payload = self._cache.pop(
            secret_name,
            None,
        )

        self._timestamps.pop(
            secret_name,
            None,
        )

        if payload:
            mutable = bytearray(
                payload
            )

            MemoryOverwriteMitigator.secure_zero(
                mutable
            )

    def clear(
        self,
    ) -> None:
        for key in list(
            self._cache.keys()
        ):
            self.destroy(
                key
            )


class SecureVaultEngine:
    """
    Async-first Secrets Protection Engine.

    Features:
    - Fernet encrypted secrets
    - Memory overwrite mitigation
    - SQLite WAL secure storage
    - Strict RBAC enforcement
    - Default Deny access
    - Low-latency encrypted cache
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
            "./data/secrets_vault.db"
        ),
    ) -> None:
        self.router = router

        self.message_bus = (
            message_bus
        )

        self._rbac = (
            SecretRBACValidator(
                router
            )
        )

        self._cipher = (
            CryptoPayloadCipher(
                master_key=
                    EnvironmentKeyExtractor.load_key()
            )
        )

        self._store = (
            SQLiteSecretsStore(
                database_path=
                    database_path
            )
        )

        self._memory = (
            InMemorySecretManager()
        )

        self._running = False

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

        self._access_counter = 0

        self._denied_counter = 0

    async def start(
        self,
    ) -> None:
        logger.info(
            "Starting SecureVaultEngine"
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
            "Stopping SecureVaultEngine"
        )

        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._maintenance_task

        self._memory.clear()

        await self._store.close()

    async def store_secret(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        secret_name: str,
        secret_value: str,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:
        """
        Encrypted secret persistence.
        """

        allowed = (
            await self._rbac.validate_write(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                metadata=
                    metadata,
            )
        )

        if not allowed:
            self._denied_counter += 1

            await self._emit_alert(
                requester_id=
                    requester_id,
                action=
                    "secret.write.denied",
                secret_name=
                    secret_name,
            )

            return False

        encrypted = (
            self._cipher.encrypt(
                secret_value
            )
        )

        record = SecretRecord(
            secret_id=
                uuid.uuid4().hex,
            secret_name=
                secret_name,
            encrypted_payload=
                encrypted,
            created_at=
                time.time(),
            updated_at=
                time.time(),
            metadata=
                metadata or {},
        )

        await self._store.store_secret(
            record
        )

        self._memory.store(
            secret_name,
            encrypted,
        )

        MemoryOverwriteMitigator.secure_string_cleanup(
            secret_value
        )

        return True

    async def read_secret(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        secret_name: str,
    ) -> SecretAccessResult:
        """
        Secure secret retrieval.
        """

        allowed = (
            await self._rbac.validate_read(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                metadata={
                    "secret_name":
                        secret_name
                },
            )
        )

        if not allowed:
            self._denied_counter += 1

            await self._emit_alert(
                requester_id=
                    requester_id,
                action=
                    "secret.read.denied",
                secret_name=
                    secret_name,
            )

            return SecretAccessResult(
                success=False,
                secret_value=None,
                reason=
                    "Access denied by RBAC",
                timestamp=
                    time.time(),
            )

        encrypted = (
            self._memory.load(
                secret_name
            )
        )

        if not encrypted:
            record = (
                await self._store.load_secret(
                    secret_name
                )
            )

            if not record:
                return SecretAccessResult(
                    success=False,
                    secret_value=None,
                    reason=
                        "Secret not found",
                    timestamp=
                        time.time(),
                )

            encrypted = (
                record.encrypted_payload
            )

            self._memory.store(
                secret_name,
                encrypted,
            )

        secret = (
            self._cipher.decrypt(
                encrypted
            )
        )

        self._access_counter += 1

        return SecretAccessResult(
            success=True,
            secret_value=
                secret,
            reason=None,
            timestamp=
                time.time(),
            metadata={
                "secret_name":
                    secret_name
            },
        )

    async def delete_secret(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        secret_name: str,
    ) -> bool:
        allowed = (
            await self._rbac.validate_write(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                metadata={
                    "secret_name":
                        secret_name
                },
            )
        )

        if not allowed:
            self._denied_counter += 1

            return False

        self._memory.destroy(
            secret_name
        )

        await self._store.delete_secret(
            secret_name
        )

        return True

    async def _emit_alert(
        self,
        *,
        requester_id: str,
        action: str,
        secret_name: str,
    ) -> None:
        if not self.message_bus:
            return

        payload = {
            "type":
                "secret_access_violation",
            "requester_id":
                requester_id,
            "action":
                action,
            "secret_name":
                secret_name,
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

                self._memory.clear()

                await asyncio.to_thread(
                    self._wal_checkpoint
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Secrets vault maintenance failure"
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
            "secret_reads":
                self._access_counter,
            "denied_requests":
                self._denied_counter,
            "cache_size":
                len(
                    self._memory._cache
                ),
            "timestamp":
                time.time(),
        }


DEFAULT_SECRETS_VAULT = (
    SecureVaultEngine
)
