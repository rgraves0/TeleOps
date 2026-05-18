from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
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
    List,
    Optional,
)

from app.core.message_bus import (
    MessageBus,
)


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AuditEvent:
    event_id: str
    event_type: str
    actor_id: str
    resource: str
    action: str
    status: str
    timestamp: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class AuditRecord:
    record_id: str
    event_id: str
    chain_hash: str
    previous_hash: str
    payload_hash: str
    event_type: str
    actor_id: str
    resource: str
    action: str
    status: str
    timestamp: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class SQLiteAuditStore:
    """
    SQLite WAL tamper-evident audit storage.
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

    async def write_record(
        self,
        record: AuditRecord,
    ) -> None:
        await asyncio.to_thread(
            self._insert_record,
            record,
        )

    async def fetch_last_hash(
        self,
    ) -> str:
        return await asyncio.to_thread(
            self._fetch_last_hash
        )

    async def fetch_recent_records(
        self,
        *,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self._fetch_recent_records,
            limit,
        )

    async def fetch_all_records(
        self,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self._fetch_all_records
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
            CREATE TABLE IF NOT EXISTS audit_logs (
                record_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                chain_hash TEXT NOT NULL,
                previous_hash TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                resource TEXT NOT NULL,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                timestamp REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_audit_timestamp
            ON audit_logs(timestamp)
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_audit_actor
            ON audit_logs(actor_id)
            """
        )

    def _insert_record(
        self,
        record: AuditRecord,
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO audit_logs (
                record_id,
                event_id,
                chain_hash,
                previous_hash,
                payload_hash,
                event_type,
                actor_id,
                resource,
                action,
                status,
                timestamp,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.record_id,
                record.event_id,
                record.chain_hash,
                record.previous_hash,
                record.payload_hash,
                record.event_type,
                record.actor_id,
                record.resource,
                record.action,
                record.status,
                record.timestamp,
                json.dumps(
                    record.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _fetch_last_hash(
        self,
    ) -> str:
        cursor = self._connection.execute(
            """
            SELECT chain_hash
            FROM audit_logs
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )

        row = cursor.fetchone()

        if not row:
            return "GENESIS"

        return row[0]

    def _fetch_recent_records(
        self,
        limit: int,
    ) -> List[Dict[str, Any]]:
        cursor = self._connection.execute(
            """
            SELECT
                record_id,
                event_id,
                chain_hash,
                previous_hash,
                payload_hash,
                event_type,
                actor_id,
                resource,
                action,
                status,
                timestamp,
                metadata
            FROM audit_logs
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (limit,),
        )

        rows = cursor.fetchall()

        return [
            {
                "record_id":
                    row[0],
                "event_id":
                    row[1],
                "chain_hash":
                    row[2],
                "previous_hash":
                    row[3],
                "payload_hash":
                    row[4],
                "event_type":
                    row[5],
                "actor_id":
                    row[6],
                "resource":
                    row[7],
                "action":
                    row[8],
                "status":
                    row[9],
                "timestamp":
                    row[10],
                "metadata":
                    json.loads(
                        row[11]
                    ),
            }
            for row in rows
        ]

    def _fetch_all_records(
        self,
    ) -> List[Dict[str, Any]]:
        cursor = self._connection.execute(
            """
            SELECT
                record_id,
                event_id,
                chain_hash,
                previous_hash,
                payload_hash,
                event_type,
                actor_id,
                resource,
                action,
                status,
                timestamp,
                metadata
            FROM audit_logs
            ORDER BY timestamp ASC
            """
        )

        rows = cursor.fetchall()

        return [
            {
                "record_id":
                    row[0],
                "event_id":
                    row[1],
                "chain_hash":
                    row[2],
                "previous_hash":
                    row[3],
                "payload_hash":
                    row[4],
                "event_type":
                    row[5],
                "actor_id":
                    row[6],
                "resource":
                    row[7],
                "action":
                    row[8],
                "status":
                    row[9],
                "timestamp":
                    row[10],
                "metadata":
                    json.loads(
                        row[11]
                    ),
            }
            for row in rows
        ]


class CryptographicHashChain:
    """
    SHA256 tamper-evident chain.
    """

    @staticmethod
    def build_payload_hash(
        event: AuditEvent,
    ) -> str:
        payload = {
            "event_id":
                event.event_id,
            "event_type":
                event.event_type,
            "actor_id":
                event.actor_id,
            "resource":
                event.resource,
            "action":
                event.action,
            "status":
                event.status,
            "timestamp":
                event.timestamp,
            "metadata":
                event.metadata,
        }

        encoded = json.dumps(
            payload,
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")

        return hashlib.sha256(
            encoded
        ).hexdigest()

    @staticmethod
    def build_chain_hash(
        *,
        previous_hash: str,
        payload_hash: str,
    ) -> str:
        combined = (
            f"{previous_hash}:{payload_hash}"
        ).encode("utf-8")

        return hashlib.sha256(
            combined
        ).hexdigest()


class LogVerificationUtility:
    """
    Tamper detection verifier.
    """

    @staticmethod
    def verify_chain(
        records: List[
            Dict[str, Any]
        ],
    ) -> bool:
        previous = "GENESIS"

        for record in records:
            expected = (
                hashlib.sha256(
                    (
                        f"{previous}:{record['payload_hash']}"
                    ).encode("utf-8")
                ).hexdigest()
            )

            if (
                expected
                != record["chain_hash"]
            ):
                return False

            previous = (
                record["chain_hash"]
            )

        return True


class AsyncLogBufferProcessor:
    """
    Non-blocking async audit buffer.
    """

    BUFFER_LIMIT = 1024

    def __init__(
        self,
        *,
        store: SQLiteAuditStore,
        message_bus: Optional[
            MessageBus
        ] = None,
    ) -> None:
        self.store = store

        self.message_bus = (
            message_bus
        )

        self._queue: asyncio.Queue[
            AuditEvent
        ] = asyncio.Queue(
            maxsize=self.BUFFER_LIMIT
        )

        self._worker: Optional[
            asyncio.Task
        ] = None

        self._running = False

        self._processed = 0

        self._failures = 0

        self._recent_events: Deque[
            str
        ] = deque(maxlen=128)

    async def start(
        self,
    ) -> None:
        self._running = True

        self._worker = (
            asyncio.create_task(
                self._processor_loop()
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
        event: AuditEvent,
    ) -> None:
        try:
            self._queue.put_nowait(
                event
            )

        except asyncio.QueueFull:
            self._failures += 1

            logger.warning(
                "Audit buffer full"
            )

    async def _processor_loop(
        self,
    ) -> None:
        while self._running:
            try:
                event = (
                    await self._queue.get()
                )

                await self._process_event(
                    event
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                self._failures += 1

                logger.exception(
                    "Audit processing failure"
                )

    async def _process_event(
        self,
        event: AuditEvent,
    ) -> None:
        previous_hash = (
            await self.store.fetch_last_hash()
        )

        payload_hash = (
            CryptographicHashChain.build_payload_hash(
                event
            )
        )

        chain_hash = (
            CryptographicHashChain.build_chain_hash(
                previous_hash=
                    previous_hash,
                payload_hash=
                    payload_hash,
            )
        )

        record = AuditRecord(
            record_id=
                uuid.uuid4().hex,
            event_id=
                event.event_id,
            chain_hash=
                chain_hash,
            previous_hash=
                previous_hash,
            payload_hash=
                payload_hash,
            event_type=
                event.event_type,
            actor_id=
                event.actor_id,
            resource=
                event.resource,
            action=
                event.action,
            status=
                event.status,
            timestamp=
                event.timestamp,
            metadata=
                event.metadata,
        )

        await self.store.write_record(
            record
        )

        self._processed += 1

        self._recent_events.append(
            event.event_id
        )

        if (
            event.status.lower()
            in {
                "denied",
                "blocked",
                "rejected",
            }
        ):
            await self._emit_security_alert(
                event
            )

    async def _emit_security_alert(
        self,
        event: AuditEvent,
    ) -> None:
        if not self.message_bus:
            return

        payload = {
            "alert_type":
                "security_violation",
            "event_id":
                event.event_id,
            "actor_id":
                event.actor_id,
            "resource":
                event.resource,
            "action":
                event.action,
            "timestamp":
                event.timestamp,
            "metadata":
                event.metadata,
        }

        await self.message_bus.publish(
            topic="security.alert",
            payload=payload,
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "processed":
                self._processed,
            "failures":
                self._failures,
            "queue_size":
                self._queue.qsize(),
            "cached_events":
                len(
                    self._recent_events
                ),
            "running":
                self._running,
        }


class ImmutabilityAuditLogger:
    """
    Async-first tamper-evident audit logger.

    Features:
    - SHA256 chained logs
    - SQLite WAL persistence
    - Async non-blocking logging
    - Security violation detection
    - RBAC denial tracking
    - Sandbox execution auditing
    - Log integrity verification
    """

    CLEANUP_INTERVAL = 3600

    def __init__(
        self,
        *,
        database_path: str = (
            "./data/audit_logs.db"
        ),
        message_bus: Optional[
            MessageBus
        ] = None,
    ) -> None:
        self.store = SQLiteAuditStore(
            database_path=
                database_path
        )

        self.buffer = (
            AsyncLogBufferProcessor(
                store=self.store,
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
            "Starting ImmutabilityAuditLogger"
        )

        await self.store.initialize()

        await self.buffer.start()

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
            "Stopping ImmutabilityAuditLogger"
        )

        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._maintenance_task

        await self.buffer.stop()

        await self.store.close()

    async def log_event(
        self,
        *,
        event_type: str,
        actor_id: str,
        resource: str,
        action: str,
        status: str,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> str:
        """
        Main audit logging entrypoint.
        """

        event = AuditEvent(
            event_id=
                uuid.uuid4().hex,
            event_type=
                event_type,
            actor_id=
                actor_id,
            resource=
                resource,
            action=
                action,
            status=
                status,
            timestamp=
                time.time(),
            metadata=
                metadata or {},
        )

        await self.buffer.enqueue(
            event
        )

        return event.event_id

    async def verify_integrity(
        self,
    ) -> bool:
        """
        Full tamper verification.
        """

        records = (
            await self.store.fetch_all_records()
        )

        return (
            LogVerificationUtility.verify_chain(
                records
            )
        )

    async def recent_logs(
        self,
        *,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        return (
            await self.store.fetch_recent_records(
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
                    "Audit maintenance failure"
                )

    def _wal_checkpoint(
        self,
    ) -> None:
        self.store._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "running":
                self._running,
            "buffer":
                self.buffer.stats(),
            "timestamp":
                time.time(),
        }


DEFAULT_AUDIT_LOGGER = (
    ImmutabilityAuditLogger()
)
