from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import sqlite3
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

from app.core.base_agent import BaseAgent
from app.core.message_bus import MessageBus


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MemoryRecord:
    key: str
    namespace: str
    value: Any
    created_at: float
    updated_at: float
    expires_at: Optional[float]
    access_count: int


class MemoryAgent(BaseAgent):
    """
    Lightweight async-first operational memory subsystem.

    Optimized for:
    - 1 CPU / 1GB RAM VPS environments
    - SQLite WAL mode
    - Low memory footprint
    - Shared multi-agent operational memory
    - Persistent contextual state
    - TTL-based automatic cleanup
    - Event-driven synchronization

    Responsibilities:
    - Shared persistent memory storage
    - Context retrieval
    - Short-term & long-term state persistence
    - Memory compaction / TTL cleanup
    - Lightweight metrics caching
    - Agent synchronization via MessageBus
    """

    DB_FILENAME = "operational_memory.db"

    VACUUM_INTERVAL_SECONDS = 3600
    CLEANUP_INTERVAL_SECONDS = 120
    METRIC_CACHE_MAX = 256

    DEFAULT_QUERY_LIMIT = 50

    SQLITE_BUSY_TIMEOUT_MS = 5000

    def __init__(
        self,
        message_bus: MessageBus,
        *,
        db_path: str = "./data",
        agent_id: str = "memory-agent",
    ) -> None:
        super().__init__(agent_id=agent_id)

        self.message_bus = message_bus

        self._running = False
        self._tasks: List[asyncio.Task] = []

        self._db_dir = Path(db_path)
        self._db_dir.mkdir(parents=True, exist_ok=True)

        self._db_file = self._db_dir / self.DB_FILENAME

        self._write_lock = asyncio.Lock()

        self._metrics_cache: Deque[Tuple[str, float]] = deque(
            maxlen=self.METRIC_CACHE_MAX
        )

        self._connection: Optional[sqlite3.Connection] = None

    async def start(self) -> None:
        logger.info("Starting MemoryAgent")

        await self._initialize_database()
        await self._register_message_handlers()

        self._running = True

        self._tasks.extend(
            [
                asyncio.create_task(self._cleanup_loop()),
                asyncio.create_task(self._vacuum_loop()),
            ]
        )

        logger.info("MemoryAgent started")

    async def stop(self) -> None:
        logger.info("Stopping MemoryAgent")

        self._running = False

        for task in self._tasks:
            task.cancel()

        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._tasks.clear()

        if self._connection:
            await asyncio.to_thread(self._connection.close)

        logger.info("MemoryAgent stopped")

    async def _initialize_database(self) -> None:
        self._connection = sqlite3.connect(
            str(self._db_file),
            check_same_thread=False,
            isolation_level=None,
        )

        await asyncio.to_thread(
            self._configure_database,
            self._connection,
        )

        await asyncio.to_thread(
            self._create_tables,
            self._connection,
        )

    def _configure_database(
        self,
        conn: sqlite3.Connection,
    ) -> None:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-2000;")
        conn.execute("PRAGMA mmap_size=268435456;")
        conn.execute(
            f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT_MS};"
        )

    def _create_tables(
        self,
        conn: sqlite3.Connection,
    ) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS operational_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                namespace TEXT NOT NULL,
                memory_key TEXT NOT NULL,
                memory_value TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                expires_at REAL,
                access_count INTEGER DEFAULT 0,
                UNIQUE(namespace, memory_key)
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_memory_namespace
            ON operational_memory(namespace)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_memory_expiry
            ON operational_memory(expires_at)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_memory_updated
            ON operational_memory(updated_at)
            """
        )

    async def _register_message_handlers(self) -> None:
        await self.message_bus.subscribe(
            "memory.write",
            self._handle_memory_write,
        )

        await self.message_bus.subscribe(
            "memory.read",
            self._handle_memory_read,
        )

        await self.message_bus.subscribe(
            "memory.delete",
            self._handle_memory_delete,
        )

        await self.message_bus.subscribe(
            "memory.query",
            self._handle_memory_query,
        )

        await self.message_bus.subscribe(
            "metrics.cache",
            self._handle_metrics_cache,
        )

    async def _handle_memory_write(
        self,
        payload: Dict[str, Any],
    ) -> None:
        namespace = payload.get("namespace", "default")
        key = payload.get("key")
        value = payload.get("value")

        ttl = payload.get("ttl")

        correlation_id = payload.get("correlation_id")
        reply_to = payload.get("reply_to")

        if not key:
            return

        success = await self.store_memory(
            namespace=namespace,
            key=key,
            value=value,
            ttl=ttl,
        )

        if reply_to:
            await self.message_bus.publish(
                reply_to,
                {
                    "success": success,
                    "operation": "write",
                    "key": key,
                    "namespace": namespace,
                    "correlation_id": correlation_id,
                },
            )

    async def _handle_memory_read(
        self,
        payload: Dict[str, Any],
    ) -> None:
        namespace = payload.get("namespace", "default")
        key = payload.get("key")

        correlation_id = payload.get("correlation_id")
        reply_to = payload.get("reply_to")

        if not key:
            return

        result = await self.get_memory(
            namespace=namespace,
            key=key,
        )

        if reply_to:
            await self.message_bus.publish(
                reply_to,
                {
                    "success": result is not None,
                    "operation": "read",
                    "key": key,
                    "namespace": namespace,
                    "value": result,
                    "correlation_id": correlation_id,
                },
            )

    async def _handle_memory_delete(
        self,
        payload: Dict[str, Any],
    ) -> None:
        namespace = payload.get("namespace", "default")
        key = payload.get("key")

        correlation_id = payload.get("correlation_id")
        reply_to = payload.get("reply_to")

        if not key:
            return

        success = await self.delete_memory(
            namespace=namespace,
            key=key,
        )

        if reply_to:
            await self.message_bus.publish(
                reply_to,
                {
                    "success": success,
                    "operation": "delete",
                    "key": key,
                    "namespace": namespace,
                    "correlation_id": correlation_id,
                },
            )

    async def _handle_memory_query(
        self,
        payload: Dict[str, Any],
    ) -> None:
        namespace = payload.get("namespace", "default")
        pattern = payload.get("pattern", "")

        limit = int(
            payload.get("limit", self.DEFAULT_QUERY_LIMIT)
        )

        correlation_id = payload.get("correlation_id")
        reply_to = payload.get("reply_to")

        results = await self.query_memory(
            namespace=namespace,
            pattern=pattern,
            limit=limit,
        )

        if reply_to:
            await self.message_bus.publish(
                reply_to,
                {
                    "success": True,
                    "operation": "query",
                    "namespace": namespace,
                    "results": results,
                    "correlation_id": correlation_id,
                },
            )

    async def _handle_metrics_cache(
        self,
        payload: Dict[str, Any],
    ) -> None:
        metric_name = payload.get("metric")

        if not metric_name:
            return

        self._metrics_cache.append(
            (
                metric_name,
                time.time(),
            )
        )

    async def store_memory(
        self,
        *,
        namespace: str,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        if not self._connection:
            return False

        expires_at = (
            time.time() + ttl if ttl else None
        )

        serialized = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
        )

        async with self._write_lock:
            try:
                await asyncio.to_thread(
                    self._upsert_memory,
                    namespace,
                    key,
                    serialized,
                    expires_at,
                )

                return True

            except Exception:
                logger.exception(
                    "Memory write failure | ns=%s key=%s",
                    namespace,
                    key,
                )

                return False

    def _upsert_memory(
        self,
        namespace: str,
        key: str,
        serialized: str,
        expires_at: Optional[float],
    ) -> None:
        now = time.time()

        self._connection.execute(
            """
            INSERT INTO operational_memory (
                namespace,
                memory_key,
                memory_value,
                created_at,
                updated_at,
                expires_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(namespace, memory_key)
            DO UPDATE SET
                memory_value=excluded.memory_value,
                updated_at=excluded.updated_at,
                expires_at=excluded.expires_at
            """,
            (
                namespace,
                key,
                serialized,
                now,
                now,
                expires_at,
            ),
        )

    async def get_memory(
        self,
        *,
        namespace: str,
        key: str,
    ) -> Optional[Any]:
        if not self._connection:
            return None

        try:
            row = await asyncio.to_thread(
                self._fetch_memory,
                namespace,
                key,
            )

            if not row:
                return None

            return json.loads(row)

        except Exception:
            logger.exception(
                "Memory read failure | ns=%s key=%s",
                namespace,
                key,
            )

            return None

    def _fetch_memory(
        self,
        namespace: str,
        key: str,
    ) -> Optional[str]:
        now = time.time()

        cursor = self._connection.execute(
            """
            SELECT memory_value
            FROM operational_memory
            WHERE namespace = ?
            AND memory_key = ?
            AND (
                expires_at IS NULL
                OR expires_at > ?
            )
            LIMIT 1
            """,
            (
                namespace,
                key,
                now,
            ),
        )

        row = cursor.fetchone()

        if not row:
            return None

        self._connection.execute(
            """
            UPDATE operational_memory
            SET access_count = access_count + 1
            WHERE namespace = ?
            AND memory_key = ?
            """,
            (
                namespace,
                key,
            ),
        )

        return row[0]

    async def delete_memory(
        self,
        *,
        namespace: str,
        key: str,
    ) -> bool:
        if not self._connection:
            return False

        async with self._write_lock:
            try:
                deleted = await asyncio.to_thread(
                    self._delete_memory_sync,
                    namespace,
                    key,
                )

                return deleted > 0

            except Exception:
                logger.exception(
                    "Memory delete failure | ns=%s key=%s",
                    namespace,
                    key,
                )

                return False

    def _delete_memory_sync(
        self,
        namespace: str,
        key: str,
    ) -> int:
        cursor = self._connection.execute(
            """
            DELETE FROM operational_memory
            WHERE namespace = ?
            AND memory_key = ?
            """,
            (
                namespace,
                key,
            ),
        )

        return cursor.rowcount

    async def query_memory(
        self,
        *,
        namespace: str,
        pattern: str = "",
        limit: int = DEFAULT_QUERY_LIMIT,
    ) -> List[Dict[str, Any]]:
        if not self._connection:
            return []

        try:
            rows = await asyncio.to_thread(
                self._query_memory_sync,
                namespace,
                pattern,
                limit,
            )

            results: List[Dict[str, Any]] = []

            for row in rows:
                results.append(
                    {
                        "key": row[0],
                        "value": json.loads(row[1]),
                        "updated_at": row[2],
                        "expires_at": row[3],
                        "access_count": row[4],
                    }
                )

            return results

        except Exception:
            logger.exception(
                "Memory query failure | ns=%s",
                namespace,
            )

            return []

    def _query_memory_sync(
        self,
        namespace: str,
        pattern: str,
        limit: int,
    ) -> List[Tuple]:
        now = time.time()

        cursor = self._connection.execute(
            """
            SELECT
                memory_key,
                memory_value,
                updated_at,
                expires_at,
                access_count
            FROM operational_memory
            WHERE namespace = ?
            AND memory_key LIKE ?
            AND (
                expires_at IS NULL
                OR expires_at > ?
            )
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (
                namespace,
                f"%{pattern}%",
                now,
                limit,
            ),
        )

        return cursor.fetchall()

    async def synchronize_state(
        self,
        *,
        namespace: str,
        state: Dict[str, Any],
        ttl: Optional[int] = None,
    ) -> None:
        for key, value in state.items():
            await self.store_memory(
                namespace=namespace,
                key=key,
                value=value,
                ttl=ttl,
            )

    async def _cleanup_loop(self) -> None:
        while self._running:
            try:
                await self._cleanup_expired_records()
            except Exception:
                logger.exception(
                    "Memory cleanup loop failure"
                )

            await asyncio.sleep(
                self.CLEANUP_INTERVAL_SECONDS
            )

    async def _cleanup_expired_records(self) -> None:
        if not self._connection:
            return

        async with self._write_lock:
            deleted = await asyncio.to_thread(
                self._cleanup_expired_sync
            )

        if deleted > 0:
            logger.info(
                "Expired memory cleanup completed | deleted=%s",
                deleted,
            )

    def _cleanup_expired_sync(self) -> int:
        now = time.time()

        cursor = self._connection.execute(
            """
            DELETE FROM operational_memory
            WHERE expires_at IS NOT NULL
            AND expires_at <= ?
            """,
            (now,),
        )

        return cursor.rowcount

    async def _vacuum_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(
                    self.VACUUM_INTERVAL_SECONDS
                )

                await self._compact_database()

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Memory vacuum loop failure"
                )

    async def _compact_database(self) -> None:
        if not self._connection:
            return

        async with self._write_lock:
            logger.info(
                "Starting SQLite WAL checkpoint + vacuum"
            )

            await asyncio.to_thread(
                self._compact_database_sync
            )

    def _compact_database_sync(self) -> None:
        self._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

        self._connection.execute(
            "VACUUM;"
        )

    async def get_namespace_stats(
        self,
        namespace: str,
    ) -> Dict[str, Any]:
        if not self._connection:
            return {}

        return await asyncio.to_thread(
            self._get_namespace_stats_sync,
            namespace,
        )

    def _get_namespace_stats_sync(
        self,
        namespace: str,
    ) -> Dict[str, Any]:
        cursor = self._connection.execute(
            """
            SELECT
                COUNT(*),
                COALESCE(SUM(LENGTH(memory_value)), 0),
                COALESCE(MAX(updated_at), 0)
            FROM operational_memory
            WHERE namespace = ?
            """,
            (namespace,),
        )

        row = cursor.fetchone()

        return {
            "namespace": namespace,
            "record_count": row[0],
            "storage_bytes": row[1],
            "last_updated": row[2],
        }

    async def publish_memory_metrics(self) -> None:
        stats = {
            "cache_events": len(self._metrics_cache),
            "database_path": str(self._db_file),
            "timestamp": time.time(),
        }

        await self.message_bus.publish(
            "memory.metrics",
            stats,
        )

    @property
    def database_path(self) -> str:
        return str(self._db_file)

    @property
    def cache_size(self) -> int:
        return len(self._metrics_cache)
