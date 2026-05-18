from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import sqlite3
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Deque,
    Dict,
    List,
    Optional,
    Set,
)

from app.knowledge.document_ingestion import (
    TextChunk,
)

from app.tools.dynamic_router import (
    DynamicToolRouter,
    RouteContext,
    RouteDecision,
)


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ChunkMetadata:
    chunk_id: str
    document_id: str
    chunk_index: int
    content_hash: str
    created_at: float
    roles: Set[str] = field(
        default_factory=set
    )
    permissions: Set[str] = field(
        default_factory=set
    )
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class IndexedChunk:
    chunk_id: str
    document_id: str
    chunk_index: int
    text: str
    metadata: ChunkMetadata


@dataclass(slots=True)
class RetrievalRequest:
    requester_id: str
    requester_roles: Set[str]
    requester_permissions: Set[str]
    query: str
    limit: int = 10
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class SemanticChunker:
    """
    Lightweight semantic chunking engine.

    Features:
    - Fixed-size chunking
    - Overlap support
    - Generator-based streaming
    - Memory-safe processing
    """

    DEFAULT_CHUNK_SIZE = 1200
    DEFAULT_OVERLAP = 150

    def __init__(
        self,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        overlap: int = DEFAULT_OVERLAP,
    ) -> None:
        self.chunk_size = max(
            256,
            chunk_size,
        )

        self.overlap = max(
            0,
            min(overlap, chunk_size // 2),
        )

    async def chunk_stream(
        self,
        *,
        text_stream: AsyncGenerator[
            TextChunk,
            None,
        ],
    ) -> AsyncGenerator[
        IndexedChunk,
        None,
    ]:
        """
        Stream-safe semantic chunking.
        """

        carry_buffer = ""

        async for source_chunk in text_stream:
            incoming = (
                carry_buffer
                + "\n"
                + source_chunk.text
            )

            normalized = (
                self._normalize_text(
                    incoming
                )
            )

            cursor = 0
            chunk_index = (
                source_chunk.chunk_index
            )

            while cursor < len(
                normalized
            ):
                end = min(
                    cursor
                    + self.chunk_size,
                    len(normalized),
                )

                chunk_text = (
                    normalized[cursor:end]
                )

                if (
                    len(chunk_text.strip())
                    < 20
                ):
                    break

                chunk_id = (
                    self._generate_chunk_id(
                        source_chunk.document_id,
                        chunk_index,
                        chunk_text,
                    )
                )

                metadata = ChunkMetadata(
                    chunk_id=chunk_id,
                    document_id=(
                        source_chunk.document_id
                    ),
                    chunk_index=chunk_index,
                    content_hash=(
                        self._hash_content(
                            chunk_text
                        )
                    ),
                    created_at=time.time(),
                    metadata=(
                        source_chunk.metadata
                    ),
                )

                yield IndexedChunk(
                    chunk_id=chunk_id,
                    document_id=(
                        source_chunk.document_id
                    ),
                    chunk_index=chunk_index,
                    text=chunk_text,
                    metadata=metadata,
                )

                cursor += (
                    self.chunk_size
                    - self.overlap
                )

                chunk_index += 1

            if (
                len(normalized)
                > self.overlap
            ):
                carry_buffer = normalized[
                    -self.overlap :
                ]

    def _normalize_text(
        self,
        text: str,
    ) -> str:
        text = (
            text.replace("\x00", "")
            .replace("\r", "")
            .strip()
        )

        while "  " in text:
            text = text.replace(
                "  ",
                " ",
            )

        return text

    def _generate_chunk_id(
        self,
        document_id: str,
        chunk_index: int,
        text: str,
    ) -> str:
        raw = (
            f"{document_id}:"
            f"{chunk_index}:"
            f"{text[:128]}"
        )

        return hashlib.sha1(
            raw.encode("utf-8")
        ).hexdigest()

    def _hash_content(
        self,
        content: str,
    ) -> str:
        return hashlib.sha1(
            content.encode("utf-8")
        ).hexdigest()


class ChunkRBACEnforcer:
    """
    Chunk-level RBAC enforcement.

    Preserves Default Deny policies.
    """

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate_access(
        self,
        *,
        requester_id: str,
        requester_roles: Set[str],
        requester_permissions: Set[str],
        document_id: str,
    ) -> bool:
        context = RouteContext(
            requester_id=requester_id,
            requester_roles=requester_roles,
            requester_permissions=(
                requester_permissions
            ),
            task_type="knowledge.read",
            metadata={
                "document_id":
                    document_id,
            },
        )

        route = await self.router.route(
            task="knowledge.read",
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class SQLiteIndexer:
    """
    SQLite WAL-mode semantic indexer.

    Features:
    - WAL mode
    - Lightweight indexing
    - Metadata persistence
    - Chunk-level RBAC storage
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

    async def store_chunk(
        self,
        chunk: IndexedChunk,
    ) -> None:
        await asyncio.to_thread(
            self._insert_chunk,
            chunk,
        )

    async def retrieve_chunks(
        self,
        *,
        query: str,
        limit: int,
    ) -> List[IndexedChunk]:
        rows = await asyncio.to_thread(
            self._search_chunks,
            query,
            limit,
        )

        results: List[
            IndexedChunk
        ] = []

        for row in rows:
            metadata = ChunkMetadata(
                chunk_id=row[0],
                document_id=row[1],
                chunk_index=row[2],
                content_hash=row[4],
                created_at=row[5],
                roles=set(
                    json.loads(row[6])
                ),
                permissions=set(
                    json.loads(row[7])
                ),
                metadata=json.loads(
                    row[8]
                ),
            )

            results.append(
                IndexedChunk(
                    chunk_id=row[0],
                    document_id=row[1],
                    chunk_index=row[2],
                    text=row[3],
                    metadata=metadata,
                )
            )

        return results

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
            CREATE TABLE IF NOT EXISTS semantic_chunks (
                chunk_id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                text_content TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                created_at REAL NOT NULL,
                roles TEXT NOT NULL,
                permissions TEXT NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_semantic_doc
            ON semantic_chunks(document_id)
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_semantic_hash
            ON semantic_chunks(content_hash)
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_semantic_chunk
            ON semantic_chunks(chunk_index)
            """
        )

    def _insert_chunk(
        self,
        chunk: IndexedChunk,
    ) -> None:
        self._connection.execute(
            """
            INSERT OR REPLACE INTO semantic_chunks (
                chunk_id,
                document_id,
                chunk_index,
                text_content,
                content_hash,
                created_at,
                roles,
                permissions,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chunk.chunk_id,
                chunk.document_id,
                chunk.chunk_index,
                chunk.text,
                chunk.metadata.content_hash,
                chunk.metadata.created_at,
                json.dumps(
                    list(
                        chunk.metadata.roles
                    )
                ),
                json.dumps(
                    list(
                        chunk.metadata.permissions
                    )
                ),
                json.dumps(
                    chunk.metadata.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _search_chunks(
        self,
        query: str,
        limit: int,
    ) -> List[Any]:
        pattern = f"%{query}%"

        cursor = self._connection.execute(
            """
            SELECT
                chunk_id,
                document_id,
                chunk_index,
                text_content,
                content_hash,
                created_at,
                roles,
                permissions,
                metadata
            FROM semantic_chunks
            WHERE text_content LIKE ?
            ORDER BY chunk_index ASC
            LIMIT ?
            """,
            (
                pattern,
                limit,
            ),
        )

        return cursor.fetchall()


class SemanticIndexer:
    """
    Async-first semantic indexing pipeline.

    Features:
    - Semantic chunking
    - SQLite WAL indexing
    - Generator-based ingestion
    - Chunk-level RBAC
    - Low-memory operation
    - Async-safe persistence
    """

    CLEANUP_INTERVAL = 3600

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        database_path: str = (
            "./data/semantic_index.db"
        ),
        chunk_size: int = 1200,
        overlap: int = 150,
    ) -> None:
        self.router = router

        self._chunker = SemanticChunker(
            chunk_size=chunk_size,
            overlap=overlap,
        )

        self._rbac = (
            ChunkRBACEnforcer(
                router
            )
        )

        self._indexer = SQLiteIndexer(
            database_path=database_path
        )

        self._running = False

        self._tasks: List[
            asyncio.Task
        ] = []

        self._cache: Deque[
            str
        ] = deque(maxlen=256)

    async def start(self) -> None:
        await self._indexer.initialize()

        self._running = True

        self._tasks.append(
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(self) -> None:
        self._running = False

        for task in self._tasks:
            task.cancel()

        for task in self._tasks:
            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await task

        self._tasks.clear()

        await self._indexer.close()

    async def index_stream(
        self,
        *,
        text_stream: AsyncGenerator[
            TextChunk,
            None,
        ],
        roles: Optional[
            Set[str]
        ] = None,
        permissions: Optional[
            Set[str]
        ] = None,
    ) -> AsyncGenerator[
        IndexedChunk,
        None,
    ]:
        """
        Stream-safe semantic indexing.
        """

        async for chunk in (
            self._chunker.chunk_stream(
                text_stream=text_stream
            )
        ):
            chunk.metadata.roles = (
                roles or set()
            )

            chunk.metadata.permissions = (
                permissions or set()
            )

            await self._indexer.store_chunk(
                chunk
            )

            self._cache.append(
                chunk.chunk_id
            )

            yield chunk

    async def retrieve(
        self,
        request: RetrievalRequest,
    ) -> List[IndexedChunk]:
        """
        RBAC-aware chunk retrieval.
        """

        chunks = (
            await self._indexer.retrieve_chunks(
                query=request.query,
                limit=request.limit,
            )
        )

        results: List[
            IndexedChunk
        ] = []

        for chunk in chunks:
            allowed = (
                await self._rbac.validate_access(
                    requester_id=
                        request.requester_id,
                    requester_roles=
                        request.requester_roles,
                    requester_permissions=
                        request.requester_permissions,
                    document_id=
                        chunk.document_id,
                )
            )

            if not allowed:
                continue

            if (
                chunk.metadata.roles
                and not (
                    request.requester_roles
                    & chunk.metadata.roles
                )
            ):
                continue

            if (
                chunk.metadata.permissions
                and not (
                    chunk.metadata.permissions
                    <= request.requester_permissions
                )
            ):
                continue

            results.append(chunk)

        return results

    async def delete_document(
        self,
        document_id: str,
    ) -> None:
        await asyncio.to_thread(
            self._delete_document_chunks,
            document_id,
        )

    def _delete_document_chunks(
        self,
        document_id: str,
    ) -> None:
        self._indexer._connection.execute(
            """
            DELETE FROM semantic_chunks
            WHERE document_id = ?
            """,
            (document_id,),
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
                    "Semantic index maintenance failure"
                )

    def _wal_checkpoint(
        self,
    ) -> None:
        self._indexer._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "cached_chunks":
                len(self._cache),
            "running":
                self._running,
            "database":
                str(
                    self._indexer.database_path
                ),
            "timestamp":
                time.time(),
        }
