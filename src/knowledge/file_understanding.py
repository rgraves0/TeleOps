from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
)

from app.tools.dynamic_router import (
    DynamicToolRouter,
    RouteContext,
    RouteDecision,
)


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FileContextRequest:
    requester_id: str
    requester_roles: Set[str]
    requester_permissions: Set[str]
    document_id: str
    query: Optional[str] = None
    max_chunks: int = 12
    max_context_chars: int = 12000
    include_ocr: bool = True
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class StructuralMetadata:
    document_id: str
    total_chunks: int
    estimated_pages: int
    section_headers: List[str]
    provenance: Dict[str, Any]
    created_at: float


@dataclass(slots=True)
class ContextPayload:
    document_id: str
    assembled_context: str
    chunk_ids: List[str]
    metadata: StructuralMetadata
    created_at: float
    optimization: Dict[str, Any] = field(
        default_factory=dict
    )


class DocumentRBACValidator:
    """
    Document-level RBAC validation.

    Enforces Default Deny policy.
    """

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate(
        self,
        request: FileContextRequest,
    ) -> bool:
        context = RouteContext(
            requester_id=(
                request.requester_id
            ),
            requester_roles=(
                request.requester_roles
            ),
            requester_permissions=(
                request.requester_permissions
            ),
            task_type="knowledge.read",
            metadata={
                "document_id":
                    request.document_id,
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


class SQLiteKnowledgeReader:
    """
    Lightweight SQLite knowledge bridge.
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

        self._connection: Optional[
            sqlite3.Connection
        ] = None

    async def initialize(self) -> None:
        self._connection = sqlite3.connect(
            str(self.database_path),
            check_same_thread=False,
        )

        await asyncio.to_thread(
            self._configure_database
        )

    async def close(self) -> None:
        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    async def fetch_document_chunks(
        self,
        *,
        document_id: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self._fetch_chunks,
            document_id,
            limit,
        )

    async def search_document_chunks(
        self,
        *,
        document_id: str,
        query: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self._search_chunks,
            document_id,
            query,
            limit,
        )

    async def fetch_ocr_content(
        self,
        *,
        document_id: str,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self._fetch_ocr_rows,
            document_id,
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

    def _fetch_chunks(
        self,
        document_id: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        cursor = self._connection.execute(
            """
            SELECT
                chunk_id,
                chunk_index,
                text_content,
                metadata
            FROM semantic_chunks
            WHERE document_id = ?
            ORDER BY chunk_index ASC
            LIMIT ?
            """,
            (
                document_id,
                limit,
            ),
        )

        rows = cursor.fetchall()

        results = []

        for row in rows:
            results.append(
                {
                    "chunk_id":
                        row[0],
                    "chunk_index":
                        row[1],
                    "text":
                        row[2],
                    "metadata":
                        json.loads(
                            row[3]
                        ),
                }
            )

        return results

    def _search_chunks(
        self,
        document_id: str,
        query: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        pattern = f"%{query}%"

        cursor = self._connection.execute(
            """
            SELECT
                chunk_id,
                chunk_index,
                text_content,
                metadata
            FROM semantic_chunks
            WHERE document_id = ?
            AND text_content LIKE ?
            ORDER BY chunk_index ASC
            LIMIT ?
            """,
            (
                document_id,
                pattern,
                limit,
            ),
        )

        rows = cursor.fetchall()

        results = []

        for row in rows:
            results.append(
                {
                    "chunk_id":
                        row[0],
                    "chunk_index":
                        row[1],
                    "text":
                        row[2],
                    "metadata":
                        json.loads(
                            row[3]
                        ),
                }
            )

        return results

    def _fetch_ocr_rows(
        self,
        document_id: str,
    ) -> List[Dict[str, Any]]:
        try:
            cursor = self._connection.execute(
                """
                SELECT
                    extracted_text,
                    metadata,
                    created_at
                FROM ocr_documents
                WHERE document_id = ?
                """,
                (document_id,),
            )

            rows = cursor.fetchall()

            results = []

            for row in rows:
                results.append(
                    {
                        "text":
                            row[0],
                        "metadata":
                            json.loads(
                                row[1]
                            ),
                        "created_at":
                            row[2],
                    }
                )

            return results

        except Exception:
            return []


class MetadataSynthesizer:
    """
    Structural metadata synthesizer.
    """

    HEADER_PREFIXES = (
        "#",
        "##",
        "###",
        "section",
        "chapter",
    )

    def synthesize(
        self,
        *,
        document_id: str,
        chunks: List[
            Dict[str, Any]
        ],
        ocr_rows: List[
            Dict[str, Any]
        ],
    ) -> StructuralMetadata:
        headers: List[str] = []

        provenance: Dict[
            str,
            Any,
        ] = {
            "ocr_enabled":
                bool(ocr_rows),
            "chunk_count":
                len(chunks),
        }

        for chunk in chunks:
            lines = (
                chunk["text"]
                .splitlines()
            )

            for line in lines:
                cleaned = (
                    line.strip()
                )

                lowered = (
                    cleaned.lower()
                )

                if any(
                    lowered.startswith(
                        prefix
                    )
                    for prefix in self.HEADER_PREFIXES
                ):
                    headers.append(
                        cleaned[:120]
                    )

        estimated_pages = max(
            1,
            len(chunks) // 4,
        )

        return StructuralMetadata(
            document_id=document_id,
            total_chunks=len(
                chunks
            ),
            estimated_pages=
                estimated_pages,
            section_headers=
                headers[:25],
            provenance=
                provenance,
            created_at=time.time(),
        )


class ContextWindowOptimizer:
    """
    Lightweight context optimizer.

    Prevents LLM window overload.
    """

    def optimize(
        self,
        *,
        chunks: List[
            Dict[str, Any]
        ],
        ocr_rows: List[
            Dict[str, Any]
        ],
        max_chars: int,
    ) -> Tuple[str, List[str]]:
        assembled: List[str] = []

        included_chunks: List[
            str
        ] = []

        current_size = 0

        for chunk in chunks:
            text = (
                chunk["text"]
                .strip()
            )

            if not text:
                continue

            next_size = (
                current_size
                + len(text)
            )

            if next_size > max_chars:
                break

            assembled.append(text)

            included_chunks.append(
                chunk["chunk_id"]
            )

            current_size = next_size

        for row in ocr_rows:
            text = (
                row["text"]
                .strip()
            )

            if not text:
                continue

            next_size = (
                current_size
                + len(text)
            )

            if next_size > max_chars:
                break

            assembled.append(
                "[OCR]\n" + text
            )

            current_size = next_size

        final_text = "\n\n".join(
            assembled
        )

        return (
            final_text,
            included_chunks,
        )


class FileUnderstandingEngine:
    """
    Async-first File Understanding Engine.

    Features:
    - Context assembly
    - Metadata synthesis
    - Lightweight memory assembly
    - OCR-aware integration
    - Context window optimization
    - SQLite WAL retrieval
    - Document-level RBAC enforcement
    - Low-memory async-safe design
    """

    CLEANUP_INTERVAL = 3600

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        database_path: str = (
            "./data/semantic_index.db"
        ),
    ) -> None:
        self.router = router

        self._rbac = (
            DocumentRBACValidator(
                router
            )
        )

        self._reader = (
            SQLiteKnowledgeReader(
                database_path=
                    database_path
            )
        )

        self._metadata = (
            MetadataSynthesizer()
        )

        self._optimizer = (
            ContextWindowOptimizer()
        )

        self._running = False

        self._tasks: List[
            asyncio.Task
        ] = []

        self._assembly_cache: deque[
            str
        ] = deque(maxlen=128)

    async def start(self) -> None:
        await self._reader.initialize()

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

        await self._reader.close()

    async def assemble_context(
        self,
        request: FileContextRequest,
    ) -> ContextPayload:
        """
        Main context assembly pipeline.
        """

        allowed = (
            await self._rbac.validate(
                request
            )
        )

        if not allowed:
            raise PermissionError(
                "RBAC denied document access"
            )

        if request.query:
            chunks = (
                await self._reader.search_document_chunks(
                    document_id=
                        request.document_id,
                    query=
                        request.query,
                    limit=
                        request.max_chunks,
                )
            )

        else:
            chunks = (
                await self._reader.fetch_document_chunks(
                    document_id=
                        request.document_id,
                    limit=
                        request.max_chunks,
                )
            )

        ocr_rows: List[
            Dict[str, Any]
        ] = []

        if request.include_ocr:
            ocr_rows = (
                await self._reader.fetch_ocr_content(
                    document_id=
                        request.document_id
                )
            )

        metadata = (
            self._metadata.synthesize(
                document_id=
                    request.document_id,
                chunks=chunks,
                ocr_rows=ocr_rows,
            )
        )

        (
            optimized_context,
            included_chunk_ids,
        ) = (
            self._optimizer.optimize(
                chunks=chunks,
                ocr_rows=ocr_rows,
                max_chars=
                    request.max_context_chars,
            )
        )

        self._assembly_cache.append(
            request.document_id
        )

        return ContextPayload(
            document_id=
                request.document_id,
            assembled_context=
                optimized_context,
            chunk_ids=
                included_chunk_ids,
            metadata=
                metadata,
            created_at=time.time(),
            optimization={
                "max_context_chars":
                    request.max_context_chars,
                "included_chunks":
                    len(
                        included_chunk_ids
                    ),
                "ocr_included":
                    bool(ocr_rows),
            },
        )

    async def summarize_structure(
        self,
        request: FileContextRequest,
    ) -> Dict[str, Any]:
        """
        Lightweight structural overview.
        """

        payload = (
            await self.assemble_context(
                request
            )
        )

        return {
            "document_id":
                payload.document_id,
            "estimated_pages":
                payload.metadata.estimated_pages,
            "section_headers":
                payload.metadata.section_headers,
            "chunk_count":
                payload.metadata.total_chunks,
            "provenance":
                payload.metadata.provenance,
        }

    async def contextual_search(
        self,
        *,
        request: FileContextRequest,
        queries: List[str],
    ) -> Dict[str, ContextPayload]:
        """
        Multi-query lightweight assembly.
        """

        results: Dict[
            str,
            ContextPayload,
        ] = {}

        for query in queries:
            scoped_request = (
                FileContextRequest(
                    requester_id=
                        request.requester_id,
                    requester_roles=
                        request.requester_roles,
                    requester_permissions=
                        request.requester_permissions,
                    document_id=
                        request.document_id,
                    query=query,
                    max_chunks=
                        request.max_chunks,
                    max_context_chars=
                        request.max_context_chars,
                    include_ocr=
                        request.include_ocr,
                    metadata=
                        request.metadata,
                )
            )

            results[query] = (
                await self.assemble_context(
                    scoped_request
                )
            )

        return results

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
                    "File understanding maintenance failure"
                )

    def _wal_checkpoint(
        self,
    ) -> None:
        self._reader._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "running":
                self._running,
            "assembly_cache":
                len(
                    self._assembly_cache
                ),
            "database":
                str(
                    self._reader.database_path
                ),
            "timestamp":
                time.time(),
        }
