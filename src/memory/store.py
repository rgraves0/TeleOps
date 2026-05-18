from __future__ import annotations

import json
import logging
from datetime import datetime
from datetime import timedelta
from typing import Any

from src.db.database import (
    DatabaseManager,
)
from src.memory.models import (
    BaseMemoryModel,
    MemoryQueryResult,
    MEMORY_SCHEMA,
    MEMORY_INDEX_SCHEMA,
    MEMORY_INDEXES,
)

logger = logging.getLogger(__name__)


# =========================================================
# MEMORY STORE
# =========================================================


class MemoryStore:

    def __init__(
        self,
        db: DatabaseManager,
    ) -> None:

        self.db = db

        logger.info(
            "MemoryStore initialized"
        )

    # =====================================================
    # INITIALIZE
    # =====================================================

    async def initialize(
        self,
    ) -> None:

        await self.db.execute(
            MEMORY_SCHEMA
        )

        await self.db.execute(
            MEMORY_INDEX_SCHEMA
        )

        for index_sql in (
            MEMORY_INDEXES
        ):

            await self.db.execute(
                index_sql
            )

        logger.info(
            "MemoryStore schema initialized"
        )

    # =====================================================
    # STORE MEMORY
    # =====================================================

    async def store_memory(
        self,
        memory: BaseMemoryModel,
    ) -> bool:

        try:

            await self.db.execute(

                """

                INSERT INTO memory_store (

                    memory_id,
                    memory_type,
                    content,
                    embedding_ref,
                    tags,
                    metadata,
                    importance_score,
                    access_count,
                    expires_at,
                    created_at,
                    updated_at

                )

                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

                """,

                (

                    memory.memory_id,

                    memory.memory_type,

                    memory.content,

                    memory.embedding_ref,

                    json.dumps(
                        memory.tags
                    ),

                    json.dumps(
                        memory.metadata
                    ),

                    memory.importance_score,

                    memory.access_count,

                    memory.expires_at,

                    memory.created_at,

                    memory.updated_at,
                ),
            )

            await self._index_memory(
                memory
            )

            return True

        except Exception:

            logger.exception(
                "Memory store failed"
            )

            return False

    # =====================================================
    # INDEX MEMORY
    # =====================================================

    async def _index_memory(
        self,
        memory: BaseMemoryModel,
    ) -> None:

        keywords = set()

        content_words = (
            memory.content
            .lower()
            .split()
        )

        for word in content_words:

            cleaned = (
                word.strip(
                    ".,!?():[]{}\"'"
                )
            )

            if len(cleaned) >= 4:

                keywords.add(
                    cleaned
                )

        for tag in memory.tags:

            keywords.add(
                tag.lower()
            )

        rows = [

            (

                memory.memory_id,

                keyword,

                datetime.utcnow()
                .isoformat(),
            )

            for keyword
            in keywords
        ]

        if not rows:
            return

        await self.db.executemany(

            """

            INSERT INTO memory_index (

                memory_id,
                keyword,
                created_at

            )

            VALUES (?, ?, ?)

            """,

            rows,
        )

    # =====================================================
    # GET MEMORY
    # =====================================================

    async def get_memory(
        self,
        memory_id: str,
    ) -> dict | None:

        result = await (
            self.db.fetch_one(

                """

                SELECT *
                FROM memory_store

                WHERE memory_id = ?

                """,

                (memory_id,),
            )
        )

        if result:

            await self.increment_access(
                memory_id
            )

        return result

    # =====================================================
    # SEARCH MEMORY
    # =====================================================

    async def search_memory(
        self,
        query: str,
        limit: int = 10,
    ) -> list[
        MemoryQueryResult
    ]:

        keywords = [

            word.lower()

            for word in query.split()

            if len(word) >= 3
        ]

        if not keywords:
            return []

        placeholders = ",".join(
            "?"
            for _ in keywords
        )

        rows = await (
            self.db.fetch_all(

                f"""

                SELECT

                    m.memory_id,
                    m.memory_type,
                    m.content,
                    m.metadata,
                    COUNT(i.keyword) as score

                FROM memory_store m

                JOIN memory_index i
                ON m.memory_id = i.memory_id

                WHERE i.keyword IN ({placeholders})

                GROUP BY m.memory_id

                ORDER BY score DESC,
                         m.importance_score DESC

                LIMIT ?

                """,

                (
                    *keywords,
                    limit,
                ),
            )
        )

        results = []

        for row in rows:

            results.append(

                MemoryQueryResult(

                    memory_id=row[
                        "memory_id"
                    ],

                    memory_type=row[
                        "memory_type"
                    ],

                    content=row[
                        "content"
                    ],

                    score=float(
                        row["score"]
                    ),

                    metadata=json.loads(
                        row[
                            "metadata"
                        ]
                        or "{}"
                    ),
                )
            )

        return results

    # =====================================================
    # INCREMENT ACCESS
    # =====================================================

    async def increment_access(
        self,
        memory_id: str,
    ) -> None:

        await self.db.execute(

            """

            UPDATE memory_store

            SET

                access_count =
                access_count + 1,

                updated_at = ?

            WHERE memory_id = ?

            """,

            (

                datetime.utcnow()
                .isoformat(),

                memory_id,
            ),
        )

    # =====================================================
    # DELETE MEMORY
    # =====================================================

    async def delete_memory(
        self,
        memory_id: str,
    ) -> bool:

        try:

            await self.db.execute(

                """

                DELETE FROM memory_store
                WHERE memory_id = ?

                """,

                (memory_id,),
            )

            await self.db.execute(

                """

                DELETE FROM memory_index
                WHERE memory_id = ?

                """,

                (memory_id,),
            )

            return True

        except Exception:

            logger.exception(
                "Memory delete failed"
            )

            return False

    # =====================================================
    # CLEANUP EXPIRED
    # =====================================================

    async def cleanup_expired(
        self,
    ) -> int:

        now = (
            datetime.utcnow()
            .isoformat()
        )

        expired = await (
            self.db.fetch_all(

                """

                SELECT memory_id
                FROM memory_store

                WHERE expires_at IS NOT NULL
                AND expires_at <= ?

                """,

                (now,),
            )
        )

        deleted = 0

        for row in expired:

            success = await (
                self.delete_memory(
                    row[
                        "memory_id"
                    ]
                )
            )

            if success:
                deleted += 1

        if deleted > 0:

            logger.info(
                "Expired memories cleaned=%s",
                deleted,
            )

        return deleted

    # =====================================================
    # STORE WITH TTL
    # =====================================================

    async def store_with_ttl(
        self,
        memory: BaseMemoryModel,
        ttl_minutes: int,
    ) -> bool:

        memory.expires_at = (

            datetime.utcnow()
            + timedelta(
                minutes=ttl_minutes
            )

        ).isoformat()

        return await (
            self.store_memory(
                memory
            )
        )

    # =====================================================
    # MEMORY STATS
    # =====================================================

    async def stats(
        self,
    ) -> dict:

        total = await (
            self.db.fetch_one(

                """

                SELECT COUNT(*) as total
                FROM memory_store

                """
            )
        )

        expired = await (
            self.db.fetch_one(

                """

                SELECT COUNT(*) as total

                FROM memory_store

                WHERE expires_at
                IS NOT NULL

                """
            )
        )

        return {

            "total_memories":
            total["total"],

            "ttl_memories":
            expired["total"],
        }
