from __future__ import annotations

import logging
import math
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any

from src.memory.models import (
    MemoryQueryResult,
)
from src.memory.store import (
    MemoryStore,
)

logger = logging.getLogger(__name__)


# =========================================================
# RETRIEVAL RESULT
# =========================================================


@dataclass
class RetrievalResult:

    memory_id: str

    content: str

    memory_type: str

    relevance_score: float

    metadata: dict[
        str,
        Any
    ]


# =========================================================
# LIGHTWEIGHT VECTORIZER
# =========================================================


class LightweightVectorizer:

    def __init__(
        self,
    ) -> None:

        self.stop_words = {

            "the",
            "and",
            "for",
            "with",
            "this",
            "that",
            "from",
            "have",
            "will",
            "your",
            "about",
            "into",
            "there",
            "their",
        }

    # =====================================================
    # TOKENIZE
    # =====================================================

    def tokenize(
        self,
        text: str,
    ) -> list[str]:

        text = text.lower()

        tokens = re.findall(
            r"\b[a-z0-9_]+\b",
            text,
        )

        return [

            token

            for token in tokens

            if (
                len(token) >= 3
                and token
                not in self.stop_words
            )
        ]

    # =====================================================
    # VECTORIZE
    # =====================================================

    def vectorize(
        self,
        text: str,
    ) -> Counter:

        return Counter(
            self.tokenize(text)
        )

    # =====================================================
    # COSINE SIMILARITY
    # =====================================================

    def similarity(
        self,
        text_a: str,
        text_b: str,
    ) -> float:

        vec_a = self.vectorize(
            text_a
        )

        vec_b = self.vectorize(
            text_b
        )

        intersection = set(
            vec_a.keys()
        ) & set(
            vec_b.keys()
        )

        numerator = sum(

            vec_a[x] * vec_b[x]

            for x in intersection
        )

        sum_a = sum(

            value * value

            for value in vec_a.values()
        )

        sum_b = sum(

            value * value

            for value in vec_b.values()
        )

        denominator = (
            math.sqrt(sum_a)
            * math.sqrt(sum_b)
        )

        if denominator == 0:
            return 0.0

        return round(
            numerator / denominator,
            4,
        )


# =========================================================
# MEMORY RETRIEVER
# =========================================================


class MemoryRetriever:

    def __init__(
        self,
        store: MemoryStore,
    ) -> None:

        self.store = store

        self.vectorizer = (
            LightweightVectorizer()
        )

        logger.info(
            "MemoryRetriever initialized"
        )

    # =====================================================
    # RETRIEVE
    # =====================================================

    async def retrieve(
        self,
        query: str,
        limit: int = 5,
        min_score: float = 0.05,
    ) -> list[
        RetrievalResult
    ]:

        results = await (
            self.store.search_memory(
                query=query,
                limit=50,
            )
        )

        ranked = []

        for result in results:

            score = (
                self.vectorizer
                .similarity(
                    query,
                    result.content,
                )
            )

            if score < min_score:
                continue

            ranked.append(

                RetrievalResult(

                    memory_id=result.memory_id,

                    content=result.content,

                    memory_type=(
                        result.memory_type
                    ),

                    relevance_score=score,

                    metadata=(
                        result.metadata
                    ),
                )
            )

        ranked.sort(

            key=lambda item:
            item.relevance_score,

            reverse=True,
        )

        return ranked[:limit]

    # =====================================================
    # RETRIEVE BY TYPE
    # =====================================================

    async def retrieve_by_type(
        self,
        query: str,
        memory_type: str,
        limit: int = 5,
    ) -> list[
        RetrievalResult
    ]:

        results = await (
            self.retrieve(
                query=query,
                limit=25,
            )
        )

        filtered = [

            result

            for result in results

            if (
                result.memory_type
                == memory_type
            )
        ]

        return filtered[:limit]

    # =====================================================
    # WORKFLOW HISTORY
    # =====================================================

    async def workflow_history(
        self,
        workflow_name: str,
        limit: int = 10,
    ) -> list[
        RetrievalResult
    ]:

        return await (
            self.retrieve_by_type(

                query=workflow_name,

                memory_type="workflow",

                limit=limit,
            )
        )

    # =====================================================
    # OPERATIONAL MEMORY
    # =====================================================

    async def operational_memory(
        self,
        query: str,
        limit: int = 5,
    ) -> list[
        RetrievalResult
    ]:

        return await (
            self.retrieve_by_type(

                query=query,

                memory_type="operational",

                limit=limit,
            )
        )

    # =====================================================
    # RECENT MEMORIES
    # =====================================================

    async def recent_memories(
        self,
        limit: int = 10,
    ) -> list[dict]:

        rows = await (
            self.store.db.fetch_all(

                """

                SELECT *

                FROM memory_store

                ORDER BY created_at DESC

                LIMIT ?

                """,

                (limit,),
            )
        )

        return rows

    # =====================================================
    # MEMORY INSIGHTS
    # =====================================================

    async def insights(
        self,
    ) -> dict:

        stats = await (
            self.store.stats()
        )

        recent = await (
            self.recent_memories(
                limit=5
            )
        )

        return {

            "memory_stats":
            stats,

            "recent_memory_count":
            len(recent),
        }
