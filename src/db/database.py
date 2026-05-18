from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)


# =========================================================
# DATABASE CONFIG
# =========================================================


DEFAULT_DB_PATH = (
    "storage/teleops.db"
)

SQLITE_PRAGMAS = [

    "PRAGMA journal_mode=WAL;",

    "PRAGMA synchronous=NORMAL;",

    "PRAGMA temp_store=MEMORY;",

    "PRAGMA foreign_keys=ON;",

    "PRAGMA cache_size=-10000;",

    "PRAGMA mmap_size=268435456;",

    "PRAGMA busy_timeout=5000;",
]


# =========================================================
# DATABASE MANAGER
# =========================================================


class DatabaseManager:

    def __init__(
        self,
        db_path: str = (
            DEFAULT_DB_PATH
        ),
    ) -> None:

        self.db_path = db_path

        self.connection: (
            aiosqlite.Connection
            | None
        ) = None

        self.lock = (
            asyncio.Lock()
        )

        logger.info(
            "DatabaseManager initialized"
        )

    # =====================================================
    # CONNECT
    # =====================================================

    async def connect(
        self,
    ) -> None:

        async with self.lock:

            if self.connection:
                return

            Path(
                self.db_path
            ).parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            self.connection = (
                await aiosqlite.connect(
                    self.db_path,
                    isolation_level=None,
                )
            )

            self.connection.row_factory = (
                aiosqlite.Row
            )

            for pragma in (
                SQLITE_PRAGMAS
            ):

                await (
                    self.connection.execute(
                        pragma
                    )
                )

            logger.info(
                "SQLite connected "
                "with WAL mode"
            )

    # =====================================================
    # CLOSE
    # =====================================================

    async def close(
        self,
    ) -> None:

        async with self.lock:

            if not self.connection:
                return

            await (
                self.connection.close()
            )

            self.connection = None

            logger.warning(
                "Database connection closed"
            )

    # =====================================================
    # EXECUTE
    # =====================================================

    async def execute(
        self,
        query: str,
        params: tuple = (),
    ) -> aiosqlite.Cursor:

        if not self.connection:

            raise RuntimeError(
                "Database not connected"
            )

        try:

            cursor = await (
                self.connection.execute(
                    query,
                    params,
                )
            )

            return cursor

        except Exception:

            logger.exception(
                "Database execute failed"
            )

            raise

    # =====================================================
    # EXECUTEMANY
    # =====================================================

    async def executemany(
        self,
        query: str,
        params: list[
            tuple
        ],
    ) -> None:

        if not self.connection:

            raise RuntimeError(
                "Database not connected"
            )

        try:

            await (
                self.connection.executemany(
                    query,
                    params,
                )
            )

        except Exception:

            logger.exception(
                "Database executemany failed"
            )

            raise

    # =====================================================
    # FETCH ONE
    # =====================================================

    async def fetch_one(
        self,
        query: str,
        params: tuple = (),
    ) -> dict | None:

        cursor = await (
            self.execute(
                query,
                params,
            )
        )

        row = await (
            cursor.fetchone()
        )

        if row is None:
            return None

        return dict(row)

    # =====================================================
    # FETCH ALL
    # =====================================================

    async def fetch_all(
        self,
        query: str,
        params: tuple = (),
    ) -> list[dict]:

        cursor = await (
            self.execute(
                query,
                params,
            )
        )

        rows = await (
            cursor.fetchall()
        )

        return [
            dict(row)
            for row in rows
        ]

    # =====================================================
    # TRANSACTION
    # =====================================================

    @asynccontextmanager
    async def transaction(
        self,
    ) -> AsyncGenerator[
        aiosqlite.Connection,
        None,
    ]:

        if not self.connection:

            raise RuntimeError(
                "Database not connected"
            )

        try:

            await (
                self.connection.execute(
                    "BEGIN"
                )
            )

            yield self.connection

            await (
                self.connection.commit()
            )

        except Exception:

            await (
                self.connection.rollback()
            )

            logger.exception(
                "Transaction rolled back"
            )

            raise

    # =====================================================
    # VACUUM
    # =====================================================

    async def vacuum(
        self,
    ) -> None:

        if not self.connection:
            return

        await (
            self.connection.execute(
                "VACUUM"
            )
        )

        logger.info(
            "Database vacuum completed"
        )

    # =====================================================
    # HEALTHCHECK
    # =====================================================

    async def healthcheck(
        self,
    ) -> dict:

        try:

            result = await (
                self.fetch_one(
                    "SELECT 1 as ok"
                )
            )

            return {

                "healthy":
                result is not None,

                "database":
                self.db_path,
            }

        except Exception:

            logger.exception(
                "Database healthcheck failed"
            )

            return {

                "healthy":
                False,
            }


# =========================================================
# GLOBAL DATABASE
# =========================================================


database = DatabaseManager()
