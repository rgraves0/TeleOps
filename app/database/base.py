from __future__ import annotations

import json
from pathlib import Path
from typing import AsyncGenerator

import aiosqlite

from app.database.models import (
    CREATE_AUDIT_LOGS_TABLE,
    CREATE_INDEXES,
    CREATE_INBOXES_TABLE,
    CREATE_REMINDERS_TABLE,
    CREATE_ROLES_TABLE,
    CREATE_USER_INBOXES_TABLE,
    CREATE_USERS_TABLE,
    DEFAULT_ROLES,
)

DATABASE_PATH = Path("data/teleops.db")


class DatabaseManager:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._connection: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        if self._connection is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            self._connection = await aiosqlite.connect(
                self.db_path.as_posix()
            )

            self._connection.row_factory = aiosqlite.Row

            await self._connection.execute(
                "PRAGMA foreign_keys = ON;"
            )

            await self._connection.execute(
                "PRAGMA journal_mode = WAL;"
            )

            await self._connection.execute(
                "PRAGMA synchronous = NORMAL;"
            )

            await self._connection.commit()

    async def disconnect(self) -> None:
        if self._connection:
            await self._connection.close()
            self._connection = None

    async def get_connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            await self.connect()

        return self._connection

    async def execute(
        self,
        query: str,
        parameters: tuple = ()
    ) -> None:
        connection = await self.get_connection()

        await connection.execute(query, parameters)
        await connection.commit()

    async def fetch_one(
        self,
        query: str,
        parameters: tuple = ()
    ):
        connection = await self.get_connection()

        cursor = await connection.execute(query, parameters)
        row = await cursor.fetchone()

        await cursor.close()

        return row

    async def fetch_all(
        self,
        query: str,
        parameters: tuple = ()
    ):
        connection = await self.get_connection()

        cursor = await connection.execute(query, parameters)
        rows = await cursor.fetchall()

        await cursor.close()

        return rows


db = DatabaseManager(DATABASE_PATH)


async def init_db() -> None:
    connection = await db.get_connection()

    await connection.execute(CREATE_ROLES_TABLE)
    await connection.execute(CREATE_USERS_TABLE)
    await connection.execute(CREATE_INBOXES_TABLE)
    await connection.execute(CREATE_REMINDERS_TABLE)
    await connection.execute(CREATE_AUDIT_LOGS_TABLE)
    await connection.execute(CREATE_USER_INBOXES_TABLE)

    for query in CREATE_INDEXES:
        await connection.execute(query)

    await connection.commit()

    await seed_default_roles()


async def seed_default_roles() -> None:
    connection = await db.get_connection()

    for role in DEFAULT_ROLES:
        cursor = await connection.execute(
            """
            SELECT id
            FROM roles
            WHERE name = ?
            """,
            (role["name"],)
        )

        existing_role = await cursor.fetchone()

        if existing_role:
            continue

        await connection.execute(
            """
            INSERT INTO roles (
                name,
                description,
                permissions
            )
            VALUES (?, ?, ?)
            """,
            (
                role["name"],
                role["description"],
                json.dumps(role["permissions"])
            )
        )

    await connection.commit()


async def get_db() -> AsyncGenerator[aiosqlite.Connection, None]:
    connection = await db.get_connection()

    try:
        yield connection
    finally:
        pass
