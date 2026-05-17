from __future__ import annotations

import json
from typing import Any
from app.database.base import get_db

class RoleRepository:
    async def get_role_by_name(self, role_name: str) -> dict[str, Any] | None:
        db = await get_db()
        cursor = await db.execute(
            """
            SELECT *
            FROM roles
            WHERE name = ?
            """,
            (role_name,)
        )
        row = await cursor.fetchone()
        await cursor.close()

        if row is None:
            return None

        role = dict(row)
        role["permissions"] = json.loads(role["permissions"])
        return role

    async def list_roles(self) -> list[dict[str, Any]]:
        db = await get_db()
        cursor = await db.execute(
            """
            SELECT *
            FROM roles
            ORDER BY id ASC
            """
        )
        rows = await cursor.fetchall()
        await cursor.close()

        results = []
        for row in rows:
            role = dict(row)
            role["permissions"] = json.loads(role["permissions"])
            results.append(role)

        return results

    async def has_permission(self, role_name: str, permission: str) -> bool:
        role = await self.get_role_by_name(role_name)
        if role is None:
            return False

        permissions = role["permissions"]
        if "*" in permissions:
            return True

        return permission in permissions

    async def add_permission(self, role_name: str, permission: str) -> bool:
        role = await self.get_role_by_name(role_name)
        if role is None:
            raise ValueError(f"Role '{role_name}' does not exist")

        permissions = role["permissions"]
        if permission not in permissions:
            permissions.append(permission)

        db = await get_db()
        await db.execute(
            """
            UPDATE roles
            SET permissions = ?
            WHERE name = ?
            """,
            (json.dumps(permissions), role_name)
        )
        await db.commit()
        return True

    async def remove_permission(self, role_name: str, permission: str) -> bool:
        role = await self.get_role_by_name(role_name)
        if role is None:
            raise ValueError(f"Role '{role_name}' does not exist")

        permissions = role["permissions"]
        if permission in permissions:
            permissions.remove(permission)

        db = await get_db()
        await db.execute(
            """
            UPDATE roles
            SET permissions = ?
            WHERE name = ?
            """,
            (json.dumps(permissions), role_name)
        )
        await db.commit()
        return True

    async def create_role(self, name: str, description: str, permissions: list[str]) -> int:
        db = await get_db()
        cursor = await db.execute(
            """
            INSERT INTO roles (name, description, permissions)
            VALUES (?, ?, ?)
            """,
            (name, description, json.dumps(permissions))
        )
        await db.commit()
        return cursor.lastrowid
