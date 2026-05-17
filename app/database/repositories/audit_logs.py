from __future__ import annotations

from typing import Any
from app.database.base import get_db

class AuditLogRepository:
    async def create_entry(
        self,
        user_id: int | None,
        action: str,
        target_type: str | None = None,
        target_id: str | None = None,
        details: str | None = None,
        ip_address: str | None = None
    ) -> int:
        db = await get_db()
        cursor = await db.execute(
            """
            INSERT INTO audit_logs (user_id, action, target_type, target_id, details, ip_address)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, action, target_type, target_id, details, ip_address)
        )
        await db.commit()
        return cursor.lastrowid

    async def list_entries(self, limit: int = 100) -> list[dict[str, Any]]:
        db = await get_db()
        cursor = await db.execute(
            """
            SELECT id, user_id, action, target_type, target_id, details, ip_address, created_at
            FROM audit_logs
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]
