from __future__ import annotations

import asyncio
import email
import imaplib
import logging
from email.header import decode_header
from typing import Any

from app.database.repositories.inboxes import (
    InboxRepository,
)

logger = logging.getLogger(__name__)


class InboxServiceError(Exception):
    pass


class InboxService:
    def __init__(self):
        self.repository = InboxRepository()

    async def fetch_emails(
        self,
        user_id: int,
        inbox_id: int,
        imap_host: str,
        email_address: str,
        password: str,
        folder: str = "INBOX",
        limit: int = 10
    ) -> list[dict[str, Any]]:
        has_access = (
            await self.repository
            .user_has_access(
                user_id=user_id,
                inbox_id=inbox_id
            )
        )

        if not has_access:
            raise InboxServiceError(
                "Inbox access denied"
            )

        return await asyncio.to_thread(
            self._fetch_emails_sync,
            imap_host,
            email_address,
            password,
            folder,
            limit
        )

    def _fetch_emails_sync(
        self,
        imap_host: str,
        email_address: str,
        password: str,
        folder: str,
        limit: int
    ) -> list[dict[str, Any]]:
        try:
            mail = imaplib.IMAP4_SSL(
                imap_host
            )

            mail.login(
                email_address,
                password
            )

            mail.select(folder)

            status, messages = mail.search(
                None,
                "ALL"
            )

            if status != "OK":
                raise InboxServiceError(
                    "Failed to fetch emails"
                )

            email_ids = (
                messages[0]
                .split()
            )

            latest_ids = (
                email_ids[-limit:]
            )

            results = []

            for email_id in reversed(
                latest_ids
            ):
                status, msg_data = mail.fetch(
                    email_id,
                    "(RFC822)"
                )

                if status != "OK":
                    continue

                raw_email = (
                    msg_data[0][1]
                )

                parsed_email = (
                    email.message_from_bytes(
                        raw_email
                    )
                )

                subject = (
                    self._decode_header(
                        parsed_email.get(
                            "Subject",
                            ""
                        )
                    )
                )

                sender = (
                    parsed_email.get(
                        "From",
                        ""
                    )
                )

                date = (
                    parsed_email.get(
                        "Date",
                        ""
                    )
                )

                body = (
                    self._extract_body(
                        parsed_email
                    )
                )

                results.append({
                    "subject": subject,
                    "from": sender,
                    "date": date,
                    "body": body[:2000]
                })

            mail.logout()

            return results

        except Exception as exc:
            logger.exception(
                "IMAP fetch failed: %s",
                exc
            )

            raise InboxServiceError(
                f"Failed to fetch emails: {exc}"
            ) from exc

    def _decode_header(
        self,
        value: str
    ) -> str:
        decoded_parts = decode_header(
            value
        )

        decoded_string = ""

        for text, encoding in decoded_parts:
            if isinstance(text, bytes):
                decoded_string += text.decode(
                    encoding or "utf-8",
                    errors="ignore"
                )

            else:
                decoded_string += text

        return decoded_string

    def _extract_body(
        self,
        message
    ) -> str:
        if message.is_multipart():
            for part in message.walk():
                content_type = (
                    part.get_content_type()
                )

                content_disposition = str(
                    part.get(
                        "Content-Disposition"
                    )
                )

                if (
                    content_type
                    == "text/plain"
                    and "attachment"
                    not in content_disposition
                ):
                    payload = (
                        part.get_payload(
                            decode=True
                        )
                    )

                    if payload:
                        return payload.decode(
                            errors="ignore"
                        )

        else:
            payload = message.get_payload(
                decode=True
            )

            if payload:
                return payload.decode(
                    errors="ignore"
                )

        return ""
