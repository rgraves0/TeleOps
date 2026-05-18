from __future__ import annotations

import asyncio
import email
import imaplib
import logging
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from typing import Any

from src.utils.error_handling import (
    ProviderUnavailableError,
)

logger = logging.getLogger(__name__)


@dataclass
class MailAttachment:

    filename: str

    content_type: str

    size: int


@dataclass
class MailMessage:

    uid: str

    subject: str

    sender: str

    recipients: list[str]

    body: str

    html_body: str | None

    attachments: list[
        MailAttachment
    ]

    received_at: str


class AsyncIMAPClient:

    def __init__(
        self,
        imap_host: str,
        imap_port: int,
        smtp_host: str,
        smtp_port: int,
        username: str,
        password: str,
        use_ssl: bool = True,
    ) -> None:

        self.imap_host = imap_host
        self.imap_port = imap_port

        self.smtp_host = smtp_host
        self.smtp_port = smtp_port

        self.username = username
        self.password = password

        self.use_ssl = use_ssl

        self.imap: (
            imaplib.IMAP4_SSL
            | None
        ) = None

    # =====================================================
    # CONNECT
    # =====================================================

    async def connect(
        self,
    ) -> None:

        try:

            logger.info(
                "Connecting to IMAP..."
            )

            self.imap = await asyncio.to_thread(
                imaplib.IMAP4_SSL,
                self.imap_host,
                self.imap_port,
            )

            await asyncio.to_thread(
                self.imap.login,
                self.username,
                self.password,
            )

            logger.info(
                "IMAP connected"
            )

        except Exception as exc:

            logger.exception(
                "IMAP connection failed"
            )

            raise ProviderUnavailableError(
                str(exc)
            ) from exc

    # =====================================================
    # DISCONNECT
    # =====================================================

    async def disconnect(
        self,
    ) -> None:

        if self.imap is None:
            return

        try:

            await asyncio.to_thread(
                self.imap.logout
            )

            logger.info(
                "IMAP disconnected"
            )

        except Exception:

            logger.exception(
                "Failed to disconnect IMAP"
            )

    # =====================================================
    # FETCH UNREAD
    # =====================================================

    async def fetch_unread_messages(
        self,
        limit: int = 10,
    ) -> list[MailMessage]:

        if self.imap is None:

            raise RuntimeError(
                "IMAP not connected"
            )

        await asyncio.to_thread(
            self.imap.select,
            "INBOX",
        )

        status, messages = (
            await asyncio.to_thread(
                self.imap.search,
                None,
                "UNSEEN",
            )
        )

        if status != "OK":

            raise RuntimeError(
                "Failed to search inbox"
            )

        message_ids = (
            messages[0]
            .decode()
            .split()
        )

        message_ids = (
            message_ids[-limit:]
        )

        results: list[
            MailMessage
        ] = []

        for msg_id in reversed(
            message_ids
        ):

            fetched = (
                await asyncio.to_thread(
                    self.imap.fetch,
                    msg_id,
                    "(RFC822)",
                )
            )

            _, data = fetched

            raw_email = (
                data[0][1]
            )

            parsed = (
                email.message_from_bytes(
                    raw_email
                )
            )

            results.append(
                self._parse_message(
                    uid=msg_id,
                    parsed=parsed,
                )
            )

        return results

    # =====================================================
    # SEND MAIL
    # =====================================================

    async def send_email(
        self,
        recipient: str,
        subject: str,
        body: str,
        html_body: str | None = None,
        attachments: list[
            tuple[str, bytes]
        ] | None = None,
    ) -> None:

        message = EmailMessage()

        message["From"] = (
            self.username
        )

        message["To"] = recipient

        message["Subject"] = (
            subject
        )

        message.set_content(body)

        if html_body:

            message.add_alternative(
                html_body,
                subtype="html",
            )

        # =================================================
        # ATTACHMENTS
        # =================================================

        if attachments:

            for (
                filename,
                content,
            ) in attachments:

                message.add_attachment(
                    content,
                    maintype="application",
                    subtype="octet-stream",
                    filename=filename,
                )

        try:

            async def _send():

                with smtplib.SMTP_SSL(
                    self.smtp_host,
                    self.smtp_port,
                ) as smtp:

                    smtp.login(
                        self.username,
                        self.password,
                    )

                    smtp.send_message(
                        message
                    )

            await asyncio.to_thread(
                _send
            )

            logger.info(
                "Email sent to=%s",
                recipient,
            )

        except Exception as exc:

            logger.exception(
                "Send email failed"
            )

            raise ProviderUnavailableError(
                str(exc)
            ) from exc

    # =====================================================
    # PARSE MESSAGE
    # =====================================================

    def _parse_message(
        self,
        uid: str,
        parsed: Any,
    ) -> MailMessage:

        subject = (
            parsed.get(
                "Subject",
                "",
            )
        )

        sender = (
            parsed.get(
                "From",
                "",
            )
        )

        recipients = (
            parsed.get(
                "To",
                "",
            ).split(",")
        )

        date_header = (
            parsed.get(
                "Date",
                "",
            )
        )

        received_at = ""

        try:

            received_at = (
                parsedate_to_datetime(
                    date_header
                ).isoformat()
            )

        except Exception:

            received_at = (
                date_header
            )

        body = ""

        html_body = None

        attachments: list[
            MailAttachment
        ] = []

        if parsed.is_multipart():

            for part in (
                parsed.walk()
            ):

                content_type = (
                    part.get_content_type()
                )

                disposition = (
                    str(
                        part.get(
                            "Content-Disposition"
                        )
                    )
                )

                if (
                    "attachment"
                    in disposition
                ):

                    payload = (
                        part.get_payload(
                            decode=True
                        )
                        or b""
                    )

                    attachments.append(
                        MailAttachment(
                            filename=(
                                part.get_filename()
                                or "unknown"
                            ),
                            content_type=(
                                content_type
                            ),
                            size=len(
                                payload
                            ),
                        )
                    )

                    continue

                payload = (
                    part.get_payload(
                        decode=True
                    )
                )

                if payload is None:
                    continue

                decoded = (
                    payload.decode(
                        errors="ignore"
                    )
                )

                if (
                    content_type
                    == "text/plain"
                ):

                    body += decoded

                elif (
                    content_type
                    == "text/html"
                ):

                    html_body = decoded

        else:

            payload = (
                parsed.get_payload(
                    decode=True
                )
                or b""
            )

            body = payload.decode(
                errors="ignore"
            )

        return MailMessage(
            uid=uid,
            subject=subject,
            sender=sender,
            recipients=recipients,
            body=body.strip(),
            html_body=html_body,
            attachments=attachments,
            received_at=received_at,
        )
