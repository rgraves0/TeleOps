from __future__ import annotations

import logging
from dataclasses import dataclass

from src.core.provider_manager import (
    ProviderManager,
)

logger = logging.getLogger(__name__)


@dataclass
class EmailSummary:

    subject: str

    sender: str

    summary: str

    importance: str

    action_required: bool


class EmailSummarizer:

    def __init__(
        self,
        provider_manager: (
            ProviderManager
        ),
        provider_name: str,
    ) -> None:

        self.provider_manager = (
            provider_manager
        )

        self.provider_name = (
            provider_name
        )

    # =====================================================
    # SINGLE SUMMARY
    # =====================================================

    async def summarize_email(
        self,
        subject: str,
        sender: str,
        body: str,
    ) -> EmailSummary:

        prompt = f"""
You are an AI email assistant.

Analyze the following email.

Subject:
{subject}

Sender:
{sender}

Body:
{body}

Instructions:
- Summarize clearly
- Detect urgency
- Detect action required
- Keep concise
- Maximum 120 words

Return format:

SUMMARY:
...

IMPORTANCE:
low/medium/high

ACTION_REQUIRED:
yes/no
"""

        messages = [

            {
                "role": "system",
                "content": (
                    "You summarize emails."
                ),
            },

            {
                "role": "user",
                "content": prompt,
            },
        ]

        response = await (
            self.provider_manager
            .generate(
                provider_name=(
                    self.provider_name
                ),
                messages=messages,
                temperature=0.2,
                max_tokens=300,
            )
        )

        parsed = (
            self._parse_summary(
                response.content
            )
        )

        return EmailSummary(
            subject=subject,
            sender=sender,
            summary=parsed[
                "summary"
            ],
            importance=parsed[
                "importance"
            ],
            action_required=parsed[
                "action_required"
            ],
        )

    # =====================================================
    # BULK SUMMARY
    # =====================================================

    async def summarize_batch(
        self,
        emails: list[dict],
    ) -> list[EmailSummary]:

        results: list[
            EmailSummary
        ] = []

        for email in emails:

            try:

                summary = await (
                    self.summarize_email(
                        subject=email[
                            "subject"
                        ],
                        sender=email[
                            "sender"
                        ],
                        body=email[
                            "body"
                        ],
                    )
                )

                results.append(
                    summary
                )

            except Exception:

                logger.exception(
                    "Email summary failed"
                )

        return results

    # =====================================================
    # PARSE SUMMARY
    # =====================================================

    def _parse_summary(
        self,
        raw_response: str,
    ) -> dict:

        lines = (
            raw_response
            .strip()
            .splitlines()
        )

        summary = []

        importance = "medium"

        action_required = False

        current_section = None

        for line in lines:

            cleaned = (
                line.strip()
            )

            upper = (
                cleaned.upper()
            )

            if upper.startswith(
                "SUMMARY:"
            ):

                current_section = (
                    "summary"
                )

                summary_text = (
                    cleaned.replace(
                        "SUMMARY:",
                        "",
                    ).strip()
                )

                if summary_text:

                    summary.append(
                        summary_text
                    )

                continue

            if upper.startswith(
                "IMPORTANCE:"
            ):

                current_section = None

                importance = (
                    cleaned.replace(
                        "IMPORTANCE:",
                        "",
                    )
                    .strip()
                    .lower()
                )

                continue

            if upper.startswith(
                "ACTION_REQUIRED:"
            ):

                current_section = None

                value = (
                    cleaned.replace(
                        "ACTION_REQUIRED:",
                        "",
                    )
                    .strip()
                    .lower()
                )

                action_required = (
                    value == "yes"
                )

                continue

            if (
                current_section
                == "summary"
            ):

                summary.append(
                    cleaned
                )

        return {

            "summary": "\n".join(
                summary
            ).strip(),

            "importance":
            importance,

            "action_required":
            action_required,
        }
