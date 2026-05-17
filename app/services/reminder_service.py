from __future__ import annotations

import logging
import os
from datetime import (
    datetime,
    timedelta,
)

import pytz

from app.core.scheduler import (
    scheduler_manager,
)
from app.database.repositories.reminders import (
    ReminderRepository,
)

logger = logging.getLogger(__name__)


class ReminderService:
    def __init__(self):
        self.repository = (
            ReminderRepository()
        )

        timezone_name = os.getenv(
            "TIMEZONE",
            "Asia/Bangkok"
        )

        self.timezone = pytz.timezone(
            timezone_name
        )

    async def create_reminder(
        self,
        user_id: int,
        title: str,
        description: str | None,
        remind_at: datetime
    ) -> int:
        remind_at = (
            self._ensure_timezone(
                remind_at
            )
        )

        reminder_id = (
            await self.repository
            .create_reminder(
                user_id=user_id,
                title=title,
                description=description,
                remind_at=remind_at
            )
        )

        reminder = (
            await self.repository
            .get_by_id(reminder_id)
        )

        if reminder is None:
            raise RuntimeError(
                "Reminder creation failed"
            )

        await self.schedule_reminder_jobs(
            reminder
        )

        logger.info(
            "Reminder created: %s",
            reminder_id
        )

        return reminder_id

    async def update_reminder(
        self,
        reminder_id: int,
        title: str,
        description: str | None,
        remind_at: datetime
    ) -> bool:
        remind_at = (
            self._ensure_timezone(
                remind_at
            )
        )

        await self.repository.update_reminder(
            reminder_id=reminder_id,
            title=title,
            description=description,
            remind_at=remind_at
        )

        reminder = (
            await self.repository
            .get_by_id(reminder_id)
        )

        if reminder is None:
            return False

        self.remove_reminder_jobs(
            reminder_id
        )

        await self.schedule_reminder_jobs(
            reminder
        )

        logger.info(
            "Reminder updated: %s",
            reminder_id
        )

        return True

    async def delete_reminder(
        self,
        reminder_id: int
    ) -> bool:
        self.remove_reminder_jobs(
            reminder_id
        )

        await self.repository.delete_reminder(
            reminder_id
        )

        logger.info(
            "Reminder deleted: %s",
            reminder_id
        )

        return True

    async def list_user_reminders(
        self,
        user_id: int
    ):
        return (
            await self.repository
            .list_user_reminders(
                user_id
            )
        )

    async def schedule_reminder_jobs(
        self,
        reminder: dict
    ) -> None:
        reminder_id = reminder["id"]

        remind_at = datetime.fromisoformat(
            reminder["remind_at"]
        )

        twelve_hours_before = (
            remind_at - timedelta(hours=12)
        )

        one_hour_before = (
            remind_at - timedelta(hours=1)
        )

        now = datetime.now(
            self.timezone
        )

        if twelve_hours_before > now:
            scheduler_manager.add_job(
                job_id=(
                    f"reminder_12h_"
                    f"{reminder_id}"
                ),
                func=self.send_12h_notification,
                run_date=twelve_hours_before,
                kwargs={
                    "reminder_id": reminder_id
                }
            )

        if one_hour_before > now:
            scheduler_manager.add_job(
                job_id=(
                    f"reminder_1h_"
                    f"{reminder_id}"
                ),
                func=self.send_1h_notification,
                run_date=one_hour_before,
                kwargs={
                    "reminder_id": reminder_id
                }
            )

        if remind_at > now:
            scheduler_manager.add_job(
                job_id=(
                    f"reminder_exact_"
                    f"{reminder_id}"
                ),
                func=self.send_event_notification,
                run_date=remind_at,
                kwargs={
                    "reminder_id": reminder_id
                }
            )

    def remove_reminder_jobs(
        self,
        reminder_id: int
    ) -> None:
        scheduler_manager.remove_job(
            f"reminder_12h_{reminder_id}"
        )

        scheduler_manager.remove_job(
            f"reminder_1h_{reminder_id}"
        )

        scheduler_manager.remove_job(
            f"reminder_exact_{reminder_id}"
        )

    async def send_12h_notification(
        self,
        reminder_id: int
    ) -> None:
        await self._send_notification(
            reminder_id=reminder_id,
            prefix="⏰ Reminder (12 Hours Left)"
        )

    async def send_1h_notification(
        self,
        reminder_id: int
    ) -> None:
        await self._send_notification(
            reminder_id=reminder_id,
            prefix="⚠️ Reminder (1 Hour Left)"
        )

    async def send_event_notification(
        self,
        reminder_id: int
    ) -> None:
        await self._send_notification(
            reminder_id=reminder_id,
            prefix="🚀 Event Starting Now"
        )

        await self.repository.mark_as_sent(
            reminder_id
        )

    async def _send_notification(
        self,
        reminder_id: int,
        prefix: str
    ) -> None:
        reminder = (
            await self.repository
            .get_by_id(reminder_id)
        )

        if reminder is None:
            logger.warning(
                "Reminder not found: %s",
                reminder_id
            )

            return

        bot = (
            scheduler_manager.get_bot()
        )

        telegram_id = (
            reminder["telegram_id"]
        )

        remind_at = datetime.fromisoformat(
            reminder["remind_at"]
        )

        formatted_time = (
            remind_at.strftime(
                "%Y-%m-%d %H:%M"
            )
        )

        message = (
            f"{prefix}\n\n"
            f"📌 Title: {reminder['title']}\n"
            f"🕒 Time: {formatted_time}\n"
        )

        if reminder["description"]:
            message += (
                f"📝 Description: "
                f"{reminder['description']}\n"
            )

        try:
            await bot.send_message(
                chat_id=telegram_id,
                text=message
            )

            logger.info(
                "Reminder notification sent "
                "to %s",
                telegram_id
            )

        except Exception as exc:
            logger.exception(
                "Failed to send reminder "
                "notification: %s",
                exc
            )

    async def restore_pending_reminders(
        self
    ) -> None:
        now = datetime.now(
            self.timezone
        )

        future_end = (
            now + timedelta(days=365)
        )

        reminders = (
            await self.repository
            .get_due_reminders(
                start_time=now,
                end_time=future_end
            )
        )

        for reminder in reminders:
            try:
                await self.schedule_reminder_jobs(
                    reminder
                )

            except Exception as exc:
                logger.exception(
                    "Failed to restore "
                    "reminder %s: %s",
                    reminder["id"],
                    exc
                )

    def _ensure_timezone(
        self,
        dt: datetime
    ) -> datetime:
        if dt.tzinfo is None:
            return self.timezone.localize(
                dt
            )

        return dt.astimezone(
            self.timezone
        )
