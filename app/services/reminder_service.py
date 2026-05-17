from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import pytz
from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import (
    ParseMode,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =====================================================================
# FIXED: Imported scheduler_service correctly
# =====================================================================
from app.core.scheduler import (
    scheduler_service,
)
from app.database.repositories.users import (
    UserRepository,
)
from app.database.repositories.reminders import (
    ReminderRepository,
)

load_dotenv()

logger = logging.getLogger(__name__)

TIMEZONE = os.getenv(
    "TIMEZONE",
    "Asia/Bangkok"
)

timezone = pytz.timezone(
    TIMEZONE
)

user_repository = UserRepository()


class ReminderService:
    def __init__(self) -> None:
        self.repository = (
            ReminderRepository()
        )

    async def create_reminder(
        self,
        user_id: int,
        title: str,
        description: str | None,
        remind_at: datetime
    ) -> int:
        reminder_id = await (
            self.repository
            .create_reminder(
                user_id=user_id,
                title=title,
                description=description,
                remind_at=remind_at
            )
        )

        await (
            self
            .schedule_reminder_job(
                reminder_id=reminder_id,
                run_date=remind_at
            )
        )

        return reminder_id

    async def schedule_reminder_job(
        self,
        reminder_id: int,
        run_date: datetime
    ) -> None:
        # =====================================================================
        # FIXED: Linked to centralized scheduler_service
        # =====================================================================
        await scheduler_service.add_job(
            func=self.trigger_reminder,
            run_date=run_date,
            kwargs={
                "reminder_id": reminder_id
            },
            job_id=(
                f"reminder_"
                f"{reminder_id}"
            )
        )

    async def trigger_reminder(
        self,
        reminder_id: int
    ) -> None:
        logger.info(
            "Triggering scheduled "
            "reminder "
            "reminder_id=%s",
            reminder_id
        )

        reminder = await (
            self.repository
            .get_by_id(reminder_id)
        )

        if reminder is None:
            logger.warning(
                "Reminder missing "
                "at trigger "
                "reminder_id=%s",
                reminder_id
            )

            return

        if reminder.get("is_sent"):
            logger.info(
                "Reminder already "
                "delivered "
                "reminder_id=%s",
                reminder_id
            )

            return

        telegram_id = reminder.get(
            "telegram_id"
        )

        if not telegram_id:
            logger.error(
                "Telegram reference "
                "missing "
                "reminder_id=%s",
                reminder_id
            )

            return

        message = (
            "⏰ <b>Reminder "
            "Notification</b>\n\n"
            f"Title: "
            f"{reminder['title']}\n"
        )

        if reminder.get("description"):
            message += (
                f"Description: "
                f"{reminder['description']}\n"
            )

        try:
            # =====================================================================
            # FIXED: Safely fetch the centralized application bot context
            # =====================================================================
            bot = (
                scheduler_service
                .application.bot
            )

            await bot.send_message(
                chat_id=telegram_id,
                text=message,
                parse_mode=ParseMode.HTML
            )

            await (
                self.repository
                .mark_as_sent(
                    reminder_id
                )
            )

            logger.info(
                "Reminder delivered "
                "safely "
                "reminder_id=%s",
                reminder_id
            )

        except Exception:
            logger.exception(
                "Failed to deliver "
                "reminder "
                "reminder_id=%s",
                reminder_id
            )

    async def delete_reminder(
        self,
        reminder_id: int
    ) -> bool:
        # =====================================================================
        # FIXED: Removed via scheduler_service context safely
        # =====================================================================
        await scheduler_service.remove_job(
            f"reminder_{reminder_id}"
        )

        return await (
            self.repository
            .delete_reminder(
                reminder_id
            )
        )

    async def list_user_reminders(
        self,
        user_id: int
    ) -> list[dict[str, Any]]:
        return await (
            self.repository
            .list_user_reminders(
                user_id
            )
        )

    async def restore_jobs(self) -> None:
        logger.info(
            "Restoring outstanding "
            "scheduler jobs..."
        )

        # =====================================================================
        # FIXED: Mapped timezone validation dynamically
        # =====================================================================
        now = datetime.now(
            scheduler_service
            .scheduler.timezone
        )

        reminders = await (
            self.repository
            .get_due_reminders(
                start_time=now,
                end_time=datetime(
                    2035,
                    12,
                    31,
                    tzinfo=pytz.utc
                )
            )
        )

        for item in reminders:
            remind_at_str = item.get("remind_at")
            if not remind_at_str:
                continue
                
            try:
                remind_at = datetime.fromisoformat(remind_at_str)
                if remind_at.tzinfo is None:
                    remind_at = timezone.localize(remind_at)
            except ValueError:
                continue

            await (
                self
                .schedule_reminder_job(
                    reminder_id=item["id"],
                    run_date=remind_at
                )
            )

        logger.info(
            "Restored scheduler "
            "jobs count=%s",
            len(reminders)
        )


reminder_service = ReminderService()
