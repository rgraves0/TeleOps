from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import suppress

from dotenv import load_dotenv
from telegram import Update

from app.core.scheduler import (
    scheduler_manager,
)
from app.database.base import (
    db,
    init_db,
)
from app.database.repositories.rclone_meta import (
    RcloneMetaRepository,
)
from app.interfaces.telegram.bot import (
    TelegramBot,
)
from app.plugins.loader import (
    plugin_loader,
)
from app.services.reminder_service import (
    ReminderService,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(name)s | "
        "%(message)s"
    )
)

logger = logging.getLogger(__name__)


class TeleOpsApplication:
    def __init__(self):
        self.bot = TelegramBot()

        self.reminder_service = (
            ReminderService()
        )

        self.rclone_repository = (
            RcloneMetaRepository()
        )

        self.shutdown_event = (
            asyncio.Event()
        )

    async def initialize(self) -> None:
        logger.info(
            "Initializing database..."
        )

        await init_db()

        await (
            self.rclone_repository
            .initialize_table()
        )

        logger.info(
            "Database initialized"
        )

        logger.info(
            "Loading plugins..."
        )

        plugin_loader.load_all_plugins()

        plugins = (
            plugin_loader.list_plugins()
        )

        for plugin in plugins:
            logger.info(
                "Plugin loaded | "
                "name=%s | enabled=%s",
                plugin["name"],
                plugin["enabled"]
            )

        logger.info(
            "Attaching Telegram application "
            "to scheduler..."
        )

        scheduler_manager.attach_application(
            self.bot.application
        )

        logger.info(
            "Starting scheduler..."
        )

        scheduler_manager.start()

        logger.info(
            "Restoring scheduled reminders..."
        )

        await (
            self.reminder_service
            .restore_pending_reminders()
        )

        logger.info(
            "Application initialization completed"
        )

    async def start_bot(self) -> None:
        logger.info(
            "Configuring Telegram bot..."
        )

        self.bot.setup()

        application = (
            self.bot.application
        )

        logger.info(
            "Initializing Telegram application..."
        )

        await application.initialize()

        logger.info(
            "Starting Telegram application..."
        )

        await application.start()

        logger.info(
            "Starting Telegram polling..."
        )

        if application.updater is None:
            raise RuntimeError(
                "Telegram updater is unavailable"
            )

        await application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=False
        )

        logger.info(
            "Telegram bot is now online"
        )

    async def stop_bot(self) -> None:
        application = (
            self.bot.application
        )

        logger.info(
            "Stopping Telegram polling..."
        )

        if application.updater:
            with suppress(Exception):
                await application.updater.stop()

        logger.info(
            "Stopping Telegram application..."
        )

        with suppress(Exception):
            await application.stop()

        logger.info(
            "Shutting down Telegram application..."
        )

        with suppress(Exception):
            await application.shutdown()

    async def shutdown(self) -> None:
        logger.info(
            "Graceful shutdown initiated..."
        )

        await self.stop_bot()

        logger.info(
            "Stopping scheduler..."
        )

        await scheduler_manager.shutdown()

        logger.info(
            "Disconnecting database..."
        )

        await db.disconnect()

        logger.info(
            "TeleOps-AI shutdown completed"
        )

    def register_signal_handlers(
        self
    ) -> None:
        loop = asyncio.get_running_loop()

        for sig in (
            signal.SIGINT,
            signal.SIGTERM
        ):
            loop.add_signal_handler(
                sig,
                self.shutdown_event.set
            )

    async def run(self) -> None:
        self.register_signal_handlers()

        await self.initialize()

        await self.start_bot()

        logger.info(
            "TeleOps-AI fully operational"
        )

        await self.shutdown_event.wait()

        logger.info(
            "Shutdown signal received"
        )

        await self.shutdown()


async def main() -> None:
    application = (
        TeleOpsApplication()
    )

    try:
        await application.run()

    except KeyboardInterrupt:
        logger.warning(
            "Keyboard interrupt received"
        )

        await application.shutdown()

    except Exception:
        logger.exception(
            "Fatal application error"
        )

        await application.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
