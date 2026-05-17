from __future__ import annotations

import asyncio
import logging
import signal

from dotenv import load_dotenv

from app.core.scheduler import (
    scheduler_service,
)
from app.database.base import (
    close_database,
    init_db,
)
from app.database.repositories.chat_memory import (
    chat_memory_repository,
)
from app.database.repositories.rclone_meta import (
    RCloneMetaRepository,
)
from app.interfaces.telegram.bot import (
    TelegramBot,
)
from app.plugins.loader import (
    plugin_loader,
)
from app.services.reminder_service import (
    reminder_service,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(name)s | "
        "%(levelname)s | "
        "%(message)s"
    )
)

logger = logging.getLogger(
    __name__
)


class TeleOpsApplication:
    def __init__(self):
        self.bot = TelegramBot()

        self.running = False

        self.shutdown_event = (
            asyncio.Event()
        )

    async def initialize(
        self
    ) -> None:
        logger.info(
            "Initializing database..."
        )

        await init_db()

        logger.info(
            "Initializing chat "
            "memory tables..."
        )

        await (
            chat_memory_repository
            .initialize_table()
        )

        logger.info(
            "Initializing RClone "
            "metadata tables..."
        )

        rclone_repository = (
            RCloneMetaRepository()
        )

        await (
            rclone_repository
            .initialize_table()
        )

        logger.info(
            "Loading plugins..."
        )

        await plugin_loader.load_plugins()

        for plugin in (
            plugin_loader
            .list_plugins()
        ):
            logger.info(
                "Plugin loaded | "
                "name=%s | enabled=%s",
                plugin["name"],
                plugin["enabled"]
            )

        logger.info(
            "Attaching Telegram "
            "application to scheduler..."
        )

        scheduler_service.attach_application(
            self.bot.application
        )

        logger.info(
            "Starting scheduler..."
        )

        await scheduler_service.start()

        logger.info(
            "Restoring scheduled "
            "reminders..."
        )

        await (
            reminder_service
            .restore_jobs()
        )

        logger.info(
            "Core initialization "
            "completed"
        )

    async def start_bot(
        self
    ) -> None:
        logger.info(
            "Starting Telegram bot..."
        )

        await self.bot.run()

    async def shutdown(
        self
    ) -> None:
        if not self.running:
            return

        self.running = False

        logger.info(
            "Shutting down "
            "TeleOps-AI..."
        )

        try:
            await scheduler_service.shutdown()

        except Exception:
            logger.exception(
                "Scheduler shutdown failed"
            )

        try:
            await self.bot.shutdown()

        except Exception:
            logger.exception(
                "Telegram bot shutdown "
                "failed"
            )

        try:
            await close_database()

        except Exception:
            logger.exception(
                "Database shutdown failed"
            )

        logger.info(
            "TeleOps-AI shutdown "
            "completed"
        )

        self.shutdown_event.set()

    async def run(
        self
    ) -> None:
        self.running = True

        await self.initialize()

        logger.info(
            "TeleOps-AI is fully "
            "operational"
        )

        await self.start_bot()


async def main() -> None:
    application = (
        TeleOpsApplication()
    )

    loop = asyncio.get_running_loop()

    def signal_handler() -> None:
        logger.info(
            "Shutdown signal received"
        )

        asyncio.create_task(
            application.shutdown()
        )

    for sig in (
        signal.SIGINT,
        signal.SIGTERM
    ):
        loop.add_signal_handler(
            sig,
            signal_handler
        )

    try:
        await application.run()

    finally:
        await application.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
