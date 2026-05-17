from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import suppress [cite: 681]
from dotenv import load_dotenv [cite: 681]

from app.core.scheduler import scheduler_service [cite: 423]
from app.database.base import close_database, init_db [cite: 423]
from app.database.repositories.chat_memory import chat_memory_repository [cite: 423]
from app.database.repositories.rclone_meta import RcloneMetaRepository [cite: 423]
from app.interfaces.telegram.bot import TelegramBot [cite: 423]
from app.plugins.loader import plugin_loader [cite: 423]
from app.services.reminder_service import reminder_service [cite: 423]

load_dotenv() [cite: 423]

logging.basicConfig(
    level=logging.INFO, [cite: 424]
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s" [cite: 424, 425]
)
logger = logging.getLogger(__name__) [cite: 425]

class TeleOpsApplication:
    def __init__(self) -> None:
        self.bot = TelegramBot() [cite: 426]
        self.running = False [cite: 426]
        self.shutdown_event = asyncio.Event() [cite: 426]
        self.rclone_repository = RcloneMetaRepository() [cite: 426]

    async def initialize(self) -> None:
        logger.info("Initializing database...") [cite: 426]
        await init_db() [cite: 427]

        logger.info("Initializing chat memory tables...") [cite: 427]
        await chat_memory_repository.initialize_table() [cite: 427]

        logger.info("Initializing RClone metadata tables...") [cite: 427]
        await self.rclone_repository.initialize_table() [cite: 427, 428]

        logger.info("Loading plugins...") [cite: 428]
        await plugin_loader.load_all_plugins()

        logger.info("Attaching Telegram application to scheduler...") [cite: 431, 432]
        await scheduler_service.attach_application(self.bot.application) [cite: 432]

        logger.info("Starting scheduler...") [cite: 432, 433]
        await scheduler_service.start() [cite: 433]

        logger.info("Restoring scheduled reminders...") [cite: 433]
        await reminder_service.restore_jobs() [cite: 433, 434]

        logger.info("Core initialization completed") [cite: 434]

    async def start_bot(self) -> None:
        logger.info("Starting Telegram bot...") [cite: 434]
        await self.bot.run() [cite: 434]

    async def shutdown(self) -> None:
        if not self.running: [cite: 435]
            return [cite: 435]

        logger.info("Shutdown sequence started...") [cite: 435]
        self.running = False [cite: 435]

        try:
            logger.info("Stopping Telegram bot...") [cite: 435, 436]
            await self.bot.shutdown() [cite: 436]
        except Exception:
            logger.exception("Telegram bot shutdown failed") [cite: 436]

        try:
            logger.info("Stopping scheduler...") [cite: 436, 437]
            await scheduler_service.shutdown() [cite: 437]
        except Exception:
            logger.exception("Scheduler shutdown failed") [cite: 438]

        try:
            logger.info("Closing database...") [cite: 438]
            await close_database() [cite: 439]
        except Exception:
            logger.exception("Database shutdown failed") [cite: 439]

        self.shutdown_event.set() [cite: 439]
        logger.info("TeleOps-AI shutdown completed") [cite: 439]

    async def run(self) -> None:
        try:
            self.running = True [cite: 441]
            await self.initialize() [cite: 441]
            logger.info("TeleOps-AI is fully operational") [cite: 441]
            await self.start_bot() [cite: 441]
            await self.shutdown_event.wait() [cite: 442]
        except asyncio.CancelledError:
            logger.info("Application cancelled") [cite: 442]
        except Exception:
            logger.exception("Fatal application error") [cite: 443]
        finally:
            await self.shutdown() [cite: 443]

async def main() -> None:
    application = TeleOpsApplication() [cite: 444]
    loop = asyncio.get_running_loop() [cite: 444]

    def signal_handler():
        logger.info("Shutdown signal received") [cite: 444]
        asyncio.create_task(application.shutdown()) [cite: 444]

    for sig in (signal.SIGINT, signal.SIGTERM): [cite: 445]
        try:
            loop.add_signal_handler(sig, signal_handler) [cite: 445]
        except NotImplementedError:
            logger.warning("Signal handlers not supported on this platform") [cite: 445, 446]

    await application.run() [cite: 446]

if __name__ == "__main__":
    try:
        asyncio.run(main()) [cite: 446]
    except KeyboardInterrupt:
        logger.info("Application interrupted") [cite: 447]
