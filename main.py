from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import suppress

from dotenv import load_dotenv

from app.database.base import init_db, close_database
from app.core.scheduler import scheduler_service
from app.interfaces.telegram.bot import TelegramBot

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("teleops")


class TeleOpsApplication:

    def __init__(self):

        self.shutdown_event = asyncio.Event()

        logger.info("Bootstrapping lightweight Telegram runtime...")

        self.bot = TelegramBot()

    async def initialize(self):

        logger.info("Initializing database...")
        await init_db()

        logger.info("Starting scheduler...")
        await scheduler_service.start()

    async def start(self):

        await self.initialize()

        logger.info("Starting telegram bot...")

        await self.bot.run()

    async def shutdown(self):

        logger.warning("Shutdown initiated...")

        with suppress(Exception):
            await scheduler_service.shutdown()

        with suppress(Exception):
            await self.bot.shutdown()

        with suppress(Exception):
            await close_database()

        self.shutdown_event.set()

        logger.warning("Shutdown complete")


async def main():

    app = TeleOpsApplication()

    loop = asyncio.get_running_loop()

    def handle_shutdown():
        asyncio.create_task(app.shutdown())

    for sig in (signal.SIGINT, signal.SIGTERM):

        with suppress(NotImplementedError):
            loop.add_signal_handler(
                sig,
                handle_shutdown
            )

    try:

        await app.start()

        await app.shutdown_event.wait()

    except asyncio.CancelledError:
        pass

    except Exception:
        logger.exception("Fatal runtime error")

        await app.shutdown()


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        pass
