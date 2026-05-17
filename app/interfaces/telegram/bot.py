from __future__ import annotations

import logging
import os

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    ContextTypes,
)

from app.database.base import (
    db,
    init_db,
)
from app.interfaces.telegram.handlers import (
    register_handlers,
)
from app.interfaces.telegram.middleware import (
    auth_middleware,
)

load_dotenv()

logging.basicConfig(
    format=(
        "%(asctime)s | "
        "%(name)s | "
        "%(levelname)s | "
        "%(message)s"
    ),
    level=logging.INFO
)

logger = logging.getLogger(__name__)


class TelegramBot:
    def __init__(self):
        self.token = os.getenv(
            "TELEGRAM_BOT_TOKEN"
        )

        if not self.token:
            raise RuntimeError(
                "TELEGRAM_BOT_TOKEN is missing"
            )

        self.application = (
            Application.builder()
            .token(self.token)
            .build()
        )

    async def startup(self) -> None:
        logger.info(
            "Initializing database..."
        )

        await init_db()

        logger.info(
            "Database initialized"
        )

    async def shutdown(self) -> None:
        logger.info(
            "Closing database connection..."
        )

        await db.disconnect()

        logger.info(
            "Shutdown complete"
        )

    async def global_error_handler(
        self,
        update: object,
        context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        logger.exception(
            "Unhandled Telegram error",
            exc_info=context.error
        )

        if isinstance(update, Update):
            if update.effective_message:
                try:
                    await update.effective_message.reply_text(
                        "❌ Internal server error"
                    )

                except Exception:
                    pass

    def setup(self) -> None:
        self.application.add_handler(
            auth_middleware,
            group=-1
        )

        register_handlers(
            self.application
        )

        self.application.add_error_handler(
            self.global_error_handler
        )

    async def run(self) -> None:
        await self.startup()

        self.setup()

        logger.info(
            "Starting Telegram bot polling..."
        )

        await self.application.initialize()

        await self.application.start()

        await self.application.updater.start_polling()

        logger.info(
            "Bot started successfully"
        )

        try:
            await self.application.updater.idle()

        finally:
            logger.info(
                "Stopping Telegram bot..."
            )

            await self.application.updater.stop()

            await self.application.stop()

            await self.application.shutdown()

            await self.shutdown()
