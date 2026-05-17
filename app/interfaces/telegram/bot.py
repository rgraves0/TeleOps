# app/interfaces/telegram/bot.py

```python
from __future__ import annotations

import asyncio
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

        self.original_process_update = (
            self.application.process_update
        )

        self._stop_event = asyncio.Event()

    async def startup(self) -> None:
        logger.info(
            "Telegram bot startup completed"
        )

    async def shutdown(self) -> None:
        logger.info(
            "Stopping Telegram polling..."
        )

        if self.application.updater:
            await self.application.updater.stop()

        logger.info(
            "Stopping Telegram application..."
        )

        await self.application.stop()

        logger.info(
            "Shutting down Telegram application..."
        )

        await self.application.shutdown()

        logger.info(
            "Closing database connection..."
        )

        await db.disconnect()

        logger.info(
            "Telegram bot shutdown completed"
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
                    logger.exception(
                        "Failed to send error message"
                    )

    async def process_update_with_auth(
        self,
        update: Update
    ) -> None:
        context = (
            self.application.context_types.context.from_update(
                update,
                self.application
            )
        )

        try:
            await auth_middleware(
                update,
                context
            )

        except Exception:
            return

        await self.original_process_update(
            update
        )

    def setup(self) -> None:
        register_handlers(
            self.application
        )

        self.application.process_update = (
            self.process_update_with_auth
        )

        self.application.add_error_handler(
            self.global_error_handler
        )

        logger.info(
            "Telegram handlers registered"
        )

    async def run(self) -> None:
        await self.startup()

        self.setup()

        logger.info(
            "Initializing Telegram application..."
        )

        await self.application.initialize()

        logger.info(
            "Starting Telegram application..."
        )

        await self.application.start()

        if self.application.updater is None:
            raise RuntimeError(
                "Telegram updater is unavailable"
            )

        logger.info(
            "Starting Telegram polling..."
        )

        await self.application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=False
        )

        logger.info(
            "Telegram bot is running"
        )

        try:
            await self._stop_event.wait()

        finally:
            await self.shutdown()

    async def stop(self) -> None:
        self._stop_event.set()
```

---

# app/interfaces/telegram/handlers.py

```python
from __future__ import annotations

from telegram.ext import (
    Application,
    CommandHandler,
)

from app.interfaces.telegram.commands.admin import (
    register_admin_handlers,
)
from app.interfaces.telegram.commands.ai_chat import (
    register_ai_chat_handlers,
)
from app.interfaces.telegram.commands.calendar import (
    register_calendar_handlers,
)
from app.interfaces.telegram.commands.system import (
    help_command,
    start_command,
    status_command,
)


def register_handlers(
    application: Application
) -> None:
    application.add_handler(
        CommandHandler(
            "start",
            start_command
        )
    )

    application.add_handler(
        CommandHandler(
            "help",
            help_command
        )
    )

    application.add_handler(
        CommandHandler(
            "status",
            status_command
        )
    )

    register_ai_chat_handlers(
        application
    )

    register_calendar_handlers(
        application
    )

    register_admin_handlers(
        application
    )
```

---

# main.py

```python
from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import suppress

from dotenv import load_dotenv

from app.core.scheduler import (
    scheduler_manager,
)
from app.database.base import (
    db,
    init_db,
)
from app.database.repositories.rclone
```
