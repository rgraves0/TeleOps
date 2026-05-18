from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Callable
from typing import Awaitable

from telegram import Update
from telegram.ext import (
    ContextTypes,
    CommandHandler,
)

from src.core.config import (
    AppConfig,
)
from src.core.events import (
    Event,
    EventBus,
)

logger = logging.getLogger(__name__)


# =========================================================
# ADMIN ACCESS
# =========================================================


class AdminAccessManager:

    def __init__(
        self,
        config: AppConfig,
    ) -> None:

        self.admin_ids = (
            set(
                config.telegram
                .admin_ids
            )
        )

    # =====================================================
    # IS ADMIN
    # =====================================================

    def is_admin(
        self,
        telegram_id: int,
    ) -> bool:

        return (
            telegram_id
            in self.admin_ids
        )

    # =====================================================
    # REQUIRE ADMIN
    # =====================================================

    async def require_admin(
        self,
        update: Update,
    ) -> bool:

        user = (
            update.effective_user
        )

        if not user:

            return False

        if self.is_admin(
            user.id
        ):

            return True

        try:

            await (
                update.effective_message
                .reply_text(
                    "❌ Admin only command"
                )
            )

        except Exception:

            logger.exception(
                "Unauthorized reply failed"
            )

        logger.warning(
            "Unauthorized access=%s",
            user.id,
        )

        return False


# =========================================================
# ADMIN COMMANDS
# =========================================================


class AdminCommands:

    def __init__(
        self,
        config: AppConfig,
        event_bus: EventBus,
    ) -> None:

        self.config = config

        self.event_bus = event_bus

        self.access = (
            AdminAccessManager(
                config
            )
        )

        logger.info(
            "AdminCommands initialized"
        )

    # =====================================================
    # REGISTER
    # =====================================================

    def handlers(
        self,
    ) -> list[CommandHandler]:

        return [

            CommandHandler(
                "admin",
                self.admin_panel,
            ),

            CommandHandler(
                "ping",
                self.ping,
            ),

            CommandHandler(
                "alerts",
                self.alerts,
            ),

            CommandHandler(
                "shutdown",
                self.shutdown,
            ),
        ]

    # =====================================================
    # ADMIN PANEL
    # =====================================================

    async def admin_panel(
        self,
        update: Update,
        context: (
            ContextTypes.DEFAULT_TYPE
        ),
    ) -> None:

        if not await (
            self.access.require_admin(
                update
            )
        ):

            return

        text = "\n".join(

            [

                "🛠 TeleOps Admin",

                "",

                "/health - System health",

                "/metrics - Metrics",

                "/memory - RAM usage",

                "/alerts - Alert status",

                "/ping - Connectivity test",

                "/shutdown - Shutdown system",
            ]
        )

        await (
            update.effective_message
            .reply_text(text)
        )

    # =====================================================
    # PING
    # =====================================================

    async def ping(
        self,
        update: Update,
        context: (
            ContextTypes.DEFAULT_TYPE
        ),
    ) -> None:

        if not await (
            self.access.require_admin(
                update
            )
        ):

            return

        await (
            update.effective_message
            .reply_text(
                "🏓 Pong"
            )
        )

    # =====================================================
    # ALERTS
    # =====================================================

    async def alerts(
        self,
        update: Update,
        context: (
            ContextTypes.DEFAULT_TYPE
        ),
    ) -> None:

        if not await (
            self.access.require_admin(
                update
            )
        ):

            return

        text = "\n".join(

            [

                "🚨 Operational Alerts",

                "",

                "• RAM monitoring active",

                "• CPU monitoring active",

                "• Workflow monitoring active",

                "• Provider monitoring active",
            ]
        )

        await (
            update.effective_message
            .reply_text(text)
        )

    # =====================================================
    # SHUTDOWN
    # =====================================================

    async def shutdown(
        self,
        update: Update,
        context: (
            ContextTypes.DEFAULT_TYPE
        ),
    ) -> None:

        if not await (
            self.access.require_admin(
                update
            )
        ):

            return

        await (
            update.effective_message
            .reply_text(
                "⚠️ Shutdown signal emitted"
            )
        )

        await (
            self.event_bus.emit(

                "system.shutdown",

                {

                    "requested_by":
                    (
                        update
                        .effective_user
                        .id
                    ),

                    "timestamp":
                    datetime.utcnow()
                    .isoformat(),
                },
            )
        )

    # =====================================================
    # EVENT ALERTS
    # =====================================================

    async def handle_system_alert(
        self,
        event: Event,
    ) -> None:

        logger.warning(
            "Operational alert=%s",
            event.name,
        )

    # =====================================================
    # REGISTER EVENT ALERTS
    # =====================================================

    def register_event_handlers(
        self,
    ) -> None:

        events = [

            "system.error",

            "resource.critical",

            "workflow.failed",

            "provider.failed",
        ]

        for event_name in events:

            self.event_bus.subscribe(
                event_name,
                self.handle_system_alert,
            )
