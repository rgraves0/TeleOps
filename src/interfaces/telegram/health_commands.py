from __future__ import annotations

import logging
import os
import platform
import time
from datetime import datetime

import psutil
from telegram import Update
from telegram.ext import (
    ContextTypes,
    CommandHandler,
)

from src.core.config import (
    AppConfig,
)
from src.monitoring.metrics import (
    LightweightMetricsCollector,
)
from src.monitoring.health import (
    HealthMonitor,
)
from src.core.resource_manager import (
    ResourceManager,
)
from src.interfaces.telegram.admin_commands import (
    AdminAccessManager,
)

logger = logging.getLogger(__name__)


# =========================================================
# HEALTH COMMANDS
# =========================================================


class HealthCommands:

    def __init__(
        self,
        config: AppConfig,
        metrics: (
            LightweightMetricsCollector
        ),
        health_monitor: (
            HealthMonitor
        ),
        resource_manager: (
            ResourceManager
        ),
    ) -> None:

        self.config = config

        self.metrics = metrics

        self.health_monitor = (
            health_monitor
        )

        self.resource_manager = (
            resource_manager
        )

        self.access = (
            AdminAccessManager(
                config
            )
        )

        self.started_at = (
            time.time()
        )

        logger.info(
            "HealthCommands initialized"
        )

    # =====================================================
    # HANDLERS
    # =====================================================

    def handlers(
        self,
    ) -> list[CommandHandler]:

        return [

            CommandHandler(
                "health",
                self.health,
            ),

            CommandHandler(
                "metrics",
                self.metrics_command,
            ),

            CommandHandler(
                "memory",
                self.memory,
            ),

            CommandHandler(
                "resources",
                self.resources,
            ),

            CommandHandler(
                "uptime",
                self.uptime,
            ),
        ]

    # =====================================================
    # HEALTH
    # =====================================================

    async def health(
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

        status = (
            self.health_monitor
            .get_status()
        )

        text = "\n".join(

            [

                "💚 System Health",

                "",

                f"CPU: {status.get('cpu_percent')}%",

                f"RAM: {status.get('ram_percent')}%",

                f"Tasks: {status.get('active_tasks')}",

                f"Healthy: {status.get('healthy')}",
            ]
        )

        await (
            update.effective_message
            .reply_text(text)
        )

    # =====================================================
    # METRICS
    # =====================================================

    async def metrics_command(
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

        metrics = (
            self.metrics
            .health_status()
        )

        text = "\n".join(

            [

                "📊 Metrics",

                "",

                str(metrics),
            ]
        )

        await (
            update.effective_message
            .reply_text(text)
        )

    # =====================================================
    # MEMORY
    # =====================================================

    async def memory(
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

        process = psutil.Process(
            os.getpid()
        )

        ram_mb = round(

            process.memory_info().rss
            / 1024
            / 1024,

            2,
        )

        virtual = psutil.virtual_memory()

        text = "\n".join(

            [

                "🧠 Memory Usage",

                "",

                f"Process RAM: {ram_mb} MB",

                f"System RAM: {virtual.percent}%",

                f"Available: "
                f"{round(virtual.available / 1024 / 1024)} MB",
            ]
        )

        await (
            update.effective_message
            .reply_text(text)
        )

    # =====================================================
    # RESOURCES
    # =====================================================

    async def resources(
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

        summary = (
            self.resource_manager
            .summary()
        )

        text = "\n".join(

            [

                "⚙️ Resource Manager",

                "",

                str(summary),
            ]
        )

        await (
            update.effective_message
            .reply_text(text)
        )

    # =====================================================
    # UPTIME
    # =====================================================

    async def uptime(
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

        seconds = int(
            time.time()
            - self.started_at
        )

        hours = (
            seconds // 3600
        )

        minutes = (
            (seconds % 3600)
            // 60
        )

        text = "\n".join(

            [

                "⏱ Uptime",

                "",

                f"{hours}h {minutes}m",

                "",

                f"Host: {platform.node()}",
            ]
        )

        await (
            update.effective_message
            .reply_text(text)
        )
