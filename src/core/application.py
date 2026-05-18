from __future__ import annotations

import asyncio
import logging
import signal

from src.core.config import (
    get_config,
)
from src.core.container import (
    ServiceContainer,
    build_container,
)

logger = logging.getLogger(__name__)


# =========================================================
# APPLICATION
# =========================================================


class Application:

    def __init__(
        self,
    ) -> None:

        self.config = (
            get_config()
        )

        self.container: (
            ServiceContainer
        ) = build_container(
            self.config
        )

        self.running = False

        self.shutdown_event = (
            asyncio.Event()
        )

        logger.info(
            "Application initialized"
        )

    # =====================================================
    # START
    # =====================================================

    async def start(
        self,
    ) -> None:

        if self.running:
            return

        logger.info(
            "Application starting..."
        )

        self.running = True

        # =================================================
        # START SERVICES
        # =================================================

        await self._start_services()

        # =================================================
        # REGISTER SIGNALS
        # =================================================

        self._register_signals()

        logger.info(
            "Application started"
        )

    # =====================================================
    # STOP
    # =====================================================

    async def stop(
        self,
    ) -> None:

        if not self.running:
            return

        logger.warning(
            "Application shutting down..."
        )

        self.running = False

        # =================================================
        # STOP SERVICES
        # =================================================

        await self._stop_services()

        self.shutdown_event.set()

        logger.warning(
            "Application stopped"
        )

    # =====================================================
    # WAIT
    # =====================================================

    async def wait_forever(
        self,
    ) -> None:

        await self.shutdown_event.wait()

    # =====================================================
    # START SERVICES
    # =====================================================

    async def _start_services(
        self,
    ) -> None:

        # =================================================
        # EVENT BUS
        # =================================================

        event_bus = (
            self.container.get(
                "event_bus"
            )
        )

        await event_bus.start()

        logger.info(
            "EventBus started"
        )

        # =================================================
        # METRICS
        # =================================================

        metrics = (
            self.container.get(
                "metrics"
            )
        )

        await metrics.start()

        logger.info(
            "Metrics collector started"
        )

        # =================================================
        # HEALTH MONITOR
        # =================================================

        health_monitor = (
            self.container.get(
                "health_monitor"
            )
        )

        await health_monitor.start()

        logger.info(
            "Health monitor started"
        )

    # =====================================================
    # STOP SERVICES
    # =====================================================

    async def _stop_services(
        self,
    ) -> None:

        # =================================================
        # HEALTH MONITOR
        # =================================================

        try:

            health_monitor = (
                self.container.get(
                    "health_monitor"
                )
            )

            await (
                health_monitor.stop()
            )

        except Exception:

            logger.exception(
                "Health monitor stop failed"
            )

        # =================================================
        # METRICS
        # =================================================

        try:

            metrics = (
                self.container.get(
                    "metrics"
                )
            )

            await metrics.stop()

        except Exception:

            logger.exception(
                "Metrics stop failed"
            )

        # =================================================
        # EVENT BUS
        # =================================================

        try:

            event_bus = (
                self.container.get(
                    "event_bus"
                )
            )

            await event_bus.stop()

        except Exception:

            logger.exception(
                "EventBus stop failed"
            )

    # =====================================================
    # SIGNAL HANDLERS
    # =====================================================

    def _register_signals(
        self,
    ) -> None:

        loop = (
            asyncio.get_running_loop()
        )

        for sig in (
            signal.SIGINT,
            signal.SIGTERM,
        ):

            loop.add_signal_handler(

                sig,

                lambda: (
                    asyncio.create_task(
                        self.stop()
                    )
                ),
            )

        logger.info(
            "Signal handlers registered"
        )

    # =====================================================
    # HEALTH STATUS
    # =====================================================

    async def health(
        self,
    ) -> dict:

        health_monitor = (
            self.container.get(
                "health_monitor"
            )
        )

        resource_manager = (
            self.container.get(
                "resource_manager"
            )
        )

        metrics = (
            self.container.get(
                "metrics"
            )
        )

        return {

            "running":
            self.running,

            "services":
            self.container.services(),

            "service_count":
            self.container.count(),

            "health":
            (
                health_monitor
                .get_status()
            ),

            "resources":
            (
                resource_manager
                .summary()
            ),

            "metrics":
            (
                metrics.health_status()
            ),
        }


# =========================================================
# APP FACTORY
# =========================================================


def create_application(
) -> Application:

    return Application()
