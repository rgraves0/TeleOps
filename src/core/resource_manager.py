from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass

from src.monitoring.health import (
    HealthMonitor,
)

logger = logging.getLogger(__name__)


# =========================================================
# RESOURCE CONFIG
# =========================================================


@dataclass
class ResourceLimits:

    max_concurrent_ai_requests: int = 2

    max_concurrent_background_tasks: int = 3

    max_queue_size: int = 50

    ai_requests_per_minute: int = 20

    max_active_async_tasks: int = 150

    cooldown_seconds: int = 5


# =========================================================
# RESOURCE MANAGER
# =========================================================


class ResourceManager:

    def __init__(
        self,
        health_monitor: (
            HealthMonitor
        ),
        limits: (
            ResourceLimits
            | None
        ) = None,
    ) -> None:

        self.health_monitor = (
            health_monitor
        )

        self.limits = (
            limits
            or ResourceLimits()
        )

        # =================================================
        # SEMAPHORES
        # =================================================

        self.ai_semaphore = (
            asyncio.Semaphore(
                self.limits
                .max_concurrent_ai_requests
            )
        )

        self.background_semaphore = (
            asyncio.Semaphore(
                self.limits
                .max_concurrent_background_tasks
            )
        )

        # =================================================
        # RATE LIMITING
        # =================================================

        self.ai_request_times = (
            deque()
        )

        self.lock = (
            asyncio.Lock()
        )

        self.last_rejection = 0.0

        logger.info(
            "ResourceManager initialized"
        )

    # =====================================================
    # HEALTH CHECK
    # =====================================================

    async def check_resources(
        self,
    ) -> bool:

        status = (
            self.health_monitor
            .get_status()
        )

        if status is None:
            return True

        # =================================================
        # MEMORY PROTECTION
        # =================================================

        if (
            status.ram_percent
            >= 92
        ):

            logger.error(
                "Rejecting work: RAM critical"
            )

            return False

        # =================================================
        # CPU PROTECTION
        # =================================================

        if (
            status.cpu_percent
            >= 95
        ):

            logger.error(
                "Rejecting work: CPU critical"
            )

            return False

        # =================================================
        # TASK LIMIT
        # =================================================

        if (
            status.active_tasks
            >= self.limits
            .max_active_async_tasks
        ):

            logger.error(
                "Rejecting work: too many tasks"
            )

            return False

        return True

    # =====================================================
    # AI RATE LIMIT
    # =====================================================

    async def allow_ai_request(
        self,
    ) -> bool:

        async with self.lock:

            current_time = (
                time.time()
            )

            # =============================================
            # CLEAN OLD REQUESTS
            # =============================================

            while (
                self.ai_request_times
                and current_time
                - self.ai_request_times[0]
                > 60
            ):

                self.ai_request_times.popleft()

            # =============================================
            # LIMIT CHECK
            # =============================================

            if (
                len(
                    self.ai_request_times
                )
                >= self.limits
                .ai_requests_per_minute
            ):

                logger.warning(
                    "AI request throttled"
                )

                return False

            self.ai_request_times.append(
                current_time
            )

            return True

    # =====================================================
    # AI REQUEST CONTEXT
    # =====================================================

    @asynccontextmanager
    async def ai_request_context(
        self,
    ):

        # ================================================
        # HEALTH CHECK
        # ================================================

        allowed = (
            await self.check_resources()
        )

        if not allowed:

            raise RuntimeError(
                "System overloaded"
            )

        # ================================================
        # RATE LIMIT
        # ================================================

        allowed = (
            await self.allow_ai_request()
        )

        if not allowed:

            raise RuntimeError(
                "AI rate limit exceeded"
            )

        # ================================================
        # SEMAPHORE
        # ================================================

        async with self.ai_semaphore:

            yield

    # =====================================================
    # BACKGROUND TASK CONTEXT
    # =====================================================

    @asynccontextmanager
    async def background_task_context(
        self,
    ):

        allowed = (
            await self.check_resources()
        )

        if not allowed:

            raise RuntimeError(
                "Resources unavailable"
            )

        async with (
            self.background_semaphore
        ):

            yield

    # =====================================================
    # SAFE CREATE TASK
    # =====================================================

    async def safe_create_task(
        self,
        coroutine,
        timeout: int = 120,
    ):

        allowed = (
            await self.check_resources()
        )

        if not allowed:

            raise RuntimeError(
                "Cannot create task"
            )

        async def wrapped():

            try:

                return await (
                    asyncio.wait_for(
                        coroutine,
                        timeout=timeout,
                    )
                )

            except asyncio.TimeoutError:

                logger.error(
                    "Task timeout"
                )

            except Exception:

                logger.exception(
                    "Task crashed"
                )

        return asyncio.create_task(
            wrapped()
        )

    # =====================================================
    # QUEUE LIMIT
    # =====================================================

    def queue_has_capacity(
        self,
        queue_size: int,
    ) -> bool:

        return (
            queue_size
            < self.limits
            .max_queue_size
        )

    # =====================================================
    # RESOURCE SUMMARY
    # =====================================================

    def summary(
        self,
    ) -> dict:

        status = (
            self.health_monitor
            .get_status()
        )

        return {

            "healthy":
            self.health_monitor
            .can_accept_work(),

            "cpu_percent":
            (
                status.cpu_percent
                if status
                else None
            ),

            "ram_percent":
            (
                status.ram_percent
                if status
                else None
            ),

            "active_tasks":
            (
                status.active_tasks
                if status
                else None
            ),

            "ai_requests_last_minute":
            len(
                self.ai_request_times
            ),

            "max_ai_requests":
            self.limits
            .ai_requests_per_minute,
        }


# =========================================================
# GLOBAL RESOURCE MANAGER
# =========================================================


resource_manager = (
    ResourceManager(
        health_monitor=(
            health_monitor
        )
    )
)
