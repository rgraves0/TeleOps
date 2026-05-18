from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime

import psutil

logger = logging.getLogger(__name__)


# =========================================================
# HEALTH STATUS
# =========================================================


@dataclass
class HealthStatus:

    timestamp: str

    cpu_percent: float

    ram_percent: float

    ram_used_mb: float

    ram_available_mb: float

    process_memory_mb: float

    process_cpu_percent: float

    active_tasks: int

    open_files: int

    healthy: bool

    warnings: list[str] = field(
        default_factory=list
    )


# =========================================================
# HEALTH MONITOR
# =========================================================


class HealthMonitor:

    def __init__(
        self,
        interval_seconds: int = 15,
        ram_warning: int = 80,
        ram_critical: int = 92,
        cpu_warning: int = 85,
        cpu_critical: int = 95,
        process_memory_limit_mb: int = 700,
    ) -> None:

        self.interval_seconds = (
            interval_seconds
        )

        self.ram_warning = (
            ram_warning
        )

        self.ram_critical = (
            ram_critical
        )

        self.cpu_warning = (
            cpu_warning
        )

        self.cpu_critical = (
            cpu_critical
        )

        self.process_memory_limit_mb = (
            process_memory_limit_mb
        )

        self.process = (
            psutil.Process(
                os.getpid()
            )
        )

        self.running = False

        self.monitor_task = None

        self.latest_status: (
            HealthStatus
            | None
        ) = None

    # =====================================================
    # START
    # =====================================================

    async def start(
        self,
    ) -> None:

        if self.running:
            return

        self.running = True

        logger.info(
            "Health monitor started"
        )

        self.monitor_task = (
            asyncio.create_task(
                self._monitor_loop()
            )
        )

    # =====================================================
    # STOP
    # =====================================================

    async def stop(
        self,
    ) -> None:

        self.running = False

        if self.monitor_task:

            self.monitor_task.cancel()

            try:

                await (
                    self.monitor_task
                )

            except asyncio.CancelledError:
                pass

        logger.warning(
            "Health monitor stopped"
        )

    # =====================================================
    # LOOP
    # =====================================================

    async def _monitor_loop(
        self,
    ) -> None:

        while self.running:

            try:

                self.latest_status = (
                    await self.check_health()
                )

                self._log_health()

            except Exception:

                logger.exception(
                    "Health monitoring failed"
                )

            await asyncio.sleep(
                self.interval_seconds
            )

    # =====================================================
    # CHECK HEALTH
    # =====================================================

    async def check_health(
        self,
    ) -> HealthStatus:

        virtual_memory = (
            psutil.virtual_memory()
        )

        process_memory = (
            self.process.memory_info()
            .rss
            / 1024
            / 1024
        )

        warnings = []

        healthy = True

        cpu_percent = (
            psutil.cpu_percent(
                interval=None
            )
        )

        process_cpu = (
            self.process.cpu_percent()
        )

        # =================================================
        # RAM
        # =================================================

        if (
            virtual_memory.percent
            >= self.ram_warning
        ):

            warnings.append(
                "High RAM usage"
            )

        if (
            virtual_memory.percent
            >= self.ram_critical
        ):

            healthy = False

            warnings.append(
                "Critical RAM usage"
            )

        # =================================================
        # CPU
        # =================================================

        if (
            cpu_percent
            >= self.cpu_warning
        ):

            warnings.append(
                "High CPU usage"
            )

        if (
            cpu_percent
            >= self.cpu_critical
        ):

            healthy = False

            warnings.append(
                "Critical CPU usage"
            )

        # =================================================
        # PROCESS MEMORY
        # =================================================

        if (
            process_memory
            >= self.process_memory_limit_mb
        ):

            healthy = False

            warnings.append(
                "Process memory limit exceeded"
            )

        open_files = 0

        try:

            open_files = len(
                self.process.open_files()
            )

        except Exception:
            pass

        return HealthStatus(

            timestamp=(
                datetime.utcnow()
                .isoformat()
            ),

            cpu_percent=(
                cpu_percent
            ),

            ram_percent=(
                virtual_memory.percent
            ),

            ram_used_mb=(
                virtual_memory.used
                / 1024
                / 1024
            ),

            ram_available_mb=(
                virtual_memory.available
                / 1024
                / 1024
            ),

            process_memory_mb=(
                process_memory
            ),

            process_cpu_percent=(
                process_cpu
            ),

            active_tasks=(
                len(
                    asyncio.all_tasks()
                )
            ),

            open_files=(
                open_files
            ),

            healthy=healthy,

            warnings=warnings,
        )

    # =====================================================
    # LOG HEALTH
    # =====================================================

    def _log_health(
        self,
    ) -> None:

        if not self.latest_status:
            return

        status = (
            self.latest_status
        )

        logger.info(
            (
                "HEALTH "
                "cpu=%.1f%% "
                "ram=%.1f%% "
                "proc_mem=%.1fMB "
                "tasks=%s "
                "healthy=%s"
            ),
            status.cpu_percent,
            status.ram_percent,
            status.process_memory_mb,
            status.active_tasks,
            status.healthy,
        )

        for warning in (
            status.warnings
        ):

            logger.warning(
                warning
            )

    # =====================================================
    # STATUS
    # =====================================================

    def get_status(
        self,
    ) -> HealthStatus | None:

        return self.latest_status

    # =====================================================
    # SAFE TO PROCESS
    # =====================================================

    def can_accept_work(
        self,
    ) -> bool:

        if (
            self.latest_status
            is None
        ):

            return True

        return (
            self.latest_status.healthy
        )


# =========================================================
# GLOBAL INSTANCE
# =========================================================


health_monitor = (
    HealthMonitor()
)
