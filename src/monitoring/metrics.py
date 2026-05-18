from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

import psutil

logger = logging.getLogger(__name__)


# =========================================================
# METRIC SNAPSHOT
# =========================================================


@dataclass
class MetricSnapshot:

    timestamp: str

    cpu_percent: float

    ram_percent: float

    ram_used_mb: float

    ram_available_mb: float

    process_memory_mb: float

    active_threads: int

    open_files: int

    request_count: int

    error_count: int

    active_tasks: int


# =========================================================
# LIGHTWEIGHT METRICS
# =========================================================


class LightweightMetricsCollector:

    def __init__(
        self,
        collection_interval: int = 30,
        max_history: int = 100,
    ) -> None:

        self.collection_interval = (
            collection_interval
        )

        self.max_history = (
            max_history
        )

        self.process = (
            psutil.Process(
                os.getpid()
            )
        )

        self.running = False

        self.history: list[
            MetricSnapshot
        ] = []

        self.counters = (
            defaultdict(int)
        )

        self.started_at = (
            time.time()
        )

        self.collection_task = None

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
            "Metrics collector started"
        )

        self.collection_task = (
            asyncio.create_task(
                self._collector_loop()
            )
        )

    # =====================================================
    # STOP
    # =====================================================

    async def stop(
        self,
    ) -> None:

        self.running = False

        if self.collection_task:

            self.collection_task.cancel()

            try:

                await (
                    self.collection_task
                )

            except asyncio.CancelledError:
                pass

        logger.warning(
            "Metrics collector stopped"
        )

    # =====================================================
    # LOOP
    # =====================================================

    async def _collector_loop(
        self,
    ) -> None:

        while self.running:

            try:

                snapshot = (
                    await self.collect()
                )

                self.history.append(
                    snapshot
                )

                if (
                    len(self.history)
                    > self.max_history
                ):

                    self.history.pop(0)

            except Exception:

                logger.exception(
                    "Metrics collection failed"
                )

            await asyncio.sleep(
                self.collection_interval
            )

    # =====================================================
    # COLLECT
    # =====================================================

    async def collect(
        self,
    ) -> MetricSnapshot:

        virtual_memory = (
            psutil.virtual_memory()
        )

        cpu_percent = (
            psutil.cpu_percent(
                interval=None
            )
        )

        process_memory = (
            self.process.memory_info()
            .rss
            / 1024
            / 1024
        )

        open_files = 0

        try:

            open_files = len(
                self.process.open_files()
            )

        except Exception:
            pass

        snapshot = MetricSnapshot(

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

            active_threads=(
                self.process.num_threads()
            ),

            open_files=(
                open_files
            ),

            request_count=(
                self.counters[
                    "requests"
                ]
            ),

            error_count=(
                self.counters[
                    "errors"
                ]
            ),

            active_tasks=(
                len(
                    asyncio.all_tasks()
                )
            ),
        )

        logger.debug(
            "Metrics snapshot=%s",
            snapshot,
        )

        return snapshot

    # =====================================================
    # REQUEST TRACKING
    # =====================================================

    def increment_requests(
        self,
        amount: int = 1,
    ) -> None:

        self.counters[
            "requests"
        ] += amount

    # =====================================================
    # ERROR TRACKING
    # =====================================================

    def increment_errors(
        self,
        amount: int = 1,
    ) -> None:

        self.counters[
            "errors"
        ] += amount

    # =====================================================
    # ACTIVE TASK TRACKING
    # =====================================================

    def increment_tasks(
        self,
        amount: int = 1,
    ) -> None:

        self.counters[
            "tasks"
        ] += amount

    # =====================================================
    # UPTIME
    # =====================================================

    def uptime_seconds(
        self,
    ) -> int:

        return int(
            time.time()
            - self.started_at
        )

    # =====================================================
    # LATEST METRICS
    # =====================================================

    def latest(
        self,
    ) -> MetricSnapshot | None:

        if not self.history:
            return None

        return self.history[-1]

    # =====================================================
    # HEALTH CHECK
    # =====================================================

    def health_status(
        self,
    ) -> dict:

        latest = self.latest()

        if latest is None:

            return {

                "healthy": False,

                "reason":
                "No metrics available",
            }

        warnings = []

        if (
            latest.ram_percent
            >= 85
        ):

            warnings.append(
                "High RAM usage"
            )

        if (
            latest.cpu_percent
            >= 90
        ):

            warnings.append(
                "High CPU usage"
            )

        if (
            latest.process_memory_mb
            >= 600
        ):

            warnings.append(
                "Process memory high"
            )

        return {

            "healthy":
            len(warnings) == 0,

            "warnings":
            warnings,

            "uptime_seconds":
            self.uptime_seconds(),

            "latest":
            latest,
        }


# =========================================================
# GLOBAL METRICS INSTANCE
# =========================================================


metrics_collector = (
    LightweightMetricsCollector()
)
