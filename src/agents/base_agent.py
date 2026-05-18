from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


# =========================================================
# AGENT STATUS
# =========================================================


class AgentStatus(str, Enum):

    IDLE = "idle"

    STARTING = "starting"

    RUNNING = "running"

    STOPPING = "stopping"

    STOPPED = "stopped"

    FAILED = "failed"


# =========================================================
# AGENT HEALTH
# =========================================================


@dataclass
class AgentHealth:

    agent_name: str

    status: AgentStatus

    uptime_seconds: float

    heartbeat_at: str

    tasks_processed: int

    failures: int

    memory_pressure: bool = False


# =========================================================
# BASE AGENT
# =========================================================


class BaseAgent(ABC):

    def __init__(
        self,
        agent_name: str,
        heartbeat_interval: int = 10,
        sleep_interval: float = 0.1,
    ) -> None:

        self.agent_name = (
            agent_name
        )

        self.heartbeat_interval = (
            heartbeat_interval
        )

        self.sleep_interval = (
            sleep_interval
        )

        self.status = (
            AgentStatus.IDLE
        )

        self.started_at = 0.0

        self.last_heartbeat = (
            datetime.utcnow()
        )

        self.tasks_processed = 0

        self.failures = 0

        self.running = False

        self.shutdown_event = (
            asyncio.Event()
        )

        self.worker_task: (
            asyncio.Task | None
        ) = None

        self.heartbeat_task: (
            asyncio.Task | None
        ) = None

        logger.info(
            "Agent initialized=%s",
            self.agent_name,
        )

    # =====================================================
    # START
    # =====================================================

    async def start(
        self,
    ) -> None:

        if self.running:
            return

        self.status = (
            AgentStatus.STARTING
        )

        self.running = True

        self.started_at = (
            time.perf_counter()
        )

        self.shutdown_event.clear()

        self.worker_task = (
            asyncio.create_task(
                self._worker_loop()
            )
        )

        self.heartbeat_task = (
            asyncio.create_task(
                self._heartbeat_loop()
            )
        )

        self.status = (
            AgentStatus.RUNNING
        )

        logger.info(
            "Agent started=%s",
            self.agent_name,
        )

    # =====================================================
    # STOP
    # =====================================================

    async def stop(
        self,
    ) -> None:

        if not self.running:
            return

        self.status = (
            AgentStatus.STOPPING
        )

        self.running = False

        self.shutdown_event.set()

        tasks = [

            task

            for task in [

                self.worker_task,
                self.heartbeat_task,
            ]

            if task
        ]

        if tasks:

            await asyncio.gather(

                *tasks,

                return_exceptions=True,
            )

        self.status = (
            AgentStatus.STOPPED
        )

        logger.info(
            "Agent stopped=%s",
            self.agent_name,
        )

    # =====================================================
    # WORKER LOOP
    # =====================================================

    async def _worker_loop(
        self,
    ) -> None:

        while (

            self.running

            and not (
                self.shutdown_event
                .is_set()
            )
        ):

            try:

                await self.run_cycle()

            except asyncio.CancelledError:

                break

            except Exception:

                self.failures += 1

                self.status = (
                    AgentStatus.FAILED
                )

                logger.exception(
                    "Agent cycle failed=%s",
                    self.agent_name,
                )

                await asyncio.sleep(
                    1
                )

                self.status = (
                    AgentStatus.RUNNING
                )

            await asyncio.sleep(
                self.sleep_interval
            )

    # =====================================================
    # HEARTBEAT LOOP
    # =====================================================

    async def _heartbeat_loop(
        self,
    ) -> None:

        while (

            self.running

            and not (
                self.shutdown_event
                .is_set()
            )
        ):

            self.last_heartbeat = (
                datetime.utcnow()
            )

            try:

                await self.on_heartbeat()

            except Exception:

                logger.exception(
                    "Heartbeat failed=%s",
                    self.agent_name,
                )

            await asyncio.sleep(
                self.heartbeat_interval
            )

    # =====================================================
    # RUN CYCLE
    # =====================================================

    @abstractmethod
    async def run_cycle(
        self,
    ) -> None:

        raise NotImplementedError

    # =====================================================
    # HEARTBEAT HOOK
    # =====================================================

    async def on_heartbeat(
        self,
    ) -> None:

        return None

    # =====================================================
    # TASK PROCESSED
    # =====================================================

    async def increment_tasks(
        self,
    ) -> None:

        self.tasks_processed += 1

    # =====================================================
    # HEALTH
    # =====================================================

    async def health(
        self,
    ) -> AgentHealth:

        uptime = (
            time.perf_counter()
            - self.started_at
        )

        return AgentHealth(

            agent_name=
            self.agent_name,

            status=
            self.status,

            uptime_seconds=
            round(
                uptime,
                2,
            ),

            heartbeat_at=
            self.last_heartbeat
            .isoformat(),

            tasks_processed=
            self.tasks_processed,

            failures=
            self.failures,
        )

    # =====================================================
    # IS HEALTHY
    # =====================================================

    async def is_healthy(
        self,
    ) -> bool:

        if not self.running:
            return False

        heartbeat_age = (

            datetime.utcnow()

            - self.last_heartbeat

        ).total_seconds()

        return (
            heartbeat_age
            <= (
                self.heartbeat_interval
                * 2
            )
        )
