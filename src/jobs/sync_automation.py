from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime

from apscheduler.schedulers.asyncio import (
    AsyncIOScheduler,
)

from src.storage.rclone_wrapper import (
    RcloneWrapper,
)

logger = logging.getLogger(__name__)


@dataclass
class SyncTask:

    task_id: str

    source: str

    destination: str

    interval_minutes: int

    enabled: bool = True

    last_run: str | None = None

    last_status: str | None = None


class SyncAutomationManager:

    def __init__(
        self,
        rclone: RcloneWrapper,
    ) -> None:

        self.rclone = rclone

        self.scheduler = (
            AsyncIOScheduler()
        )

        self.tasks: dict[
            str,
            SyncTask
        ] = {}

    # =====================================================
    # START
    # =====================================================

    async def start(
        self,
    ) -> None:

        if not self.scheduler.running:

            self.scheduler.start()

            logger.info(
                "Sync scheduler started"
            )

    # =====================================================
    # STOP
    # =====================================================

    async def stop(
        self,
    ) -> None:

        if self.scheduler.running:

            self.scheduler.shutdown()

            logger.info(
                "Sync scheduler stopped"
            )

    # =====================================================
    # REGISTER TASK
    # =====================================================

    async def register_task(
        self,
        task: SyncTask,
    ) -> None:

        self.tasks[
            task.task_id
        ] = task

        self.scheduler.add_job(
            self.execute_task,
            "interval",
            minutes=(
                task.interval_minutes
            ),
            id=task.task_id,
            args=[task.task_id],
            replace_existing=True,
        )

        logger.info(
            "Registered sync task=%s",
            task.task_id,
        )

    # =====================================================
    # REMOVE TASK
    # =====================================================

    async def remove_task(
        self,
        task_id: str,
    ) -> None:

        if task_id in self.tasks:

            del self.tasks[
                task_id
            ]

        try:

            self.scheduler.remove_job(
                task_id
            )

        except Exception:

            logger.exception(
                "Failed removing task"
            )

    # =====================================================
    # EXECUTE TASK
    # =====================================================

    async def execute_task(
        self,
        task_id: str,
    ) -> None:

        task = self.tasks.get(
            task_id
        )

        if task is None:

            logger.warning(
                "Sync task not found=%s",
                task_id,
            )

            return

        if not task.enabled:

            logger.info(
                "Sync task disabled=%s",
                task_id,
            )

            return

        logger.info(
            "Running sync task=%s",
            task_id,
        )

        try:

            result = await (
                self.rclone.sync(
                    task.source,
                    task.destination,
                )
            )

            task.last_run = (
                datetime.utcnow()
                .isoformat()
            )

            if result.success:

                task.last_status = (
                    "success"
                )

                logger.info(
                    "Sync success task=%s",
                    task_id,
                )

            else:

                task.last_status = (
                    "failed"
                )

                logger.error(
                    "Sync failed=%s",
                    result.stderr,
                )

        except Exception:

            task.last_status = (
                "failed"
            )

            logger.exception(
                "Sync automation crashed"
            )

    # =====================================================
    # LIST TASKS
    # =====================================================

    async def list_tasks(
        self,
    ) -> list[SyncTask]:

        return list(
            self.tasks.values()
        )

    # =====================================================
    # ENABLE TASK
    # =====================================================

    async def enable_task(
        self,
        task_id: str,
    ) -> None:

        task = self.tasks.get(
            task_id
        )

        if task:

            task.enabled = True

    # =====================================================
    # DISABLE TASK
    # =====================================================

    async def disable_task(
        self,
        task_id: str,
    ) -> None:

        task = self.tasks.get(
            task_id
        )

        if task:

            task.enabled = False
