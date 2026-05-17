from __future__ import annotations

import logging
from typing import Any

from apscheduler.schedulers.asyncio import (
    AsyncIOScheduler,
)
from apscheduler.triggers.date import (
    DateTrigger,
)

logger = logging.getLogger(__name__)


class SchedulerService:
    def __init__(self) -> None:
        self.scheduler = AsyncIOScheduler()

        self.started = False

    async def start(self) -> None:
        if self.started:
            return

        self.scheduler.start()

        self.started = True

        logger.info(
            "AsyncIOScheduler started"
        )

    async def shutdown(self) -> None:
        if not self.started:
            return

        self.scheduler.shutdown(
            wait=False
        )

        self.started = False

        logger.info(
            "AsyncIOScheduler stopped"
        )

    async def add_job(
        self,
        func,
        run_date,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        job_id: str | None = None,
        replace_existing: bool = True
    ) -> None:
        if args is None:
            args = []

        if kwargs is None:
            kwargs = {}

        self.scheduler.add_job(
            func=func,
            trigger=DateTrigger(
                run_date=run_date
            ),
            args=args,
            kwargs=kwargs,
            id=job_id,
            replace_existing=(
                replace_existing
            )
        )

        logger.info(
            "Scheduled job added "
            "job_id=%s run_date=%s",
            job_id,
            run_date
        )

    async def remove_job(
        self,
        job_id: str
    ) -> None:
        try:
            self.scheduler.remove_job(
                job_id
            )

            logger.info(
                "Scheduled job removed "
                "job_id=%s",
                job_id
            )

        except Exception:
            logger.exception(
                "Failed to remove "
                "job_id=%s",
                job_id
            )

    async def get_jobs(
        self
    ):
        return self.scheduler.get_jobs()

    async def attach_application(
        self,
        application
    ) -> None:
        self.application = application

        logger.info(
            "Telegram application "
            "attached to scheduler"
        )


scheduler_service = SchedulerService()
