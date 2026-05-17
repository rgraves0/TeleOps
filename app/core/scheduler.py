from __future__ import annotations

import logging
import os

from apscheduler.schedulers.asyncio import (
    AsyncIOScheduler,
)
from apscheduler.triggers.date import DateTrigger
from dotenv import load_dotenv
from telegram.ext import Application

load_dotenv()

logger = logging.getLogger(__name__)


class SchedulerManager:
    def __init__(self):
        self.timezone = os.getenv(
            "SCHEDULER_TIMEZONE",
            "Asia/Bangkok"
        )

        self.scheduler = AsyncIOScheduler(
            timezone=self.timezone
        )

        self.application: (
            Application | None
        ) = None

        self.started = False

    def attach_application(
        self,
        application: Application
    ) -> None:
        self.application = application

    def get_bot(self):
        if self.application is None:
            raise RuntimeError(
                "Telegram application is not attached"
            )

        return self.application.bot

    def start(self) -> None:
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

    def add_job(
        self,
        job_id: str,
        func,
        run_date,
        kwargs: dict | None = None
    ) -> None:
        existing_job = (
            self.scheduler.get_job(job_id)
        )

        if existing_job:
            self.scheduler.remove_job(
                job_id
            )

        self.scheduler.add_job(
            func=func,
            trigger=DateTrigger(
                run_date=run_date
            ),
            id=job_id,
            kwargs=kwargs or {},
            replace_existing=True,
            misfire_grace_time=300
        )

        logger.info(
            "Scheduled job: %s",
            job_id
        )

    def remove_job(
        self,
        job_id: str
    ) -> None:
        job = self.scheduler.get_job(
            job_id
        )

        if job:
            self.scheduler.remove_job(
                job_id
            )

            logger.info(
                "Removed job: %s",
                job_id
            )

    def list_jobs(self):
        return self.scheduler.get_jobs()


scheduler_manager = SchedulerManager()
