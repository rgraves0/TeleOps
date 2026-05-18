from __future__ import annotations

import asyncio
import json
import logging
import logging.handlers
import os
import queue
import sys
import traceback
from datetime import datetime
from pathlib import Path


# =========================================================
# LOG DIRECTORIES
# =========================================================

LOG_DIRECTORY = Path(
    "logs"
)

LOG_DIRECTORY.mkdir(
    parents=True,
    exist_ok=True,
)

APP_LOG_FILE = (
    LOG_DIRECTORY
    / "teleops.log"
)

ERROR_LOG_FILE = (
    LOG_DIRECTORY
    / "errors.log"
)


# =========================================================
# JSON FORMATTER
# =========================================================


class JSONFormatter(
    logging.Formatter
):

    def format(
        self,
        record: logging.LogRecord,
    ) -> str:

        payload = {

            "timestamp":
            datetime.utcnow()
            .isoformat(),

            "level":
            record.levelname,

            "logger":
            record.name,

            "message":
            record.getMessage(),

            "module":
            record.module,

            "function":
            record.funcName,

            "line":
            record.lineno,

            "process":
            record.process,

            "thread":
            record.thread,
        }

        if record.exc_info:

            payload[
                "exception"
            ] = self.formatException(
                record.exc_info
            )

        return json.dumps(
            payload,
            ensure_ascii=False,
        )


# =========================================================
# ASYNC LOGGER
# =========================================================


class AsyncLoggerManager:

    def __init__(
        self,
        app_name: str = (
            "TeleOps"
        ),
        log_level: int = (
            logging.INFO
        ),
        max_bytes: int = (
            5 * 1024 * 1024
        ),
        backup_count: int = 3,
    ) -> None:

        self.app_name = app_name

        self.log_level = log_level

        self.max_bytes = (
            max_bytes
        )

        self.backup_count = (
            backup_count
        )

        self.log_queue: (
            queue.Queue
        ) = queue.Queue(
            maxsize=10000
        )

        self.queue_handler = (
            logging.handlers.QueueHandler(
                self.log_queue
            )
        )

        self.listener = None

        self.initialized = False

    # =====================================================
    # INITIALIZE
    # =====================================================

    def initialize(
        self,
    ) -> None:

        if self.initialized:
            return

        formatter = (
            JSONFormatter()
        )

        # =================================================
        # APP HANDLER
        # =================================================

        app_handler = (
            logging.handlers.RotatingFileHandler(
                APP_LOG_FILE,
                maxBytes=(
                    self.max_bytes
                ),
                backupCount=(
                    self.backup_count
                ),
                encoding="utf-8",
            )
        )

        app_handler.setFormatter(
            formatter
        )

        app_handler.setLevel(
            logging.INFO
        )

        # =================================================
        # ERROR HANDLER
        # =================================================

        error_handler = (
            logging.handlers.RotatingFileHandler(
                ERROR_LOG_FILE,
                maxBytes=(
                    self.max_bytes
                ),
                backupCount=(
                    self.backup_count
                ),
                encoding="utf-8",
            )
        )

        error_handler.setFormatter(
            formatter
        )

        error_handler.setLevel(
            logging.ERROR
        )

        # =================================================
        # CONSOLE HANDLER
        # =================================================

        console_handler = (
            logging.StreamHandler(
                sys.stdout
            )
        )

        console_handler.setFormatter(
            logging.Formatter(
                (
                    "%(asctime)s | "
                    "%(name)s | "
                    "%(levelname)s | "
                    "%(message)s"
                )
            )
        )

        console_handler.setLevel(
            logging.INFO
        )

        # =================================================
        # LISTENER
        # =================================================

        self.listener = (
            logging.handlers.QueueListener(
                self.log_queue,
                app_handler,
                error_handler,
                console_handler,
                respect_handler_level=True,
            )
        )

        self.listener.start()

        # =================================================
        # ROOT LOGGER
        # =================================================

        root_logger = (
            logging.getLogger()
        )

        root_logger.handlers.clear()

        root_logger.setLevel(
            self.log_level
        )

        root_logger.addHandler(
            self.queue_handler
        )

        self.initialized = True

        logging.getLogger(
            __name__
        ).info(
            "Async logging initialized"
        )

    # =====================================================
    # SHUTDOWN
    # =====================================================

    def shutdown(
        self,
    ) -> None:

        if self.listener:

            self.listener.stop()

        self.initialized = False


# =========================================================
# LOGGER HELPERS
# =========================================================


def get_logger(
    name: str,
) -> logging.Logger:

    return logging.getLogger(
        name
    )


# =========================================================
# EXCEPTION LOGGER
# =========================================================


def log_exception(
    logger: logging.Logger,
    exc: Exception,
    context: str | None = None,
) -> None:

    message = (
        f"{context}: {exc}"
        if context
        else str(exc)
    )

    logger.error(
        message,
        exc_info=True,
    )


# =========================================================
# ASYNC SAFE LOGGING
# =========================================================


async def async_log(
    logger: logging.Logger,
    level: str,
    message: str,
) -> None:

    await asyncio.to_thread(
        getattr(logger, level),
        message,
    )


# =========================================================
# GLOBAL LOGGER MANAGER
# =========================================================


logger_manager = (
    AsyncLoggerManager()
)
