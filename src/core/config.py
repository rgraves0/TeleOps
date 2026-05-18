from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)


# =========================================================
# ENV HELPERS
# =========================================================


def get_env(
    key: str,
    default: str | None = None,
    required: bool = False,
) -> str:

    value = os.getenv(
        key,
        default,
    )

    if required and not value:

        raise RuntimeError(
            f"Missing required env: {key}"
        )

    return value or ""


def get_env_int(
    key: str,
    default: int,
) -> int:

    try:

        return int(
            os.getenv(
                key,
                str(default),
            )
        )

    except ValueError:

        logger.warning(
            "Invalid int env=%s",
            key,
        )

        return default


def get_env_bool(
    key: str,
    default: bool = False,
) -> bool:

    value = os.getenv(
        key,
        str(default),
    ).lower()

    return value in (
        "1",
        "true",
        "yes",
        "on",
    )


# =========================================================
# CORE CONFIG
# =========================================================


@dataclass(frozen=True)
class CoreConfig:

    app_name: str

    environment: str

    debug: bool

    timezone: str

    log_level: str


# =========================================================
# TELEGRAM CONFIG
# =========================================================


@dataclass(frozen=True)
class TelegramConfig:

    bot_token: str

    admin_ids: list[int]


# =========================================================
# AI CONFIG
# =========================================================


@dataclass(frozen=True)
class AIConfig:

    provider: str

    model: str

    openrouter_keys: list[
        str
    ]

    groq_keys: list[str]

    max_tokens: int

    temperature: float


# =========================================================
# MAIL CONFIG
# =========================================================


@dataclass(frozen=True)
class MailConfig:

    imap_host: str

    imap_port: int

    smtp_host: str

    smtp_port: int

    email_address: str

    email_password: str


# =========================================================
# RESOURCE CONFIG
# =========================================================


@dataclass(frozen=True)
class ResourceConfig:

    max_concurrent_ai_requests: int

    max_background_tasks: int

    max_queue_size: int

    max_ram_percent: int

    max_cpu_percent: int


# =========================================================
# STORAGE CONFIG
# =========================================================


@dataclass(frozen=True)
class StorageConfig:

    rclone_binary: str

    rclone_config_path: str

    storage_path: str


# =========================================================
# APP CONFIG
# =========================================================


@dataclass(frozen=True)
class AppConfig:

    core: CoreConfig

    telegram: TelegramConfig

    ai: AIConfig

    mail: MailConfig

    resources: ResourceConfig

    storage: StorageConfig


# =========================================================
# CONFIG LOADER
# =========================================================


class ConfigLoader:

    @staticmethod
    def load() -> AppConfig:

        logger.info(
            "Loading configuration"
        )

        config = AppConfig(

            core=CoreConfig(

                app_name=get_env(
                    "APP_NAME",
                    "TeleOps",
                ),

                environment=get_env(
                    "APP_ENV",
                    "production",
                ),

                debug=get_env_bool(
                    "DEBUG",
                    False,
                ),

                timezone=get_env(
                    "TIMEZONE",
                    "UTC",
                ),

                log_level=get_env(
                    "LOG_LEVEL",
                    "INFO",
                ),
            ),

            telegram=TelegramConfig(

                bot_token=get_env(
                    "TELEGRAM_BOT_TOKEN",
                    required=True,
                ),

                admin_ids=[
                    int(x)
                    for x in get_env(
                        "ADMIN_IDS",
                        "",
                    ).split(",")
                    if x.strip()
                ],
            ),

            ai=AIConfig(

                provider=get_env(
                    "AI_PROVIDER",
                    "openrouter",
                ),

                model=get_env(
                    "AI_MODEL",
                    "meta-llama/llama-3.3-70b-instruct:free",
                ),

                openrouter_keys=[
                    key.strip()
                    for key in get_env(
                        "OPENROUTER_API_KEYS",
                        "",
                    ).split(",")
                    if key.strip()
                ],

                groq_keys=[
                    key.strip()
                    for key in get_env(
                        "GROQ_API_KEYS",
                        "",
                    ).split(",")
                    if key.strip()
                ],

                max_tokens=get_env_int(
                    "AI_MAX_TOKENS",
                    1200,
                ),

                temperature=float(
                    get_env(
                        "AI_TEMPERATURE",
                        "0.3",
                    )
                ),
            ),

            mail=MailConfig(

                imap_host=get_env(
                    "IMAP_HOST",
                    "imap.gmail.com",
                ),

                imap_port=get_env_int(
                    "IMAP_PORT",
                    993,
                ),

                smtp_host=get_env(
                    "SMTP_HOST",
                    "smtp.gmail.com",
                ),

                smtp_port=get_env_int(
                    "SMTP_PORT",
                    465,
                ),

                email_address=get_env(
                    "EMAIL_ADDRESS",
                    "",
                ),

                email_password=get_env(
                    "EMAIL_PASSWORD",
                    "",
                ),
            ),

            resources=ResourceConfig(

                max_concurrent_ai_requests=get_env_int(
                    "MAX_CONCURRENT_AI_REQUESTS",
                    2,
                ),

                max_background_tasks=get_env_int(
                    "MAX_BACKGROUND_TASKS",
                    3,
                ),

                max_queue_size=get_env_int(
                    "MAX_QUEUE_SIZE",
                    50,
                ),

                max_ram_percent=get_env_int(
                    "MAX_RAM_PERCENT",
                    90,
                ),

                max_cpu_percent=get_env_int(
                    "MAX_CPU_PERCENT",
                    95,
                ),
            ),

            storage=StorageConfig(

                rclone_binary=get_env(
                    "RCLONE_BINARY",
                    "rclone",
                ),

                rclone_config_path=get_env(
                    "RCLONE_CONFIG_PATH",
                    str(
                        Path.home()
                        / ".config"
                        / "rclone"
                        / "rclone.conf"
                    ),
                ),

                storage_path=get_env(
                    "STORAGE_PATH",
                    "./storage",
                ),
            ),
        )

        logger.info(
            "Configuration loaded successfully"
        )

        return config


# =========================================================
# CACHED CONFIG
# =========================================================


@lru_cache(maxsize=1)
def get_config() -> AppConfig:

    return ConfigLoader.load()
