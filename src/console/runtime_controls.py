from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
import time
import traceback
from dataclasses import (
    dataclass,
    field,
)
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
)

from app.core.message_bus import (
    MessageBus,
)

from app.tools.dynamic_router import (
    DynamicToolRouter,
    RouteContext,
    RouteDecision,
)

logger = logging.getLogger(__name__)


class ProviderType(
    str,
    Enum,
):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GROQ = "groq"
    GOOGLE = "google"


@dataclass(slots=True)
class RuntimeConfiguration:
    provider: str
    log_level: str
    concurrency_limit: int
    rate_limit_per_minute: int
    model_temperature: float
    updated_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class ProviderSwitchResult:
    success: bool
    previous_provider: str
    active_provider: str
    switched_at: float
    reason: Optional[str]
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class RuntimeRBACValidator:
    """
    Default Deny runtime RBAC.
    """

    REQUIRED_PERMISSION = (
        "runtime.control.manage"
    )

    SYSTEM_ROLES = {
        "admin",
        "superuser",
        "system",
    }

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        admin_ids: Set[int],
    ) -> None:

        self.router = router
        self.admin_ids = admin_ids

    async def validate(
        self,
        *,
        telegram_user_id: int,
        permissions: Set[str],
        roles: Set[str],
        task_type: str,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:

        if (
            telegram_user_id
            not in self.admin_ids
        ):
            return False

        if not (
            roles & self.SYSTEM_ROLES
        ):
            return False

        if (
            self.REQUIRED_PERMISSION
            not in permissions
        ):
            return False

        context = RouteContext(
            requester_id=str(
                telegram_user_id
            ),
            requester_roles=roles,
            requester_permissions=
                permissions,
            task_type=task_type,
            metadata=metadata or {},
        )

        route = await self.router.route(
            task=task_type,
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class RuntimeConfigValidator:
    """
    Runtime safety guardrails.
    """

    MIN_CONCURRENCY = 1
    MAX_CONCURRENCY = 32

    MIN_RATE_LIMIT = 1
    MAX_RATE_LIMIT = 5000

    MIN_TEMPERATURE = 0.0
    MAX_TEMPERATURE = 2.0

    ALLOWED_LOG_LEVELS = {
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    }

    ALLOWED_PROVIDERS = {
        ProviderType.OPENAI.value,
        ProviderType.ANTHROPIC.value,
        ProviderType.GROQ.value,
        ProviderType.GOOGLE.value,
    }

    async def validate_provider(
        self,
        provider: str,
    ) -> bool:

        return (
            provider
            in self.ALLOWED_PROVIDERS
        )

    async def validate_config(
        self,
        *,
        log_level: str,
        concurrency_limit: int,
        rate_limit_per_minute: int,
        model_temperature: float,
    ) -> bool:

        if (
            log_level
            not in self.ALLOWED_LOG_LEVELS
        ):
            return False

        if not (
            self.MIN_CONCURRENCY
            <= concurrency_limit
            <= self.MAX_CONCURRENCY
        ):
            return False

        if not (
            self.MIN_RATE_LIMIT
            <= rate_limit_per_minute
            <= self.MAX_RATE_LIMIT
        ):
            return False

        if not (
            self.MIN_TEMPERATURE
            <= model_temperature
            <= self.MAX_TEMPERATURE
        ):
            return False

        return True


class SQLiteRuntimeStore:
    """
    SQLite WAL runtime store.
    """

    SQLITE_BUSY_TIMEOUT = 5000

    def __init__(
        self,
        *,
        database_path: str,
    ) -> None:

        self.database_path = (
            Path(database_path)
        )

        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._connection: Optional[
            sqlite3.Connection
        ] = None

    async def initialize(
        self,
    ) -> None:

        self._connection = sqlite3.connect(
            str(self.database_path),
            check_same_thread=False,
            isolation_level=None,
        )

        await asyncio.to_thread(
            self._configure
        )

        await asyncio.to_thread(
            self._create_tables
        )

    async def close(
        self,
    ) -> None:

        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    async def save_runtime_config(
        self,
        config: RuntimeConfiguration,
    ) -> None:

        await asyncio.to_thread(
            self._save_runtime_config,
            config,
        )

    async def load_runtime_config(
        self,
    ) -> Optional[
        RuntimeConfiguration
    ]:

        row = await asyncio.to_thread(
            self._load_runtime_config
        )

        if not row:
            return None

        return RuntimeConfiguration(
            provider=row[0],
            log_level=row[1],
            concurrency_limit=row[2],
            rate_limit_per_minute=row[3],
            model_temperature=row[4],
            updated_at=row[5],
            metadata=json.loads(
                row[6]
            ),
        )

    def _configure(
        self,
    ) -> None:

        self._connection.execute(
            "PRAGMA journal_mode=WAL;"
        )

        self._connection.execute(
            "PRAGMA synchronous=NORMAL;"
        )

        self._connection.execute(
            "PRAGMA temp_store=MEMORY;"
        )

        self._connection.execute(
            "PRAGMA cache_size=-1200;"
        )

        self._connection.execute(
            f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT};"
        )

    def _create_tables(
        self,
    ) -> None:

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_configuration (
                config_id INTEGER PRIMARY KEY CHECK(config_id = 1),
                provider TEXT NOT NULL,
                log_level TEXT NOT NULL,
                concurrency_limit INTEGER NOT NULL,
                rate_limit_per_minute INTEGER NOT NULL,
                model_temperature REAL NOT NULL,
                updated_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

    def _save_runtime_config(
        self,
        config: RuntimeConfiguration,
    ) -> None:

        self._connection.execute(
            """
            INSERT OR REPLACE INTO runtime_configuration (
                config_id,
                provider,
                log_level,
                concurrency_limit,
                rate_limit_per_minute,
                model_temperature,
                updated_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                config.provider,
                config.log_level,
                config.concurrency_limit,
                config.rate_limit_per_minute,
                config.model_temperature,
                config.updated_at,
                json.dumps(
                    config.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _load_runtime_config(
        self,
    ) -> Optional[Any]:

        cursor = self._connection.execute(
            """
            SELECT
                provider,
                log_level,
                concurrency_limit,
                rate_limit_per_minute,
                model_temperature,
                updated_at,
                metadata
            FROM runtime_configuration
            WHERE config_id = 1
            LIMIT 1
            """
        )

        return cursor.fetchone()


class RuntimeMemoryCache:
    """
    Lightweight runtime cache.
    """

    def __init__(
        self,
    ) -> None:

        self._cache: Dict[
            str,
            Any,
        ] = {}

    async def update(
        self,
        key: str,
        value: Any,
    ) -> None:

        self._cache[key] = value

    async def get(
        self,
        key: str,
        default: Any = None,
    ) -> Any:

        return self._cache.get(
            key,
            default,
        )

    async def snapshot(
        self,
    ) -> Dict[str, Any]:

        return dict(
            self._cache
        )


class LLMProviderHotSwapper:
    """
    Live provider switch engine.
    """

    def __init__(
        self,
        *,
        cache: RuntimeMemoryCache,
        store: SQLiteRuntimeStore,
        validator: RuntimeConfigValidator,
        message_bus: Optional[
            MessageBus
        ] = None,
    ) -> None:

        self.cache = cache
        self.store = store
        self.validator = validator
        self.message_bus = (
            message_bus
        )

    async def switch_provider(
        self,
        *,
        provider: str,
    ) -> ProviderSwitchResult:

        valid = (
            await self.validator.validate_provider(
                provider
            )
        )

        if not valid:
            return ProviderSwitchResult(
                success=False,
                previous_provider=
                    "unknown",
                active_provider=
                    "unknown",
                switched_at=
                    time.time(),
                reason=
                    "Provider validation failed",
            )

        config = (
            await self.store.load_runtime_config()
        )

        if not config:
            raise RuntimeError(
                "Runtime configuration missing"
            )

        previous = config.provider

        config.provider = provider

        config.updated_at = (
            time.time()
        )

        await self.store.save_runtime_config(
            config
        )

        await self.cache.update(
            "active_provider",
            provider,
        )

        await self._emit(
            "runtime.provider.switch",
            {
                "previous_provider":
                    previous,
                "active_provider":
                    provider,
            },
        )

        return ProviderSwitchResult(
            success=True,
            previous_provider=
                previous,
            active_provider=
                provider,
            switched_at=
                time.time(),
            reason=None,
        )

    async def _emit(
        self,
        topic: str,
        payload: Dict[str, Any],
    ) -> None:

        if not self.message_bus:
            return

        await self.message_bus.publish(
            topic=topic,
            payload={
                "timestamp":
                    time.time(),
                **payload,
            },
        )


class RuntimeConfigUpdater:
    """
    Dynamic runtime config updater.
    """

    def __init__(
        self,
        *,
        cache: RuntimeMemoryCache,
        store: SQLiteRuntimeStore,
        validator: RuntimeConfigValidator,
        message_bus: Optional[
            MessageBus
        ] = None,
    ) -> None:

        self.cache = cache
        self.store = store
        self.validator = validator
        self.message_bus = (
            message_bus
        )

    async def update_config(
        self,
        *,
        log_level: str,
        concurrency_limit: int,
        rate_limit_per_minute: int,
        model_temperature: float,
    ) -> RuntimeConfiguration:

        valid = (
            await self.validator.validate_config(
                log_level=
                    log_level,
                concurrency_limit=
                    concurrency_limit,
                rate_limit_per_minute=
                    rate_limit_per_minute,
                model_temperature=
                    model_temperature,
            )
        )

        if not valid:
            raise ValueError(
                "Runtime config validation failed"
            )

        config = (
            await self.store.load_runtime_config()
        )

        if not config:
            config = RuntimeConfiguration(
                provider=
                    ProviderType.OPENAI.value,
                log_level=
                    log_level,
                concurrency_limit=
                    concurrency_limit,
                rate_limit_per_minute=
                    rate_limit_per_minute,
                model_temperature=
                    model_temperature,
                updated_at=
                    time.time(),
            )

        else:
            config.log_level = (
                log_level
            )

            config.concurrency_limit = (
                concurrency_limit
            )

            config.rate_limit_per_minute = (
                rate_limit_per_minute
            )

            config.model_temperature = (
                model_temperature
            )

            config.updated_at = (
                time.time()
            )

        await self.store.save_runtime_config(
            config
        )

        await self.cache.update(
            "runtime_config",
            {
                "log_level":
                    log_level,
                "concurrency_limit":
                    concurrency_limit,
                "rate_limit_per_minute":
                    rate_limit_per_minute,
                "model_temperature":
                    model_temperature,
            },
        )

        await self._emit(
            "runtime.config.updated",
            {
                "log_level":
                    log_level,
                "concurrency_limit":
                    concurrency_limit,
                "rate_limit_per_minute":
                    rate_limit_per_minute,
                "model_temperature":
                    model_temperature,
            },
        )

        return config

    async def _emit(
        self,
        topic: str,
        payload: Dict[str, Any],
    ) -> None:

        if not self.message_bus:
            return

        await self.message_bus.publish(
            topic=topic,
            payload={
                "timestamp":
                    time.time(),
                **payload,
            },
        )


class LiveParameterTuner:
    """
    Telegram inline runtime tuner.
    """

    def __init__(
        self,
    ) -> None:

        self._inline_templates = {
            "provider_menu": {
                "buttons": [
                    [
                        {
                            "text":
                                "OpenAI",
                            "callback_data":
                                "provider:openai",
                        },
                        {
                            "text":
                                "Anthropic",
                            "callback_data":
                                "provider:anthropic",
                        },
                    ],
                    [
                        {
                            "text":
                                "Groq",
                            "callback_data":
                                "provider:groq",
                        },
                        {
                            "text":
                                "Google",
                            "callback_data":
                                "provider:google",
                        },
                    ],
                ]
            }
        }

    async def provider_menu(
        self,
    ) -> Dict[str, Any]:

        return self._inline_templates[
            "provider_menu"
        ]


class RuntimeControlsRuntime:
    """
    Async-first Telegram runtime controls.

    Features:
    - LLM provider hot-swapping
    - Dynamic runtime tuning
    - Inline Telegram menus
    - Live config cache updates
    - WAL-backed persistence
    - Default Deny RBAC
    - Runtime validation guardrails
    """

    WAL_CHECKPOINT_INTERVAL = 1800

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: Optional[
            MessageBus
        ] = None,
        admin_ids: Optional[
            Set[int]
        ] = None,
        database_path: str = (
            "./data/runtime_controls.db"
        ),
    ) -> None:

        self.router = router

        self.message_bus = (
            message_bus
        )

        self.admin_ids = (
            admin_ids or set()
        )

        self._validator = (
            RuntimeRBACValidator(
                router=router,
                admin_ids=
                    self.admin_ids,
            )
        )

        self._config_validator = (
            RuntimeConfigValidator()
        )

        self._store = (
            SQLiteRuntimeStore(
                database_path=
                    database_path
            )
        )

        self._cache = (
            RuntimeMemoryCache()
        )

        self.provider_switcher = (
            LLMProviderHotSwapper(
                cache=self._cache,
                store=self._store,
                validator=
                    self._config_validator,
                message_bus=
                    message_bus,
            )
        )

        self.config_updater = (
            RuntimeConfigUpdater(
                cache=self._cache,
                store=self._store,
                validator=
                    self._config_validator,
                message_bus=
                    message_bus,
            )
        )

        self.parameter_tuner = (
            LiveParameterTuner()
        )

        self._running = False

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting RuntimeControlsRuntime"
        )

        await self._store.initialize()

        existing = (
            await self._store.load_runtime_config()
        )

        if not existing:
            default_config = (
                RuntimeConfiguration(
                    provider=
                        ProviderType.OPENAI.value,
                    log_level="INFO",
                    concurrency_limit=4,
                    rate_limit_per_minute=120,
                    model_temperature=0.7,
                    updated_at=
                        time.time(),
                )
            )

            await self._store.save_runtime_config(
                default_config
            )

        self._running = True

        self._maintenance_task = (
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(
        self,
    ) -> None:

        logger.info(
            "Stopping RuntimeControlsRuntime"
        )

        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._maintenance_task

        await self._store.close()

    async def authorize(
        self,
        *,
        telegram_user_id: int,
        permissions: Set[str],
        roles: Set[str],
        task_type: str,
    ) -> bool:

        return await self._validator.validate(
            telegram_user_id=
                telegram_user_id,
            permissions=
                permissions,
            roles=roles,
            task_type=
                task_type,
        )

    async def process_callback(
        self,
        callback_data: str,
    ) -> Dict[str, Any]:

        if callback_data.startswith(
            "provider:"
        ):
            provider = (
                callback_data.split(
                    ":",
                    1,
                )[1]
            )

            result = (
                await self.provider_switcher.switch_provider(
                    provider=provider
                )
            )

            return {
                "type":
                    "provider_switch",
                "success":
                    result.success,
                "provider":
                    result.active_provider,
            }

        raise ValueError(
            "Unsupported runtime callback"
        )

    async def current_runtime(
        self,
    ) -> Optional[
        RuntimeConfiguration
    ]:

        return await self._store.load_runtime_config()

    async def cache_snapshot(
        self,
    ) -> Dict[str, Any]:

        return await self._cache.snapshot()

    async def _maintenance_loop(
        self,
    ) -> None:

        while self._running:
            try:
                await asyncio.sleep(
                    self.WAL_CHECKPOINT_INTERVAL
                )

                await asyncio.to_thread(
                    self._wal_checkpoint
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.error(
                    traceback.format_exc()
                )

    def _wal_checkpoint(
        self,
    ) -> None:

        self._store._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:

        return {
            "running":
                self._running,
            "admin_count":
                len(
                    self.admin_ids
                ),
            "timestamp":
                time.time(),
        }


DEFAULT_RUNTIME_CONTROLS = (
    RuntimeControlsRuntime
)
