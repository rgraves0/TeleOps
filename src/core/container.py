from __future__ import annotations

import logging
from typing import Any
from typing import Callable

from src.core.config import (
    AppConfig,
)
from src.core.events import (
    EventBus,
)
from src.monitoring.health import (
    HealthMonitor,
)
from src.monitoring.metrics import (
    LightweightMetricsCollector,
)
from src.core.resource_manager import (
    ResourceManager,
    ResourceLimits,
)

logger = logging.getLogger(__name__)


# =========================================================
# SERVICE CONTAINER
# =========================================================


class ServiceContainer:

    def __init__(
        self,
        config: AppConfig,
    ) -> None:

        self.config = config

        self._services: dict[
            str,
            Any
        ] = {}

        self._factories: dict[
            str,
            Callable[[], Any]
        ] = {}

        logger.info(
            "ServiceContainer initialized"
        )

    # =====================================================
    # REGISTER INSTANCE
    # =====================================================

    def register_instance(
        self,
        name: str,
        instance: Any,
    ) -> None:

        self._services[
            name
        ] = instance

        logger.debug(
            "Registered instance=%s",
            name,
        )

    # =====================================================
    # REGISTER FACTORY
    # =====================================================

    def register_factory(
        self,
        name: str,
        factory: Callable[[], Any],
    ) -> None:

        self._factories[
            name
        ] = factory

        logger.debug(
            "Registered factory=%s",
            name,
        )

    # =====================================================
    # GET SERVICE
    # =====================================================

    def get(
        self,
        name: str,
    ) -> Any:

        # =================================================
        # EXISTING INSTANCE
        # =================================================

        if (
            name
            in self._services
        ):

            return self._services[
                name
            ]

        # =================================================
        # FACTORY
        # =================================================

        if (
            name
            in self._factories
        ):

            instance = (
                self._factories[
                    name
                ]()
            )

            self._services[
                name
            ] = instance

            return instance

        raise KeyError(
            f"Service not found: {name}"
        )

    # =====================================================
    # HAS
    # =====================================================

    def has(
        self,
        name: str,
    ) -> bool:

        return (
            name in self._services
            or name in self._factories
        )

    # =====================================================
    # REMOVE
    # =====================================================

    def remove(
        self,
        name: str,
    ) -> None:

        self._services.pop(
            name,
            None,
        )

        self._factories.pop(
            name,
            None,
        )

    # =====================================================
    # CLEAR
    # =====================================================

    def clear(
        self,
    ) -> None:

        self._services.clear()

        self._factories.clear()

        logger.warning(
            "Container cleared"
        )

    # =====================================================
    # SERVICE COUNT
    # =====================================================

    def count(
        self,
    ) -> int:

        return (
            len(self._services)
            + len(self._factories)
        )

    # =====================================================
    # LIST SERVICES
    # =====================================================

    def services(
        self,
    ) -> list[str]:

        names = set()

        names.update(
            self._services.keys()
        )

        names.update(
            self._factories.keys()
        )

        return sorted(names)


# =========================================================
# DEFAULT CONTAINER BUILDER
# =========================================================


def build_container(
    config: AppConfig,
) -> ServiceContainer:

    container = ServiceContainer(
        config=config
    )

    # =====================================================
    # CONFIG
    # =====================================================

    container.register_instance(
        "config",
        config,
    )

    # =====================================================
    # EVENT BUS
    # =====================================================

    container.register_factory(

        "event_bus",

        lambda: EventBus(
            queue_limit=(
                config.resources
                .max_queue_size
            )
        ),
    )

    # =====================================================
    # METRICS
    # =====================================================

    container.register_factory(

        "metrics",

        lambda: (
            LightweightMetricsCollector(
                collection_interval=30,
                max_history=100,
            )
        ),
    )

    # =====================================================
    # HEALTH
    # =====================================================

    container.register_factory(

        "health_monitor",

        lambda: HealthMonitor(
            interval_seconds=15,
            ram_warning=80,
            ram_critical=92,
            cpu_warning=85,
            cpu_critical=95,
            process_memory_limit_mb=700,
        ),
    )

    # =====================================================
    # RESOURCE MANAGER
    # =====================================================

    def resource_factory():

        health_monitor = (
            container.get(
                "health_monitor"
            )
        )

        return ResourceManager(

            health_monitor=(
                health_monitor
            ),

            limits=ResourceLimits(

                max_concurrent_ai_requests=(
                    config.resources
                    .max_concurrent_ai_requests
                ),

                max_concurrent_background_tasks=(
                    config.resources
                    .max_background_tasks
                ),

                max_queue_size=(
                    config.resources
                    .max_queue_size
                ),

                ai_requests_per_minute=20,

                max_active_async_tasks=150,

                cooldown_seconds=5,
            ),
        )

    container.register_factory(
        "resource_manager",
        resource_factory,
    )

    logger.info(
        "Container build complete"
    )

    return container
