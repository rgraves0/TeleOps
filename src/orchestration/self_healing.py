from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from typing import Awaitable
from typing import Callable

from src.memory.operational_memory import (
    OperationalMemory,
)
from src.scheduler.recovery_engine import (
    RecoveryEngine,
)

logger = logging.getLogger(__name__)


# =========================================================
# EXECUTION MODE
# =========================================================


class ExecutionMode(str, Enum):

    NORMAL = "normal"

    DEGRADED = "degraded"

    FALLBACK = "fallback"

    RECOVERY = "recovery"


# =========================================================
# WORKFLOW STATUS
# =========================================================


class WorkflowExecutionStatus(
    str,
    Enum,
):

    SUCCESS = "success"

    FAILED = "failed"

    PARTIAL = "partial"

    RECOVERED = "recovered"


# =========================================================
# PROVIDER ROUTE
# =========================================================


@dataclass
class ProviderRoute:

    provider_name: str

    priority: int = 1

    enabled: bool = True

    degraded_capable: bool = True

    average_latency_ms: float = 0.0

    reliability_score: float = 1.0


# =========================================================
# WORKFLOW RESULT
# =========================================================


@dataclass
class SelfHealingResult:

    success: bool

    status: WorkflowExecutionStatus

    execution_mode: ExecutionMode

    provider_used: (
        str | None
    )

    duration_ms: float

    retries: int = 0

    rerouted: bool = False

    fallback_used: bool = False

    error: (
        str | None
    ) = None

    result: Any = None


# =========================================================
# SELF HEALING ENGINE
# =========================================================


class SelfHealingEngine:

    def __init__(
        self,
        recovery_engine: (
            RecoveryEngine
        ),
        operational_memory: (
            OperationalMemory
        ),
    ) -> None:

        self.recovery_engine = (
            recovery_engine
        )

        self.operational_memory = (
            operational_memory
        )

        self.provider_routes: dict[
            str,
            list[ProviderRoute]
        ] = {}

        self.degraded_mode = False

        self.failure_counts: dict[
            str,
            int
        ] = {}

        logger.info(
            "SelfHealingEngine initialized"
        )

    # =====================================================
    # REGISTER PROVIDERS
    # =====================================================

    def register_provider_routes(
        self,
        workflow_name: str,
        providers: list[
            ProviderRoute
        ],
    ) -> None:

        ordered = sorted(

            providers,

            key=lambda item:
            item.priority,
        )

        self.provider_routes[
            workflow_name
        ] = ordered

        logger.info(
            "Provider routes registered=%s",
            workflow_name,
        )

    # =====================================================
    # EXECUTE WORKFLOW
    # =====================================================

    async def execute_workflow(
        self,
        workflow_name: str,
        workflow_callable: Callable[
            ...,
            Awaitable,
        ],
        *args,
        fallback_callable: (
            Callable[
                ...,
                Awaitable,
            ]
            | None
        ) = None,
        **kwargs,
    ) -> SelfHealingResult:

        started = (
            time.perf_counter()
        )

        routes = (
            self.provider_routes.get(
                workflow_name,
                [],
            )
        )

        retries = 0

        rerouted = False

        for route in routes:

            if not route.enabled:

                continue

            if not (

                self.recovery_engine
                .provider_available(

                    route.provider_name
                )
            ):

                continue

            try:

                mode = (
                    ExecutionMode.NORMAL
                )

                if (
                    self.degraded_mode
                ):

                    if not (
                        route
                        .degraded_capable
                    ):

                        continue

                    mode = (
                        ExecutionMode
                        .DEGRADED
                    )

                result = await (

                    self.recovery_engine
                    .execute_with_recovery(

                        operation_name=
                        workflow_name,

                        operation=
                        workflow_callable,

                        provider_name=
                        route.provider_name,

                        *args,

                        **kwargs,
                    )
                )

                duration_ms = round(

                    (
                        time.perf_counter()
                        - started
                    )
                    * 1000,

                    2,
                )

                await (

                    self.operational_memory
                    .record_workflow_execution(

                        workflow_name=
                        workflow_name,

                        duration_ms=
                        duration_ms,

                        success=True,
                    )
                )

                return SelfHealingResult(

                    success=True,

                    status=
                    WorkflowExecutionStatus
                    .SUCCESS,

                    execution_mode=
                    mode,

                    provider_used=
                    route.provider_name,

                    duration_ms=
                    duration_ms,

                    retries=
                    retries,

                    rerouted=
                    rerouted,

                    result=result,
                )

            except Exception as exc:

                retries += 1

                rerouted = True

                logger.warning(
                    "Workflow failed provider=%s error=%s",
                    route.provider_name,
                    str(exc),
                )

                await (

                    self.operational_memory
                    .record_provider_failure(

                        provider_name=
                        route.provider_name,

                        reason=
                        str(exc),
                    )
                )

                continue

        # =================================================
        # FALLBACK EXECUTION
        # =================================================

        if fallback_callable:

            try:

                logger.warning(
                    "Fallback execution=%s",
                    workflow_name,
                )

                result = await (
                    fallback_callable(
                        *args,
                        **kwargs,
                    )
                )

                duration_ms = round(

                    (
                        time.perf_counter()
                        - started
                    )
                    * 1000,

                    2,
                )

                return SelfHealingResult(

                    success=True,

                    status=
                    WorkflowExecutionStatus
                    .RECOVERED,

                    execution_mode=
                    ExecutionMode
                    .FALLBACK,

                    provider_used=None,

                    duration_ms=
                    duration_ms,

                    retries=
                    retries,

                    rerouted=
                    rerouted,

                    fallback_used=True,

                    result=result,
                )

            except Exception as exc:

                logger.exception(
                    "Fallback execution failed"
                )

        # =================================================
        # COMPLETE FAILURE
        # =================================================

        duration_ms = round(

            (
                time.perf_counter()
                - started
            )
            * 1000,

            2,
        )

        self.failure_counts[
            workflow_name
        ] = (
            self.failure_counts.get(
                workflow_name,
                0,
            )
            + 1
        )

        await (
            self._check_degraded_mode()
        )

        return SelfHealingResult(

            success=False,

            status=
            WorkflowExecutionStatus
            .FAILED,

            execution_mode=
            ExecutionMode.RECOVERY,

            provider_used=None,

            duration_ms=
            duration_ms,

            retries=retries,

            rerouted=rerouted,

            error=(
                "all providers failed"
            ),
        )

    # =====================================================
    # DEGRADED MODE CHECK
    # =====================================================

    async def _check_degraded_mode(
        self,
    ) -> None:

        total_failures = sum(

            self.failure_counts
            .values()
        )

        if total_failures >= 10:

            self.degraded_mode = True

            logger.warning(
                "System entered degraded mode"
            )

    # =====================================================
    # RECOVER DEGRADED MODE
    # =====================================================

    async def recover_degraded_mode(
        self,
    ) -> bool:

        insights = await (

            self.operational_memory
            .provider_insights()
        )

        providers = (
            insights.get(
                "providers",
                [],
            )
        )

        stable = [

            provider

            for provider
            in providers

            if provider.get(
                "reliability",
                0,
            ) >= 0.7
        ]

        if len(stable) >= 1:

            self.degraded_mode = False

            self.failure_counts.clear()

            logger.info(
                "Degraded mode recovered"
            )

            return True

        return False

    # =====================================================
    # FAILOVER PROVIDER
    # =====================================================

    async def best_provider(
        self,
        workflow_name: str,
    ) -> str | None:

        routes = (
            self.provider_routes.get(
                workflow_name,
                [],
            )
        )

        available = []

        for route in routes:

            if not route.enabled:
                continue

            if not (

                self.recovery_engine
                .provider_available(
                    route.provider_name
                )
            ):

                continue

            available.append(
                route
            )

        if not available:

            return None

        ranked = sorted(

            available,

            key=lambda item: (

                item.reliability_score,

                -item.average_latency_ms,
            ),

            reverse=True,
        )

        return (
            ranked[0]
            .provider_name
        )

    # =====================================================
    # PARTIAL EXECUTION
    # =====================================================

    async def partial_execution(
        self,
        tasks: list[
            Callable[
                ...,
                Awaitable,
            ]
        ],
    ) -> list[Any]:

        results = []

        for task in tasks:

            try:

                result = await task()

                results.append(
                    result
                )

            except Exception:

                logger.exception(
                    "Partial task failed"
                )

                continue

            await asyncio.sleep(
                0
            )

        return results

    # =====================================================
    # HEALTH STATUS
    # =====================================================

    async def health_status(
        self,
    ) -> dict:

        insights = await (

            self.operational_memory
            .provider_insights()
        )

        return {

            "degraded_mode":
            self.degraded_mode,

            "workflow_failures":
            dict(
                self.failure_counts
            ),

            "provider_insights":
            insights,

            "cooldowns":
            self.recovery_engine
            .cooldowns(),
        }

    # =====================================================
    # RESET FAILURES
    # =====================================================

    async def reset_failures(
        self,
    ) -> None:

        self.failure_counts.clear()

        self.degraded_mode = False

        logger.info(
            "Self-healing state reset"
        )
