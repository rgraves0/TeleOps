from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from typing import Awaitable
from typing import Callable

from src.memory.operational_memory import (
    OperationalMemory,
)
from src.scheduler.retry_policy import (
    FailureType,
    RetryDecision,
    RetryPolicyEngine,
)

logger = logging.getLogger(__name__)


# =========================================================
# RECOVERY RESULT
# =========================================================


@dataclass
class RecoveryResult:

    recovered: bool

    retry_scheduled: bool

    retry_after_seconds: int

    failure_type: str

    message: str


# =========================================================
# RECOVERY ENGINE
# =========================================================


class RecoveryEngine:

    def __init__(
        self,
        operational_memory: (
            OperationalMemory
        ),
        retry_policy: (
            RetryPolicyEngine
        ),
    ) -> None:

        self.operational_memory = (
            operational_memory
        )

        self.retry_policy = (
            retry_policy
        )

        self.provider_cooldowns: dict[
            str,
            str
        ] = {}

        self.active_recoveries: set[
            str
        ] = set()

        logger.info(
            "RecoveryEngine initialized"
        )

    # =====================================================
    # EXECUTE WITH RECOVERY
    # =====================================================

    async def execute_with_recovery(
        self,
        operation_name: str,
        operation: Callable[
            ...,
            Awaitable,
        ],
        *args,
        provider_name: (
            str | None
        ) = None,
        retry_count: int = 0,
        **kwargs,
    ) -> Any:

        recovery_id = (
            f"{operation_name}:"
            f"{retry_count}"
        )

        self.active_recoveries.add(
            recovery_id
        )

        started = (
            datetime.utcnow()
        )

        try:

            result = await operation(
                *args,
                **kwargs,
            )

            if provider_name:

                latency_ms = (
                    (
                        datetime.utcnow()
                        - started
                    ).total_seconds()
                    * 1000
                )

                await (

                    self.operational_memory
                    .record_provider_success(

                        provider_name=
                        provider_name,

                        latency_ms=
                        latency_ms,
                    )
                )

            return result

        except Exception as exc:

            error = str(exc)

            logger.warning(
                "Recovery triggered=%s error=%s",
                operation_name,
                error,
            )

            if provider_name:

                timeout = (
                    "timeout"
                    in error.lower()
                )

                await (

                    self.operational_memory
                    .record_provider_failure(

                        provider_name=
                        provider_name,

                        error=
                        error,

                        timeout=
                        timeout,
                    )
                )

            decision = (
                self.retry_policy
                .should_retry(

                    retry_count=
                    retry_count,

                    error=error,
                )
            )

            if not (
                decision.should_retry
            ):

                raise

            await self._apply_cooldown(

                provider_name=
                provider_name,

                decision=
                decision,
            )

            logger.info(
                "Retry scheduled=%s after=%ss",
                operation_name,
                decision
                .retry_after_seconds,
            )

            await asyncio.sleep(

                decision
                .retry_after_seconds
            )

            return await (
                self.execute_with_recovery(

                    operation_name=
                    operation_name,

                    operation=
                    operation,

                    provider_name=
                    provider_name,

                    retry_count=
                    retry_count
                    + 1,

                    *args,

                    **kwargs,
                )
            )

        finally:

            self.active_recoveries.discard(
                recovery_id
            )

    # =====================================================
    # APPLY COOLDOWN
    # =====================================================

    async def _apply_cooldown(
        self,
        provider_name: (
            str | None
        ),
        decision: RetryDecision,
    ) -> None:

        if not provider_name:
            return

        if (
            decision.failure_type
            == FailureType.RATE_LIMIT
        ):

            self.provider_cooldowns[
                provider_name
            ] = (
                self.retry_policy
                .next_retry_time(

                    decision
                    .retry_after_seconds
                )
            )

            await (

                self.operational_memory
                .record_cooldown(

                    provider_name=
                    provider_name,

                    reason=
                    decision.reason,
                )
            )

    # =====================================================
    # PROVIDER AVAILABLE
    # =====================================================

    def provider_available(
        self,
        provider_name: str,
    ) -> bool:

        cooldown_until = (
            self.provider_cooldowns
            .get(provider_name)
        )

        if not cooldown_until:

            return True

        return (
            datetime.utcnow()
            >= datetime.fromisoformat(
                cooldown_until
            )
        )

    # =====================================================
    # PROVIDER COOLDOWNS
    # =====================================================

    def cooldowns(
        self,
    ) -> dict:

        return dict(
            self.provider_cooldowns
        )

    # =====================================================
    # RECOVERY STATS
    # =====================================================

    async def stats(
        self,
    ) -> dict:

        insights = await (
            self.operational_memory
            .provider_insights()
        )

        return {

            "active_recoveries":
            len(
                self.active_recoveries
            ),

            "provider_cooldowns":
            len(
                self.provider_cooldowns
            ),

            "provider_insights":
            insights,
        }

    # =====================================================
    # WORKFLOW RECOVERY
    # =====================================================

    async def recover_workflow(
        self,
        workflow_name: str,
        workflow_callable: Callable[
            ...,
            Awaitable,
        ],
        *args,
        **kwargs,
    ) -> RecoveryResult:

        started = (
            datetime.utcnow()
        )

        try:

            await self.execute_with_recovery(

                operation_name=
                workflow_name,

                operation=
                workflow_callable,

                *args,

                **kwargs,
            )

            runtime_ms = (
                (
                    datetime.utcnow()
                    - started
                ).total_seconds()
                * 1000
            )

            await (

                self.operational_memory
                .record_workflow_execution(

                    workflow_name=
                    workflow_name,

                    duration_ms=
                    runtime_ms,

                    success=True,
                )
            )

            return RecoveryResult(

                recovered=True,

                retry_scheduled=False,

                retry_after_seconds=0,

                failure_type="none",

                message=(
                    "workflow recovered"
                ),
            )

        except Exception as exc:

            runtime_ms = (
                (
                    datetime.utcnow()
                    - started
                ).total_seconds()
                * 1000
            )

            await (

                self.operational_memory
                .record_workflow_execution(

                    workflow_name=
                    workflow_name,

                    duration_ms=
                    runtime_ms,

                    success=False,

                    failure_reason=
                    str(exc),
                )
            )

            return RecoveryResult(

                recovered=False,

                retry_scheduled=False,

                retry_after_seconds=0,

                failure_type="workflow",

                message=str(exc),
            )
