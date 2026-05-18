from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from enum import Enum


# =========================================================
# RETRY STRATEGY
# =========================================================


class RetryStrategy(str, Enum):

    FIXED = "fixed"

    EXPONENTIAL = "exponential"

    ADAPTIVE = "adaptive"


# =========================================================
# FAILURE TYPE
# =========================================================


class FailureType(str, Enum):

    TIMEOUT = "timeout"

    RATE_LIMIT = "rate_limit"

    NETWORK = "network"

    PROVIDER = "provider"

    UNKNOWN = "unknown"


# =========================================================
# RETRY POLICY
# =========================================================


@dataclass
class RetryPolicy:

    strategy: RetryStrategy = (
        RetryStrategy.EXPONENTIAL
    )

    max_retries: int = 5

    base_delay_seconds: int = 5

    max_delay_seconds: int = 300

    jitter: bool = True

    cooldown_multiplier: float = 1.5

    provider_penalty: int = 30


# =========================================================
# RETRY DECISION
# =========================================================


@dataclass
class RetryDecision:

    should_retry: bool

    retry_after_seconds: int

    failure_type: FailureType

    reason: str


# =========================================================
# RETRY ENGINE
# =========================================================


class RetryPolicyEngine:

    def __init__(
        self,
        policy: (
            RetryPolicy
            | None
        ) = None,
    ) -> None:

        self.policy = (
            policy
            or RetryPolicy()
        )

    # =====================================================
    # CLASSIFY FAILURE
    # =====================================================

    def classify_failure(
        self,
        error: str,
    ) -> FailureType:

        error = error.lower()

        if (
            "timeout" in error
            or "timed out" in error
        ):

            return FailureType.TIMEOUT

        if (
            "429" in error
            or "rate limit" in error
        ):

            return FailureType.RATE_LIMIT

        if (
            "network" in error
            or "connection" in error
        ):

            return FailureType.NETWORK

        if (
            "provider" in error
            or "api" in error
        ):

            return FailureType.PROVIDER

        return FailureType.UNKNOWN

    # =====================================================
    # COMPUTE DELAY
    # =====================================================

    def compute_delay(
        self,
        retry_count: int,
        failure_type: FailureType,
    ) -> int:

        delay = (
            self.policy
            .base_delay_seconds
        )

        if (
            self.policy.strategy
            == RetryStrategy.FIXED
        ):

            delay = (
                self.policy
                .base_delay_seconds
            )

        elif (
            self.policy.strategy
            == RetryStrategy.EXPONENTIAL
        ):

            delay = min(

                self.policy
                .base_delay_seconds

                * (
                    2
                    ** retry_count
                ),

                self.policy
                .max_delay_seconds,
            )

        else:

            delay = min(

                int(

                    self.policy
                    .base_delay_seconds

                    * math.pow(
                        1.8,
                        retry_count,
                    )
                ),

                self.policy
                .max_delay_seconds,
            )

        # =============================================
        # FAILURE TYPE PENALTIES
        # =============================================

        if (
            failure_type
            == FailureType.RATE_LIMIT
        ):

            delay *= 2

        elif (
            failure_type
            == FailureType.TIMEOUT
        ):

            delay += 15

        elif (
            failure_type
            == FailureType.PROVIDER
        ):

            delay += (
                self.policy
                .provider_penalty
            )

        # =============================================
        # JITTER
        # =============================================

        if self.policy.jitter:

            jitter = random.randint(
                0,
                5,
            )

            delay += jitter

        return min(

            int(delay),

            self.policy
            .max_delay_seconds,
        )

    # =====================================================
    # SHOULD RETRY
    # =====================================================

    def should_retry(
        self,
        retry_count: int,
        error: str,
    ) -> RetryDecision:

        failure_type = (
            self.classify_failure(
                error
            )
        )

        if (
            retry_count
            >= self.policy.max_retries
        ):

            return RetryDecision(

                should_retry=False,

                retry_after_seconds=0,

                failure_type=
                failure_type,

                reason=(
                    "max retries exceeded"
                ),
            )

        delay = self.compute_delay(

            retry_count=
            retry_count,

            failure_type=
            failure_type,
        )

        return RetryDecision(

            should_retry=True,

            retry_after_seconds=
            delay,

            failure_type=
            failure_type,

            reason="retry allowed",
        )

    # =====================================================
    # NEXT RETRY TIME
    # =====================================================

    def next_retry_time(
        self,
        retry_after_seconds: int,
    ) -> str:

        return (

            datetime.utcnow()

            + timedelta(
                seconds=
                retry_after_seconds
            )

        ).isoformat()
