from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import (
    Callable,
)
from typing import Any

logger = logging.getLogger(__name__)


# =========================================================
# EXCEPTIONS
# =========================================================


class TeleOpsError(
    Exception
):
    pass


class RetryableProviderError(
    TeleOpsError
):
    pass


class ProviderUnavailableError(
    TeleOpsError
):
    pass


class ProviderRateLimitError(
    TeleOpsError
):
    pass


class AuthenticationError(
    TeleOpsError
):
    pass


class WorkflowValidationError(
    TeleOpsError
):
    pass


class CooldownActiveError(
    TeleOpsError
):
    pass


# =========================================================
# ASYNC RETRY DECORATOR
# =========================================================


def async_retry(
    retries: int = 3,
    delay: int = 2,
    retry_exceptions: tuple[
        type[Exception],
        ...
    ] = (
        Exception,
    ),
):

    def decorator(
        func: Callable,
    ):

        @functools.wraps(func)
        async def wrapper(
            *args,
            **kwargs,
        ):

            last_error = None

            for attempt in range(
                1,
                retries + 1,
            ):

                try:

                    return await func(
                        *args,
                        **kwargs,
                    )

                except retry_exceptions as exc:

                    last_error = exc

                    logger.warning(
                        "Retry attempt=%s/%s "
                        "function=%s "
                        "error=%s",
                        attempt,
                        retries,
                        func.__name__,
                        exc,
                    )

                    if (
                        attempt
                        >= retries
                    ):

                        break

                    await asyncio.sleep(
                        delay
                    )

            raise last_error

        return wrapper

    return decorator


# =========================================================
# ASYNC COOLDOWN DECORATOR
# =========================================================


def async_cooldown(
    cooldown_seconds: int = 10,
):

    last_called = {}

    def decorator(
        func: Callable,
    ):

        @functools.wraps(func)
        async def wrapper(
            *args,
            **kwargs,
        ):

            key = (
                f"{func.__module__}:"
                f"{func.__name__}"
            )

            now = (
                asyncio.get_event_loop()
                .time()
            )

            last_time = (
                last_called.get(key)
            )

            if (
                last_time is not None
                and now - last_time
                < cooldown_seconds
            ):

                remaining = (
                    cooldown_seconds
                    - (
                        now
                        - last_time
                    )
                )

                raise CooldownActiveError(
                    f"Cooldown active. "
                    f"Retry in "
                    f"{remaining:.2f}s"
                )

            last_called[key] = now

            return await func(
                *args,
                **kwargs,
            )

        return wrapper

    return decorator
