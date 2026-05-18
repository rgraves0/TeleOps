from __future__ import annotations

import asyncio
import logging
import random
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import httpx

from src.utils.error_handling import (
    AuthenticationError,
    CooldownActiveError,
    ProviderRateLimitError,
    ProviderUnavailableError,
    RetryableProviderError,
    async_retry,
)

logger = logging.getLogger(__name__)


@dataclass
class ProviderResponse:
    provider: str
    model: str
    content: str
    raw_response: dict[str, Any] | None = None


class BaseProvider(ABC):

    def __init__(
        self,
        provider_name: str,
        api_keys: list[str],
        timeout: int = 60,
        cooldown_seconds: int = 20,
    ) -> None:

        self.provider_name = provider_name
        self.api_keys = [
            key.strip()
            for key in api_keys
            if key.strip()
        ]

        self.timeout = timeout
        self.cooldown_seconds = cooldown_seconds

        self.current_index = 0

        self.cooldowns: dict[str, float] = {}

        if not self.api_keys:
            raise ValueError(
                f"{provider_name} requires "
                "at least one API key."
            )

    @property
    def current_key(self) -> str:

        return self.api_keys[
            self.current_index
        ]

    def rotate_key(self) -> None:

        self.current_index = (
            (
                self.current_index + 1
            )
            % len(self.api_keys)
        )

    def set_cooldown(
        self,
        api_key: str,
    ) -> None:

        self.cooldowns[api_key] = (
            time.time()
            + self.cooldown_seconds
        )

    def is_key_available(
        self,
        api_key: str,
    ) -> bool:

        expires_at = (
            self.cooldowns.get(api_key)
        )

        if expires_at is None:
            return True

        return time.time() >= expires_at

    async def get_available_key(
        self,
    ) -> str:

        checked = 0

        while checked < len(
            self.api_keys
        ):

            candidate = self.current_key

            if self.is_key_available(
                candidate
            ):

                return candidate

            self.rotate_key()

            checked += 1

        raise CooldownActiveError(
            f"All {self.provider_name} "
            "keys are cooling down."
        )

    @abstractmethod
    async def generate(
        self,
        messages: list[dict[str, str]],
        **kwargs,
    ) -> ProviderResponse:
        raise NotImplementedError


class OpenRouterProvider(
    BaseProvider
):

    BASE_URL = (
        "https://openrouter.ai/"
        "api/v1/chat/completions"
    )

    def __init__(
        self,
        api_keys: list[str],
        model: str,
        timeout: int = 60,
    ) -> None:

        super().__init__(
            provider_name="openrouter",
            api_keys=api_keys,
            timeout=timeout,
        )

        self.model = model

    @async_retry(
        retries=3,
        delay=2,
        retry_exceptions=(
            RetryableProviderError,
            ProviderUnavailableError,
        ),
    )
    async def generate(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 1024,
    ) -> ProviderResponse:

        last_error = None

        for _ in range(
            len(self.api_keys)
        ):

            api_key = (
                await self.get_available_key()
            )

            headers = {
                "Authorization": (
                    f"Bearer {api_key}"
                ),
                "Content-Type": (
                    "application/json"
                ),
                "HTTP-Referer": (
                    "https://github.com"
                ),
                "X-Title": (
                    "TeleOps-AI"
                ),
            }

            payload = {
                "model": self.model,
                "messages": messages,
                "temperature": (
                    temperature
                ),
                "max_tokens": (
                    max_tokens
                ),
            }

            logger.info(
                "OpenRouter request "
                "using key index=%s",
                self.current_index,
            )

            try:

                async with httpx.AsyncClient(
                    timeout=self.timeout
                ) as client:

                    response = await (
                        client.post(
                            self.BASE_URL,
                            headers=headers,
                            json=payload,
                        )
                    )

                status = (
                    response.status_code
                )

                logger.info(
                    "OpenRouter response "
                    "status=%s",
                    status,
                )

                if status == 200:

                    data = (
                        response.json()
                    )

                    content = (
                        data["choices"][0]
                        ["message"]["content"]
                        .strip()
                    )

                    return ProviderResponse(
                        provider=(
                            self.provider_name
                        ),
                        model=self.model,
                        content=content,
                        raw_response=data,
                    )

                if status == 429:

                    self.set_cooldown(
                        api_key
                    )

                    self.rotate_key()

                    last_error = (
                        response.text
                    )

                    logger.warning(
                        "Rate limit hit. "
                        "Rotating key."
                    )

                    continue

                if status in (
                    401,
                    403,
                ):

                    self.rotate_key()

                    raise AuthenticationError(
                        response.text
                    )

                if status >= 500:

                    raise ProviderUnavailableError(
                        response.text
                    )

                raise RetryableProviderError(
                    response.text
                )

            except httpx.TimeoutException:

                last_error = (
                    "Request timeout"
                )

                self.rotate_key()

                await asyncio.sleep(1)

            except Exception as exc:

                logger.exception(
                    "OpenRouter failed"
                )

                last_error = str(exc)

                self.rotate_key()

                await asyncio.sleep(1)

        raise ProviderRateLimitError(
            str(last_error)
        )


class GroqProvider(
    BaseProvider
):

    BASE_URL = (
        "https://api.groq.com/"
        "openai/v1/chat/completions"
    )

    def __init__(
        self,
        api_keys: list[str],
        model: str,
        timeout: int = 60,
    ) -> None:

        super().__init__(
            provider_name="groq",
            api_keys=api_keys,
            timeout=timeout,
        )

        self.model = model

    @async_retry(
        retries=3,
        delay=2,
        retry_exceptions=(
            RetryableProviderError,
            ProviderUnavailableError,
        ),
    )
    async def generate(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 1024,
    ) -> ProviderResponse:

        last_error = None

        for _ in range(
            len(self.api_keys)
        ):

            api_key = (
                await self.get_available_key()
            )

            headers = {
                "Authorization": (
                    f"Bearer {api_key}"
                ),
                "Content-Type": (
                    "application/json"
                ),
            }

            payload = {
                "model": self.model,
                "messages": messages,
                "temperature": (
                    temperature
                ),
                "max_tokens": (
                    max_tokens
                ),
            }

            logger.info(
                "Groq request "
                "using key index=%s",
                self.current_index,
            )

            try:

                async with httpx.AsyncClient(
                    timeout=self.timeout
                ) as client:

                    response = await (
                        client.post(
                            self.BASE_URL,
                            headers=headers,
                            json=payload,
                        )
                    )

                status = (
                    response.status_code
                )

                logger.info(
                    "Groq response "
                    "status=%s",
                    status,
                )

                if status == 200:

                    data = (
                        response.json()
                    )

                    content = (
                        data["choices"][0]
                        ["message"]["content"]
                        .strip()
                    )

                    return ProviderResponse(
                        provider=(
                            self.provider_name
                        ),
                        model=self.model,
                        content=content,
                        raw_response=data,
                    )

                if status == 429:

                    self.set_cooldown(
                        api_key
                    )

                    self.rotate_key()

                    continue

                if status in (
                    401,
                    403,
                ):

                    self.rotate_key()

                    raise AuthenticationError(
                        response.text
                    )

                if status >= 500:

                    raise ProviderUnavailableError(
                        response.text
                    )

                raise RetryableProviderError(
                    response.text
                )

            except Exception as exc:

                logger.exception(
                    "Groq provider failed"
                )

                last_error = str(exc)

                self.rotate_key()

                await asyncio.sleep(1)

        raise ProviderRateLimitError(
            str(last_error)
        )


class ProviderManager:

    def __init__(self) -> None:

        self.providers: dict[
            str,
            BaseProvider
        ] = {}

        self.health_scores = (
            defaultdict(lambda: 100)
        )

    def register_provider(
        self,
        name: str,
        provider: BaseProvider,
    ) -> None:

        self.providers[name] = provider

        logger.info(
            "Registered provider=%s",
            name,
        )

    def get_provider(
        self,
        name: str,
    ) -> BaseProvider:

        provider = (
            self.providers.get(name)
        )

        if provider is None:

            raise ValueError(
                f"Provider not found: {name}"
            )

        return provider

    async def generate(
        self,
        provider_name: str,
        messages: list[dict[str, str]],
        **kwargs,
    ) -> ProviderResponse:

        provider = self.get_provider(
            provider_name
        )

        try:

            response = await (
                provider.generate(
                    messages=messages,
                    **kwargs,
                )
            )

            self.health_scores[
                provider_name
            ] += 1

            return response

        except Exception:

            self.health_scores[
                provider_name
            ] -= 10

            raise

    async def failover_generate(
        self,
        providers: list[str],
        messages: list[dict[str, str]],
        **kwargs,
    ) -> ProviderResponse:

        last_error = None

        sorted_providers = sorted(
            providers,
            key=lambda p: (
                self.health_scores[p]
            ),
            reverse=True,
        )

        for provider_name in (
            sorted_providers
        ):

            try:

                logger.info(
                    "Trying provider=%s",
                    provider_name,
                )

                return await (
                    self.generate(
                        provider_name=(
                            provider_name
                        ),
                        messages=messages,
                        **kwargs,
                    )
                )

            except Exception as exc:

                logger.exception(
                    "Provider failed=%s",
                    provider_name,
                )

                last_error = exc

                await asyncio.sleep(1)

        raise ProviderUnavailableError(
            f"All providers failed: "
            f"{last_error}"
        )
