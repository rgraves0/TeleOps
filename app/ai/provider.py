from __future__ import annotations

import asyncio
import logging
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class AIProviderError(Exception):
    pass


class AIProviderException(
    AIProviderError
):
    pass


class AIProvider:

    def __init__(self):

        self.provider = os.getenv(
            "AI_PROVIDER",
            "openrouter"
        ).lower()

        self.timeout = int(
            os.getenv(
                "AI_TIMEOUT_SECONDS",
                "60"
            )
        )

        self.temperature = float(
            os.getenv(
                "AI_TEMPERATURE",
                "0.3"
            )
        )

        self.max_tokens = int(
            os.getenv(
                "AI_MAX_TOKENS",
                "1024"
            )
        )

        # =====================================================
        # OPENROUTER KEYS
        # =====================================================

        self.openrouter_keys = [
            key.strip()
            for key in os.getenv(
                "OPENROUTER_API_KEYS",
                ""
            ).split(",")
            if key.strip()
        ]

        self.openrouter_index = 0

        # =====================================================
        # GROQ KEYS
        # =====================================================

        self.groq_keys = [
            key.strip()
            for key in os.getenv(
                "GROQ_API_KEYS",
                ""
            ).split(",")
            if key.strip()
        ]

        self.groq_index = 0

    # =========================================================
    # PUBLIC API
    # =========================================================

    async def generate_response(
        self,
        messages: list[dict],
        temperature: float | None = None,
        max_tokens: int | None = None,
        **kwargs
    ) -> str:

        original_temperature = (
            self.temperature
        )

        original_max_tokens = (
            self.max_tokens
        )

        if temperature is not None:
            self.temperature = temperature

        if max_tokens is not None:
            self.max_tokens = max_tokens

        try:

            return await (
                self.chat_completion(
                    messages
                )
            )

        finally:

            self.temperature = (
                original_temperature
            )

            self.max_tokens = (
                original_max_tokens
            )

    async def chat_completion(
        self,
        messages: list[dict]
    ) -> str:

        if self.provider == "openrouter":

            try:

                return await (
                    self._openrouter_chat(
                        messages
                    )
                )

            except Exception as exc:

                logger.exception(
                    "OpenRouter failed"
                )

                if self.groq_keys:

                    logger.warning(
                        "Falling back to Groq..."
                    )

                    return await (
                        self._groq_chat(
                            messages
                        )
                    )

                raise exc

        if self.provider == "groq":

            return await (
                self._groq_chat(
                    messages
                )
            )

        if self.provider == "gemini":

            return await (
                self._gemini_chat(
                    messages
                )
            )

        raise AIProviderError(
            f"Unsupported provider: "
            f"{self.provider}"
        )

    # =========================================================
    # OPENROUTER
    # =========================================================

    async def _openrouter_chat(
        self,
        messages: list[dict]
    ) -> str:

        if not self.openrouter_keys:

            raise AIProviderError(
                "Missing OPENROUTER_API_KEYS"
            )

        model = os.getenv(
            "OPENROUTER_MODEL",
            "openrouter/auto"
        )

        url = (
            "https://openrouter.ai/"
            "api/v1/chat/completions"
        )

        last_error = None

        for _ in range(
            len(self.openrouter_keys)
        ):

            api_key = (
                self.openrouter_keys[
                    self.openrouter_index
                ]
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
                )
            }

            payload = {
                "model": model,
                "messages": messages,
                "temperature": (
                    self.temperature
                ),
                "max_tokens": (
                    self.max_tokens
                )
            }

            logger.info(
                "Sending OpenRouter request "
                "using key index=%s",
                self.openrouter_index
            )

            try:

                async with httpx.AsyncClient(
                    timeout=self.timeout
                ) as client:

                    response = await (
                        client.post(
                            url,
                            headers=headers,
                            json=payload
                        )
                    )

                logger.info(
                    "OpenRouter response "
                    "status=%s",
                    response.status_code
                )

                # =================================================
                # SUCCESS
                # =================================================

                if response.status_code == 200:

                    data = response.json()

                    return (
                        data["choices"][0]
                        ["message"]["content"]
                        .strip()
                    )

                # =================================================
                # RATE LIMIT
                # =================================================

                if response.status_code == 429:

                    logger.warning(
                        "OpenRouter rate limit "
                        "hit on key index=%s",
                        self.openrouter_index
                    )

                    last_error = (
                        response.text
                    )

                    self.openrouter_index = (
                        (
                            self.openrouter_index
                            + 1
                        )
                        % len(
                            self.openrouter_keys
                        )
                    )

                    await asyncio.sleep(2)

                    continue

                # =================================================
                # INVALID KEY
                # =================================================

                if response.status_code in (
                    401,
                    403
                ):

                    logger.warning(
                        "Invalid OpenRouter key "
                        "index=%s",
                        self.openrouter_index
                    )

                    last_error = (
                        response.text
                    )

                    self.openrouter_index = (
                        (
                            self.openrouter_index
                            + 1
                        )
                        % len(
                            self.openrouter_keys
                        )
                    )

                    continue

                # =================================================
                # OTHER ERRORS
                # =================================================

                raise AIProviderError(
                    response.text
                )

            except Exception as exc:

                logger.exception(
                    "OpenRouter request failed"
                )

                last_error = str(exc)

                self.openrouter_index = (
                    (
                        self.openrouter_index
                        + 1
                    )
                    % len(
                        self.openrouter_keys
                    )
                )

                await asyncio.sleep(2)

        raise AIProviderError(
            f"All OpenRouter keys failed: "
            f"{last_error}"
        )

    # =========================================================
    # GROQ
    # =========================================================

    async def _groq_chat(
        self,
        messages: list[dict]
    ) -> str:

        if not self.groq_keys:

            raise AIProviderError(
                "Missing GROQ_API_KEYS"
            )

        model = os.getenv(
            "GROQ_MODEL",
            "qwen/qwen3-32b"
        )

        url = (
            "https://api.groq.com/"
            "openai/v1/chat/completions"
        )

        last_error = None

        for _ in range(
            len(self.groq_keys)
        ):

            api_key = (
                self.groq_keys[
                    self.groq_index
                ]
            )

            headers = {
                "Authorization": (
                    f"Bearer {api_key}"
                ),
                "Content-Type": (
                    "application/json"
                )
            }

            payload = {
                "model": model,
                "messages": messages,
                "temperature": (
                    self.temperature
                ),
                "max_tokens": (
                    self.max_tokens
                )
            }

            logger.info(
                "Sending Groq request "
                "using key index=%s",
                self.groq_index
            )

            try:

                async with httpx.AsyncClient(
                    timeout=self.timeout
                ) as client:

                    response = await (
                        client.post(
                            url,
                            headers=headers,
                            json=payload
                        )
                    )

                logger.info(
                    "Groq response "
                    "status=%s",
                    response.status_code
                )

                if response.status_code == 200:

                    data = response.json()

                    return (
                        data["choices"][0]
                        ["message"]["content"]
                        .strip()
                    )

                if response.status_code == 429:

                    logger.warning(
                        "Groq rate limit hit "
                        "on key index=%s",
                        self.groq_index
                    )

                    last_error = (
                        response.text
                    )

                    self.groq_index = (
                        (
                            self.groq_index
                            + 1
                        )
                        % len(
                            self.groq_keys
                        )
                    )

                    await asyncio.sleep(2)

                    continue

                raise AIProviderError(
                    response.text
                )

            except Exception as exc:

                logger.exception(
                    "Groq request failed"
                )

                last_error = str(exc)

                self.groq_index = (
                    (
                        self.groq_index
                        + 1
                    )
                    % len(
                        self.groq_keys
                    )
                )

                await asyncio.sleep(2)

        raise AIProviderError(
            f"All Groq keys failed: "
            f"{last_error}"
        )

    # =========================================================
    # GEMINI
    # =========================================================

    async def _gemini_chat(
        self,
        messages: list[dict]
    ) -> str:

        api_key = os.getenv(
            "GEMINI_API_KEY"
        )

        model = os.getenv(
            "GEMINI_MODEL",
            "gemini-2.0-flash-lite"
        )

        if not api_key:

            raise AIProviderError(
                "Missing GEMINI_API_KEY"
            )

        url = (
            "https://generativelanguage.googleapis.com/"
            f"v1beta/models/{model}:generateContent"
            f"?key={api_key}"
        )

        prompt = "\n".join(
            msg["content"]
            for msg in messages
        )

        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": prompt
                        }
                    ]
                }
            ]
        }

        logger.info(
            "Sending Gemini request..."
        )

        async with httpx.AsyncClient(
            timeout=self.timeout
        ) as client:

            response = await client.post(
                url,
                json=payload
            )

        logger.info(
            "Gemini response status=%s",
            response.status_code
        )

        if response.status_code >= 400:

            raise AIProviderError(
                response.text
            )

        data = response.json()

        try:

            return (
                data["candidates"][0]
                ["content"]["parts"][0]
                ["text"]
                .strip()
            )

        except Exception as exc:

            logger.exception(
                "Invalid Gemini response"
            )

            raise AIProviderError(
                "Failed to parse "
                "Gemini response"
            ) from exc
