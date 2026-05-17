from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Any

from app.ai.prompts import (
    INTENT_PARSER_PROMPT,
    SYSTEM_PROMPT,
)
from app.ai.provider import (
    AIProvider,
    AIProviderException,
)
from app.plugins.loader import (
    plugin_loader,
)

logger = logging.getLogger(__name__)


class AIService:
    def __init__(self):
        self.provider = AIProvider()

        self.memory: dict[
            int,
            list[dict[str, str]]
        ] = defaultdict(list)

        self.max_memory_messages = 12

        self.tool_keywords = {
            "weather": [
                "weather",
                "temperature",
                "rain",
                "forecast",
                "climate",
                "ရာသီဥတု",
                "မိုးလေဝသ"
            ],
            "web_search": [
                "search",
                "google",
                "find",
                "lookup",
                "news",
                "latest",
                "ရှာ",
                "သတင်း"
            ],
            "calendar_add": [
                "remind",
                "reminder",
                "schedule",
                "calendar",
                "meeting",
                "alarm",
                "သတိပေး",
                "အချိန်ဇယား"
            ]
        }

    async def process_user_message(
        self,
        telegram_user_id: int,
        message: str
    ) -> dict[str, Any]:
        try:
            route_type = (
                self.detect_route_type(
                    message
                )
            )

            logger.info(
                "AI route type=%s "
                "user_id=%s",
                route_type,
                telegram_user_id
            )

            if route_type == "chat":
                response = await (
                    self.handle_chat(
                        telegram_user_id,
                        message
                    )
                )

                return {
                    "type": "chat",
                    "response": response
                }

            intent_result = await (
                self.parse_intent(
                    message
                )
            )

            tool_response = await (
                self.dispatch_tool(
                    intent_result,
                    message
                )
            )

            return {
                "type": "tool",
                "response": tool_response,
                "intent_data": intent_result
            }

        except Exception as exc:
            logger.exception(
                "AIService process failed: %s",
                exc
            )

            return {
                "type": "error",
                "response": (
                    "❌ AI processing failed."
                )
            }

    def detect_route_type(
        self,
        message: str
    ) -> str:
        lowered = message.lower()

        for keywords in (
            self.tool_keywords.values()
        ):
            for keyword in keywords:
                if keyword in lowered:
                    return "tool"

        return "chat"

    async def handle_chat(
        self,
        telegram_user_id: int,
        message: str
    ) -> str:
        memory_context = (
            self.get_memory_context(
                telegram_user_id
            )
        )

        messages = [
            {
                "role": "system",
                "content": SYSTEM_PROMPT
            }
        ]

        messages.extend(
            memory_context
        )

        messages.append(
            {
                "role": "user",
                "content": message
            }
        )

        response = await (
            self.provider.generate_response(
                messages=messages
            )
        )

        self.append_memory(
            telegram_user_id,
            "user",
            message
        )

        self.append_memory(
            telegram_user_id,
            "assistant",
            response
        )

        return response

    async def parse_intent(
        self,
        message: str
    ) -> dict[str, Any]:
        messages = [
            {
                "role": "system",
                "content": (
                    INTENT_PARSER_PROMPT
                )
            },
            {
                "role": "user",
                "content": message
            }
        ]

        raw_response = await (
            self.provider.generate_response(
                messages=messages,
                temperature=0.2
            )
        )

        logger.info(
            "Intent parser raw response=%s",
            raw_response
        )

        try:
            parsed = json.loads(
                raw_response
            )

            if not isinstance(
                parsed,
                dict
            ):
                raise ValueError(
                    "Intent response "
                    "must be dict"
                )

            return parsed

        except Exception:
            logger.exception(
                "Failed to parse intent JSON"
            )

            return {
                "intent": "chat",
                "confidence": 0.0,
                "summary": message,
                "action_required": False,
                "entities": {}
            }

    async def dispatch_tool(
        self,
        intent_data: dict[str, Any],
        original_message: str
    ) -> str:
        intent = (
            intent_data.get(
                "intent",
                "chat"
            )
        )

        logger.info(
            "Dispatching tool "
            "intent=%s",
            intent
        )

        try:
            if intent == "weather":
                return await (
                    self.handle_weather(
                        intent_data,
                        original_message
                    )
                )

            if intent == "web_search":
                return await (
                    self.handle_web_search(
                        intent_data,
                        original_message
                    )
                )

            if (
                intent
                in [
                    "calendar_add",
                    "reminder"
                ]
            ):
                return (
                    "📅 Reminder feature "
                    "routing is ready."
                )

            return await (
                self.handle_chat_fallback(
                    original_message
                )
            )

        except Exception as exc:
            logger.exception(
                "Tool dispatch failed: %s",
                exc
            )

            return (
                "❌ Tool execution failed."
            )

    async def handle_weather(
        self,
        intent_data: dict[str, Any],
        original_message: str
    ) -> str:
        plugin = (
            plugin_loader.get_plugin(
                "weather"
            )
        )

        if plugin is None:
            return (
                "⚠️ Weather plugin "
                "is unavailable."
            )

        entities = (
            intent_data.get(
                "entities",
                {}
            )
        )

        city = (
            entities.get("city")
            or entities.get("location")
            or original_message
        )

        result = await plugin.get_weather(
            city
        )

        if not result:
            return (
                "⚠️ Weather data "
                "could not be retrieved."
            )

        summary_prompt = [
            {
                "role": "system",
                "content": (
                    "Summarize weather "
                    "results naturally for "
                    "Telegram users."
                )
            },
            {
                "role": "user",
                "content": result
            }
        ]

        return await (
            self.provider.generate_response(
                messages=summary_prompt
            )
        )

    async def handle_web_search(
        self,
        intent_data: dict[str, Any],
        original_message: str
    ) -> str:
        plugin = (
            plugin_loader.get_plugin(
                "websearch"
            )
        )

        if plugin is None:
            return (
                "⚠️ Web search plugin "
                "is unavailable."
            )

        entities = (
            intent_data.get(
                "entities",
                {}
            )
        )

        query = (
            entities.get("query")
            or original_message
        )

        results = await plugin.search(
            query=query
        )

        if not results:
            return (
                "⚠️ No search results found."
            )

        summary_prompt = [
            {
                "role": "system",
                "content": (
                    "Summarize search "
                    "results clearly and "
                    "concisely for "
                    "Telegram users."
                )
            },
            {
                "role": "user",
                "content": results
            }
        ]

        return await (
            self.provider.generate_response(
                messages=summary_prompt
            )
        )

    async def handle_chat_fallback(
        self,
        message: str
    ) -> str:
        messages = [
            {
                "role": "system",
                "content": SYSTEM_PROMPT
            },
            {
                "role": "user",
                "content": message
            }
        ]

        return await (
            self.provider.generate_response(
                messages=messages
            )
        )

    def append_memory(
        self,
        telegram_user_id: int,
        role: str,
        content: str
    ) -> None:
        self.memory[
            telegram_user_id
        ].append(
            {
                "role": role,
                "content": content
            }
        )

        if (
            len(
                self.memory[
                    telegram_user_id
                ]
            )
            > self.max_memory_messages
        ):
            self.memory[
                telegram_user_id
            ] = (
                self.memory[
                    telegram_user_id
                ][
                    -self.max_memory_messages:
                ]
            )

    def get_memory_context(
        self,
        telegram_user_id: int
    ) -> list[dict[str, str]]:
        return list(
            self.memory.get(
                telegram_user_id,
                []
            )
        )

    async def clear_memory(
        self,
        telegram_user_id: int
    ) -> None:
        if (
            telegram_user_id
            in self.memory
        ):
            del self.memory[
                telegram_user_id
            ]

    async def health_check(
        self
    ) -> bool:
        try:
            response = await (
                self.provider.generate_response(
                    messages=[
                        {
                            "role": "user",
                            "content": "ping"
                        }
                    ]
                )
            )

            return bool(response)

        except AIProviderException:
            logger.exception(
                "AI provider health "
                "check failed"
            )

            return False
