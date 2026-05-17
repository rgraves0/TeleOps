from __future__ import annotations

import json
import logging
import os
import platform
from datetime import datetime
from typing import Any

import psutil

from app.ai.prompts import (
    SYSTEM_PROMPT,
)
from app.ai.provider import (
    AIProvider,
    AIProviderException,
)
from app.database.repositories.chat_memory import (
    chat_memory_repository,
)
from app.plugins.loader import (
    plugin_loader,
)

logger = logging.getLogger(__name__)


AGENT_ROUTER_PROMPT = """
You are TeleOps-AI Autonomous Router.

Your task is to decide whether the user's request:
1. Needs a tool/plugin
2. Or can be answered directly as casual conversation

AVAILABLE TOOLS:

1. web_search
Purpose:
- Search latest news
- Search facts
- Search internet information
- Search current events
Parameters:
{
  "tool": "web_search",
  "query": "search query"
}

2. weather
Purpose:
- Weather forecast
- Temperature
- Climate
- Rain information
Parameters:
{
  "tool": "weather",
  "city": "city name"
}

3. system_status
Purpose:
- CPU usage
- RAM usage
- Disk usage
- System health
Parameters:
{
  "tool": "system_status"
}

RULES:
- If tool is needed, return ONLY valid JSON.
- If no tool is needed, return:
{
  "tool": "none"
}

- Never explain reasoning.
- Never return markdown.
- Never return extra text.
"""


SUMMARY_PROMPT = """
You are TeleOps-AI.

Convert raw tool outputs into natural conversational replies.

Rules:
- Never expose raw JSON
- Never expose internal system details
- Keep responses concise
- Reply in same language as user
- Burmese user => Burmese response
- English user => English response
- Sound natural and human
"""


class AIService:
    def __init__(self):
        self.provider = AIProvider()

        self.max_memory_messages = 20

    async def process_user_message(
        self,
        telegram_user_id: int,
        message: str
    ) -> dict[str, Any]:
        try:
            memory_context = (
                await self.build_memory_context(
                    telegram_user_id
                )
            )

            tool_decision = (
                await self.autonomous_tool_selection(
                    message=message,
                    memory_context=memory_context
                )
            )

            selected_tool = (
                tool_decision.get(
                    "tool",
                    "none"
                )
            )

            logger.info(
                "Autonomous tool "
                "selection=%s",
                selected_tool
            )

            if selected_tool == "none":
                response = await (
                    self.handle_chat(
                        telegram_user_id,
                        message,
                        memory_context
                    )
                )

                await (
                    self.store_conversation_pair(
                        telegram_user_id,
                        message,
                        response
                    )
                )

                return {
                    "type": "chat",
                    "response": response
                }

            tool_result = await (
                self.execute_autonomous_tool(
                    tool_decision
                )
            )

            summarized_response = (
                await self.summarize_tool_output(
                    original_user_message=message,
                    tool_name=selected_tool,
                    raw_output=tool_result
                )
            )

            await (
                self.store_conversation_pair(
                    telegram_user_id,
                    message,
                    summarized_response
                )
            )

            return {
                "type": "tool",
                "tool": selected_tool,
                "response": summarized_response
            }

        except Exception as exc:
            logger.exception(
                "AIService process failed: %s",
                exc
            )

            fallback_response = (
                await self.generate_friendly_error(
                    user_message=message
                )
            )

            await (
                self.store_conversation_pair(
                    telegram_user_id,
                    message,
                    fallback_response
                )
            )

            return {
                "type": "error",
                "response": fallback_response
            }

    async def autonomous_tool_selection(
        self,
        message: str,
        memory_context: list[
            dict[str, str]
        ]
    ) -> dict[str, Any]:
        router_messages = [
            {
                "role": "system",
                "content": (
                    AGENT_ROUTER_PROMPT
                )
            }
        ]

        router_messages.extend(
            memory_context[-10:]
        )

        router_messages.append(
            {
                "role": "user",
                "content": message
            }
        )

        raw_response = await (
            self.provider.generate_response(
                messages=router_messages,
                temperature=0.1
            )
        )

        logger.info(
            "Autonomous router "
            "response=%s",
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
                    "Router response "
                    "must be dict"
                )

            return parsed

        except Exception:
            logger.exception(
                "Router JSON parse failed"
            )

            return {
                "tool": "none"
            }

    async def build_memory_context(
        self,
        telegram_user_id: int
    ) -> list[dict[str, str]]:
        history = await (
            chat_memory_repository
            .get_recent_history(
                telegram_user_id=(
                    telegram_user_id
                ),
                limit=(
                    self.max_memory_messages
                )
            )
        )

        messages: list[
            dict[str, str]
        ] = []

        for item in history:
            role = item.get(
                "role",
                "user"
            )

            content = item.get(
                "content",
                ""
            )

            if not content:
                continue

            messages.append(
                {
                    "role": role,
                    "content": content
                }
            )

        return messages

    async def handle_chat(
        self,
        telegram_user_id: int,
        message: str,
        memory_context: list[
            dict[str, str]
        ]
    ) -> str:
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
                messages=messages,
                temperature=0.7
            )
        )

        return response

    async def execute_autonomous_tool(
        self,
        tool_decision: dict[str, Any]
    ) -> str:
        tool_name = (
            tool_decision.get(
                "tool"
            )
        )

        logger.info(
            "Executing autonomous "
            "tool=%s",
            tool_name
        )

        if tool_name == "web_search":
            return await (
                self.execute_web_search(
                    tool_decision
                )
            )

        if tool_name == "weather":
            return await (
                self.execute_weather(
                    tool_decision
                )
            )

        if tool_name == "system_status":
            return await (
                self.execute_system_status()
            )

        return (
            "No suitable tool "
            "was selected."
        )

    async def execute_web_search(
        self,
        tool_decision: dict[str, Any]
    ) -> str:
        plugin = (
            plugin_loader.get_plugin(
                "websearch"
            )
        )

        if plugin is None:
            return (
                "Web search plugin "
                "is unavailable."
            )

        query = (
            tool_decision.get(
                "query",
                ""
            )
        )

        if not query:
            return (
                "Search query "
                "was empty."
            )

        logger.info(
            "Web search query=%s",
            query
        )

        result = await plugin.search(
            query=query
        )

        return str(result)

    async def execute_weather(
        self,
        tool_decision: dict[str, Any]
    ) -> str:
        plugin = (
            plugin_loader.get_plugin(
                "weather"
            )
        )

        if plugin is None:
            return (
                "Weather plugin "
                "is unavailable."
            )

        city = (
            tool_decision.get(
                "city",
                ""
            )
        )

        if not city:
            return (
                "City parameter "
                "was empty."
            )

        logger.info(
            "Weather city=%s",
            city
        )

        result = await (
            plugin.get_weather(
                city
            )
        )

        return str(result)

    async def execute_system_status(
        self
    ) -> str:
        memory = (
            psutil.virtual_memory()
        )

        cpu_usage = (
            psutil.cpu_percent(
                interval=1
            )
        )

        disk = psutil.disk_usage(
            "/"
        )

        boot_time = datetime.fromtimestamp(
            psutil.boot_time()
        )

        plugins = (
            plugin_loader.list_plugins()
        )

        plugin_names = [
            plugin["name"]
            for plugin in plugins
            if plugin["enabled"]
        ]

        return (
            f"System Status\n\n"
            f"Platform: "
            f"{platform.system()} "
            f"{platform.release()}\n"
            f"CPU Usage: "
            f"{cpu_usage}%\n"
            f"RAM Usage: "
            f"{memory.percent}%\n"
            f"Available RAM: "
            f"{round(memory.available / 1024 / 1024)} MB\n"
            f"Disk Usage: "
            f"{disk.percent}%\n"
            f"Python Version: "
            f"{platform.python_version()}\n"
            f"Boot Time: "
            f"{boot_time}\n"
            f"Loaded Plugins: "
            f"{', '.join(plugin_names)}\n"
            f"AI Provider: "
            f"{os.getenv('AI_PROVIDER')}"
        )

    async def summarize_tool_output(
        self,
        original_user_message: str,
        tool_name: str,
        raw_output: str
    ) -> str:
        messages = [
            {
                "role": "system",
                "content": SUMMARY_PROMPT
            },
            {
                "role": "user",
                "content": (
                    f"User Message:\n"
                    f"{original_user_message}\n\n"
                    f"Tool Used:\n"
                    f"{tool_name}\n\n"
                    f"Raw Tool Output:\n"
                    f"{raw_output}"
                )
            }
        ]

        try:
            response = await (
                self.provider.generate_response(
                    messages=messages,
                    temperature=0.4
                )
            )

            return response

        except Exception as exc:
            logger.exception(
                "Summary generation "
                "failed: %s",
                exc
            )

            return (
                "⚠️ Sorry, I couldn't "
                "summarize the result "
                "properly."
            )

    async def store_conversation_pair(
        self,
        telegram_user_id: int,
        user_message: str,
        assistant_message: str
    ) -> None:
        try:
            await (
                chat_memory_repository
                .store_message(
                    telegram_user_id=(
                        telegram_user_id
                    ),
                    role="user",
                    content=user_message
                )
            )

            await (
                chat_memory_repository
                .store_message(
                    telegram_user_id=(
                        telegram_user_id
                    ),
                    role="assistant",
                    content=assistant_message
                )
            )

        except Exception:
            logger.exception(
                "Failed to store "
                "chat memory"
            )

    async def clear_memory(
        self,
        telegram_user_id: int
    ) -> None:
        try:
            await (
                chat_memory_repository
                .clear_history(
                    telegram_user_id
                )
            )

            logger.info(
                "Memory cleared "
                "telegram_user_id=%s",
                telegram_user_id
            )

        except Exception:
            logger.exception(
                "Failed to clear "
                "memory"
            )

    async def generate_friendly_error(
        self,
        user_message: str
    ) -> str:
        messages = [
            {
                "role": "system",
                "content": SUMMARY_PROMPT
            },
            {
                "role": "user",
                "content": (
                    "Generate a friendly "
                    "AI assistant error "
                    "reply for this "
                    f"user message:\n"
                    f"{user_message}"
                )
            }
        ]

        try:
            return await (
                self.provider.generate_response(
                    messages=messages,
                    temperature=0.3
                )
            )

        except Exception:
            return (
                "⚠️ Sorry, something "
                "went wrong while "
                "processing your request."
            )

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
