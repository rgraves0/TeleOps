from __future__ import annotations

import asyncio
import logging
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# =========================================================
# TOOL RESULT
# =========================================================


@dataclass
class ToolResult:

    success: bool

    output: Any = None

    error: str | None = None

    execution_time_ms: float = 0.0

    metadata: dict[
        str,
        Any
    ] = field(
        default_factory=dict
    )


# =========================================================
# TOOL CONTEXT
# =========================================================


@dataclass
class ToolContext:

    user_id: str | None = None

    permissions: list[
        str
    ] = field(
        default_factory=list
    )

    workflow_id: str | None = None

    metadata: dict[
        str,
        Any
    ] = field(
        default_factory=dict
    )


# =========================================================
# BASE TOOL
# =========================================================


class BaseTool(ABC):

    name: str = "base_tool"

    description: str = ""

    version: str = "1.0.0"

    timeout_seconds: int = 30

    requires_permission: (
        str | None
    ) = None

    enabled: bool = True

    # =====================================================
    # EXECUTE
    # =====================================================

    @abstractmethod
    async def execute(
        self,
        payload: dict,
        context: ToolContext,
    ) -> ToolResult:

        raise NotImplementedError

    # =====================================================
    # VALIDATE
    # =====================================================

    async def validate(
        self,
        payload: dict,
    ) -> bool:

        return True

    # =====================================================
    # HEALTHCHECK
    # =====================================================

    async def healthcheck(
        self,
    ) -> bool:

        return True

    # =====================================================
    # SAFE EXECUTION
    # =====================================================

    async def safe_execute(
        self,
        payload: dict,
        context: ToolContext,
    ) -> ToolResult:

        if not self.enabled:

            return ToolResult(

                success=False,

                error=(
                    f"Tool disabled: "
                    f"{self.name}"
                ),
            )

        try:

            valid = await (
                self.validate(
                    payload
                )
            )

            if not valid:

                return ToolResult(

                    success=False,

                    error=(
                        "Payload validation failed"
                    ),
                )

            start = (
                asyncio.get_running_loop()
                .time()
            )

            result = await (
                asyncio.wait_for(
                    self.execute(
                        payload,
                        context,
                    ),
                    timeout=(
                        self.timeout_seconds
                    ),
                )
            )

            end = (
                asyncio.get_running_loop()
                .time()
            )

            result.execution_time_ms = (
                round(
                    (end - start)
                    * 1000,
                    2,
                )
            )

            return result

        except asyncio.TimeoutError:

            logger.warning(
                "Tool timeout=%s",
                self.name,
            )

            return ToolResult(

                success=False,

                error="Tool timeout",
            )

        except Exception as exc:

            logger.exception(
                "Tool crashed=%s",
                self.name,
            )

            return ToolResult(

                success=False,

                error=str(exc),
            )

    # =====================================================
    # PERMISSION CHECK
    # =====================================================

    def has_permission(
        self,
        context: ToolContext,
    ) -> bool:

        if (
            self.requires_permission
            is None
        ):

            return True

        return (
            self.requires_permission
            in context.permissions
        )

    # =====================================================
    # TOOL INFO
    # =====================================================

    def info(
        self,
    ) -> dict:

        return {

            "name":
            self.name,

            "description":
            self.description,

            "version":
            self.version,

            "enabled":
            self.enabled,

            "timeout_seconds":
            self.timeout_seconds,

            "requires_permission":
            self.requires_permission,
        }


# =========================================================
# BASE PLUGIN
# =========================================================


class BasePlugin(ABC):

    name: str = "base_plugin"

    version: str = "1.0.0"

    enabled: bool = True

    # =====================================================
    # STARTUP
    # =====================================================

    async def startup(
        self,
    ) -> None:

        logger.info(
            "Plugin startup=%s",
            self.name,
        )

    # =====================================================
    # SHUTDOWN
    # =====================================================

    async def shutdown(
        self,
    ) -> None:

        logger.info(
            "Plugin shutdown=%s",
            self.name,
        )

    # =====================================================
    # HEALTHCHECK
    # =====================================================

    async def healthcheck(
        self,
    ) -> bool:

        return True

    # =====================================================
    # TOOL EXPORT
    # =====================================================

    @abstractmethod
    def tools(
        self,
    ) -> list[BaseTool]:

        raise NotImplementedError

    # =====================================================
    # INFO
    # =====================================================

    def info(
        self,
    ) -> dict:

        return {

            "name":
            self.name,

            "version":
            self.version,

            "enabled":
            self.enabled,

            "tools":
            [
                tool.info()
                for tool
                in self.tools()
            ],
        }
