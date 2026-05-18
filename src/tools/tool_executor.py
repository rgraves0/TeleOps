from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.core.events import (
    EventBus,
)
from src.plugins.base import (
    ToolContext,
    ToolResult,
)
from src.plugins.runtime import (
    PluginRuntime,
)

logger = logging.getLogger(__name__)


# =========================================================
# TOOL REQUEST
# =========================================================


@dataclass
class ToolExecutionRequest:

    tool_name: str

    payload: dict

    context: ToolContext

    execution_id: str

    created_at: str


# =========================================================
# TOOL EXECUTOR
# =========================================================


class ToolExecutor:

    def __init__(
        self,
        runtime: PluginRuntime,
        event_bus: EventBus,
    ) -> None:

        self.runtime = runtime

        self.event_bus = event_bus

        logger.info(
            "ToolExecutor initialized"
        )

    # =====================================================
    # EXECUTE
    # =====================================================

    async def execute(
        self,
        tool_name: str,
        payload: dict,
        context: ToolContext,
    ) -> ToolResult:

        request = (
            ToolExecutionRequest(

                tool_name=tool_name,

                payload=payload,

                context=context,

                execution_id=str(
                    uuid.uuid4()
                ),

                created_at=(
                    datetime.utcnow()
                    .isoformat()
                ),
            )
        )

        logger.info(
            "Executing tool=%s",
            tool_name,
        )

        await self.event_bus.emit(
            "tool.execution.started",
            {

                "execution_id":
                request.execution_id,

                "tool_name":
                tool_name,
            },
        )

        result = await (
            self.runtime.execute_tool(
                tool_name,
                payload,
                context,
            )
        )

        # =================================================
        # SUCCESS
        # =================================================

        if result.success:

            await self.event_bus.emit(
                "tool.execution.completed",
                {

                    "execution_id":
                    request.execution_id,

                    "tool_name":
                    tool_name,

                    "execution_time_ms":
                    result.execution_time_ms,
                },
            )

            logger.info(
                "Tool success=%s",
                tool_name,
            )

        # =================================================
        # FAILURE
        # =================================================

        else:

            await self.event_bus.emit(
                "tool.execution.failed",
                {

                    "execution_id":
                    request.execution_id,

                    "tool_name":
                    tool_name,

                    "error":
                    result.error,
                },
            )

            logger.warning(
                "Tool failed=%s",
                tool_name,
            )

        return result

    # =====================================================
    # ROUTE WORKFLOW STEP
    # =====================================================

    async def execute_workflow_step(
        self,
        step: dict,
        context: ToolContext,
    ) -> ToolResult:

        tool_name = step.get(
            "tool"
        )

        payload = step.get(
            "payload",
            {},
        )

        if not tool_name:

            return ToolResult(

                success=False,

                error="Missing tool name",
            )

        return await self.execute(
            tool_name,
            payload,
            context,
        )

    # =====================================================
    # EXECUTE WORKFLOW
    # =====================================================

    async def execute_workflow(
        self,
        workflow: list[dict],
        context: ToolContext,
    ) -> list[ToolResult]:

        results = []

        for step in workflow:

            result = await (
                self.execute_workflow_step(
                    step,
                    context,
                )
            )

            results.append(
                result
            )

            # =============================================
            # STOP ON FAILURE
            # =============================================

            if not result.success:

                logger.warning(
                    "Workflow halted"
                )

                break

        return results

    # =====================================================
    # HEALTHCHECK
    # =====================================================

    async def healthcheck(
        self,
    ) -> dict:

        return await (
            self.runtime
            .healthcheck()
        )
