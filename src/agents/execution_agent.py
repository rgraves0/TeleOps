from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any
from typing import Callable

from src.agents.agent_messages import (
    AgentMessage,
    MessagePriority,
    MessageType,
)
from src.agents.base_agent import (
    BaseAgent,
)
from src.agents.message_bus import (
    MessageBus,
)

logger = logging.getLogger(__name__)


# =========================================================
# EXECUTION AGENT
# =========================================================


class ExecutionAgent(BaseAgent):

    def __init__(
        self,
        message_bus: MessageBus,
        max_pending_tasks: int = 100,
        execution_timeout: int = 30,
    ) -> None:

        super().__init__(
            agent_name="execution-agent",
            heartbeat_interval=10,
            sleep_interval=0.05,
        )

        self.message_bus = (
            message_bus
        )

        self.execution_timeout = (
            execution_timeout
        )

        self.pending_queue: asyncio.Queue = (
            asyncio.Queue(
                maxsize=max_pending_tasks
            )
        )

        self.task_registry: dict[
            str,
            Callable
        ] = {}

        self.running_tasks: dict[
            str,
            asyncio.Task
        ] = {}

        self.completed_tasks = 0

        self.failed_tasks = 0

        self._register_builtin_tasks()

    # =====================================================
    # START
    # =====================================================

    async def start(
        self,
    ) -> None:

        await super().start()

        await self.message_bus.register_agent(
            self.agent_name
        )

        await self.message_bus.subscribe(
            "execution.run",
            self._handle_execution_message,
        )

        logger.info(
            "ExecutionAgent subscriptions ready"
        )

    # =====================================================
    # BUILTIN TASKS
    # =====================================================

    def _register_builtin_tasks(
        self,
    ) -> None:

        self.task_registry[
            "generic.execute"
        ] = self._generic_task

        self.task_registry[
            "email.fetch"
        ] = self._email_fetch

        self.task_registry[
            "email.summarize"
        ] = self._email_summarize

    # =====================================================
    # EXECUTION MESSAGE
    # =====================================================

    async def _handle_execution_message(
        self,
        message: AgentMessage,
    ) -> None:

        if (
            self.pending_queue.full()
        ):

            logger.warning(
                "Execution queue full"
            )

            return

        await self.pending_queue.put(
            message
        )

    # =====================================================
    # RUN CYCLE
    # =====================================================

    async def run_cycle(
        self,
    ) -> None:

        try:

            message = await asyncio.wait_for(

                self.pending_queue.get(),

                timeout=0.5,
            )

        except asyncio.TimeoutError:

            return

        try:

            await self._execute_message(
                message
            )

        finally:

            self.pending_queue.task_done()

    # =====================================================
    # EXECUTE MESSAGE
    # =====================================================

    async def _execute_message(
        self,
        message: AgentMessage,
    ) -> None:

        payload = message.payload

        task_type = payload.get(
            "task_type",
            "generic.execute",
        )

        executor = (
            self.task_registry.get(
                task_type
            )
        )

        if not executor:

            await self._send_error(

                message,

                f"unknown task={task_type}",
            )

            return

        task = asyncio.create_task(

            self._safe_execute(

                message,

                executor,
            )
        )

        self.running_tasks[
            payload.get(
                "task_id",
                "unknown",
            )
        ] = task

    # =====================================================
    # SAFE EXECUTE
    # =====================================================

    async def _safe_execute(
        self,
        message: AgentMessage,
        executor: Callable,
    ) -> None:

        payload = message.payload

        task_id = payload.get(
            "task_id",
            "unknown",
        )

        try:

            result = await asyncio.wait_for(

                executor(payload),

                timeout=
                self.execution_timeout,
            )

            self.completed_tasks += 1

            await self._send_success(

                message,

                result,
            )

        except Exception as exc:

            self.failed_tasks += 1

            logger.exception(
                "Execution failed"
            )

            await self._send_error(

                message,

                str(exc),
            )

        finally:

            self.running_tasks.pop(
                task_id,
                None,
            )

            await self.increment_tasks()

    # =====================================================
    # SEND SUCCESS
    # =====================================================

    async def _send_success(
        self,
        original: AgentMessage,
        result: dict[str, Any],
    ) -> None:

        response = AgentMessage(

            message_type=
            MessageType.RESPONSE,

            sender=
            self.agent_name,

            recipient=
            "planner-agent",

            topic=
            "planner.response",

            payload={

                "success":
                True,

                "result":
                result,
            },

            correlation_id=
            original.correlation_id,

            priority=
            MessagePriority.NORMAL,
        )

        await self.message_bus.publish(
            response
        )

    # =====================================================
    # SEND ERROR
    # =====================================================

    async def _send_error(
        self,
        original: AgentMessage,
        error: str,
    ) -> None:

        response = AgentMessage(

            message_type=
            MessageType.ERROR,

            sender=
            self.agent_name,

            recipient=
            "planner-agent",

            topic=
            "planner.response",

            payload={

                "success":
                False,

                "error":
                error,
            },

            correlation_id=
            original.correlation_id,

            priority=
            MessagePriority.HIGH,
        )

        await self.message_bus.publish(
            response
        )

    # =====================================================
    # BUILTIN EXECUTORS
    # =====================================================

    async def _generic_task(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:

        await asyncio.sleep(
            0.1
        )

        return {

            "status":
            "completed",

            "payload":
            payload,
        }

    async def _email_fetch(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:

        await asyncio.sleep(
            0.2
        )

        return {

            "emails":
            [],

            "status":
            "fetched",
        }

    async def _email_summarize(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:

        await asyncio.sleep(
            0.3
        )

        return {

            "summary":
            "Email summarized",

            "status":
            "summarized",
        }

    # =====================================================
    # HEARTBEAT
    # =====================================================

    async def on_heartbeat(
        self,
    ) -> None:

        logger.debug(

            "Execution heartbeat "
            "pending=%s running=%s",

            self.pending_queue.qsize(),

            len(self.running_tasks),
        )

    # =====================================================
    # HEALTH
    # =====================================================

    async def execution_stats(
        self,
    ) -> dict[str, Any]:

        return {

            "pending":
            self.pending_queue.qsize(),

            "running":
            len(self.running_tasks),

            "completed":
            self.completed_tasks,

            "failed":
            self.failed_tasks,
        }
