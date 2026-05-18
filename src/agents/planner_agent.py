from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any

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
# PLANNED TASK
# =========================================================


@dataclass(slots=True)
class PlannedTask:

    task_id: str

    task_type: str

    payload: dict[str, Any]

    priority: int = 2


# =========================================================
# ACTION PLAN
# =========================================================


@dataclass(slots=True)
class ActionPlan:

    plan_id: str

    created_at: float

    source_task: dict[str, Any]

    subtasks: list[PlannedTask]

    metadata: dict[str, Any]


# =========================================================
# PLANNER AGENT
# =========================================================


class PlannerAgent(BaseAgent):

    def __init__(
        self,
        message_bus: MessageBus,
        execution_agent: str = "execution-agent",
        max_pending_tasks: int = 100,
    ) -> None:

        super().__init__(
            agent_name="planner-agent",
            heartbeat_interval=10,
            sleep_interval=0.05,
        )

        self.message_bus = (
            message_bus
        )

        self.execution_agent = (
            execution_agent
        )

        self.pending_queue: asyncio.Queue = (
            asyncio.Queue(
                maxsize=max_pending_tasks
            )
        )

        self.active_plans: dict[
            str,
            ActionPlan
        ] = {}

        self.completed_plans = 0

        self.failed_plans = 0

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
            "planner.task",
            self._handle_task_message,
        )

        await self.message_bus.subscribe(
            "planner.response",
            self._handle_execution_response,
        )

        logger.info(
            "PlannerAgent subscriptions ready"
        )

    # =====================================================
    # TASK MESSAGE
    # =====================================================

    async def _handle_task_message(
        self,
        message: AgentMessage,
    ) -> None:

        if (
            self.pending_queue.full()
        ):

            logger.warning(
                "Planner queue full"
            )

            return

        await self.pending_queue.put(
            message
        )

    # =====================================================
    # EXECUTION RESPONSE
    # =====================================================

    async def _handle_execution_response(
        self,
        message: AgentMessage,
    ) -> None:

        correlation_id = (
            message.correlation_id
        )

        if (
            correlation_id
            not in self.active_plans
        ):

            return

        payload = message.payload

        success = payload.get(
            "success",
            False,
        )

        if success:

            self.completed_plans += 1

        else:

            self.failed_plans += 1

            logger.warning(
                "Execution failed=%s",
                payload,
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

            await self._process_task(
                message
            )

        finally:

            self.pending_queue.task_done()

    # =====================================================
    # PROCESS TASK
    # =====================================================

    async def _process_task(
        self,
        message: AgentMessage,
    ) -> None:

        payload = message.payload

        plan = await self.create_plan(
            payload
        )

        self.active_plans[
            plan.plan_id
        ] = plan

        await self.dispatch_plan(
            plan
        )

        await self.increment_tasks()

    # =====================================================
    # CREATE PLAN
    # =====================================================

    async def create_plan(
        self,
        payload: dict[str, Any],
    ) -> ActionPlan:

        plan_id = str(
            uuid.uuid4()
        )

        task_type = payload.get(
            "task_type",
            "generic",
        )

        subtasks = []

        # =============================================
        # SIMPLE WORKLOAD AWARENESS
        # =============================================

        if task_type == "email":

            subtasks.append(

                PlannedTask(

                    task_id=str(
                        uuid.uuid4()
                    ),

                    task_type=
                    "email.fetch",

                    payload=payload,
                )
            )

            subtasks.append(

                PlannedTask(

                    task_id=str(
                        uuid.uuid4()
                    ),

                    task_type=
                    "email.summarize",

                    payload=payload,
                )
            )

        elif task_type == "workflow":

            steps = payload.get(
                "steps",
                [],
            )

            for step in steps:

                subtasks.append(

                    PlannedTask(

                        task_id=str(
                            uuid.uuid4()
                        ),

                        task_type=
                        step.get(
                            "type",
                            "workflow.step",
                        ),

                        payload=step,
                    )
                )

        else:

            subtasks.append(

                PlannedTask(

                    task_id=str(
                        uuid.uuid4()
                    ),

                    task_type=
                    "generic.execute",

                    payload=payload,
                )
            )

        return ActionPlan(

            plan_id=plan_id,

            created_at=time.time(),

            source_task=payload,

            subtasks=subtasks,

            metadata={

                "subtask_count":
                len(subtasks),
            },
        )

    # =====================================================
    # DISPATCH PLAN
    # =====================================================

    async def dispatch_plan(
        self,
        plan: ActionPlan,
    ) -> None:

        for subtask in (
            plan.subtasks
        ):

            message = AgentMessage(

                message_type=
                MessageType.COMMAND,

                sender=
                self.agent_name,

                recipient=
                self.execution_agent,

                topic=
                "execution.run",

                payload={

                    "plan_id":
                    plan.plan_id,

                    "task_id":
                    subtask.task_id,

                    "task_type":
                    subtask.task_type,

                    "payload":
                    subtask.payload,
                },

                correlation_id=
                plan.plan_id,

                priority=
                MessagePriority.NORMAL,
            )

            await self.message_bus.publish(
                message
            )

    # =====================================================
    # HEARTBEAT
    # =====================================================

    async def on_heartbeat(
        self,
    ) -> None:

        logger.debug(

            "Planner heartbeat "
            "pending=%s active=%s",

            self.pending_queue.qsize(),

            len(self.active_plans),
        )

    # =====================================================
    # HEALTH
    # =====================================================

    async def planner_stats(
        self,
    ) -> dict[str, Any]:

        return {

            "pending":
            self.pending_queue.qsize(),

            "active_plans":
            len(self.active_plans),

            "completed":
            self.completed_plans,

            "failed":
            self.failed_plans,
        }
