from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from src.core.provider_manager import (
    ProviderManager,
)
from src.core.validators import (
    validate_workflow_json,
)

logger = logging.getLogger(__name__)


# =========================================================
# TASK PRIORITY
# =========================================================


class TaskPriority(
    str,
    Enum,
):

    LOW = "low"

    MEDIUM = "medium"

    HIGH = "high"

    CRITICAL = "critical"


# =========================================================
# TASK STATUS
# =========================================================


class TaskStatus(
    str,
    Enum,
):

    PENDING = "pending"

    RUNNING = "running"

    COMPLETED = "completed"

    FAILED = "failed"


# =========================================================
# AGENT TASK
# =========================================================


@dataclass
class AgentTask:

    task_id: str

    title: str

    objective: str

    priority: TaskPriority

    status: TaskStatus

    workflow: dict

    created_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )

    updated_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )

    retries: int = 0

    metadata: dict = field(
        default_factory=dict
    )


# =========================================================
# PLANNING ENGINE
# =========================================================


class AutonomousPlanningEngine:

    def __init__(
        self,
        provider_manager: (
            ProviderManager
        ),
        provider_name: str,
    ) -> None:

        self.provider_manager = (
            provider_manager
        )

        self.provider_name = (
            provider_name
        )

        self.active_tasks: dict[
            str,
            AgentTask
        ] = {}

        logger.info(
            "AutonomousPlanningEngine initialized"
        )

    # =====================================================
    # CREATE TASK
    # =====================================================

    async def create_task(
        self,
        user_input: str,
        priority: (
            TaskPriority
        ) = TaskPriority.MEDIUM,
    ) -> AgentTask:

        workflow = await (
            self.generate_workflow(
                user_input
            )
        )

        task = AgentTask(
            task_id=str(
                uuid.uuid4()
            ),
            title=(
                self._generate_title(
                    user_input
                )
            ),
            objective=user_input,
            priority=priority,
            status=TaskStatus.PENDING,
            workflow=workflow,
        )

        self.active_tasks[
            task.task_id
        ] = task

        logger.info(
            "Created task=%s",
            task.task_id,
        )

        return task

    # =====================================================
    # GENERATE WORKFLOW
    # =====================================================

    async def generate_workflow(
        self,
        user_input: str,
    ) -> dict:

        system_prompt = """
You are an autonomous planning engine.

Generate workflow JSON.

Rules:
- Return valid JSON only
- Never use markdown
- Keep workflows lightweight

Workflow format:
{
  "workflow": [
    {
      "step": 1,
      "type": "tool",
      "tool": "web_search"
    }
  ]
}
"""

        messages = [

            {
                "role": "system",
                "content": (
                    system_prompt
                ),
            },

            {
                "role": "user",
                "content": user_input,
            },
        ]

        response = await (
            self.provider_manager
            .generate(
                provider_name=(
                    self.provider_name
                ),
                messages=messages,
                temperature=0.2,
                max_tokens=700,
            )
        )

        raw = (
            response.content
            .strip()
        )

        logger.info(
            "Generated workflow=%s",
            raw,
        )

        parsed = json.loads(
            raw
        )

        validate_workflow_json(
            parsed
        )

        return parsed

    # =====================================================
    # EXECUTE TASK
    # =====================================================

    async def execute_task(
        self,
        task_id: str,
    ) -> AgentTask:

        task = self.active_tasks.get(
            task_id
        )

        if task is None:

            raise ValueError(
                "Task not found"
            )

        task.status = (
            TaskStatus.RUNNING
        )

        task.updated_at = (
            datetime.utcnow()
            .isoformat()
        )

        try:

            for step in (
                task.workflow[
                    "workflow"
                ]
            ):

                logger.info(
                    "Executing step=%s "
                    "task=%s",
                    step.get(
                        "step"
                    ),
                    task_id,
                )

                await asyncio.sleep(
                    1
                )

            task.status = (
                TaskStatus.COMPLETED
            )

            logger.info(
                "Task completed=%s",
                task_id,
            )

        except Exception:

            task.status = (
                TaskStatus.FAILED
            )

            task.retries += 1

            logger.exception(
                "Task execution failed"
            )

        task.updated_at = (
            datetime.utcnow()
            .isoformat()
        )

        return task

    # =====================================================
    # GET TASK
    # =====================================================

    async def get_task(
        self,
        task_id: str,
    ) -> AgentTask | None:

        return self.active_tasks.get(
            task_id
        )

    # =====================================================
    # LIST TASKS
    # =====================================================

    async def list_tasks(
        self,
    ) -> list[AgentTask]:

        return list(
            self.active_tasks.values()
        )

    # =====================================================
    # INTERNAL
    # =====================================================

    def _generate_title(
        self,
        text: str,
    ) -> str:

        shortened = (
            text.strip()[:60]
        )

        return shortened or "Task"
