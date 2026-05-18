from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)


class WorkflowType(
    str,
    Enum,
):
    TOOL = "tool"
    CHAT = "chat"
    SUMMARIZE = "summarize"


class ToolName(
    str,
    Enum,
):
    WEB_SEARCH = "web_search"
    WEATHER = "weather"
    SYSTEM_STATUS = (
        "system_status"
    )
    EMAIL_SUMMARY = (
        "email_summary"
    )
    RCLONE_SEARCH = (
        "rclone_search"
    )


class WorkflowStep(
    BaseModel
):

    model_config = ConfigDict(
        extra="forbid"
    )

    step: int = Field(
        ge=1
    )

    type: WorkflowType

    tool: ToolName | None = None

    query: str | None = None

    city: str | None = None

    keyword: str | None = None

    message: str | None = None

    @field_validator(
        "tool"
    )
    @classmethod
    def validate_tool_required(
        cls,
        value,
        info,
    ):

        workflow_type = (
            info.data.get("type")
        )

        if (
            workflow_type
            == WorkflowType.TOOL
            and value is None
        ):

            raise ValueError(
                "Tool type requires tool."
            )

        return value


class WorkflowSchema(
    BaseModel
):

    model_config = ConfigDict(
        extra="forbid"
    )

    workflow: list[
        WorkflowStep
    ]

    @field_validator(
        "workflow"
    )
    @classmethod
    def validate_workflow(
        cls,
        value,
    ):

        if not value:

            raise ValueError(
                "Workflow cannot be empty."
            )

        seen_steps = set()

        for step in value:

            if step.step in seen_steps:

                raise ValueError(
                    "Duplicate workflow step."
                )

            seen_steps.add(
                step.step
            )

        return value


def validate_workflow_json(
    payload: dict[str, Any],
) -> WorkflowSchema:

    try:

        return WorkflowSchema(
            **payload
        )

    except ValidationError as exc:

        raise ValueError(
            f"Workflow validation failed: "
            f"{exc}"
        ) from exc
