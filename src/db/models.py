from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Any


# =========================================================
# BASE MODEL
# =========================================================


@dataclass
class BaseModel:

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


# =========================================================
# WORKFLOW MODEL
# =========================================================


@dataclass
class WorkflowModel(
    BaseModel
):

    workflow_id: str = ""

    name: str = ""

    status: str = "pending"

    total_steps: int = 0

    completed_steps: int = 0

    metadata: dict[
        str,
        Any
    ] = field(
        default_factory=dict
    )


# =========================================================
# WORKFLOW RUN MODEL
# =========================================================


@dataclass
class WorkflowRunModel(
    BaseModel
):

    run_id: str = ""

    workflow_id: str = ""

    success: bool = False

    failed_step: str | None = None

    execution_time_ms: float = 0.0

    error_message: str | None = None


# =========================================================
# TASK MODEL
# =========================================================


@dataclass
class TaskModel(
    BaseModel
):

    task_id: str = ""

    task_name: str = ""

    status: str = "pending"

    priority: int = 1

    assigned_agent: (
        str | None
    ) = None

    payload: dict[
        str,
        Any
    ] = field(
        default_factory=dict
    )

    result: dict[
        str,
        Any
    ] = field(
        default_factory=dict
    )


# =========================================================
# PROVIDER STATE
# =========================================================


@dataclass
class ProviderStateModel(
    BaseModel
):

    provider_name: str = ""

    api_key_hash: str = ""

    status: str = "active"

    cooldown_until: (
        str | None
    ) = None

    last_error: (
        str | None
    ) = None

    total_requests: int = 0

    failed_requests: int = 0


# =========================================================
# AUDIT LOG
# =========================================================


@dataclass
class AuditLogModel(
    BaseModel
):

    log_id: str = ""

    event_type: str = ""

    actor_id: str = ""

    action: str = ""

    resource: str = ""

    success: bool = True

    details: dict[
        str,
        Any
    ] = field(
        default_factory=dict
    )


# =========================================================
# MEMORY MODEL
# =========================================================


@dataclass
class MemoryModel(
    BaseModel
):

    memory_id: str = ""

    memory_type: str = ""

    content: str = ""

    embedding_ref: (
        str | None
    ) = None

    metadata: dict[
        str,
        Any
    ] = field(
        default_factory=dict
    )


# =========================================================
# SCHEMA DEFINITIONS
# =========================================================


SCHEMA_DEFINITIONS = {

    "workflows": """

    CREATE TABLE IF NOT EXISTS workflows (

        workflow_id TEXT PRIMARY KEY,

        name TEXT NOT NULL,

        status TEXT NOT NULL,

        total_steps INTEGER DEFAULT 0,

        completed_steps INTEGER DEFAULT 0,

        metadata TEXT,

        created_at TEXT NOT NULL,

        updated_at TEXT NOT NULL
    )

    """,

    "workflow_runs": """

    CREATE TABLE IF NOT EXISTS workflow_runs (

        run_id TEXT PRIMARY KEY,

        workflow_id TEXT NOT NULL,

        success INTEGER NOT NULL,

        failed_step TEXT,

        execution_time_ms REAL,

        error_message TEXT,

        created_at TEXT NOT NULL,

        updated_at TEXT NOT NULL
    )

    """,

    "tasks": """

    CREATE TABLE IF NOT EXISTS tasks (

        task_id TEXT PRIMARY KEY,

        task_name TEXT NOT NULL,

        status TEXT NOT NULL,

        priority INTEGER DEFAULT 1,

        assigned_agent TEXT,

        payload TEXT,

        result TEXT,

        created_at TEXT NOT NULL,

        updated_at TEXT NOT NULL
    )

    """,

    "provider_states": """

    CREATE TABLE IF NOT EXISTS provider_states (

        provider_name TEXT PRIMARY KEY,

        api_key_hash TEXT,

        status TEXT,

        cooldown_until TEXT,

        last_error TEXT,

        total_requests INTEGER DEFAULT 0,

        failed_requests INTEGER DEFAULT 0,

        created_at TEXT NOT NULL,

        updated_at TEXT NOT NULL
    )

    """,

    "audit_logs": """

    CREATE TABLE IF NOT EXISTS audit_logs (

        log_id TEXT PRIMARY KEY,

        event_type TEXT,

        actor_id TEXT,

        action TEXT,

        resource TEXT,

        success INTEGER,

        details TEXT,

        created_at TEXT NOT NULL,

        updated_at TEXT NOT NULL
    )

    """,

    "memories": """

    CREATE TABLE IF NOT EXISTS memories (

        memory_id TEXT PRIMARY KEY,

        memory_type TEXT,

        content TEXT,

        embedding_ref TEXT,

        metadata TEXT,

        created_at TEXT NOT NULL,

        updated_at TEXT NOT NULL
    )

    """,
}
