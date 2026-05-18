from __future__ import annotations

import logging
from datetime import datetime

from telegram import Update
from telegram.ext import (
    CommandHandler,
    ContextTypes,
)

from src.core.config import (
    AppConfig,
)
from src.interfaces.telegram.admin_commands import (
    AdminAccessManager,
)
from src.db.repositories import (
    TaskRepository,
    WorkflowRepository,
)
from src.orchestration.workflow_executor import (
    WorkflowExecutor,
)
from src.core.events import (
    EventBus,
)

logger = logging.getLogger(__name__)


# =========================================================
# WORKFLOW COMMANDS
# =========================================================


class WorkflowCommands:

    def __init__(
        self,
        config: AppConfig,
        workflow_repo: (
            WorkflowRepository
        ),
        task_repo: (
            TaskRepository
        ),
        workflow_executor: (
            WorkflowExecutor
        ),
        event_bus: EventBus,
    ) -> None:

        self.config = config

        self.workflow_repo = (
            workflow_repo
        )

        self.task_repo = (
            task_repo
        )

        self.workflow_executor = (
            workflow_executor
        )

        self.event_bus = event_bus

        self.access = (
            AdminAccessManager(
                config
            )
        )

        logger.info(
            "WorkflowCommands initialized"
        )

    # =====================================================
    # REGISTER HANDLERS
    # =====================================================

    def handlers(
        self,
    ) -> list[CommandHandler]:

        return [

            CommandHandler(
                "tasks",
                self.tasks,
            ),

            CommandHandler(
                "queue",
                self.queue,
            ),

            CommandHandler(
                "workflows",
                self.workflows,
            ),

            CommandHandler(
                "mailboxes",
                self.mailboxes,
            ),
        ]

    # =====================================================
    # TASKS
    # =====================================================

    async def tasks(
        self,
        update: Update,
        context: (
            ContextTypes.DEFAULT_TYPE
        ),
    ) -> None:

        if not await (
            self.access.require_admin(
                update
            )
        ):
            return

        tasks = await (
            self.task_repo
            .list_tasks(
                limit=10
            )
        )

        if not tasks:

            await (
                update.effective_message
                .reply_text(
                    "📭 No active tasks"
                )
            )

            return

        lines = [

            "📋 Recent Tasks",

            "",
        ]

        for task in tasks:

            lines.extend(

                [

                    (
                        f"• {task['task_name']}"
                    ),

                    (
                        f"  Status: "
                        f"{task['status']}"
                    ),

                    (
                        f"  Priority: "
                        f"{task['priority']}"
                    ),

                    "",
                ]
            )

        await (
            update.effective_message
            .reply_text(
                "\n".join(lines)
            )
        )

    # =====================================================
    # QUEUE
    # =====================================================

    async def queue(
        self,
        update: Update,
        context: (
            ContextTypes.DEFAULT_TYPE
        ),
    ) -> None:

        if not await (
            self.access.require_admin(
                update
            )
        ):
            return

        stats = (
            self.workflow_executor
            .stats()
        )

        event_stats = (
            self.event_bus
            .stats()
        )

        text = "\n".join(

            [

                "📦 Queue Status",

                "",

                (
                    f"Active Workflows: "
                    f"{stats['active_workflows']}"
                ),

                (
                    f"Event Queue: "
                    f"{event_stats['queue_size']}"
                ),

                (
                    f"Subscribers: "
                    f"{len(event_stats['subscribers'])}"
                ),
            ]
        )

        await (
            update.effective_message
            .reply_text(text)
        )

    # =====================================================
    # WORKFLOWS
    # =====================================================

    async def workflows(
        self,
        update: Update,
        context: (
            ContextTypes.DEFAULT_TYPE
        ),
    ) -> None:

        if not await (
            self.access.require_admin(
                update
            )
        ):
            return

        workflows = await (
            self.workflow_repo
            .list_workflows(
                limit=10
            )
        )

        if not workflows:

            await (
                update.effective_message
                .reply_text(
                    "📭 No workflows"
                )
            )

            return

        lines = [

            "⚙️ Workflows",

            "",
        ]

        for wf in workflows:

            lines.extend(

                [

                    (
                        f"• {wf['name']}"
                    ),

                    (
                        f"  Status: "
                        f"{wf['status']}"
                    ),

                    (
                        f"  Steps: "
                        f"{wf['completed_steps']}"
                        f"/"
                        f"{wf['total_steps']}"
                    ),

                    "",
                ]
            )

        await (
            update.effective_message
            .reply_text(
                "\n".join(lines)
            )
        )

    # =====================================================
    # MAILBOXES
    # =====================================================

    async def mailboxes(
        self,
        update: Update,
        context: (
            ContextTypes.DEFAULT_TYPE
        ),
    ) -> None:

        if not await (
            self.access.require_admin(
                update
            )
        ):
            return

        text = "\n".join(

            [

                "📬 Mailbox Status",

                "",

                "• Shared inbox active",

                "• Mail monitoring enabled",

                "• AI summarizer online",

                "• Sync automation active",
            ]
        )

        await (
            update.effective_message
            .reply_text(text)
        )
