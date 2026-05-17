from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.services.ai_service import (
    AIService,
)

logger = logging.getLogger(__name__)

ai_service = AIService()


async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_message:
        return

    user = context.user_data.get(
        "user",
        {}
    )

    full_name = user.get(
        "full_name",
        "User"
    )

    role_name = user.get(
        "role_name",
        "unknown"
    )

    message = (
        f"✅ TeleOps-AI Ready\n\n"
        f"User: {full_name}\n"
        f"Role: {role_name}"
    )

    await update.effective_message.reply_text(
        message
    )


async def ping_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_message:
        return

    await update.effective_message.reply_text(
        "🏓 Pong"
    )


async def whoami_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_message:
        return

    user = context.user_data.get(
        "user"
    )

    if not user:
        await update.effective_message.reply_text(
            "❌ User session not found"
        )

        return

    message = (
        f"👤 User Information\n\n"
        f"ID: {user['id']}\n"
        f"Telegram ID: {user['telegram_id']}\n"
        f"Username: @{user['username']}\n"
        f"Role: {user['role_name']}"
    )

    await update.effective_message.reply_text(
        message
    )


async def clear_memory_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_user:
        return

    if not update.effective_message:
        return

    ai_service.clear_memory(
        update.effective_user.id
    )

    await update.effective_message.reply_text(
        "🧠 Chat memory cleared"
    )


async def ai_chat_message_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_user:
        return

    if not update.effective_message:
        return

    if not update.effective_message.text:
        return

    user_message = (
        update.effective_message.text
        .strip()
    )

    if not user_message:
        return

    telegram_user_id = (
        update.effective_user.id
    )

    try:
        processing_message = (
            await update.effective_message.reply_text(
                "🤖 Processing..."
            )
        )

        result = (
            await ai_service
            .process_user_message(
                telegram_user_id,
                user_message
            )
        )

        if result["type"] == "chat":
            response_text = (
                result["response"]
            )

        else:
            intent = result["intent"]

            summary = (
                result["intent_data"]
                .get("summary")
            )

            response_text = (
                f"🧠 Intent Detected\n\n"
                f"Intent: {intent}\n"
                f"Summary: {summary}"
            )

        await processing_message.edit_text(
            response_text
        )

    except Exception as exc:
        logger.exception(
            "AI message handler failed: %s",
            exc
        )

        await update.effective_message.reply_text(
            "❌ Failed to process request"
        )


async def unknown_command_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_message:
        return

    await update.effective_message.reply_text(
        "❌ Unknown command"
    )


def register_handlers(
    application: Application
) -> None:
    application.add_handler(
        CommandHandler(
            "start",
            start_command
        )
    )

    application.add_handler(
        CommandHandler(
            "ping",
            ping_command
        )
    )

    application.add_handler(
        CommandHandler(
            "whoami",
            whoami_command
        )
    )

    application.add_handler(
        CommandHandler(
            "clear",
            clear_memory_command
        )
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND,
            ai_chat_message_handler
        )
    )

    application.add_handler(
        MessageHandler(
            filters.COMMAND,
            unknown_command_handler
        )
    )
