from __future__ import annotations

import asyncio
import contextlib
import html
import logging
import os
from datetime import datetime

import pytz
from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import (
    ChatAction,
    ParseMode,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.database.repositories.users import UserRepository
from app.services.ai_service import AIService
from app.services.reminder_service import ReminderService

load_dotenv()

logger = logging.getLogger(__name__)

TIMEZONE = os.getenv(
    "TIMEZONE",
    "Asia/Bangkok"
)

timezone = pytz.timezone(
    TIMEZONE
)

user_repository = UserRepository()
reminder_service = ReminderService()
ai_service = AIService()


# =========================================================
# AI MODE COMMANDS
# =========================================================

async def ai_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    context.user_data["ai_mode"] = True

    message = (
        "🤖 <b>AI Chat Mode Enabled</b>\n\n"
        "You can now chat naturally with TeleOps-AI.\n\n"
        "Examples:\n"
        "• Search latest AI news\n"
        "• Check unread emails\n"
        "• Find backup.zip\n"
        "• What's the weather in Bangkok?\n\n"
        "Use /exitai to leave AI mode."
    )

    await update.message.reply_text(
        text=message,
        parse_mode=ParseMode.HTML
    )


async def exit_ai_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    context.user_data["ai_mode"] = False

    await update.message.reply_text(
        text=(
            "✅ <b>AI Chat Mode Disabled</b>\n\n"
            "You are now back in normal command mode."
        ),
        parse_mode=ParseMode.HTML
    )


async def clear_memory_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    telegram_user_id = (
        update.effective_user.id
    )

    await ai_service.clear_memory(
        telegram_user_id
    )

    await update.message.reply_text(
        text=(
            "🧠 <b>Conversation memory cleared.</b>"
        ),
        parse_mode=ParseMode.HTML
    )


# =========================================================
# AI HELPERS
# =========================================================

async def typing_indicator_loop(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    stop_event: asyncio.Event
) -> None:

    try:
        while not stop_event.is_set():

            await context.bot.send_chat_action(
                chat_id=chat_id,
                action=ChatAction.TYPING
            )

            await asyncio.sleep(4)

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        logger.exception(
            "Typing indicator failed: %s",
            exc
        )


async def update_processing_message(
    processing_message,
    text: str
) -> None:

    try:
        await processing_message.edit_text(
            text=text,
            parse_mode=ParseMode.HTML
        )

    except Exception as exc:
        logger.debug(
            "Processing message update skipped: %s",
            exc
        )


def detect_processing_stage(
    user_message: str
) -> str:

    lowered = user_message.lower()

    if any(
        keyword in lowered
        for keyword in [
            "email",
            "mail",
            "inbox"
        ]
    ):
        return "📧 <b>Fetching unread emails...</b>"

    if any(
        keyword in lowered
        for keyword in [
            "weather",
            "temperature",
            "rain"
        ]
    ):
        return "🌦 <b>Checking weather data...</b>"

    if any(
        keyword in lowered
        for keyword in [
            "search",
            "news",
            "google",
            "internet"
        ]
    ):
        return "🌐 <b>Searching the web...</b>"

    if any(
        keyword in lowered
        for keyword in [
            ".zip",
            ".pdf",
            ".doc",
            ".docx",
            "find",
            "storage",
            "backup"
        ]
    ):
        return "☁️ <b>Searching cloud storage...</b>"

    if any(
        keyword in lowered
        for keyword in [
            "system",
            "status",
            "cpu",
            "ram"
        ]
    ):
        return "🖥 <b>Collecting system status...</b>"

    return "🤔 <b>Thinking...</b>"


def format_ai_response(
    response_text: str
) -> str:

    cleaned = response_text.strip()

    cleaned = html.escape(
        cleaned
    )

    cleaned = cleaned.replace(
        "\n",
        "<br>"
    )

    return cleaned


# =========================================================
# AI CHAT CORE
# =========================================================

async def ai_chat_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    if update.message is None:
        return

    if update.effective_user is None:
        return

    telegram_user_id = (
        update.effective_user.id
    )

    user_message = (
        update.message.text or ""
    ).strip()

    if not user_message:
        return

    ai_mode = context.user_data.get(
        "ai_mode",
        False
    )

    if not ai_mode:
        return

    chat_id = update.effective_chat.id

    processing_message = (
        await update.message.reply_text(
            text="🤖 <b>Processing request...</b>",
            parse_mode=ParseMode.HTML
        )
    )

    processing_stage = (
        detect_processing_stage(
            user_message
        )
    )

    await update_processing_message(
        processing_message,
        processing_stage
    )

    stop_event = asyncio.Event()

    typing_task = asyncio.create_task(
        typing_indicator_loop(
            context=context,
            chat_id=chat_id,
            stop_event=stop_event
        )
    )

    try:

        result = (
            await ai_service.process_user_message(
                telegram_user_id=telegram_user_id,
                message=user_message
            )
        )

        response_text = result.get(
            "response",
            "No response generated."
        )

        formatted_response = (
            format_ai_response(
                response_text
            )
        )

        response_type = result.get(
            "type",
            "chat"
        )

        header = ""

        if response_type == "workflow":
            header = (
                "⚡ <b>Workflow Completed</b>\n\n"
            )

        elif response_type == "tool":
            header = (
                "🛠 <b>Task Completed</b>\n\n"
            )

        elif response_type == "error":
            header = (
                "⚠️ <b>Processing Issue</b>\n\n"
            )

        final_message = (
            f"{header}{formatted_response}"
        )

        await processing_message.edit_text(
            text=final_message,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )

        return

    except Exception as exc:

        logger.exception(
            "AI chat handler failed: %s",
            exc
        )

        await processing_message.edit_text(
            text=(
                "⚠️ <b>Something went wrong.</b>"
                "<br><br>"
                "Please try again."
            ),
            parse_mode=ParseMode.HTML
        )

    finally:

        stop_event.set()

        typing_task.cancel()

        with contextlib.suppress(
            asyncio.CancelledError
        ):
            await typing_task


# =========================================================
# CALENDAR COMMANDS
# =========================================================

async def calendar_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    if not update.effective_message:
        return

    keyboard = [
        [
            InlineKeyboardButton(
                text="➕ Add Event",
                callback_data="calendar_add"
            )
        ],
        [
            InlineKeyboardButton(
                text="📅 List Events",
                callback_data="calendar_list"
            )
        ]
    ]

    await update.effective_message.reply_text(
        text="📅 Calendar Menu",
        reply_markup=InlineKeyboardMarkup(
            keyboard
        )
    )


async def calendar_callback_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    query = update.callback_query

    if query is None:
        return

    await query.answer()

    telegram_user = update.effective_user

    if telegram_user is None:
        return

    user = (
        await user_repository
        .get_by_telegram_id(
            telegram_user.id
        )
    )

    if user is None:

        await query.edit_message_text(
            "❌ User not found"
        )

        return

    if query.data == "calendar_add":

        context.user_data[
            "calendar_create_mode"
        ] = True

        await query.edit_message_text(
            (
                "📝 Send event using format:\n\n"
                "title | YYYY-MM-DD HH:MM | description\n\n"
                f"Timezone: {TIMEZONE}"
            )
        )

        return

    if query.data == "calendar_list":

        reminders = (
            await reminder_service
            .list_user_reminders(
                user["id"]
            )
        )

        if not reminders:

            await query.edit_message_text(
                "📭 No events found"
            )

            return

        keyboard = []

        lines = []

        for reminder in reminders:

            remind_at = (
                reminder["remind_at"]
            )

            lines.append(
                (
                    f"📌 {reminder['title']}\n"
                    f"⏰ {remind_at}"
                )
            )

            keyboard.append([
                InlineKeyboardButton(
                    text=(
                        f"🗑 Delete "
                        f"{reminder['id']}"
                    ),
                    callback_data=(
                        f"delete_event_"
                        f"{reminder['id']}"
                    )
                )
            ])

        await query.edit_message_text(
            text="\n\n".join(lines),
            reply_markup=InlineKeyboardMarkup(
                keyboard
            )
        )


async def create_event_message_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    if not context.user_data.get(
        "calendar_create_mode"
    ):
        return

    if not update.effective_message:
        return

    if not update.effective_user:
        return

    text = (
        update.effective_message.text
    )

    if text is None:
        return

    user_message = text.strip()

    if not user_message:
        return

    telegram_user = (
        update.effective_user
    )

    user = (
        await user_repository
        .get_by_telegram_id(
            telegram_user.id
        )
    )

    if user is None:

        await update.effective_message.reply_text(
            "❌ User not found"
        )

        return

    parts = [
        part.strip()
        for part in user_message.split("|")
    ]

    if len(parts) < 2:

        await update.effective_message.reply_text(
            (
                "❌ Invalid format\n\n"
                "Example:\n"
                "Meeting | 2026-05-18 14:00 | Team sync"
            )
        )

        return

    title = parts[0]

    description = ""

    if len(parts) >= 3:
        description = parts[2]

    try:

        naive_datetime = datetime.strptime(
            parts[1],
            "%Y-%m-%d %H:%M"
        )

        localized_datetime = (
            timezone.localize(
                naive_datetime
            )
        )

    except ValueError:

        await update.effective_message.reply_text(
            (
                "❌ Invalid date/time format\n\n"
                "Use:\n"
                "YYYY-MM-DD HH:MM"
            )
        )

        return

    current_time = datetime.now(
        timezone
    )

    if localized_datetime <= current_time:

        await update.effective_message.reply_text(
            (
                "❌ Event time must be "
                "in the future"
            )
        )

        return

    reminder_id = (
        await reminder_service
        .create_reminder(
            user_id=user["id"],
            title=title,
            description=description,
            remind_at=localized_datetime
        )
    )

    context.user_data[
        "calendar_create_mode"
    ] = False

    formatted_time = (
        localized_datetime.strftime(
            "%Y-%m-%d %H:%M %Z"
        )
    )

    await update.effective_message.reply_text(
        (
            "✅ Event created successfully\n\n"
            f"🆔 ID: {reminder_id}\n"
            f"📌 Title: {title}\n"
            f"⏰ Time: {formatted_time}\n"
            f"📝 Description: "
            f"{description or 'None'}"
        )
    )


async def delete_event_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    query = update.callback_query

    if query is None:
        return

    await query.answer()

    reminder_id = int(
        query.data.split("_")[-1]
    )

    await reminder_service.delete_reminder(
        reminder_id
    )

    await query.edit_message_text(
        (
            "🗑 Event deleted successfully\n\n"
            f"Event ID: {reminder_id}"
        )
    )


# =========================================================
# HANDLER REGISTRATION
# =========================================================

def register_ai_chat_handlers(
    application: Application
) -> None:

    # AI COMMANDS

    application.add_handler(
        CommandHandler(
            "ai",
            ai_command
        )
    )

    application.add_handler(
        CommandHandler(
            "exitai",
            exit_ai_command
        )
    )

    application.add_handler(
        CommandHandler(
            "clear",
            clear_memory_command
        )
    )

    # CALENDAR COMMANDS

    application.add_handler(
        CommandHandler(
            "calendar",
            calendar_command
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            calendar_callback_handler,
            pattern=r"^calendar_"
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            delete_event_callback,
            pattern=r"^delete_event_"
        )
    )

    # =====================================================
    # PRIORITY GROUP 0
    # AI CHAT FIRST
    # =====================================================

    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            ai_chat_handler
        ),
        group=0
    )

    # =====================================================
    # PRIORITY GROUP 1
    # CALENDAR EVENT CREATION
    # =====================================================

    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            create_event_message_handler
        ),
        group=1
    )
