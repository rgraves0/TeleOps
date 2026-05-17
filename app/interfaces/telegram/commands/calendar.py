from __future__ import annotations

from datetime import datetime

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from app.database.repositories.users import (
    UserRepository,
)
from app.services.reminder_service import (
    ReminderService,
)

user_repository = UserRepository()
reminder_service = ReminderService()


async def calendar_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_message:
        return

    keyboard = [
        [
            InlineKeyboardButton(
                "➕ Add Event",
                callback_data="calendar_add"
            )
        ],
        [
            InlineKeyboardButton(
                "📅 List Events",
                callback_data="calendar_list"
            )
        ]
    ]

    await update.effective_message.reply_text(
        "📆 Calendar Menu",
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
        return

    if query.data == "calendar_add":
        context.user_data[
            "calendar_create_mode"
        ] = True

        await query.edit_message_text(
            (
                "📝 Send event in format:\n\n"
                "title | YYYY-MM-DD HH:MM | description"
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
                "No events found."
            )

            return

        keyboard = []

        lines = []

        for reminder in reminders:
            lines.append(
                (
                    f"• {reminder['title']}\n"
                    f"⏰ {reminder['remind_at']}"
                )
            )

            keyboard.append([
                InlineKeyboardButton(
                    text=(
                        f"Delete "
                        f"{reminder['id']}"
                    ),
                    callback_data=(
                        f"delete_event_"
                        f"{reminder['id']}"
                    )
                )
            ])

        await query.edit_message_text(
            "\n\n".join(lines),
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
        return

    text = (
        update.effective_message.text
        .strip()
    )

    parts = [
        part.strip()
        for part in text.split("|")
    ]

    if len(parts) < 2:
        await update.effective_message.reply_text(
            "Invalid format."
        )

        return

    title = parts[0]

    event_time = datetime.strptime(
        parts[1],
        "%Y-%m-%d %H:%M"
    )

    description = ""

    if len(parts) >= 3:
        description = parts[2]

    reminder_id = (
        await reminder_service
        .create_reminder(
            user_id=user["id"],
            title=title,
            description=description,
            remind_at=event_time
        )
    )

    context.user_data[
        "calendar_create_mode"
    ] = False

    await update.effective_message.reply_text(
        (
            f"✅ Event created\n\n"
            f"ID: {reminder_id}\n"
            f"Title: {title}"
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
            f"🗑 Event deleted\n"
            f"ID: {reminder_id}"
        )
    )


def register_calendar_handlers(
    application
) -> None:
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
