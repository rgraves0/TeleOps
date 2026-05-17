from __future__ import annotations

import os
from functools import wraps
from typing import Callable, Awaitable, Any

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ContextTypes

load_dotenv()

OWNER_IDS = {
    int(user_id.strip())
    for user_id in os.getenv(
        "TELEGRAM_ADMIN_IDS",
        ""
    ).split(",")
    if user_id.strip().isdigit()
}

DEFAULT_DENY = (
    os.getenv("DEFAULT_DENY", "true")
    .lower()
    .strip()
    == "true"
)


class SecurityException(Exception):
    pass


class AuthorizationException(SecurityException):
    pass


class AuthenticationException(SecurityException):
    pass


def get_telegram_user_id(
    update: Update
) -> int | None:
    if update.effective_user:
        return update.effective_user.id

    return None


def is_owner(
    telegram_user_id: int
) -> bool:
    return telegram_user_id in OWNER_IDS


def ensure_owner(
    telegram_user_id: int
) -> None:
    if not is_owner(telegram_user_id):
        raise AuthorizationException(
            "Access denied: owner only"
        )


def owner_required(
    func: Callable[
        [Update, ContextTypes.DEFAULT_TYPE],
        Awaitable[Any]
    ]
):
    @wraps(func)
    async def wrapper(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *args,
        **kwargs
    ):
        telegram_user_id = get_telegram_user_id(update)

        if telegram_user_id is None:
            raise AuthenticationException(
                "Telegram user not found"
            )

        if DEFAULT_DENY and not is_owner(
            telegram_user_id
        ):
            raise AuthorizationException(
                "Default deny policy active"
            )

        return await func(
            update,
            context,
            *args,
            **kwargs
        )

    return wrapper


def private_chat_only(
    func: Callable[
        [Update, ContextTypes.DEFAULT_TYPE],
        Awaitable[Any]
    ]
):
    @wraps(func)
    async def wrapper(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *args,
        **kwargs
    ):
        if not update.effective_chat:
            raise AuthorizationException(
                "Chat not found"
            )

        if update.effective_chat.type != "private":
            raise AuthorizationException(
                "This command can only be used in private chat"
            )

        return await func(
            update,
            context,
            *args,
            **kwargs
        )

    return wrapper
