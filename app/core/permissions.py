from __future__ import annotations

from functools import wraps
from typing import Callable, Awaitable, Any

from telegram import Update
from telegram.ext import ContextTypes

from app.core.security import (
    AuthenticationException,
    AuthorizationException,
    get_telegram_user_id,
)
from app.database.repositories.roles import (
    RoleRepository,
)
from app.database.repositories.users import (
    UserRepository,
)

user_repository = UserRepository()
role_repository = RoleRepository()


async def get_user_role(
    telegram_user_id: int
) -> str | None:
    user = await user_repository.get_by_telegram_id(
        telegram_user_id
    )

    if user is None:
        return None

    return user["role_name"]


async def has_permission(
    telegram_user_id: int,
    permission: str
) -> bool:
    role_name = await get_user_role(
        telegram_user_id
    )

    if role_name is None:
        return False

    return await role_repository.has_permission(
        role_name,
        permission
    )


def permission_required(
    permission: str
):
    def decorator(
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
            telegram_user_id = (
                get_telegram_user_id(update)
            )

            if telegram_user_id is None:
                raise AuthenticationException(
                    "Unauthenticated user"
                )

            allowed = await has_permission(
                telegram_user_id,
                permission
            )

            if not allowed:
                raise AuthorizationException(
                    f"Missing permission: {permission}"
                )

            return await func(
                update,
                context,
                *args,
                **kwargs
            )

        return wrapper

    return decorator


def role_required(
    allowed_roles: list[str]
):
    def decorator(
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
            telegram_user_id = (
                get_telegram_user_id(update)
            )

            if telegram_user_id is None:
                raise AuthenticationException(
                    "Unauthenticated user"
                )

            role_name = await get_user_role(
                telegram_user_id
            )

            if role_name is None:
                raise AuthorizationException(
                    "User role not found"
                )

            if role_name not in allowed_roles:
                raise AuthorizationException(
                    "Role access denied"
                )

            return await func(
                update,
                context,
                *args,
                **kwargs
            )

        return wrapper

    return decorator
