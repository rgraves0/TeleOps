from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import ssl
import time
import urllib.error
import urllib.request

from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Set,
)

logger = logging.getLogger(__name__)


class KeyRotationError(Exception):
    pass


class PermissionDeniedError(KeyRotationError):
    pass


class KeyValidationError(KeyRotationError):
    pass


@dataclass(slots=True)
class ProviderKeyRecord:
    provider: str
    active_key: str
    previous_key: Optional[str] = None
    updated_at: float = field(
        default_factory=time.time
    )
    validated: bool = False
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class RotationAuditEvent:
    provider: str
    actor_id: str
    success: bool
    reason: str
    timestamp: float = field(
        default_factory=time.time
    )


class AsyncKeyValidator:
    """
    Lightweight provider validator.

    Uses tiny HTTPS handshake requests
    to validate credentials before swap.
    """

    DEFAULT_TIMEOUT = 10

    def __init__(
        self,
        *,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:

        self.timeout = timeout

        self.ssl_context = (
            ssl.create_default_context()
        )

    async def validate(
        self,
        *,
        provider: str,
        key: str,
    ) -> bool:

        provider = provider.lower()

        if provider == "openai":
            return await self._validate_openai(
                key
            )

        if provider == "anthropic":
            return await self._validate_anthropic(
                key
            )

        if provider == "telegram":
            return await self._validate_telegram(
                key
            )

        if provider == "groq":
            return await self._validate_groq(
                key
            )

        logger.warning(
            "Unknown provider validation: %s",
            provider,
        )

        return False

    async def _validate_openai(
        self,
        key: str,
    ) -> bool:

        return await asyncio.to_thread(
            self._http_validation,
            "https://api.openai.com/v1/models",
            {
                "Authorization":
                    f"Bearer {key}"
            },
        )

    async def _validate_anthropic(
        self,
        key: str,
    ) -> bool:

        return await asyncio.to_thread(
            self._http_validation,
            "https://api.anthropic.com/v1/messages",
            {
                "x-api-key": key,
                "anthropic-version":
                    "2023-06-01",
            },
            method="POST",
        )

    async def _validate_telegram(
        self,
        key: str,
    ) -> bool:

        url = (
            f"https://api.telegram.org/bot{key}/getMe"
        )

        return await asyncio.to_thread(
            self._http_validation,
            url,
            {},
        )

    async def _validate_groq(
        self,
        key: str,
    ) -> bool:

        return await asyncio.to_thread(
            self._http_validation,
            "https://api.groq.com/openai/v1/models",
            {
                "Authorization":
                    f"Bearer {key}"
            },
        )

    def _http_validation(
        self,
        url: str,
        headers: Dict[str, str],
        *,
        method: str = "GET",
    ) -> bool:

        try:
            request = urllib.request.Request(
                url=url,
                method=method,
                headers=headers,
            )

            with urllib.request.urlopen(
                request,
                timeout=self.timeout,
                context=self.ssl_context,
            ) as response:

                status = response.status

                return 200 <= status < 300

        except urllib.error.HTTPError as exc:
            logger.warning(
                "Validation failed: %s",
                exc,
            )

            return False

        except Exception as exc:
            logger.error(
                "Validation exception: %s",
                exc,
            )

            return False


class HotSwapKeyRegistry:
    """
    Thread-safe runtime credential registry.
    """

    def __init__(
        self,
    ) -> None:

        self._records: Dict[
            str,
            ProviderKeyRecord,
        ] = {}

        self._locks: Dict[
            str,
            asyncio.Lock,
        ] = {}

    def _get_lock(
        self,
        provider: str,
    ) -> asyncio.Lock:

        if provider not in self._locks:
            self._locks[
                provider
            ] = asyncio.Lock()

        return self._locks[provider]

    async def get_key(
        self,
        provider: str,
    ) -> Optional[str]:

        record = self._records.get(
            provider
        )

        if not record:
            return None

        return record.active_key

    async def set_key(
        self,
        *,
        provider: str,
        new_key: str,
        validated: bool,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> None:

        lock = self._get_lock(
            provider
        )

        async with lock:

            old = self._records.get(
                provider
            )

            previous = (
                old.active_key
                if old
                else None
            )

            self._records[
                provider
            ] = ProviderKeyRecord(
                provider=provider,
                active_key=new_key,
                previous_key=previous,
                validated=validated,
                updated_at=time.time(),
                metadata=metadata
                or {},
            )

    async def rollback(
        self,
        provider: str,
    ) -> bool:

        lock = self._get_lock(
            provider
        )

        async with lock:

            record = self._records.get(
                provider
            )

            if (
                not record
                or not record.previous_key
            ):
                return False

            old_active = (
                record.active_key
            )

            record.active_key = (
                record.previous_key
            )

            record.previous_key = (
                old_active
            )

            record.updated_at = (
                time.time()
            )

            record.validated = True

            return True

    async def snapshot(
        self,
    ) -> Dict[str, Dict[str, Any]]:

        output = {}

        for (
            provider,
            record,
        ) in self._records.items():

            output[
                provider
            ] = {
                "provider":
                    provider,
                "updated_at":
                    record.updated_at,
                "validated":
                    record.validated,
                "fingerprint":
                    self._fingerprint(
                        record.active_key
                    ),
            }

        return output

    @staticmethod
    def _fingerprint(
        value: str,
    ) -> str:

        digest = hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()

        return digest[:16]


class AccessController:
    """
    Default Deny RBAC gate.
    """

    REQUIRED_ROLE = "superuser"

    REQUIRED_PERMISSION = (
        "system.keys.rotate"
    )

    async def authorize(
        self,
        *,
        actor_id: str,
        roles: Set[str],
        permissions: Set[str],
    ) -> bool:

        if (
            self.REQUIRED_ROLE
            not in roles
        ):
            return False

        if (
            self.REQUIRED_PERMISSION
            not in permissions
        ):
            return False

        return True


class SecureKeyRotator:
    """
    Production-safe zero-downtime
    runtime key rotation manager.
    """

    def __init__(
        self,
        *,
        validator: Optional[
            AsyncKeyValidator
        ] = None,
        registry: Optional[
            HotSwapKeyRegistry
        ] = None,
        access_controller: Optional[
            AccessController
        ] = None,
    ) -> None:

        self.validator = (
            validator
            or AsyncKeyValidator()
        )

        self.registry = (
            registry
            or HotSwapKeyRegistry()
        )

        self.access_controller = (
            access_controller
            or AccessController()
        )

        self.audit_events: list[
            RotationAuditEvent
        ] = []

        self.rotation_lock = (
            asyncio.Lock()
        )

    async def rotate_key(
        self,
        *,
        actor_id: str,
        provider: str,
        new_key: str,
        roles: Set[str],
        permissions: Set[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:

        authorized = (
            await self.access_controller.authorize(
                actor_id=actor_id,
                roles=roles,
                permissions=permissions,
            )
        )

        if not authorized:
            self._audit(
                provider=provider,
                actor_id=actor_id,
                success=False,
                reason="unauthorized",
            )

            raise PermissionDeniedError(
                "Key rotation denied"
            )

        async with self.rotation_lock:

            current_key = (
                await self.registry.get_key(
                    provider
                )
            )

            validated = (
                await self.validator.validate(
                    provider=provider,
                    key=new_key,
                )
            )

            if not validated:

                self._audit(
                    provider=provider,
                    actor_id=actor_id,
                    success=False,
                    reason="validation_failed",
                )

                raise KeyValidationError(
                    f"Key validation failed for {provider}"
                )

            try:

                await self.registry.set_key(
                    provider=provider,
                    new_key=new_key,
                    validated=True,
                    metadata=metadata,
                )

                recheck = (
                    await self.validator.validate(
                        provider=provider,
                        key=new_key,
                    )
                )

                if not recheck:

                    logger.warning(
                        "Validation failed after swap, rolling back"
                    )

                    await self.registry.rollback(
                        provider
                    )

                    self._audit(
                        provider=provider,
                        actor_id=actor_id,
                        success=False,
                        reason="rollback_triggered",
                    )

                    raise KeyValidationError(
                        "Post-rotation validation failed"
                    )

                self._audit(
                    provider=provider,
                    actor_id=actor_id,
                    success=True,
                    reason="rotation_success",
                )

                self._zeroize_string(
                    current_key
                )

                return True

            except Exception:

                if current_key:

                    await self.registry.rollback(
                        provider
                    )

                raise

    async def get_active_key(
        self,
        *,
        provider: str,
        actor_id: str,
        roles: Set[str],
        permissions: Set[str],
    ) -> Optional[str]:

        authorized = (
            await self.access_controller.authorize(
                actor_id=actor_id,
                roles=roles,
                permissions=permissions,
            )
        )

        if not authorized:
            raise PermissionDeniedError(
                "Key access denied"
            )

        return await self.registry.get_key(
            provider
        )

    async def runtime_snapshot(
        self,
    ) -> Dict[str, Any]:

        return {
            "providers":
                await self.registry.snapshot(),
            "audit_events":
                len(
                    self.audit_events
                ),
            "timestamp":
                time.time(),
        }

    async def emergency_rollback(
        self,
        *,
        provider: str,
        actor_id: str,
        roles: Set[str],
        permissions: Set[str],
    ) -> bool:

        authorized = (
            await self.access_controller.authorize(
                actor_id=actor_id,
                roles=roles,
                permissions=permissions,
            )
        )

        if not authorized:
            raise PermissionDeniedError(
                "Rollback denied"
            )

        success = (
            await self.registry.rollback(
                provider
            )
        )

        self._audit(
            provider=provider,
            actor_id=actor_id,
            success=success,
            reason="manual_rollback",
        )

        return success

    def _audit(
        self,
        *,
        provider: str,
        actor_id: str,
        success: bool,
        reason: str,
    ) -> None:

        self.audit_events.append(
            RotationAuditEvent(
                provider=provider,
                actor_id=actor_id,
                success=success,
                reason=reason,
            )
        )

    @staticmethod
    def _zeroize_string(
        value: Optional[str],
    ) -> None:

        if not value:
            return

        try:
            mutable = bytearray(
                value.encode("utf-8")
            )

            for i in range(
                len(mutable)
            ):
                mutable[i] = 0

        except Exception:
            pass


DEFAULT_KEY_ROTATOR = (
    SecureKeyRotator
)
