from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import mimetypes
import os
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Set,
)

from app.tools.dynamic_router import (
    DynamicToolRouter,
    RouteContext,
    RouteDecision,
)


logger = logging.getLogger(__name__)


class OCRState(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CIRCUIT_OPEN = "circuit_open"


@dataclass(slots=True)
class OCRRequest:
    requester_id: str
    requester_roles: Set[str]
    requester_permissions: Set[str]
    file_path: str
    provider: str
    model: str
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class OCRChunk:
    chunk_index: int
    content: str
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class OCRResult:
    document_id: str
    extracted_text: str
    provider: str
    model: str
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class RetryableOCRError(
    Exception
):
    pass


class CircuitBreakerOpen(
    Exception
):
    pass


class RBACValidator:
    """
    Default Deny + RBAC enforcement.
    """

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate(
        self,
        request: OCRRequest,
    ) -> bool:
        context = RouteContext(
            requester_id=(
                request.requester_id
            ),
            requester_roles=(
                request.requester_roles
            ),
            requester_permissions=(
                request.requester_permissions
            ),
            task_type="knowledge.ocr",
            metadata={
                "file_path":
                    request.file_path,
            },
        )

        route = await self.router.route(
            task="knowledge.ocr",
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class Base64StreamEncoder:
    """
    Memory-safe base64 stream encoder.
    """

    DEFAULT_CHUNK_SIZE = (
        256 * 1024
    )

    async def encode_stream(
        self,
        *,
        file_path: str,
        chunk_size: int = (
            DEFAULT_CHUNK_SIZE
        ),
    ) -> AsyncGenerator[str, None]:
        loop = asyncio.get_running_loop()

        with open(
            file_path,
            "rb",
        ) as file_handle:

            while True:
                chunk = (
                    await loop.run_in_executor(
                        None,
                        file_handle.read,
                        chunk_size,
                    )
                )

                if not chunk:
                    break

                encoded = (
                    base64.b64encode(
                        chunk
                    ).decode("utf-8")
                )

                yield encoded


class BackoffRetryHandler:
    """
    Async exponential backoff retry handler.
    """

    MAX_RETRIES = 5

    async def execute(
        self,
        *,
        operation: Callable[
            ...,
            Awaitable[Any],
        ],
        base_delay: float = 1.0,
    ) -> Any:
        last_error = None

        for attempt in range(
            self.MAX_RETRIES
        ):
            try:
                return await operation()

            except (
                RetryableOCRError,
                asyncio.TimeoutError,
            ) as exc:
                last_error = exc

                delay = (
                    base_delay
                    * (2**attempt)
                )

                delay += random.uniform(
                    0.0,
                    0.5,
                )

                logger.warning(
                    "OCR retry scheduled | attempt=%s delay=%.2f",
                    attempt + 1,
                    delay,
                )

                await asyncio.sleep(
                    delay
                )

        raise last_error


class CircuitBreaker:
    """
    Lightweight circuit breaker.
    """

    FAILURE_THRESHOLD = 5
    RECOVERY_TIMEOUT = 60

    def __init__(self) -> None:
        self.failure_count = 0

        self.last_failure_time = 0.0

        self.opened = False

    def check(self) -> None:
        if not self.opened:
            return

        elapsed = (
            time.time()
            - self.last_failure_time
        )

        if (
            elapsed
            >= self.RECOVERY_TIMEOUT
        ):
            self.reset()

            return

        raise CircuitBreakerOpen(
            "OCR circuit breaker open"
        )

    def record_success(
        self,
    ) -> None:
        self.reset()

    def record_failure(
        self,
    ) -> None:
        self.failure_count += 1

        self.last_failure_time = (
            time.time()
        )

        if (
            self.failure_count
            >= self.FAILURE_THRESHOLD
        ):
            self.opened = True

    def reset(self) -> None:
        self.failure_count = 0

        self.last_failure_time = 0.0

        self.opened = False


class MultimodalOCRBridge:
    """
    Async multimodal OCR provider bridge.
    """

    MAX_CONCURRENT_REQUESTS = 2

    def __init__(
        self,
        *,
        provider_manager: Any,
    ) -> None:
        self.provider_manager = (
            provider_manager
        )

        self._retry_handler = (
            BackoffRetryHandler()
        )

        self._circuit_breaker = (
            CircuitBreaker()
        )

        self._semaphore = (
            asyncio.Semaphore(
                self.MAX_CONCURRENT_REQUESTS
            )
        )

    async def extract_text(
        self,
        *,
        provider: str,
        model: str,
        mime_type: str,
        encoded_payload: str,
    ) -> str:
        async with self._semaphore:
            self._circuit_breaker.check()

            try:
                result = (
                    await self._retry_handler.execute(
                        operation=lambda:
                        self._call_provider(
                            provider=
                                provider,
                            model=model,
                            mime_type=
                                mime_type,
                            encoded_payload=
                                encoded_payload,
                        )
                    )
                )

                self._circuit_breaker.record_success()

                return result

            except Exception:
                self._circuit_breaker.record_failure()

                raise

    async def _call_provider(
        self,
        *,
        provider: str,
        model: str,
        mime_type: str,
        encoded_payload: str,
    ) -> str:
        """
        Provider manager bridge.

        Expected provider_manager API:
        await provider_manager.process_multimodal(...)
        """

        try:
            response = (
                await self.provider_manager.process_multimodal(
                    provider=provider,
                    model=model,
                    mime_type=mime_type,
                    base64_data=encoded_payload,
                    prompt=(
                        "Extract all readable text "
                        "from this image or scanned document."
                    ),
                )
            )

        except Exception as exc:
            message = str(exc).lower()

            if (
                "timeout"
                in message
                or "connection"
                in message
            ):
                raise RetryableOCRError(
                    str(exc)
                ) from exc

            if (
                "429"
                in message
                or "rate"
                in message
            ):
                raise RetryableOCRError(
                    str(exc)
                ) from exc

            raise

        return self._extract_text(
            response
        )

    def _extract_text(
        self,
        response: Any,
    ) -> str:
        """
        Provider-normalized OCR extraction.
        """

        if isinstance(
            response,
            str,
        ):
            return response.strip()

        if isinstance(
            response,
            dict,
        ):
            if "text" in response:
                return str(
                    response["text"]
                ).strip()

            if "content" in response:
                return str(
                    response["content"]
                ).strip()

            if "output_text" in response:
                return str(
                    response[
                        "output_text"
                    ]
                ).strip()

        raise ValueError(
            "Unsupported OCR response"
        )


class OCRStreamProcessor:
    """
    Stream-safe OCR buffering processor.
    """

    MAX_FILE_SIZE_MB = 15

    def __init__(
        self,
    ) -> None:
        self._encoder = (
            Base64StreamEncoder()
        )

    async def stream_payload(
        self,
        *,
        file_path: str,
    ) -> AsyncGenerator[str, None]:
        self._validate_size(
            file_path
        )

        async for encoded in (
            self._encoder.encode_stream(
                file_path=file_path
            )
        ):
            yield encoded

    def _validate_size(
        self,
        file_path: str,
    ) -> None:
        size = os.path.getsize(
            file_path
        )

        limit = (
            self.MAX_FILE_SIZE_MB
            * 1024
            * 1024
        )

        if size > limit:
            raise ValueError(
                "OCR file exceeds size limit"
            )


class OCRPipeline:
    """
    Async-first lightweight OCR pipeline.

    Features:
    - Multimodal OCR bridge
    - Base64 stream encoding
    - Circuit breaker protection
    - Backoff retry logic
    - RBAC enforcement
    - Failure isolation
    - Low-memory streaming
    """

    CLEANUP_INTERVAL = 3600

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        provider_manager: Any,
    ) -> None:
        self.router = router

        self._rbac = (
            RBACValidator(
                router
            )
        )

        self._stream_processor = (
            OCRStreamProcessor()
        )

        self._ocr_bridge = (
            MultimodalOCRBridge(
                provider_manager=
                    provider_manager
            )
        )

        self._running = False

        self._tasks: List[
            asyncio.Task
        ] = []

        self._active_jobs: Dict[
            str,
            OCRState,
        ] = {}

    async def start(self) -> None:
        self._running = True

        self._tasks.append(
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(self) -> None:
        self._running = False

        for task in self._tasks:
            task.cancel()

        for task in self._tasks:
            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await task

        self._tasks.clear()

    async def process(
        self,
        request: OCRRequest,
    ) -> AsyncGenerator[
        OCRChunk,
        None,
    ]:
        """
        Stream-safe OCR extraction pipeline.
        """

        allowed = (
            await self._rbac.validate(
                request
            )
        )

        if not allowed:
            raise PermissionError(
                "RBAC denied OCR access"
            )

        resolved_path = (
            Path(request.file_path)
            .expanduser()
            .resolve()
        )

        if not resolved_path.exists():
            raise FileNotFoundError(
                str(resolved_path)
            )

        document_id = (
            self._generate_document_id(
                str(resolved_path)
            )
        )

        self._active_jobs[
            document_id
        ] = OCRState.PROCESSING

        mime_type = (
            mimetypes.guess_type(
                str(resolved_path)
            )[0]
            or "image/png"
        )

        try:
            chunk_index = 0

            async for payload in (
                self._stream_processor.stream_payload(
                    file_path=
                        str(resolved_path)
                )
            ):
                text = (
                    await self._ocr_bridge.extract_text(
                        provider=
                            request.provider,
                        model=
                            request.model,
                        mime_type=
                            mime_type,
                        encoded_payload=
                            payload,
                    )
                )

                normalized = (
                    self._normalize_text(
                        text
                    )
                )

                if not normalized:
                    continue

                yield OCRChunk(
                    chunk_index=
                        chunk_index,
                    content=
                        normalized,
                    created_at=
                        time.time(),
                    metadata={
                        "document_id":
                            document_id,
                        "provider":
                            request.provider,
                        "model":
                            request.model,
                    },
                )

                chunk_index += 1

            self._active_jobs[
                document_id
            ] = OCRState.COMPLETED

        except Exception:
            self._active_jobs[
                document_id
            ] = OCRState.FAILED

            logger.exception(
                "OCR processing failed"
            )

            raise

    async def process_document(
        self,
        request: OCRRequest,
    ) -> OCRResult:
        """
        Aggregate OCR output.
        """

        collected: List[str] = []

        document_id = (
            self._generate_document_id(
                request.file_path
            )
        )

        async for chunk in self.process(
            request
        ):
            collected.append(
                chunk.content
            )

        final_text = "\n".join(
            collected
        )

        return OCRResult(
            document_id=document_id,
            extracted_text=final_text,
            provider=request.provider,
            model=request.model,
            created_at=time.time(),
            metadata=request.metadata,
        )

    async def _maintenance_loop(
        self,
    ) -> None:
        while self._running:
            try:
                await asyncio.sleep(
                    self.CLEANUP_INTERVAL
                )

                self._cleanup_finished_jobs()

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "OCR maintenance failure"
                )

    def _cleanup_finished_jobs(
        self,
    ) -> None:
        removable = []

        for job_id, state in (
            self._active_jobs.items()
        ):
            if state in {
                OCRState.COMPLETED,
                OCRState.FAILED,
            }:
                removable.append(
                    job_id
                )

        for job_id in removable:
            self._active_jobs.pop(
                job_id,
                None,
            )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "running":
                self._running,
            "active_jobs":
                len(
                    self._active_jobs
                ),
            "circuit_open":
                self._ocr_bridge._circuit_breaker.opened,
            "timestamp":
                time.time(),
        }

    def _normalize_text(
        self,
        text: str,
    ) -> str:
        text = (
            text.replace("\x00", "")
            .replace("\r", "")
            .strip()
        )

        while "  " in text:
            text = text.replace(
                "  ",
                " ",
            )

        if len(text) > 12000:
            text = (
                text[:12000]
                + "...[truncated]"
            )

        return text

    def _generate_document_id(
        self,
        file_path: str,
    ) -> str:
        stat = os.stat(file_path)

        raw = (
            f"{file_path}:"
            f"{stat.st_size}:"
            f"{stat.st_mtime}"
        )

        return str(
            abs(hash(raw))
        )
