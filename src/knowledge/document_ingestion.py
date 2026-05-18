from __future__ import annotations

import asyncio
import contextlib
import io
import json
import logging
import mimetypes
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
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

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


logger = logging.getLogger(__name__)


class DocumentType(str, Enum):
    PDF = "pdf"
    TEXT = "text"
    JSON = "json"
    MARKDOWN = "markdown"
    UNKNOWN = "unknown"


class IngestionDecision(str, Enum):
    ALLOWED = "allowed"
    DENIED = "denied"
    INVALID = "invalid"
    TOO_LARGE = "too_large"
    UNSUPPORTED = "unsupported"


@dataclass(slots=True)
class FileAccessRequest:
    requester_id: str
    requester_roles: Set[str]
    requester_permissions: Set[str]
    file_path: str
    task_type: str = "document.ingestion"
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class TextChunk:
    document_id: str
    chunk_index: int
    text: str
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class FileMetadata:
    path: str
    size_bytes: int
    mime_type: str
    extension: str
    document_type: DocumentType
    created_at: float


class FileSizeValidator:
    """
    Lightweight file size validator.
    """

    DEFAULT_MAX_MB = 20

    def __init__(
        self,
        *,
        max_size_mb: int = DEFAULT_MAX_MB,
    ) -> None:
        self.max_size_bytes = (
            max_size_mb * 1024 * 1024
        )

    def validate(
        self,
        file_path: str,
    ) -> bool:
        try:
            size = os.path.getsize(
                file_path
            )

            return (
                size <= self.max_size_bytes
            )

        except Exception:
            return False


class FileTypeIdentifier:
    """
    Lightweight document type detector.
    """

    EXTENSION_MAP = {
        ".pdf": DocumentType.PDF,
        ".txt": DocumentType.TEXT,
        ".json": DocumentType.JSON,
        ".md": DocumentType.MARKDOWN,
        ".markdown": (
            DocumentType.MARKDOWN
        ),
    }

    def identify(
        self,
        file_path: str,
    ) -> DocumentType:
        extension = (
            Path(file_path)
            .suffix
            .lower()
        )

        return self.EXTENSION_MAP.get(
            extension,
            DocumentType.UNKNOWN,
        )


class PermissionEnforcer:
    """
    Default Deny + RBAC validation.
    """

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate(
        self,
        request: FileAccessRequest,
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
            task_type=request.task_type,
            metadata=request.metadata,
        )

        route = await self.router.route(
            task=request.task_type,
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class AsyncTextExtractor:
    """
    Async chunk-based text extractor.
    """

    TEXT_CHUNK_SIZE = 4096

    async def extract(
        self,
        *,
        file_path: str,
        document_type: DocumentType,
    ) -> AsyncGenerator[str, None]:
        if document_type in {
            DocumentType.TEXT,
            DocumentType.MARKDOWN,
        }:
            async for chunk in (
                self._stream_text_file(
                    file_path
                )
            ):
                yield chunk

            return

        if document_type == (
            DocumentType.JSON
        ):
            async for chunk in (
                self._stream_json_file(
                    file_path
                )
            ):
                yield chunk

            return

        if document_type == (
            DocumentType.PDF
        ):
            async for chunk in (
                self._stream_pdf_file(
                    file_path
                )
            ):
                yield chunk

            return

        raise ValueError(
            f"Unsupported document type: {document_type}"
        )

    async def _stream_text_file(
        self,
        file_path: str,
    ) -> AsyncGenerator[str, None]:
        loop = asyncio.get_running_loop()

        with open(
            file_path,
            "r",
            encoding="utf-8",
            errors="ignore",
        ) as handle:

            while True:
                chunk = (
                    await loop.run_in_executor(
                        None,
                        handle.read,
                        self.TEXT_CHUNK_SIZE,
                    )
                )

                if not chunk:
                    break

                yield chunk

    async def _stream_json_file(
        self,
        file_path: str,
    ) -> AsyncGenerator[str, None]:
        loop = asyncio.get_running_loop()

        content = (
            await loop.run_in_executor(
                None,
                self._read_json,
                file_path,
            )
        )

        serialized = json.dumps(
            content,
            ensure_ascii=False,
            separators=(",", ":"),
        )

        for idx in range(
            0,
            len(serialized),
            self.TEXT_CHUNK_SIZE,
        ):
            yield serialized[
                idx:
                idx
                + self.TEXT_CHUNK_SIZE
            ]

    def _read_json(
        self,
        file_path: str,
    ) -> Any:
        with open(
            file_path,
            "r",
            encoding="utf-8",
            errors="ignore",
        ) as handle:
            return json.load(handle)

    async def _stream_pdf_file(
        self,
        file_path: str,
    ) -> AsyncGenerator[str, None]:
        if PdfReader is None:
            raise RuntimeError(
                "pypdf library not installed"
            )

        loop = asyncio.get_running_loop()

        reader = (
            await loop.run_in_executor(
                None,
                PdfReader,
                file_path,
            )
        )

        for page_index, page in enumerate(
            reader.pages
        ):
            text = (
                await loop.run_in_executor(
                    None,
                    page.extract_text,
                )
            )

            if not text:
                continue

            for idx in range(
                0,
                len(text),
                self.TEXT_CHUNK_SIZE,
            ):
                yield text[
                    idx:
                    idx
                    + self.TEXT_CHUNK_SIZE
                ]


class DocumentIngestionPipeline:
    """
    Async-first Document Ingestion Pipeline.

    Features:
    - Stream-based extraction
    - Chunk-by-chunk processing
    - PDF/TXT/JSON/Markdown support
    - Default deny security
    - RBAC enforcement
    - File size validation
    - Minimal RAM footprint
    - Async-safe ingestion
    """

    MAX_ACTIVE_STREAMS = 8

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        max_file_size_mb: int = 20,
    ) -> None:
        self.router = router

        self._permission_enforcer = (
            PermissionEnforcer(router)
        )

        self._file_validator = (
            FileSizeValidator(
                max_size_mb=
                    max_file_size_mb
            )
        )

        self._type_identifier = (
            FileTypeIdentifier()
        )

        self._extractor = (
            AsyncTextExtractor()
        )

        self._active_streams: Dict[
            str,
            float,
        ] = {}

        self._lock = asyncio.Lock()

    async def ingest(
        self,
        *,
        request: FileAccessRequest,
    ) -> AsyncGenerator[
        TextChunk,
        None,
    ]:
        """
        Main async ingestion pipeline.
        """

        start = time.time()

        allowed = (
            await self._permission_enforcer.validate(
                request
            )
        )

        if not allowed:
            raise PermissionError(
                "RBAC denied file access"
            )

        file_path = (
            Path(request.file_path)
            .expanduser()
            .resolve()
        )

        if not file_path.exists():
            raise FileNotFoundError(
                str(file_path)
            )

        valid_size = (
            self._file_validator.validate(
                str(file_path)
            )
        )

        if not valid_size:
            raise ValueError(
                "File exceeds size limit"
            )

        document_type = (
            self._type_identifier.identify(
                str(file_path)
            )
        )

        if document_type == (
            DocumentType.UNKNOWN
        ):
            raise ValueError(
                "Unsupported file type"
            )

        document_id = (
            self._generate_document_id(
                str(file_path)
            )
        )

        async with self._lock:
            self._active_streams[
                document_id
            ] = start

        try:
            chunk_index = 0

            async for chunk in (
                self._extractor.extract(
                    file_path=str(file_path),
                    document_type=
                        document_type,
                )
            ):
                cleaned = (
                    self._normalize_chunk(
                        chunk
                    )
                )

                if not cleaned:
                    continue

                yield TextChunk(
                    document_id=
                        document_id,
                    chunk_index=
                        chunk_index,
                    text=cleaned,
                    created_at=
                        time.time(),
                    metadata={
                        "document_type":
                            document_type.value,
                        "source":
                            str(file_path),
                    },
                )

                chunk_index += 1

        finally:
            async with self._lock:
                self._active_streams.pop(
                    document_id,
                    None,
                )

    async def inspect(
        self,
        *,
        file_path: str,
    ) -> FileMetadata:
        resolved = (
            Path(file_path)
            .expanduser()
            .resolve()
        )

        if not resolved.exists():
            raise FileNotFoundError(
                str(resolved)
            )

        stat = resolved.stat()

        mime_type = (
            mimetypes.guess_type(
                str(resolved)
            )[0]
            or "application/octet-stream"
        )

        document_type = (
            self._type_identifier.identify(
                str(resolved)
            )
        )

        return FileMetadata(
            path=str(resolved),
            size_bytes=stat.st_size,
            mime_type=mime_type,
            extension=resolved.suffix,
            document_type=document_type,
            created_at=time.time(),
        )

    async def batch_ingest(
        self,
        *,
        requests: List[
            FileAccessRequest
        ],
    ) -> AsyncGenerator[
        TextChunk,
        None,
    ]:
        """
        Sequential low-memory batch ingestion.
        """

        for request in requests:
            async for chunk in self.ingest(
                request=request
            ):
                yield chunk

    def active_streams(
        self,
    ) -> int:
        return len(
            self._active_streams
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "active_streams":
                len(
                    self._active_streams
                ),
            "max_streams":
                self.MAX_ACTIVE_STREAMS,
            "timestamp":
                time.time(),
        }

    def _normalize_chunk(
        self,
        text: str,
    ) -> str:
        text = (
            text.replace("\x00", "")
            .replace("\r", "")
            .strip()
        )

        if len(text) > 8192:
            text = (
                text[:8192]
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
