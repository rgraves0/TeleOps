from __future__ import annotations

import asyncio
import logging
import mimetypes
import os
from pathlib import Path

import aiofiles

from src.storage.rclone_wrapper import (
    RcloneWrapper,
)

logger = logging.getLogger(__name__)


class FileOperations:

    def __init__(
        self,
        rclone: RcloneWrapper,
        preview_directory: str,
        chunk_size: int = (
            1024 * 1024
        ),
    ) -> None:

        self.rclone = rclone

        self.chunk_size = (
            chunk_size
        )

        self.preview_directory = (
            Path(preview_directory)
        )

        self.preview_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

    # =====================================================
    # UPLOAD
    # =====================================================

    async def upload_file(
        self,
        local_path: str,
        remote_path: str,
    ) -> bool:

        logger.info(
            "Uploading file=%s",
            local_path,
        )

        result = await (
            self.rclone.copy(
                local_path,
                remote_path,
            )
        )

        return result.success

    # =====================================================
    # DOWNLOAD
    # =====================================================

    async def download_file(
        self,
        remote_path: str,
        local_path: str,
    ) -> bool:

        logger.info(
            "Downloading file=%s",
            remote_path,
        )

        result = await (
            self.rclone.copy(
                remote_path,
                local_path,
            )
        )

        return result.success

    # =====================================================
    # STREAM FILE
    # =====================================================

    async def stream_file(
        self,
        file_path: str,
    ):

        async with aiofiles.open(
            file_path,
            "rb",
        ) as file:

            while True:

                chunk = await file.read(
                    self.chunk_size
                )

                if not chunk:
                    break

                yield chunk

    # =====================================================
    # GENERATE PREVIEW
    # =====================================================

    async def generate_preview(
        self,
        file_path: str,
        max_chars: int = 2000,
    ) -> str:

        path = Path(file_path)

        if not path.exists():

            raise FileNotFoundError(
                file_path
            )

        mime_type, _ = (
            mimetypes.guess_type(
                str(path)
            )
        )

        mime_type = (
            mime_type or ""
        )

        logger.info(
            "Generating preview "
            "mime=%s",
            mime_type,
        )

        # =================================================
        # TEXT FILE
        # =================================================

        if (
            mime_type.startswith(
                "text/"
            )
            or path.suffix
            in [
                ".log",
                ".md",
                ".json",
                ".py",
                ".txt",
            ]
        ):

            async with aiofiles.open(
                path,
                "r",
                errors="ignore",
            ) as file:

                content = (
                    await file.read(
                        max_chars
                    )
                )

            return content

        # =================================================
        # IMAGE FILE
        # =================================================

        if mime_type.startswith(
            "image/"
        ):

            return (
                f"[IMAGE PREVIEW]\n"
                f"Filename: {path.name}\n"
                f"Size: "
                f"{path.stat().st_size} bytes"
            )

        # =================================================
        # VIDEO FILE
        # =================================================

        if mime_type.startswith(
            "video/"
        ):

            return (
                f"[VIDEO PREVIEW]\n"
                f"Filename: {path.name}\n"
                f"Size: "
                f"{path.stat().st_size} bytes"
            )

        # =================================================
        # PDF FILE
        # =================================================

        if path.suffix.lower() == ".pdf":

            return (
                f"[PDF DOCUMENT]\n"
                f"Filename: {path.name}\n"
                f"Size: "
                f"{path.stat().st_size} bytes"
            )

        return (
            f"[BINARY FILE]\n"
            f"Filename: {path.name}\n"
            f"Size: "
            f"{path.stat().st_size} bytes"
        )

    # =====================================================
    # DELETE LOCAL FILE
    # =====================================================

    async def delete_local_file(
        self,
        file_path: str,
    ) -> None:

        path = Path(file_path)

        if not path.exists():
            return

        await asyncio.to_thread(
            os.remove,
            path,
        )

        logger.info(
            "Deleted local file=%s",
            file_path,
        )

    # =====================================================
    # CREATE TEMP FILE
    # =====================================================

    async def create_temp_file(
        self,
        filename: str,
        content: bytes,
    ) -> str:

        temp_path = (
            self.preview_directory
            / filename
        )

        async with aiofiles.open(
            temp_path,
            "wb",
        ) as file:

            await file.write(
                content
            )

        return str(temp_path)
