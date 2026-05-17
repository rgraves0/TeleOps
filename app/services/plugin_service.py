from __future__ import annotations

import configparser
import json
import logging
from pathlib import Path
from typing import Any

from app.utils.encryption import (
    decrypt_text,
    encrypt_text,
)

logger = logging.getLogger(__name__)


class PluginServiceError(Exception):
    pass


class PluginService:
    def __init__(self):
        self.storage_dir = Path(
            "data/plugins"
        )

        self.storage_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        self.rclone_config_file = (
            self.storage_dir
            / "rclone.conf.enc"
        )

        self.mail_settings_file = (
            self.storage_dir
            / "mail_settings.enc"
        )

    async def save_rclone_config(
        self,
        raw_config: str
    ) -> bool:
        if not raw_config.strip():
            raise PluginServiceError(
                "RClone config is empty"
            )

        encrypted = encrypt_text(
            raw_config
        )

        self.rclone_config_file.write_text(
            encrypted,
            encoding="utf-8"
        )

        logger.info(
            "Encrypted RClone config saved"
        )

        return True

    async def load_rclone_config(
        self
    ) -> str:
        if not self.rclone_config_file.exists():
            raise PluginServiceError(
                "RClone config not found"
            )

        encrypted = (
            self.rclone_config_file
            .read_text(
                encoding="utf-8"
            )
        )

        return decrypt_text(
            encrypted
        )

    async def list_rclone_remotes(
        self
    ) -> list[str]:
        raw_config = (
            await self.load_rclone_config()
        )

        parser = configparser.ConfigParser()

        parser.read_string(
            raw_config
        )

        remotes = parser.sections()

        return remotes

    async def get_remote_config(
        self,
        remote_name: str
    ) -> dict[str, Any]:
        raw_config = (
            await self.load_rclone_config()
        )

        parser = configparser.ConfigParser()

        parser.read_string(
            raw_config
        )

        if remote_name not in parser:
            raise PluginServiceError(
                f"Remote not found: "
                f"{remote_name}"
            )

        return dict(
            parser[remote_name]
        )

    async def save_mail_settings(
        self,
        settings: dict
    ) -> bool:
        json_data = json.dumps(
            settings
        )

        encrypted = encrypt_text(
            json_data
        )

        self.mail_settings_file.write_text(
            encrypted,
            encoding="utf-8"
        )

        logger.info(
            "Encrypted mail settings saved"
        )

        return True

    async def load_mail_settings(
        self
    ) -> dict:
        if not self.mail_settings_file.exists():
            raise PluginServiceError(
                "Mail settings not found"
            )

        encrypted = (
            self.mail_settings_file
            .read_text(
                encoding="utf-8"
            )
        )

        decrypted = decrypt_text(
            encrypted
        )

        return json.loads(
            decrypted
        )

    async def delete_rclone_config(
        self
    ) -> bool:
        if self.rclone_config_file.exists():
            self.rclone_config_file.unlink()

            logger.info(
                "RClone config deleted"
            )

        return True

    async def delete_mail_settings(
        self
    ) -> bool:
        if self.mail_settings_file.exists():
            self.mail_settings_file.unlink()

            logger.info(
                "Mail settings deleted"
            )

        return True
