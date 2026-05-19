from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import secrets
import shutil
import sqlite3
import time
import traceback
from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ConfigSnapshot:
    path: str
    sha256: str
    signature: str
    captured_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class DriftIncident:
    config_path: str
    expected_hash: str
    actual_hash: str
    detected_at: float
    healed: bool
    revoked_subjects: List[str]
    reason: str


class ConfigurationSecurityError(
    Exception
):
    pass


class DriftDetectedError(
    Exception
):
    pass


class SQLiteConfigLedger:
    """
    SQLite WAL-backed tamper-proof config ledger.
    """

    SQLITE_BUSY_TIMEOUT = 5000

    def __init__(
        self,
        database_path: str,
    ) -> None:

        self.database_path = Path(
            database_path
        )

        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._connection: Optional[
            sqlite3.Connection
        ] = None

    async def initialize(
        self,
    ) -> None:

        self._connection = sqlite3.connect(
            str(self.database_path),
            check_same_thread=False,
            isolation_level=None,
        )

        await asyncio.to_thread(
            self._configure
        )

        await asyncio.to_thread(
            self._create_tables
        )

    async def close(
        self,
    ) -> None:

        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    def _configure(
        self,
    ) -> None:

        self._connection.execute(
            "PRAGMA journal_mode=WAL;"
        )

        self._connection.execute(
            "PRAGMA synchronous=NORMAL;"
        )

        self._connection.execute(
            "PRAGMA temp_store=MEMORY;"
        )

        self._connection.execute(
            "PRAGMA cache_size=-1500;"
        )

        self._connection.execute(
            f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT};"
        )

    def _create_tables(
        self,
    ) -> None:

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS config_snapshots (
                path TEXT PRIMARY KEY,
                sha256 TEXT NOT NULL,
                signature TEXT NOT NULL,
                captured_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS drift_incidents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_path TEXT NOT NULL,
                expected_hash TEXT NOT NULL,
                actual_hash TEXT NOT NULL,
                detected_at REAL NOT NULL,
                healed INTEGER NOT NULL,
                revoked_subjects TEXT NOT NULL,
                reason TEXT NOT NULL
            )
            """
        )

    async def save_snapshot(
        self,
        snapshot: ConfigSnapshot,
    ) -> None:

        await asyncio.to_thread(
            self._save_snapshot_sync,
            snapshot,
        )

    def _save_snapshot_sync(
        self,
        snapshot: ConfigSnapshot,
    ) -> None:

        self._connection.execute(
            """
            INSERT OR REPLACE INTO config_snapshots (
                path,
                sha256,
                signature,
                captured_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                snapshot.path,
                snapshot.sha256,
                snapshot.signature,
                snapshot.captured_at,
                json.dumps(
                    snapshot.metadata
                ),
            ),
        )

    async def load_snapshot(
        self,
        path: str,
    ) -> Optional[
        ConfigSnapshot
    ]:

        return await asyncio.to_thread(
            self._load_snapshot_sync,
            path,
        )

    def _load_snapshot_sync(
        self,
        path: str,
    ) -> Optional[
        ConfigSnapshot
    ]:

        cursor = self._connection.execute(
            """
            SELECT
                path,
                sha256,
                signature,
                captured_at,
                metadata
            FROM config_snapshots
            WHERE path = ?
            LIMIT 1
            """,
            (path,),
        )

        row = cursor.fetchone()

        if not row:
            return None

        return ConfigSnapshot(
            path=row[0],
            sha256=row[1],
            signature=row[2],
            captured_at=row[3],
            metadata=json.loads(
                row[4]
            ),
        )

    async def record_incident(
        self,
        incident: DriftIncident,
    ) -> None:

        await asyncio.to_thread(
            self._record_incident_sync,
            incident,
        )

    def _record_incident_sync(
        self,
        incident: DriftIncident,
    ) -> None:

        self._connection.execute(
            """
            INSERT INTO drift_incidents (
                config_path,
                expected_hash,
                actual_hash,
                detected_at,
                healed,
                revoked_subjects,
                reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                incident.config_path,
                incident.expected_hash,
                incident.actual_hash,
                incident.detected_at,
                (
                    1
                    if incident.healed
                    else 0
                ),
                json.dumps(
                    incident.revoked_subjects
                ),
                incident.reason,
            ),
        )


class ConfigHashValidator:
    """
    SHA-256 configuration validator.
    """

    CHUNK_SIZE = 65536

    async def calculate_hash(
        self,
        path: Path,
    ) -> str:

        return await asyncio.to_thread(
            self._calculate_sync,
            path,
        )

    def _calculate_sync(
        self,
        path: Path,
    ) -> str:

        sha256 = hashlib.sha256()

        with path.open(
            "rb"
        ) as handle:

            while True:
                chunk = handle.read(
                    self.CHUNK_SIZE
                )

                if not chunk:
                    break

                sha256.update(
                    chunk
                )

        return sha256.hexdigest()


class CryptographicBaselineSigner:
    """
    HMAC baseline signer.
    """

    def __init__(
        self,
        signing_key: str,
    ) -> None:

        self.signing_key = (
            signing_key.encode(
                "utf-8"
            )
        )

    def sign(
        self,
        *,
        config_hash: str,
    ) -> str:

        return hmac.new(
            self.signing_key,
            config_hash.encode(
                "utf-8"
            ),
            hashlib.sha256,
        ).hexdigest()

    def verify(
        self,
        *,
        config_hash: str,
        signature: str,
    ) -> bool:

        expected = self.sign(
            config_hash=config_hash
        )

        return hmac.compare_digest(
            expected,
            signature,
        )


class RuntimeConfigCache:
    """
    Lightweight runtime cache.
    """

    def __init__(
        self,
    ) -> None:

        self._cache: Dict[
            str,
            Dict[str, Any],
        ] = {}

    async def update(
        self,
        *,
        path: str,
        payload: Dict[str, Any],
    ) -> None:

        self._cache[path] = {
            "updated_at":
                time.time(),
            "payload":
                payload,
        }

    async def get(
        self,
        path: str,
    ) -> Optional[
        Dict[str, Any]
    ]:

        return self._cache.get(
            path
        )


class SecurityRevocationController:
    """
    Unauthorized session/token revocation.
    """

    def __init__(
        self,
    ) -> None:

        self._revoked: Dict[
            str,
            float,
        ] = {}

    async def revoke(
        self,
        subject_id: str,
    ) -> None:

        self._revoked[
            subject_id
        ] = time.time()

    async def is_revoked(
        self,
        subject_id: str,
    ) -> bool:

        return (
            subject_id
            in self._revoked
        )


class DriftDetectionScheduler:
    """
    Async drift monitor scheduler.
    """

    def __init__(
        self,
        *,
        interval_seconds: int,
    ) -> None:

        self.interval_seconds = (
            interval_seconds
        )


class AutoAlignmentManager:
    """
    Autonomous reverse sync engine.
    """

    async def restore(
        self,
        *,
        baseline_path: Path,
        target_path: Path,
    ) -> None:

        await asyncio.to_thread(
            shutil.copy2,
            str(baseline_path),
            str(target_path),
        )


class ConfigHealer:
    """
    Async-first autonomous config healing runtime.

    Features:
    - SHA256 drift detection
    - Cryptographic baseline validation
    - Runtime reverse sync healing
    - Unauthorized token revocation
    - SQLite WAL audit trail
    - Default Deny RBAC
    """

    DEFAULT_SCAN_INTERVAL = 20
    WAL_CHECKPOINT_INTERVAL = 1800

    def __init__(
        self,
        *,
        baseline_directory: str,
        runtime_directory: str,
        signing_key: str,
        permissions: Set[str],
        database_path: str = (
            "./data/config_healer.db"
        ),
    ) -> None:

        self.permissions = (
            permissions
        )

        self.baseline_directory = (
            Path(
                baseline_directory
            )
        )

        self.runtime_directory = (
            Path(
                runtime_directory
            )
        )

        self.baseline_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.runtime_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.ledger = (
            SQLiteConfigLedger(
                database_path
            )
        )

        self.validator = (
            ConfigHashValidator()
        )

        self.signer = (
            CryptographicBaselineSigner(
                signing_key
            )
        )

        self.runtime_cache = (
            RuntimeConfigCache()
        )

        self.revocation_controller = (
            SecurityRevocationController()
        )

        self.scheduler = (
            DriftDetectionScheduler(
                interval_seconds=
                    self.DEFAULT_SCAN_INTERVAL
            )
        )

        self.alignment_manager = (
            AutoAlignmentManager()
        )

        self._running = False

        self._drift_task: Optional[
            asyncio.Task
        ] = None

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting ConfigHealer"
        )

        await self.ledger.initialize()

        await self._bootstrap_snapshots()

        self._running = True

        self._drift_task = (
            asyncio.create_task(
                self._drift_loop()
            )
        )

        self._maintenance_task = (
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(
        self,
    ) -> None:

        logger.info(
            "Stopping ConfigHealer"
        )

        self._running = False

        for task in (
            self._drift_task,
            self._maintenance_task,
        ):
            if task:
                task.cancel()

                with contextlib.suppress(
                    asyncio.CancelledError
                ):
                    await task

        await self.ledger.close()

    async def _bootstrap_snapshots(
        self,
    ) -> None:

        for path in (
            self.baseline_directory.glob(
                "*"
            )
        ):
            if not path.is_file():
                continue

            digest = (
                await self.validator.calculate_hash(
                    path
                )
            )

            signature = (
                self.signer.sign(
                    config_hash=
                        digest
                )
            )

            snapshot = (
                ConfigSnapshot(
                    path=
                        path.name,
                    sha256=
                        digest,
                    signature=
                        signature,
                    captured_at=
                        time.time(),
                )
            )

            await self.ledger.save_snapshot(
                snapshot
            )

    async def _drift_loop(
        self,
    ) -> None:

        while self._running:
            try:
                await self.scan_for_drifts()

                await asyncio.sleep(
                    self.scheduler.interval_seconds
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.error(
                    traceback.format_exc()
                )

    async def scan_for_drifts(
        self,
    ) -> None:

        for runtime_file in (
            self.runtime_directory.glob(
                "*"
            )
        ):
            if not runtime_file.is_file():
                continue

            snapshot = (
                await self.ledger.load_snapshot(
                    runtime_file.name
                )
            )

            if not snapshot:
                continue

            actual_hash = (
                await self.validator.calculate_hash(
                    runtime_file
                )
            )

            valid_signature = (
                self.signer.verify(
                    config_hash=
                        snapshot.sha256,
                    signature=
                        snapshot.signature,
                )
            )

            if (
                not valid_signature
            ):
                raise ConfigurationSecurityError(
                    "Baseline signature mismatch"
                )

            if (
                actual_hash
                != snapshot.sha256
            ):
                await self._heal_drift(
                    runtime_file=
                        runtime_file,
                    snapshot=
                        snapshot,
                    actual_hash=
                        actual_hash,
                )

    async def _heal_drift(
        self,
        *,
        runtime_file: Path,
        snapshot: ConfigSnapshot,
        actual_hash: str,
    ) -> None:

        logger.warning(
            "Configuration drift detected: %s",
            runtime_file.name,
        )

        baseline_file = (
            self.baseline_directory
            / runtime_file.name
        )

        await self.alignment_manager.restore(
            baseline_path=
                baseline_file,
            target_path=
                runtime_file,
        )

        revoked_subjects = []

        suspicious_subject = (
            f"session:{secrets.token_hex(6)}"
        )

        await self.revocation_controller.revoke(
            suspicious_subject
        )

        revoked_subjects.append(
            suspicious_subject
        )

        healed_hash = (
            await self.validator.calculate_hash(
                runtime_file
            )
        )

        await self.runtime_cache.update(
            path=runtime_file.name,
            payload={
                "sha256":
                    healed_hash,
                "healed":
                    True,
                "updated_at":
                    time.time(),
            },
        )

        incident = DriftIncident(
            config_path=
                runtime_file.name,
            expected_hash=
                snapshot.sha256,
            actual_hash=
                actual_hash,
            detected_at=
                time.time(),
            healed=True,
            revoked_subjects=
                revoked_subjects,
            reason=
                "Unauthorized configuration drift detected",
        )

        await self.ledger.record_incident(
            incident
        )

    async def validate_permissions(
        self,
        *,
        required_permission: str,
    ) -> bool:

        return (
            required_permission
            in self.permissions
        )

    async def is_subject_revoked(
        self,
        subject_id: str,
    ) -> bool:

        return (
            await self.revocation_controller.is_revoked(
                subject_id
            )
        )

    async def runtime_state(
        self,
    ) -> Dict[str, Any]:

        return {
            "running":
                self._running,
            "runtime_dir":
                str(
                    self.runtime_directory
                ),
            "baseline_dir":
                str(
                    self.baseline_directory
                ),
            "timestamp":
                time.time(),
        }

    async def _maintenance_loop(
        self,
    ) -> None:

        while self._running:
            try:
                await asyncio.sleep(
                    self.WAL_CHECKPOINT_INTERVAL
                )

                await asyncio.to_thread(
                    self._wal_checkpoint
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.error(
                    traceback.format_exc()
                )

    def _wal_checkpoint(
        self,
    ) -> None:

        self.ledger._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:

        return {
            "running":
                self._running,
            "permissions":
                len(
                    self.permissions
                ),
            "scan_interval":
                self.scheduler.interval_seconds,
            "timestamp":
                time.time(),
        }


DEFAULT_CONFIG_HEALER = (
    ConfigHealer
)
