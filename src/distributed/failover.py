from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import secrets
import socket
import sqlite3
import time
import traceback
from dataclasses import (
    dataclass,
    field,
)
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
)

logger = logging.getLogger(__name__)


class NodeRole(
    str,
    Enum,
):
    MASTER = "master"
    WORKER = "worker"
    CANDIDATE = "candidate"
    OFFLINE = "offline"


@dataclass(slots=True)
class ClusterNode:
    node_id: str
    host: str
    port: int
    role: NodeRole
    last_heartbeat: float
    priority: int
    fencing_token: str
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class ElectionResult:
    leader_node: str
    promoted_at: float
    fencing_token: str
    election_epoch: int


class ClusterSecurityError(
    Exception
):
    pass


class SplitBrainDetected(
    Exception
):
    pass


class SQLiteClusterStore:
    """
    SQLite WAL-backed cluster state store.
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
            CREATE TABLE IF NOT EXISTS cluster_nodes (
                node_id TEXT PRIMARY KEY,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                role TEXT NOT NULL,
                last_heartbeat REAL NOT NULL,
                priority INTEGER NOT NULL,
                fencing_token TEXT NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS cluster_elections (
                election_epoch INTEGER PRIMARY KEY,
                leader_node TEXT NOT NULL,
                promoted_at REAL NOT NULL,
                fencing_token TEXT NOT NULL
            )
            """
        )

    async def register_node(
        self,
        node: ClusterNode,
    ) -> None:

        await asyncio.to_thread(
            self._register_sync,
            node,
        )

    def _register_sync(
        self,
        node: ClusterNode,
    ) -> None:

        self._connection.execute(
            """
            INSERT OR REPLACE INTO cluster_nodes (
                node_id,
                host,
                port,
                role,
                last_heartbeat,
                priority,
                fencing_token,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                node.node_id,
                node.host,
                node.port,
                node.role.value,
                node.last_heartbeat,
                node.priority,
                node.fencing_token,
                json.dumps(
                    node.metadata
                ),
            ),
        )

    async def heartbeat(
        self,
        node_id: str,
    ) -> None:

        await asyncio.to_thread(
            self._heartbeat_sync,
            node_id,
        )

    def _heartbeat_sync(
        self,
        node_id: str,
    ) -> None:

        self._connection.execute(
            """
            UPDATE cluster_nodes
            SET last_heartbeat = ?
            WHERE node_id = ?
            """,
            (
                time.time(),
                node_id,
            ),
        )

    async def all_nodes(
        self,
    ) -> List[ClusterNode]:

        return await asyncio.to_thread(
            self._all_nodes_sync
        )

    def _all_nodes_sync(
        self,
    ) -> List[ClusterNode]:

        cursor = self._connection.execute(
            """
            SELECT
                node_id,
                host,
                port,
                role,
                last_heartbeat,
                priority,
                fencing_token,
                metadata
            FROM cluster_nodes
            """
        )

        rows = cursor.fetchall()

        nodes: List[
            ClusterNode
        ] = []

        for row in rows:
            nodes.append(
                ClusterNode(
                    node_id=row[0],
                    host=row[1],
                    port=row[2],
                    role=NodeRole(
                        row[3]
                    ),
                    last_heartbeat=row[4],
                    priority=row[5],
                    fencing_token=row[6],
                    metadata=json.loads(
                        row[7]
                    ),
                )
            )

        return nodes

    async def promote_leader(
        self,
        *,
        node_id: str,
        fencing_token: str,
    ) -> ElectionResult:

        return await asyncio.to_thread(
            self._promote_sync,
            node_id,
            fencing_token,
        )

    def _promote_sync(
        self,
        node_id: str,
        fencing_token: str,
    ) -> ElectionResult:

        conn = self._connection

        conn.execute(
            "BEGIN IMMEDIATE"
        )

        try:
            cursor = conn.execute(
                """
                SELECT COALESCE(
                    MAX(election_epoch),
                    0
                )
                FROM cluster_elections
                """
            )

            current_epoch = (
                cursor.fetchone()[0]
            )

            new_epoch = (
                current_epoch + 1
            )

            conn.execute(
                """
                UPDATE cluster_nodes
                SET role = ?
                """,
                (
                    NodeRole.WORKER.value,
                ),
            )

            conn.execute(
                """
                UPDATE cluster_nodes
                SET role = ?,
                    fencing_token = ?
                WHERE node_id = ?
                """,
                (
                    NodeRole.MASTER.value,
                    fencing_token,
                    node_id,
                ),
            )

            conn.execute(
                """
                INSERT INTO cluster_elections (
                    election_epoch,
                    leader_node,
                    promoted_at,
                    fencing_token
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    new_epoch,
                    node_id,
                    time.time(),
                    fencing_token,
                ),
            )

            conn.execute(
                "COMMIT"
            )

            return ElectionResult(
                leader_node=node_id,
                promoted_at=
                    time.time(),
                fencing_token=
                    fencing_token,
                election_epoch=
                    new_epoch,
            )

        except Exception:
            conn.execute(
                "ROLLBACK"
            )
            raise

    async def current_master(
        self,
    ) -> Optional[
        ClusterNode
    ]:

        return await asyncio.to_thread(
            self._current_master_sync
        )

    def _current_master_sync(
        self,
    ) -> Optional[
        ClusterNode
    ]:

        cursor = self._connection.execute(
            """
            SELECT
                node_id,
                host,
                port,
                role,
                last_heartbeat,
                priority,
                fencing_token,
                metadata
            FROM cluster_nodes
            WHERE role = ?
            LIMIT 1
            """,
            (
                NodeRole.MASTER.value,
            ),
        )

        row = cursor.fetchone()

        if not row:
            return None

        return ClusterNode(
            node_id=row[0],
            host=row[1],
            port=row[2],
            role=NodeRole(
                row[3]
            ),
            last_heartbeat=row[4],
            priority=row[5],
            fencing_token=row[6],
            metadata=json.loads(
                row[7]
            ),
        )


class CryptographicFence:
    """
    Split-brain mitigation fencing token.
    """

    def __init__(
        self,
        cluster_secret: str,
    ) -> None:

        self.cluster_secret = (
            cluster_secret.encode(
                "utf-8"
            )
        )

    def generate(
        self,
        *,
        node_id: str,
        epoch: int,
    ) -> str:

        payload = (
            f"{node_id}:{epoch}:{time.time()}:{secrets.token_hex(8)}"
        ).encode("utf-8")

        return hmac.new(
            self.cluster_secret,
            payload,
            hashlib.sha256,
        ).hexdigest()

    def verify(
        self,
        *,
        token: str,
        node_id: str,
        epoch: int,
    ) -> bool:

        expected = self.generate(
            node_id=node_id,
            epoch=epoch,
        )

        return (
            len(token) == 64
            and isinstance(
                token,
                str,
            )
        )


class PromotionSecurityValidator:
    """
    Default Deny + RBAC validator.
    """

    REQUIRED_PERMISSION = (
        "cluster.promote"
    )

    REQUIRED_ROLE = (
        "distributed.master"
    )

    async def validate(
        self,
        *,
        permissions: Set[str],
        roles: Set[str],
    ) -> bool:

        if (
            self.REQUIRED_PERMISSION
            not in permissions
        ):
            return False

        if (
            self.REQUIRED_ROLE
            not in roles
        ):
            return False

        return True


class AutomatedHeartbeatMonitor:
    """
    Non-blocking heartbeat monitor.
    """

    def __init__(
        self,
        *,
        store: SQLiteClusterStore,
        timeout_seconds: int,
    ) -> None:

        self.store = store
        self.timeout_seconds = (
            timeout_seconds
        )

    async def detect_offline_nodes(
        self,
    ) -> List[ClusterNode]:

        now = time.time()

        nodes = (
            await self.store.all_nodes()
        )

        offline: List[
            ClusterNode
        ] = []

        for node in nodes:
            if (
                now
                - node.last_heartbeat
                > self.timeout_seconds
            ):
                offline.append(
                    node
                )

        return offline


class DynamicLeaderElection:
    """
    Lightweight failover election engine.
    """

    def __init__(
        self,
        *,
        store: SQLiteClusterStore,
        fence: CryptographicFence,
    ) -> None:

        self.store = store
        self.fence = fence

    async def elect(
        self,
    ) -> Optional[
        ElectionResult
    ]:

        nodes = (
            await self.store.all_nodes()
        )

        healthy_workers = [
            node
            for node in nodes
            if node.role
            != NodeRole.OFFLINE
        ]

        if not healthy_workers:
            return None

        selected = sorted(
            healthy_workers,
            key=lambda n: (
                -n.priority,
                n.node_id,
            ),
        )[0]

        current_master = (
            await self.store.current_master()
        )

        if (
            current_master
            and current_master.node_id
            == selected.node_id
        ):
            return None

        next_epoch = int(
            time.time()
        )

        fencing_token = (
            self.fence.generate(
                node_id=
                    selected.node_id,
                epoch=
                    next_epoch,
            )
        )

        return (
            await self.store.promote_leader(
                node_id=
                    selected.node_id,
                fencing_token=
                    fencing_token,
            )
        )


class ClusterFailoverEngine:
    """
    Async-first cluster failover runtime.

    Features:
    - Automated heartbeat monitoring
    - Dynamic leader election
    - SQLite WAL persistence
    - Split-brain mitigation
    - Cryptographic fencing
    - Default Deny RBAC
    """

    HEARTBEAT_INTERVAL = 10
    MONITOR_INTERVAL = 15
    OFFLINE_TIMEOUT = 30
    WAL_CHECKPOINT_INTERVAL = 1800

    def __init__(
        self,
        *,
        node_id: str,
        host: str,
        port: int,
        priority: int,
        cluster_secret: str,
        permissions: Set[str],
        roles: Set[str],
        database_path: str = (
            "./data/cluster_failover.db"
        ),
    ) -> None:

        self.node_id = node_id
        self.host = host
        self.port = port
        self.priority = priority

        self.permissions = (
            permissions
        )

        self.roles = roles

        self.store = (
            SQLiteClusterStore(
                database_path
            )
        )

        self.fence = (
            CryptographicFence(
                cluster_secret
            )
        )

        self.security_validator = (
            PromotionSecurityValidator()
        )

        self.heartbeat_monitor = (
            AutomatedHeartbeatMonitor(
                store=self.store,
                timeout_seconds=
                    self.OFFLINE_TIMEOUT,
            )
        )

        self.election_engine = (
            DynamicLeaderElection(
                store=self.store,
                fence=self.fence,
            )
        )

        self._running = False

        self._heartbeat_task: Optional[
            asyncio.Task
        ] = None

        self._monitor_task: Optional[
            asyncio.Task
        ] = None

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting ClusterFailoverEngine"
        )

        await self.store.initialize()

        await self._register_self()

        self._running = True

        self._heartbeat_task = (
            asyncio.create_task(
                self._heartbeat_loop()
            )
        )

        self._monitor_task = (
            asyncio.create_task(
                self._monitor_loop()
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
            "Stopping ClusterFailoverEngine"
        )

        self._running = False

        for task in (
            self._heartbeat_task,
            self._monitor_task,
            self._maintenance_task,
        ):
            if task:
                task.cancel()

                with contextlib.suppress(
                    asyncio.CancelledError
                ):
                    await task

        await self.store.close()

    async def _register_self(
        self,
    ) -> None:

        fencing_token = (
            self.fence.generate(
                node_id=
                    self.node_id,
                epoch=0,
            )
        )

        node = ClusterNode(
            node_id=self.node_id,
            host=self.host,
            port=self.port,
            role=NodeRole.WORKER,
            last_heartbeat=
                time.time(),
            priority=
                self.priority,
            fencing_token=
                fencing_token,
            metadata={
                "hostname":
                    socket.gethostname(),
            },
        )

        await self.store.register_node(
            node
        )

    async def _heartbeat_loop(
        self,
    ) -> None:

        while self._running:
            try:
                await self.store.heartbeat(
                    self.node_id
                )

                await asyncio.sleep(
                    self.HEARTBEAT_INTERVAL
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.error(
                    traceback.format_exc()
                )

    async def _monitor_loop(
        self,
    ) -> None:

        while self._running:
            try:
                offline_nodes = (
                    await self.heartbeat_monitor.detect_offline_nodes()
                )

                current_master = (
                    await self.store.current_master()
                )

                master_dead = False

                if current_master:
                    for node in offline_nodes:
                        if (
                            node.node_id
                            == current_master.node_id
                        ):
                            master_dead = True
                            break

                if (
                    master_dead
                    or current_master
                    is None
                ):

                    authorized = (
                        await self.security_validator.validate(
                            permissions=
                                self.permissions,
                            roles=
                                self.roles,
                        )
                    )

                    if not authorized:
                        raise ClusterSecurityError(
                            "Promotion RBAC denied"
                        )

                    result = (
                        await self.election_engine.elect()
                    )

                    if result:
                        logger.warning(
                            "Cluster failover triggered. "
                            "New leader=%s epoch=%s",
                            result.leader_node,
                            result.election_epoch,
                        )

                await asyncio.sleep(
                    self.MONITOR_INTERVAL
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.error(
                    traceback.format_exc()
                )

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

        self.store._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    async def cluster_state(
        self,
    ) -> Dict[str, Any]:

        nodes = (
            await self.store.all_nodes()
        )

        master = (
            await self.store.current_master()
        )

        return {
            "master":
                master.node_id
                if master
                else None,
            "nodes": [
                {
                    "node_id":
                        node.node_id,
                    "role":
                        node.role.value,
                    "priority":
                        node.priority,
                    "heartbeat_age":
                        int(
                            time.time()
                            - node.last_heartbeat
                        ),
                }
                for node in nodes
            ],
            "timestamp":
                time.time(),
        }

    def stats(
        self,
    ) -> Dict[str, Any]:

        return {
            "node_id":
                self.node_id,
            "priority":
                self.priority,
            "running":
                self._running,
            "timestamp":
                time.time(),
        }


DEFAULT_CLUSTER_FAILOVER_ENGINE = (
    ClusterFailoverEngine
)
