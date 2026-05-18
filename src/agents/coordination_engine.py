from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from app.core.message_bus import MessageBus

from app.agents.monitoring_agent import MonitoringAgent
from app.agents.recovery_agent import RecoveryAgent
from app.agents.memory_agent import MemoryAgent


logger = logging.getLogger(__name__)


class EngineState(str, Enum):
    INITIALIZING = "initializing"
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(slots=True)
class AgentRegistration:
    agent_id: str
    instance: Any
    critical: bool = True
    auto_start: bool = True
    started: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


class CoordinationEngine:
    """
    Centralized orchestration engine for multi-agent coordination.

    Responsibilities:
    - Shared lifecycle management
    - Agent orchestration
    - Central event routing
    - Topology coordination
    - Graceful shutdown
    - Signal-safe termination
    - Lightweight VPS-optimized execution model
    """

    HEARTBEAT_INTERVAL = 15
    ENGINE_MONITOR_INTERVAL = 10
    SHUTDOWN_TIMEOUT = 20

    def __init__(
        self,
        *,
        message_bus: Optional[MessageBus] = None,
    ) -> None:
        self.message_bus = message_bus or MessageBus()

        self.state = EngineState.INITIALIZING

        self._agents: Dict[str, AgentRegistration] = {}

        self._tasks: List[asyncio.Task] = []

        self._shutdown_event = asyncio.Event()

        self._signal_handlers_registered = False

        self._started_at: Optional[float] = None

        self._routing_lock = asyncio.Lock()

        self._topology_version = 1

        self._event_subscriptions: Set[str] = set()

    async def initialize(self) -> None:
        logger.info("Initializing CoordinationEngine")

        self.state = EngineState.STARTING

        await self._register_core_event_handlers()

        await self._bootstrap_core_agents()

        self._register_signal_handlers()

        self.state = EngineState.RUNNING
        self._started_at = time.time()

        self._tasks.extend(
            [
                asyncio.create_task(
                    self._engine_health_loop()
                ),
                asyncio.create_task(
                    self._topology_sync_loop()
                ),
                asyncio.create_task(
                    self._heartbeat_loop()
                ),
            ]
        )

        logger.info("CoordinationEngine initialized")

    async def start(self) -> None:
        logger.info("Starting CoordinationEngine")

        try:
            await self.initialize()

            await self._start_registered_agents()

            logger.info("CoordinationEngine running")

            await self._shutdown_event.wait()

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "Fatal engine initialization failure"
            )

            self.state = EngineState.FAILED

            raise

        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        if self.state in {
            EngineState.STOPPING,
            EngineState.STOPPED,
        }:
            return

        logger.warning(
            "CoordinationEngine graceful shutdown started"
        )

        self.state = EngineState.STOPPING

        await self._publish_shutdown_notice()

        for task in self._tasks:
            task.cancel()

        for task in self._tasks:
            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await task

        self._tasks.clear()

        await self._shutdown_agents()

        self.state = EngineState.STOPPED

        logger.info(
            "CoordinationEngine shutdown complete"
        )

    async def register_agent(
        self,
        agent: Any,
        *,
        critical: bool = True,
        auto_start: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        agent_id = getattr(agent, "agent_id", None)

        if not agent_id:
            raise ValueError(
                "Agent missing required 'agent_id'"
            )

        if agent_id in self._agents:
            raise ValueError(
                f"Agent already registered: {agent_id}"
            )

        registration = AgentRegistration(
            agent_id=agent_id,
            instance=agent,
            critical=critical,
            auto_start=auto_start,
            metadata=metadata or {},
        )

        self._agents[agent_id] = registration

        logger.info(
            "Agent registered | id=%s critical=%s",
            agent_id,
            critical,
        )

        await self.message_bus.publish(
            "engine.agent_registered",
            {
                "agent_id": agent_id,
                "critical": critical,
                "timestamp": time.time(),
            },
        )

    async def unregister_agent(
        self,
        agent_id: str,
    ) -> None:
        registration = self._agents.pop(
            agent_id,
            None,
        )

        if not registration:
            return

        try:
            if hasattr(registration.instance, "stop"):
                await registration.instance.stop()

        except Exception:
            logger.exception(
                "Agent shutdown failure | agent=%s",
                agent_id,
            )

        await self.message_bus.publish(
            "engine.agent_unregistered",
            {
                "agent_id": agent_id,
                "timestamp": time.time(),
            },
        )

        logger.info(
            "Agent unregistered | id=%s",
            agent_id,
        )

    async def orchestrate_task(
        self,
        *,
        task_type: str,
        payload: Dict[str, Any],
    ) -> None:
        """
        Central orchestration router.

        Routes complex tasks/events to proper agents using
        lightweight event dispatching.
        """

        async with self._routing_lock:
            route = self._resolve_route(task_type)

            logger.info(
                "Orchestrating task | type=%s route=%s",
                task_type,
                route,
            )

            event_payload = {
                "task_type": task_type,
                "payload": payload,
                "timestamp": time.time(),
            }

            await self.message_bus.publish(
                route,
                event_payload,
            )

    async def emit_system_event(
        self,
        event_name: str,
        payload: Dict[str, Any],
    ) -> None:
        await self.message_bus.publish(
            event_name,
            payload,
        )

    async def _bootstrap_core_agents(self) -> None:
        """
        Bootstraps built-in coordination agents.
        """

        memory_agent = MemoryAgent(
            message_bus=self.message_bus,
        )

        monitoring_agent = MonitoringAgent(
            message_bus=self.message_bus,
        )

        recovery_agent = RecoveryAgent(
            message_bus=self.message_bus,
            agent_registry={},
        )

        await self.register_agent(
            memory_agent,
            critical=True,
        )

        await self.register_agent(
            monitoring_agent,
            critical=True,
        )

        await self.register_agent(
            recovery_agent,
            critical=True,
        )

        recovery_agent.agent_registry = {
            agent_id: registration.instance
            for agent_id, registration
            in self._agents.items()
        }

    async def _start_registered_agents(self) -> None:
        for registration in self._agents.values():
            if not registration.auto_start:
                continue

            try:
                logger.info(
                    "Starting agent | id=%s",
                    registration.agent_id,
                )

                await registration.instance.start()

                registration.started = True

                await self.message_bus.publish(
                    "agent.lifecycle",
                    {
                        "agent_id": registration.agent_id,
                        "state": "started",
                        "timestamp": time.time(),
                    },
                )

            except Exception:
                logger.exception(
                    "Agent startup failure | agent=%s",
                    registration.agent_id,
                )

                if registration.critical:
                    raise

    async def _shutdown_agents(self) -> None:
        shutdown_tasks: List[asyncio.Task] = []

        for registration in reversed(
            list(self._agents.values())
        ):
            if not registration.started:
                continue

            shutdown_tasks.append(
                asyncio.create_task(
                    self._safe_stop_agent(
                        registration
                    )
                )
            )

        if shutdown_tasks:
            done, pending = await asyncio.wait(
                shutdown_tasks,
                timeout=self.SHUTDOWN_TIMEOUT,
            )

            for task in pending:
                task.cancel()

    async def _safe_stop_agent(
        self,
        registration: AgentRegistration,
    ) -> None:
        try:
            logger.info(
                "Stopping agent | id=%s",
                registration.agent_id,
            )

            await registration.instance.stop()

            registration.started = False

            await self.message_bus.publish(
                "agent.lifecycle",
                {
                    "agent_id": registration.agent_id,
                    "state": "stopped",
                    "timestamp": time.time(),
                },
            )

        except Exception:
            logger.exception(
                "Agent stop failure | agent=%s",
                registration.agent_id,
            )

    async def _register_core_event_handlers(
        self,
    ) -> None:
        subscriptions = {
            "system.degradation_mode":
                self._handle_degradation_event,

            "system.emergency_mode":
                self._handle_emergency_event,

            "monitor.alert":
                self._handle_monitor_alert,

            "engine.orchestrate":
                self._handle_orchestration_request,
        }

        for topic, handler in subscriptions.items():
            await self.message_bus.subscribe(
                topic,
                handler,
            )

            self._event_subscriptions.add(topic)

    async def _handle_degradation_event(
        self,
        payload: Dict[str, Any],
    ) -> None:
        logger.warning(
            "Engine entering degraded state | payload=%s",
            payload,
        )

        self.state = EngineState.DEGRADED

    async def _handle_emergency_event(
        self,
        payload: Dict[str, Any],
    ) -> None:
        logger.critical(
            "Emergency mode activated | payload=%s",
            payload,
        )

        self.state = EngineState.DEGRADED

    async def _handle_monitor_alert(
        self,
        payload: Dict[str, Any],
    ) -> None:
        severity = payload.get("severity")

        if severity == "critical":
            logger.warning(
                "Critical monitor alert received"
            )

    async def _handle_orchestration_request(
        self,
        payload: Dict[str, Any],
    ) -> None:
        task_type = payload.get("task_type")

        if not task_type:
            return

        await self.orchestrate_task(
            task_type=task_type,
            payload=payload,
        )

    async def _heartbeat_loop(self) -> None:
        while self.state in {
            EngineState.RUNNING,
            EngineState.DEGRADED,
        }:
            try:
                for registration in (
                    self._agents.values()
                ):
                    if not registration.started:
                        continue

                    await self.message_bus.publish(
                        "agent.heartbeat",
                        {
                            "agent_id":
                                registration.agent_id,
                            "source": "engine",
                            "timestamp":
                                time.time(),
                        },
                    )

            except Exception:
                logger.exception(
                    "Engine heartbeat loop failure"
                )

            await asyncio.sleep(
                self.HEARTBEAT_INTERVAL
            )

    async def _engine_health_loop(self) -> None:
        while self.state in {
            EngineState.RUNNING,
            EngineState.DEGRADED,
        }:
            try:
                await self._publish_engine_state()

            except Exception:
                logger.exception(
                    "Engine health loop failure"
                )

            await asyncio.sleep(
                self.ENGINE_MONITOR_INTERVAL
            )

    async def _topology_sync_loop(self) -> None:
        while self.state in {
            EngineState.RUNNING,
            EngineState.DEGRADED,
        }:
            try:
                await self._publish_topology()

            except Exception:
                logger.exception(
                    "Topology sync loop failure"
                )

            await asyncio.sleep(30)

    async def _publish_engine_state(
        self,
    ) -> None:
        await self.message_bus.publish(
            "engine.state",
            {
                "state": self.state.value,
                "agents":
                    len(self._agents),
                "uptime":
                    self._calculate_uptime(),
                "timestamp":
                    time.time(),
            },
        )

    async def _publish_topology(
        self,
    ) -> None:
        topology = {
            "version": self._topology_version,
            "state": self.state.value,
            "agents": [],
            "timestamp": time.time(),
        }

        for registration in self._agents.values():
            topology["agents"].append(
                {
                    "agent_id":
                        registration.agent_id,
                    "critical":
                        registration.critical,
                    "started":
                        registration.started,
                    "metadata":
                        registration.metadata,
                }
            )

        await self.message_bus.publish(
            "engine.topology",
            topology,
        )

    async def _publish_shutdown_notice(
        self,
    ) -> None:
        await self.message_bus.publish(
            "engine.shutdown",
            {
                "timestamp": time.time(),
                "state": self.state.value,
            },
        )

    def _resolve_route(
        self,
        task_type: str,
    ) -> str:
        """
        Lightweight routing table optimized for
        centralized orchestration.
        """

        routing_map = {
            "memory":
                "memory.write",

            "memory.read":
                "memory.read",

            "memory.query":
                "memory.query",

            "recovery":
                "monitor.alert",

            "monitor":
                "monitor.health_snapshot",

            "execution":
                "execution.request",

            "planner":
                "planner.request",
        }

        for prefix, route in routing_map.items():
            if task_type.startswith(prefix):
                return route

        return "system.events"

    def _register_signal_handlers(
        self,
    ) -> None:
        if self._signal_handlers_registered:
            return

        loop = asyncio.get_running_loop()

        signals = (
            signal.SIGINT,
            signal.SIGTERM,
        )

        for sig in signals:
            with contextlib.suppress(
                NotImplementedError
            ):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig:
                    asyncio.create_task(
                        self._handle_shutdown_signal(s)
                    ),
                )

        self._signal_handlers_registered = True

    async def _handle_shutdown_signal(
        self,
        sig: signal.Signals,
    ) -> None:
        logger.warning(
            "Shutdown signal received | signal=%s",
            sig.name,
        )

        self._shutdown_event.set()

    def _calculate_uptime(self) -> float:
        if not self._started_at:
            return 0.0

        return round(
            time.time() - self._started_at,
            2,
        )

    @property
    def topology_version(self) -> int:
        return self._topology_version

    @property
    def registered_agents(self) -> List[str]:
        return list(self._agents.keys())

    @property
    def uptime_seconds(self) -> float:
        return self._calculate_uptime()
