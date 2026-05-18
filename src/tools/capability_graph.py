from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

from app.tools.dynamic_router import (
    DynamicToolRouter,
    RouteContext,
    RouteDecision,
)


logger = logging.getLogger(__name__)


class GraphValidationState(str, Enum):
    VALID = "valid"
    INVALID = "invalid"
    CYCLIC = "cyclic"
    DENIED = "denied"


class ExecutionState(str, Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(slots=True)
class ToolNode:
    tool_name: str
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class DependencyEdge:
    source: str
    target: str
    condition: Optional[str] = None


@dataclass(slots=True)
class GraphExecutionResult:
    success: bool
    execution_order: List[str]
    failed_nodes: List[str]
    skipped_nodes: List[str]
    duration_ms: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class CapabilityGraph:
    """
    Lightweight in-memory DAG-based
    tool capability graph.

    Features:
    - Adjacency-list graph storage
    - Topological sorting
    - Cycle detection
    - RBAC verification
    - Dependency-aware execution
    - Async-safe orchestration
    - Minimal RAM footprint
    """

    MAX_GRAPH_NODES = 256

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

        self._nodes: Dict[
            str,
            ToolNode,
        ] = {}

        self._adjacency: Dict[
            str,
            Set[str],
        ] = defaultdict(set)

        self._reverse_adjacency: Dict[
            str,
            Set[str],
        ] = defaultdict(set)

        self._execution_history: Deque[
            Dict[str, Any]
        ] = deque(maxlen=128)

        self._lock = asyncio.Lock()

    async def add_tool(
        self,
        *,
        tool_name: str,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> None:
        async with self._lock:
            if (
                len(self._nodes)
                >= self.MAX_GRAPH_NODES
            ):
                raise MemoryError(
                    "Maximum graph node limit reached"
                )

            self._nodes[tool_name] = ToolNode(
                tool_name=tool_name,
                metadata=metadata or {},
            )

    async def add_dependency(
        self,
        *,
        source_tool: str,
        target_tool: str,
    ) -> None:
        """
        source_tool -> target_tool

        Means:
        target_tool depends on source_tool
        """

        async with self._lock:
            self._assert_tool_exists(
                source_tool
            )

            self._assert_tool_exists(
                target_tool
            )

            self._adjacency[
                source_tool
            ].add(target_tool)

            self._reverse_adjacency[
                target_tool
            ].add(source_tool)

            if await self.has_cycle():
                self._adjacency[
                    source_tool
                ].discard(target_tool)

                self._reverse_adjacency[
                    target_tool
                ].discard(source_tool)

                raise ValueError(
                    "Circular dependency detected"
                )

    async def remove_tool(
        self,
        tool_name: str,
    ) -> None:
        async with self._lock:
            self._nodes.pop(
                tool_name,
                None,
            )

            self._adjacency.pop(
                tool_name,
                None,
            )

            self._reverse_adjacency.pop(
                tool_name,
                None,
            )

            for edges in (
                self._adjacency.values()
            ):
                edges.discard(tool_name)

            for edges in (
                self._reverse_adjacency.values()
            ):
                edges.discard(tool_name)

    async def validate_graph(
        self,
        *,
        context: RouteContext,
    ) -> GraphValidationState:
        if await self.has_cycle():
            return (
                GraphValidationState.CYCLIC
            )

        for tool_name in self._nodes:
            route = await self.router.route(
                task=tool_name,
                context=context,
            )

            if (
                route.decision
                != RouteDecision.ALLOWED
            ):
                logger.warning(
                    "RBAC denied tool in graph | tool=%s",
                    tool_name,
                )

                return (
                    GraphValidationState.DENIED
                )

        return GraphValidationState.VALID

    async def has_cycle(
        self,
    ) -> bool:
        """
        DFS-based cycle detection.
        """

        visited: Set[str] = set()
        recursion_stack: Set[str] = set()

        for node in self._nodes:
            if node not in visited:
                if self._dfs_cycle_check(
                    node,
                    visited,
                    recursion_stack,
                ):
                    return True

        return False

    def _dfs_cycle_check(
        self,
        node: str,
        visited: Set[str],
        recursion_stack: Set[str],
    ) -> bool:
        visited.add(node)
        recursion_stack.add(node)

        for neighbor in (
            self._adjacency.get(
                node,
                set(),
            )
        ):
            if neighbor not in visited:
                if self._dfs_cycle_check(
                    neighbor,
                    visited,
                    recursion_stack,
                ):
                    return True

            elif neighbor in recursion_stack:
                return True

        recursion_stack.remove(node)

        return False

    async def topological_sort(
        self,
    ) -> List[str]:
        """
        Kahn's Algorithm.
        """

        indegree: Dict[
            str,
            int,
        ] = {
            node: 0
            for node in self._nodes
        }

        for source in self._adjacency:
            for target in (
                self._adjacency[source]
            ):
                indegree[target] += 1

        queue: Deque[str] = deque(
            [
                node
                for node, degree
                in indegree.items()
                if degree == 0
            ]
        )

        ordered: List[str] = []

        while queue:
            node = queue.popleft()

            ordered.append(node)

            for neighbor in (
                self._adjacency.get(
                    node,
                    set(),
                )
            ):
                indegree[neighbor] -= 1

                if indegree[neighbor] == 0:
                    queue.append(
                        neighbor
                    )

        if len(ordered) != len(
            self._nodes
        ):
            raise ValueError(
                "Graph contains cycle"
            )

        return ordered

    async def execute_graph(
        self,
        *,
        context: RouteContext,
        payloads: Optional[
            Dict[str, Dict[str, Any]]
        ] = None,
    ) -> GraphExecutionResult:
        """
        Dependency-aware graph execution.

        Lightweight sequential executor
        optimized for low-resource VPS.
        """

        start = time.perf_counter()

        payloads = payloads or {}

        validation = await self.validate_graph(
            context=context
        )

        if validation != (
            GraphValidationState.VALID
        ):
            return GraphExecutionResult(
                success=False,
                execution_order=[],
                failed_nodes=[],
                skipped_nodes=list(
                    self._nodes.keys()
                ),
                duration_ms=0.0,
                metadata={
                    "validation":
                        validation.value,
                },
            )

        execution_order = (
            await self.topological_sort()
        )

        completed: List[str] = []
        failed: List[str] = []
        skipped: List[str] = []

        states: Dict[
            str,
            ExecutionState,
        ] = {
            tool:
                ExecutionState.PENDING
            for tool in execution_order
        }

        for tool_name in execution_order:
            dependencies = (
                self._reverse_adjacency.get(
                    tool_name,
                    set(),
                )
            )

            dependency_failed = any(
                dep in failed
                for dep in dependencies
            )

            if dependency_failed:
                states[tool_name] = (
                    ExecutionState.SKIPPED
                )

                skipped.append(tool_name)

                logger.warning(
                    "Skipping tool due to dependency failure | tool=%s",
                    tool_name,
                )

                continue

            route = await self.router.route(
                task=tool_name,
                context=context,
            )

            if (
                route.decision
                != RouteDecision.ALLOWED
            ):
                failed.append(tool_name)

                states[tool_name] = (
                    ExecutionState.FAILED
                )

                logger.warning(
                    "RBAC denied execution | tool=%s",
                    tool_name,
                )

                continue

            try:
                states[tool_name] = (
                    ExecutionState.RUNNING
                )

                result = await self.router.execute(
                    task=tool_name,
                    context=context,
                    payload=payloads.get(
                        tool_name,
                        {},
                    ),
                )

                completed.append(tool_name)

                states[tool_name] = (
                    ExecutionState.COMPLETED
                )

                logger.info(
                    "Tool executed | tool=%s",
                    tool_name,
                )

                payloads = (
                    self._propagate_result(
                        tool_name,
                        result,
                        payloads,
                    )
                )

            except Exception:
                logger.exception(
                    "Tool execution failed | tool=%s",
                    tool_name,
                )

                failed.append(tool_name)

                states[tool_name] = (
                    ExecutionState.FAILED
                )

        duration_ms = round(
            (
                time.perf_counter()
                - start
            ) * 1000,
            2,
        )

        success = len(failed) == 0

        result = GraphExecutionResult(
            success=success,
            execution_order=execution_order,
            failed_nodes=failed,
            skipped_nodes=skipped,
            duration_ms=duration_ms,
            metadata={
                "completed":
                    completed,
                "states": {
                    key: value.value
                    for key, value
                    in states.items()
                },
            },
        )

        self._execution_history.append(
            {
                "success":
                    success,
                "duration_ms":
                    duration_ms,
                "timestamp":
                    time.time(),
            }
        )

        return result

    def _propagate_result(
        self,
        tool_name: str,
        result: Any,
        payloads: Dict[
            str,
            Dict[str, Any],
        ],
    ) -> Dict[
        str,
        Dict[str, Any],
    ]:
        """
        Lightweight output propagation.

        Tool output becomes downstream
        dependency input.
        """

        downstream_tools = (
            self._adjacency.get(
                tool_name,
                set(),
            )
        )

        for downstream in downstream_tools:
            if downstream not in payloads:
                payloads[downstream] = {}

            payloads[downstream][
                f"{tool_name}_output"
            ] = result

        return payloads

    async def execution_plan(
        self,
    ) -> Dict[str, Any]:
        order = await self.topological_sort()

        return {
            "nodes":
                list(
                    self._nodes.keys()
                ),
            "execution_order":
                order,
            "edges": {
                node:
                    list(edges)
                for node, edges
                in self._adjacency.items()
            },
            "timestamp":
                time.time(),
        }

    def graph_stats(
        self,
    ) -> Dict[str, Any]:
        edge_count = sum(
            len(edges)
            for edges in (
                self._adjacency.values()
            )
        )

        return {
            "nodes":
                len(self._nodes),
            "edges":
                edge_count,
            "history_entries":
                len(
                    self._execution_history
                ),
            "timestamp":
                time.time(),
        }

    def clear(
        self,
    ) -> None:
        self._nodes.clear()

        self._adjacency.clear()

        self._reverse_adjacency.clear()

        self._execution_history.clear()

    def _assert_tool_exists(
        self,
        tool_name: str,
    ) -> None:
        if tool_name not in self._nodes:
            raise KeyError(
                f"Unknown tool: {tool_name}"
            )
