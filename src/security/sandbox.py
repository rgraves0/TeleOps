from __future__ import annotations

import ast
import asyncio
import builtins
import contextlib
import io
import logging
import multiprocessing
import queue
import resource
import signal
import sys
import time
import traceback
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
)


logger = logging.getLogger(__name__)


class SandboxStatus(
    str,
    Enum,
):
    SUCCESS = "success"
    REJECTED = "rejected"
    TIMEOUT = "timeout"
    MEMORY_LIMIT = "memory_limit"
    EXECUTION_ERROR = (
        "execution_error"
    )


@dataclass(slots=True)
class SandboxResult:
    status: SandboxStatus
    output: str
    error: Optional[str]
    execution_time: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class ASTSecurityAnalyzer:
    """
    AST-based security validator.

    Rejects:
    - Dangerous imports
    - OS/system access
    - File operations
    - Dynamic eval/exec abuse
    - Reflection attacks
    """

    BLOCKED_MODULES = {
        "os",
        "sys",
        "subprocess",
        "socket",
        "multiprocessing",
        "ctypes",
        "shutil",
        "pathlib",
        "signal",
        "resource",
        "pickle",
        "marshal",
        "importlib",
        "inspect",
        "builtins",
        "asyncio.subprocess",
    }

    BLOCKED_FUNCTIONS = {
        "exec",
        "eval",
        "compile",
        "__import__",
        "open",
        "input",
        "globals",
        "locals",
        "vars",
        "dir",
        "getattr",
        "setattr",
        "delattr",
        "breakpoint",
        "memoryview",
    }

    BLOCKED_ATTRIBUTES = {
        "__dict__",
        "__class__",
        "__bases__",
        "__subclasses__",
        "__globals__",
        "__code__",
        "__closure__",
        "__func__",
        "__self__",
    }

    MAX_AST_NODES = 1000

    def analyze(
        self,
        code: str,
    ) -> Tuple[
        bool,
        Optional[str],
    ]:
        try:
            tree = ast.parse(
                code,
                mode="exec",
            )

        except SyntaxError as exc:
            return (
                False,
                f"Syntax error: {exc}",
            )

        node_count = sum(
            1 for _ in ast.walk(tree)
        )

        if (
            node_count
            > self.MAX_AST_NODES
        ):
            return (
                False,
                "AST complexity limit exceeded",
            )

        for node in ast.walk(tree):

            violation = (
                self._inspect_node(
                    node
                )
            )

            if violation:
                return (
                    False,
                    violation,
                )

        return (
            True,
            None,
        )

    def _inspect_node(
        self,
        node: ast.AST,
    ) -> Optional[str]:

        if isinstance(
            node,
            (
                ast.Import,
                ast.ImportFrom,
            ),
        ):
            for alias in node.names:
                root = (
                    alias.name.split(
                        "."
                    )[0]
                )

                if (
                    root
                    in self.BLOCKED_MODULES
                ):
                    return (
                        f"Blocked import: {root}"
                    )

        if isinstance(
            node,
            ast.Call,
        ):
            function_name = (
                self._resolve_name(
                    node.func
                )
            )

            if (
                function_name
                in self.BLOCKED_FUNCTIONS
            ):
                return (
                    f"Blocked function: {function_name}"
                )

        if isinstance(
            node,
            ast.Attribute,
        ):
            if (
                node.attr
                in self.BLOCKED_ATTRIBUTES
            ):
                return (
                    f"Blocked attribute: {node.attr}"
                )

        if isinstance(
            node,
            (
                ast.Global,
                ast.Nonlocal,
            ),
        ):
            return (
                "Global/nonlocal access denied"
            )

        return None

    def _resolve_name(
        self,
        node: ast.AST,
    ) -> str:
        if isinstance(
            node,
            ast.Name,
        ):
            return node.id

        if isinstance(
            node,
            ast.Attribute,
        ):
            return node.attr

        return ""


class ExecutionPolicy:
    """
    Strict execution whitelist.
    """

    SAFE_BUILTINS = MappingProxyType(
        {
            "abs": abs,
            "all": all,
            "any": any,
            "bool": bool,
            "dict": dict,
            "enumerate": enumerate,
            "filter": filter,
            "float": float,
            "int": int,
            "len": len,
            "list": list,
            "map": map,
            "max": max,
            "min": min,
            "pow": pow,
            "print": print,
            "range": range,
            "reversed": reversed,
            "round": round,
            "set": set,
            "sorted": sorted,
            "str": str,
            "sum": sum,
            "tuple": tuple,
            "zip": zip,
        }
    )

    SAFE_MODULES = MappingProxyType(
        {
            "math": __import__(
                "math"
            ),
            "json": __import__(
                "json"
            ),
            "statistics": __import__(
                "statistics"
            ),
            "datetime": __import__(
                "datetime"
            ),
        }
    )

    @classmethod
    def build_globals(
        cls,
    ) -> Dict[str, Any]:
        return {
            "__builtins__":
                cls.SAFE_BUILTINS,
            **cls.SAFE_MODULES,
        }


class MemoryTimeGuard:
    """
    Resource restriction guard.
    """

    DEFAULT_CPU_SECONDS = 2
    DEFAULT_MEMORY_MB = 64

    @classmethod
    def apply_limits(
        cls,
        *,
        cpu_seconds: int,
        memory_mb: int,
    ) -> None:
        memory_bytes = (
            memory_mb
            * 1024
            * 1024
        )

        try:
            resource.setrlimit(
                resource.RLIMIT_CPU,
                (
                    cpu_seconds,
                    cpu_seconds,
                ),
            )

            resource.setrlimit(
                resource.RLIMIT_AS,
                (
                    memory_bytes,
                    memory_bytes,
                ),
            )

        except Exception:
            logger.exception(
                "Failed to apply resource limits"
            )


class IsolatedExecutionEnvironment:
    """
    Process-isolated runtime.

    Uses:
    - Separate process
    - Restricted builtins
    - Resource limits
    - AST validation
    """

    def __init__(
        self,
        *,
        cpu_limit_seconds: int = 2,
        memory_limit_mb: int = 64,
    ) -> None:
        self.cpu_limit_seconds = max(
            1,
            cpu_limit_seconds,
        )

        self.memory_limit_mb = max(
            16,
            memory_limit_mb,
        )

        self._analyzer = (
            ASTSecurityAnalyzer()
        )

    async def execute(
        self,
        code: str,
    ) -> SandboxResult:
        """
        Async sandbox execution entrypoint.
        """

        start_time = time.monotonic()

        valid, reason = (
            self._analyzer.analyze(
                code
            )
        )

        if not valid:
            return SandboxResult(
                status=
                    SandboxStatus.REJECTED,
                output="",
                error=reason,
                execution_time=round(
                    time.monotonic()
                    - start_time,
                    6,
                ),
            )

        result = await asyncio.to_thread(
            self._run_isolated,
            code,
        )

        result.execution_time = round(
            time.monotonic()
            - start_time,
            6,
        )

        return result

    def _run_isolated(
        self,
        code: str,
    ) -> SandboxResult:
        result_queue: multiprocessing.Queue = (
            multiprocessing.Queue(
                maxsize=1
            )
        )

        process = (
            multiprocessing.Process(
                target=
                    self._sandbox_worker,
                args=(
                    code,
                    result_queue,
                    self.cpu_limit_seconds,
                    self.memory_limit_mb,
                ),
                daemon=True,
            )
        )

        process.start()

        process.join(
            timeout=
                self.cpu_limit_seconds
                + 1
        )

        if process.is_alive():
            process.terminate()

            process.join(
                timeout=0.5
            )

            return SandboxResult(
                status=
                    SandboxStatus.TIMEOUT,
                output="",
                error=
                    "Execution timeout exceeded",
                execution_time=0.0,
            )

        try:
            payload = (
                result_queue.get_nowait()
            )

        except queue.Empty:
            return SandboxResult(
                status=
                    SandboxStatus.EXECUTION_ERROR,
                output="",
                error=
                    "Sandbox execution failed",
                execution_time=0.0,
            )

        return SandboxResult(
            status=SandboxStatus(
                payload["status"]
            ),
            output=payload[
                "output"
            ],
            error=payload[
                "error"
            ],
            execution_time=0.0,
            metadata=payload.get(
                "metadata",
                {},
            ),
        )

    @staticmethod
    def _sandbox_worker(
        code: str,
        result_queue: multiprocessing.Queue,
        cpu_limit: int,
        memory_limit: int,
    ) -> None:
        try:
            signal.signal(
                signal.SIGXCPU,
                signal.SIG_DFL,
            )

            MemoryTimeGuard.apply_limits(
                cpu_seconds=
                    cpu_limit,
                memory_mb=
                    memory_limit,
            )

            sandbox_globals = (
                ExecutionPolicy.build_globals()
            )

            sandbox_locals: Dict[
                str,
                Any,
            ] = {}

            stdout_buffer = (
                io.StringIO()
            )

            stderr_buffer = (
                io.StringIO()
            )

            with contextlib.redirect_stdout(
                stdout_buffer
            ):
                with contextlib.redirect_stderr(
                    stderr_buffer
                ):
                    compiled = compile(
                        code,
                        "<sandbox>",
                        "exec",
                    )

                    exec(
                        compiled,
                        sandbox_globals,
                        sandbox_locals,
                    )

            output = (
                stdout_buffer.getvalue()
            )

            stderr = (
                stderr_buffer.getvalue()
            )

            result_queue.put(
                {
                    "status":
                        SandboxStatus.SUCCESS.value,
                    "output":
                        output,
                    "error":
                        stderr
                        or None,
                    "metadata": {
                        "memory_limit_mb":
                            memory_limit,
                        "cpu_limit_seconds":
                            cpu_limit,
                    },
                }
            )

        except MemoryError:
            result_queue.put(
                {
                    "status":
                        SandboxStatus.MEMORY_LIMIT.value,
                    "output": "",
                    "error":
                        "Memory limit exceeded",
                }
            )

        except BaseException as exc:
            formatted = (
                traceback.format_exc(
                    limit=5
                )
            )

            result_queue.put(
                {
                    "status":
                        SandboxStatus.EXECUTION_ERROR.value,
                    "output": "",
                    "error":
                        f"{exc}\n{formatted}",
                }
            )


class SandboxManager:
    """
    High-level sandbox controller.

    Features:
    - Async-first execution
    - AST security validation
    - Process isolation
    - Resource guardrails
    - Default Deny execution policy
    """

    def __init__(
        self,
        *,
        cpu_limit_seconds: int = 2,
        memory_limit_mb: int = 64,
        max_concurrent: int = 2,
    ) -> None:
        self.cpu_limit_seconds = (
            cpu_limit_seconds
        )

        self.memory_limit_mb = (
            memory_limit_mb
        )

        self._executor = (
            IsolatedExecutionEnvironment(
                cpu_limit_seconds=
                    cpu_limit_seconds,
                memory_limit_mb=
                    memory_limit_mb,
            )
        )

        self._semaphore = (
            asyncio.Semaphore(
                max(
                    1,
                    max_concurrent,
                )
            )
        )

        self._execution_count = 0

        self._rejection_count = 0

    async def execute_code(
        self,
        code: str,
    ) -> SandboxResult:
        """
        Main execution gateway.
        """

        async with self._semaphore:
            result = (
                await self._executor.execute(
                    code
                )
            )

            self._execution_count += 1

            if (
                result.status
                == SandboxStatus.REJECTED
            ):
                self._rejection_count += 1

            return result

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "executions":
                self._execution_count,
            "rejections":
                self._rejection_count,
            "cpu_limit_seconds":
                self.cpu_limit_seconds,
            "memory_limit_mb":
                self.memory_limit_mb,
            "timestamp":
                time.time(),
        }


DEFAULT_SANDBOX = SandboxManager(
    cpu_limit_seconds=2,
    memory_limit_mb=64,
    max_concurrent=2,
)
