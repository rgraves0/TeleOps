from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha1
from typing import Any, Dict, List, Optional, Set, Tuple

from app.tools.dynamic_router import (
    DynamicToolRouter,
    RouteContext,
    RouteDecision,
    ToolMetadata,
)


logger = logging.getLogger(__name__)


class SelectionDecision(str, Enum):
    SELECTED = "selected"
    DENIED = "denied"
    NO_MATCH = "no_match"
    INVALID = "invalid"


@dataclass(slots=True)
class FunctionCallRequest:
    prompt: str
    requester_id: str
    requester_roles: Set[str]
    requester_permissions: Set[str]
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class FunctionCallResult:
    decision: SelectionDecision
    tool_name: Optional[str]
    confidence: float
    parameters: Dict[str, Any]
    reasoning: str
    schema: Optional[Dict[str, Any]]
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class ContextFilter:
    """
    Lightweight context compressor/filter.

    Prevents:
    - Context explosion
    - Excessive LLM token pressure
    - Large metadata propagation
    """

    MAX_CONTEXT_LENGTH = 4096
    MAX_LINES = 48
    MAX_METADATA_ITEMS = 20

    SENSITIVE_PATTERNS = [
        r"sk-[a-zA-Z0-9]+",
        r"password\s*[:=]\s*\S+",
        r"token\s*[:=]\s*\S+",
        r"secret\s*[:=]\s*\S+",
    ]

    def compress(
        self,
        text: str,
    ) -> str:
        text = self._sanitize(text)

        lines = text.splitlines()

        if len(lines) > self.MAX_LINES:
            lines = lines[: self.MAX_LINES]

        normalized = "\n".join(lines)

        if (
            len(normalized)
            > self.MAX_CONTEXT_LENGTH
        ):
            normalized = (
                normalized[
                    : self.MAX_CONTEXT_LENGTH
                ] + "\n...[compressed]"
            )

        return normalized.strip()

    def compress_metadata(
        self,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        for idx, (key, value) in enumerate(
            metadata.items()
        ):
            if idx >= self.MAX_METADATA_ITEMS:
                break

            result[key] = self._truncate(
                str(value)
            )

        return result

    def _sanitize(
        self,
        text: str,
    ) -> str:
        for pattern in (
            self.SENSITIVE_PATTERNS
        ):
            text = re.sub(
                pattern,
                "[REDACTED]",
                text,
                flags=re.IGNORECASE,
            )

        return text

    def _truncate(
        self,
        value: str,
    ) -> str:
        if len(value) > 256:
            return value[:256] + "..."

        return value


class JSONSchemaBuilder:
    """
    Lightweight JSON schema builder
    for native LLM function calling.
    """

    MAX_DESCRIPTION = 256

    def build(
        self,
        tool: ToolMetadata,
    ) -> Dict[str, Any]:
        schema = {
            "name": tool.tool_name,
            "description": (
                tool.description[
                    : self.MAX_DESCRIPTION
                ]
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        }

        params = (
            tool.metadata.get(
                "parameters",
                {},
            )
        )

        for key, definition in (
            params.items()
        ):
            schema["parameters"][
                "properties"
            ][key] = {
                "type":
                    definition.get(
                        "type",
                        "string",
                    ),
                "description":
                    definition.get(
                        "description",
                        "",
                    )[:128],
            }

            if definition.get(
                "required",
                False,
            ):
                schema["parameters"][
                    "required"
                ].append(key)

        return schema


class IntentAnalyzer:
    """
    Lightweight semantic intent analyzer.

    No embeddings/vector DBs used.
    """

    STOPWORDS = {
        "the",
        "a",
        "an",
        "please",
        "help",
        "execute",
        "run",
        "tool",
        "using",
        "with",
    }

    def score(
        self,
        prompt: str,
        tool: ToolMetadata,
    ) -> float:
        prompt_keywords = (
            self._extract_keywords(prompt)
        )

        capability_keywords = set()

        for capability in (
            tool.capabilities
        ):
            capability_keywords.update(
                capability.lower().split(".")
            )

        tool_keywords = set(
            tool.tool_name.lower().split(".")
        )

        description_keywords = (
            self._extract_keywords(
                tool.description
            )
        )

        total_keywords = (
            capability_keywords
            | tool_keywords
            | description_keywords
        )

        overlap = (
            prompt_keywords
            & total_keywords
        )

        if not total_keywords:
            return 0.0

        score = (
            len(overlap)
            / len(total_keywords)
        )

        return round(
            min(score * 3, 1.0),
            3,
        )

    def _extract_keywords(
        self,
        text: str,
    ) -> Set[str]:
        tokens = re.findall(
            r"[a-zA-Z0-9_]+",
            text.lower(),
        )

        return {
            token
            for token in tokens
            if token not in self.STOPWORDS
            and len(token) > 2
        }


class ParameterValidator:
    """
    Strict lightweight parameter validator.
    """

    MAX_STRING_LENGTH = 4096

    def validate(
        self,
        *,
        schema: Dict[str, Any],
        parameters: Dict[str, Any],
    ) -> Tuple[bool, Optional[str]]:
        required = (
            schema.get(
                "parameters",
                {},
            ).get(
                "required",
                [],
            )
        )

        properties = (
            schema.get(
                "parameters",
                {},
            ).get(
                "properties",
                {},
            )
        )

        for field in required:
            if field not in parameters:
                return (
                    False,
                    f"Missing required parameter: {field}",
                )

        for key, value in (
            parameters.items()
        ):
            prop = properties.get(key)

            if not prop:
                return (
                    False,
                    f"Unexpected parameter: {key}",
                )

            expected_type = prop.get(
                "type",
                "string",
            )

            valid = self._validate_type(
                value,
                expected_type,
            )

            if not valid:
                return (
                    False,
                    f"Invalid parameter type: {key}",
                )

        return True, None

    def _validate_type(
        self,
        value: Any,
        expected_type: str,
    ) -> bool:
        if expected_type == "string":
            if not isinstance(
                value,
                str,
            ):
                return False

            return (
                len(value)
                <= self.MAX_STRING_LENGTH
            )

        if expected_type == "integer":
            return isinstance(
                value,
                int,
            )

        if expected_type == "number":
            return isinstance(
                value,
                (
                    int,
                    float,
                ),
            )

        if expected_type == "boolean":
            return isinstance(
                value,
                bool,
            )

        if expected_type == "object":
            return isinstance(
                value,
                dict,
            )

        if expected_type == "array":
            return isinstance(
                value,
                list,
            )

        return True


class NativeToolSelector:
    """
    AI-native Tool Selection Runtime.

    Features:
    - Native function calling support
    - Intent-to-tool mapping
    - Lightweight semantic routing
    - Context compression
    - RBAC validation
    - Default deny enforcement
    - JSON schema transformation
    - Production-safe validation
    """

    MAX_TOOL_CANDIDATES = 8

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

        self._context_filter = (
            ContextFilter()
        )

        self._schema_builder = (
            JSONSchemaBuilder()
        )

        self._intent_analyzer = (
            IntentAnalyzer()
        )

        self._parameter_validator = (
            ParameterValidator()
        )

        self._selection_cache: Dict[
            str,
            Tuple[
                FunctionCallResult,
                float,
            ],
        ] = {}

        self._selection_history: Deque[
            str
        ] = deque(maxlen=128)

    async def select_tool(
        self,
        request: FunctionCallRequest,
    ) -> FunctionCallResult:
        start = time.perf_counter()

        compressed_prompt = (
            self._context_filter.compress(
                request.prompt
            )
        )

        cache_key = self._cache_key(
            compressed_prompt,
            request.requester_roles,
        )

        cached = (
            self._selection_cache.get(
                cache_key
            )
        )

        if cached:
            return cached[0]

        tools = self.router.list_tools()

        if not tools:
            return self._no_match_result()

        candidates = await self._score_tools(
            compressed_prompt
        )

        if not candidates:
            return self._no_match_result()

        for tool_name, confidence in (
            candidates
        ):
            tool = self.router.get_tool(
                tool_name
            )

            if not tool:
                continue

            schema = (
                self._schema_builder.build(
                    tool
                )
            )

            parameters = (
                self._extract_parameters(
                    compressed_prompt,
                    schema,
                )
            )

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
                task_type=tool_name,
                metadata=(
                    self._context_filter.compress_metadata(
                        request.metadata
                    )
                ),
            )

            route = await self.router.route(
                task=tool_name,
                context=context,
            )

            if (
                route.decision
                != RouteDecision.ALLOWED
            ):
                continue

            valid, reason = (
                self._parameter_validator.validate(
                    schema=schema,
                    parameters=parameters,
                )
            )

            if not valid:
                logger.warning(
                    "Parameter validation failed | tool=%s reason=%s",
                    tool_name,
                    reason,
                )

                continue

            result = FunctionCallResult(
                decision=(
                    SelectionDecision.SELECTED
                ),
                tool_name=tool_name,
                confidence=confidence,
                parameters=parameters,
                reasoning=(
                    "Intent matched tool capability"
                ),
                schema=schema,
                created_at=time.time(),
                metadata={
                    "route_time_ms":
                        round(
                            (
                                time.perf_counter()
                                - start
                            ) * 1000,
                            2,
                        ),
                },
            )

            self._cache_result(
                cache_key,
                result,
            )

            self._selection_history.append(
                tool_name
            )

            return result

        return FunctionCallResult(
            decision=SelectionDecision.DENIED,
            tool_name=None,
            confidence=0.0,
            parameters={},
            reasoning=(
                "RBAC or validation denied"
            ),
            schema=None,
            created_at=time.time(),
        )

    async def export_function_schemas(
        self,
    ) -> List[Dict[str, Any]]:
        """
        Export lightweight schemas
        for native LLM function calling.
        """

        schemas: List[
            Dict[str, Any]
        ] = []

        tools = self.router.list_tools()

        for tool_info in tools[
            : self.MAX_TOOL_CANDIDATES
        ]:
            tool = self.router.get_tool(
                tool_info["tool_name"]
            )

            if not tool:
                continue

            schemas.append(
                self._schema_builder.build(
                    tool
                )
            )

        return schemas

    async def _score_tools(
        self,
        prompt: str,
    ) -> List[Tuple[str, float]]:
        scored: List[
            Tuple[str, float]
        ] = []

        tools = self.router.list_tools()

        for tool_info in tools:
            tool = self.router.get_tool(
                tool_info["tool_name"]
            )

            if not tool:
                continue

            score = (
                self._intent_analyzer.score(
                    prompt,
                    tool,
                )
            )

            if score > 0:
                scored.append(
                    (
                        tool.tool_name,
                        score,
                    )
                )

        scored.sort(
            key=lambda item: item[1],
            reverse=True,
        )

        return scored[
            : self.MAX_TOOL_CANDIDATES
        ]

    def _extract_parameters(
        self,
        prompt: str,
        schema: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Lightweight heuristic parameter parser.
        """

        parameters: Dict[
            str,
            Any,
        ] = {}

        properties = (
            schema.get(
                "parameters",
                {},
            ).get(
                "properties",
                {},
            )
        )

        for key, definition in (
            properties.items()
        ):
            value = (
                self._extract_parameter_value(
                    prompt,
                    key,
                    definition,
                )
            )

            if value is not None:
                parameters[key] = value

        return parameters

    def _extract_parameter_value(
        self,
        prompt: str,
        key: str,
        definition: Dict[str, Any],
    ) -> Optional[Any]:
        expected_type = definition.get(
            "type",
            "string",
        )

        patterns = [
            rf"{key}\s*[:=]\s*([^\n,]+)",
            rf"{key}\s+is\s+([^\n,]+)",
        ]

        for pattern in patterns:
            match = re.search(
                pattern,
                prompt,
                flags=re.IGNORECASE,
            )

            if not match:
                continue

            raw = match.group(1).strip()

            try:
                if expected_type == "integer":
                    return int(raw)

                if expected_type == "number":
                    return float(raw)

                if expected_type == "boolean":
                    return raw.lower() in (
                        "true",
                        "1",
                        "yes",
                    )

                return raw

            except Exception:
                return None

        return None

    def _cache_result(
        self,
        key: str,
        result: FunctionCallResult,
    ) -> None:
        if (
            len(self._selection_cache)
            > 128
        ):
            oldest = next(
                iter(
                    self._selection_cache
                )
            )

            self._selection_cache.pop(
                oldest,
                None,
            )

        self._selection_cache[key] = (
            result,
            time.time(),
        )

    def _cache_key(
        self,
        prompt: str,
        roles: Set[str],
    ) -> str:
        raw = (
            prompt
            + "|"
            + ",".join(sorted(roles))
        )

        return sha1(
            raw.encode("utf-8")
        ).hexdigest()

    def _no_match_result(
        self,
    ) -> FunctionCallResult:
        return FunctionCallResult(
            decision=SelectionDecision.NO_MATCH,
            tool_name=None,
            confidence=0.0,
            parameters={},
            reasoning=(
                "No matching tool found"
            ),
            schema=None,
            created_at=time.time(),
        )

    def selection_stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "cache_entries":
                len(
                    self._selection_cache
                ),
            "selection_history":
                len(
                    self._selection_history
                ),
            "timestamp":
                time.time(),
        }

    def clear_cache(
        self,
    ) -> None:
        self._selection_cache.clear()

    def recent_selections(
        self,
    ) -> List[str]:
        return list(
            self._selection_history
        )
