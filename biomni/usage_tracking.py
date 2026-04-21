from __future__ import annotations

import contextvars
import threading
from contextlib import contextmanager
from typing import Any

from langchain_core.callbacks.base import BaseCallbackHandler


_ACTIVE_USAGE_COLLECTOR: contextvars.ContextVar[Any | None] = contextvars.ContextVar(
    "biomni_active_usage_collector",
    default=None,
)

_GLOBAL_COLLECTOR_STACK: list[Any] = []
_GLOBAL_COLLECTOR_LOCK = threading.Lock()


@contextmanager
def activate_usage_collector(collector: Any):
    """Temporarily register a collector for callback-based token accounting."""
    token = _ACTIVE_USAGE_COLLECTOR.set(collector)
    with _GLOBAL_COLLECTOR_LOCK:
        _GLOBAL_COLLECTOR_STACK.append(collector)
    try:
        yield
    finally:
        with _GLOBAL_COLLECTOR_LOCK:
            if _GLOBAL_COLLECTOR_STACK:
                _GLOBAL_COLLECTOR_STACK.pop()
        _ACTIVE_USAGE_COLLECTOR.reset(token)


def get_active_usage_collector() -> Any | None:
    collector = _ACTIVE_USAGE_COLLECTOR.get()
    if collector is not None:
        return collector

    # Fallback for threaded execution paths where contextvars do not propagate.
    with _GLOBAL_COLLECTOR_LOCK:
        if _GLOBAL_COLLECTOR_STACK:
            return _GLOBAL_COLLECTOR_STACK[-1]
    return None


def _safe_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _normalize_usage(usage: dict[str, Any] | None) -> dict[str, int]:
    if not isinstance(usage, dict):
        return {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}

    input_tokens = _safe_int(usage.get("input_tokens") or usage.get("prompt_tokens"))
    output_tokens = _safe_int(usage.get("output_tokens") or usage.get("completion_tokens"))
    total_tokens = _safe_int(usage.get("total_tokens"))

    if total_tokens <= 0:
        total_tokens = input_tokens + output_tokens
    elif total_tokens < (input_tokens + output_tokens):
        total_tokens = input_tokens + output_tokens

    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
    }


def _extract_usage_from_generation_list(generations: Any) -> dict[str, Any] | None:
    """Extract usage payload from LangChain LLMResult.generations."""
    if not isinstance(generations, list):
        return None

    for generation_group in generations:
        if not isinstance(generation_group, list):
            continue
        for generation in generation_group:
            message = getattr(generation, "message", None)
            if message is None:
                continue

            usage_metadata = getattr(message, "usage_metadata", None)
            if isinstance(usage_metadata, dict):
                return usage_metadata

            response_metadata = getattr(message, "response_metadata", None)
            if isinstance(response_metadata, dict):
                usage = response_metadata.get("usage") or response_metadata.get("token_usage")
                if isinstance(usage, dict):
                    return usage

    return None


class UsageTrackingCallback(BaseCallbackHandler):
    """Collect token usage for llm calls not routed through A1._invoke_llm."""

    def on_llm_end(self, response, **kwargs):
        usage_payload = None

        llm_output = getattr(response, "llm_output", None)
        if isinstance(llm_output, dict):
            usage_payload = llm_output.get("token_usage") or llm_output.get("usage")

        if usage_payload is None:
            usage_payload = _extract_usage_from_generation_list(getattr(response, "generations", None))

        usage = _normalize_usage(usage_payload)
        if usage["total_tokens"] <= 0:
            return

        collector = get_active_usage_collector()
        if collector is None:
            return

        record = getattr(collector, "record_usage_from_callback", None)
        if callable(record):
            record(usage=usage, stage="annotation", source="callback")
