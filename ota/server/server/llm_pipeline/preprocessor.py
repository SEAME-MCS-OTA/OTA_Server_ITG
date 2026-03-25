"""Utilities to preprocess OTA log JSON for LLM analysis."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List

_IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_TOKEN_RE = re.compile(r"(bearer\s+)?[A-Za-z0-9_\-]{20,}", re.IGNORECASE)


def _mask_sensitive(text: str) -> str:
    masked = _IP_RE.sub("[REDACTED_IP]", text)
    masked = _TOKEN_RE.sub("[REDACTED_TOKEN]", masked)
    return masked


def _sanitize_node(node: Any, *, max_list_items: int, max_str_len: int) -> Any:
    if isinstance(node, dict):
        out: Dict[str, Any] = {}
        for key, value in node.items():
            out[str(key)] = _sanitize_node(
                value,
                max_list_items=max_list_items,
                max_str_len=max_str_len,
            )
        return out

    if isinstance(node, list):
        trimmed: List[Any] = [
            _sanitize_node(item, max_list_items=max_list_items, max_str_len=max_str_len)
            for item in node[:max_list_items]
        ]
        omitted = len(node) - len(trimmed)
        if omitted > 0:
            trimmed.append({"_omitted_items": omitted})
        return trimmed

    if isinstance(node, str):
        text = _mask_sensitive(node)
        if len(text) <= max_str_len:
            return text
        return text[:max_str_len] + "...[TRUNCATED]"

    return node


def _make_compact_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    compact = _sanitize_node(payload, max_list_items=40, max_str_len=400)
    if not isinstance(compact, dict):
        compact = {"log": compact}
    return compact


def preprocess_log_payload(payload: Dict[str, Any], *, max_chars: int = 12000) -> Dict[str, Any]:
    """
    Prepare log-only JSON string for LLM prompt.

    Returns:
      {
        "log_json": "{...}",
        "truncated": bool,
        "log_json_length": int
      }
    """
    if not isinstance(payload, dict):
        raise TypeError("payload must be dict")

    max_chars = max(1000, int(max_chars))       # 최대 문자열(1000 - 12000)

    sanitized = _sanitize_node(payload, max_list_items=200, max_str_len=1500)
    if not isinstance(sanitized, dict):
        sanitized = {"log": sanitized}

    log_json = json.dumps(sanitized, ensure_ascii=False, sort_keys=True)
    truncated = False

    if len(log_json) > max_chars:
        truncated = True
        compact = _make_compact_payload(payload)
        compact["_llm_input_truncated"] = True
        compact["_llm_input_note"] = (
            f"Original JSON length={len(log_json)} chars. Compacted before prompt delivery."
        )
        log_json = json.dumps(compact, ensure_ascii=False, sort_keys=True)

    if len(log_json) > max_chars:
        truncated = True
        excerpt_len = max(300, max_chars - 260)
        log_json = json.dumps(
            {
                "_llm_input_truncated": True,
                "_llm_input_note": (
                    "Compacted JSON still exceeds prompt budget. "
                    f"Only first {excerpt_len} characters are included."
                ),
                "log_json_excerpt": log_json[:excerpt_len],
            },
            ensure_ascii=False,
            sort_keys=True,
        )

    return {
        "log_json": log_json,
        "truncated": truncated,
        "log_json_length": len(log_json),
    }
