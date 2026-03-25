"""Analyzer orchestration: preprocess log JSON -> Claude -> APPROVE/REJECT."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from config import Config

from .claude_client import ClaudeClient, ClaudeClientError
from .preprocessor import preprocess_log_payload


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fail_safe_decision(reason: str) -> Dict[str, Any]:
    decision = "REJECT" if Config.FAILSAFE_DECISION not in {"APPROVE", "REJECT"} else Config.FAILSAFE_DECISION
    return {
        "decision": decision,
        "reason": reason,
        "source": "fail_safe",
    }


def analyze_monitoring_payload(
    payload: Dict[str, Any],
    *,
    force_llm: bool = False,
    llm_enabled_override: bool | None = None,
) -> Dict[str, Any]:
    max_chars = int(
        getattr(
            Config,
            "LLM_MAX_LOG_JSON_CHARS",
            getattr(Config, "LLM_MAX_EVIDENCE_CHARS", 12000),
        )
    )
    preprocessed = preprocess_log_payload(payload, max_chars=max_chars)         # 전처리한 데이터(문자열 길이 제한, 민감 정보 마스킹 등)

    if llm_enabled_override is None:
        llm_enabled = bool(Config.LLM_VERIFICATION_ENABLED) or bool(force_llm)
    else:
        llm_enabled = bool(llm_enabled_override) or bool(force_llm)
    has_api_key = bool(getattr(Config, "ANTHROPIC_API_KEY", "").strip())

    if llm_enabled and has_api_key:
        try:
            client = ClaudeClient(
                api_key=Config.ANTHROPIC_API_KEY,
                model=Config.LLM_MODEL,
                timeout_sec=float(getattr(Config, "LLM_TIMEOUT_SEC", 10.0)),
            )
            llm_result = client.analyze_log(log_json=preprocessed["log_json"])
            decision = {
                "decision": llm_result["decision"],
                "reason": llm_result["reason"],
                "source": "claude",
                "raw_model_output": llm_result.get("raw_output", ""),
            }
        except ClaudeClientError as exc:
            decision = _fail_safe_decision(f"Claude 호출 실패로 Fail-safe 처리: {exc}")
    else:
        if not llm_enabled:
            decision = _fail_safe_decision("LLM_VERIFY=false 상태이므로 Fail-safe 처리")
        else:
            decision = _fail_safe_decision("ANTHROPIC_API_KEY 누락으로 Fail-safe 처리")

    return {
        "decision": decision["decision"],
        "reason": decision["reason"],
        "source": decision["source"],
        "analyzed_at": _utc_now_iso(),
        "input_summary": {
            "truncated": bool(preprocessed.get("truncated", False)),
            "log_json_length": int(preprocessed.get("log_json_length", 0) or 0),
        },
        "preprocessed_input": preprocessed,
        "raw_model_output": decision.get("raw_model_output", ""),
    }
