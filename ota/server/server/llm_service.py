"""Core request normalization and LLM decision generation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4

from llm_pipeline import analyze_monitoring_payload


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _extract_log(payload: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(payload.get("log"), dict):
        return payload["log"]
    if isinstance(payload.get("payload"), dict):
        nested = payload["payload"]
        if isinstance(nested.get("log"), dict):
            return nested["log"]
        if isinstance(nested.get("ota_log"), dict):
            return nested["ota_log"]
        return nested
    if isinstance(payload.get("ota_log"), dict):
        return payload["ota_log"]
    return payload


def _extract_vehicle_id(payload: Dict[str, Any], log: Dict[str, Any]) -> str:
    device = log.get("device")
    payload_device_state = payload.get("device_state")
    payload_ota_log = payload.get("ota_log")
    payload_payload = payload.get("payload")
    log_device_state = log.get("device_state")

    candidates = (
        payload.get("vehicle_id"),
        log.get("vehicle_id"),
        payload.get("device_id"),
        log.get("device_id"),
        device.get("device_id") if isinstance(device, dict) else None,
        device.get("vehicle_id") if isinstance(device, dict) else None,
        payload_device_state.get("vehicle_id") if isinstance(payload_device_state, dict) else None,
        (payload_ota_log.get("vehicle_id") if isinstance(payload_ota_log, dict) else None),
        (
            (payload_ota_log.get("device_state") or {}).get("vehicle_id")
            if isinstance(payload_ota_log, dict)
            else None
        ),
        (payload_payload.get("vehicle_id") if isinstance(payload_payload, dict) else None),
        (
            (payload_payload.get("device_state") or {}).get("vehicle_id")
            if isinstance(payload_payload, dict)
            else None
        ),
        (
            ((payload_payload.get("ota_log") or {}).get("device_state") or {}).get("vehicle_id")
            if isinstance(payload_payload, dict)
            else None
        ),
        log_device_state.get("vehicle_id") if isinstance(log_device_state, dict) else None,
    )
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            return text
    return "unknown"


def _extract_ota_id(payload: Dict[str, Any], log: Dict[str, Any]) -> str:
    if payload.get("ota_id"):
        return str(payload["ota_id"])
    if log.get("ota_id"):
        return str(log["ota_id"])
    ota = log.get("ota")
    if isinstance(ota, dict) and ota.get("ota_id"):
        return str(ota["ota_id"])
    return ""


def _extract_versions(log: Dict[str, Any]) -> Dict[str, str]:
    current_version = ""
    new_version = ""
    target_version = ""
    if not isinstance(log, dict):
        return {
            "current_version": current_version,
            "new_version": new_version,
            "target_version": target_version,
        }

    fw = log.get("firmware_metadata")
    if isinstance(fw, dict):
        current_version = str(
            fw.get("current_active_version")
            or fw.get("current_version")
            or fw.get("from_version")
            or ""
        ).strip()
        new_version = str(
            fw.get("new_installed_version")
            or fw.get("new_version")
            or fw.get("target_version")
            or fw.get("requested_target_version")
            or ""
        ).strip()
        target_version = str(
            fw.get("requested_target_version")
            or fw.get("target_version")
            or fw.get("new_installed_version")
            or fw.get("new_version")
            or ""
        ).strip()

    ota = log.get("ota")
    if isinstance(ota, dict):
        if not current_version:
            current_version = str(ota.get("current_version") or "").strip()
        if not target_version:
            target_version = str(ota.get("target_version") or ota.get("new_version") or "").strip()
        if not new_version:
            new_version = str(ota.get("new_version") or ota.get("target_version") or "").strip()

    return {
        "current_version": current_version,
        "new_version": new_version,
        "target_version": target_version or new_version,
    }


def normalize_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise TypeError("payload must be JSON object")

    log = _extract_log(payload)
    if not isinstance(log, dict):
        raise TypeError("log must be JSON object")

    request_id_raw = payload.get("request_id")
    request_id = str(request_id_raw).strip() if request_id_raw else str(uuid4())

    return {
        "request_id": request_id,
        "vehicle_id": _extract_vehicle_id(payload, log),
        "ota_id": _extract_ota_id(payload, log),
        "response_topic": str(payload.get("response_topic") or "").strip(),
        "force_llm": _to_bool(payload.get("force_llm"), False),
        "publish_mqtt": _to_bool(payload.get("publish_mqtt"), True),
        "include_input": _to_bool(payload.get("include_input"), False),
        "received_at": _utc_now_iso(),
        "log": log,
    }


def analyze_request(
    req: Dict[str, Any],
    *,
    transport: str,
    llm_enabled_override: bool | None = None,
) -> Dict[str, Any]:
    analysis = analyze_monitoring_payload(
        req["log"],
        force_llm=bool(req.get("force_llm", False)),
        llm_enabled_override=llm_enabled_override,
    )
    versions = _extract_versions(req.get("log", {}))

    result = {
        "request_id": req.get("request_id", ""),
        "vehicle_id": req.get("vehicle_id", "unknown"),
        "ota_id": req.get("ota_id", ""),
        "current_version": versions.get("current_version", ""),
        "new_version": versions.get("new_version", ""),
        "target_version": versions.get("target_version", ""),
        "decision": analysis.get("decision", "REJECT"),
        "reason": analysis.get("reason", "No reason"),
        "source": analysis.get("source", "unknown"),
        "analyzed_at": analysis.get("analyzed_at", _utc_now_iso()),
        "input_summary": analysis.get("input_summary", {}),
        "raw_model_output": analysis.get("raw_model_output", ""),
        "trace": {
            "transport": transport,
            "received_at": req.get("received_at", ""),
            "sent_at": _utc_now_iso(),
        },
    }

    preprocessed_input = analysis.get("preprocessed_input")
    if req.get("include_input") and isinstance(preprocessed_input, dict):
        result["preprocessed_input"] = preprocessed_input

    return result
