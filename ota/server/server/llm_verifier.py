"""
Minimal LLM verifier/storage helpers.

NOTE:
- This is intentionally lightweight so server/runtime endpoints work.
- It provides the interface expected by app.py:
  preprocess_log, call_llm_verification, save_verification_result,
  get_llm_log_path, _get_verification_db
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

from config import Config

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_llm_log_path() -> str:
    return str(getattr(Config, "LLM_LOG_PATH", "./logs/llm-verifier.log") or "./logs/llm-verifier.log")


def _get_verification_db_path() -> str:
    env_path = str(os.getenv("LLM_RESULTS_DB_PATH", "") or "").strip()
    if env_path:
        return env_path
    log_path = get_llm_log_path()
    base_dir = os.path.dirname(log_path) or "."
    return os.path.join(base_dir, "llm-verification.db")


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(str(path or "").strip())
    if parent and (not os.path.exists(parent)):
        os.makedirs(parent, exist_ok=True)


def _append_runtime_log(message: str) -> None:
    path = get_llm_log_path()
    _ensure_parent_dir(path)
    line = f"{_utc_now_iso()} {message}\n"
    try:
        with open(path, "a", encoding="utf-8") as fp:
            fp.write(line)
    except Exception as exc:
        logger.warning("Failed to append llm runtime log: %s", exc)


def _get_verification_db() -> sqlite3.Connection:
    db_path = _get_verification_db_path()
    _ensure_parent_dir(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS verification_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT,
            vehicle_id TEXT,
            ota_id TEXT,
            current_version TEXT,
            new_version TEXT,
            decision TEXT NOT NULL,
            reason TEXT NOT NULL,
            source TEXT,
            raw_response TEXT,
            ota_log_json TEXT,
            analyzed_at TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_verification_results_created_at
        ON verification_results(created_at DESC)
        """
    )
    conn.commit()
    return conn


def preprocess_log(ota_log: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(ota_log, dict):
        raise TypeError("ota_log must be JSON object")
    return ota_log


def _is_passed(text: Any) -> bool:
    value = str(text or "").strip().upper()
    return value in {"", "PASS", "PASSED", "OK", "SUCCESS"}


def call_llm_verification(preprocessed: Dict[str, Any], model: str | None = None) -> Dict[str, Any]:
    # [MIN-LLM] Simple deterministic fallback rule-set.
    fw = preprocessed.get("firmware_metadata") if isinstance(preprocessed.get("firmware_metadata"), dict) else {}
    process_log = preprocessed.get("process_log") if isinstance(preprocessed.get("process_log"), dict) else {}
    error_obj = preprocessed.get("error") if isinstance(preprocessed.get("error"), dict) else {}

    reasons = []
    if not _is_passed(fw.get("signature_verification")):
        reasons.append(f"signature_verification={fw.get('signature_verification')}")
    if not _is_passed(process_log.get("integrity_check_result")):
        reasons.append(f"integrity_check_result={process_log.get('integrity_check_result')}")
    exit_code = process_log.get("rauc_install_exit_code")
    if exit_code not in (None, "", 0, "0"):
        reasons.append(f"rauc_install_exit_code={exit_code}")
    if str(error_obj.get("code") or "").strip():
        reasons.append(f"error_code={error_obj.get('code')}")

    if reasons:
        decision = "REJECT"
        reason = "Fail-safe decision by minimal verifier: " + ", ".join(reasons)
    else:
        decision = "APPROVE"
        reason = "Minimal verifier approved: no explicit failure signals in payload."

    used_model = str(model or getattr(Config, "LLM_MODEL", "minimal-fallback")).strip() or "minimal-fallback"
    raw = f"{decision}\n{reason}\nmodel={used_model}"
    _append_runtime_log(f"verify decision={decision} model={used_model} reason={reason}")
    return {
        "decision": decision,
        "reason": reason,
        "raw_response": raw,
        "model": used_model,
    }


def _extract_vehicle_id(ota_log: Dict[str, Any]) -> str:
    if not isinstance(ota_log, dict):
        return "unknown"
    for candidate in (
        ota_log.get("vehicle_id"),
        ((ota_log.get("device_state") or {}).get("vehicle_id") if isinstance(ota_log.get("device_state"), dict) else None),
        ((ota_log.get("device") or {}).get("device_id") if isinstance(ota_log.get("device"), dict) else None),
    ):
        text = str(candidate or "").strip()
        if text:
            return text
    return "unknown"


def _extract_ota_id(ota_log: Dict[str, Any]) -> str:
    if not isinstance(ota_log, dict):
        return ""
    for candidate in (
        ota_log.get("ota_id"),
        ((ota_log.get("ota") or {}).get("ota_id") if isinstance(ota_log.get("ota"), dict) else None),
    ):
        text = str(candidate or "").strip()
        if text:
            return text
    return ""


def _extract_versions(ota_log: Dict[str, Any]) -> Tuple[str, str]:
    current_version = ""
    new_version = ""
    if not isinstance(ota_log, dict):
        return current_version, new_version

    fw = ota_log.get("firmware_metadata")
    if isinstance(fw, dict):
        current_version = str(
            fw.get("current_active_version")
            or fw.get("current_version")
            or fw.get("from_version")
            or ""
        ).strip()
        new_version = str(
            fw.get("new_installed_version")
            or fw.get("target_version")
            or fw.get("new_version")
            or ""
        ).strip()

    ota = ota_log.get("ota")
    if isinstance(ota, dict):
        if not current_version:
            current_version = str(ota.get("current_version") or "").strip()
        if not new_version:
            new_version = str(ota.get("target_version") or ota.get("new_version") or "").strip()

    return current_version, new_version


def save_verification_result(ota_log: Dict[str, Any], result: Dict[str, Any]) -> int:
    if not isinstance(ota_log, dict):
        ota_log = {}
    if not isinstance(result, dict):
        result = {}

    decision = str(result.get("decision") or "REJECT").strip().upper()
    if decision not in {"APPROVE", "REJECT"}:
        decision = "REJECT"

    reason = str(result.get("reason") or "No reason").strip() or "No reason"
    raw_response = str(result.get("raw_response") or result.get("raw_model_output") or "").strip()
    source = str(result.get("source") or "llm_verifier").strip() or "llm_verifier"

    current_version, new_version = _extract_versions(ota_log)
    vehicle_id = _extract_vehicle_id(ota_log)
    ota_id = _extract_ota_id(ota_log)
    analyzed_at = str(result.get("analyzed_at") or _utc_now_iso()).strip()
    created_at = _utc_now_iso()

    conn = _get_verification_db()
    try:
        cur = conn.execute(
            """
            INSERT INTO verification_results (
                request_id, vehicle_id, ota_id, current_version, new_version,
                decision, reason, source, raw_response, ota_log_json, analyzed_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(result.get("request_id") or "").strip(),
                vehicle_id,
                ota_id,
                current_version,
                new_version,
                decision,
                reason,
                source,
                raw_response,
                json.dumps(ota_log, ensure_ascii=False),
                analyzed_at,
                created_at,
            ),
        )
        conn.commit()
        row_id = int(cur.lastrowid or 0)
    finally:
        conn.close()

    _append_runtime_log(
        f"saved verification_result id={row_id} vehicle={vehicle_id} decision={decision} source={source}"
    )
    return row_id

