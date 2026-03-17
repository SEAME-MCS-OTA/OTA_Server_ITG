"""
OTA LLM 2차 검증 모듈
Claude API를 호출하여 OTA 업데이트 로그의 보안 이상징후를 탐지한다.
"""
import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import Optional

import anthropic

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 시스템 프롬프트
# ──────────────────────────────────────────────

SYSTEM_PROMPT = """너는 차량 OTA(Over-the-Air) 업데이트 보안 분석 에이전트이다.

아래에 제공되는 OTA 업데이트 로그(JSON)를 분석하여 보안 이상징후를 탐지하라.

## 판단 기준

다음 항목 중 하나라도 해당되면 REJECT 판정을 내려라:

1. 버전 다운그레이드: new_installed_version이 current_active_version보다 낮은 경우
2. 파일 사이즈 이상: firmware_file_size_bytes와 expected_file_size_bytes 간 차이가 10% 이상인 경우
3. 비정상 배포 경로: update_trigger_server 또는 build_server_info가 알려진 정상 서버 목록에 없는 경우
4. 비정상 업데이트 빈도: recent_update_history를 기반으로 최근 24시간 내 3회 이상 업데이트 시도가 있는 경우
5. 비정상 MQTT 패턴: 짧은 시간 내 동일 명령 반복 수신 (10분 이내 3회 이상)
6. RAUC 설치 과정 이상: exit code가 0이 아니거나, 로그에 error/warning이 다수 포함된 경우
7. 시스템 리소스 이상: CPU 또는 메모리 사용률이 비정상적으로 높은 상태 (90% 이상)에서 업데이트 수행
8. 복합 이상 패턴: 개별 항목은 정상 범위이나, 여러 항목을 종합했을 때 의심스러운 패턴

## 출력 형식

반드시 첫 번째 줄에 판정 결과만 출력하라: APPROVE 또는 REJECT
두 번째 줄부터 판단 근거를 간결하게 설명하라.

예시:
REJECT
버전 다운그레이드 탐지: 현재 활성 버전 1.0.0에서 0.8.0으로 다운그레이드 시도. 롤백 공격 가능성이 있음."""


# ──────────────────────────────────────────────
# Claude API 호출
# ──────────────────────────────────────────────

def call_llm_verification(ota_log_json: dict, model: str = "claude-sonnet-4-20250514") -> dict:
    """
    OTA 로그를 Claude API에 전송하여 검증 결과를 받는다.

    Args:
        ota_log_json: 클라이언트에서 수신한 OTA 로그 JSON
        model: 사용할 Claude 모델 ID

    Returns:
        {
            "decision": "APPROVE" or "REJECT",
            "reason": "판단 근거",
            "raw_response": "LLM 원본 응답"
        }
    """
    try:
        client = anthropic.Anthropic()

        response = client.messages.create(
            model=model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": (
                        "다음 OTA 업데이트 로그를 분석하여 보안 이상징후를 판단하라:\n\n"
                        f"{json.dumps(ota_log_json, indent=2, ensure_ascii=False)}"
                    )
                }
            ]
        )

        result_text = response.content[0].text
        lines = result_text.strip().split('\n')
        decision = lines[0].strip().upper()
        reason = '\n'.join(lines[1:]).strip()

        # APPROVE/REJECT 외의 응답은 안전하게 REJECT 처리
        if decision not in ("APPROVE", "REJECT"):
            decision = "REJECT"
            reason = f"LLM 응답 파싱 불가. 원본: {result_text[:200]}"

        return {
            "decision": decision,
            "reason": reason,
            "raw_response": result_text,
        }

    except anthropic.APITimeoutError:
        logger.error("Claude API timeout")
        return {
            "decision": "REJECT",
            "reason": "Claude API 타임아웃. Fail-safe 정책에 따라 업데이트 거부.",
            "raw_response": None,
        }

    except Exception as e:
        logger.error(f"LLM verification error: {e}")
        return {
            "decision": "REJECT",
            "reason": f"LLM 검증 중 예외 발생: {str(e)}. Fail-safe 정책에 따라 업데이트 거부.",
            "raw_response": None,
        }


# ──────────────────────────────────────────────
# 로그 전처리
# ──────────────────────────────────────────────

def preprocess_log(ota_log: dict) -> dict:
    """
    LLM에 전송하기 전 로그를 전처리한다.
    - RAUC 로그가 너무 길면 에러/워닝만 추출
    - 토큰 절약을 위해 불필요 필드 축약
    """
    processed = json.loads(json.dumps(ota_log))  # deep copy

    # RAUC 로그 요약
    rauc_log = processed.get("process_log", {}).get("rauc_install_log_summary", "")
    if len(rauc_log) > 2000:
        lines = rauc_log.split('\n')
        important_lines = [
            l for l in lines
            if any(kw in l.lower() for kw in ['error', 'warning', 'fail', 'denied'])
        ]
        if important_lines:
            processed["process_log"]["rauc_install_log_summary"] = '\n'.join(important_lines)
        else:
            processed["process_log"]["rauc_install_log_summary"] = (
                "Installation completed without notable errors. (Log truncated for brevity)"
            )

    return processed


# ──────────────────────────────────────────────
# 검증 결과 DB 저장 (SQLite)
# ──────────────────────────────────────────────

_VERIFICATION_DB_PATH = os.getenv(
    "LLM_VERIFICATION_DB",
    os.path.join(os.path.dirname(__file__), "llm_verification.db"),
)


def _get_verification_db() -> sqlite3.Connection:
    """SQLite 연결을 반환하고, 테이블이 없으면 생성한다."""
    conn = sqlite3.connect(_VERIFICATION_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS verification_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id TEXT,
            current_version TEXT,
            new_version TEXT,
            decision TEXT NOT NULL,
            reason TEXT,
            raw_response TEXT,
            ota_log_json TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def save_verification_result(ota_log: dict, result: dict):
    """검증 결과를 SQLite DB에 저장한다."""
    try:
        conn = _get_verification_db()
        vehicle_id = ota_log.get("device_state", {}).get("vehicle_id", "unknown")
        current_version = ota_log.get("firmware_metadata", {}).get("current_active_version", "")
        new_version = ota_log.get("firmware_metadata", {}).get("new_installed_version", "")

        conn.execute(
            """INSERT INTO verification_results
               (vehicle_id, current_version, new_version, decision, reason, raw_response, ota_log_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                vehicle_id,
                current_version,
                new_version,
                result["decision"],
                result["reason"],
                result.get("raw_response"),
                json.dumps(ota_log, ensure_ascii=False),
                datetime.utcnow().isoformat() + "Z",
            ),
        )
        conn.commit()
        conn.close()
        logger.info(f"Verification result saved: vehicle={vehicle_id} decision={result['decision']}")
    except Exception as e:
        logger.error(f"Failed to save verification result: {e}")
