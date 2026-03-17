"""
LLM 2차 검증 요청 클라이언트 모듈
수집된 OTA 로그 JSON을 서버에 전송하고 검증 결과를 수신한다.
"""
import json
import logging
from typing import Dict

import requests

logger = logging.getLogger(__name__)

# Fail-safe 기본 결과: 모든 예외 상황에서 REJECT
_FAILSAFE_RESULT = {
    "decision": "REJECT",
    "reason": "Fail-safe: 서버 검증 불가, 업데이트 거부",
}


def request_llm_verification(
    server_url: str,
    ota_log: Dict,
    timeout: int = 30,
) -> Dict:
    """
    서버의 /api/ota/verify 엔드포인트에 OTA 로그를 전송하고 검증 결과를 반환한다.

    Args:
        server_url: OTA 서버 기본 URL (예: http://192.168.1.100:8080)
        ota_log: OTALogCollector.build_verification_log()로 생성한 JSON
        timeout: 요청 타임아웃 (초). LLM API 응답 시간을 감안한 값.

    Returns:
        {"decision": "APPROVE" or "REJECT", "reason": "..."}
    """
    endpoint = f"{server_url.rstrip('/')}/api/ota/verify"

    try:
        response = requests.post(
            endpoint,
            json=ota_log,
            timeout=timeout,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        data = response.json()

        decision = data.get("decision", "REJECT").upper()
        reason = data.get("reason", "No reason provided")

        if decision not in ("APPROVE", "REJECT"):
            logger.warning(f"Unexpected decision from server: {decision}, treating as REJECT")
            decision = "REJECT"
            reason = f"비정상 서버 응답: {decision}. {reason}"

        logger.info(f"LLM verification result: {decision}")
        return {"decision": decision, "reason": reason}

    except requests.exceptions.Timeout:
        logger.error(f"LLM verification request timed out ({timeout}s)")
        return {
            "decision": "REJECT",
            "reason": f"서버 검증 요청 타임아웃 ({timeout}초). Fail-safe REJECT.",
        }

    except requests.exceptions.ConnectionError as e:
        logger.error(f"LLM verification connection failed: {e}")
        return {
            "decision": "REJECT",
            "reason": f"서버 연결 실패: {e}. Fail-safe REJECT.",
        }

    except requests.exceptions.HTTPError as e:
        logger.error(f"LLM verification HTTP error: {e}")
        # 서버가 500 에러와 함께 REJECT JSON을 반환할 수 있음
        try:
            data = e.response.json()
            return {
                "decision": data.get("decision", "REJECT"),
                "reason": data.get("reason", str(e)),
            }
        except Exception:
            return {
                "decision": "REJECT",
                "reason": f"서버 HTTP 오류: {e}. Fail-safe REJECT.",
            }

    except Exception as e:
        logger.error(f"LLM verification unexpected error: {e}")
        return {
            "decision": "REJECT",
            "reason": f"검증 요청 중 예외: {e}. Fail-safe REJECT.",
        }
