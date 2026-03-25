"""Claude API client for OTA log APPROVE/REJECT decisioning."""

from __future__ import annotations

from typing import Any, Dict, List

import requests

SECURITY_ANALYSIS_PROMPT_KO = """너는 차량 OTA 업데이트 보안 분석 에이전트이다.

아래에 제공되는 OTA 업데이트 로그(JSON)를 분석하여 보안 이상징후를 탐지하라.

## 판단 기준

다음 항목 중 하나라도 해당되면 REJECT 판정을 내려라:

1. 버전 다운그레이드: new_installed_version이 current_active_version보다 낮은 경우
2. 파일 사이즈 이상: firmware_file_size_bytes와 expected_file_size_bytes 간 차이가 10% 이상인 경우
3. 비정상 배포 경로: update_trigger_server 또는 build_server_info가 알려진 정상 서버 목록에 없는 경우
4. 비정상 MQTT 패턴: 짧은 시간 내 동일 명령 반복 수신 (10분 이내 3회 이상)
5. RAUC 설치 과정 이상: exit code가 0이 아니거나, 로그에 error/warning이 다수 포함된 경우
6. 시스템 리소스 이상: CPU 또는 메모리 사용률이 비정상적으로 높은 상태 (90% 이상)에서 업데이트 수행
7. 복합 이상 패턴: 개별 항목은 정상 범위이나, 여러 항목을 종합했을 때 의심스러운 패턴
8. 1~7 항목 외에 로그 분석을 통해 발견된 보안 이상징후

## 출력 형식

반드시 첫 번째 줄에 판정 결과만 출력하라: APPROVE 또는 REJECT
두 번째 줄부터 판단 근거를 간결하게 설명하라.

예시:
REJECT
버전 다운그레이드 탐지: 현재 활성 버전 1.0.0에서 0.8.0으로 다운그레이드 시도. 롤백 공격 가능성이 있음.
"""


class ClaudeClientError(RuntimeError):
    """Raised when Claude API request or response validation fails."""


class ClaudeClient:
    API_URL = "https://api.anthropic.com/v1/messages"

    def __init__(self, *, api_key: str, model: str, timeout_sec: float = 10.0):
        self.api_key = str(api_key or "").strip()
        self.model = str(model or "").strip()
        self.timeout_sec = float(timeout_sec)
        if not self.api_key:
            raise ClaudeClientError("ANTHROPIC_API_KEY is empty")
        if not self.model:
            raise ClaudeClientError("LLM model is empty")

    @staticmethod
    def _extract_text(response_json: Dict[str, Any]) -> str:            # Claude API 응답에서 텍스트 콘텐츠를 추출
        content = response_json.get("content", [])
        if not isinstance(content, list):
            raise ClaudeClientError("Invalid Claude response content")

        chunks: List[str] = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                chunks.append(str(block.get("text") or ""))

        text = "\n".join(chunks).strip()
        if not text:
            raise ClaudeClientError("Claude response did not contain text")
        return text

    @staticmethod
    def _parse_line_based_decision(text: str) -> Dict[str, Any]:            # LLM 출력에서 첫 줄을 APPROVE/REJECT로 해석하고, 나머지 줄을 판단 근거로 추출
        lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
        if not lines:
            raise ClaudeClientError("LLM output is empty")

        head = lines[0].upper()
        if head not in {"APPROVE", "REJECT"}:
            if "REJECT" in head:
                decision = "REJECT"
            elif "APPROVE" in head:
                decision = "APPROVE"
            else:
                raise ClaudeClientError(
                    "First line must be APPROVE or REJECT. "
                    f"Received: {lines[0]}"
                )
        else:
            decision = head

        reason = "\n".join(lines[1:]).strip()
        if not reason:
            reason = "판단 근거가 응답에 포함되지 않았습니다."

        return {
            "decision": decision,
            "reason": reason,
            "raw_output": text,
        }

    def analyze_log(self, *, log_json: str) -> Dict[str, Any]:
        user_prompt = (
            f"{SECURITY_ANALYSIS_PROMPT_KO}\n\n"
            "[OTA 업데이트 로그(JSON)]\n"
            f"{log_json}"
        )

        body = {
            "model": self.model,
            "max_tokens": 600,
            "temperature": 0,
            "system": "출력 형식을 반드시 지켜라. 첫 줄은 APPROVE 또는 REJECT만 허용된다.",
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_prompt}],
                }
            ],
        }

        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

        try:
            resp = requests.post(       # Claude API에 POST 요청을 보내 로그 분석 결과를 받는다
                self.API_URL,
                headers=headers,
                json=body,
                timeout=self.timeout_sec,
            )
        except Exception as exc:
            raise ClaudeClientError(f"Claude request error: {exc}") from exc

        if resp.status_code < 200 or resp.status_code >= 300:
            raise ClaudeClientError(
                f"Claude API failed: status={resp.status_code} body={(resp.text or '')[:300]}"
            )

        try:
            response_json = resp.json()
        except Exception as exc:
            raise ClaudeClientError(f"Invalid Claude JSON response: {exc}") from exc

        text = self._extract_text(response_json)
        return self._parse_line_based_decision(text)
