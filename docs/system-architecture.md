# OTA 시스템 아키텍처

설계 갱신: 2026-10-03

## 1단계 목표 시스템 아키텍처

기존 OTA 배포·설치 흐름을 유지하고, 서버의 Claude API 기반 2차 검증을 Ethernet으로 연결한 Jetson Orin Nano의 로컬 모델로 이전한다. 모델은 LLM으로 한정하지 않는다. Gateway가 설치 여부를 판단·실행하고, 서버는 배포와 이력 저장을 담당한다.

**설계 상태:** 아래는 합의된 목표 구조다. 현재 코드는 서버 `/api/ota/verify`를 통한 외부 LLM 검증을 사용한다. Jetson 호출, 결과 전달, 장애 시 보류·재시도는 아직 구현되지 않았다. 이 문서 변경은 실행 코드나 배포 설정을 바꾸지 않는다.

```text
+--------------------------------------------------------------------+
| OTA Server - Raspberry Pi                                          |
| Dashboard | OTA API | Firmware Storage | MQTT Broker | Databases   |
+--------------------------------------------------------------------+
       | [1] MQTT             ^ [2] MQTT          ^ [3][4] HTTPS
       | Notice / Command     | Register /       | GET firmware
       |                      | Request /        | POST logs, events,
       |                      | Status / Progress| verification records
       v                      |                  |
+--------------------------------------------------------------------+
| Gateway - Telechips D3-G                                           |
| OTA Client | Primary Verification | Installation Decision          |
| RAUC A/B Update | Boot Confirmation | OP-TEE Anti-rollback         |
+--------------------------------------------------------------------+
       | [5] Verification request                ^ [6] Result
       | Metadata / Logs / Device status         | Decision / Reason /
       |                                         | Model version
       |          Ethernet - Verification API    |
       |          (application protocol: TBD)    |
       v                                         |
+--------------------------------------------------------------------+
| Secondary Verifier - Jetson Orin Nano                              |
| Verification API | On-device Model                                 |
+--------------------------------------------------------------------+
```

화살표 [3]은 HTTPS GET **요청 방향**이다. 펌웨어 본문은 서버 → Gateway의 응답으로 전달된다. MQTT와 HTTPS는 대체 관계가 아니라 메시지 종류에 따라 역할을 나눈다. [5][6]은 요청·응답의 논리적 방향이며 Ethernet 자체가 API 프로토콜을 뜻하지 않는다.

## 통신별 역할

| 번호 | 방향 | 프로토콜 | 내용 / 기존 구현 경로 |
| --- | --- | --- | --- |
| 1 | 서버 → Gateway | MQTT | `ota/releases/announce` 업데이트 알림, `ota/{vehicle_id}/cmd` 설치 명령 |
| 2 | Gateway → 서버 | MQTT | `ota/vehicles/register` 장치 등록·업데이트 요청, `ota/{vehicle_id}/status` 상태, `ota/{vehicle_id}/progress` 진행률 |
| 3 | Gateway → 서버 요청 / 서버 → Gateway 응답 | HTTPS GET | 배포 URL의 `.raucb` 다운로드. 실제 저장소 또는 리다이렉트 구성은 배포 설정에 따름 |
| 4 | Gateway → 서버 | HTTPS POST | `/ingest` 이벤트, `/api/v1/client-logs` 상세 로그, `/api/ota/verify`의 `record_only` 검증 기록. 각 보고 기능은 설정에 따라 활성화됨 |
| 5 | Gateway → Jetson | Ethernet 위 검증 API, 프로토콜 미정 | 업데이트 메타데이터, 1차 검증 결과·로그, 장치 상태 |
| 6 | Jetson → Gateway | 같은 API의 응답 | 검증 판단·근거·모델 버전. 상세 응답 스키마는 후속 설계 |

서버 링크의 HTTPS는 목표 배포 기준이다. 현재 클라이언트의 URL과 TLS 설정은 실행 구성에 따르며, 문서 수정만으로 모든 HTTP 호출이 HTTPS로 전환되지는 않는다.

## 판단과 장애 처리

- Gateway는 기존 서명·무결성·버전 정책과 deterministic rule 검사를 유지한다. 1차 검증 실패를 모델의 승인으로 덮어쓰지 않는다.
- Jetson은 2차 검증 결과를 반환한다. RAUC 설치를 실행하거나 부팅 슬롯을 변경하는 주체는 Gateway다.
- 명시적 거부와 통신 장애를 구분한다. Jetson 무응답·연결 실패 시 설치를 **보류하고 재시도**하며, 모델 검증을 생략하고 설치하지 않는다. 재시도 간격·상한·운영자 개입 조건은 미정이다.
- Gateway가 Jetson 결과와 실제 처리 결과를 서버에 HTTPS로 기록한다. Jetson → 서버 직접 보고 경로는 현재 목표에 포함하지 않는다.
- 기존 서버의 `record_only` 기능을 기록 경로로 활용하되, Jetson 결과·모델 버전의 보존과 표시를 위한 연동은 후속 구현이다. `record_only`는 검증 승인 API가 아니다.
- 모델의 입력·출력 형식, 조건부 승인 처리, Ethernet API의 인증·TLS·재전송 방지 정책은 후속 설계 대상이다. 물리적 직결만으로 신뢰할 수 있는 검증 결과가 되는 것은 아니다.

## 현재 구현과 전환 범위

| 항목 | 현재 구현 | 목표 |
| --- | --- | --- |
| 2차 검증 요청 | Gateway → OTA 서버 `/api/ota/verify` (`gate`) | Gateway → Jetson 검증 API |
| 모델 실행 | 서버가 외부 Claude API 호출 | Jetson에서 로컬 모델 추론 |
| 응답 처리 | `APPROVE`, `CONDITIONAL_APPROVE`, `REJECT`; 통신 오류도 실패 응답으로 정규화 | 모델 판단과 통신 실패를 구분하고 후자는 보류·재시도 |
| 결과 기록 | 서버 검증 기록 및 `record_only` 저장 | Gateway가 Jetson 검증 결과를 서버에 기록 |
| 설치·부팅 | RAUC A/B, 부트로더 슬롯 선택, mark-good | 기존 역할 유지 |
| 버전 방지 | OP-TEE AVB TA/RPMB를 사용하는 설계 | 기존 보안 기준 유지; 런타임 정상 동작 확인 필요 |

Gateway 코드의 연결 지점은 `services/ota-backend/app/main.py`, `llm_verify_client.py`, `log_collector.py`, `rule_engine.py`, `ota_logic.py`다. 서버 연결 지점은 `ota/server/server/app.py`, `llm_verifier.py`와 대시보드다. 두 저장소에 나뉜 경로이며 이번 작업은 문서만 갱신한다.

OP-TEE 표기는 설계상 역할이다. 2026-10-03 보드 점검에서 TA 세션 통신 오류가 관찰되어 런타임 검증이 남아 있다. 모델 이전으로 이 오류가 해결되지는 않는다.

## 다음 문서 단계

2단계는 D3-G Gateway 내부를 확대하여 UI/API, OTA 처리, 검증, Normal/Secure World, RAUC·부팅·저장장치 경계를 상세히 그린다. Jetson 내부 모델 구조는 제외하고 외부 검증기로만 표시한다. 이번 문서에는 1단계만 반영한다.
