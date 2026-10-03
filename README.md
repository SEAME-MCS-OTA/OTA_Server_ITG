# OTA_Server_ITG

라즈베리파이에서 OTA 서버를 운영하기 위한 서버 전용 저장소입니다.

이 저장소는 다음 구성을 포함합니다.

- OTA API 서버
- OTA 대시보드
- PostgreSQL
- Mosquitto MQTT broker
- Monitoring backend
- Monitoring MySQL

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

### 통신별 역할

| 번호 | 방향 | 프로토콜 | 내용 / 기존 구현 경로 |
| --- | --- | --- | --- |
| 1 | 서버 → Gateway | MQTT | `ota/releases/announce` 업데이트 알림, `ota/{vehicle_id}/cmd` 설치 명령 |
| 2 | Gateway → 서버 | MQTT | `ota/vehicles/register` 장치 등록·업데이트 요청, `ota/{vehicle_id}/status` 상태, `ota/{vehicle_id}/progress` 진행률 |
| 3 | Gateway → 서버 요청 / 서버 → Gateway 응답 | HTTPS GET | 배포 URL의 `.raucb` 다운로드. 실제 저장소 또는 리다이렉트 구성은 배포 설정에 따름 |
| 4 | Gateway → 서버 | HTTPS POST | `/ingest` 이벤트, `/api/v1/client-logs` 상세 로그, `/api/ota/verify`의 `record_only` 검증 기록. 각 보고 기능은 설정에 따라 활성화됨 |
| 5 | Gateway → Jetson | Ethernet 위 검증 API, 프로토콜 미정 | 업데이트 메타데이터, 1차 검증 결과·로그, 장치 상태 |
| 6 | Jetson → Gateway | 같은 API의 응답 | 검증 판단·근거·모델 버전. 상세 응답 스키마는 후속 설계 |

서버 링크의 HTTPS는 목표 배포 기준이다. 현재 클라이언트의 URL과 TLS 설정은 실행 구성에 따르며, 문서 수정만으로 모든 HTTP 호출이 HTTPS로 전환되지는 않는다.

### 판단과 장애 처리

- Gateway는 기존 서명·무결성·버전 정책과 deterministic rule 검사를 유지한다. 1차 검증 실패를 모델의 승인으로 덮어쓰지 않는다.
- Jetson은 2차 검증 결과를 반환한다. RAUC 설치를 실행하거나 부팅 슬롯을 변경하는 주체는 Gateway다.
- 명시적 거부와 통신 장애를 구분한다. Jetson 무응답·연결 실패 시 설치를 **보류하고 재시도**하며, 모델 검증을 생략하고 설치하지 않는다. 재시도 간격·상한·운영자 개입 조건은 미정이다.
- Gateway가 Jetson 결과와 실제 처리 결과를 서버에 HTTPS로 기록한다. Jetson → 서버 직접 보고 경로는 현재 목표에 포함하지 않는다.
- 기존 서버의 `record_only` 기능을 기록 경로로 활용하되, Jetson 결과·모델 버전의 보존과 표시를 위한 연동은 후속 구현이다. `record_only`는 검증 승인 API가 아니다.
- 모델의 입력·출력 형식, 조건부 승인 처리, Ethernet API의 인증·TLS·재전송 방지 정책은 후속 설계 대상이다. 물리적 직결만으로 신뢰할 수 있는 검증 결과가 되는 것은 아니다.

## 포함 서비스

- `ota_gh_postgres`: OTA 메타데이터 저장 DB
- `ota_gh_mosquitto`: MQTT broker
- `ota_gh_server`: OTA API 서버
- `ota_gh_dashboard`: OTA 대시보드
- `ota_gh_monitoring_mysql`: monitoring DB
- `ota_gh_monitoring_backend`: monitoring API

## 기본 포트

- API: `8080`
- Dashboard: `3001`
- Monitoring API: `4000`
- MQTT TCP: `1883`
- MQTT WebSocket: `9001`

## 요구사항

- Docker
- Docker Compose v2 (`docker compose`)
- 위 포트들이 호스트에서 사용 가능해야 함

## 실행

```bash
cd /path/to/OTA_Server_ITG
./ota/tools/ota-stack-up.sh
```

직접 compose로 실행해도 됩니다.

```bash
cd /path/to/OTA_Server_ITG
docker compose -f docker-compose.ota-stack.yml up -d
```

## 중지

```bash
cd /path/to/OTA_Server_ITG
./ota/tools/ota-stack-down.sh
```

또는:

```bash
cd /path/to/OTA_Server_ITG
docker compose -f docker-compose.ota-stack.yml down
```

## 기본 주소

- API: `http://localhost:8080`
- Dashboard: `http://localhost:3001`
- Monitoring API: `http://localhost:4000`
- MQTT TCP: `localhost:1883`
- MQTT WS: `localhost:9001`

대시보드는 현재 브라우저 접속 주소의 `/api/`, `/health`, `/monitoring/`으로 데이터를 요청합니다. 대시보드 Nginx가 각 백엔드로 전달하므로 서버 IP가 바뀌어도 프런트엔드를 다시 빌드할 필요가 없습니다. 외부 HTTPS 프록시를 사용하는 경우에도 동일 경로를 전달해야 합니다. 기존 `VITE_APP_API_URL` 및 `VITE_VLM_API_URL` 빌드 변수는 사용하지 않습니다.

## 기본 점검

```bash
curl -sS http://localhost:8080/health
curl -sS http://localhost:4000/health
docker compose -f docker-compose.ota-stack.yml ps
```

## 주요 API (현재 구현)

- `GET /health`
- `GET /api/v1/vehicles`
- `GET /api/v1/firmware`
- `POST /api/v1/admin/firmware`
- `POST /api/v1/admin/trigger-update`
- `POST /api/ota/verify`
- `POST /api/v1/llm/analyze` (OTA_LLM 통합 분석 API)
- `GET /llm/dashboard` (OTA_LLM 통합 대시보드)
- `GET /firmware/<filename>`
- `GET /stats/summary`

## 주요 환경 변수 (현재 구현)

- `OTA_GH_FIRMWARE_BASE_URL`: 디바이스가 접근 가능한 펌웨어 base URL
- `OTA_GH_SERVER_PORT`, `OTA_GH_DASHBOARD_PORT`: API와 dashboard 포트
- `OTA_GH_MONITORING_PORT`: monitoring backend 포트
- `OTA_GH_MQTT_PORT`, `OTA_GH_MQTT_WS_PORT`: MQTT 포트
- `OTA_GH_REQUIRE_RECENT_VEHICLE`: 최근 heartbeat 차량만 trigger 허용
- `OTA_GH_VEHICLE_ONLINE_WINDOW_SEC`: 온라인 판정 윈도우
- `OTA_GH_LOCAL_PROBE_ENABLED`: local probe fallback 사용 여부
- `OTA_GH_LOCAL_DEVICE_MAP`: local probe 또는 HTTP fallback용 장치 매핑
- `OTA_GH_MQTT_COMMAND_ONLY`: MQTT-only trigger 정책
- `OTA_GH_LLM_VERIFY`, `OTA_GH_LLM_MODEL`: 현재 서버의 외부 LLM 검증 설정. Jetson 목표 구조 전환 전까지 적용되며, 이 값을 끄는 것만으로 Jetson 검증이 연결되지는 않음
- `OTA_GH_MONITORING_INGEST_URL`: monitoring ingest 연동 주소

`ota-stack-up.sh`는 `OTA_GH_FIRMWARE_BASE_URL`이 비어 있으면 호스트 IP를 계산해 `.env`에 기록합니다.

## 펌웨어 업로드 예시

```bash
curl -sS -X POST http://localhost:8080/api/v1/admin/firmware \
  -F "file=@/path/to/update.raucb" \
  -F "version=1.0.0" \
  -F "release_notes=Initial release" \
  -F "overwrite=true"
```

## 운영 메모

- API 서버는 Flask dev server가 아니라 `gunicorn gthread`로 실행됩니다.
- MQTT subscription loop를 중복 실행하지 않도록 `1 worker + threads` 구성입니다.
- local probe는 기본적으로 `off`입니다. MQTT heartbeat 기반 운용에서는 켜지지 않아도 됩니다.
- dashboard는 현재 활성 탭만 주기적으로 갱신합니다.
- dashboard 자동 갱신 주기:
  - operations: `10s`
  - monitoring: `30s`
  - llm: `30s`
- 브라우저 탭이 백그라운드면 자동 폴링을 멈춥니다.

## 디렉터리 구조

```text
OTA_Server_ITG/
├── docker-compose.ota-stack.yml
├── ota/
│   ├── keys/
│   ├── server/
│   │   ├── dashboard/
│   │   ├── monitoring_backend/
│   │   ├── mosquitto/
│   │   ├── schema.sql
│   │   ├── firmware_files/
│   │   └── server/
│   └── tools/
└── README.md
```
