# OTA_Server_ITG

라즈베리파이에서 OTA 서버를 운영하기 위한 서버 전용 저장소입니다.

이 저장소는 다음 구성을 포함합니다.

- OTA API 서버
- OTA 대시보드
- PostgreSQL
- Mosquitto MQTT broker
- Monitoring backend
- Monitoring MySQL

## 현재 시스템 역할 (2026-11-11 소스)

```text
OTA Server ── MQTT 공지·서명 명령 / HTTPS 펌웨어·시각 ──> D3-G Gateway
OTA Server <── 등록·요청·상태·진행률 / 이벤트·audit ──── D3-G Gateway
                                                          │
                                      전용 Ethernet HTTPS │
                                                          ▼
                                             Jetson Qwen3-8B QLoRA
                                           APPROVE / REJECT / 실행 오류
```

Gateway 소스는 외부 Claude gate 호출을 중단하고 Jetson `https://10.77.0.1:8443/api/v1/ota/verify`를 사용한다. 모델은 판정만 반환하고 설치 결정·RAUC·재부팅·TEE commit은 D3-G가 담당한다. Qt UI와 UART TUI는 같은 Gateway API에 요청한다. Jetson은 서버에 직접 보고하지 않는다.

서버의 `/api/ota/verify`와 Claude 관련 코드는 기존 도구/benchmark용으로 남아 있다. 현재 Gateway는 해당 endpoint를 record_only 저장에 사용하며, 서버 LLM toggle을 바꿔도 Gateway의 Jetson gate를 우회하지 않는다. 전체 Jetson 원본 응답의 DB/대시보드 연동 완료를 주장하지 않는다.

### 명령 Freshness v2 및 전환 조건

새 서버 소스는 대상 장치와 issued_at/expires_at을 포함한 Ed25519 `ota-command-v2` 서명을 발급하고 `/api/ota/verification-time`을 제공한다. 새 Gateway가 이 규격과 인증된 서버 시각을 확인한다. [상세 계약](ota/server/README.md#서명-명령-v2와-freshness).

**2026-11-15 실서버 반영 완료:** 라즈베리 파이에 v2 서명과 시간 API를 배포했고, 현재 D3-G에도 v2 검증 코드가 있음을 확인했다. 기존 CA를 이용한 D3-G → 서버 HTTPS 시간 조회(200, nonce 일치, no-store)와 실제 서버 키로 서명한 명령의 Gateway 검증이 통과했다. 잘못된 nonce는 400으로 거절했다. 실제 OTA 설치는 요청하지 않았다. v1 Gateway와 v2 서명 서버는 호환되지 않으므로 다른 보드에는 운영 버전을 먼저 확인한다. [상세 계약·배포 기록](ota/server/README.md#서명-명령-v2와-freshness), [Gateway 보안 설계](https://github.com/SEAME-MCS-OTA/OTA_Telechips/blob/main/docs/security-design.md).

2026-11-11 초기 TUI 시도의 모델 REJECT 기록과 이후 배포·실행 상태는 구분한다. 이번 서버 프로토콜 검증은 펌웨어 다운로드·설치·재부팅·TEE commit 전체 사이클의 시험을 대신하지 않는다.

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
- `GET /api/ota/verification-time?nonce=<32자리 hex>` (새 Gateway용 인증된 HTTPS 시각 조회)
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
- `OTA_GH_LLM_VERIFY`, `OTA_GH_LLM_MODEL`: 서버에 남아 있는 외부 LLM 검증/기존 평가용 설정. 새 Gateway의 Jetson gate를 활성화·비활성화하지 않음
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
