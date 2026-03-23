# OTA_Server_ITG

라즈베리파이에서 OTA 서버를 운영하기 위한 서버 전용 저장소입니다.

이 저장소는 다음 구성을 포함합니다.

- OTA API 서버
- OTA 대시보드
- PostgreSQL
- Mosquitto MQTT broker
- Monitoring backend
- Monitoring MySQL

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

## 기본 점검

```bash
curl -sS http://localhost:8080/health
curl -sS http://localhost:4000/health
docker compose -f docker-compose.ota-stack.yml ps
```

## 주요 API

- `GET /health`
- `GET /api/v1/vehicles`
- `GET /api/v1/firmware`
- `POST /api/v1/admin/firmware`
- `POST /api/v1/admin/trigger-update`
- `POST /api/ota/verify`
- `GET /firmware/<filename>`
- `GET /stats/summary`

## 주요 환경 변수

- `OTA_GH_FIRMWARE_BASE_URL`: 디바이스가 접근 가능한 펌웨어 base URL
- `OTA_GH_SERVER_PORT`, `OTA_GH_DASHBOARD_PORT`: API와 dashboard 포트
- `OTA_GH_MONITORING_PORT`: monitoring backend 포트
- `OTA_GH_MQTT_PORT`, `OTA_GH_MQTT_WS_PORT`: MQTT 포트
- `OTA_GH_REQUIRE_RECENT_VEHICLE`: 최근 heartbeat 차량만 trigger 허용
- `OTA_GH_VEHICLE_ONLINE_WINDOW_SEC`: 온라인 판정 윈도우
- `OTA_GH_LOCAL_PROBE_ENABLED`: local probe fallback 사용 여부
- `OTA_GH_LOCAL_DEVICE_MAP`: local probe 또는 HTTP fallback용 장치 매핑
- `OTA_GH_MQTT_COMMAND_ONLY`: MQTT-only trigger 정책
- `OTA_GH_LLM_VERIFY`, `OTA_GH_LLM_MODEL`: OTA verify LLM 설정
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
