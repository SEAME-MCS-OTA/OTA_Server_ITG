# OTA_GH

Flask + PostgreSQL + MQTT 기반 OTA(Over-The-Air) 업데이트 데모 시스템입니다.  
서버가 차량(클라이언트)의 버전을 관리하고, 펌웨어 배포 및 업데이트 상태를 수집합니다.  
React 대시보드로 차량/펌웨어 상태를 모니터링할 수 있습니다.
10년동안 연평균 20퍼를 상회하는 수익률은 진짜 미친거아냐?
## 1. 구성 요소

- `server/`: OTA REST API 서버(Flask), DB 모델, MQTT 핸들러
- `client/`: OTA 업데이트 클라이언트(Python)
- `dashboard/`: OTA 상태 모니터링 웹 대시보드(React + Vite)
- `mosquitto/`: MQTT 브로커 설정
- `scripts/`: 펌웨어 생성/업로드/상태 확인 스크립트
- `firmware_files/`: 서버에서 배포할 펌웨어 파일 저장 경로
- `docker-compose.yml`: PostgreSQL, Mosquitto, OTA 서버 통합 실행
- `quickstart.sh`: 전체 시스템 빠른 시작 스크립트

## 2. OTA 동작 흐름

1. 클라이언트가 `/api/v1/update-check`로 업데이트 확인
2. 서버가 최신 활성 펌웨어 버전/URL/SHA256 반환
3. 클라이언트가 펌웨어 다운로드 및 SHA256 검증
4. 클라이언트가 설치 후 상태를 HTTP/MQTT로 리포트
5. 대시보드에서 차량 상태와 펌웨어 목록 확인

## 3. 빠른 시작

### 3.1 전체 실행 (권장)

```bash
chmod +x quickstart.sh
./quickstart.sh
```

실행 후 기본 접속 주소:

- OTA Server: `http://localhost:8080`
- Dashboard: `http://localhost:3001`
- PostgreSQL: `localhost:5432`
- MQTT: `localhost:1883`

### 3.2 클라이언트 실행

```bash
cd client
pip install -r requirements.txt
python client.py
```

## 4. 펌웨어 테스트 절차

### 4.1 펌웨어 생성

```bash
chmod +x scripts/*.sh
./scripts/create_firmware.sh 1.0.1
```

### 4.2 펌웨어 업로드 

```bash
./scripts/upload_firmware_1.0.1.sh
```

또는:

```bash
curl -X POST http://localhost:8080/api/v1/admin/firmware \
  -F "file=@./firmware_files/app_1.0.1.tar.gz" \
  -F "version=1.0.1" \
  -F "release_notes=Release 1.0.1"
```

### 4.3 차량 업데이트 트리거

```bash
curl -X POST http://localhost:8080/api/v1/admin/trigger-update \
  -H "Content-Type: application/json" \
  -d '{"vehicle_id":"vehicle_001","version":"1.0.1"}'
```
잠깐 지금까지 너가 한걸 정리해봐
### 4.4 상태 확인

```bash
./scripts/check_status.sh vehicle_001
```

## 5. 주요 API

- `GET /health`: 서버 헬스체크
- `GET /api/v1/update-check`: 차량 업데이트 가능 여부 확인
- `POST /api/v1/report`: 클라이언트 업데이트 상태 보고
- `GET /api/v1/vehicles`: 차량 목록 조회
- `GET /api/v1/firmware`: 펌웨어 목록 조회
- `POST /api/v1/admin/firmware`: 펌웨어 업로드(관리자)
- `POST /api/v1/admin/trigger-update`: 업데이트 명령 발행(관리자)
  - 기본: MQTT `ota/{vehicle_id}/cmd`
  - 폴백: `LOCAL_DEVICE_MAP`에 등록된 장치의 `POST /ota/start`

## 6. 환경 변수

루트 `.env`(docker compose용) 주요 항목:

- `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_PORT`
- `SERVER_PORT`, `SECRET_KEY`, `DEBUG`, `LOG_LEVEL`
- `MQTT_BROKER_PORT`
- `MQTT_TRANSPORT` (`tcp` 또는 `websockets`)
- `MQTT_WS_PATH` (예: `/mqtt`, websockets 사용 시)
- `MQTT_TLS_ENABLED`, `MQTT_TLS_INSECURE`
- `MQTT_CA_CERTS`, `MQTT_CERTFILE`, `MQTT_KEYFILE`
- `LOCAL_DEVICE_MAP` (예: `vw-ivi-0026@192.168.86.250:8080`)
- `LOCAL_PROBE_INTERVAL_SEC`, `LOCAL_PROBE_TIMEOUT_SEC`

클라이언트(`client/.env`) 주요 항목:

- `OTA_SERVER_URL`, `VEHICLE_ID`
- `OTA_MODE` (`mqtt` 또는 `polling`)
- `INSTALL_MODE` (`file_copy` 또는 `systemd`)

대시보드(`dashboard/.env`) 주요 항목:

- `VITE_APP_API_URL` (예: `http://localhost:8080`)

## 7. 디렉토리 구조 (요약)

```text
OTA_GH/
├── client/
├── dashboard/
├── firmware_files/
├── mosquitto/
├── scripts/
├── server/
├── docker-compose.yml
├── quickstart.sh
├── schema.sql
└── README.md
```

## 8. 업로드 전 체크 권장 사항

- 실제 운영용 비밀번호/시크릿 값이 `.env`에 남아있지 않은지 확인
- 불필요한 실행 산출물(`dashboard.log`, `.dashboard.pid`, 백업 폴더 등) 정리
- `dashboard/node_modules`는 일반적으로 저장소에 포함하지 않음
