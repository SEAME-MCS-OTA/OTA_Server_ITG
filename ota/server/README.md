# OTA Server (ota/server)

서버 운영에 필요한 OTA 컴포넌트만 남긴 디렉토리입니다.

## 1단계 아키텍처와 역할

[루트 README의 ASCII 아키텍처와 통신·검증 정책](../../README.md#1단계-목표-시스템-아키텍처)을 기준으로 한다.

목표 구조에서 이 서버는 MQTT 알림·명령, HTTPS 펌웨어 제공, 로그·검증 기록 저장 및 대시보드를 담당한다. Gateway는 MQTT로 등록·요청·상태·진행률을 보내고 HTTPS POST로 상세 기록을 보낸다. 2차 검증은 D3-G Gateway가 Ethernet으로 Jetson에 직접 요청하며, Jetson 결과의 서버 기록은 Gateway가 담당한다.

현재 서버 코드에는 외부 Claude API를 호출하는 `/api/ota/verify` gate가 남아 있다. Jetson 연동과 무응답 시 설치 보류·재시도는 목표 설계이며 이번 변경으로 구현되지는 않는다. 아래 실행·API 설명은 현재 구현 기준이다.

Gateway 내부 계층과 보안 경계는 [2단계 Gateway 아키텍처](https://github.com/SEAME-MCS-OTA/OTA_Telechips/blob/main/README.md#2단계-gateway-계층-아키텍처)를 참고한다.

## 구성

- `server/`: Flask 기반 OTA API 서버
- `dashboard/`: React 대시보드
- `mosquitto/`: MQTT 브로커 설정 파일
- `schema.sql`: PostgreSQL 초기 스키마
- `firmware_files/`: 컨테이너 마운트 작업 디렉토리

## 루트에서 실행

```bash
cd /path/to/OTA_Server_ITG
./ota/tools/ota-stack-up.sh
```

중지:

```bash
cd /path/to/OTA_Server_ITG
./ota/tools/ota-stack-down.sh
```

## 접속 주소

- API: `http://localhost:8080`
- Dashboard: `http://localhost:3001`
- MQTT TCP: `localhost:1883`
- MQTT WS: `localhost:9001`

## 기본 확인

```bash
curl -sS http://localhost:8080/health
curl -sS http://localhost:8080/api/v1/vehicles
curl -sS http://localhost:8080/api/v1/firmware
```

## 주요 API

- `GET /health`
- `GET /api/v1/update-check`
- `POST /api/v1/report`
- `GET /api/v1/vehicles`
- `GET /api/v1/firmware`
- `POST /api/v1/admin/firmware`
- `POST /api/v1/admin/trigger-update`
- `GET /firmware/<filename>`

## 펌웨어 업로드 예시

```bash
curl -sS -X POST http://localhost:8080/api/v1/admin/firmware \
  -F "file=@/path/to/update.raucb" \
  -F "version=1.0.0" \
  -F "release_notes=Initial release" \
  -F "overwrite=true"
```

## 운영 시 주의

- 업로드는 로컬 파일 영구 저장이 아니라 OCI Object Storage 직접 업로드입니다.
- 다운로드 엔드포인트(`GET /firmware/<filename>`)는 OCI URL로 리다이렉트합니다.
- OCI 관련 환경 변수(`OTA_GH_OCI_*`)가 올바르지 않으면 업로드/다운로드가 실패합니다.
