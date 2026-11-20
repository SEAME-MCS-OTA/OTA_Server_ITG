# OTA Server (ota/server)

서버 운영에 필요한 OTA 컴포넌트만 남긴 디렉토리입니다.

## 현재 아키텍처와 역할

[루트 README의 시스템 구조와 전환 조건](../../README.md#현재-시스템-역할-2026-11-11-소스)을 기준으로 한다.

이 서버는 MQTT 공지·서명 명령, HTTPS 펌웨어/시각 제공, 이벤트·기존 검증 기록 저장 및 대시보드를 담당한다. D3-G Gateway는 Jetson의 Qwen3-8B QLoRA에 HTTPS로 직접 요청하고, 검증된 APPROVE에 한해 RAUC를 실행한다. 모델/API 오류는 Gateway HOLD이며 외부 Claude fallback은 없다.

서버의 `/api/ota/verify`는 명시적으로 선택한 Claude 검증과 record_only 기록 저장을 지원한다. 기본 ON_DEVICE 모드에서는 Gateway가 이 endpoint를 record_only 기록용으로만 사용한다. 아래 Verification comparison modes 절을 참고한다. 이 저장 응답을 새 설치 승인 또는 Jetson 원본 판정으로 해석하지 않는다. 명령 Freshness v2와 시간 API는 2026-11-15 실서버에 배포하고 현재 D3-G와의 서명·시간 조회 호환성을 확인했다. 이 검증에서는 실제 OTA 설치를 요청하지 않았다.

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
- `GET /api/ota/verification-time?nonce=<32자리 hex>`
- `POST /api/ota/verify` (기존 gate/record_only)
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

- 현재 소스의 업로드는 `Config.FIRMWARE_DIR`에 파일을 저장하고 SHA256/크기를 계산합니다.
- 다운로드 endpoint(`GET /firmware/<filename>`)는 DB의 `firmware.file_path`를 `send_file`로 직접 제공합니다. 이전 문서의 OCI redirect 설명은 현재 코드와 일치하지 않아 제거했습니다.
- 서버와 DB/MQTT, 서명 키·인증서, 펌웨어 파일을 현장에 준비하면 OTA 데이터 전송 자체는 로컬 LAN에서 가능합니다. URL/인증서·시각 조건은 별도로 맞춰야 합니다. 인터넷 회선이 필요한 다른 운영 기능은 별도로 확인합니다.

## 서명 명령 v2와 Freshness

`server/app.py`의 `_build_signed_firmware_info()`는 다음 값을 포함해 발급한다.

| 항목 | 규약 |
|---|---|
| 기존 OTA 식별/이미지 정보 | ota_id, URL, target_version, expected_sha256, expected_size |
| 대상 | device_id = 명령을 받을 vehicle_id |
| 발급/만료 | issued_at/expires_at, 정수 Unix seconds, 현재 발급 기간 3,600초 |
| 서명 domain | 서명용 JSON 내부 command_schema_version = ota-command-v2 |
| 알고리즘 | 기존 Ed25519 key로 canonical JSON 서명 |

서명 JSON은 키 정렬·공백 없는 구분자·UTF-8로 직렬화한다. Firmware payload의 version/sha256/size는 Gateway parser가 target_version/expected_sha256/expected_size로 매핑한다. Release note는 서명 payload에 포함되지 않으므로 비신뢰 텍스트다. 서명 private key는 서버 밖으로 배포하지 않는다.

시간 endpoint는 정확히 32자리 소문자 hex nonce를 요구한다. 정상 응답은 `{"nonce":"<요청값>","unix_seconds":<서버 epoch 정수>}`이며 Cache-Control: no-store를 설정한다. 잘못된 nonce는 400이다. Gateway는 **기존 CA로 인증한 HTTPS listener**에서 이 경로를 호출하고 redirect 없이 nonce·왕복 시간·시각을 확인한다. 내부 HTTP 개발 포트가 있다고 해서 HTTP 응답을 Freshness 근거로 승인하지 않는다. 외부 reverse proxy에서도 `/api/ota/verification-time`을 전달해야 한다. 서버 시각을 올바르게 유지한다.

v1과 v2 서명은 호환되지 않는다. 운영 서버와 현재 부팅된 Gateway의 버전을 먼저 확인하고 초기 이미지 전환 순서를 정한다. 서버만 먼저 v2로 바꾸면 기존 Gateway는 서명 검증에 실패할 수 있다. 새 Gateway만 올라온 상태에서 v1 명령/시간 endpoint 누락은 HOLD 또는 Rule 거절로 설치를 막는다. 이미지를 업로드한 것만으로 운영 서버 코드가 갱신되지는 않는다.

최신 코드의 Gateway 재시도는 서명된 명령이 동일하더라도 관측 증거가 바뀌면 새 Jetson request/attempt ID를 사용한다. 증거가 같을 때만 이전 요청을 복구한다. 이는 Gateway 구현이며 서버가 임의로 이전 Jetson 승인 결과를 만들어 반환하는 경로가 아니다.

## 검증 기록

2026-11-15: 실서버의 다른 수정사항은 유지하고 서명 관련 네 함수와 시간 API만 반영했다. 기존 실행 이미지 위에 app.py만 교체하는 레이어를 만들어 Python·시스템 라이브러리 환경을 유지했다. 기존 서명 키와 HTTPS 인증서는 재사용했다. 컨테이너 healthy 및 MQTT connected, D3-G에서 HTTPS 시간 조회 200/nonce 일치/no-store(약 37ms), 잘못된 nonce 400을 확인했다. 현재 Gateway의 실제 서명 검증·freshness 함수가 서버 키로 만든 테스트 명령을 수락하고 대상·시각 변조를 거절했다. 테스트 명령은 MQTT로 발행하지 않았다.

서버 단위 테스트 7 passed, Gateway freshness/서명/gate 회귀 테스트 47 passed. 서버 소스와 이전 실행 이미지의 복귀 스크립트는 `/home/mqtt/deploy-backups/ota-freshness-20261115T145001Z/restore.sh`에 있다. 실서버 app.py SHA256은 `351f1f81ef2f5de48df7f7bfc26810e31e536d7028625be0afa0976ffb6129a0`이다. DB·펌웨어 저장소와 다른 서비스는 교체하지 않았다.

서버/Gateway의 v2 서명 직렬화 일치와 실제 시간 route의 nonce/no-store 응답을 별도 확인했다. 당시 서버 전체 테스트는 로컬 python-dotenv 누락으로 수집되지 않았고, 별도 실행한 기존 테스트는 15 passed / 1 skipped였다. 이미지 설치·재부팅·TEE commit의 성공 근거는 아니며 새 배포 시 별도 확인한다.

## Verification comparison modes (2026-11-17 deployed)

`GET/POST /api/v1/verification/config` controls a persistent, mutually exclusive mode:

| Mode | Cloud LLM | Qwen3-8B | Installation gate |
| --- | --- | --- | --- |
| `ON_DEVICE` (default) | OFF | ON | Mandatory checks + validated Jetson APPROVE |
| `CLAUDE` | ON | OFF | Mandatory checks + Claude APPROVE |
| `RULES_ONLY` | OFF | OFF | Mandatory checks; model explicitly skipped |

The dashboard provides two switches; enabling one turns the other off. Neither
`LLM_VERIFY=true` nor `force_llm` selects cloud verification any longer.
`server/claude_verification.py` isolates the optional Claude feature. Its `ENABLED`
constant controls availability; runtime use additionally requires persisted
`CLAUDE` mode. The default is cloud OFF/on-device ON. Setting the constant to
False blocks both SDK and direct HTTP clients and rejects runtime activation.

Policy is stored atomically in `/app/logs/verification-policy.json` on the existing
persistent logs mount, and survives container restart. The Gateway fetches the
policy over existing-CA HTTPS with a fresh echoed nonce. Retrieval/validation
failure HOLDs; it never silently skips a model. The selected mode/revision is
checked again after verification. A changed mode holds that attempt for retry.

In all modes, command signature, device binding, signed freshness, replay checks,
bundle integrity/compatibility, anti-rollback and install prerequisites remain.
Cloud and on-device errors or uncertain verdicts HOLD; no automatic fallback.
The explicit `RULES_ONLY` result has `response: null` and
`model_call_status: SKIPPED_BY_POLICY`, not a fabricated model APPROVE.

The server `record_only` endpoint still stores post-install audit records without
inference. New Gateway records include `_verification_context` for mode, revision,
request ID, model call status, model decision and Gateway decision. This does not
make the post-install record a raw model prediction. Original request/response
records remain in the Gateway's `jetson-verification.jsonl` (including cloud or
rules-only comparison attempts for compatibility with the existing audit path).
The terminal's model-only test still directly tests Jetson; it does not install
and does not follow the OTA comparison-mode switch.

A coordinated Gateway deployment is required before the dashboard switches can
control real installs. Existing histories are retained. A training-case demo is
a regression example, not an independent detection performance measurement.

Deployment: server, dashboard and D3-G runtime updated on 2026-11-17.
Gateway authenticated HTTPS policy retrieval verified. No OTA install or paid
provider invocation was run during deployment. Server rollback:
`/home/mqtt/deploy-backups/verification-modes-20261117T073642Z/restore.py`.

### AI Verification display (2026-11-17)

The history is a compact table of time, vehicle, version, mode and stored result.
Each row expands to model decision, Gateway decision and install outcome when
recorded. Legacy audit approvals are not inferred to be model predictions or
successful installs. Missing fields show `Not recorded`; historical conditional
results retain their original value. Stored model response, populated metrics
and original stored evidence are nested disclosures. Old reject-rate/conditional
summary cards were removed because they mixed historical audit and model results.
LLM Runtime Log remains available below the history, collapsed by default; it
loads only when opened or manually refreshed and does not block history refresh.
This is the server/cloud log, not the Jetson runtime log. Existing APIs and records
are preserved. These presentation changes do not alter model or installation
policy and do not require a D3-G recipe change.

### Release notes before installation (2026-11-17)

The release announcement producer now includes `release_notes` from the selected
firmware in the existing MQTT announcement payload. The signed installation
command already carried release notes; D3-G already reads the announcement field
and exposes it to the UI. No new topic or signature schema is required. Display
changes apply to subsequent announcements; deployment did not publish a release
or start an update. Server rollback:
`/home/mqtt/deploy-backups/announcement-notes-20261117T084502Z/restore.py`.
