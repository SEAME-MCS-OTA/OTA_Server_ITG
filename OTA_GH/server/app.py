"""
OTA Server - Main Application
Flask REST API 서버
"""
import json
import os
import logging
import hashlib
import shutil
import sqlite3
import time
import mimetypes
import threading
from datetime import datetime, timedelta
from urllib.parse import urlparse

import requests
from flask import Flask, request, jsonify, send_file    # request: 클라이언트의 HTTP 요청 전체 
from flask_cors import CORS
from packaging import version
from sqlalchemy.exc import IntegrityError
from sqlalchemy import inspect as sa_inspect, text
from werkzeug.utils import secure_filename

from config import Config
from models import db, Vehicle, Firmware, UpdateHistory
from mqtt_handler import MQTTHandler
from monitoring_reporter import publish_update_result, should_report_final_status
from llm_verifier import call_llm_verification, preprocess_log, save_verification_result

# 로깅 설정
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flask 앱 초기화
app = Flask(__name__)
app.config.from_object(Config)
CORS(app)

# 데이터베이스 초기화
db.init_app(app)

# MQTT 핸들러 (나중에 초기화)
mqtt_handler = None
_mqtt_last_retry_at = 0.0
_MQTT_RETRY_INTERVAL_SEC = 5.0
_local_probe_last_at = 0.0
_local_probe_lock = threading.Lock()


def _parse_local_device_map(raw: str):
    """
    Parse Config.LOCAL_DEVICE_MAP into:
      {vehicle_id: "http://ip:port"}
    """
    result = {}
    text = str(raw or "").strip()
    if not text:
        return result
    for token in text.split(","):
        item = token.strip()
        if not item:
            continue
        if "@" in item:
            vehicle_id, endpoint = item.split("@", 1)
        elif "=" in item:
            vehicle_id, endpoint = item.split("=", 1)
        else:
            continue
        vehicle_id = vehicle_id.strip()
        endpoint = endpoint.strip().rstrip("/")
        if not vehicle_id or not endpoint:
            continue
        if "://" not in endpoint:
            endpoint = f"http://{endpoint}"
        result[vehicle_id] = endpoint
    return result


def _local_endpoint_for_vehicle(vehicle_id: str) -> str:
    mapping = _parse_local_device_map(Config.LOCAL_DEVICE_MAP)
    return str(mapping.get(str(vehicle_id or "").strip(), "")).strip()


def _phase_event_to_status(phase: str, event: str) -> str:
    phase_upper = str(phase or "").strip().upper()
    event_upper = str(event or "").strip().upper()
    if event_upper == "FAIL":
        return "failed"
    if phase_upper == "DOWNLOAD":
        return "downloading"
    if phase_upper in {"APPLY", "COMMIT"}:
        return "installing"
    if phase_upper == "REBOOT":
        return "completed" if event_upper == "OK" else "installing"
    return "idle"


def _probe_local_devices_once(force: bool = False):
    """Probe configured local device endpoints and refresh Vehicle heartbeat."""
    global _local_probe_last_at
    interval = max(1, int(Config.LOCAL_PROBE_INTERVAL_SEC))
    now_mono = time.monotonic()
    if (not force) and (now_mono - _local_probe_last_at) < interval:
        return

    with _local_probe_lock:
        now_mono = time.monotonic()
        if (not force) and (now_mono - _local_probe_last_at) < interval:
            return
        _local_probe_last_at = now_mono

        mapping = _parse_local_device_map(Config.LOCAL_DEVICE_MAP)
        if not mapping:
            return

        for mapped_vehicle_id, base_url in mapping.items():
            try:
                resp = requests.get(
                    f"{base_url}/health",
                    timeout=float(Config.LOCAL_PROBE_TIMEOUT_SEC),
                )
                if resp.status_code >= 300:
                    continue
            except Exception:
                continue

            try:
                status_resp = requests.get(
                    f"{base_url}/ota/status",
                    timeout=float(Config.LOCAL_PROBE_TIMEOUT_SEC),
                )
                if status_resp.status_code < 300:
                    status_data = status_resp.json() if status_resp.content else {}
                else:
                    status_data = {}
            except Exception:
                status_data = {}

            try:
                device_id = str(status_data.get("device_id") or mapped_vehicle_id).strip()
                current_version = str(status_data.get("current_version") or "").strip()
                phase = str(status_data.get("phase") or "").strip()
                event = str(status_data.get("event") or "").strip()
                status = _phase_event_to_status(phase, event)

                vehicle = Vehicle.query.filter_by(vehicle_id=device_id).first()
                if not vehicle:
                    vehicle = Vehicle(vehicle_id=device_id, status=status or "idle")
                    db.session.add(vehicle)

                if current_version:
                    vehicle.current_version = current_version
                vehicle.status = status or "idle"
                vehicle.last_seen = datetime.utcnow()

                db.session.commit()
            except Exception as ex:
                db.session.rollback()
                logger.warning("Local probe DB update failed for %s: %s", mapped_vehicle_id, ex)


def init_db():
    """데이터베이스 테이블 생성"""
    with app.app_context():
        try:
            db.create_all()
            ensure_schema_compatibility()
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
            raise


def ensure_schema_compatibility():
    """기존 DB와 ORM 모델 간 스키마 차이를 최소한으로 보정"""
    inspector = sa_inspect(db.engine)
    table_names = set(inspector.get_table_names())

    if 'update_history' not in table_names:
        return

    update_history_columns = {
        column['name'] for column in inspector.get_columns('update_history')
    }

    with db.engine.begin() as conn:
        if 'update_type' not in update_history_columns:
            conn.execute(text("ALTER TABLE update_history ADD COLUMN update_type VARCHAR(20)"))
            logger.info("Added missing column: update_history.update_type")

def init_mqtt():
    """MQTT 핸들러 초기화"""
    global mqtt_handler
    global _mqtt_last_retry_at
    try:
        _mqtt_last_retry_at = time.monotonic()
        if mqtt_handler:
            try:
                mqtt_handler.disconnect()
            except Exception:
                pass
        with app.app_context():
            mqtt_handler = MQTTHandler(app.app_context)
            mqtt_handler.connect()
            logger.info("MQTT handler initialized and connected")
    except Exception as e:
        logger.error(f"Failed to initialize MQTT handler: {e}")
        logger.warning("Server will run without MQTT support")

@app.before_request
def before_request():
    """첫 요청 시 MQTT 초기화"""
    global mqtt_handler
    global _mqtt_last_retry_at
    _probe_local_devices_once(force=False)
    need_init = mqtt_handler is None or not mqtt_handler.is_connected()
    retry_due = (time.monotonic() - _mqtt_last_retry_at) >= _MQTT_RETRY_INTERVAL_SEC
    if need_init and retry_due:
        init_mqtt()

def compare_versions(v1: str, v2: str) -> int:
    """
    버전 비교 (semver)
    
    Returns:
        -1: v1 < v2
         0: v1 == v2
         1: v1 > v2
    """
    try:
        ver1 = version.parse(v1)
        ver2 = version.parse(v2)
        
        if ver1 < ver2:
            return -1
        elif ver1 > ver2:
            return 1
        else:
            return 0
    except Exception as e:
        logger.warning(f"Version comparison error: {e}, falling back to string comparison")
        # Fallback to string comparison
        if v1 < v2:
            return -1
        elif v1 > v2:
            return 1
        else:
            return 0


def normalize_completed_status_by_version(
    status: str,
    prev_version: str,
    target_version: str,
    message: str = "",
) -> tuple[str, str]:
    """
    서버 성공 판정 보정:
    - completed 수신 시, 이전 버전과 target_version이 동일하면 실패로 강등한다.
    - 비교 기준값이 없는 경우(prev_version empty)는 기존 completed를 유지한다.
    """
    status_norm = str(status or "").strip().lower()
    if status_norm != "completed":
        return status_norm, str(message or "")

    prev = str(prev_version or "").strip()
    target = str(target_version or "").strip()
    msg = str(message or "")

    if not target:
        reason = "target_version missing"
        return "failed", f"{msg} | {reason}".strip(" |")

    if prev and prev == target:
        reason = f"version unchanged: {prev}"
        return "failed", f"{msg} | {reason}".strip(" |")

    return "completed", msg


def parse_bool(value, default: bool = False) -> bool:
    """문자열/폼 값을 불리언으로 변환"""
    if value is None:
        return default
    return str(value).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def build_firmware_url(filename: str) -> str:
    """외부 장치가 접근 가능한 펌웨어 URL 생성"""
    if Config.FIRMWARE_BASE_URL:
        return f"{Config.FIRMWARE_BASE_URL}/firmware/{filename}"
    return f"{request.url_root.rstrip('/')}/firmware/{filename}"


def _url_points_to_localhost(url: str) -> bool:
    """장치 관점에서 접근 불가능한 localhost URL 여부"""
    try:
        host = (urlparse(str(url)).hostname or "").strip().lower()
    except Exception:
        return False
    return host in {"localhost", "127.0.0.1", "::1"} or host.startswith("127.")


def _is_rauc_bundle_filename(filename: str) -> bool:
    return str(filename or '').lower().endswith('.raucb')


def _format_cmd_topic(vehicle_id: str) -> str:
    template = str(Config.MQTT_TOPIC_CMD or 'ota/{vehicle_id}/cmd')
    try:
        return template.format(vehicle_id=vehicle_id, device_id=vehicle_id)
    except Exception:
        return template.replace('{vehicle_id}', vehicle_id).replace('{device_id}', vehicle_id)


def _build_ota_id(vehicle_id: str) -> str:
    safe_id = str(vehicle_id or "device").replace(" ", "-")
    return f"ota-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{safe_id}"


def _trigger_device_http(vehicle_id: str, firmware_info: dict):
    """Fallback path: send OTA command directly to device HTTP API."""
    endpoint = _local_endpoint_for_vehicle(vehicle_id)
    if not endpoint:
        return False, "No local device endpoint configured"

    ota_id = _build_ota_id(vehicle_id)
    payload = {
        "ota_id": ota_id,
        "url": firmware_info.get("url", ""),
        "target_version": firmware_info.get("version", ""),
    }
    try:
        resp = requests.post(
            f"{endpoint}/ota/start",
            json=payload,
            timeout=max(2.0, float(Config.LOCAL_PROBE_TIMEOUT_SEC) * 2.0),
        )
        if resp.status_code < 300:
            return True, f"http trigger ok endpoint={endpoint}"
        body = (resp.text or "")[:200]
        return False, f"http trigger failed {resp.status_code}: {body}"
    except Exception as ex:
        return False, f"http trigger error: {ex}"


def _pick_latest_active_firmware():
    active_query = Firmware.query.filter_by(is_active=True)
    if Config.PREFER_RAUCB_FIRMWARE:
        preferred = active_query.filter(Firmware.filename.ilike('%.raucb')).order_by(Firmware.created_at.desc()).first()
        if preferred:
            return preferred
    return active_query.order_by(Firmware.created_at.desc()).first()


def _normalize_active_firmware():
    active = Firmware.query.filter_by(is_active=True).order_by(Firmware.created_at.desc()).all()
    if not active:
        return None
    if len(active) == 1:
        return active[0]

    keep = None
    if Config.PREFER_RAUCB_FIRMWARE:
        for fw in active:
            if _is_rauc_bundle_filename(fw.filename):
                keep = fw
                break
    if keep is None:
        keep = active[0]

    Firmware.query.filter(
        Firmware.id != keep.id,
        Firmware.is_active.is_(True)
    ).update(
        {Firmware.is_active: False},
        synchronize_session=False
    )
    db.session.commit()
    logger.warning(
        "Normalized active firmware list: kept id=%s version=%s filename=%s, disabled=%d",
        keep.id, keep.version, keep.filename, len(active) - 1
    )
    return keep


@app.route('/health', methods=['GET'])
def health_check():
    """서버의 헬스체크 엔드포인트"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'mqtt_connected': mqtt_handler.is_connected() if mqtt_handler else False,
        'mqtt_transport': Config.MQTT_TRANSPORT,
        'mqtt_ws_path': Config.MQTT_WS_PATH if Config.MQTT_TRANSPORT == 'websockets' else '',
        'mqtt_tls_enabled': Config.MQTT_TLS_ENABLED,
    }), 200


@app.route('/', methods=['POST'])
@app.route('/ingest', methods=['POST'])
def ingest_proxy():
    """
    Device 이벤트를 OTA_VLM ingest로 전달하는 호환 프록시.
    - 과거 이미지가 collector_url을 OTA_GH(8080)로 가리킬 때도 수집이 끊기지 않도록 보완.
    """
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid JSON payload'}), 400

    ingest_url = (Config.MONITORING_INGEST_URL or '').strip()
    if not ingest_url:
        return jsonify({'error': 'MONITORING_INGEST_URL is not configured'}), 503

    try:
        parsed = urlparse(ingest_url)
        req_host = (request.host or '').split(':')[0].strip().lower()
        target_host = (parsed.hostname or '').strip().lower()
        # Prevent accidental self-loop forwarding.
        if target_host and target_host in {req_host, '127.0.0.1', 'localhost'} and parsed.port == request.environ.get('SERVER_PORT'):
            return jsonify({'error': 'Invalid MONITORING_INGEST_URL loop detected'}), 500
    except Exception:
        pass

    try:
        resp = requests.post(ingest_url, json=data, timeout=3)
        if 200 <= resp.status_code < 300:
            return jsonify({'ok': True, 'forward_status': resp.status_code}), 200
        logger.warning("ingest_proxy forward failed: status=%s body=%s", resp.status_code, (resp.text or '')[:200])
        return jsonify({'ok': False, 'forward_status': resp.status_code}), 502
    except Exception as exc:
        logger.warning("ingest_proxy error: %s", exc)
        return jsonify({'ok': False, 'error': str(exc)}), 502


@app.route('/api/v1/update-check', methods=['GET'])
def update_check():
    """
    업데이트 확인 API
    
    Query Parameters:
        vehicle_id: 차량 ID (필수)
        current_version: 현재 버전 (필수)
    
    Response:
        {
            "update_available": true/false,
            "version": "1.0.1",
            "url": "http://localhost:8080/firmware/app_1.0.1.tar.gz",
            "sha256": "...",
            "size": 123456,
            "release_notes": "..."
        }
    """
    try:
        vehicle_id = request.args.get('vehicle_id')
        current_version = request.args.get('current_version')
        
        # 필수 파라미터 검증
        if not vehicle_id or not current_version:
            return jsonify({
                'error': 'Missing required parameters: vehicle_id, current_version'
            }), 400
        
        logger.info(f"Update check request from vehicle {vehicle_id}, version {current_version}")
        
        # Vehicle upsert (없으면 생성, 있으면 업데이트)
        vehicle = Vehicle.query.filter_by(vehicle_id=vehicle_id).first()
        if vehicle:
            vehicle.last_seen = datetime.utcnow()
            vehicle.current_version = current_version
        else:
            vehicle = Vehicle(
                vehicle_id=vehicle_id,
                current_version=current_version,
                status='idle'
            )
            db.session.add(vehicle)
            logger.info(f"New vehicle registered: {vehicle_id}")
        
        db.session.commit()
        
        # 최신 active 펌웨어 조회
        # 지금 당장은 단순히 가장 최신 버전만 제공
        latest_firmware = _pick_latest_active_firmware()
        
        if not latest_firmware:
            logger.warning("No active firmware available")
            return jsonify({
                'update_available': False,
                'message': 'No firmware available'
            })
        
        # 버전 비교
        comparison = compare_versions(current_version, latest_firmware.version)
        
        if comparison < 0:  # current_version < latest_version
            # 업데이트 가능
            firmware_url = build_firmware_url(latest_firmware.filename)
            
            response = {
                'update_available': True,
                'version': latest_firmware.version,
                'url': firmware_url,
                'sha256': latest_firmware.sha256,
                'size': latest_firmware.file_size,
                'release_notes': latest_firmware.release_notes or ''
            }
            
            logger.info(
                f"Update available for {vehicle_id}: "
                f"{current_version} -> {latest_firmware.version}"
            )
            
            return jsonify(response)
        else:
            # 이미 최신 버전
            logger.info(f"Vehicle {vehicle_id} is up to date: {current_version}")
            return jsonify({
                'update_available': False,
                'current_version': current_version,
                'latest_version': latest_firmware.version
            })
    
    except Exception as e:
        logger.error(f"Error in update_check: {e}", exc_info=True)
        db.session.rollback()
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/v1/report', methods=['POST'])
def report_status():
    """
    업데이트 상태 리포트 API
    
    Request Body:
        {
            "vehicle_id": "vehicle_001",
            "target_version": "1.0.1",
            "status": "downloading|verifying|installing|completed|failed",
            "progress": 0-100,
            "message": "optional message"
        }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        # 필수 필드 검증
        required_fields = ['vehicle_id', 'target_version', 'status']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        vehicle_id = data['vehicle_id']
        target_version = data['target_version']
        incoming_status = data['status']
        progress = data.get('progress', 0)
        message = data.get('message', '')
        
        # 유효한 status 값 검증
        valid_statuses = ['downloading', 'verifying', 'installing', 'completed', 'failed']
        if incoming_status not in valid_statuses:
            return jsonify({'error': f'Invalid status: {incoming_status}'}), 400
        
        logger.info(
            f"Status report from {vehicle_id}: {incoming_status} "
            f"({progress}%) for version {target_version}"
        )
        
        # Vehicle 조회 또는 생성
        vehicle = Vehicle.query.filter_by(vehicle_id=vehicle_id).first()
        if not vehicle:
            vehicle = Vehicle(vehicle_id=vehicle_id, status=incoming_status)
            db.session.add(vehicle)
        else:
            vehicle.last_seen = datetime.utcnow()
        prev_version = str(vehicle.current_version or "").strip()

        status, message = normalize_completed_status_by_version(
            incoming_status,
            prev_version=prev_version,
            target_version=target_version,
            message=message,
        )

        vehicle.status = status
        if status == 'completed':
            vehicle.current_version = target_version
        
        # UpdateHistory 업데이트 또는 생성
        history = UpdateHistory.query.filter_by(
            vehicle_id=vehicle_id,
            target_version=target_version
        ).order_by(UpdateHistory.started_at.desc()).first()
        prev_history_status = history.status if history else None
        
        if history:
            history.status = status
            history.progress = progress
            history.message = message
            if status in ['completed', 'failed']:
                history.completed_at = datetime.utcnow()
        else:
            # 새 히스토리 생성
            # firmware_id 조회
            firmware = Firmware.query.filter_by(version=target_version).first()
            
            history = UpdateHistory(
                vehicle_id=vehicle_id,
                firmware_id=firmware.id if firmware else None,
                from_version=prev_version or None,
                target_version=target_version,
                status=status,
                progress=progress,
                message=message
            )
            db.session.add(history)
        
        db.session.commit()

        # completed/failed 최종 상태를 관제 서버로 자동 전달
        if should_report_final_status(status, prev_history_status):
            publish_update_result(
                vehicle_id=vehicle_id,
                target_version=target_version,
                status=status,
                message=message,
                current_version=vehicle.current_version,
                from_version=history.from_version if history else None,
                progress=progress,
                source="api_report",
            )
        
        return jsonify({
            'success': True,
            'vehicle_id': vehicle_id,
            'status': status,
            'progress': progress
        })
    
    except Exception as e:
        logger.error(f"Error in report_status: {e}", exc_info=True)
        db.session.rollback()
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/firmware/<filename>', methods=['GET'])
def download_firmware(filename):
    """
    펌웨어 파일 다운로드
    
    Args:
        filename: 펌웨어 파일명
    """
    try:
        firmware = Firmware.query.filter_by(filename=filename).first_or_404()

        guessed = mimetypes.guess_type(firmware.filename)[0]
        return send_file(
            firmware.file_path,
            as_attachment=True,
            download_name=filename,
            mimetype=guessed or 'application/octet-stream'
        )
    
    except Exception as e:
        logger.error(f"Error serving firmware: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/v1/admin/firmware', methods=['POST'])
def upload_firmware():
    """
    펌웨어 업로드 및 등록 (관리자용)
    
    Form Data:
        file: 펌웨어 파일
        version: 버전 (예: 1.0.1)
        release_notes: 릴리즈 노트 (선택)
        overwrite: 기존 버전/파일 덮어쓰기 여부 (선택, 기본 false)
    """
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        version_str = (request.form.get('version') or '').strip()
        release_notes = request.form.get('release_notes', '')
        overwrite = parse_bool(request.form.get('overwrite'), default=False)
        
        if not version_str:
            return jsonify({'error': 'Version is required'}), 400
        
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # 파일명 생성
        original_name = secure_filename(file.filename or '')
        if not original_name:
            return jsonify({'error': 'Invalid original filename'}), 400

        custom_name = secure_filename((request.form.get('filename') or '').strip())
        if custom_name:
            filename = custom_name
        else:
            if original_name.lower().endswith('.tar.gz'):
                ext = '.tar.gz'
            else:
                _, ext = os.path.splitext(original_name)
                ext = ext.lower()
            if ext in {'', '.'}:
                ext = '.tar.gz'
            filename = f"app_{version_str}{ext}"

        filepath = os.path.join(Config.FIRMWARE_DIR, filename)

        # 같은 버전이 이미 등록된 경우 기본적으로 거부
        existing_firmware = Firmware.query.filter_by(version=version_str).first()
        if existing_firmware and not overwrite:
            return jsonify({
                'error': (
                    f'Firmware version {version_str} already exists. '
                    f'Use overwrite=true to replace it.'
                ),
                'firmware': existing_firmware.to_dict()
            }), 409

        old_filepath = existing_firmware.file_path if existing_firmware else None

        # 파일 저장: overwrite=false일 때 기존 파일 덮어쓰기 방지
        if os.path.exists(filepath):
            if not overwrite:
                return jsonify({
                    'error': (
                        f'Firmware file {filename} already exists on server. '
                        f'Use overwrite=true to replace it.'
                    )
                }), 409
            os.remove(filepath)

        file.stream.seek(0)
        with open(filepath, 'wb') as fw:
            shutil.copyfileobj(file.stream, fw)
        
        if not os.path.exists(filepath):
            return jsonify({
                'error': 'Failed to save uploaded firmware file'
            }), 500
        
        # SHA256 계산
        sha256_hash = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256_hash.update(chunk)
        
        sha256 = sha256_hash.hexdigest()
        file_size = os.path.getsize(filepath)

        # 기존 버전 파일 경로가 달라졌다면 잔여 파일 정리
        if (
            overwrite and
            old_filepath and
            old_filepath != filepath and
            os.path.exists(old_filepath)
        ):
            os.remove(old_filepath)
        
        # DB 반영 (신규 등록 또는 기존 버전 갱신)
        if existing_firmware:
            existing_firmware.filename = filename
            existing_firmware.file_path = filepath
            existing_firmware.file_size = file_size
            existing_firmware.sha256 = sha256
            existing_firmware.release_notes = release_notes
            existing_firmware.is_active = True
            firmware = existing_firmware
            status_code = 200
            logger.info(f"Firmware replaced: {version_str} ({filename})")
        else:
            firmware = Firmware(
                version=version_str,
                filename=filename,
                file_path=filepath, 
                file_size=file_size,
                sha256=sha256,
                release_notes=release_notes,
                is_active=True
            )
            db.session.add(firmware)
            status_code = 201
            logger.info(f"Firmware uploaded: {version_str} ({filename})")

        # Keep a single active firmware to avoid dashboard/operator confusion.
        db.session.flush()
        Firmware.query.filter(
            Firmware.id != firmware.id,
            Firmware.is_active.is_(True)
        ).update(
            {Firmware.is_active: False},
            synchronize_session=False
        )

        db.session.commit()

        return jsonify({
            'success': True,
            'updated': bool(existing_firmware),
            'firmware': firmware.to_dict()
        }), status_code

    except IntegrityError:
        db.session.rollback()
        return jsonify({
            'error': f'Firmware version {version_str} already exists'
        }), 409
    
    except Exception as e:
        logger.error(f"Error uploading firmware: {e}", exc_info=True)
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/v1/admin/firmware/activate', methods=['POST'])
def activate_firmware():
    """활성 펌웨어를 단일 버전으로 강제 정리"""
    try:
        data = request.get_json(silent=True) or {}
        version_str = str(data.get('version') or '').strip()
        firmware_id_raw = data.get('id')

        firmware = None
        if firmware_id_raw is not None:
            try:
                firmware_id = int(firmware_id_raw)
            except Exception:
                return jsonify({'error': 'id must be integer'}), 400
            firmware = Firmware.query.filter_by(id=firmware_id).first()
        elif version_str:
            firmware = Firmware.query.filter_by(version=version_str).first()
        else:
            firmware = _pick_latest_active_firmware() or Firmware.query.order_by(Firmware.created_at.desc()).first()

        if not firmware:
            return jsonify({'error': 'Firmware not found'}), 404

        firmware.is_active = True
        db.session.flush()
        Firmware.query.filter(
            Firmware.id != firmware.id,
            Firmware.is_active.is_(True)
        ).update(
            {Firmware.is_active: False},
            synchronize_session=False
        )
        db.session.commit()

        return jsonify({
            'success': True,
            'firmware': firmware.to_dict()
        }), 200

    except Exception as e:
        logger.error(f"Error activating firmware: {e}", exc_info=True)
        db.session.rollback()
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/v1/admin/firmware/<int:firmware_id>', methods=['DELETE'])
def delete_firmware(firmware_id):
    """펌웨어 삭제 (관리자용).

    기본 정책:
    - 삭제 대상이 active면 남은 최신 펌웨어를 자동 active로 승격
    - 남은 펌웨어가 없으면 active 없음 상태 허용
    """
    try:
        firmware = Firmware.query.filter_by(id=firmware_id).first()
        if not firmware:
            return jsonify({'error': 'Firmware not found'}), 404

        was_active = bool(firmware.is_active)
        removed_file = None
        file_path = firmware.file_path

        # Keep update history rows; detach FK before delete.
        UpdateHistory.query.filter(
            UpdateHistory.firmware_id == firmware.id
        ).update(
            {UpdateHistory.firmware_id: None},
            synchronize_session=False
        )

        db.session.delete(firmware)
        db.session.flush()

        if was_active:
            fallback = Firmware.query.order_by(Firmware.created_at.desc()).first()
            if fallback:
                fallback.is_active = True
                Firmware.query.filter(
                    Firmware.id != fallback.id,
                    Firmware.is_active.is_(True)
                ).update(
                    {Firmware.is_active: False},
                    synchronize_session=False
                )

        db.session.commit()

        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                removed_file = file_path
            except Exception:
                removed_file = None

        return jsonify({
            'success': True,
            'deleted_id': firmware_id,
            'removed_file': removed_file,
        }), 200

    except Exception as e:
        logger.error(f"Error deleting firmware id={firmware_id}: {e}", exc_info=True)
        db.session.rollback()
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/v1/admin/trigger-update', methods=['POST'])
def trigger_update():
    """
    특정 차량에 업데이트 명령 전송 (MQTT) -> 관리자가
    
    Request Body:
        {
            "vehicle_id": "vehicle_001",
            "version": "1.0.1"  # 선택적, 없으면 최신 버전
        }
    """
    try:
        data = request.get_json()
        
        if not data or 'vehicle_id' not in data:
            return jsonify({'error': 'vehicle_id is required'}), 400
        
        vehicle_id = data['vehicle_id']
        _probe_local_devices_once(force=True)
        target_version = str(data.get('version') or '').strip() or None
        force_trigger = parse_bool(data.get('force'), default=False)
        
        # 특정 버전이 지정되면 active 여부와 무관하게 해당 버전을 사용한다.
        # (대시보드에서 이전 버전/동일 버전 재설치를 허용하기 위함)
        if target_version:
            firmware = Firmware.query.filter_by(version=target_version).first()
            if not firmware:
                return jsonify({
                    'error': f'Firmware version {target_version} not found',
                    'target_version': target_version,
                }), 404
        else:
            firmware = _pick_latest_active_firmware()
        
        if not firmware:
            return jsonify({'error': 'No active firmware found'}), 404

        # 기본 정책: 최근 접속 차량만 트리거 허용 (오프라인/ID 불일치 조기 탐지)
        vehicle = Vehicle.query.filter_by(vehicle_id=vehicle_id).first()
        if Config.REQUIRE_RECENT_VEHICLE and not force_trigger:
            if not vehicle:
                return jsonify({
                    'error': 'Vehicle not registered',
                    'vehicle_id': vehicle_id,
                    'hint': (
                        "Check device_id in /etc/ota-backend/config.json and ensure "
                        "it matches dashboard vehicle_id."
                    ),
                    'cmd_topic': _format_cmd_topic(vehicle_id),
                }), 409

        # 진행 중 상태에서 중복 트리거를 막아 false fail(OTA already running) 발생을 방지.
        if vehicle and not force_trigger:
            in_progress_states = {'pending', 'downloading', 'verifying', 'installing'}
            if str(vehicle.status or '').strip().lower() in in_progress_states:
                return jsonify({
                    'error': 'Update already in progress',
                    'vehicle_id': vehicle_id,
                    'status': vehicle.status,
                    'hint': 'Wait for completion/reboot before sending another trigger.',
                }), 409

            # 이미 동일(또는 더 높은) 버전이면 기본적으로 트리거를 막는다.
            if vehicle.current_version:
                cmp_result = compare_versions(vehicle.current_version, firmware.version)
                if cmp_result >= 0:
                    return jsonify({
                        'error': 'Vehicle already up to date',
                        'vehicle_id': vehicle_id,
                        'current_version': vehicle.current_version,
                        'target_version': firmware.version,
                        'hint': 'Set force=true to trigger anyway.',
                    }), 409

            last_seen = vehicle.last_seen
            now = datetime.utcnow()
            if (not last_seen) or ((now - last_seen) > timedelta(seconds=Config.VEHICLE_ONLINE_WINDOW_SEC)):
                return jsonify({
                    'error': 'Vehicle is offline or stale',
                    'vehicle_id': vehicle_id,
                    'last_seen': last_seen.isoformat() if last_seen else None,
                    'online_window_sec': Config.VEHICLE_ONLINE_WINDOW_SEC,
                    'hint': 'Set force=true to bypass this check.',
                    'cmd_topic': _format_cmd_topic(vehicle_id),
                }), 409
        
        # 펌웨어 정보 구성
        firmware_url = build_firmware_url(firmware.filename)
        if _url_points_to_localhost(firmware_url):
            logger.error(
                "Refusing trigger for %s: computed firmware URL points to localhost (%s).",
                vehicle_id,
                firmware_url,
            )
            return jsonify({
                'error': 'Firmware URL resolves to localhost',
                'computed_url': firmware_url,
                'hint': 'Set OTA_GH_FIRMWARE_BASE_URL=http://<HOST_IP>:8080 and restart ota_gh_server.',
            }), 409

        firmware_info = {
            'version': firmware.version,
            'url': firmware_url,
            'sha256': firmware.sha256,
            'size': firmware.file_size,
            'release_notes': firmware.release_notes or ''
        }

        # MQTT 연결이 끊겨 있으면 즉시 재연결 시도
        if (mqtt_handler is None) or (not mqtt_handler.is_connected()):
            init_mqtt()

        sent_via = None
        trigger_note = ""
        local_endpoint = _local_endpoint_for_vehicle(vehicle_id)
        prefer_http = bool(local_endpoint) and bool(Config.LOCAL_TRIGGER_FIRST)

        if prefer_http:
            ok_http, reason_http = _trigger_device_http(vehicle_id, firmware_info)
            if ok_http:
                sent_via = "http"
                trigger_note = reason_http
            elif mqtt_handler and mqtt_handler.is_connected() and mqtt_handler.publish_update_command(vehicle_id, firmware_info):
                sent_via = "mqtt"
                trigger_note = (
                    f"HTTP trigger failed ({reason_http}); "
                    f"MQTT command published to {_format_cmd_topic(vehicle_id)}"
                )
            else:
                logger.error(
                    "Failed to trigger update for %s via HTTP-first and MQTT fallback: %s",
                    vehicle_id, reason_http
                )
                return jsonify({
                    'error': 'Failed to send update command',
                    'detail': reason_http,
                    'cmd_topic': _format_cmd_topic(vehicle_id),
                }), 500
        else:
            # MQTT 우선, 실패 시 HTTP 폴백
            if mqtt_handler and mqtt_handler.is_connected() and mqtt_handler.publish_update_command(vehicle_id, firmware_info):
                sent_via = "mqtt"
                trigger_note = f"MQTT command published to {_format_cmd_topic(vehicle_id)}"
            else:
                ok_http, reason_http = _trigger_device_http(vehicle_id, firmware_info)
                if ok_http:
                    sent_via = "http"
                    trigger_note = reason_http
                else:
                    logger.error(
                        "Failed to trigger update for %s via both MQTT and HTTP fallback: %s",
                        vehicle_id, reason_http
                    )
                    return jsonify({
                        'error': 'Failed to send update command',
                        'detail': reason_http,
                        'cmd_topic': _format_cmd_topic(vehicle_id),
                    }), 500

        if sent_via:
            # IMPORTANT: keep last_seen as device heartbeat only.
            # Do not refresh it on trigger request, otherwise stale/offline checks become meaningless.
            if vehicle:
                vehicle.status = 'pending'
                pending = UpdateHistory(
                    vehicle_id=vehicle_id,
                    firmware_id=firmware.id,
                    from_version=vehicle.current_version,
                    target_version=firmware.version,
                    status='pending',
                    progress=0,
                    message=trigger_note
                )
                db.session.add(pending)
                db.session.commit()
            else:
                logger.warning(
                    "Update command sent to unknown vehicle_id=%s (no DB row, no pending history).",
                    vehicle_id
                )

            logger.info(
                "Update command sent to %s: %s via %s",
                vehicle_id, firmware.version, sent_via
            )
            return jsonify({
                'success': True,
                'vehicle_id': vehicle_id,
                'version': firmware.version,
                'url': firmware_url,
                'cmd_topic': _format_cmd_topic(vehicle_id),
                'transport': sent_via,
            })
    
    except Exception as e:
        logger.error(f"Error triggering update: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500


# ── LLM 런타임 토글 상태 ──────────────────────
_llm_enabled = Config.LLM_VERIFICATION_ENABLED


@app.route('/api/v1/llm/config', methods=['GET'])
def get_llm_config():
    """LLM 검증 설정 조회."""
    return jsonify({
        'enabled': _llm_enabled,
        'model': Config.LLM_MODEL,
    })


@app.route('/api/v1/llm/config', methods=['POST'])
def set_llm_config():
    """LLM 검증 ON/OFF 런타임 토글."""
    global _llm_enabled
    data = request.get_json() or {}
    if 'enabled' in data:
        _llm_enabled = bool(data['enabled'])
        logger.info(f"LLM verification toggled: {'ON' if _llm_enabled else 'OFF'}")
    return jsonify({
        'enabled': _llm_enabled,
        'model': Config.LLM_MODEL,
    })


@app.route('/api/ota/verify', methods=['POST'])
def verify_ota_update():
    """
    OTA 업데이트 LLM 2차 검증 엔드포인트.
    클라이언트로부터 OTA 로그를 수신하고, LLM 기반 검증 결과를 반환한다.
    """
    try:
        ota_log = request.get_json()
        if not ota_log:
            return jsonify({"error": "No JSON body provided"}), 400

        required_fields = ["firmware_metadata", "process_log", "device_state"]
        for field in required_fields:
            if field not in ota_log:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        if _llm_enabled:
            preprocessed = preprocess_log(ota_log)
            result = call_llm_verification(preprocessed, model=Config.LLM_MODEL)
        else:
            # LLM OFF: 로그만 저장하고 무조건 APPROVE
            result = {
                "decision": "APPROVE",
                "reason": "LLM verification disabled. Relying on RAUC integrity check only.",
                "raw_response": None,
            }

        save_verification_result(ota_log, result)

        logger.info(
            "OTA verify: vehicle=%s decision=%s llm_enabled=%s",
            ota_log.get("device_state", {}).get("vehicle_id", "unknown"),
            result["decision"],
            _llm_enabled,
        )

        return jsonify({
            "decision": result["decision"],
            "reason": result["reason"],
            "timestamp": datetime.utcnow().isoformat() + "Z",
        })

    except Exception as e:
        logger.error(f"Error in verify_ota_update: {e}", exc_info=True)
        return jsonify({
            "decision": "REJECT",
            "reason": f"서버 내부 오류로 Fail-safe REJECT: {str(e)}",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 500


@app.route('/api/v1/llm/results', methods=['GET'])
def list_llm_results():
    """LLM 검증 결과 목록 조회 (대시보드용)."""
    try:
        from llm_verifier import _get_verification_db
        limit = request.args.get('limit', 50, type=int)
        conn = _get_verification_db()
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM verification_results ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        results = []
        for row in rows:
            r = dict(row)
            try:
                r['ota_log'] = json.loads(r.get('ota_log_json') or '{}')
            except Exception:
                r['ota_log'] = {}
            r.pop('ota_log_json', None)
            results.append(r)
        return jsonify({'results': results, 'total': len(results), 'llm_enabled': _llm_enabled})
    except Exception as e:
        logger.error(f"Error listing LLM results: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/v1/vehicles', methods=['GET'])
def list_vehicles():
    """차량 목록 조회"""
    try:
        _probe_local_devices_once(force=True)
        vehicles = Vehicle.query.order_by(Vehicle.last_seen.desc()).all()
        return jsonify({
            'vehicles': [v.to_dict() for v in vehicles],
            'total': len(vehicles)
        })
    except Exception as e:
        logger.error(f"Error listing vehicles: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/v1/vehicles/<vehicle_id>', methods=['GET'])
def get_vehicle(vehicle_id):
    """특정 차량 기본 정보 조회"""
    try:
        vehicle = Vehicle.query.filter_by(vehicle_id=vehicle_id).first()
        if not vehicle:
            return jsonify({'error': 'Vehicle not found'}), 404

        return jsonify(vehicle.to_dict()), 200

    except Exception as e:
        logger.error(f"Error getting vehicle: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/v1/firmware', methods=['GET'])
def list_firmware():
    """펌웨어 목록 조회"""
    try:
        active_only = request.args.get('active_only', 'false').lower() == 'true'
        
        query = Firmware.query
        if active_only:
            query = query.filter_by(is_active=True)
        
        firmwares = query.order_by(Firmware.created_at.desc()).all()
        
        return jsonify({
            'firmware': [f.to_dict() for f in firmwares],
            'total': len(firmwares)
        })
    except Exception as e:
        logger.error(f"Error listing firmware: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500


@app.teardown_appcontext
def shutdown_session(exception=None):
    """요청 종료 시 세션 정리"""
    db.session.remove()


if __name__ == '__main__':
    # 설정 검증
    Config.validate()
    
    # 데이터베이스 초기화
    init_db()
    with app.app_context():
        _normalize_active_firmware()
    
    # MQTT 핸들러 초기화
    init_mqtt()
    
    # 서버 시작
    logger.info(f"Starting OTA Server on {Config.HOST}:{Config.PORT}")
    app.run(
        host=Config.HOST,
        port=Config.PORT,
        debug=Config.DEBUG
    )
