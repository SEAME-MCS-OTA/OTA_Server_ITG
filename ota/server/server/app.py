"""
OTA Server - Main Application
Flask REST API 서버
"""
import os
import logging
import hashlib
import json
import base64
import time
import threading
import subprocess
import tempfile
from datetime import datetime, timedelta
from urllib.parse import quote, urlparse

import requests
from flask import Flask, has_request_context, jsonify, redirect, request, send_from_directory
from flask_cors import CORS
from packaging import version
from sqlalchemy.exc import IntegrityError
from sqlalchemy import inspect as sa_inspect, text
from werkzeug.utils import secure_filename

from config import Config
from models import db, Vehicle, Firmware, UpdateHistory
from mqtt_handler import MQTTHandler
from monitoring_reporter import publish_update_result, should_report_final_status

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
_llm_enabled = Config.LLM_VERIFICATION_ENABLED


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
                ip_addr = str(status_data.get("ip") or status_data.get("ip_address") or "").strip()
                phase = str(status_data.get("phase") or "").strip()
                event = str(status_data.get("event") or "").strip()
                status = _phase_event_to_status(phase, event)

                vehicle = Vehicle.query.filter_by(vehicle_id=device_id).first()
                if not vehicle:
                    vehicle = Vehicle(vehicle_id=device_id, status=status or "idle")
                    db.session.add(vehicle)

                if current_version:
                    vehicle.current_version = current_version
                if ip_addr:
                    vehicle.last_ip = ip_addr
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

    with db.engine.begin() as conn:
        if 'update_history' in table_names:
            update_history_columns = {
                column['name'] for column in inspector.get_columns('update_history')
            }
            if 'update_type' not in update_history_columns:
                conn.execute(text("ALTER TABLE update_history ADD COLUMN update_type VARCHAR(20)"))
                logger.info("Added missing column: update_history.update_type")

        if 'firmware' in table_names:
            firmware_columns = {
                column['name'] for column in inspector.get_columns('firmware')
            }
            if 'oci_uploaded' not in firmware_columns:
                conn.execute(text("ALTER TABLE firmware ADD COLUMN oci_uploaded BOOLEAN DEFAULT FALSE"))
                logger.info("Added missing column: firmware.oci_uploaded")

        if 'vehicles' in table_names:
            vehicles_columns = {
                column['name'] for column in inspector.get_columns('vehicles')
            }
            if 'last_ip' not in vehicles_columns:
                conn.execute(text("ALTER TABLE vehicles ADD COLUMN last_ip VARCHAR(64)"))
                logger.info("Added missing column: vehicles.last_ip")

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
    if Config.LOCAL_PROBE_ENABLED:
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


def parse_bool(value, default: bool = False) -> bool:
    """문자열/폼 값을 불리언으로 변환"""
    if value is None:
        return default
    return str(value).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _get_oci_object_url(filename: str) -> str:
    """OCI Object Storage object URL 조합 (PAR 토큰 기반). 토큰 미설정 시 빈 문자열 반환."""
    token = (Config.OCI_PAR_TOKEN or '').strip()
    if not token:
        return ''
    # Keep a user-provided leading slash (e.g. "/releases") because
    # some PAR scopes are configured with object names that start with "/".
    prefix = (Config.OCI_FIRMWARE_PREFIX or 'firmware').rstrip('/')
    if not prefix:
        prefix = 'firmware'
    return (
        f"https://objectstorage.{Config.OCI_REGION}.oraclecloud.com"
        f"/p/{token}"
        f"/n/{Config.OCI_NAMESPACE}"
        f"/b/{Config.OCI_BUCKET}"
        f"/o/{prefix}/{filename}"
    )


def _get_oci_object_ref(filename: str) -> str:
    """DB 저장용 OCI object reference."""
    prefix = (Config.OCI_FIRMWARE_PREFIX or 'firmware').rstrip('/')
    if not prefix:
        prefix = 'firmware'
    return f"oci://{Config.OCI_BUCKET}/{prefix}/{filename}"


def _local_firmware_path(filename: str) -> str:
    safe_name = secure_filename(filename or "")
    if not safe_name:
        return ""
    return os.path.join(Config.FIRMWARE_DIR, safe_name)


def _build_local_firmware_url(filename: str) -> str:
    base_url = str(Config.FIRMWARE_BASE_URL or "").strip().rstrip("/")
    if not base_url and has_request_context():
        base_url = request.url_root.rstrip("/")
    if not base_url:
        return ""
    return f"{base_url}/firmware/{quote(str(filename or ''))}"


def build_firmware_url(filename: str) -> str:
    """외부 장치가 접근 가능한 펌웨어 URL(OCI 우선, 로컬 fallback) 생성."""
    firmware = Firmware.query.filter_by(filename=filename).first()
    if not firmware:
        return ""

    if firmware.oci_uploaded:
        oci_url = _get_oci_object_url(filename)
        if oci_url:
            return oci_url

    local_path = _local_firmware_path(filename)
    if local_path and os.path.isfile(local_path):
        return _build_local_firmware_url(filename)
    return ""


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


def _build_upload_filename(original_name: str, version_str: str, custom_name: str = "") -> str:
    custom_name = secure_filename(custom_name or "")
    if custom_name:
        return custom_name

    safe_original_name = secure_filename(original_name or "")
    if not safe_original_name:
        return ""

    if safe_original_name.lower().endswith('.tar.gz'):
        ext = '.tar.gz'
    else:
        _, ext = os.path.splitext(safe_original_name)
        ext = ext.lower()
    if ext in {'', '.'}:
        ext = '.tar.gz'
    return f"app_{version_str}{ext}"


def _canonical_command_payload(
    ota_id: str,
    url: str,
    target_version: str,
    expected_sha256: str,
    expected_size: int,
) -> bytes:
    body = {
        "ota_id": str(ota_id or "").strip(),
        "url": str(url or "").strip(),
        "target_version": str(target_version or "").strip(),
        "expected_sha256": str(expected_sha256 or "").strip().lower(),
        "expected_size": int(expected_size or 0),
    }
    return json.dumps(
        body,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _sign_command_payload(payload: bytes):
    if not Config.COMMAND_SIGN_ENABLED:
        return None, None

    algo = str(Config.COMMAND_SIGN_ALGO or "").strip().lower()
    if algo != "ed25519":
        return None, f"Unsupported command signing algorithm: {algo or 'empty'}"

    key_path = str(Config.COMMAND_SIGN_KEY_PATH or "").strip()
    if not key_path:
        return None, "COMMAND_SIGN_KEY_PATH is empty"
    if not os.path.exists(key_path):
        return None, f"Signing key not found: {key_path}"

    msg_fd = -1
    sig_fd = -1
    msg_path = ""
    sig_path = ""
    try:
        msg_fd, msg_path = tempfile.mkstemp(prefix="ota-cmd-", suffix=".msg")
        sig_fd, sig_path = tempfile.mkstemp(prefix="ota-cmd-", suffix=".sig")
        os.close(msg_fd)
        os.close(sig_fd)
        msg_fd = -1
        sig_fd = -1

        with open(msg_path, "wb") as f:
            f.write(payload)

        proc = subprocess.run(
            [
                "openssl",
                "pkeyutl",
                "-sign",
                "-rawin",
                "-inkey",
                key_path,
                "-in",
                msg_path,
                "-out",
                sig_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            return None, f"OpenSSL signing failed: {detail or f'rc={proc.returncode}'}"

        with open(sig_path, "rb") as f:
            sig_b64 = base64.b64encode(f.read()).decode("ascii")

        return {
            "algorithm": "ed25519",
            "key_id": str(Config.COMMAND_SIGN_KEY_ID or "ota-ed25519-v1"),
            "value": sig_b64,
        }, None
    except Exception as ex:
        return None, f"Command signing error: {ex.__class__.__name__}: {ex}"
    finally:
        if msg_fd >= 0:
            try:
                os.close(msg_fd)
            except Exception:
                pass
        if sig_fd >= 0:
            try:
                os.close(sig_fd)
            except Exception:
                pass
        for p in (msg_path, sig_path):
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass


def _trigger_device_http(vehicle_id: str, firmware_info: dict):
    """Fallback path: send OTA command directly to device HTTP API."""
    endpoint = _local_endpoint_for_vehicle(vehicle_id)
    if not endpoint:
        return False, "No local device endpoint configured"

    ota_id = str(firmware_info.get("ota_id") or "").strip() or _build_ota_id(vehicle_id)
    payload = {
        "ota_id": ota_id,
        "url": firmware_info.get("url", ""),
        "target_version": firmware_info.get("version", ""),
        "expected_sha256": firmware_info.get("sha256", ""),
        "expected_size": int(firmware_info.get("size") or 0),
    }
    if isinstance(firmware_info.get("signature"), dict):
        payload["signature"] = firmware_info.get("signature")
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
        return None
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


def _resolve_target_firmware(target_version: str | None):
    if target_version:
        firmware = Firmware.query.filter_by(version=target_version).first()
        if not firmware:
            return None, (
                jsonify({
                    'error': f'Firmware version {target_version} not found',
                    'target_version': target_version,
                }),
                404,
            )
        if Config.PREFER_RAUCB_FIRMWARE and not _is_rauc_bundle_filename(firmware.filename):
            return None, (
                jsonify({
                    'error': 'Only .raucb firmware bundles are supported',
                    'target_version': firmware.version,
                    'filename': firmware.filename,
                }),
                409,
            )
        return firmware, None

    firmware = _pick_latest_active_firmware()
    if not firmware:
        if Config.PREFER_RAUCB_FIRMWARE:
            return None, (
                jsonify({'error': 'No active .raucb firmware found'}),
                404,
            )
        return None, (jsonify({'error': 'No active firmware found'}), 404)
    return firmware, None


def _validate_trigger_vehicle(vehicle_id: str, firmware: Firmware, force_trigger: bool, cmd_topic: str):
    vehicle = Vehicle.query.filter_by(vehicle_id=vehicle_id).first()
    if Config.REQUIRE_RECENT_VEHICLE and not force_trigger and not vehicle:
        return None, (
            jsonify({
                'error': 'Vehicle not registered',
                'vehicle_id': vehicle_id,
                'hint': (
                    "Check device_id in /etc/ota-backend/config.json and ensure "
                    "it matches dashboard vehicle_id."
                ),
                'cmd_topic': cmd_topic,
            }),
            409,
        )

    if not vehicle or force_trigger:
        return vehicle, None

    in_progress_states = {'pending', 'downloading', 'verifying', 'installing'}
    if str(vehicle.status or '').strip().lower() in in_progress_states:
        return None, (
            jsonify({
                'error': 'Update already in progress',
                'vehicle_id': vehicle_id,
                'status': vehicle.status,
                'hint': 'Wait for completion/reboot before sending another trigger.',
            }),
            409,
        )

    if vehicle.current_version and compare_versions(vehicle.current_version, firmware.version) >= 0:
        return None, (
            jsonify({
                'error': 'Vehicle already up to date',
                'vehicle_id': vehicle_id,
                'current_version': vehicle.current_version,
                'target_version': firmware.version,
                'hint': 'Set force=true to trigger anyway.',
            }),
            409,
        )

    last_seen = vehicle.last_seen
    now = datetime.utcnow()
    if (not last_seen) or ((now - last_seen) > timedelta(seconds=Config.VEHICLE_ONLINE_WINDOW_SEC)):
        return None, (
            jsonify({
                'error': 'Vehicle is offline or stale',
                'vehicle_id': vehicle_id,
                'last_seen': last_seen.isoformat() if last_seen else None,
                'online_window_sec': Config.VEHICLE_ONLINE_WINDOW_SEC,
                'hint': 'Set force=true to bypass this check.',
                'cmd_topic': cmd_topic,
            }),
            409,
        )

    return vehicle, None


def _build_trigger_firmware_info(vehicle_id: str, firmware: Firmware):
    if Config.PREFER_RAUCB_FIRMWARE and not _is_rauc_bundle_filename(firmware.filename):
        return None, (
            jsonify({
                'error': 'Only .raucb firmware bundles are supported',
                'target_version': firmware.version,
                'filename': firmware.filename,
            }),
            409,
        )

    firmware_url = build_firmware_url(firmware.filename)
    if not firmware_url:
        return None, (
            jsonify({
                'error': 'Firmware is not available for distribution',
                'target_version': firmware.version,
                'hint': 'Check firmware upload result and OTA_GH_FIRMWARE_BASE_URL/OCI settings.',
            }),
            409,
        )
    if _url_points_to_localhost(firmware_url):
        return None, (
            jsonify({
                'error': 'Firmware URL resolves to localhost',
                'computed_url': firmware_url,
                'hint': 'Set OTA_GH_FIRMWARE_BASE_URL=http://<HOST_IP>:8080 and restart ota_gh_server.',
            }),
            409,
        )

    firmware_info = {
        'ota_id': _build_ota_id(vehicle_id),
        'version': firmware.version,
        'url': firmware_url,
        'sha256': firmware.sha256,
        'size': firmware.file_size,
        'release_notes': firmware.release_notes or '',
    }
    return firmware_info, None


def _attach_command_signature(vehicle_id: str, firmware_info: dict):
    payload_bytes = _canonical_command_payload(
        ota_id=firmware_info['ota_id'],
        url=firmware_info['url'],
        target_version=firmware_info['version'],
        expected_sha256=firmware_info['sha256'],
        expected_size=int(firmware_info['size'] or 0),
    )
    signature, sign_error = _sign_command_payload(payload_bytes)
    if signature:
        firmware_info['signature'] = signature
        return None
    if sign_error and Config.COMMAND_SIGN_REQUIRE:
        logger.error("Refusing trigger for %s: %s", vehicle_id, sign_error)
        return jsonify({
            'error': 'Failed to sign OTA command',
            'detail': sign_error,
            'hint': 'Check OTA_GH command-signing key configuration.',
        }), 500
    if sign_error:
        logger.warning("Command signing skipped for %s: %s", vehicle_id, sign_error)
    return None


def _publish_trigger_command(vehicle_id: str, firmware_info: dict, cmd_topic: str):
    if (mqtt_handler is None) or (not mqtt_handler.is_connected()):
        init_mqtt()

    local_endpoint = _local_endpoint_for_vehicle(vehicle_id)
    prefer_http = bool(local_endpoint) and bool(Config.LOCAL_TRIGGER_FIRST)

    def _publish_mqtt() -> bool:
        return bool(
            mqtt_handler and
            mqtt_handler.is_connected() and
            mqtt_handler.publish_update_command(vehicle_id, firmware_info, firmware_info['ota_id'])
        )

    if Config.MQTT_COMMAND_ONLY:
        if _publish_mqtt():
            return "mqtt", f"MQTT command published to {cmd_topic}", None
        return None, None, (
            jsonify({
                'error': 'Failed to send update command',
                'detail': 'MQTT command publish failed or broker disconnected',
                'cmd_topic': cmd_topic,
            }),
            500,
        )

    if prefer_http:
        ok_http, reason_http = _trigger_device_http(vehicle_id, firmware_info)
        if ok_http:
            return "http", reason_http, None
        if _publish_mqtt():
            return "mqtt", f"HTTP trigger failed ({reason_http}); MQTT command published to {cmd_topic}", None
        logger.error(
            "Failed to trigger update for %s via HTTP-first and MQTT fallback: %s",
            vehicle_id, reason_http
        )
        return None, None, (
            jsonify({
                'error': 'Failed to send update command',
                'detail': reason_http,
                'cmd_topic': cmd_topic,
            }),
            500,
        )

    if _publish_mqtt():
        return "mqtt", f"MQTT command published to {cmd_topic}", None

    ok_http, reason_http = _trigger_device_http(vehicle_id, firmware_info)
    if ok_http:
        return "http", reason_http, None

    logger.error(
        "Failed to trigger update for %s via both MQTT and HTTP fallback: %s",
        vehicle_id, reason_http
    )
    return None, None, (
        jsonify({
            'error': 'Failed to send update command',
            'detail': reason_http,
            'cmd_topic': cmd_topic,
        }),
        500,
    )


def _record_pending_trigger(vehicle, vehicle_id: str, firmware: Firmware, trigger_note: str):
    if not vehicle:
        logger.warning(
            "Update command sent to unknown vehicle_id=%s (no DB row, no pending history).",
            vehicle_id
        )
        return

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


@app.route('/health', methods=['GET'])
def health_check():
    """서버의 헬스체크 엔드포인트"""
    sign_key_path = str(Config.COMMAND_SIGN_KEY_PATH or "").strip()
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'mqtt_connected': mqtt_handler.is_connected() if mqtt_handler else False,
        'require_recent_vehicle': bool(Config.REQUIRE_RECENT_VEHICLE),
        'vehicle_online_window_sec': int(Config.VEHICLE_ONLINE_WINDOW_SEC),
        'command_sign_enabled': bool(Config.COMMAND_SIGN_ENABLED),
        'command_sign_required': bool(Config.COMMAND_SIGN_REQUIRE),
        'command_sign_algo': Config.COMMAND_SIGN_ALGO,
        'command_sign_key_exists': bool(sign_key_path and os.path.exists(sign_key_path)),
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
            if not firmware_url:
                logger.warning(
                    "Update available for %s but firmware is not distributable: version=%s",
                    vehicle_id,
                    latest_firmware.version,
                )
                return jsonify({
                    'update_available': False,
                    'message': 'Active firmware is not downloadable yet'
                })
            
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
        status = data['status']
        progress = data.get('progress', 0)
        message = data.get('message', '')
        
        # 유효한 status 값 검증
        valid_statuses = ['downloading', 'verifying', 'installing', 'completed', 'failed']
        if status not in valid_statuses:
            return jsonify({'error': f'Invalid status: {status}'}), 400
        
        logger.info(
            f"Status report from {vehicle_id}: {status} "
            f"({progress}%) for version {target_version}"
        )
        
        # Vehicle 조회 또는 생성
        vehicle = Vehicle.query.filter_by(vehicle_id=vehicle_id).first()
        if not vehicle:
            vehicle = Vehicle(vehicle_id=vehicle_id, status=status)
            db.session.add(vehicle)
        else:
            vehicle.status = status
            vehicle.last_seen = datetime.utcnow()
        
        # completed 상태면 current_version 업데이트
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
                from_version=vehicle.current_version if status != 'completed' else None,
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


def _stream_file_stats(stream) -> tuple[str, int]:
    """업로드 스트림의 SHA256과 크기를 계산하고 시작 위치로 되돌린다."""
    hasher = hashlib.sha256()
    total_size = 0
    try:
        stream.seek(0)
    except Exception as ex:
        raise ValueError("Upload stream is not seekable") from ex

    while True:
        chunk = stream.read(1024 * 1024)
        if not chunk:
            break
        hasher.update(chunk)
        total_size += len(chunk)

    stream.seek(0)
    return hasher.hexdigest(), total_size


def _upload_stream_to_oci(stream, filename: str) -> bool:
    """영구 로컬 저장 없이 업로드 스트림을 OCI로 직접 전송."""
    oci_url = _get_oci_object_url(filename)
    if not oci_url:
        logger.info("OCI PAR token not configured, skipping OCI upload")
        return False
    try:
        stream.seek(0)
        resp = requests.put(oci_url, data=stream, timeout=300)
        if resp.status_code < 300:
            logger.info("OCI upload success: %s (status=%d)", filename, resp.status_code)
            stream.seek(0)
            return True
        else:
            logger.error("OCI upload failed: %s (status=%d)", filename, resp.status_code)
            stream.seek(0)
            return False
    except Exception as e:
        logger.error("OCI upload error for %s: %s", filename, e)
        try:
            stream.seek(0)
        except Exception:
            pass
        return False


def _save_stream_to_local(stream, filename: str) -> str:
    """업로드 스트림을 로컬 펌웨어 디렉토리에 저장."""
    target_path = _local_firmware_path(filename)
    if not target_path:
        raise ValueError("Invalid local firmware filename")
    os.makedirs(Config.FIRMWARE_DIR, exist_ok=True)
    stream.seek(0)
    with open(target_path, "wb") as out_file:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            out_file.write(chunk)
    stream.seek(0)
    return target_path


def _announce_new_release(firmware: Firmware):
    """새 릴리즈 업로드 후 차량 announce 토픽으로 브로드캐스트."""
    if not firmware:
        return
    firmware_url = build_firmware_url(firmware.filename)
    if not firmware_url:
        logger.warning(
            "Skipping release announce: firmware URL unavailable (version=%s)",
            firmware.version,
        )
        return
    if not mqtt_handler or not mqtt_handler.is_connected():
        logger.warning(
            "Skipping release announce: MQTT not connected (version=%s)",
            firmware.version,
        )
        return
    release_info = {
        "ota_id": _build_ota_id("release"),
        "version": firmware.version,
        "filename": firmware.filename,
        "url": firmware_url,
        "sha256": firmware.sha256,
        "size": firmware.file_size,
    }
    ok = mqtt_handler.publish_release_announcement(release_info)
    if not ok:
        logger.warning("Release announce publish failed for version=%s", firmware.version)


@app.route('/firmware/<filename>', methods=['GET'])
def download_firmware(filename):
    """
    펌웨어 파일 다운로드
    OCI URL 리다이렉트 또는 로컬 파일 다운로드를 제공한다.
    """
    try:
        firmware = Firmware.query.filter_by(filename=filename).first_or_404()
        if firmware.oci_uploaded:
            oci_url = _get_oci_object_url(firmware.filename)
            if oci_url:
                return redirect(oci_url)

        local_path = _local_firmware_path(firmware.filename)
        if local_path and os.path.isfile(local_path):
            return send_from_directory(
                Config.FIRMWARE_DIR,
                firmware.filename,
                as_attachment=True,
                download_name=firmware.filename,
            )
        return jsonify({'error': 'Firmware file not found'}), 404
    
    except Exception as e:
        logger.error(f"Error serving firmware: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/ota/verify', methods=['POST'])
def verify_ota_update():
    """
    OTA 업데이트 2차 검증 엔드포인트.
    실서버에서는 LLM 검증이 비활성화된 경우 RAUC 무결성 검증 결과를 그대로 승인한다.
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
            try:
                from llm_verifier import (
                    call_llm_verification,
                    preprocess_log,
                    save_verification_result,
                )
            except Exception as import_ex:
                logger.error("LLM verifier unavailable: %s", import_ex, exc_info=True)
                return jsonify({
                    "decision": "REJECT",
                    "reason": f"LLM verifier unavailable: {import_ex}. Fail-safe REJECT.",
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                }), 500

            preprocessed = preprocess_log(ota_log)
            result = call_llm_verification(preprocessed, model=Config.LLM_MODEL)
            try:
                save_verification_result(ota_log, result)
            except Exception as save_ex:
                logger.error("Failed to save LLM verification result: %s", save_ex, exc_info=True)
        else:
            result = {
                "decision": "APPROVE",
                "reason": "LLM verification disabled. Relying on RAUC integrity check only.",
            }

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
        
        original_name = file.filename or ''
        if not secure_filename(original_name):
            return jsonify({'error': 'Invalid original filename'}), 400

        filename = _build_upload_filename(
            original_name=original_name,
            version_str=version_str,
            custom_name=(request.form.get('filename') or '').strip(),
        )
        if not filename:
            return jsonify({'error': 'Invalid upload filename'}), 400
        if Config.PREFER_RAUCB_FIRMWARE and not _is_rauc_bundle_filename(filename):
            return jsonify({
                'error': 'Only .raucb firmware bundles are supported',
                'filename': filename,
            }), 400

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

        file.stream.seek(0)
        sha256, file_size = _stream_file_stats(file.stream)
        oci_uploaded = _upload_stream_to_oci(file.stream, filename)
        if oci_uploaded:
            object_ref = _get_oci_object_ref(filename)
        else:
            try:
                object_ref = _save_stream_to_local(file.stream, filename)
                logger.info("Stored firmware locally: %s", object_ref)
            except Exception as ex:
                logger.error("Failed to store firmware locally: %s", ex, exc_info=True)
                return jsonify({
                    'error': 'Failed to store firmware',
                    'detail': str(ex),
                }), 500
        
        # DB 반영 (신규 등록 또는 기존 버전 갱신)
        if existing_firmware:
            existing_firmware.filename = filename
            existing_firmware.file_path = object_ref
            existing_firmware.file_size = file_size
            existing_firmware.sha256 = sha256
            existing_firmware.release_notes = release_notes
            existing_firmware.is_active = True
            existing_firmware.oci_uploaded = oci_uploaded
            firmware = existing_firmware
            status_code = 200
            logger.info(f"Firmware replaced: {version_str} ({filename})")
        else:
            firmware = Firmware(
                version=version_str,
                filename=filename,
                file_path=object_ref,
                file_size=file_size,
                sha256=sha256,
                release_notes=release_notes,
                is_active=True,
                oci_uploaded=oci_uploaded,
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

        _announce_new_release(firmware)

        return jsonify({
            'success': True,
            'updated': bool(existing_firmware),
            'oci_uploaded': oci_uploaded,
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
        if Config.PREFER_RAUCB_FIRMWARE and not _is_rauc_bundle_filename(firmware.filename):
            return jsonify({
                'error': 'Only .raucb firmware bundles can be activated',
                'id': firmware.id,
                'version': firmware.version,
                'filename': firmware.filename,
            }), 409

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
        if Config.LOCAL_PROBE_ENABLED:
            _probe_local_devices_once(force=True)
        target_version = str(data.get('version') or '').strip() or None
        force_trigger = parse_bool(data.get('force'), default=False)
        cmd_topic = _format_cmd_topic(vehicle_id)
        firmware, error_response = _resolve_target_firmware(target_version)
        if error_response:
            return error_response

        vehicle, error_response = _validate_trigger_vehicle(vehicle_id, firmware, force_trigger, cmd_topic)
        if error_response:
            return error_response

        firmware_info, error_response = _build_trigger_firmware_info(vehicle_id, firmware)
        if error_response:
            logger.error(
                "Refusing trigger for %s: firmware version %s is not distributable.",
                vehicle_id,
                firmware.version,
            )
            return error_response

        signature_error = _attach_command_signature(vehicle_id, firmware_info)
        if signature_error:
            return signature_error

        sent_via, trigger_note, error_response = _publish_trigger_command(vehicle_id, firmware_info, cmd_topic)
        if error_response:
            return error_response

        _record_pending_trigger(vehicle, vehicle_id, firmware, trigger_note)
        logger.info(
            "Update command sent to %s: %s via %s",
            vehicle_id, firmware.version, sent_via
        )
        return jsonify({
            'success': True,
            'vehicle_id': vehicle_id,
            'version': firmware.version,
            'url': firmware_info['url'],
            'cmd_topic': cmd_topic,
            'transport': sent_via,
        })
    
    except Exception as e:
        logger.error(f"Error triggering update: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/v1/vehicles', methods=['GET'])
def list_vehicles():
    """차량 목록 조회"""
    try:
        if Config.LOCAL_PROBE_ENABLED:
            _probe_local_devices_once(force=True)
        vehicles = Vehicle.query.order_by(Vehicle.last_seen.desc()).all()
        now = datetime.utcnow()
        online_window_sec = int(Config.VEHICLE_ONLINE_WINDOW_SEC)

        def _to_vehicle_payload(vehicle: Vehicle) -> dict:
            payload = vehicle.to_dict()
            is_offline_status = str(vehicle.status or "").strip().lower() == "offline"
            if vehicle.last_seen:
                age_sec = max(0, int((now - vehicle.last_seen).total_seconds()))
                payload['last_seen_age_sec'] = age_sec
                payload['online_window_sec'] = online_window_sec
                payload['online'] = (not is_offline_status) and (age_sec <= online_window_sec)
            else:
                payload['last_seen_age_sec'] = None
                payload['online_window_sec'] = online_window_sec
                payload['online'] = False
            return payload

        return jsonify({
            'vehicles': [_to_vehicle_payload(v) for v in vehicles],
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

        payload = vehicle.to_dict()
        online_window_sec = int(Config.VEHICLE_ONLINE_WINDOW_SEC)
        is_offline_status = str(vehicle.status or "").strip().lower() == "offline"
        if vehicle.last_seen:
            age_sec = max(0, int((datetime.utcnow() - vehicle.last_seen).total_seconds()))
            payload['last_seen_age_sec'] = age_sec
            payload['online_window_sec'] = online_window_sec
            payload['online'] = (not is_offline_status) and (age_sec <= online_window_sec)
        else:
            payload['last_seen_age_sec'] = None
            payload['online_window_sec'] = online_window_sec
            payload['online'] = False
        return jsonify(payload), 200

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
