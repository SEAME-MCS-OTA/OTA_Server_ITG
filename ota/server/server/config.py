"""
OTA Server - Configuration
환경 변수 및 설정 관리
"""
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _env_first(*names: str, default: str = '') -> str:
    for name in names:
        raw = os.getenv(name)
        if raw is not None:
            return str(raw).strip()
    return str(default).strip()


def _env_bool_first(*names: str, default: bool = False) -> bool:
    for name in names:
        raw = os.getenv(name)
        if raw is not None:
            return str(raw).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}
    return default


def _mqtt_transport() -> str:
    raw = str(os.getenv('MQTT_TRANSPORT', 'tcp') or '').strip().lower()
    if raw in {'ws', 'wss', 'websocket', 'websockets'}:
        return 'websockets'
    return 'tcp'


def _mqtt_ws_path() -> str:
    raw = str(os.getenv('MQTT_WS_PATH', '/mqtt') or '').strip() or '/mqtt'
    return raw if raw.startswith('/') else f'/{raw}'


class Config:
    """서버 설정 클래스"""
    
    # Flask 설정
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
    
    # 데이터베이스 설정
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'ota_db')
    DB_USER = os.getenv('DB_USER', 'ota_user')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'ota_password')
    
    SQLALCHEMY_DATABASE_URI = (
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = DEBUG
    
    # 서버 설정
    HOST = os.getenv('HOST', '0.0.0.0')
    PORT = int(os.getenv('PORT', '8080'))
    FIRMWARE_BASE_URL = os.getenv('FIRMWARE_BASE_URL', '').rstrip('/')
    
    # 펌웨어 저장 경로
    FIRMWARE_DIR = os.getenv('FIRMWARE_DIR', './firmware_files')
    CLIENT_LOG_DIR = os.getenv('CLIENT_LOG_DIR', './client_logs')
    COMMAND_SIGNING_ENABLED = _env_bool_first(
        'COMMAND_SIGNING_ENABLED',
        'COMMAND_SIGN_ENABLED',
        default=True,
    )
    COMMAND_SIGNING_PRIVATE_KEY_PATH = _env_first(
        'COMMAND_SIGNING_PRIVATE_KEY_PATH',
        'COMMAND_SIGN_KEY_PATH',
        default='',
    )
    COMMAND_SIGNING_KEY_ID = _env_first(
        'COMMAND_SIGNING_KEY_ID',
        'COMMAND_SIGN_KEY_ID',
        default='ota-ed25519-v1',
    )
    
    # MQTT 설정
    MQTT_BROKER_HOST = os.getenv('MQTT_BROKER_HOST', 'localhost')
    MQTT_BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
    MQTT_TRANSPORT = _mqtt_transport()
    MQTT_WS_PATH = _mqtt_ws_path()
    MQTT_CLIENT_ID = os.getenv('MQTT_CLIENT_ID', 'ota-server')
    MQTT_USERNAME = os.getenv('MQTT_USERNAME', '')
    MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', '')
    MQTT_KEEPALIVE = int(os.getenv('MQTT_KEEPALIVE', '60'))
    MQTT_QOS = int(os.getenv('MQTT_QOS', 2))
    MQTT_TLS_ENABLED = _env_bool('MQTT_TLS_ENABLED', default=False)
    MQTT_TLS_INSECURE = _env_bool('MQTT_TLS_INSECURE', default=False)
    MQTT_CA_CERTS = os.getenv('MQTT_CA_CERTS', '')
    MQTT_CERTFILE = os.getenv('MQTT_CERTFILE', '')
    MQTT_KEYFILE = os.getenv('MQTT_KEYFILE', '')
    
    # MQTT 토픽 템플릿
    MQTT_TOPIC_CMD = 'ota/{vehicle_id}/cmd'
    MQTT_TOPIC_STATUS = 'ota/{vehicle_id}/status'
    MQTT_TOPIC_PROGRESS = 'ota/{vehicle_id}/progress'
    MQTT_TOPIC_RELEASE_ANNOUNCE = os.getenv('MQTT_TOPIC_RELEASE_ANNOUNCE', 'ota/releases/announce')
    MQTT_TOPIC_VEHICLE_REGISTER = os.getenv('MQTT_TOPIC_VEHICLE_REGISTER', 'ota/vehicles/register')
    MQTT_ANNOUNCE_RETAIN = _env_bool('MQTT_ANNOUNCE_RETAIN', False)

    # OTA 결과 관제 서버(OTA_VLM) 연동
    MONITORING_INGEST_URL = os.getenv('MONITORING_INGEST_URL', '').strip()
    MONITORING_TIMEOUT_SEC = float(os.getenv('MONITORING_TIMEOUT_SEC', '3.0'))
    MONITORING_DEVICE_MODEL = os.getenv('MONITORING_DEVICE_MODEL', 'raspberrypi4')
    MONITORING_OTA_TYPE = os.getenv('MONITORING_OTA_TYPE', 'RAUCB')
    MONITORING_VEHICLE_BRAND = os.getenv('MONITORING_VEHICLE_BRAND', 'Volkswagen')
    MONITORING_VEHICLE_SERIES = os.getenv('MONITORING_VEHICLE_SERIES', 'ID.5')
    MONITORING_VEHICLE_SEGMENT = os.getenv('MONITORING_VEHICLE_SEGMENT', 'C')
    MONITORING_VEHICLE_FUEL = os.getenv('MONITORING_VEHICLE_FUEL', 'EV')
    MONITORING_REGION_COUNTRY = os.getenv('MONITORING_REGION_COUNTRY', 'DE')
    MONITORING_REGION_CITY = os.getenv('MONITORING_REGION_CITY', 'Wolfsburg')
    MONITORING_REGION_TIMEZONE = os.getenv('MONITORING_REGION_TIMEZONE', 'Europe/Berlin')

    # Trigger 정책
    REQUIRE_RECENT_VEHICLE = os.getenv('REQUIRE_RECENT_VEHICLE', 'true').lower() in {
        '1', 'true', 'yes', 'y', 'on'
    }
    VEHICLE_ONLINE_WINDOW_SEC = int(os.getenv('VEHICLE_ONLINE_WINDOW_SEC', '60'))
    PREFER_RAUCB_FIRMWARE = os.getenv('PREFER_RAUCB_FIRMWARE', 'true').lower() in {
        '1', 'true', 'yes', 'y', 'on'
    }

    # Local device probe fallback (MQTT heartbeat 보조 경로)
    # Format: "vehicle_id@ip:port,vehicle_id2@ip2:port2"
    # Example: "vw-ivi-0026@192.168.86.23:8080"
    LOCAL_DEVICE_MAP = os.getenv('LOCAL_DEVICE_MAP', 'vw-ivi-0026@192.168.86.23:8080').strip()
    LOCAL_PROBE_ENABLED = _env_bool('LOCAL_PROBE_ENABLED', default=False)
    LOCAL_PROBE_INTERVAL_SEC = int(os.getenv('LOCAL_PROBE_INTERVAL_SEC', '5'))
    LOCAL_PROBE_TIMEOUT_SEC = float(os.getenv('LOCAL_PROBE_TIMEOUT_SEC', '1.5'))
    LOCAL_TRIGGER_FIRST = os.getenv('LOCAL_TRIGGER_FIRST', 'true').lower() in {
        '1', 'true', 'yes', 'y', 'on'
    }
    MQTT_COMMAND_ONLY = _env_bool('MQTT_COMMAND_ONLY', default=True)
    
    # LLM 2차 검증 설정
    LLM_VERIFICATION_ENABLED = os.getenv('LLM_VERIFY', 'true').lower() in {
        '1', 'true', 'yes', 'y', 'on'
    }
    LLM_MODEL = os.getenv('LLM_MODEL', 'claude-sonnet-4-20250514')
    LLM_LOG_PATH = os.getenv(
        'LLM_LOG_PATH',
        os.path.join('.', 'logs', 'llm-verifier.log'),
    )

    # 로깅 설정
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def validate(cls):
        """설정 검증"""
        if not os.path.exists(cls.FIRMWARE_DIR):
            os.makedirs(cls.FIRMWARE_DIR)
            print(f"Created firmware directory: {cls.FIRMWARE_DIR}")
        if not os.path.exists(cls.CLIENT_LOG_DIR):
            os.makedirs(cls.CLIENT_LOG_DIR)
            print(f"Created client log directory: {cls.CLIENT_LOG_DIR}")
        llm_log_dir = os.path.dirname(cls.LLM_LOG_PATH)
        if llm_log_dir and not os.path.exists(llm_log_dir):
            os.makedirs(llm_log_dir)
            print(f"Created LLM log directory: {llm_log_dir}")
        
        return True
