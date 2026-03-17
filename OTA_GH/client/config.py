"""
OTA Client - Configuration
환경 변수 및 설정 관리
"""
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()


class Config:
    """클라이언트 설정 클래스"""
    
    # ========== 서버 설정 ==========
    SERVER_URL = os.getenv('OTA_SERVER_URL', 'http://localhost:8080')
    # OTA_ERROR_REPORT_URL: 기본 리포트 전송 endpoint override (선택)
    ERROR_REPORT_URL = os.getenv('OTA_ERROR_REPORT_URL', '')
    # OTA_MONITOR_INGEST_URL: 관제 서버 ingest 미러 전송 endpoint (선택)
    MONITOR_INGEST_URL = os.getenv('OTA_MONITOR_INGEST_URL', '')
    
    # ========== 차량 정보 ==========
    VEHICLE_ID = os.getenv('VEHICLE_ID', 'vehicle_001')
    VEHICLE_BRAND = os.getenv('VEHICLE_BRAND', '')
    VEHICLE_SERIES = os.getenv('VEHICLE_SERIES', '')
    VEHICLE_SEGMENT = os.getenv('VEHICLE_SEGMENT', '')
    VEHICLE_FUEL = os.getenv('VEHICLE_FUEL', '')

    # ========== 리포트 컨텍스트(선택) ==========
    REGION_COUNTRY = os.getenv('OTA_REGION_COUNTRY', '')
    REGION_CITY = os.getenv('OTA_REGION_CITY', '')
    REGION_TIMEZONE = os.getenv('OTA_REGION_TIMEZONE', '')
    POWER_SOURCE = os.getenv('OTA_POWER_SOURCE', '')
    BATTERY_PCT = os.getenv('OTA_BATTERY_PCT', '')
    NETWORK_RSSI_DBM = os.getenv('OTA_NETWORK_RSSI_DBM', '')
    NETWORK_LATENCY_MS = os.getenv('OTA_NETWORK_LATENCY_MS', '')
    
    # ========== MQTT 설정 ==========
    MQTT_BROKER_HOST = os.getenv('MQTT_BROKER_HOST', 'localhost')
    MQTT_BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
    MQTT_QOS = int(os.getenv('MQTT_QOS', '2'))
    MQTT_USERNAME = os.getenv('MQTT_USERNAME', '')
    MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', '')
    
    # ========== 로컬 경로 ==========
    FIRMWARE_DIR = os.getenv('FIRMWARE_DIR', '/tmp/ota_downloads')
    CURRENT_VERSION_FILE = os.getenv('CURRENT_VERSION_FILE', './current_version.txt')
    INSTALL_DIR = os.getenv('INSTALL_DIR', './installed_app')
    
    # ========== 업데이트 설정 ==========
    UPDATE_CHECK_INTERVAL = int(os.getenv('UPDATE_CHECK_INTERVAL', '60'))
    INSTALL_MODE = os.getenv('INSTALL_MODE', 'file_copy')  # file_copy or systemd
    SERVICE_NAME = os.getenv('SERVICE_NAME', 'myapp.service')
    
    # ========== 동작 모드 ==========
    MODE = os.getenv('OTA_MODE', 'mqtt')  # mqtt or polling
    
    # ========== LLM 2차 검증 설정 ==========
    LLM_VERIFY_ENABLED = os.getenv('LLM_VERIFY', 'true').lower() in {
        '1', 'true', 'yes', 'y', 'on'
    }
    LLM_VERIFY_SERVER_URL = os.getenv('LLM_VERIFY_SERVER_URL', '')  # 빈 값이면 SERVER_URL 사용
    LLM_VERIFY_TIMEOUT = int(os.getenv('LLM_VERIFY_TIMEOUT', '30'))

    # ========== 로깅 설정 ==========
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
       
    # MQTT 토픽 템플릿
    @classmethod
    def get_topic_cmd(cls):
        return f'ota/{cls.VEHICLE_ID}/cmd'
    
    @classmethod
    def get_topic_status(cls):
        return f'ota/{cls.VEHICLE_ID}/status'
    
    @classmethod
    def get_topic_progress(cls):
        return f'ota/{cls.VEHICLE_ID}/progress'
    
    @classmethod
    def validate(cls):
        """설정 검증 및 디렉토리 생성"""
        os.makedirs(cls.FIRMWARE_DIR, exist_ok=True)
        os.makedirs(cls.INSTALL_DIR, exist_ok=True)
        return True
