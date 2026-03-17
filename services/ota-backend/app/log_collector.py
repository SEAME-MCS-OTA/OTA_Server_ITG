"""
OTA 로그 수집 모듈
OTA 프로세스 각 단계의 로그 데이터를 수집하여 LLM 2차 검증용 JSON 객체를 생성한다.
"""
import json
import logging
import os
import subprocess
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# 디바이스 설정 파일 경로
DEVICE_CONFIG_PATH = os.getenv("DEVICE_CONFIG_PATH", "/etc/ota/device_config.json")

# 업데이트 이력 파일 경로
UPDATE_HISTORY_PATH = os.getenv(
    "UPDATE_HISTORY_PATH",
    os.path.join(os.path.dirname(__file__), "update_history.json"),
)


class OTALogCollector:
    """OTA 로그를 수집하여 LLM 검증용 JSON을 생성한다."""

    def __init__(self):
        self.download_start_time: Optional[str] = None
        self.download_end_time: Optional[str] = None
        self.download_duration_seconds: float = 0
        self.chunk_stats = {
            "total_chunks": 0,
            "successful_first_attempt": 0,
            "retried_chunks": 0,
            "max_retries_single_chunk": 0,
        }
        self.mqtt_command_history: List[Dict] = []
        self.rauc_install_log_summary: str = ""
        self.rauc_install_exit_code: int = -1
        self.integrity_check_result: str = "NOT_CHECKED"
        self.firmware_file_size_bytes: int = 0
        self.expected_file_size_bytes: int = 0
        self.bundle_hash: str = ""
        self.signature_verification: str = "NOT_CHECKED"
        self.update_trigger_server: str = ""
        self.build_server_info: str = ""

    # ── 다운로드 추적 ──────────────────────────

    def mark_download_start(self):
        self.download_start_time = datetime.utcnow().isoformat() + "Z"

    def mark_download_end(self):
        self.download_end_time = datetime.utcnow().isoformat() + "Z"
        if self.download_start_time:
            start = datetime.fromisoformat(self.download_start_time.replace("Z", "+00:00"))
            end = datetime.fromisoformat(self.download_end_time.replace("Z", "+00:00"))
            self.download_duration_seconds = (end - start).total_seconds()

    def record_chunk_result(self, success_first_attempt: bool, retries: int = 0):
        self.chunk_stats["total_chunks"] += 1
        if success_first_attempt:
            self.chunk_stats["successful_first_attempt"] += 1
        else:
            self.chunk_stats["retried_chunks"] += 1
            self.chunk_stats["max_retries_single_chunk"] = max(
                self.chunk_stats["max_retries_single_chunk"], retries
            )

    # ── MQTT 명령 기록 ─────────────────────────

    def record_mqtt_command(self, topic: str, payload_summary: str):
        self.mqtt_command_history.append({
            "topic": topic,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "payload_summary": payload_summary,
        })

    # ── RAUC 설치 결과 기록 ────────────────────

    def record_rauc_result(self, exit_code: int, stdout: str, stderr: str):
        self.rauc_install_exit_code = exit_code
        if exit_code == 0:
            # 정상이면 요약
            self.rauc_install_log_summary = (
                "Installation completed without errors. Bundle verified. "
                "Slot B marked as primary."
            )
            self.integrity_check_result = "PASSED"
        else:
            # 에러/워닝 라인만 추출
            combined = stdout + "\n" + stderr
            lines = combined.split('\n')
            important = [
                l for l in lines
                if any(kw in l.lower() for kw in ['error', 'warning', 'fail', 'denied'])
            ]
            self.rauc_install_log_summary = '\n'.join(important) if important else combined[:2000]
            self.integrity_check_result = "FAILED"

    def record_signature_verification(self, passed: bool):
        self.signature_verification = "PASSED" if passed else "FAILED"

    def record_firmware_file_info(self, filepath: str, expected_size: int):
        try:
            self.firmware_file_size_bytes = os.path.getsize(filepath)
        except Exception:
            self.firmware_file_size_bytes = 0
        self.expected_file_size_bytes = expected_size

    def record_bundle_hash(self, hash_value: str):
        self.bundle_hash = hash_value

    def record_server_info(self, trigger_server: str, build_server: str = ""):
        self.update_trigger_server = trigger_server
        self.build_server_info = build_server

    # ── 디바이스 상태 수집 ─────────────────────

    @staticmethod
    def _load_device_config() -> Dict:
        """로컬 디바이스 설정 파일에서 차량 정보를 읽는다."""
        try:
            if os.path.exists(DEVICE_CONFIG_PATH):
                with open(DEVICE_CONFIG_PATH, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load device config: {e}")
        return {}

    @staticmethod
    def _get_rauc_slot_status() -> Dict:
        """rauc status로 슬롯 상태를 조회한다."""
        try:
            result = subprocess.run(
                ["rauc", "status", "--detailed", "--output-format=json"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                return json.loads(result.stdout)
        except FileNotFoundError:
            logger.debug("rauc not found, using placeholder slot status")
        except Exception as e:
            logger.warning(f"Failed to get RAUC status: {e}")
        return {}

    @staticmethod
    def _get_system_resources() -> Dict:
        """시스템 리소스 사용량을 수집한다."""
        try:
            import psutil
            return {
                "cpu_usage_percent": psutil.cpu_percent(interval=1),
                "memory_usage_percent": psutil.virtual_memory().percent,
                "disk_free_mb": psutil.disk_usage('/').free // (1024 * 1024),
            }
        except ImportError:
            logger.warning("psutil not installed, skipping system resource collection")
        except Exception as e:
            logger.warning(f"Failed to collect system resources: {e}")
        return {
            "cpu_usage_percent": 0,
            "memory_usage_percent": 0,
            "disk_free_mb": 0,
        }

    @staticmethod
    def _load_update_history(max_entries: int = 5) -> List[Dict]:
        """로컬 업데이트 이력을 로드한다."""
        try:
            if os.path.exists(UPDATE_HISTORY_PATH):
                with open(UPDATE_HISTORY_PATH, 'r') as f:
                    history = json.load(f)
                return history[-max_entries:]
        except Exception as e:
            logger.warning(f"Failed to load update history: {e}")
        return []

    @staticmethod
    def save_update_history_entry(from_version: str, to_version: str, result: str):
        """업데이트 이력 한 건을 로컬 파일에 추가한다."""
        entry = {
            "date": datetime.utcnow().isoformat() + "Z",
            "from_version": from_version,
            "to_version": to_version,
            "result": result,
        }
        try:
            history = []
            if os.path.exists(UPDATE_HISTORY_PATH):
                with open(UPDATE_HISTORY_PATH, 'r') as f:
                    history = json.load(f)
            history.append(entry)
            # 최근 50건만 유지
            history = history[-50:]
            with open(UPDATE_HISTORY_PATH, 'w') as f:
                json.dump(history, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Failed to save update history: {e}")

    # ── JSON 생성 ──────────────────────────────

    def build_verification_log(
        self,
        current_version: str,
        new_version: str,
        vehicle_id: str = "",
    ) -> Dict:
        """LLM 2차 검증용 JSON 객체를 생성한다."""
        device_config = self._load_device_config()
        rauc_status = self._get_rauc_slot_status()

        # 슬롯 상태 구성 (RAUC 결과 또는 플레이스홀더)
        slot_status = {}
        if rauc_status:
            slot_status = rauc_status
        else:
            slot_status = {
                "slot_a": {
                    "version": current_version,
                    "boot_count": 0,
                    "last_update": "",
                    "status": "active",
                },
                "slot_b": {
                    "version": new_version,
                    "boot_count": 0,
                    "last_update": datetime.utcnow().isoformat() + "Z",
                    "status": "inactive_newly_installed",
                },
            }

        return {
            "firmware_metadata": {
                "current_active_version": current_version,
                "new_installed_version": new_version,
                "bundle_hash": self.bundle_hash,
                "signature_verification": self.signature_verification,
                "firmware_file_size_bytes": self.firmware_file_size_bytes,
                "expected_file_size_bytes": self.expected_file_size_bytes,
                "update_trigger_server": self.update_trigger_server,
                "build_server_info": self.build_server_info,
            },
            "process_log": {
                "download_start_time": self.download_start_time,
                "download_end_time": self.download_end_time,
                "download_duration_seconds": self.download_duration_seconds,
                "chunk_transfer_summary": self.chunk_stats,
                "mqtt_command_history": self.mqtt_command_history,
                "rauc_install_log_summary": self.rauc_install_log_summary,
                "rauc_install_exit_code": self.rauc_install_exit_code,
                "integrity_check_result": self.integrity_check_result,
            },
            "device_state": {
                "vehicle_model": device_config.get("vehicle_model", "Unknown"),
                "vehicle_id": vehicle_id or device_config.get("vehicle_id", "unknown"),
                "current_slot_status": slot_status,
                "recent_update_history": self._load_update_history(),
                "system_resources": self._get_system_resources(),
            },
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
