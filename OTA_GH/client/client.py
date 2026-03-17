"""
OTA Client - 업데이트 클라이언트
"""
import os
import sys
import time
import json
import logging
import hashlib
import tarfile
import shutil
import tempfile
import subprocess
from datetime import datetime
from typing import Optional, Dict
from config import Config
import requests
import paho.mqtt.client as mqtt
from error_reporter import (
    OTAPhase,
    ErrorCode,
    classify_exception,
    classify_systemd_error,
    report_ota_error,
    report_ota_success,
)
from log_collector import OTALogCollector
from llm_verify_client import request_llm_verification

# 로깅 설정
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OTAClient:
    """OTA 업데이트 클라이언트"""
    
    def __init__(self):
        Config.validate()
        self.current_version = self._load_version()
        self.state = 'idle'
        self.mqtt_client = None
        self.log_collector: Optional[OTALogCollector] = None
        logger.info(f"Client initialized: {Config.VEHICLE_ID} v{self.current_version}")
        if Config.MONITOR_INGEST_URL:
            logger.info(f"Monitoring ingest mirror enabled: {Config.MONITOR_INGEST_URL}")
    
    # 버전 관리

    def _load_version(self) -> str:
        """현재 버전 로드"""
        try:
            if os.path.exists(Config.CURRENT_VERSION_FILE):
                with open(Config.CURRENT_VERSION_FILE, 'r') as f:
                    return f.read().strip()
        except Exception as e:
            logger.error(f"Version load error: {e}")
        
        # 기본 버전
        self._save_version('1.0.0')
        return '1.0.0'
    
    def _save_version(self, version: str):
        """버전 저장"""
        try:
            with open(Config.CURRENT_VERSION_FILE, 'w') as f:
                f.write(version)
            self.current_version = version
        except Exception as e:
            logger.error(f"Version save error: {e}")

    @staticmethod
    def _to_optional_int(value: str) -> Optional[int]:
        if value is None:
            return None
        text = str(value).strip()
        if text == '':
            return None
        try:
            return int(text)
        except ValueError:
            return None

    def _report_context_kwargs(self) -> Dict:
        kwargs: Dict = {}
        if Config.VEHICLE_BRAND:
            kwargs["vehicle_brand"] = Config.VEHICLE_BRAND
        if Config.VEHICLE_SERIES:
            kwargs["vehicle_series"] = Config.VEHICLE_SERIES
        if Config.VEHICLE_SEGMENT:
            kwargs["vehicle_segment"] = Config.VEHICLE_SEGMENT
        if Config.VEHICLE_FUEL:
            kwargs["vehicle_fuel"] = Config.VEHICLE_FUEL

        if Config.REGION_COUNTRY:
            kwargs["country"] = Config.REGION_COUNTRY
        if Config.REGION_CITY:
            kwargs["city"] = Config.REGION_CITY
        if Config.REGION_TIMEZONE:
            kwargs["tz_name"] = Config.REGION_TIMEZONE
        if Config.POWER_SOURCE:
            kwargs["power_source"] = Config.POWER_SOURCE

        battery_pct = self._to_optional_int(Config.BATTERY_PCT)
        rssi_dbm = self._to_optional_int(Config.NETWORK_RSSI_DBM)
        latency_ms = self._to_optional_int(Config.NETWORK_LATENCY_MS)

        if battery_pct is not None:
            kwargs["battery_pct"] = battery_pct
        if rssi_dbm is not None:
            kwargs["rssi_dbm"] = rssi_dbm
        if latency_ms is not None:
            kwargs["latency_ms"] = latency_ms

        return kwargs

    def _send_failure_log(
        self,
        phase: str,
        target_version: str,
        error_code: str,
        error_message: str,
        ota_log: Optional[list] = None,
        current_version: Optional[str] = None,
    ) -> None:
        try:
            report_ota_error(
                device_id=Config.VEHICLE_ID,
                current_version=current_version or self.current_version,
                target_version=target_version,
                phase=phase,
                error_code=error_code,
                error_message=error_message,
                server_url=Config.SERVER_URL,
                ota_log=ota_log or [],
                **self._report_context_kwargs(),
            )
        except Exception as ex:
            logger.debug(f"Failure report send failed: {ex}")

    def _send_success_log(
        self,
        phase: str,
        target_version: str,
        message: str,
        ota_log: Optional[list] = None,
        current_version: Optional[str] = None,
    ) -> None:
        try:
            report_ota_success(
                device_id=Config.VEHICLE_ID,
                current_version=current_version or self.current_version,
                target_version=target_version,
                phase=phase,
                server_url=Config.SERVER_URL,
                message=message,
                ota_log=ota_log or [],
                **self._report_context_kwargs(),
            )
        except Exception as ex:
            logger.debug(f"Success report send failed: {ex}")
    
    # 업데이트 확인 및 다운로드
    
    def check_for_updates(self) -> Optional[Dict]:
        """업데이트 확인"""
        try:
            url = f"{Config.SERVER_URL}/api/v1/update-check"
            params = {
                'vehicle_id': Config.VEHICLE_ID,
                'current_version': self.current_version
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status() # HTTP 오류 발생 시 예외 발생
            data = response.json()
            
            if data.get('update_available'):
                logger.info(f"Update available: {self.current_version} -> {data['version']}")
                return data
            else:
                logger.info("No update available")
                return None
                
        except Exception as e:
            logger.error(f"Update check failed: {e}")
            return None
    
    def download_firmware(self, firmware_info: Dict) -> Optional[str]:
        """펌웨어 다운로드"""
        try:
            self._report_status('downloading', firmware_info['version'])
            
            url = firmware_info['url']
            filename = url.split('/')[-1]
            filepath = os.path.join(Config.FIRMWARE_DIR, filename)
            
            logger.info(f"Downloading: {url}")
            
            response = requests.get(url, stream=True, timeout=30)   #stream=True: chunk 단위 다운로드
            response.raise_for_status()
            
            # 진행률 표시
            downloaded = 0
            file_size = firmware_info['size']
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        progress = int((downloaded / file_size) * 100) if file_size > 0 else 0
                        
                        if progress % 20 == 0:  # 20% 단위 리포트
                            self._report_progress(firmware_info['version'], progress)
            
            logger.info(f"Download complete: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            self._report_status('failed', firmware_info['version'], str(e))
            http_status = getattr(getattr(e, "response", None), "status_code", None)
            error_code = classify_exception(e, http_status=http_status)
            self._send_failure_log(
                phase=OTAPhase.DOWNLOAD,
                target_version=firmware_info.get('version', 'unknown'),
                error_code=error_code,
                error_message=str(e),
                ota_log=[
                    "DOWNLOAD START",
                    f"DOWNLOAD FAIL code={error_code}",
                ],
            )
            return None
    
    def verify_firmware(self, filepath: str, expected_sha256: str, target_version: str) -> bool:
        """SHA256 검증"""
        try:
            logger.info("Verifying firmware...")
            
            sha256_hash = hashlib.sha256()
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    sha256_hash.update(chunk)
            
            calculated = sha256_hash.hexdigest()
            
            if calculated == expected_sha256:
                logger.info("Verification successful")
                return True
            else:
                msg = f"SHA256 mismatch: expected {expected_sha256[:16]}..., got {calculated[:16]}..."
                logger.error(msg)
                self._send_failure_log(
                    phase=OTAPhase.VERIFY,
                    target_version=target_version,
                    error_code=ErrorCode.HASH_MISMATCH,
                    error_message=msg,
                    ota_log=[
                        "VERIFY START",
                        "VERIFY FAIL code=HASH_MISMATCH",
                    ],
                )
                return False
                
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            error_code = classify_exception(e)
            self._send_failure_log(
                phase=OTAPhase.VERIFY,
                target_version=target_version,
                error_code=error_code,
                error_message=str(e),
                ota_log=[
                    "VERIFY START",
                    f"VERIFY FAIL code={error_code}",
                ],
            )
            return False
    
    # 설치
    
    def install_firmware(self, filepath: str, version: str) -> bool:
        """펌웨어 설치"""
        try:
            self._report_status('installing', version)
            
            if Config.INSTALL_MODE == 'file_copy':
                success = self._install_file_copy(filepath, version)
            elif Config.INSTALL_MODE == 'systemd':
                success = self._install_systemd(filepath, version)
            else:
                logger.error(f"Unknown install mode: {Config.INSTALL_MODE}")
                self._send_failure_log(
                    phase=OTAPhase.INSTALL,
                    target_version=version,
                    error_code=ErrorCode.UNKNOWN,
                    error_message=f"Unknown install mode: {Config.INSTALL_MODE}",
                    ota_log=[
                        "INSTALL START",
                        "INSTALL FAIL code=UNKNOWN mode",
                    ],
                )
                return False
            
            return success
            
        except Exception as e:
            logger.error(f"Installation failed: {e}")
            error_code = classify_exception(e)
            self._send_failure_log(
                phase=OTAPhase.INSTALL,
                target_version=version,
                error_code=error_code,
                error_message=str(e),
                ota_log=[
                    "INSTALL START",
                    f"INSTALL FAIL code={error_code} exception",
                ],
            )
            return False
    
    def _install_file_copy(self, filepath: str, version: str, save_version: bool = True) -> bool:
        """파일 복사 설치"""
        backup_dir = None
        try:
            logger.info("Installing (file_copy mode)...")
            
            # 백업
            if os.path.exists(Config.INSTALL_DIR):
                backup_dir = f"{Config.INSTALL_DIR}_backup_{int(time.time())}"
                shutil.copytree(Config.INSTALL_DIR, backup_dir) # 설치 디렉토리를 통째로 복사
                logger.info(f"Backup created: {backup_dir}")
            
            # 압축 해제 및 업데이트
            with tempfile.TemporaryDirectory() as temp_dir:
                with tarfile.open(filepath, 'r:gz') as tar:
                    tar.extractall(path=temp_dir)
                
                if os.path.exists(Config.INSTALL_DIR):
                    shutil.rmtree(Config.INSTALL_DIR)   # 기존 설치 디렉토리를 완전히 삭제
                
                shutil.copytree(temp_dir, Config.INSTALL_DIR)
            
            if save_version:
                self._save_version(version)
            logger.info("Installation complete")
            return True
            
        except Exception as e:
            logger.error(f"Installation failed: {e}")
            error_code = classify_exception(e)
            
            # 롤백
            if backup_dir and os.path.exists(backup_dir):
                logger.info("Rolling back...")
                try:
                    if os.path.exists(Config.INSTALL_DIR):
                        shutil.rmtree(Config.INSTALL_DIR)
                    shutil.copytree(backup_dir, Config.INSTALL_DIR)
                    logger.info("Rollback successful")
                except Exception as rollback_error:
                    logger.error(f"Rollback failed: {rollback_error}")
            self._send_failure_log(
                phase=OTAPhase.INSTALL,
                target_version=version,
                error_code=error_code,
                error_message=str(e),
                ota_log=[
                    "INSTALL START",
                    f"INSTALL FAIL code={error_code} file_copy",
                ],
            )
            
            return False
    
    def _install_systemd(self, filepath: str, version: str) -> bool:
        """systemd 재시작 설치"""
        try:
            # 파일 복사
            if not self._install_file_copy(filepath, version, save_version=False):
                return False
            
            # 서비스 재시작
            logger.info(f"Restarting service: {Config.SERVICE_NAME}")
            
            result = subprocess.run(
                ['systemctl', 'restart', Config.SERVICE_NAME],  # 서비스 재시작 명령
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                logger.error(f"Service restart failed: {result.stderr}")
                code = classify_systemd_error(result.returncode, result.stderr, True)
                self._send_failure_log(
                    phase=OTAPhase.INSTALL,
                    target_version=version,
                    error_code=code,
                    error_message=result.stderr.strip() or "systemctl restart failed",
                    ota_log=[
                        "INSTALL START",
                        f"SERVICE RESTART FAIL code={code}",
                    ],
                )
                return False
            
            # 상태 확인
            time.sleep(2)
            result = subprocess.run(
                ['systemctl', 'is-active', Config.SERVICE_NAME],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.stdout.strip() == 'active':
                logger.info("Service active")
                self._save_version(version)
                return True
            else:
                logger.error(f"Service not active: {result.stdout}")
                code = classify_systemd_error(0, result.stdout, False)
                self._send_failure_log(
                    phase=OTAPhase.INSTALL,
                    target_version=version,
                    error_code=code,
                    error_message=result.stdout.strip() or "service is not active",
                    ota_log=[
                        "INSTALL START",
                        f"SERVICE HEALTH CHECK FAIL code={code}",
                    ],
                )
                return False
                
        except FileNotFoundError:
            logger.error("systemctl not found")
            self._send_failure_log(
                phase=OTAPhase.INSTALL,
                target_version=version,
                error_code=ErrorCode.SYSTEMD_UNIT_FAILED,
                error_message="systemctl not found",
                ota_log=[
                    "INSTALL START",
                    "INSTALL FAIL code=SYSTEMD_UNIT_FAILED systemctl not found",
                ],
            )
            return False
        except Exception as e:
            logger.error(f"systemd installation failed: {e}")
            error_code = classify_exception(e)
            self._send_failure_log(
                phase=OTAPhase.INSTALL,
                target_version=version,
                error_code=error_code,
                error_message=str(e),
                ota_log=[
                    "INSTALL START",
                    f"INSTALL FAIL code={error_code} systemd",
                ],
            )
            return False
    
    # 상태 리포트
    
    def _report_status(self, status: str, target_version: str, message: str = ''):
        """상태 리포트 (HTTP + MQTT)"""
        # HTTP 리포트
        # try:
        #     url = f"{Config.SERVER_URL}/api/v1/report"
        #     data = {
        #         'vehicle_id': Config.VEHICLE_ID,
        #         'target_version': target_version,
        #         'status': status,
        #         'message': message
        #     }
        #     requests.post(url, json=data, timeout=10)
        # except Exception as e:
        #     logger.debug(f"HTTP report failed: {e}")
        
        # MQTT 리포트
        if self.mqtt_client and self.mqtt_client.is_connected():
            try:
                payload = json.dumps({
                    'vehicle_id': Config.VEHICLE_ID,
                    'status': status,
                    'target_version': target_version,
                    'message': message,
                    'timestamp': datetime.utcnow().isoformat()
                })
                self.mqtt_client.publish(
                    Config.get_topic_status(),
                    payload,
                    qos=Config.MQTT_QOS
                )
            except Exception as e:
                logger.debug(f"MQTT status publish failed: {e}")
    
    def _report_progress(self, target_version: str, progress: int):
        """진행률 리포트 (MQTT)"""
        if self.mqtt_client and self.mqtt_client.is_connected():
            try:
                payload = json.dumps({
                    'vehicle_id': Config.VEHICLE_ID,
                    'target_version': target_version,
                    'progress': progress,
                    'message': f'Progress: {progress}%',
                    'timestamp': datetime.utcnow().isoformat()
                })
                self.mqtt_client.publish(
                    Config.get_topic_progress(),
                    payload,
                    qos=Config.MQTT_QOS
                )
            except Exception as e:
                logger.debug(f"MQTT progress publish failed: {e}")
    
    # 업데이트 실행
    
    def perform_update(self, firmware_info: Dict) -> bool:
        """전체 업데이트 프로세스 (LLM 2차 검증 포함)"""
        version = firmware_info['version']
        start_version = self.current_version

        # LLM 검증용 로그 수집기 초기화
        self.log_collector = OTALogCollector()
        self.log_collector.record_server_info(
            trigger_server=Config.SERVER_URL,
        )

        try:
            logger.info(f"Update started: {self.current_version} -> {version}")

            # 다운로드
            self.log_collector.mark_download_start()
            filepath = self.download_firmware(firmware_info)
            if not filepath:
                return False
            self.log_collector.mark_download_end()

            # 펌웨어 파일 정보 기록
            self.log_collector.record_firmware_file_info(
                filepath=filepath,
                expected_size=firmware_info.get('size', 0),
            )
            self.log_collector.record_bundle_hash(f"sha256:{firmware_info.get('sha256', '')}")

            # SHA256 검증
            self._report_status('verifying', version)
            if not self.verify_firmware(filepath, firmware_info['sha256'], version):
                self._report_status('failed', version, 'Verification failed')
                return False
            self.log_collector.record_signature_verification(True)

            # 설치
            if not self.install_firmware(filepath, version):
                self._report_status('failed', version, 'Installation failed')
                return False

            # RAUC 설치 결과 기록 (file_copy/systemd 모드에서는 RAUC를 직접 사용하지 않으므로 성공으로 기록)
            self.log_collector.record_rauc_result(
                exit_code=0,
                stdout="Installation completed successfully.",
                stderr="",
            )

            # ── [NEW] LLM 2차 검증 ──────────────────────────────
            if Config.LLM_VERIFY_ENABLED:
                logger.info("Requesting LLM secondary verification...")
                self._report_status('verifying', version, 'LLM secondary verification')

                verification_log = self.log_collector.build_verification_log(
                    current_version=start_version,
                    new_version=version,
                    vehicle_id=Config.VEHICLE_ID,
                )

                verify_result = request_llm_verification(
                    server_url=Config.LLM_VERIFY_SERVER_URL or Config.SERVER_URL,
                    ota_log=verification_log,
                    timeout=Config.LLM_VERIFY_TIMEOUT,
                )

                if verify_result["decision"] == "REJECT":
                    logger.warning(
                        f"LLM verification REJECTED: {verify_result['reason']}"
                    )
                    self._report_status(
                        'failed', version,
                        f"LLM REJECT: {verify_result['reason'][:200]}"
                    )
                    # 업데이트 이력에 REJECT 기록
                    OTALogCollector.save_update_history_entry(
                        from_version=start_version,
                        to_version=version,
                        result="REJECTED_BY_LLM",
                    )
                    self._send_failure_log(
                        phase=OTAPhase.VERIFY,
                        target_version=version,
                        error_code="LLM_REJECT",
                        error_message=verify_result['reason'][:500],
                        current_version=start_version,
                        ota_log=[
                            "DOWNLOAD OK",
                            "VERIFY OK",
                            "INSTALL OK",
                            f"LLM_VERIFY REJECT: {verify_result['reason'][:200]}",
                        ],
                    )
                    # 정리
                    try:
                        os.remove(filepath)
                    except Exception:
                        pass
                    return False

                logger.info("LLM verification APPROVED")
            # ── LLM 2차 검증 끝 ──────────────────────────────────

            # 완료
            self._report_status('completed', version)
            logger.info(f"Update completed: {version}")

            # 업데이트 이력 저장
            OTALogCollector.save_update_history_entry(
                from_version=start_version,
                to_version=version,
                result="SUCCESS",
            )

            self._send_success_log(
                phase=OTAPhase.INSTALL,
                target_version=version,
                message="OTA update completed",
                current_version=start_version,
                ota_log=[
                    "DOWNLOAD OK",
                    "VERIFY OK",
                    "INSTALL OK",
                    "LLM_VERIFY APPROVE" if Config.LLM_VERIFY_ENABLED else "LLM_VERIFY SKIPPED",
                    "REPORT OK",
                ],
            )

            # 정리
            try:
                os.remove(filepath)
            except Exception as e:
                logger.warning(f"Cleanup failed: {e}")

            return True

        except Exception as e:
            logger.error(f"Update failed: {e}")
            self._report_status('failed', version, str(e))
            error_code = classify_exception(e)
            self._send_failure_log(
                phase=OTAPhase.INSTALL,
                target_version=version,
                error_code=error_code,
                error_message=str(e),
                current_version=start_version,
                ota_log=[
                    "OTA START",
                    f"OTA FAIL code={error_code} perform_update",
                ],
            )
            return False
    
    # MQTT 통신
    
    def _init_mqtt(self):
        """MQTT 초기화"""
        self.mqtt_client = mqtt.Client(
            client_id=f"{Config.VEHICLE_ID}_client",
            protocol=mqtt.MQTTv311,
            clean_session=True
        )
        
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_message = self._on_mqtt_message
        
        if Config.MQTT_USERNAME and Config.MQTT_PASSWORD:
            self.mqtt_client.username_pw_set(
                Config.MQTT_USERNAME,
                Config.MQTT_PASSWORD
            )
        
        logger.info("MQTT initialized")
    
    def _connect_mqtt(self):
        """MQTT 연결"""
        logger.info(f"Connecting to MQTT: {Config.MQTT_BROKER_HOST}:{Config.MQTT_BROKER_PORT}")
        self.mqtt_client.connect(
            Config.MQTT_BROKER_HOST,
            Config.MQTT_BROKER_PORT,
            keepalive=60
        )
        self.mqtt_client.loop_start()
    
    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """MQTT 연결 콜백"""
        if rc == 0:
            logger.info("MQTT connected")
            client.subscribe(Config.get_topic_cmd(), Config.MQTT_QOS)
            logger.info(f"Subscribed to {Config.get_topic_cmd()}")
        else:
            logger.error(f"MQTT connection failed: {rc}")
    
    def _on_mqtt_message(self, client, userdata, msg):
        """MQTT 메시지 수신"""
        try:
            data = json.loads(msg.payload.decode('utf-8'))
            logger.info(f"MQTT command: {data.get('command')}")

            # LLM 검증용 MQTT 명령 이력 기록
            if self.log_collector:
                self.log_collector.record_mqtt_command(
                    topic=msg.topic,
                    payload_summary=data.get('command', str(data)[:100]),
                )

            if data.get('command') == 'update':
                firmware_info = data.get('firmware')
                if firmware_info:
                    self.perform_update(firmware_info)

        except Exception as e:
            logger.error(f"MQTT message error: {e}")
    
    # 실행 모드
    
    def run_polling_mode(self):
        """Polling 모드"""
        logger.info(f"Polling mode (interval: {Config.UPDATE_CHECK_INTERVAL}s)")
        
        try:
            while True:
                firmware_info = self.check_for_updates()
                if firmware_info:
                    self.perform_update(firmware_info)
                
                time.sleep(Config.UPDATE_CHECK_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("Stopped by user")
    
    def run_mqtt_mode(self):
        """MQTT 모드"""
        logger.info("MQTT mode")
        
        try:
            self._init_mqtt()
            self._connect_mqtt()
            
            # 서버에 등록
            logger.info("Registering with server...")
            self.check_for_updates()  # ← 추가!
            logger.info("Waiting for commands... (Ctrl+C to exit)")
            
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Stopped by user")
        finally:
            if self.mqtt_client:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
    
    def run(self):
        """메인 실행"""
        logger.info(f"OTA Client v{self.current_version}")
        logger.info(f"Vehicle: {Config.VEHICLE_ID}")
        logger.info(f"Mode: {Config.MODE}")
        logger.info(f"Install: {Config.INSTALL_MODE}")
        
        if Config.MODE == 'mqtt':
            self.run_mqtt_mode()
        elif Config.MODE == 'polling':
            self.run_polling_mode()
        else:
            logger.error(f"Unknown mode: {Config.MODE}")
            sys.exit(1)


if __name__ == '__main__':
    client = OTAClient()
    client.run()
