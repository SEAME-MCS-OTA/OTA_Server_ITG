"""
OTA Server - MQTT Handler
MQTT 브로커와의 통신 및 메시지 처리
"""
import json
import logging
import threading
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt

from models import db, Vehicle, UpdateHistory
from config import Config
from monitoring_reporter import publish_update_result, should_report_final_status

logger = logging.getLogger(__name__)


class MQTTHandler:
    """MQTT 메시지 처리 핸들러"""
    
    def __init__(self, app_context):
        """
        Args:
            app_context: Flask 애플리케이션 컨텍스트
        """
        self.app_context = app_context
        self.client: Optional[mqtt.Client] = None
        self.connected = False
        self._lock = threading.Lock()
        
        # MQTT 클라이언트 초기화
        self._init_client()
    
    def _init_client(self):
        """MQTT 클라이언트 초기화"""
        try:
            # MQTT 클라이언트 생성 (paho-mqtt 1.x/2.x 호환)
            kwargs = {
                "client_id": Config.MQTT_CLIENT_ID,
                "protocol": mqtt.MQTTv311,
                "clean_session": True,
            }
            callback_api = getattr(mqtt, "CallbackAPIVersion", None)
            if callback_api is not None:
                try:
                    self.client = mqtt.Client(callback_api_version=callback_api.VERSION1, **kwargs)
                except Exception:
                    self.client = mqtt.Client(**kwargs)
            else:
                self.client = mqtt.Client(**kwargs)
            
            # 콜백 설정
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message
            
            # 인증 설정 (있는 경우)
            if Config.MQTT_USERNAME and Config.MQTT_PASSWORD:
                self.client.username_pw_set(
                    Config.MQTT_USERNAME,
                    Config.MQTT_PASSWORD
                )
            
            logger.info("MQTT client initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize MQTT client: {e}")
            raise
    
    def connect(self):
        """MQTT 브로커에 연결"""
        try:
            logger.info(
                f"Connecting to MQTT broker at {Config.MQTT_BROKER_HOST}:{Config.MQTT_BROKER_PORT}"
            )
            self.client.connect(
                Config.MQTT_BROKER_HOST,
                Config.MQTT_BROKER_PORT,
                Config.MQTT_KEEPALIVE
            )
            # 백그라운드 루프 시작
            self.client.loop_start()
            logger.info("MQTT connection initiated")
            
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            raise
    
    def disconnect(self):
        """MQTT 브로커 연결 해제"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            self.connected = False
            logger.info("MQTT client disconnected")
    
    def _on_connect(self, client, userdata, flags, rc):
        """연결 성공 콜백"""
        if rc == 0:
            self.connected = True
            logger.info("Successfully connected to MQTT broker")
            
            # OTA 상태/진행 + 차량 등록 토픽 구독
            topics = [
                ("ota/+/status", Config.MQTT_QOS),
                ("ota/+/progress", Config.MQTT_QOS),
                (Config.MQTT_TOPIC_VEHICLE_REGISTER, Config.MQTT_QOS),
            ]
            
            for topic, qos in topics:
                result = client.subscribe(topic, qos)
                logger.info(f"Subscribed to topic: {topic} with QoS {qos}, result: {result}")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code: {rc}")
            self.connected = False
    
    def _on_disconnect(self, client, userdata, rc):
        """연결 해제 콜백"""
        self.connected = False
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnection, return code: {rc}")
        else:
            logger.info("MQTT client disconnected cleanly")
    
    def _on_message(self, client, userdata, msg):
        """메시지 수신 콜백"""
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            
            logger.info(f"Received message on topic '{topic}': {payload}")

            # JSON 파싱
            try:
                data = json.loads(payload)  # JSON 문자열 → Python 객체(dict)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON payload: {e}")
                return

            # Register 토픽은 일반 ota/<vehicle_id>/<type> 포맷이 아니어도 처리
            if topic == Config.MQTT_TOPIC_VEHICLE_REGISTER:
                self._handle_register_message(data)
                return
            
            # 토픽 파싱: ota/<vehicle_id>/<type>
            parts = topic.split('/')
            if len(parts) != 3 or parts[0] != 'ota':
                logger.warning(f"Invalid topic format: {topic}")
                return
            
            vehicle_id = parts[1]
            msg_type = parts[2]  # status or progress
            
            # 메시지 타입별 처리
            if msg_type == 'status':
                self._handle_status_message(vehicle_id, data)
            elif msg_type == 'progress':
                self._handle_progress_message(vehicle_id, data)
            else:
                logger.warning(f"Unknown message type: {msg_type}")
                
        except Exception as e:
            logger.error(f"Error processing MQTT message: {e}", exc_info=True)

    def _upsert_vehicle(self, vehicle_id: str, default_status: str = 'idle') -> Vehicle:
        vehicle = Vehicle.query.filter_by(vehicle_id=vehicle_id).first()
        if vehicle:
            return vehicle
        vehicle = Vehicle(vehicle_id=vehicle_id, status=default_status)
        db.session.add(vehicle)
        logger.info(f"Registered new vehicle from MQTT: {vehicle_id}")
        return vehicle

    @staticmethod
    def _extract_ip(data: dict) -> Optional[str]:
        if not isinstance(data, dict):
            return None
        for candidate in (
            data.get("ip"),
            data.get("ip_address"),
            ((data.get("context") or {}).get("network") or {}).get("ip"),
        ):
            text = str(candidate or "").strip()
            if text:
                return text
        return None

    @staticmethod
    def _parse_message_timestamp(data: dict) -> Optional[datetime]:
        if not isinstance(data, dict):
            return None
        raw = str(data.get("timestamp") or "").strip()
        if not raw:
            return None
        try:
            normalized = raw.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        except Exception:
            return None

    @staticmethod
    def _presence_stale_threshold_sec() -> int:
        try:
            return max(30, int(Config.VEHICLE_ONLINE_WINDOW_SEC))
        except Exception:
            return 300

    @staticmethod
    def _in_progress_recovery_timeout_sec(status: str = "") -> int:
        try:
            online_window = max(30, int(Config.VEHICLE_ONLINE_WINDOW_SEC))
        except Exception:
            online_window = 300

        status_norm = str(status or "").strip().lower()
        if status_norm == "pending":
            # A published trigger should transition quickly.
            # If the device stays idle for over ~90 seconds, the command
            # likely never started and retriggering should not be blocked.
            return 90
        return max(1800, online_window * 6)

    @staticmethod
    def _is_in_progress_status(status: str) -> bool:
        return str(status or "").strip().lower() in {"pending", "downloading", "verifying", "installing"}

    @staticmethod
    def _extract_current_version(data: dict) -> str:
        if not isinstance(data, dict):
            return ""
        for candidate in (
            data.get("current_version"),
            ((data.get("ota") or {}).get("current_version")),
        ):
            text = str(candidate or "").strip()
            if text:
                return text
        return ""

    @staticmethod
    def _append_status_note(existing: str, note: str) -> str:
        base = str(existing or "").strip()
        extra = str(note or "").strip()
        if not extra:
            return base
        if not base:
            return extra
        if extra in base:
            return base
        return f"{base} | {extra}"

    def _latest_in_progress_history(self, vehicle_id: str) -> Optional[UpdateHistory]:
        return (
            UpdateHistory.query.filter_by(vehicle_id=vehicle_id)
            .filter(UpdateHistory.status.in_(("pending", "downloading", "verifying", "installing")))
            .order_by(UpdateHistory.updated_at.desc(), UpdateHistory.started_at.desc())
            .first()
        )

    def _should_preserve_in_progress_status(
        self,
        vehicle_id: str,
        prev_status: str,
        incoming_status: str,
        reported_version: str = "",
    ) -> bool:
        if incoming_status not in {"idle", "offline"}:
            return False
        if not self._is_in_progress_status(prev_status):
            return False

        history = self._latest_in_progress_history(vehicle_id)
        if history is None:
            logger.info(
                "Allowing presence overwrite without active update history: vehicle_id=%s prev=%s new=%s",
                vehicle_id,
                prev_status,
                incoming_status,
            )
            return False

        now = datetime.utcnow()
        last_activity = history.updated_at or history.started_at or now
        age_sec = max(0.0, (now - last_activity).total_seconds())
        recovery_timeout_sec = self._in_progress_recovery_timeout_sec(str(history.status or prev_status))
        if age_sec <= recovery_timeout_sec:
            logger.info(
                "Ignoring in-progress presence overwrite: vehicle_id=%s prev=%s new=%s age=%.1fs timeout=%.1fs",
                vehicle_id,
                prev_status,
                incoming_status,
                age_sec,
                recovery_timeout_sec,
            )
            return True

        recovered_status = "failed"
        target_version = str(history.target_version or "").strip()
        if reported_version and target_version and target_version == reported_version:
            recovered_status = "completed"
            history.progress = 100

        history.status = recovered_status
        history.completed_at = now
        history.message = self._append_status_note(
            history.message,
            f"Recovered stale in-progress state via presence={incoming_status} age={age_sec:.1f}s",
        )
        logger.warning(
            "Recovered stale in-progress state: vehicle_id=%s prev=%s new=%s recovered=%s age=%.1fs",
            vehicle_id,
            prev_status,
            incoming_status,
            recovered_status,
            age_sec,
        )
        return False

    def _handle_register_message(self, data: dict):
        """
        Vehicle register 메시지 처리.
        Payload 예시:
        {
          "vehicle_id": "vw-ivi-0026",
          "current_version": "1.0.2",
          "ip": "192.168.86.22",
          "message": "REGISTER_ON_RELEASE"
        }
        """
        try:
            with self.app_context():
                try:
                    vehicle_id = str(data.get("vehicle_id") or data.get("device_id") or "").strip()
                    if not vehicle_id:
                        logger.warning("Missing vehicle_id in register message: %s", data)
                        return

                    current_version = str(data.get("current_version") or "").strip()
                    status = str(data.get("status") or "idle").strip() or "idle"
                    ip_addr = self._extract_ip(data)
                    timestamp = self._parse_message_timestamp(data)
                    now = datetime.utcnow()
                    if timestamp is not None:
                        age_sec = (now - timestamp).total_seconds()
                        if age_sec > self._presence_stale_threshold_sec():
                            logger.info(
                                "Ignoring stale register message: vehicle_id=%s age=%.1fs status=%s",
                                vehicle_id,
                                age_sec,
                                status,
                            )
                            return

                    vehicle = self._upsert_vehicle(vehicle_id, default_status=status)
                    prev_status = str(vehicle.status or "").strip().lower()
                    if self._should_preserve_in_progress_status(
                        vehicle_id,
                        prev_status,
                        status,
                        current_version or str(vehicle.current_version or "").strip(),
                    ):
                        # Keep recent in-progress state to avoid flicker during live updates.
                        pass
                    else:
                        vehicle.status = status
                    vehicle.last_seen = now
                    if current_version:
                        vehicle.current_version = current_version
                    if ip_addr:
                        vehicle.last_ip = ip_addr

                    db.session.commit()
                    logger.info(
                        "Vehicle registered from MQTT announce response: %s ip=%s version=%s",
                        vehicle_id,
                        ip_addr or "-",
                        current_version or "-",
                    )
                except Exception as e:
                    db.session.rollback()
                    logger.error(f"Database error in register handler: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error in register message handler: {e}", exc_info=True)
    
    def _handle_status_message(self, vehicle_id: str, data: dict):
        """
        Status 메시지 처리
        
        Payload 예시:
        {
            "vehicle_id": "vehicle_001",
            "status": "downloading|verifying|installing|completed|failed",
            "target_version": "1.0.1",
            "message": "optional error message"
        }
        """
        try:
            # Flask 애플리케이션 컨텍스트 내에서 DB 작업 수행
            with self.app_context():
                try:
                    status = str(data.get('status') or '').strip().lower()
                    target_version = str(data.get('target_version') or '').strip()
                    message = data.get('message', '')
                    timestamp = self._parse_message_timestamp(data)
                    now = datetime.utcnow()

                    if not status:
                        logger.warning(f"Missing required fields in status message: {data}")
                        return

                    presence_statuses = {'idle', 'online', 'offline'}
                    is_presence = status in presence_statuses
                    if (not is_presence) and (not target_version):
                        logger.warning(f"Missing target_version in status message: {data}")
                        return

                    existing_vehicle = Vehicle.query.filter_by(vehicle_id=vehicle_id).first()
                    if status == 'offline' and existing_vehicle is None:
                        # Ignore ghost LWT updates from never-registered devices.
                        logger.info("Ignoring offline status for unknown vehicle_id=%s", vehicle_id)
                        return

                    # Vehicle upsert + status 반영
                    vehicle = existing_vehicle or self._upsert_vehicle(vehicle_id, default_status=status)
                    prev_version = vehicle.current_version
                    prev_status = str(vehicle.status or "").strip().lower()
                    ip_addr = self._extract_ip(data)
                    reported_version = self._extract_current_version(data)

                    if is_presence and timestamp is not None:
                        age_sec = (now - timestamp).total_seconds()
                        if age_sec > self._presence_stale_threshold_sec():
                            logger.info(
                                "Ignoring stale presence status: vehicle_id=%s status=%s age=%.1fs",
                                vehicle_id,
                                status,
                                age_sec,
                            )
                            db.session.commit()
                            return

                    vehicle.last_seen = now
                    if ip_addr:
                        vehicle.last_ip = ip_addr
                    if reported_version:
                        vehicle.current_version = reported_version

                    if is_presence and self._should_preserve_in_progress_status(
                        vehicle_id,
                        prev_status,
                        status,
                        reported_version or str(vehicle.current_version or "").strip(),
                    ):
                        pass
                    else:
                        vehicle.status = status

                    # completed 상태면 current_version 업데이트
                    if status == 'completed' and target_version:
                        vehicle.current_version = target_version

                    if is_presence:
                        db.session.commit()
                        logger.info(
                            "Updated vehicle %s presence status='%s' (message=%s)",
                            vehicle_id,
                            status,
                            str(message or "").strip() or "-",
                        )
                        return

                    # UpdateHistory 업데이트
                    history = UpdateHistory.query.filter_by(
                        vehicle_id=vehicle_id,
                        target_version=target_version
                    ).order_by(UpdateHistory.started_at.desc()).first()
                    prev_history_status = history.status if history else None

                    if history:
                        history.status = status
                        history.message = message
                        if status in ['completed', 'failed']:
                            history.completed_at = datetime.utcnow()
                            if status == 'completed':
                                history.progress = 100
                    else:
                        # 히스토리가 없으면 생성
                        history = UpdateHistory(
                            vehicle_id=vehicle_id,
                            from_version=prev_version,
                            target_version=target_version,
                            status=status,
                            message=message
                        )
                        db.session.add(history)

                    db.session.commit()
                    logger.info(
                        f"Updated vehicle {vehicle_id} status to '{status}' "
                        f"for version {target_version}"
                    )

                    if should_report_final_status(status, prev_history_status):
                        publish_update_result(
                            vehicle_id=vehicle_id,
                            target_version=target_version,
                            status=status,
                            message=message,
                            current_version=vehicle.current_version,
                            from_version=(history.from_version if history else prev_version),
                            progress=history.progress if history else None,
                            source="mqtt_status",
                            status_payload=data,
                        )
                        
                except Exception as e:
                    db.session.rollback()
                    logger.error(f"Database error in status handler: {e}", exc_info=True)
                    
        except Exception as e:
            logger.error(f"Error in status message handler: {e}", exc_info=True)
    
    def _handle_progress_message(self, vehicle_id: str, data: dict):
        """
        Progress 메시지 처리
        
        Payload 예시:
        {
            "vehicle_id": "vehicle_001",
            "target_version": "1.0.1",
            "progress": 45,  # 0-100
            "message": "Downloading... 45%"
        }
        """
        try:
            with self.app_context():
                try:
                    target_version = data.get('target_version')
                    progress = data.get('progress', 0)
                    message = data.get('message', '')
                    
                    if target_version is None:
                        logger.warning(f"Missing target_version in progress message: {data}")
                        return
                    
                    # Vehicle upsert + heartbeat 갱신
                    vehicle = self._upsert_vehicle(vehicle_id, default_status='downloading')
                    vehicle.last_seen = datetime.utcnow()
                    ip_addr = self._extract_ip(data)
                    if ip_addr:
                        vehicle.last_ip = ip_addr

                    # UpdateHistory 업데이트
                    history = UpdateHistory.query.filter_by(
                        vehicle_id=vehicle_id,
                        target_version=target_version
                    ).order_by(UpdateHistory.started_at.desc()).first()
                    
                    if history:
                        history.progress = min(100, max(0, progress))  # 0-100 범위 제한
                        if message:
                            history.message = message
                    else:
                        history = UpdateHistory(
                            vehicle_id=vehicle_id,
                            from_version=vehicle.current_version,
                            target_version=target_version,
                            status='downloading',
                            progress=min(100, max(0, progress)),
                            message=message
                        )
                        db.session.add(history)

                    db.session.commit()
                    logger.debug(
                        f"Updated progress for vehicle {vehicle_id}: "
                        f"{progress}% (version {target_version})"
                    )
                        
                except Exception as e:
                    db.session.rollback()
                    logger.error(f"Database error in progress handler: {e}", exc_info=True)
                    
        except Exception as e:
            logger.error(f"Error in progress message handler: {e}", exc_info=True)
    
    def publish_update_command(self, vehicle_id: str, firmware_info: dict, ota_id: Optional[str] = None) -> bool:
        """
        차량에 업데이트 명령 발행
        
        Args:
            vehicle_id: 차량 ID
            firmware_info: 펌웨어 정보 딕셔너리
                {
                    "version": "1.0.1",
                    "url": "http://...",
                    "sha256": "...",
                    "size": 123456,
                    "release_notes": "..."
                }
        
        Returns:
            bool: 발행 성공 여부
        """
        if not self.connected:
            logger.error("Cannot publish: MQTT client not connected")
            return False
        
        try:
            topic = Config.MQTT_TOPIC_CMD.format(vehicle_id=vehicle_id)
            resolved_ota_id = str(ota_id or firmware_info.get("ota_id") or "").strip()
            payload = json.dumps({
                "command": "update",
                "ota_id": resolved_ota_id,
                "firmware": firmware_info,
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # QoS 2로 발행 (Exactly Once)
            result = self.client.publish(topic, payload, qos=Config.MQTT_QOS)
            
            # 발행 대기 (blocking)
            result.wait_for_publish()
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info(f"Published update command to {vehicle_id}: {firmware_info['version']}")
                return True
            else:
                logger.error(f"Failed to publish update command: {result.rc}")
                return False
                
        except Exception as e:
            logger.error(f"Error publishing update command: {e}", exc_info=True)
            return False

    def publish_release_announcement(self, firmware_info: dict) -> bool:
        """
        업로드된 새 릴리즈를 차량(announce 구독자)에게 브로드캐스트.
        """
        if not self.connected:
            logger.error("Cannot publish release announce: MQTT client not connected")
            return False

        try:
            topic = Config.MQTT_TOPIC_RELEASE_ANNOUNCE
            payload = json.dumps(
                {
                    "event": "release_available",
                    "release_id": str(firmware_info.get("ota_id") or ""),
                    "version": str(firmware_info.get("version") or ""),
                    "filename": str(firmware_info.get("filename") or ""),
                    "url": str(firmware_info.get("url") or ""),
                    "sha256": str(firmware_info.get("sha256") or ""),
                    "size": int(firmware_info.get("size") or 0),
                    "published_at": datetime.utcnow().isoformat(),
                }
            )
            result = self.client.publish(
                topic,
                payload,
                qos=Config.MQTT_QOS,
                retain=bool(Config.MQTT_ANNOUNCE_RETAIN),
            )
            result.wait_for_publish()
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info("Published release announce: topic=%s version=%s", topic, firmware_info.get("version"))
                return True
            logger.error("Failed to publish release announce: rc=%s", result.rc)
            return False
        except Exception as e:
            logger.error("Error publishing release announce: %s", e, exc_info=True)
            return False
    
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        return self.connected
