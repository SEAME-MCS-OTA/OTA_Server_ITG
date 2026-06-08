"""
OTA Server - MQTT Handler
MQTT 브로커와의 통신 및 메시지 처리
"""
import json
import logging
import threading
from datetime import datetime
from typing import Any, Callable, Dict, Optional

import paho.mqtt.client as mqtt

from models import db, Vehicle, UpdateHistory
from config import Config
from monitoring_reporter import publish_update_result, should_report_final_status

logger = logging.getLogger(__name__)


def normalize_completed_status_by_version(
    status: str,
    prev_version: str,
    target_version: str,
    message: str = "",
) -> tuple[str, str]:
    """
    Preserve the client-reported terminal status.

    Same-version OTA can be valid when the user explicitly forced a reinstall.
    The trigger path already decides whether same-version updates are allowed, so
    result handling must not downgrade a completed report only because the
    version string is unchanged. Malformed terminal reports are still
    fail-closed when the target version is missing.
    """
    status_norm = str(status or "").strip().lower()
    msg = str(message or "")
    target = str(target_version or "").strip()
    if status_norm == "completed" and not target:
        reason = "target_version missing"
        return "failed", f"{msg} | {reason}".strip(" |")
    return status_norm, msg


class MQTTHandler:
    """MQTT 메시지 처리 핸들러"""
    
    def __init__(
        self,
        app_context,
        on_update_request: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ):
        """
        Args:
            app_context: Flask 애플리케이션 컨텍스트
        """
        self.app_context = app_context
        self.on_update_request = on_update_request
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
                "transport": Config.MQTT_TRANSPORT,
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

            if Config.MQTT_TRANSPORT == "websockets":
                self.client.ws_set_options(path=Config.MQTT_WS_PATH)

            if Config.MQTT_TLS_ENABLED:
                tls_kwargs = {}
                if Config.MQTT_CA_CERTS:
                    tls_kwargs["ca_certs"] = Config.MQTT_CA_CERTS
                if Config.MQTT_CERTFILE:
                    tls_kwargs["certfile"] = Config.MQTT_CERTFILE
                if Config.MQTT_KEYFILE:
                    tls_kwargs["keyfile"] = Config.MQTT_KEYFILE
                if tls_kwargs:
                    self.client.tls_set(**tls_kwargs)
                else:
                    self.client.tls_set()
                if Config.MQTT_TLS_INSECURE:
                    self.client.tls_insecure_set(True)
            
            # 인증 설정 (있는 경우)
            if Config.MQTT_USERNAME and Config.MQTT_PASSWORD:
                self.client.username_pw_set(
                    Config.MQTT_USERNAME,
                    Config.MQTT_PASSWORD
                )
            
            logger.info(
                "MQTT client initialized transport=%s ws_path=%s tls=%s",
                Config.MQTT_TRANSPORT,
                Config.MQTT_WS_PATH if Config.MQTT_TRANSPORT == "websockets" else "-",
                "on" if Config.MQTT_TLS_ENABLED else "off",
            )
            
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
            
            # 모든 차량의 status/progress 토픽 구독
            # 와일드카드 사용: ota/+/status, ota/+/progress
            topics = [
                ("ota/+/status", Config.MQTT_QOS),
                ("ota/+/progress", Config.MQTT_QOS),
            ]
            register_topic = str(getattr(Config, "MQTT_TOPIC_VEHICLE_REGISTER", "") or "").strip()
            if register_topic:
                topics.append((register_topic, Config.MQTT_QOS))
            
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

            try:
                data = json.loads(payload) # JSON 문자열 → Python 객체(dict)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON payload: {e}")
                return
            
            register_topic = str(getattr(Config, "MQTT_TOPIC_VEHICLE_REGISTER", "") or "").strip()
            if register_topic and topic == register_topic:
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

    def _handle_update_request_trigger(self, vehicle_id: str, data: dict):
        try:
            if self.on_update_request is None:
                logger.debug("No update-request callback configured; ignoring register trigger")
                return
            with self.app_context():
                self.on_update_request(vehicle_id, data if isinstance(data, dict) else {})
        except Exception as e:
            logger.error("Error in update-request trigger handler: %s", e, exc_info=True)

    def _handle_register_message(self, data: dict):
        try:
            with self.app_context():
                vehicle_id = str(data.get("vehicle_id") or data.get("device_id") or "").strip()
                if not vehicle_id:
                    logger.warning("Missing vehicle_id in register message: %s", data)
                    return

                current_version = str(data.get("current_version") or "").strip()
                status = str(data.get("status") or "idle").strip() or "idle"
                ip_addr = str(data.get("ip") or "").strip()

                vehicle = self._upsert_vehicle(vehicle_id, default_status=status)
                vehicle.status = status
                vehicle.last_seen = datetime.utcnow()
                if current_version:
                    vehicle.current_version = current_version
                if ip_addr:
                    vehicle.last_ip = ip_addr
                db.session.commit()
                logger.info(
                    "Vehicle registered from MQTT: %s ip=%s version=%s",
                    vehicle_id,
                    ip_addr or "-",
                    current_version or "-",
                )

                trigger = str(data.get("trigger") or "").strip().lower()
                if trigger in {"ui_update_request", "update_request", "request_update"}:
                    logger.info(
                        "Received register update-request trigger: vehicle_id=%s trigger=%s version=%s release_id=%s",
                        vehicle_id,
                        trigger,
                        str(data.get("version") or data.get("target_version") or "").strip() or "-",
                        str(data.get("release_id") or data.get("ota_id") or "").strip() or "-",
                    )
                    threading.Thread(
                        target=self._handle_update_request_trigger,
                        args=(vehicle_id, data if isinstance(data, dict) else {}),
                        daemon=True,
                        name=f"mqtt-update-request-{vehicle_id}",
                    ).start()
        except Exception as e:
            db.session.rollback()
            logger.error("Error in register message handler: %s", e, exc_info=True)

    def _upsert_vehicle(self, vehicle_id: str, default_status: str = 'idle') -> Vehicle:
        vehicle = Vehicle.query.filter_by(vehicle_id=vehicle_id).first()
        if vehicle:
            return vehicle
        vehicle = Vehicle(vehicle_id=vehicle_id, status=default_status)
        db.session.add(vehicle)
        logger.info(f"Registered new vehicle from MQTT: {vehicle_id}")
        return vehicle
    
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
                    incoming_status = data.get('status')
                    target_version = data.get('target_version')
                    message = data.get('message', '')
                    payload_ip = str(
                        ((data.get('context') or {}).get('network') or {}).get('ip')
                        or data.get('ip')
                        or ''
                    ).strip()
                    
                    if not incoming_status or not target_version:
                        logger.warning(f"Missing required fields in status message: {data}")
                        return
                    
                    # Vehicle upsert + status 반영
                    vehicle = self._upsert_vehicle(vehicle_id, default_status=incoming_status)
                    prev_version = vehicle.current_version
                    vehicle.last_seen = datetime.utcnow()
                    if payload_ip:
                        vehicle.last_ip = payload_ip

                    status, message = normalize_completed_status_by_version(
                        incoming_status,
                        prev_version=str(prev_version or ""),
                        target_version=str(target_version or ""),
                        message=message,
                    )
                    vehicle.status = status

                    # 보정 이후 completed 상태일 때만 current_version 업데이트
                    if status == 'completed':
                        vehicle.current_version = target_version

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
                    payload_ip = str(
                        ((data.get('context') or {}).get('network') or {}).get('ip')
                        or data.get('ip')
                        or ''
                    ).strip()
                    
                    if target_version is None:
                        logger.warning(f"Missing target_version in progress message: {data}")
                        return
                    
                    # Vehicle upsert + heartbeat 갱신
                    vehicle = self._upsert_vehicle(vehicle_id, default_status='downloading')
                    vehicle.last_seen = datetime.utcnow()
                    if payload_ip:
                        vehicle.last_ip = payload_ip

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
    
    def publish_update_command(self, vehicle_id: str, firmware_info: dict, ota_id: str = "") -> bool:
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
            payload_obj = {
                "command": "update",
                "firmware": firmware_info,
                "timestamp": datetime.utcnow().isoformat()
            }
            if ota_id:
                payload_obj["ota_id"] = str(ota_id).strip()
            payload = json.dumps(payload_obj)
            
            # QoS 2로 발행 (Exactly Once)
            result = self.client.publish(topic, payload, qos=Config.MQTT_QOS)
            
            # 발행 대기 (blocking)
            result.wait_for_publish()
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info(
                    "Published update command to %s: %s ota_id=%s",
                    vehicle_id,
                    firmware_info['version'],
                    ota_id or '-',
                )
                return True
            else:
                logger.error(f"Failed to publish update command: {result.rc}")
                return False
                
        except Exception as e:
            logger.error(f"Error publishing update command: {e}", exc_info=True)
            return False

    def publish_release_announce(self, announce_info: dict) -> bool:
        if not self.connected:
            logger.error("Cannot publish announce: MQTT client not connected")
            return False

        topic = str(getattr(Config, "MQTT_TOPIC_RELEASE_ANNOUNCE", "") or "").strip()
        if not topic:
            logger.error("Cannot publish announce: MQTT_TOPIC_RELEASE_ANNOUNCE is empty")
            return False

        try:
            payload = json.dumps(announce_info)
            result = self.client.publish(topic, payload, qos=Config.MQTT_QOS)
            result.wait_for_publish()
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info(
                    "Published release announce: vehicle_id=%s version=%s release_id=%s topic=%s",
                    str(announce_info.get("vehicle_id") or "").strip() or "-",
                    str(announce_info.get("version") or announce_info.get("target_version") or "").strip() or "-",
                    str(announce_info.get("release_id") or announce_info.get("ota_id") or "").strip() or "-",
                    topic,
                )
                return True
            logger.error("Failed to publish release announce: %s", result.rc)
            return False
        except Exception as e:
            logger.error("Error publishing release announce: %s", e, exc_info=True)
            return False

    def publish_release_announcement(self, firmware_info: dict) -> bool:
        # Backward-compatible alias for mixed app/mqtt_handler deployments.
        return self.publish_release_announce(firmware_info)
    
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        return self.connected
