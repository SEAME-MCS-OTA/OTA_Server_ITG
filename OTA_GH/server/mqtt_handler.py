"""
OTA Server - MQTT Handler
MQTT 브로커와의 통신 및 메시지 처리
"""
import json
import logging
import threading
from datetime import datetime
from typing import Optional

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
            
            # 토픽 파싱: ota/<vehicle_id>/<type>
            parts = topic.split('/')
            if len(parts) != 3 or parts[0] != 'ota':
                logger.warning(f"Invalid topic format: {topic}")
                return
            
            vehicle_id = parts[1]
            msg_type = parts[2]  # status or progress
            
            # JSON 파싱
            try:
                data = json.loads(payload) # JSON 문자열 → Python 객체(dict)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON payload: {e}")
                return
            
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
                    
                    if not incoming_status or not target_version:
                        logger.warning(f"Missing required fields in status message: {data}")
                        return
                    
                    # Vehicle upsert + status 반영
                    vehicle = self._upsert_vehicle(vehicle_id, default_status=incoming_status)
                    prev_version = vehicle.current_version
                    vehicle.last_seen = datetime.utcnow()

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
                    
                    if target_version is None:
                        logger.warning(f"Missing target_version in progress message: {data}")
                        return
                    
                    # Vehicle upsert + heartbeat 갱신
                    vehicle = self._upsert_vehicle(vehicle_id, default_status='downloading')
                    vehicle.last_seen = datetime.utcnow()

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
    
    def publish_update_command(self, vehicle_id: str, firmware_info: dict) -> bool:
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
            payload = json.dumps({
                "command": "update",
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
    
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        return self.connected
