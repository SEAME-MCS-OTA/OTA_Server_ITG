"""Minimal MQTT bridge for request consume and decision publish."""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Dict, Optional

import paho.mqtt.client as mqtt

from config import Config
from llm_service import normalize_request

logger = logging.getLogger(__name__)


class LLMMQTTBridge:
    def __init__(self, *, on_request: Callable[[Dict[str, Any]], Dict[str, Any]]):
        self._on_request = on_request
        self._client: Optional[mqtt.Client] = None
        self._connected = False

    def is_connected(self) -> bool:
        return bool(self._connected)

    def start(self) -> None:
        if not Config.LLM_MQTT_BRIDGE_ENABLED:
            logger.info("LLM MQTT bridge disabled")
            return

        kwargs = {
            "client_id": Config.LLM_MQTT_CLIENT_ID,
            "protocol": mqtt.MQTTv311,
            "clean_session": True,
        }

        callback_api = getattr(mqtt, "CallbackAPIVersion", None)
        if callback_api is not None:
            try:
                client = mqtt.Client(callback_api_version=callback_api.VERSION1, **kwargs)
            except Exception:
                client = mqtt.Client(**kwargs)
        else:
            client = mqtt.Client(**kwargs)

        if Config.MQTT_USERNAME and Config.MQTT_PASSWORD:
            client.username_pw_set(Config.MQTT_USERNAME, Config.MQTT_PASSWORD)

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message

        client.connect(
            Config.MQTT_BROKER_HOST,
            Config.MQTT_BROKER_PORT,
            Config.LLM_MQTT_KEEPALIVE,
        )
        client.loop_start()
        self._client = client

    def stop(self) -> None:
        if self._client is None:
            return
        self._client.loop_stop()
        self._client.disconnect()
        self._connected = False

    def _on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            self._connected = False
            logger.error("MQTT connect failed rc=%s", rc)
            return

        self._connected = True
        client.subscribe(Config.LLM_MQTT_TOPIC_REQUEST, qos=Config.LLM_MQTT_QOS)
        logger.info(
            "LLM MQTT bridge connected, subscribed: %s",
            Config.LLM_MQTT_TOPIC_REQUEST,
        )

    def _on_disconnect(self, client, userdata, rc):
        self._connected = False
        logger.warning("MQTT disconnected rc=%s", rc)

    def _on_message(self, client, userdata, msg):
        try:
            raw = (msg.payload or b"").decode("utf-8", errors="replace")
            payload = json.loads(raw)
            req = normalize_request(payload)
            decision = self._on_request(req)
            self.publish_decision(decision, req)
        except Exception as exc:
            logger.exception("MQTT request handling failed: %s", exc)

    def publish_decision(self, decision: Dict[str, Any], req: Dict[str, Any]) -> bool:
        if self._client is None:
            return False

        response_topic = str(req.get("response_topic") or "").strip()
        if response_topic:
            topic = response_topic
        else:
            topic = str(Config.LLM_MQTT_TOPIC_DECISION_TEMPLATE).format(
                vehicle_id=req.get("vehicle_id", "unknown")
            )

        payload = json.dumps(decision, ensure_ascii=False)
        result = self._client.publish(
            topic,
            payload,
            qos=Config.LLM_MQTT_QOS,
            retain=bool(Config.LLM_MQTT_DECISION_RETAIN),
        )
        ok = result.rc == mqtt.MQTT_ERR_SUCCESS
        if not ok:
            logger.error("Failed publish decision topic=%s rc=%s", topic, result.rc)
        return ok

    def publish_request(self, payload: Dict[str, Any]) -> bool:
        if self._client is None or not self._connected:
            return False

        message = json.dumps(payload, ensure_ascii=False)
        result = self._client.publish(
            Config.LLM_MQTT_TOPIC_REQUEST,
            message,
            qos=Config.LLM_MQTT_QOS,
            retain=False,
        )
        ok = result.rc == mqtt.MQTT_ERR_SUCCESS
        if not ok:
            logger.error(
                "Failed publish request topic=%s rc=%s",
                Config.LLM_MQTT_TOPIC_REQUEST,
                result.rc,
            )
        return ok
