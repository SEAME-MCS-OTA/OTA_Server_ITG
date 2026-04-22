import json
import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock


HERE = os.path.dirname(__file__)
SERVER_DIR = os.path.abspath(os.path.join(HERE, ".."))
if SERVER_DIR not in sys.path:
    sys.path.insert(0, SERVER_DIR)

from config import Config  # noqa: E402
from mqtt_handler import MQTTHandler  # noqa: E402


class MQTTHandlerMessageTests(unittest.TestCase):
    def _build_handler(self):
        handler = object.__new__(MQTTHandler)
        handler._handle_register_message = Mock()
        handler._handle_status_message = Mock()
        handler._handle_progress_message = Mock()
        return handler

    def test_on_message_dispatches_register_after_json_parse(self):
        handler = self._build_handler()
        msg = SimpleNamespace(
            topic=Config.MQTT_TOPIC_VEHICLE_REGISTER,
            payload=json.dumps(
                {
                    "vehicle_id": "vw-ivi-0026",
                    "trigger": "ui_update_request",
                    "version": "3.3.9",
                }
            ).encode("utf-8"),
        )

        handler._on_message(None, None, msg)

        handler._handle_register_message.assert_called_once_with(
            {
                "vehicle_id": "vw-ivi-0026",
                "trigger": "ui_update_request",
                "version": "3.3.9",
            }
        )
        handler._handle_status_message.assert_not_called()
        handler._handle_progress_message.assert_not_called()

    def test_on_message_dispatches_status_with_parsed_json(self):
        handler = self._build_handler()
        msg = SimpleNamespace(
            topic="ota/vw-ivi-0026/status",
            payload=json.dumps(
                {
                    "vehicle_id": "vw-ivi-0026",
                    "status": "idle",
                    "target_version": "3.3.8",
                }
            ).encode("utf-8"),
        )

        handler._on_message(None, None, msg)

        handler._handle_status_message.assert_called_once_with(
            "vw-ivi-0026",
            {
                "vehicle_id": "vw-ivi-0026",
                "status": "idle",
                "target_version": "3.3.8",
            },
        )
        handler._handle_register_message.assert_not_called()
        handler._handle_progress_message.assert_not_called()


if __name__ == "__main__":
    unittest.main()
