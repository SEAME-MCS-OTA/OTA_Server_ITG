import os
import json
import logging
import socket
import threading
import time
import struct
import subprocess
import re
import glob
from datetime import datetime
from typing import Dict, Any, Optional, Callable, Tuple
from urllib.parse import urlparse
from flask import Flask, jsonify, request

try:
    import fcntl  # type: ignore
except Exception:
    fcntl = None  # type: ignore

try:
    import paho.mqtt.client as mqtt  # type: ignore
except Exception:
    mqtt = None  # type: ignore

from .ota_logic import (
    build_event,
    download_with_retries,
    cleanup_old_bundles,
    rauc_status_json,
    parse_rauc_status,
    rauc_install,
    rauc_mark_good,
    state,
    PHASE_DOWNLOAD,
    PHASE_APPLY,
    PHASE_REBOOT,
    PHASE_COMMIT,
    EVENT_START,
    EVENT_OK,
    EVENT_FAIL,
    load_config,
    _write_event,
    _post_event,
    start_queue_flusher,
)
from .mqtt_utils import parse_mqtt_update_command
from .log_collector import OTALogCollector
from .llm_verify_client import request_llm_verification

app = Flask(__name__)
logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _append_runtime_log(line: str) -> None:
    try:
        os.makedirs("/data/log/ui", exist_ok=True)
        ts = datetime.now().astimezone().isoformat(timespec="seconds")
        with open("/data/log/ui/ota-backend-requests.log", "a", encoding="utf-8") as f:
            f.write(f"{ts} {line}\n")
    except Exception:
        pass


@app.before_request
def _log_request():
    _append_runtime_log(f"{request.method} {request.path}")


@app.after_request
def _add_cors_headers(resp):
    # Allow QML (qrc/file origin) and local tools to call backend endpoints.
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


def _normalize_ip(raw: str) -> Optional[str]:
    ip = str(raw or "").strip()
    if not ip or ip.startswith("127."):
        return None
    return ip


def _first_ipv4_from_text(text: str) -> Optional[str]:
    # Accept first valid IPv4 token from arbitrary command output.
    for match in re.findall(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", str(text or "")):
        try:
            socket.inet_aton(match)
        except OSError:
            continue
        ip = _normalize_ip(match)
        if ip:
            return ip
    return None


def _ip_tool_candidates() -> list[str]:
    candidates = ["/usr/sbin/ip", "/sbin/ip", "/usr/bin/ip", "/bin/ip", "ip"]
    seen = set()
    resolved = []
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        if c == "ip" or os.path.exists(c):
            resolved.append(c)
    return resolved

def _default_iface() -> Optional[str]:
    try:
        with open("/proc/net/route", "r", encoding="utf-8") as f:
            for line in f.readlines()[1:]:
                cols = line.strip().split()
                if len(cols) < 11:
                    continue
                iface, destination, flags = cols[0], cols[1], cols[3]
                if destination != "00000000":
                    continue
                if (int(flags, 16) & 0x2) == 0:
                    continue
                return iface
    except Exception:
        pass
    return None

def _iface_ipv4(iface: str) -> Optional[str]:
    if fcntl is None:
        return None
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        packed = struct.pack("256s", iface[:15].encode("utf-8"))
        res = fcntl.ioctl(sock.fileno(), 0x8915, packed)  # SIOCGIFADDR
        ip = _normalize_ip(socket.inet_ntoa(res[20:24]))
        if ip:
            return ip
    except Exception:
        return None
    finally:
        sock.close()
    return None

def _first_cmd_ip() -> Optional[str]:
    # Try multiple commands to support both full iproute2 and minimal userspace.
    commands = []
    for ip_tool in _ip_tool_candidates():
        commands.append([ip_tool, "-4", "addr", "show", "scope", "global"])
        commands.append([ip_tool, "-4", "addr", "show"])
        commands.append([ip_tool, "addr", "show"])
    commands.append(["hostname", "-I"])
    for cmd in commands:
        try:
            out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
            ip = _first_ipv4_from_text(out)
            if ip:
                return ip
        except Exception:
            continue
    return None


def _fib_trie_ip() -> Optional[str]:
    # Fallback for images where ip/ifconfig options are limited.
    try:
        with open("/proc/net/fib_trie", "r", encoding="utf-8") as f:
            ip = _first_ipv4_from_text(f.read())
            if ip:
                return ip
    except Exception:
        pass
    return None

def _get_ip_and_source() -> tuple[str, str]:
    cmd_ip = _first_cmd_ip()
    if cmd_ip:
        return cmd_ip, "cmd"

    # Prefer interface IPs (works without internet egress).
    seen = set()
    candidates = []
    dflt = _default_iface()
    if dflt:
        candidates.append(dflt)
    candidates.extend(["wlan0", "eth0"])

    for iface in candidates:
        if not iface or iface in seen:
            continue
        seen.add(iface)
        ip = _iface_ipv4(iface)
        if ip:
            return ip, f"ioctl:{iface}"

    # Fallback: route-based local address.
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = _normalize_ip(s.getsockname()[0])
        s.close()
        if ip:
            return ip, "route-socket"
    except Exception:
        pass

    # Last resort.
    try:
        ip = _normalize_ip(socket.gethostbyname(socket.gethostname()))
        if ip:
            return ip, "hostname"
    except Exception:
        pass

    fib_ip = _fib_trie_ip()
    if fib_ip:
        return fib_ip, "fib-trie"

    return "-", "none"


def _get_ip() -> str:
    return _get_ip_and_source()[0]


def _slot_from_hint(raw: str) -> Optional[str]:
    s = str(raw or "").strip().lower()
    if not s:
        return None

    if s in ("a", "slot-a", "rootfsa", "rootfs.0", "rootfs0"):
        return "A"
    if s in ("b", "slot-b", "rootfsb", "rootfs.1", "rootfs1"):
        return "B"

    if "rootfsa" in s or "rootfs.0" in s:
        return "A"
    if "rootfsb" in s or "rootfs.1" in s:
        return "B"

    # Common A/B partition layout used in this image: p2=A, p3=B
    if re.search(r"(?:^|/)mmcblk\d+p2(?:\D|$)", s) or re.search(r"(?:^|/)sd[a-z]2(?:\D|$)", s):
        return "A"
    if re.search(r"(?:^|/)mmcblk\d+p3(?:\D|$)", s) or re.search(r"(?:^|/)sd[a-z]3(?:\D|$)", s):
        return "B"

    # PARTUUID layout used by Raspberry Pi cmdline (e.g. PARTUUID=xxxx-02 / xxxx-03)
    if re.search(r"partuuid=[0-9a-f-]+-0*2(?:\D|$)", s) or re.search(r"/by-partuuid/[0-9a-f-]+-0*2(?:\D|$)", s):
        return "A"
    if re.search(r"partuuid=[0-9a-f-]+-0*3(?:\D|$)", s) or re.search(r"/by-partuuid/[0-9a-f-]+-0*3(?:\D|$)", s):
        return "B"

    return None


def _infer_current_slot() -> tuple[Optional[str], str]:
    # 1) RAUC runtime hint files
    for path in ("/run/rauc/slot", "/run/rauc/booted"):
        try:
            with open(path, "r", encoding="utf-8") as f:
                slot = _slot_from_hint(f.read())
                if slot:
                    return slot, f"hint:{path}"
        except Exception:
            pass

    # 2) Kernel cmdline hints
    try:
        with open("/proc/cmdline", "r", encoding="utf-8") as f:
            cmdline = f.read().strip()

        m_slot = re.search(r"(?:^|\s)rauc\.slot=([^\s]+)", cmdline)
        if m_slot:
            slot = _slot_from_hint(m_slot.group(1))
            if slot:
                return slot, "cmdline:rauc.slot"

        m_root = re.search(r"(?:^|\s)root=([^\s]+)", cmdline)
        if m_root:
            slot = _slot_from_hint(m_root.group(1))
            if slot:
                return slot, "cmdline:root"
    except Exception:
        pass

    # 3) Mounted root source
    try:
        with open("/proc/mounts", "r", encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and parts[1] == "/":
                    slot = _slot_from_hint(parts[0])
                    if slot:
                        return slot, "proc-mounts:/"
                    break
    except Exception:
        pass

    return None, "none"


def _is_unknown_version(raw: Any) -> bool:
    s = str(raw or "").strip().lower()
    return s in ("", "-", "unknown", "none", "n/a")


def _infer_current_version_from_ota_logs(log_root: str) -> Optional[str]:
    root = str(log_root or "").strip() or "/data/log/ota"
    try:
        pattern = os.path.join(root, "*", "events.jsonl")
        files = [p for p in glob.glob(pattern) if os.path.isfile(p)]
        files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    except Exception:
        return None

    # Inspect recent OTA event files first.
    for path in files[:20]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
        except Exception:
            continue

        for line in reversed(lines):
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except Exception:
                continue

            ota = evt.get("ota", {}) if isinstance(evt, dict) else {}
            event = str(ota.get("event") or "").strip().upper()
            phase = str(ota.get("phase") or "").strip().upper()
            target = str(ota.get("target_version") or "").strip()
            current = str(ota.get("current_version") or "").strip()

            # Prefer successful apply/commit records.
            if event == "OK" and phase in ("REBOOT", "COMMIT", "APPLY"):
                if not _is_unknown_version(target):
                    return target
                if not _is_unknown_version(current):
                    return current

            # Conservative fallback: keep current_version inference stable.
            # Do not use target_version from non-final events (e.g. DOWNLOAD START),
            # otherwise UI may show current==target before apply/reboot is complete.
            if not _is_unknown_version(current):
                return current

    return None

ACTIVE_PHASES = (PHASE_DOWNLOAD, PHASE_APPLY, PHASE_REBOOT, PHASE_COMMIT)
_ota_start_lock = threading.Lock()


def _cfg_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _cfg_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _normalize_mqtt_transport(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"ws", "wss", "websocket", "websockets"}:
        return "websockets"
    return "tcp"


def _format_mqtt_topic(template: str, device_id: str) -> str:
    topic = str(template or "").strip()
    if not topic:
        return ""
    try:
        return topic.format(vehicle_id=device_id, device_id=device_id)
    except Exception:
        return topic.replace("{vehicle_id}", device_id).replace("{device_id}", device_id)


def _safe_cmd_output(cmd: list[str], timeout_sec: float = 1.0) -> str:
    try:
        return subprocess.check_output(
            cmd,
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=timeout_sec,
        )
    except Exception:
        return ""


def _default_gateway_and_iface() -> tuple[str, str]:
    out = _safe_cmd_output(["ip", "route", "show", "default"])
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        gw = ""
        iface = ""
        m = re.search(r"\bvia\s+(\d{1,3}(?:\.\d{1,3}){3})\b", line)
        if m:
            gw = m.group(1)
        parts = line.split()
        if "dev" in parts:
            idx = parts.index("dev")
            if idx + 1 < len(parts):
                iface = parts[idx + 1]
        return gw, iface
    return "", ""


def _measure_rssi_dbm(iface: str) -> Optional[int]:
    iface = str(iface or "").strip()
    if not iface:
        return None

    out = _safe_cmd_output(["iw", "dev", iface, "link"], timeout_sec=0.8)
    m = re.search(r"signal:\s*(-?\d+)\s*dBm", out)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass

    out = _safe_cmd_output(["iwconfig", iface], timeout_sec=0.8)
    m = re.search(r"Signal level[=:\s]*(-?\d+)\s*dBm", out)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass

    return None


def _measure_latency_ms(target_ip: str) -> Optional[int]:
    target = str(target_ip or "").strip()
    if not target:
        return None
    out = _safe_cmd_output(["ping", "-c", "1", "-W", "1", target], timeout_sec=1.8)
    m = re.search(r"time[=<]\s*([0-9.]+)\s*ms", out)
    if not m:
        return None
    try:
        return int(round(float(m.group(1))))
    except Exception:
        return None


def _mqtt_network_snapshot() -> Dict[str, Any]:
    ip, ip_source = _get_ip_and_source()
    gw_ip, gw_iface = _default_gateway_and_iface()
    iface = gw_iface or _default_iface() or "wlan0"
    rssi_dbm = _measure_rssi_dbm(iface)
    latency_ms = _measure_latency_ms(gw_ip)
    return {
        "iface": iface or "wlan0",
        "ip": "" if ip == "-" else ip,
        "ip_source": ip_source,
        "rssi_dbm": int(rssi_dbm) if rssi_dbm is not None else 0,
        "latency_ms": int(latency_ms) if latency_ms is not None else 0,
        "gateway_reachable": latency_ms is not None,
    }


def _host_from_url(raw_url: Any) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
        return str(parsed.hostname or "").strip()
    except Exception:
        return ""


def _create_mqtt_client(client_id: str, transport: str = "tcp"):
    """
    Create a paho client compatible with both v1.x and v2.x.
    v2.x may require callback_api_version and defaults to v2 callbacks.
    """
    kwargs = {
        "client_id": client_id,
        "protocol": mqtt.MQTTv311,
        "clean_session": True,
        "transport": _normalize_mqtt_transport(transport),
    }
    callback_api = getattr(mqtt, "CallbackAPIVersion", None)
    if callback_api is not None:
        try:
            return mqtt.Client(callback_api_version=callback_api.VERSION1, **kwargs)
        except Exception:
            pass
    return mqtt.Client(**kwargs)


class MQTTCommandBridge:
    def __init__(
        self,
        cfg: Dict[str, Any],
        on_update_request: Callable[[str, str, str, str], Tuple[bool, str]],
    ):
        self.cfg = cfg
        self.on_update_request = on_update_request
        self.enabled = _cfg_bool(cfg.get("mqtt_enabled"), default=False)
        raw_broker_host = str(cfg.get("mqtt_broker_host", "localhost")).strip()
        collector_host = _host_from_url(cfg.get("collector_url"))
        if raw_broker_host.lower() in ("", "localhost", "127.0.0.1", "::1") and collector_host:
            self.broker_host = collector_host
        else:
            self.broker_host = raw_broker_host
        self.broker_port = _cfg_int(cfg.get("mqtt_broker_port", 1883), 1883)
        self.keepalive = _cfg_int(cfg.get("mqtt_keepalive_sec", 60), 60)
        self.qos = _cfg_int(cfg.get("mqtt_qos", 1), 1)
        self.username = str(cfg.get("mqtt_username", "")).strip()
        self.password = str(cfg.get("mqtt_password", "")).strip()
        self.transport = _normalize_mqtt_transport(cfg.get("mqtt_transport", "tcp"))
        ws_path = str(cfg.get("mqtt_ws_path", "/mqtt")).strip() or "/mqtt"
        self.ws_path = ws_path if ws_path.startswith("/") else f"/{ws_path}"
        auto_tls = self.transport == "websockets" and self.broker_port == 443
        self.tls_enabled = _cfg_bool(cfg.get("mqtt_tls"), default=auto_tls)
        self.tls_insecure = _cfg_bool(cfg.get("mqtt_tls_insecure"), default=False)
        self.tls_ca_certs = str(cfg.get("mqtt_tls_ca_certs", "")).strip()
        self.tls_certfile = str(cfg.get("mqtt_tls_certfile", "")).strip()
        self.tls_keyfile = str(cfg.get("mqtt_tls_keyfile", "")).strip()
        self.device_id = str(cfg.get("device_id", "")).strip()
        default_client_id = f"ota-backend-{self.device_id or 'device'}"
        self.client_id = str(cfg.get("mqtt_client_id", default_client_id)).strip() or default_client_id
        self.topic_cmd_template = str(cfg.get("mqtt_topic_cmd", "ota/{vehicle_id}/cmd"))
        self.topic_status_template = str(cfg.get("mqtt_topic_status", "ota/{vehicle_id}/status"))
        self.topic_progress_template = str(cfg.get("mqtt_topic_progress", "ota/{vehicle_id}/progress"))
        self.client = None
        self.connected = False
        self._lock = threading.Lock()

    def _topic_cmd(self) -> str:
        return _format_mqtt_topic(self.topic_cmd_template, self.device_id)

    def _topic_status(self) -> str:
        return _format_mqtt_topic(self.topic_status_template, self.device_id)

    def _topic_progress(self) -> str:
        return _format_mqtt_topic(self.topic_progress_template, self.device_id)

    def is_connected(self) -> bool:
        return bool(self.connected)

    def start(self) -> None:
        if not self.enabled:
            _append_runtime_log("mqtt disabled")
            return
        if mqtt is None:
            logger.warning("MQTT is enabled but paho-mqtt is not installed")
            _append_runtime_log("mqtt unavailable: paho-mqtt missing")
            return
        if not self.device_id:
            logger.warning("MQTT is enabled but device_id is empty; command subscription skipped")
            _append_runtime_log("mqtt disabled: empty device_id")
            return

        try:
            client = _create_mqtt_client(self.client_id, self.transport)
            client.on_connect = self._on_connect
            client.on_disconnect = self._on_disconnect
            client.on_message = self._on_message

            if self.username:
                client.username_pw_set(self.username, self.password)

            if self.transport == "websockets":
                try:
                    client.ws_set_options(path=self.ws_path)
                except Exception as ex:
                    logger.warning("MQTT websocket setup failed: %s", ex)
                    _append_runtime_log(f"mqtt ws setup failed: {ex.__class__.__name__}")
                    return

            if self.tls_enabled:
                try:
                    tls_kwargs: Dict[str, Any] = {}
                    if self.tls_ca_certs:
                        tls_kwargs["ca_certs"] = self.tls_ca_certs
                    if self.tls_certfile:
                        tls_kwargs["certfile"] = self.tls_certfile
                    if self.tls_keyfile:
                        tls_kwargs["keyfile"] = self.tls_keyfile
                    if tls_kwargs:
                        client.tls_set(**tls_kwargs)
                    else:
                        client.tls_set()
                    if hasattr(client, "tls_insecure_set"):
                        client.tls_insecure_set(bool(self.tls_insecure))
                except Exception as ex:
                    logger.warning("MQTT TLS setup failed: %s", ex)
                    _append_runtime_log(f"mqtt tls setup failed: {ex.__class__.__name__}")
                    return

            # Never block backend startup on broker reachability.
            # connect_async() schedules connection in the network loop thread.
            if hasattr(client, "reconnect_delay_set"):
                try:
                    client.reconnect_delay_set(min_delay=1, max_delay=10)
                except Exception:
                    pass

            if hasattr(client, "connect_async"):
                client.connect_async(self.broker_host, self.broker_port, keepalive=self.keepalive)
            else:
                client.connect(self.broker_host, self.broker_port, keepalive=self.keepalive)
            client.loop_start()
            self.client = client
            _append_runtime_log(
                f"mqtt connecting host={self.broker_host}:{self.broker_port} "
                f"transport={self.transport} "
                f"ws_path={self.ws_path if self.transport == 'websockets' else '-'} "
                f"tls={'on' if self.tls_enabled else 'off'} "
                f"topic={self._topic_cmd()} qos={self.qos}"
            )
        except Exception as ex:
            logger.warning("MQTT start failed: %s", ex)
            _append_runtime_log(f"mqtt start failed: {ex.__class__.__name__}")

    def stop(self) -> None:
        if not self.client:
            return
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass
        self.connected = False

    def _on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            self.connected = False
            _append_runtime_log(f"mqtt connect failed rc={rc}")
            return

        topic = self._topic_cmd()
        if not topic:
            self.connected = False
            _append_runtime_log("mqtt connect failed: empty cmd topic")
            return

        client.subscribe(topic, qos=self.qos)
        self.connected = True
        _append_runtime_log(f"mqtt connected sub={topic}")

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        _append_runtime_log(f"mqtt disconnected rc={rc}")

    def _on_message(self, client, userdata, msg):
        try:
            payload_text = msg.payload.decode("utf-8", errors="replace")
            data = json.loads(payload_text)
        except Exception as ex:
            _append_runtime_log(f"mqtt invalid payload topic={msg.topic} err={ex.__class__.__name__}")
            return

        req = parse_mqtt_update_command(
            data,
            default_ota_id_prefix=f"{self.device_id or 'device'}-mqtt",
        )
        if not req:
            _append_runtime_log(f"mqtt ignored payload topic={msg.topic}")
            return

        ok, reason = self.on_update_request(
            req["ota_id"],
            req["url"],
            req["target_version"],
            "mqtt",
        )
        if ok:
            _append_runtime_log(
                f"mqtt accepted ota_id={req['ota_id']} target={req['target_version']}"
            )
        else:
            _append_runtime_log(
                f"mqtt rejected ota_id={req['ota_id']} reason={reason}"
            )
            self.publish_status("failed", req["target_version"], f"Command rejected: {reason}")

    def _publish(self, topic: str, payload: Dict[str, Any]) -> None:
        if not topic:
            return
        with self._lock:
            if not self.client or not self.connected:
                return
            try:
                result = self.client.publish(topic, json.dumps(payload), qos=self.qos)
                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    logger.debug("MQTT publish failed topic=%s rc=%s", topic, result.rc)
            except Exception:
                return

    def publish_status(self, status: str, target_version: str, message: str = "") -> None:
        network = _mqtt_network_snapshot()
        payload = {
            "vehicle_id": self.device_id,
            "status": status,
            "target_version": target_version,
            "message": message,
            "timestamp": datetime.utcnow().isoformat(),
            "ota": {
                "current_version": str(state.current_version or ""),
                "target_version": str(target_version or ""),
                "phase": str(state.phase or ""),
                "event": str(state.event or ""),
            },
            "context": {
                "network": network,
            },
        }
        self._publish(self._topic_status(), payload)

    def publish_progress(self, target_version: str, progress: int, message: str = "") -> None:
        payload = {
            "vehicle_id": self.device_id,
            "target_version": target_version,
            "progress": max(0, min(100, int(progress))),
            "message": message or f"Progress: {progress}%",
            "timestamp": datetime.utcnow().isoformat(),
        }
        self._publish(self._topic_progress(), payload)


CFG_PATH = os.environ.get("OTA_BACKEND_CONFIG", "/etc/ota-backend/config.json")
CFG: Dict[str, Any] = load_config(CFG_PATH)
MQTT_BRIDGE: Optional[MQTTCommandBridge] = None

_stop_event = threading.Event()
start_queue_flusher(CFG, _stop_event)
_mqtt_heartbeat_started = False

@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "mqtt_enabled": bool(MQTT_BRIDGE and MQTT_BRIDGE.enabled),
        "mqtt_connected": bool(MQTT_BRIDGE and MQTT_BRIDGE.is_connected()),
        "mqtt_transport": str(getattr(MQTT_BRIDGE, "transport", "tcp") or "tcp"),
        "mqtt_tls": bool(getattr(MQTT_BRIDGE, "tls_enabled", False)),
    })

@app.get("/ota/status")
def ota_status():
    try:
        status = parse_rauc_status(rauc_status_json())
    except Exception:
        status = {"compatible": None, "current_slot": None, "slots": []}

    try:
        ip_address, ip_source = _get_ip_and_source()
    except Exception:
        ip_address, ip_source = "-", "exception"

    current_slot = status.get("current_slot")
    slot_source = "rauc"
    if not current_slot:
        inferred_slot, inferred_from = _infer_current_slot()
        if inferred_slot:
            current_slot = inferred_slot
            slot_source = inferred_from
        else:
            slot_source = "none"

    slots = status.get("slots", [])
    if not isinstance(slots, list):
        slots = []
    if not slots and current_slot in ("A", "B"):
        # UI fallback: show A/B even when RAUC JSON is unavailable.
        slots = [
            {
                "name": "rootfs.0",
                "state": "booted" if current_slot == "A" else "inactive",
                "bootname": "A",
                "device": "/dev/mmcblk0p2",
            },
            {
                "name": "rootfs.1",
                "state": "booted" if current_slot == "B" else "inactive",
                "bootname": "B",
                "device": "/dev/mmcblk0p3",
            },
        ]

    current_version = state.current_version
    if _is_unknown_version(current_version):
        inferred_version = _infer_current_version_from_ota_logs(CFG.get("ota_log_dir", "/data/log/ota"))
        if inferred_version and not _is_unknown_version(inferred_version):
            current_version = inferred_version
            state.current_version = inferred_version

    _append_runtime_log(f"status ip={ip_address} src={ip_source} slot={current_slot or '-'} slot_src={slot_source}")
    return jsonify({
        "ts": datetime.now().astimezone().isoformat(timespec="seconds"),
        "compatible": status.get("compatible"),
        "current_slot": current_slot,
        "slots": slots,
        "ota_id": state.active_ota_id,
        "ota_log": state.ota_log,
        "current_version": current_version,
        "target_version": state.target_version,
        "phase": state.phase,
        "event": state.event,
        "last_error": state.last_error,
        "ip_address": ip_address,
        "ip": ip_address,
        "ip_source": ip_source,
        "slot_source": slot_source,
        "device_id": CFG.get("device_id"),
    })

@app.post("/ota/start")
def ota_start():
    req = request.get_json(silent=True) or {}
    ota_id = str(req.get("ota_id", "")).strip()
    url = str(req.get("url", "")).strip()
    target_version = str(req.get("target_version", "")).strip()

    if not ota_id or not url or not target_version:
        return jsonify({"detail": "ota_id, url, target_version are required"}), 400

    ok, reason = _start_ota_job(ota_id, url, target_version, "api")
    if not ok:
        return jsonify({"detail": reason}), 409
    return jsonify({"ok": True})


def _publish_mqtt_status(status: str, target_version: str, message: str = "") -> None:
    if MQTT_BRIDGE:
        MQTT_BRIDGE.publish_status(status, target_version, message)


def _publish_mqtt_progress(target_version: str, progress: int, message: str = "") -> None:
    if MQTT_BRIDGE:
        MQTT_BRIDGE.publish_progress(target_version, progress, message)


def _start_ota_job(ota_id: str, url: str, target_version: str, trigger_source: str) -> Tuple[bool, str]:
    ota_id = str(ota_id or "").strip()
    url = str(url or "").strip()
    target_version = str(target_version or "").strip()
    trigger_source = str(trigger_source or "api").strip() or "api"

    if not ota_id or not url or not target_version:
        return False, "ota_id, url, target_version are required"

    with _ota_start_lock:
        if state.phase in ACTIVE_PHASES:
            return False, "OTA already running"
        state.active_ota_id = ota_id
        state.target_version = target_version
        state.last_error = None
        state.phase = PHASE_DOWNLOAD
        state.event = EVENT_START
        state.ota_log = []

    def _run():
        ota_log = []

        def _log(msg: str):
            ota_log.append(msg)
            state.ota_log = ota_log[:]

        try:
            state.active_ota_id = ota_id
            state.target_version = target_version
            state.last_error = None

            state.phase = PHASE_DOWNLOAD
            state.event = EVENT_START
            _log("DOWNLOAD START")
            _log(f"SOURCE {trigger_source}")
            _publish_mqtt_status("downloading", target_version, f"DOWNLOAD START source={trigger_source}")
            _publish_mqtt_progress(target_version, 5, "Downloading bundle")
            event = build_event(CFG, ota_id, state.current_version, target_version,
                                PHASE_DOWNLOAD, EVENT_START, {}, ota_log)
            _write_event(CFG, ota_id, event)
            _post_event(CFG, event)

            bundle_dir = CFG.get("bundle_dir", "/data/ota")
            bundle_path = os.path.join(bundle_dir, f"{ota_id}.raucb")

            removed = cleanup_old_bundles(
                bundle_dir=bundle_dir,
                keep=int(CFG.get("bundle_keep", 0)),
                preserve=[bundle_path],
            )
            if removed:
                _log(f"CLEANUP OLD_BUNDLES removed={removed}")

            err_code, last_status = download_with_retries(
                url,
                bundle_path,
                int(CFG.get("download_retries", 3)),
                int(CFG.get("download_timeout_sec", 30)),
                _log,
            )
            if err_code == "NO_SPACE":
                # Retry once after aggressive cleanup (keeps only current target path).
                removed_extra = cleanup_old_bundles(
                    bundle_dir=bundle_dir,
                    keep=0,
                    preserve=[bundle_path],
                )
                if removed_extra:
                    _log(f"CLEANUP RETRY removed={removed_extra}")
                _log("DOWNLOAD RETRY after NO_SPACE")
                err_code, last_status = download_with_retries(
                    url,
                    bundle_path,
                    int(CFG.get("download_retries", 1)),
                    int(CFG.get("download_timeout_sec", 30)),
                    _log,
                )
            if err_code:
                state.event = EVENT_FAIL
                state.last_error = err_code
                if err_code == "HTTP_5XX":
                    msg = f"Server error: {last_status} Service Unavailable" if last_status else "Server error: 5xx"
                elif err_code == "NO_SPACE":
                    msg = "No space left on bundle storage"
                elif err_code == "IO_ERROR":
                    msg = "Storage I/O error during download"
                else:
                    msg = "Download error"
                error = {"code": err_code, "message": msg, "retryable": err_code == "HTTP_5XX"}
                event = build_event(CFG, ota_id, state.current_version, target_version,
                                    PHASE_DOWNLOAD, EVENT_FAIL, error, ota_log)
                _write_event(CFG, ota_id, event)
                _post_event(CFG, event)
                _publish_mqtt_status("failed", target_version, msg)
                state.phase = None
                return

            _publish_mqtt_progress(target_version, 40, "Download complete")
            state.phase = PHASE_APPLY
            state.event = EVENT_START
            _log("APPLY START")
            _publish_mqtt_status("installing", target_version, "APPLY START")
            _publish_mqtt_progress(target_version, 70, "Applying bundle")
            event = build_event(CFG, ota_id, state.current_version, target_version,
                                PHASE_APPLY, EVENT_START, {}, ota_log)
            _write_event(CFG, ota_id, event)
            _post_event(CFG, event)

            rc = rauc_install(bundle_path)
            if rc != 0:
                state.event = EVENT_FAIL
                state.last_error = "RAUC_INSTALL"
                error = {"code": "RAUC_INSTALL", "message": "RAUC install failed", "retryable": False}
                event = build_event(CFG, ota_id, state.current_version, target_version,
                                    PHASE_APPLY, EVENT_FAIL, error, ota_log)
                _write_event(CFG, ota_id, event)
                _post_event(CFG, event)
                _publish_mqtt_status("failed", target_version, "RAUC install failed")
                state.phase = None
                return

            _log("APPLY OK - RAUC install succeeded")
            _publish_mqtt_progress(target_version, 75, "RAUC install done, LLM verification...")

            # ── LLM 2차 검증 ──────────────────────────
            _log("LLM_VERIFY START")
            _publish_mqtt_status("verifying", target_version, "LLM 2차 검증 진행 중")
            try:
                log_collector = OTALogCollector()
                log_collector.record_rauc_result(rc, "RAUC install succeeded", "")
                log_collector.record_signature_verification(True)
                log_collector.record_firmware_file_info(bundle_path, 0)
                log_collector.record_server_info(url)
                for mqtt_cmd in ota_log:
                    if mqtt_cmd.startswith("SOURCE"):
                        log_collector.record_mqtt_command("ota/update/trigger", mqtt_cmd)

                verification_log = log_collector.build_verification_log(
                    current_version=state.current_version,
                    new_version=target_version,
                    vehicle_id=CFG.get("device_id", "unknown"),
                )

                server_url = CFG.get("collector_url", "").rsplit("/", 1)[0]  # strip /ingest
                if not server_url:
                    server_url = f"http://{CFG.get('mqtt_broker_host', 'localhost')}:8080"

                llm_result = request_llm_verification(
                    server_url=server_url,
                    ota_log=verification_log,
                    timeout=30,
                )

                _log(f"LLM_VERIFY RESULT: {llm_result['decision']}")
                _log(f"LLM_VERIFY REASON: {llm_result['reason']}")

                if llm_result["decision"] == "REJECT":
                    state.event = EVENT_FAIL
                    state.last_error = "LLM_REJECT"
                    error = {
                        "code": "LLM_REJECT",
                        "message": f"LLM rejected: {llm_result['reason']}",
                        "retryable": False,
                    }
                    event = build_event(CFG, ota_id, state.current_version, target_version,
                                        PHASE_APPLY, EVENT_FAIL, error, ota_log)
                    _write_event(CFG, ota_id, event)
                    _post_event(CFG, event)
                    _publish_mqtt_status("failed", target_version,
                                         f"LLM REJECT: {llm_result['reason'][:100]}")
                    state.phase = None
                    return

                _log("LLM_VERIFY APPROVED")
            except Exception as llm_ex:
                _log(f"LLM_VERIFY ERROR: {llm_ex}, Fail-safe REJECT")
                state.event = EVENT_FAIL
                state.last_error = "LLM_ERROR"
                error = {
                    "code": "LLM_ERROR",
                    "message": f"LLM verification failed: {llm_ex}",
                    "retryable": False,
                }
                event = build_event(CFG, ota_id, state.current_version, target_version,
                                    PHASE_APPLY, EVENT_FAIL, error, ota_log)
                _write_event(CFG, ota_id, event)
                _post_event(CFG, event)
                _publish_mqtt_status("failed", target_version, "LLM verification error, Fail-safe REJECT")
                state.phase = None
                return
            # ── LLM 2차 검증 끝 ──────────────────────

            state.phase = PHASE_REBOOT
            state.event = EVENT_OK
            state.current_version = target_version
            _publish_mqtt_progress(target_version, 90, "Apply complete")
            event = build_event(CFG, ota_id, state.current_version, target_version,
                                PHASE_REBOOT, EVENT_OK, {}, ota_log)
            _write_event(CFG, ota_id, event)
            _post_event(CFG, event)

            if bool(CFG.get("reboot_after_apply", False)):
                _publish_mqtt_status("completed", target_version, "APPLY OK, rebooting")
                _publish_mqtt_progress(target_version, 100, "Completed")
                os.system("systemctl reboot")
                return

            state.phase = PHASE_COMMIT
            state.event = EVENT_START
            _log("COMMIT START")
            _publish_mqtt_status("installing", target_version, "COMMIT START")
            event = build_event(CFG, ota_id, state.current_version, target_version,
                                PHASE_COMMIT, EVENT_START, {}, ota_log)
            _write_event(CFG, ota_id, event)
            _post_event(CFG, event)

            if bool(CFG.get("mark_good_on_commit", True)):
                rauc_mark_good()

            state.event = EVENT_OK
            _log("COMMIT OK")
            _publish_mqtt_status("completed", target_version, "COMMIT OK")
            _publish_mqtt_progress(target_version, 100, "Completed")
            event = build_event(CFG, ota_id, state.current_version, target_version,
                                PHASE_COMMIT, EVENT_OK, {}, ota_log)
            _write_event(CFG, ota_id, event)
            _post_event(CFG, event)
            state.phase = None
        except Exception as ex:
            state.event = EVENT_FAIL
            state.last_error = "INTERNAL"
            _log(f"INTERNAL ERROR {ex.__class__.__name__}")
            error = {"code": "INTERNAL", "message": str(ex), "retryable": False}
            event = build_event(
                CFG,
                ota_id,
                state.current_version,
                target_version,
                state.phase or "UNKNOWN",
                EVENT_FAIL,
                error,
                ota_log,
            )
            _write_event(CFG, ota_id, event)
            _post_event(CFG, event)
            _publish_mqtt_status("failed", target_version, f"INTERNAL: {ex.__class__.__name__}")
            state.phase = None

    try:
        t = threading.Thread(target=_run, daemon=True)
        t.start()
        _append_runtime_log(
            f"ota start source={trigger_source} ota_id={ota_id} target={target_version} url={url}"
        )
        return True, "started"
    except Exception as ex:
        with _ota_start_lock:
            state.phase = None
            state.event = EVENT_FAIL
            state.last_error = "THREAD_START"
        _append_runtime_log(f"ota start failed source={trigger_source} err={ex.__class__.__name__}")
        return False, "failed to start ota worker"

@app.post("/ota/reboot")
def ota_reboot():
    os.system("systemctl reboot")
    return jsonify({"ok": True})


def _init_mqtt_bridge() -> None:
    global MQTT_BRIDGE
    if MQTT_BRIDGE is None:
        MQTT_BRIDGE = MQTTCommandBridge(CFG, _start_ota_job)
    MQTT_BRIDGE.start()


def _start_mqtt_heartbeat() -> None:
    global _mqtt_heartbeat_started
    if _mqtt_heartbeat_started:
        return

    interval_sec = _cfg_int(CFG.get("mqtt_heartbeat_sec", 10), 10)
    if interval_sec <= 0:
        _append_runtime_log("mqtt heartbeat disabled")
        return

    def _run():
        # Publish idle heartbeat so OTA server can keep device last_seen fresh.
        while not _stop_event.is_set():
            _stop_event.wait(interval_sec)
            if _stop_event.is_set():
                return

            bridge = MQTT_BRIDGE
            if not bridge or not bridge.enabled or not bridge.is_connected():
                continue
            if state.phase in ACTIVE_PHASES:
                continue

            target_version = str(state.current_version or "unknown")
            bridge.publish_status("idle", target_version, "HEARTBEAT")

    threading.Thread(target=_run, daemon=True).start()
    _mqtt_heartbeat_started = True
    _append_runtime_log(f"mqtt heartbeat started interval={interval_sec}s")


def main():
    _init_mqtt_bridge()
    _start_mqtt_heartbeat()
    app.run(host="0.0.0.0", port=8080)
