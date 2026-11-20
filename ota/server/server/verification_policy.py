"""Persistent demo verification mode, independent of legacy LLM_VERIFY settings."""
import json
import os
from pathlib import Path
import tempfile
import threading

MODES = {"ON_DEVICE", "CLAUDE", "RULES_ONLY"}
POLICY_PATH = Path(os.getenv("VERIFICATION_POLICY_PATH", "/app/logs/verification-policy.json"))
_lock = threading.RLock()


def snapshot():
    with _lock:
        # New deployments always start with the existing on-device gate enabled.
        value = json.loads(POLICY_PATH.read_text()) if POLICY_PATH.exists() else {"mode": "ON_DEVICE", "revision": 0}
        if value.get("mode") not in MODES or type(value.get("revision")) is not int:
            raise ValueError("Invalid persisted verification policy")
        import claude_verification
        mode = value["mode"]
        return {"schema_version": "ota-verification-policy-v1", "mode": mode,
                "revision": value["revision"], "enabled": mode == "CLAUDE",
                "ondevice_enabled": mode == "ON_DEVICE",
                "claude_available": claude_verification.ENABLED}


def set_mode(mode):
    if mode not in MODES:
        raise ValueError("mode must be ON_DEVICE, CLAUDE or RULES_ONLY")
    import claude_verification
    if mode == "CLAUDE" and not claude_verification.ENABLED:
        raise ValueError("Claude feature is disabled in code")
    with _lock:
        previous = snapshot()
        value = {"mode": mode, "revision": previous["revision"] + 1}
        POLICY_PATH.parent.mkdir(parents=True, exist_ok=True)
        name = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=POLICY_PATH.parent, delete=False) as f:
                name = f.name
                json.dump(value, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(name, POLICY_PATH)
        finally:
            if name and os.path.exists(name):
                os.unlink(name)
        return snapshot()
