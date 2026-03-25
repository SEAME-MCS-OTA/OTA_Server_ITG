"""Simple web dashboard for visualizing LLM decision results."""

from __future__ import annotations

import json
from pathlib import Path

from flask import Blueprint, current_app, jsonify, render_template

from config import Config


llm_dashboard_bp = Blueprint("llm_dashboard", __name__, template_folder="templates")

_EXAMPLE_REQUEST_PATH = (
    Path(__file__).resolve().parent / "examples" / "llm" / "http_analyze_request_example.json"
)


@llm_dashboard_bp.get("/llm")
def root_dashboard():
    return render_template(
        "llm/dashboard.html",
        llm_verify=bool(
            current_app.config.get("LLM_RUNTIME_ENABLED", Config.LLM_VERIFICATION_ENABLED)
        ),
        mqtt_enabled=bool(Config.LLM_MQTT_BRIDGE_ENABLED),
    )


@llm_dashboard_bp.get("/llm/dashboard")
@llm_dashboard_bp.get("/dashboard")
def dashboard_page():
    return render_template(
        "llm/dashboard.html",
        llm_verify=bool(
            current_app.config.get("LLM_RUNTIME_ENABLED", Config.LLM_VERIFICATION_ENABLED)
        ),
        mqtt_enabled=bool(Config.LLM_MQTT_BRIDGE_ENABLED),
    )


@llm_dashboard_bp.get("/api/v1/llm/example-request")
def get_example_request():
    try:
        payload = json.loads(_EXAMPLE_REQUEST_PATH.read_text(encoding="utf-8"))
        return jsonify(payload), 200
    except FileNotFoundError:
        return jsonify({"error": "example file not found"}), 404
    except json.JSONDecodeError as exc:
        return jsonify({"error": f"invalid example json: {exc}"}), 500
