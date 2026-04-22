import os
import sys
import tempfile
import unittest


HERE = os.path.dirname(__file__)
SERVER_DIR = os.path.abspath(os.path.join(HERE, ".."))
if SERVER_DIR not in sys.path:
    sys.path.insert(0, SERVER_DIR)

from llm_verifier import (  # noqa: E402
    _extract_json_object,
    _normalize_llm_json_result,
    preprocess_log,
    save_verification_result,
)


class LLMVerifierPreprocessTests(unittest.TestCase):
    def test_record_only_result_keeps_gate_log_visible(self):
        import llm_verifier  # noqa: E402
        import app as app_module  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            original_db_path = llm_verifier._VERIFICATION_DB_PATH
            llm_verifier._VERIFICATION_DB_PATH = os.path.join(tmpdir, "llm_verification.db")
            try:
                gate_log = {
                    "schema_version": "ota-verify-v2",
                    "environment": "lab",
                    "rule_check_results": {},
                    "context_data": {
                        "firmware_metadata": {
                            "current_active_version": "3.3.7",
                            "new_installed_version": "3.3.8",
                        },
                        "slot_analysis": {
                            "vehicle_id": "vw-ivi-0026",
                        },
                        "mqtt_analysis": {
                            "commands": [
                                {
                                    "parsed_command": {
                                        "ota_id": "ota-123",
                                    }
                                }
                            ]
                        },
                        "logs": {
                            "system_log_excerpt": "gate-log",
                        },
                    },
                }
                record_only_log = {
                    "schema_version": "ota-verify-v2",
                    "environment": "lab",
                    "rule_check_results": {},
                    "context_data": {
                        "firmware_metadata": {
                            "current_active_version": "3.3.7",
                            "new_installed_version": "3.3.8",
                        },
                        "slot_analysis": {
                            "vehicle_id": "vw-ivi-0026",
                        },
                        "mqtt_analysis": {
                            "commands": [
                                {
                                    "parsed_command": {
                                        "ota_id": "ota-123",
                                    }
                                }
                            ]
                        },
                        "logs": {
                            "system_log_excerpt": "record-only-log",
                        },
                    },
                }

                save_verification_result(
                    gate_log,
                    {"decision": "APPROVE", "reason": "gate-pass", "raw_response": "{}"},
                    verify_mode="gate",
                )
                save_verification_result(
                    record_only_log,
                    {"decision": "APPROVE", "reason": "record-pass", "raw_response": "{}"},
                    verify_mode="record_only",
                )

                conn = llm_verifier._get_verification_db()
                rows = conn.execute(
                    "SELECT verify_mode, ota_log_json FROM verification_results ORDER BY id ASC"
                ).fetchall()
                conn.close()

                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0][0], "gate")
                self.assertEqual(rows[1][0], "record_only")
                self.assertIn("gate-log", rows[0][1])
                self.assertIn("record-only-log", rows[1][1])

                with app_module.app.test_request_context("/api/v1/llm/results?limit=10"):
                    response = app_module.list_llm_results()
                payload = response.get_json()
                self.assertEqual(payload["total"], 1)
                result = payload["results"][0]
                self.assertEqual(result["verify_mode"], "gate")
                self.assertEqual(
                    result["ota_log"]["context_data"]["logs"]["system_log_excerpt"],
                    "gate-log",
                )
            finally:
                llm_verifier._VERIFICATION_DB_PATH = original_db_path

    def test_preprocess_returns_clean_v2_payload(self):
        raw = {
            "schema_version": "ota-verify-v2",
            "environment": "lab",
            "rule_check_results": {
                "sha256_hash_match": {
                    "status": "PASSED",
                    "reason": "hash_match",
                    "fail_action": "NONE",
                    "severity": "PASS",
                },
                "booted_slot_matches_reported": {
                    "status": "FLAG",
                    "reason": "booted_slot_alignment_unknown",
                    "fail_action": "FLAG_TO_LLM",
                    "severity": "FLAG",
                },
            },
            "timestamp": "2026-04-07T12:00:00Z",
            "context_data": {
                "firmware_metadata": {
                    "current_active_version": "unknown",
                    "requested_target_version": "3.1.0",
                    "new_installed_version": "3.1.0",
                    "bundle_compatible": "vw-ivi-telechips",
                    "bundle_format": "verity",
                    "firmware_file_size_bytes": 188463437,
                    "expected_file_size_bytes": 188463437,
                    "update_trigger_server": "http://192.168.86.40:8080/firmware/app_3.1.0.raucb",
                    "build_server_info": "",
                    "version_alignment": {
                        "boot_primary_bundle_version": "3.1.0",
                        "booted_slot_bundle_version": "",
                        "booted_slot_matches_reported": False,
                        "new_installed_version": "3.1.0",
                        "reported_current_active_version": "unknown",
                    },
                },
                "slot_analysis": {
                    "vehicle_model": "ivi-telechips",
                    "vehicle_id": "vw-ivi-0026",
                    "current_slot_status": {
                        "slots": [
                            {
                                "rootfs.0": {
                                    "bootname": "A",
                                    "boot_status": "good",
                                    "state": "booted",
                                    "is_booted": True,
                                    "is_next_boot_primary": False,
                                    "slot_role": "current",
                                    "confirmation_state": "confirmed",
                                    "slot_status": {
                                        "bundle": {
                                            "compatible": None,
                                        }
                                    },
                                }
                            },
                            {
                                "rootfs.1": {
                                    "bootname": "B",
                                    "boot_status": "bad",
                                    "state": "inactive",
                                    "is_booted": False,
                                    "is_next_boot_primary": True,
                                    "slot_role": "next_boot",
                                    "confirmation_state": "pending",
                                    "slot_status": {
                                        "bundle": {
                                            "version": "3.1.0",
                                            "hash": "aa54ff70253caf4b",
                                            "compatible": "vw-ivi-telechips",
                                        }
                                    },
                                }
                            },
                        ]
                    },
                    "booted_slot_matches_reported": False,
                },
                "transfer_metrics": {
                    "download_duration_seconds": 27.443,
                    "download_size_bytes": 188463437,
                    "download_rate_mbps": 54.93,
                    "current_verify_roundtrip_s": 2.91,
                    "total_chunks": 180,
                    "retried_chunks": 0,
                    "retry_ratio_percent": 0,
                },
                "mqtt_analysis": {
                    "conflicting_payloads": False,
                    "release_notes": "fix bugs",
                    "commands": [
                        {
                            "topic": "ota/update/trigger",
                            "payload": {
                                "firmware": {
                                    "sha256": "c3038968332efdfb",
                                }
                            },
                            "parsed_command": {
                                "expected_sha256": "c3038968332efdfb",
                            },
                        }
                    ],
                },
                "system_resources": {
                    "cpu_usage_percent": 11,
                    "memory_usage_percent": 6,
                    "disk_free_mb": 3120,
                },
                "logs": {
                    "rauc_install_log_summary": "Installation completed without errors...",
                    "system_log_excerpt": "RAUC STDOUT: ...",
                },
                "server_allowlist_status": {
                    "in_allowlist": True,
                    "server_url": "http://192.168.86.40:8080/firmware/app_3.1.0.raucb",
                    "flagged_reason": "",
                },
                "recent_update_history": [],
                "recent_update_summary": {
                    "window": "last_10_entries",
                    "attempts": 4,
                    "avg_download_rate_mbps": 61.2,
                    "download_rate_mbps_stddev": 5.4,
                    "updates_in_last_1h": 1,
                    "updates_in_last_24h": 3,
                    "version_skips_in_window": 1,
                },
                "recent_update_signals": {
                    "last_entry": {
                        "to_version": "3.1.0",
                        "verify_roundtrip_s": 3.2,
                    }
                },
                "certificate_chain": {
                    "self_signed": False,
                    "ca_trusted": True,
                    "depth": 2,
                    "days_until_expiry": 20,
                    "expires_within_30_days": True,
                },
            },
        }

        processed = preprocess_log(raw)

        self.assertEqual(set(processed.keys()), {"schema_version", "environment", "rule_check_results", "context_data"})
        self.assertEqual(processed["schema_version"], "ota-verify-v2")
        self.assertNotIn("timestamp", processed)
        self.assertNotIn("normalized_from_schema_version", processed)

        firmware = processed["context_data"]["firmware_metadata"]
        self.assertNotIn("expected_bundle_sha256", firmware)
        self.assertNotIn("bundle_hash", firmware)
        self.assertNotIn("update_trigger_server", firmware)

        commands = processed["context_data"]["mqtt_analysis"]["commands"]
        self.assertEqual(commands[0]["parsed_command"]["expected_sha256"], "c3038968332efdfb")
        self.assertEqual(commands[0]["payload"]["firmware"]["sha256"], "c3038968332efdfb")

        slot_analysis = processed["context_data"]["slot_analysis"]
        self.assertNotIn("current_slot_status", slot_analysis)
        self.assertNotIn("booted_slot_matches_reported", slot_analysis)
        self.assertEqual(slot_analysis["booted_slot"]["name"], "rootfs.0")
        self.assertEqual(slot_analysis["target_slot"]["name"], "rootfs.1")
        self.assertEqual(slot_analysis["next_boot_slot"]["name"], "rootfs.1")
        self.assertEqual(slot_analysis["booted_slot"]["confirmation_state"], "confirmed")
        self.assertEqual(slot_analysis["target_slot"]["confirmation_state"], "pending")
        self.assertEqual(slot_analysis["target_slot"]["slot_role"], "next_boot")
        self.assertFalse(slot_analysis["booted_slot"]["metadata_complete"])
        self.assertTrue(slot_analysis["target_slot"]["metadata_complete"])

        transfer_metrics = processed["context_data"]["transfer_metrics"]
        self.assertNotIn("baseline_expected_seconds", transfer_metrics)
        self.assertEqual(transfer_metrics["download_rate_mbps"], 54.93)
        self.assertEqual(transfer_metrics["download_size_bytes"], 188463437)
        self.assertEqual(transfer_metrics["current_verify_roundtrip_s"], 2.91)

        recent_summary = processed["context_data"]["recent_update_summary"]
        self.assertEqual(recent_summary["attempts"], 4)
        self.assertEqual(recent_summary["download_rate_mbps_stddev"], 5.4)
        self.assertEqual(recent_summary["updates_in_last_1h"], 1)
        self.assertEqual(recent_summary["version_skips_in_window"], 1)
        self.assertEqual(
            processed["context_data"]["recent_update_signals"]["last_entry"]["to_version"],
            "3.1.0",
        )
        certificate_chain = processed["context_data"]["certificate_chain"]
        self.assertTrue(certificate_chain["ca_trusted"])
        self.assertEqual(certificate_chain["days_until_expiry"], 20)
        self.assertTrue(certificate_chain["expires_within_30_days"])

    def test_extract_json_object_handles_markdown_wrapped_json(self):
        payload = _extract_json_object(
            """```json
            {"decision":"APPROVE","summary":"clean","warnings":[],"analysis":{"A_log_interpretation":null,"B_slot_verification":null,"C_transfer_anomaly":null,"D_build_provenance":null,"E_mqtt_integrity":null,"F_system_resources":null,"G_deployment_path":null,"H_compound_pattern":null},"causal_analysis":{"hypothesis":null,"supporting_fields":[],"alternative_hypothesis":null},"recommended_actions":[]}
            ```"""
        )

        self.assertIsInstance(payload, dict)
        self.assertEqual(payload["decision"], "APPROVE")

    def test_normalize_llm_json_result_maps_summary_to_reason_and_recommendations(self):
        normalized = _normalize_llm_json_result(
            {
                "decision": "CONDITIONAL_APPROVE",
                "summary": "Suspicious but inconclusive transfer anomalies detected.",
                "warnings": ["high_retry_ratio"],
                "analysis": {
                    "A_log_interpretation": None,
                    "B_slot_verification": None,
                    "C_transfer_anomaly": "Retry ratio exceeded baseline.",
                    "D_build_provenance": None,
                    "E_mqtt_integrity": None,
                    "F_system_resources": None,
                    "G_deployment_path": None,
                    "H_compound_pattern": None,
                },
                "causal_analysis": {
                    "hypothesis": "Network interference affected bundle transfer.",
                    "triggered_hc": None,
                    "supporting_fields": ["context_data.transfer_metrics.retry_ratio_percent"],
                    "alternative_hypothesis": "Operational network instability",
                },
                "recommended_actions": ["Monitor the next transfer and inspect network telemetry."],
            },
            raw_response='{"decision":"CONDITIONAL_APPROVE"}',
        )

        self.assertEqual(normalized["decision"], "CONDITIONAL_APPROVE")
        self.assertEqual(normalized["reason"], normalized["summary"])
        self.assertEqual(
            normalized["recommendations"],
            ["Monitor the next transfer and inspect network telemetry."],
        )
        self.assertEqual(
            normalized["analysis"]["C_transfer_anomaly"],
            "Retry ratio exceeded baseline.",
        )
        self.assertIsNone(normalized["causal_analysis"]["triggered_hc"])

    def test_normalize_llm_json_result_rejects_invalid_schema(self):
        normalized = _normalize_llm_json_result(
            {
                "decision": "REJECT",
                "summary": "Invalid output without actions.",
                "warnings": [],
                "analysis": {},
                "causal_analysis": {},
                "recommended_actions": [],
            },
            raw_response='{"decision":"REJECT"}',
        )

        self.assertEqual(normalized["decision"], "REJECT")
        self.assertEqual(normalized["warnings"], ["llm_response_schema_invalid"])


if __name__ == "__main__":
    unittest.main()
