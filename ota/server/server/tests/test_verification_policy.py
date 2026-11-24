"""Exercise the real Flask route bodies without DB/MQTT startup or paid calls."""
import ast
import json
import logging
from pathlib import Path
import re
import sys
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from flask import Flask, jsonify, request

SERVER = Path(__file__).parents[1]
sys.path.insert(0, str(SERVER))
import claude_verification
import verification_policy as policy


@pytest.fixture
def api(tmp_path, monkeypatch):
    monkeypatch.setattr(policy, 'POLICY_PATH', tmp_path / 'policy.json')
    names = {'verification_config', 'get_llm_config', 'set_llm_config', 'verify_ota_update'}
    nodes = [n for n in ast.parse((SERVER / 'app.py').read_text()).body
             if isinstance(n, ast.FunctionDef) and n.name in names]
    for n in nodes:
        n.decorator_list = []
    scope = dict(verification_policy=policy, claude_verification=claude_verification,
        Config=SimpleNamespace(LLM_MODEL='claude-test'), request=request, jsonify=jsonify,
        re=re, datetime=datetime, logger=logging.getLogger('test'),
        preprocess_log=lambda x:x, _verify_payload_all_passed=lambda _:True,
        _verify_collect_non_passing_rules=lambda _:[], save_verification_result=Mock(),
        _llm_enabled=True)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SERVER / 'app.py'), 'exec'), scope)
    app = Flask('policy-test')
    for path, name, methods in [('/api/v1/verification/config', 'verification_config', ['GET','POST']),
                               ('/api/ota/verify', 'verify_ota_update', ['POST'])]:
        app.add_url_rule(path, view_func=scope[name], methods=methods)
    return app.test_client(), scope


def test_default_ignores_legacy_on_and_persists_modes(api):
    client, _ = api
    assert client.get('/api/v1/verification/config').json['mode'] == 'ON_DEVICE'
    for mode in ['RULES_ONLY','CLAUDE','ON_DEVICE']:
        r=client.post('/api/v1/verification/config',json={'mode':mode})
        assert r.status_code == 200
        assert r.json['enabled'] == (mode == 'CLAUDE')
        assert r.json['ondevice_enabled'] == (mode == 'ON_DEVICE')
        assert json.loads(policy.POLICY_PATH.read_text())['mode'] == mode
    assert client.post('/api/v1/verification/config',json={'mode':'BOTH'}).status_code==400
    assert client.get('/api/v1/verification/config?nonce='+'a'*32).json['nonce']=='a'*32
    assert client.get('/api/v1/verification/config').headers['Cache-Control']=='no-store'


def test_disabled_cloud_cannot_autoapprove_or_call_provider(api, monkeypatch):
    client, scope = api
    call=Mock(side_effect=AssertionError('must not invoke paid API'))
    monkeypatch.setattr(claude_verification, 'verify', call)
    payload={'schema_version':'ota-verify-v2','rule_check_results':{},'context_data':{}}
    for mode in ['ON_DEVICE','RULES_ONLY']:
        policy.set_mode(mode)
        r=client.post('/api/ota/verify',json=payload)
        assert r.status_code==503 and r.json['decision']=='HOLD'
        r=client.post('/api/ota/verify',json=payload,headers={'X-OTA-Verify-Mode':'record_only'})
        assert r.status_code==200
    assert scope['save_verification_result'].call_count==2
    call.assert_not_called()


def test_cloud_enabled_calls_module_and_echoes_binding(api, monkeypatch):
    client, _=api
    policy.set_mode('CLAUDE')
    call=Mock(return_value={'decision':'REJECT','reason':'attack detected'})
    monkeypatch.setattr(claude_verification, 'verify', call)
    binding={'request_id':'req-test','image_sha256':'a'*64}
    r=client.post('/api/ota/verify',json={'schema_version':'ota-verify-v2',
        'rule_check_results':{},'context_data':{},'_verification_binding':binding})
    assert r.status_code==200 and r.json['decision']=='REJECT'
    assert r.json['binding']==binding
    call.assert_called_once()


@pytest.mark.parametrize('decision', ['APPROVE', 'REJECT', 'HOLD'])
def test_record_only_preserves_gateway_decisions_without_paid_inference(api, monkeypatch, decision):
    client, scope = api
    call = Mock(side_effect=AssertionError('must not invoke paid API'))
    monkeypatch.setattr(claude_verification, 'verify', call)
    payload = {'schema_version': 'ota-verify-v3', 'rule_check_results': {}, 'context_data': {},
               '_verification_context': {'mode': 'ON_DEVICE', 'gateway_decision': decision}}
    r = client.post('/api/ota/verify', json=payload, headers={
        'X-OTA-Verify-Mode': 'record_only', 'X-OTA-Verify-Decision': decision})
    assert r.status_code == 200 and r.json['decision'] == decision
    saved_payload, result = scope['save_verification_result'].call_args.args
    assert saved_payload == payload and result['decision'] == decision
    call.assert_not_called()


def test_unknown_recorded_decision_is_not_stored_as_approval(api):
    client, scope = api
    r = client.post('/api/ota/verify', json={'schema_version': 'ota-verify-v3',
        'rule_check_results': {}, 'context_data': {}}, headers={
        'X-OTA-Verify-Mode': 'record_only', 'X-OTA-Verify-Decision': 'MAYBE'})
    assert r.status_code == 400
    scope['save_verification_result'].assert_not_called()


def test_code_switch_blocks_runtime_enable(api, monkeypatch):
    client, _=api
    monkeypatch.setattr(claude_verification,'ENABLED',False)
    assert client.post('/api/v1/verification/config',json={'mode':'CLAUDE'}).status_code==400
    with pytest.raises(claude_verification.ClaudeVerificationDisabled):
        claude_verification.require_enabled()


def test_sdk_and_raw_http_entrypoints_are_guarded(api, monkeypatch):
    import llm_verifier
    import importlib.util
    spec=importlib.util.spec_from_file_location('isolated_claude_client',SERVER/'llm_pipeline/claude_client.py')
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    ClaudeClient=module.ClaudeClient
    network=Mock(side_effect=AssertionError('unexpected paid network call'))
    monkeypatch.setattr('requests.post',network)
    with pytest.raises(claude_verification.ClaudeVerificationDisabled):
        llm_verifier.call_llm_verification({})
    with pytest.raises(claude_verification.ClaudeVerificationDisabled):
        ClaudeClient(api_key='dummy',model='dummy',timeout_sec=1).analyze_log(log_json='{}')
    network.assert_not_called()
