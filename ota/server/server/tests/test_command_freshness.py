"""Protocol helpers are isolated from the production DB/MQTT startup path."""
import ast
import base64
import json
import logging
import os
from pathlib import Path
import subprocess
import tempfile
import time
from types import SimpleNamespace

from flask import Flask, jsonify, request
import pytest

APP = Path(__file__).parents[1] / 'app.py'


def helpers():
    names = {'_command_payload_bytes', '_sign_command_payload', 'verification_time'}
    nodes = [n for n in ast.parse(APP.read_text()).body
             if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in nodes} == names
    for node in nodes:
        node.decorator_list = []
    scope = dict(json=json, base64=base64, logging=logging, os=os,
                 subprocess=subprocess, tempfile=tempfile, time=time,
                 jsonify=jsonify, request=request, logger=logging.getLogger('test'))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(APP), 'exec'), scope)
    return scope


@pytest.mark.parametrize('nonce', ['', 'a'*31, 'A'*32, 'g'*32, 'a'*33])
def test_time_query_rejects_invalid_nonce(nonce):
    h = helpers()
    with Flask('test').test_request_context('/api/ota/verification-time?nonce='+nonce):
        assert h['verification_time']()[1] == 400


def test_time_query_echoes_fresh_nonce_without_caching():
    h = helpers()
    nonce = '0123456789abcdef'*2
    with Flask('test').test_request_context('/api/ota/verification-time?nonce='+nonce):
        reply = h['verification_time']()
        value = reply.get_json()
        assert value['nonce'] == nonce
        assert type(value['unix_seconds']) is int
        assert abs(value['unix_seconds']-time.time()) < 2
        assert reply.headers['Cache-Control'] == 'no-store'


def test_real_ed25519_signature_binds_device_and_time(tmp_path):
    h = helpers()
    key, pub = tmp_path/'test.key', tmp_path/'test.pub'
    subprocess.run(['openssl', 'genpkey', '-algorithm', 'ed25519', '-out', str(key)],
                   check=True, capture_output=True)
    subprocess.run(['openssl', 'pkey', '-in', str(key), '-pubout', '-out', str(pub)],
                   check=True, capture_output=True)
    h['Config'] = SimpleNamespace(COMMAND_SIGNING_ENABLED=True,
        COMMAND_SIGNING_PRIVATE_KEY_PATH=str(key), COMMAND_SIGNING_KEY_ID='test')
    command = dict(ota_id='compatibility-test-no-install', url='https://server/example.raucb',
        target_version='3.7.6', expected_sha256='a'*64, expected_size=1,
        device_id='test-device', issued_at=1000, expires_at=4600)
    signature, error = h['_sign_command_payload'](**command)
    assert error is None
    data = json.loads(h['_command_payload_bytes'](**command))
    assert data['command_schema_version'] == 'ota-command-v2'
    sig = tmp_path/'signature'; sig.write_bytes(base64.b64decode(signature['value']))
    msg = tmp_path/'message'
    def verify(value):
        msg.write_bytes(h['_command_payload_bytes'](**value))
        return subprocess.run(['openssl', 'pkeyutl', '-verify', '-pubin', '-inkey',
            str(pub), '-rawin', '-in', str(msg), '-sigfile', str(sig)], capture_output=True).returncode == 0
    assert verify(command)
    for name in ('device_id', 'issued_at', 'expires_at'):
        changed = dict(command)
        changed[name] = command[name]+1 if isinstance(command[name], int) else 'other-device'
        assert not verify(changed)
