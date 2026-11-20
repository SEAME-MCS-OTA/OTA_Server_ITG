import test from 'node:test';
import assert from 'node:assert/strict';
import { recordView, resultColor } from './record.mjs';

test('old audit approval is neither a model verdict nor proof of installation', () => {
  const value = recordView({decision:'APPROVE',verify_mode:'record_only',llm_metrics:{model:null,call_count:0}});
  assert.equal(value.mode,'Legacy');
  assert.equal(value.modelDecision,'Not recorded');
  assert.equal(value.gatewayDecision,'Not recorded');
  assert.equal(value.install,'Not recorded');
  assert.equal(value.hasMetrics,false);
});

test('explicitly skipped inference is distinct from Gateway approval and failed install', () => {
  const value = recordView({decision:'REJECT',verify_mode:'record_only',reason:'RAUC install failed',
    ota_log_raw:{_verification_context:{mode:'RULES_ONLY',model_call_status:'SKIPPED_BY_POLICY',
      model_decision:null,gateway_decision:'APPROVE'}}});
  assert.equal(value.mode,'Rules only');
  assert.equal(value.result,'REJECT');
  assert.equal(value.modelDecision,'Skipped');
  assert.equal(value.gatewayDecision,'APPROVE');
  assert.equal(value.install,'Failed');
});

test('legacy conditional and unknown results are never relabelled REJECT', () => {
  const value = recordView({decision:'CONDITIONAL_APPROVE',verify_mode:'gate',
    llm_metrics:{model:'claude-sonnet-4-6',call_count:1}});
  assert.equal(value.mode,'LLM');
  assert.equal(value.result,'CONDITIONAL_APPROVE');
  assert.equal(value.modelDecision,'Not recorded');
  assert.match(resultColor(value.result),/amber/);
  assert.match(resultColor('UNKNOWN'),/gray/);
});

test('current on-device audit keeps prediction, decision and install separate', () => {
  const value = recordView({decision:'APPROVE',verify_mode:'record_only',reason:'Post-install verification record stored.',
    ota_log_raw:{_verification_context:{mode:'ON_DEVICE',model_decision:'APPROVE',gateway_decision:'APPROVE',request_id:'req-1'}}});
  assert.equal(value.mode,'OnDevice');
  assert.equal(value.modelDecision,'APPROVE');
  assert.equal(value.install,'Installed');
  assert.equal(value.requestId,'req-1');
});

test('stored Claude prediction is preserved when the processed result differs', () => {
  const value = recordView({decision:'REJECT', verify_mode:'gate',
    raw_response:'{"decision":"CONDITIONAL_APPROVE"}',
    llm_metrics:{model:'claude-sonnet-4-6',call_count:1}});
  assert.equal(value.modelDecision,'CONDITIONAL_APPROVE');
  assert.equal(value.result,'REJECT');
  assert.equal(value.gatewayDecision,'Not recorded');
});
