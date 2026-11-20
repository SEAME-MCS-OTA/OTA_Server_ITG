// Stored audit results and model predictions are separate facts.
export function recordView(record) {
  const context = record.ota_log_raw?._verification_context || {};
  const metrics = record.llm_metrics || {};
  const modes = { ON_DEVICE: 'OnDevice', CLAUDE: 'LLM', RULES_ONLY: 'Rules only' };
  const cloudCalled = Number(metrics.call_count) > 0 && /^claude/i.test(metrics.model || '');
  const mode = modes[context.mode] || (cloudCalled ? 'LLM' : 'Legacy');
  const skipped = context.model_call_status === 'SKIPPED_BY_POLICY';
  let storedModelDecision = null;
  if (cloudCalled && record.raw_response) {
    try {
      const response = typeof record.raw_response === 'string' ? JSON.parse(record.raw_response) : record.raw_response;
      if (['APPROVE', 'REJECT', 'HOLD', 'CONDITIONAL_APPROVE'].includes(response.decision)) {
        storedModelDecision = response.decision;
      }
    } catch { /* A stored analysis string is not a structured model prediction. */ }
  }
  let install = 'Not recorded';
  if (record.verify_mode === 'record_only') {
    if (record.reason === 'Post-install verification record stored.') install = 'Installed';
    if (record.reason === 'RAUC install failed') install = 'Failed';
  }
  return {
    mode,
    result: record.decision || 'UNKNOWN',
    modelDecision: skipped ? 'Skipped' : context.model_decision || storedModelDecision || 'Not recorded',
    gatewayDecision: context.gateway_decision || 'Not recorded',
    install,
    requestId: context.request_id || null,
    hasMetrics: Object.values(metrics).some(value => value !== null && value !== undefined && value !== 0 && value !== ''),
  };
}

export function resultColor(decision) {
  if (decision === 'APPROVE') return 'bg-green-100 text-green-800';
  if (decision === 'REJECT') return 'bg-red-100 text-red-800';
  if (['HOLD', 'CONDITIONAL_APPROVE'].includes(decision)) return 'bg-amber-100 text-amber-800';
  return 'bg-gray-100 text-gray-600';
}
