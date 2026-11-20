"""Code-level switch for the optional, legacy cloud verification feature.

This switch is independent of the on-device Qwen3 verification policy.
ENABLED controls feature availability; verification_policy controls runtime use.
Legacy environment variables and force_llm cannot select the runtime mode.
"""

ENABLED = True


class ClaudeVerificationDisabled(RuntimeError):
    pass


def require_enabled():
    from verification_policy import snapshot
    if not ENABLED or snapshot()["mode"] != "CLAUDE":
        raise ClaudeVerificationDisabled("Claude verification is disabled by the active policy.")


def verify(ota_log, *, model):
    require_enabled()
    from llm_verifier import call_llm_verification, apply_longitudinal_escalation

    result = call_llm_verification(ota_log, model=model)
    return apply_longitudinal_escalation(ota_log, result)
