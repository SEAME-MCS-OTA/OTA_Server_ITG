#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.ota-stack.yml"
LEGACY_OTA_GH_COMPOSE="${ROOT_DIR}/OTA_GH/docker-compose.yml"
ENV_FILE="${ROOT_DIR}/.env"
DOCKER_CMD=(docker)

cd "${ROOT_DIR}"

detect_host_ip() {
  local ip=""
  if command -v ip >/dev/null 2>&1; then
    ip="$(ip route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1); exit}}')"
  fi
  if [[ -z "${ip}" ]] && command -v hostname >/dev/null 2>&1; then
    ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
  fi
  echo "${ip}"
}

upsert_env_var() {
  local key="$1"
  local value="$2"
  local file="$3"
  touch "${file}"
  if grep -qE "^${key}=" "${file}"; then
    sed -i "s#^${key}=.*#${key}=${value}#g" "${file}"
  else
    printf '%s=%s\n' "${key}" "${value}" >> "${file}"
  fi
}

ensure_docker_access() {
  if docker info >/dev/null 2>&1; then
    DOCKER_CMD=(docker)
    return 0
  fi

  if command -v sudo >/dev/null 2>&1; then
    echo "[warn] Docker daemon permission denied for current user. Falling back to sudo."
    if sudo docker info >/dev/null 2>&1; then
      DOCKER_CMD=(sudo docker)
      return 0
    fi
  fi

  echo "[error] Cannot access Docker daemon. Re-login after adding user to docker group, or run with sudo." >&2
  exit 1
}

# RPi must download firmware from a host-reachable URL.
# If not provided, auto-derive a sane default from current host IP.
if [[ -z "${OTA_GH_FIRMWARE_BASE_URL:-}" ]]; then
  HOST_IP="$(detect_host_ip)"
  if [[ -n "${HOST_IP}" ]]; then
    export OTA_GH_FIRMWARE_BASE_URL="http://${HOST_IP}:${OTA_GH_SERVER_PORT:-8080}"
    echo "[info] OTA_GH_FIRMWARE_BASE_URL not set. Using ${OTA_GH_FIRMWARE_BASE_URL}"
    upsert_env_var "OTA_GH_FIRMWARE_BASE_URL" "${OTA_GH_FIRMWARE_BASE_URL}" "${ENV_FILE}"
  else
    echo "[warn] Could not detect host IP. OTA firmware URL may fall back to localhost."
  fi
fi

# If legacy OTA_GH stack is running, it usually occupies 8080/1883 and
# prevents this unified stack from starting. Stop it automatically.
ensure_docker_access

if [[ "${OTA_STACK_SKIP_LEGACY_DOWN:-0}" != "1" ]] && [[ -f "${LEGACY_OTA_GH_COMPOSE}" ]]; then
  if "${DOCKER_CMD[@]}" compose -f "${LEGACY_OTA_GH_COMPOSE}" ps -q | grep -q .; then
    echo "[info] Stopping legacy OTA_GH stack to avoid port conflicts..."
    "${DOCKER_CMD[@]}" compose -f "${LEGACY_OTA_GH_COMPOSE}" down
  fi
fi

"${DOCKER_CMD[@]}" compose -f "${COMPOSE_FILE}" up -d --build

echo
echo "== OTA stack services =="
"${DOCKER_CMD[@]}" compose -f "${COMPOSE_FILE}" ps
echo
echo "OTA_GH API:        http://localhost:${OTA_GH_SERVER_PORT:-8080}"
echo "OTA_GH Dashboard:  http://localhost:${OTA_GH_DASHBOARD_PORT:-3001}"
echo "OTA_VLM Backend:   http://localhost:${OTA_VLM_BACKEND_PORT:-4000}"
echo "OTA_VLM Frontend:  http://localhost:${OTA_VLM_FRONTEND_PORT:-5173}"
