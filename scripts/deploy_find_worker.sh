#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

ENVIRONMENT="${1:-${MODAL_ENVIRONMENT:-main}}"
APP_NAME="${MX8_FIND_MODAL_APP_NAME:-mx8-find-worker}"
FUNCTION_NAME="${MX8_FIND_MODAL_FUNCTION_NAME:-process_shard}"
GPU="${MX8_FIND_MODAL_GPU:-L4}"
CACHE_VOLUME="${MX8_FIND_MODAL_HF_CACHE_VOLUME:-mx8-find-hf-cache}"

MODAL_CMD=()
if command -v modal >/dev/null 2>&1; then
  MODAL_CMD=(modal)
elif python3 -c "import modal" >/dev/null 2>&1; then
  MODAL_CMD=(python3 -m modal)
else
  echo "error: Modal is not installed. Install the CLI or the Python package first." >&2
  exit 1
fi

cd "${REPO_ROOT}"

echo "Deploying ${APP_NAME} to Modal environment ${ENVIRONMENT}"
echo "GPU=${GPU}"
echo "HF cache volume=${CACHE_VOLUME}"

MODAL_ENVIRONMENT="${ENVIRONMENT}" \
MX8_FIND_MODAL_APP_NAME="${APP_NAME}" \
MX8_FIND_MODAL_FUNCTION_NAME="${FUNCTION_NAME}" \
MX8_FIND_MODAL_GPU="${GPU}" \
MX8_FIND_MODAL_HF_CACHE_VOLUME="${CACHE_VOLUME}" \
"${MODAL_CMD[@]}" deploy inference/modal_find_worker.py --env "${ENVIRONMENT}"

cat <<EOF

Modal worker deployed.

Set these env vars on the MX8 API:
  export MX8_FIND_PROVIDER=modal
  export MX8_FIND_MODAL_APP_NAME=${APP_NAME}
  export MX8_FIND_MODAL_FUNCTION_NAME=${FUNCTION_NAME}
  export MX8_FIND_MODAL_ENVIRONMENT=${ENVIRONMENT}
  export MX8_FIND_S3_REGION=\${MX8_FIND_S3_REGION:-us-east-1}
  export MX8_FIND_S3_PRESIGN_EXPIRES_SECS=\${MX8_FIND_S3_PRESIGN_EXPIRES_SECS:-3600}

Then start the API and submit a small find job as a smoke test.
EOF
