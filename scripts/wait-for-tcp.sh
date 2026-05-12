#!/bin/bash
set -euo pipefail

HOST="$1"
PORT="$2"
TIMEOUT_SECONDS="${3:-120}"

echo "Aguardando ${HOST}:${PORT} por ate ${TIMEOUT_SECONDS}s..."

for _ in $(seq 1 "${TIMEOUT_SECONDS}"); do
    if timeout 1 bash -c "cat < /dev/null > /dev/tcp/${HOST}/${PORT}" 2>/dev/null; then
        echo "${HOST}:${PORT} disponivel."
        exit 0
    fi
    sleep 1
done

echo "Timeout aguardando ${HOST}:${PORT}."
exit 1
