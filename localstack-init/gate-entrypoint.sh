#!/bin/bash
# Wrapper entrypoint that installs our init script into LocalStack's ready.d
# at container start. This avoids the script being missing when the base image
# or runtime mounts over /etc/localstack/init (e.g. anonymous volume or bind).
set -euo pipefail
READY_D="/etc/localstack/init/ready.d"
INIT_SCRIPT="/opt/gate-init/init-aws.sh"
if [ -f "$INIT_SCRIPT" ]; then
  mkdir -p "$READY_D"
  cp -f "$INIT_SCRIPT" "$READY_D/init-aws.sh"
  chmod +x "$READY_D/init-aws.sh"
fi
exec /usr/local/bin/docker-entrypoint.sh "$@"
