#!/usr/bin/env bash
# Install systemd wiring that recovers the disposable Browser Links SeekDB index
# if seekdb.service repeatedly fails after a reboot.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RECOVER_SCRIPT="$SCRIPT_DIR/seekdb-index-recover.sh"
RECOVERY_SERVICE="/etc/systemd/system/xarta-seekdb-index-recover.service"
DROPIN_DIR="/etc/systemd/system/seekdb.service.d"
RECOVERY_DROPIN="$DROPIN_DIR/xarta-recovery.conf"

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: must run as root" >&2
    exit 1
fi

if [[ ! -x "$RECOVER_SCRIPT" ]]; then
    echo "ERROR: recovery script is missing or not executable: $RECOVER_SCRIPT" >&2
    exit 1
fi

install -d -m 0755 "$DROPIN_DIR"

cat > "$RECOVERY_SERVICE" <<EOF
[Unit]
Description=Xarta recover disposable SeekDB Browser Links index
Documentation=file:$RECOVER_SCRIPT
After=network-online.target

[Service]
Type=oneshot
ExecStart=$RECOVER_SCRIPT --recover --from-systemd --no-blueprints-restart
EOF

cat > "$RECOVERY_DROPIN" <<'EOF'
[Unit]
OnFailure=xarta-seekdb-index-recover.service
EOF

systemctl daemon-reload

echo "installed $RECOVERY_SERVICE"
echo "installed $RECOVERY_DROPIN"
