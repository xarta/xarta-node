#!/usr/bin/env bash
# bp-backup.sh — create a local DB backup (works even when blueprints-app is down)
#
# Creates a timestamped copy of blueprints.db in BLUEPRINTS_BACKUP_DIR with
# the sync_queue table cleared.  Uses Python's sqlite3 backup API for a
# WAL-safe consistent copy.
#
# Usage:
#   ./bp-backup.sh           — create backup, print filename
#   ./bp-backup.sh --api     — use the HTTP API instead (app must be running)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# ── Load env ──────────────────────────────────────────────────────────────────
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: .env not found at $ENV_FILE" >&2
  exit 1
fi
set -o allexport
# shellcheck source=/dev/null
source "$ENV_FILE"
set +o allexport

DB_PATH="${BLUEPRINTS_DB_DIR:-/opt/blueprints/data/db}/blueprints.db"
BACKUP_DIR="${BLUEPRINTS_BACKUP_DIR:-}"

if [[ -z "$BACKUP_DIR" ]]; then
  echo "ERROR: BLUEPRINTS_BACKUP_DIR is not set in .env" >&2
  exit 1
fi

mkdir -p "$BACKUP_DIR"

# ── API mode ──────────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--api" ]]; then
  BASE="${BLUEPRINTS_UI_URL:-http://localhost:8080}"
  echo "Calling POST ${BASE}/api/v1/backup …"
  curl -sf -X POST "${BASE}/api/v1/backup" \
    -H "Content-Type: application/json" | python3 -m json.tool
  exit $?
fi

# ── Direct mode (offline-safe) ────────────────────────────────────────────────
if [[ ! -f "$DB_PATH" ]]; then
  echo "ERROR: DB not found at $DB_PATH" >&2
  exit 1
fi

TIMESTAMP="$(date -u '+%Y-%m-%d-%H%M%S')"
FILENAME="${TIMESTAMP}-blueprints.db.tar.gz"
DEST="${BACKUP_DIR}/${FILENAME}"

python3 - <<PYEOF
import sqlite3, tarfile, os, tempfile
from pathlib import Path

src_path = "$DB_PATH"
dst_path = "$DEST"
db_dir   = Path(src_path).parent

# Clone DB to a temp file first (WAL-safe)
with tempfile.NamedTemporaryFile(dir=db_dir, suffix='.db.tmp', delete=False) as f:
    tmp_db = Path(f.name)
try:
    src = sqlite3.connect(src_path)
    dst = sqlite3.connect(str(tmp_db))
    try:
        src.backup(dst)
    finally:
        src.close()
        dst.close()
    # Strip sync_queue
    c = sqlite3.connect(str(tmp_db))
    c.execute('DELETE FROM sync_queue')
    c.commit()
    c.close()
    # Compress to destination
    with tarfile.open(dst_path, 'w:gz') as tar:
        tar.add(str(tmp_db), arcname='blueprints.db')
finally:
    try:
        os.unlink(str(tmp_db))
    except OSError:
        pass

size = os.path.getsize(dst_path)
print(f'Backup created: {dst_path}')
print(f'Size: {size:,} bytes')
PYEOF
