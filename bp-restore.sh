#!/usr/bin/env bash
# bp-restore.sh — restore a local DB backup (works even when blueprints-app is down)
#
# ⚠ This only restores THIS node's DB.  Other nodes are NOT automatically
#   updated.  The restored gen will usually be below current peer gens, so
#   peers will push their state back to this node at next sync, eventually
#   overwriting the restore.
#
#   Use --force to query peers for their max gen and set the restored DB's
#   gen to max+1.  This node will then win the gen guard on next sync and
#   propagate the restored state to all peers.  Only use this for disaster
#   recovery / corruption fix scenarios.
#
# Usage:
#   ./bp-restore.sh                      — interactive selection
#   ./bp-restore.sh <filename>           — restore specific backup
#   ./bp-restore.sh <filename> --force   — restore + bump gen above all peers
#   ./bp-restore.sh --api <filename>     — use HTTP API (app must be running)

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

# ── Parse args ────────────────────────────────────────────────────────────────
FORCE=false
API_MODE=false
FILENAME=""

for arg in "$@"; do
  case "$arg" in
    --force) FORCE=true ;;
    --api)   API_MODE=true ;;
    *)       FILENAME="$arg" ;;
  esac
done

# ── API mode ──────────────────────────────────────────────────────────────────
if [[ "$API_MODE" == true ]]; then
  BASE="${BLUEPRINTS_UI_URL:-http://localhost:8080}"
  if [[ -z "$FILENAME" ]]; then
    echo "Listing backups via API…"
    curl -sf "${BASE}/api/v1/backup" | python3 -m json.tool
    echo ""
    read -rp "Enter filename to restore: " FILENAME
  fi
  FORCE_PARAM=""
  if [[ "$FORCE" == true ]]; then
    FORCE_PARAM="?force=true"
  fi
  echo ""
  echo "⚠  WARNING: This will replace the live DB on this node."
  if [[ "$FORCE" == true ]]; then
    echo "⚠  FORCE mode: gen will be bumped above all peers."
    echo "   All peers will be overwritten on next sync."
  fi
  read -rp "Continue? [y/N] " CONFIRM
  [[ "$CONFIRM" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }
  echo "Restoring via API…"
  curl -sf -X POST "${BASE}/api/v1/backup/restore/${FILENAME}${FORCE_PARAM}" \
    -H "Content-Type: application/json" | python3 -m json.tool
  echo ""
  echo "Restarting blueprints-app…"
  systemctl restart blueprints-app
  exit $?
fi

# ── Direct mode (offline-safe) ────────────────────────────────────────────────
if [[ ! -d "$BACKUP_DIR" ]]; then
  echo "ERROR: Backup directory not found: $BACKUP_DIR" >&2
  exit 1
fi

# List available backups
mapfile -t BACKUPS < <(ls -1r "${BACKUP_DIR}"/*-blueprints.db.tar.gz 2>/dev/null || true)

if [[ ${#BACKUPS[@]} -eq 0 ]]; then
  echo "No backups found in $BACKUP_DIR" >&2
  exit 1
fi

# If no filename given, show interactive picker
if [[ -z "$FILENAME" ]]; then
  echo "Available backups (newest first):"
  echo ""
  for i in "${!BACKUPS[@]}"; do
    f="${BACKUPS[$i]}"
    size=$(du -h "$f" | cut -f1)
    printf "  %2d) %s  (%s)\n" "$((i+1))" "$(basename "$f")" "$size"
  done
  echo ""
  read -rp "Select backup number (or q to quit): " CHOICE
  [[ "$CHOICE" == "q" ]] && exit 0
  if ! [[ "$CHOICE" =~ ^[0-9]+$ ]] || (( CHOICE < 1 || CHOICE > ${#BACKUPS[@]} )); then
    echo "Invalid selection." >&2
    exit 1
  fi
  FILENAME="$(basename "${BACKUPS[$((CHOICE-1))]}")"
fi

SRC="${BACKUP_DIR}/${FILENAME}"
if [[ ! -f "$SRC" ]]; then
  echo "ERROR: Backup not found: $SRC" >&2
  exit 1
fi

# ── Confirmation ──────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  ⚠  RESTORE WARNING                                         ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  This will replace the live DB on THIS node only.           ║"
echo "║  Other nodes will NOT be updated automatically.             ║"
echo "║                                                              ║"
echo "║  Without --force: the restored gen will be below peer gens. ║"
echo "║  Peers will push their current state back to this node at   ║"
echo "║  next sync, overwriting this restore.                       ║"
if [[ "$FORCE" == true ]]; then
echo "║                                                              ║"
echo "║  ⚠  FORCE MODE ACTIVE — gen will be bumped above ALL peers  ║"
echo "║  All peers will be overwritten on next sync drain.          ║"
echo "║  Only use this for disaster recovery / corruption fixes.    ║"
fi
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  Restoring: $FILENAME"
echo "  Into:      $DB_PATH"
echo ""
read -rp "Continue? [y/N] " CONFIRM
[[ "$CONFIRM" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }

# ── Stop service ──────────────────────────────────────────────────────────────
SERVICE_CMD="${SERVICE_RESTART_CMD:-}"
SERVICE_UNIT=""
if [[ "$SERVICE_CMD" =~ systemctl\ restart\ (.+) ]]; then
  SERVICE_UNIT="${BASH_REMATCH[1]}"
fi

if [[ -n "$SERVICE_UNIT" ]]; then
  echo "Stopping $SERVICE_UNIT …"
  systemctl stop "$SERVICE_UNIT" || echo "(stop failed — continuing anyway)"
fi

# ── Perform restore ───────────────────────────────────────────────────────────
python3 - <<PYEOF
import sqlite3, tarfile, os, json, tempfile
from pathlib import Path

src_path = "$SRC"
db_path  = "$DB_PATH"
force    = "$FORCE" == "true"
self_url = "${BLUEPRINTS_UI_URL:-}"

db_dir   = Path(db_path).parent
tmp_path = str(db_dir / 'blueprints.db.restore_tmp')

# Extract blueprints.db from the .tar.gz archive
with tarfile.open(src_path, 'r:gz') as tar:
    member = tar.getmember('blueprints.db')
    with tar.extractfile(member) as f_in, open(tmp_path, 'wb') as f_out:
        f_out.write(f_in.read())

conn = sqlite3.connect(tmp_path)
try:
    # Always clear sync_queue
    conn.execute('DELETE FROM sync_queue')

    if force:
        import urllib.request
        # Query peers for their max gen
        with sqlite3.connect(db_path) as live:
            rows = live.execute(
                'SELECT addresses FROM nodes WHERE addresses IS NOT NULL'
            ).fetchall()
        peer_gens = []
        for row in rows:
            try:
                addrs = json.loads(row[0] or '[]')
            except:
                addrs = []
            for addr in addrs:
                url = addr.rstrip('/') + '/health'
                try:
                    with urllib.request.urlopen(url, timeout=5) as resp:
                        data = json.loads(resp.read())
                        g = data.get('gen')
                        if g is not None:
                            peer_gens.append(int(g))
                            print(f'  peer {addr}: gen={g}')
                except Exception as e:
                    print(f'  peer {addr}: unreachable ({e})')

        if peer_gens:
            new_gen = max(peer_gens) + 1
            conn.execute('UPDATE sync_meta SET value=? WHERE key=\'gen\'', (str(new_gen),))
            print(f'Force: setting gen={new_gen} (was max peer gen {max(peer_gens)})')
        else:
            print('Force: no peers reachable — gen not bumped')

    # Read final gen
    row = conn.execute('SELECT value FROM sync_meta WHERE key=\'gen\'').fetchone()
    final_gen = row[0] if row else 'unknown'
    conn.commit()
finally:
    conn.close()

os.replace(tmp_path, db_path)
print(f'Restored: {src_path} → {db_path}')
print(f'Gen after restore: {final_gen}')
PYEOF

# ── Restart service ───────────────────────────────────────────────────────────
if [[ -n "$SERVICE_UNIT" ]]; then
  echo "Starting $SERVICE_UNIT …"
  systemctl start "$SERVICE_UNIT"
  echo "Done."
else
  echo "No SERVICE_RESTART_CMD configured — start the service manually."
fi
