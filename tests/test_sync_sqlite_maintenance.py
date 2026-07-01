import os
import sqlite3
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-sync-sqlite-maint-nodes.json"
NODES_JSON.write_text(
    """
    {
      "nodes": [
        {
          "node_id": "test-node",
          "display_name": "Test Node",
          "host_machine": "test-host",
          "primary_hostname": "test-node.local",
          "tailnet_hostname": "test-node.tailnet",
          "primary_ip": "127.0.0.1",
          "sync_port": 8080,
          "tailnet": "test",
          "tailnet_ip": "100.64.0.1",
          "active": true
        }
      ]
    }
    """,
    encoding="utf-8",
)
os.environ.setdefault("BLUEPRINTS_NODE_ID", "test-node")
os.environ.setdefault("NODES_JSON_PATH", str(NODES_JSON))
os.environ.setdefault(
    "BLUEPRINTS_DB_DIR", tempfile.mkdtemp(prefix="blueprints-test-sync-maint-db-")
)
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

from app import routes_backup  # noqa: E402
from app.sync import restore  # noqa: E402
from app.sync.sqlite_maintenance import clone_without_sync_queue  # noqa: E402


def _make_source_db(path: Path, *, queue_rows: int = 3) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE sync_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO sync_meta (key, value) VALUES ('gen', '42');
            CREATE TABLE sync_queue (
                queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_node_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                table_name TEXT NOT NULL,
                row_id TEXT NOT NULL,
                row_data TEXT,
                gen INTEGER NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                sent INTEGER DEFAULT 0,
                sent_at TEXT DEFAULT '',
                guid TEXT DEFAULT ''
            );
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO settings (key, value) VALUES ('probe', 'ok');
            """
        )
        for i in range(queue_rows):
            conn.execute(
                """
                INSERT INTO sync_queue
                    (target_node_id, action_type, table_name, row_id, row_data, gen, sent, sent_at, guid)
                VALUES (?, 'UPDATE', 'settings', ?, ?, 42, 1, '2026-07-01 00:00:00', ?)
                """,
                ("peer-1", f"row-{i}", '{"value":"ok"}', f"guid-{i}"),
            )
        conn.commit()
    finally:
        conn.close()


def _sync_queue_count(path: Path) -> int:
    conn = sqlite3.connect(path)
    try:
        return int(conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0])
    finally:
        conn.close()


def _integrity(path: Path) -> str:
    conn = sqlite3.connect(path)
    try:
        return str(conn.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        conn.close()


def test_clone_without_sync_queue_strips_and_vacuums_copy(tmp_path):
    source = tmp_path / "source.db"
    clone = tmp_path / "clone.db"
    _make_source_db(source, queue_rows=5)

    report = clone_without_sync_queue(source, clone, vacuum=True)

    assert report["deleted_sync_queue_rows"] == 5
    assert _sync_queue_count(source) == 5
    assert _sync_queue_count(clone) == 0
    assert _integrity(clone) == "ok"


def test_make_full_backup_uses_queue_free_clone(tmp_path, monkeypatch):
    source = tmp_path / "source.db"
    extracted = tmp_path / "extracted.db"
    _make_source_db(source, queue_rows=4)
    monkeypatch.setattr(restore.cfg, "DB_PATH", str(source))

    zip_bytes, checksum = restore.make_full_backup()

    assert checksum
    zip_path = tmp_path / "backup.zip"
    zip_path.write_bytes(zip_bytes)
    with zipfile.ZipFile(zip_path, "r") as zf:
        extracted.write_bytes(zf.read("blueprints.db"))
    assert _sync_queue_count(extracted) == 0
    assert _integrity(extracted) == "ok"


def test_api_backup_file_uses_queue_free_vacuumed_clone(tmp_path, monkeypatch):
    source = tmp_path / "source.db"
    archive = tmp_path / "2026-07-01-000000-blueprints.db.tar.gz"
    extracted = tmp_path / "extracted.db"
    _make_source_db(source, queue_rows=6)
    monkeypatch.setattr(routes_backup.cfg, "DB_PATH", str(source))

    routes_backup._create_backup_file(archive)

    with tarfile.open(archive, "r:gz") as tar:
        member = tar.getmember("blueprints.db")
        file_obj = tar.extractfile(member)
        assert file_obj is not None
        extracted.write_bytes(file_obj.read())
    assert _sync_queue_count(extracted) == 0
    assert _integrity(extracted) == "ok"
